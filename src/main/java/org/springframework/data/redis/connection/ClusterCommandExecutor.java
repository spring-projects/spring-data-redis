/*
 * Copyright 2015-2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.redis.connection;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.ClusterRedirectException;
import org.springframework.data.redis.ClusterStateFailureException;
import org.springframework.data.redis.ExceptionTranslationStrategy;
import org.springframework.data.redis.TooManyClusterRedirectionsException;
import org.springframework.data.redis.connection.util.ByteArraySet;
import org.springframework.data.redis.connection.util.ByteArrayWrapper;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;

/**
 * {@link ClusterCommandExecutor} takes care of running commands across the known cluster nodes. By providing an
 * {@link AsyncTaskExecutor} the execution behavior can be influenced.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 1.7
 */
public class ClusterCommandExecutor implements DisposableBean {

	private AsyncTaskExecutor executor;
	private final ClusterTopologyProvider topologyProvider;
	private final ClusterNodeResourceProvider resourceProvider;
	private final ExceptionTranslationStrategy exceptionTranslationStrategy;
	private int maxRedirects = 5;

	/**
	 * Create a new instance of {@link ClusterCommandExecutor}.
	 *
	 * @param topologyProvider must not be {@literal null}.
	 * @param resourceProvider must not be {@literal null}.
	 * @param exceptionTranslation must not be {@literal null}.
	 */
	public ClusterCommandExecutor(ClusterTopologyProvider topologyProvider, ClusterNodeResourceProvider resourceProvider,
			ExceptionTranslationStrategy exceptionTranslation) {

		Assert.notNull(topologyProvider);
		Assert.notNull(resourceProvider);
		Assert.notNull(exceptionTranslation);

		this.topologyProvider = topologyProvider;
		this.resourceProvider = resourceProvider;
		this.exceptionTranslationStrategy = exceptionTranslation;
	}

	/**
	 * @param topologyProvider must not be {@literal null}.
	 * @param resourceProvider must not be {@literal null}.
	 * @param exceptionTranslation must not be {@literal null}.
	 * @param executor can be {@literal null}.
	 */
	public ClusterCommandExecutor(ClusterTopologyProvider topologyProvider, ClusterNodeResourceProvider resourceProvider,
			ExceptionTranslationStrategy exceptionTranslation, AsyncTaskExecutor executor) {

		this(topologyProvider, resourceProvider, exceptionTranslation);
		this.executor = executor;
	}

	{
		if (executor == null) {
			ThreadPoolTaskExecutor threadPoolTaskExecutor = new ThreadPoolTaskExecutor();
			threadPoolTaskExecutor.initialize();
			this.executor = threadPoolTaskExecutor;
		}
	}

	/**
	 * Run {@link ClusterCommandCallback} on a random node.
	 *
	 * @param cmd must not be {@literal null}.
	 * @return
	 */
	public <T> NodeResult<T> executeCommandOnArbitraryNode(ClusterCommandCallback<?, T> cmd) {

		Assert.notNull(cmd, "ClusterCommandCallback must not be null!");
		List<RedisClusterNode> nodes = new ArrayList<RedisClusterNode>(getClusterTopology().getActiveNodes());
		return executeCommandOnSingleNode(cmd, nodes.get(new Random().nextInt(nodes.size())));
	}

	/**
	 * Run {@link ClusterCommandCallback} on given {@link RedisClusterNode}.
	 *
	 * @param cmd must not be {@literal null}.
	 * @param node must not be {@literal null}.
	 * @throws IllegalArgumentException in case no resource can be acquired for given node.
	 * @return
	 */
	public <S, T> NodeResult<T> executeCommandOnSingleNode(ClusterCommandCallback<S, T> cmd, RedisClusterNode node) {
		return executeCommandOnSingleNode(cmd, node, 0);
	}

	private <S, T> NodeResult<T> executeCommandOnSingleNode(ClusterCommandCallback<S, T> cmd, RedisClusterNode node,
			int redirectCount) {

		Assert.notNull(cmd, "ClusterCommandCallback must not be null!");
		Assert.notNull(node, "RedisClusterNode must not be null!");

		if (redirectCount > maxRedirects) {
			throw new TooManyClusterRedirectionsException(String.format(
					"Cannot follow Cluster Redirects over more than %s legs. Please consider increasing the number of redirects to follow. Current value is: %s.",
					redirectCount, maxRedirects));
		}

		RedisClusterNode nodeToUse = lookupNode(node);

		S client = this.resourceProvider.getResourceForSpecificNode(nodeToUse);
		Assert.notNull(client, "Could not acquire resource for node. Is your cluster info up to date?");

		try {
			return new NodeResult<T>(node, cmd.doInCluster(client));
		} catch (RuntimeException ex) {

			RuntimeException translatedException = convertToDataAccessExeption(ex);
			if (translatedException instanceof ClusterRedirectException) {
				ClusterRedirectException cre = (ClusterRedirectException) translatedException;
				return executeCommandOnSingleNode(cmd,
						topologyProvider.getTopology().lookup(cre.getTargetHost(), cre.getTargetPort()), redirectCount + 1);
			} else {
				throw translatedException != null ? translatedException : ex;
			}
		} finally {
			this.resourceProvider.returnResourceForSpecificNode(nodeToUse, client);
		}
	}

	/**
	 * Lookup node from the topology.
	 *
	 * @param node
	 * @return
	 * @throws IllegalArgumentException in case the node could not be resolved to a topology-known node
	 */
	private RedisClusterNode lookupNode(RedisClusterNode node) {
		try {
			return topologyProvider.getTopology().lookup(node);
		} catch (ClusterStateFailureException e) {
			throw new IllegalArgumentException(String.format("Node %s is unknown to cluster", node), e);
		}
	}

	/**
	 * Run {@link ClusterCommandCallback} on all reachable master nodes.
	 *
	 * @param cmd
	 * @return
	 * @throws ClusterCommandExecutionFailureException
	 */
	public <S, T> MulitNodeResult<T> executeCommandOnAllNodes(final ClusterCommandCallback<S, T> cmd) {
		return executeCommandAsyncOnNodes(cmd, getClusterTopology().getActiveMasterNodes());
	}

	/**
	 * @param callback
	 * @param nodes
	 * @return
	 * @throws ClusterCommandExecutionFailureException
	 * @throws IllegalArgumentException in case the node could not be resolved to a topology-known node
	 */
	public <S, T> MulitNodeResult<T> executeCommandAsyncOnNodes(final ClusterCommandCallback<S, T> callback,
			Iterable<RedisClusterNode> nodes) {

		Assert.notNull(callback, "Callback must not be null!");
		Assert.notNull(nodes, "Nodes must not be null!");

		List<RedisClusterNode> resolvedRedisClusterNodes = new ArrayList<RedisClusterNode>();
		ClusterTopology topology = topologyProvider.getTopology();

		for (final RedisClusterNode node : nodes) {
			try {
				resolvedRedisClusterNodes.add(topology.lookup(node));
			} catch (ClusterStateFailureException e) {
				throw new IllegalArgumentException(String.format("Node %s is unknown to cluster", node), e);
			}
		}

		Map<NodeExecution, Future<NodeResult<T>>> futures = new LinkedHashMap<NodeExecution, Future<NodeResult<T>>>();
		for (final RedisClusterNode node : resolvedRedisClusterNodes) {

			futures.put(new NodeExecution(node), executor.submit(new Callable<NodeResult<T>>() {

				@Override
				public NodeResult<T> call() throws Exception {
					return executeCommandOnSingleNode(callback, node);
				}
			}));
		}

		return collectResults(futures);
	}

	private <T> MulitNodeResult<T> collectResults(Map<NodeExecution, Future<NodeResult<T>>> futures) {

		boolean done = false;

		MulitNodeResult<T> result = new MulitNodeResult<T>();
		Map<RedisClusterNode, Throwable> exceptions = new HashMap<RedisClusterNode, Throwable>();

		Set<String> saveGuard = new HashSet<String>();
		while (!done) {

			done = true;
			for (Map.Entry<NodeExecution, Future<NodeResult<T>>> entry : futures.entrySet()) {

				if (!entry.getValue().isDone() && !entry.getValue().isCancelled()) {
					done = false;
				} else {
					try {

						String futureId = ObjectUtils.getIdentityHexString(entry.getValue());
						if (!saveGuard.contains(futureId)) {
							result.add(entry.getValue().get());
							saveGuard.add(futureId);
						}
					} catch (ExecutionException e) {

						RuntimeException ex = convertToDataAccessExeption((Exception) e.getCause());
						exceptions.put(entry.getKey().getNode(), ex != null ? ex : e.getCause());
					} catch (InterruptedException e) {

						Thread.currentThread().interrupt();

						RuntimeException ex = convertToDataAccessExeption((Exception) e.getCause());
						exceptions.put(entry.getKey().getNode(), ex != null ? ex : e.getCause());
						break;
					}
				}
			}
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {

				done = true;
				Thread.currentThread().interrupt();
			}
		}

		if (!exceptions.isEmpty()) {
			throw new ClusterCommandExecutionFailureException(new ArrayList<Throwable>(exceptions.values()));
		}
		return result;
	}

	/**
	 * Run {@link MultiKeyClusterCommandCallback} with on a curated set of nodes serving one or more keys.
	 *
	 * @param cmd
	 * @return
	 * @throws ClusterCommandExecutionFailureException
	 */
	public <S, T> MulitNodeResult<T> executeMuliKeyCommand(final MultiKeyClusterCommandCallback<S, T> cmd,
			Iterable<byte[]> keys) {

		Map<RedisClusterNode, Set<byte[]>> nodeKeyMap = new HashMap<RedisClusterNode, Set<byte[]>>();

		for (byte[] key : keys) {
			for (RedisClusterNode node : getClusterTopology().getKeyServingNodes(key)) {

				if (nodeKeyMap.containsKey(node)) {
					nodeKeyMap.get(node).add(key);
				} else {
					Set<byte[]> keySet = new LinkedHashSet<byte[]>();
					keySet.add(key);
					nodeKeyMap.put(node, keySet);
				}
			}
		}

		Map<NodeExecution, Future<NodeResult<T>>> futures = new LinkedHashMap<NodeExecution, Future<NodeResult<T>>>();

		for (final Entry<RedisClusterNode, Set<byte[]>> entry : nodeKeyMap.entrySet()) {

			if (entry.getKey().isMaster()) {
				for (final byte[] key : entry.getValue()) {
					futures.put(new NodeExecution(entry.getKey(), key), executor.submit(new Callable<NodeResult<T>>() {

						@Override
						public NodeResult<T> call() throws Exception {
							return executeMultiKeyCommandOnSingleNode(cmd, entry.getKey(), key);
						}
					}));
				}
			}
		}

		return collectResults(futures);
	}

	private <S, T> NodeResult<T> executeMultiKeyCommandOnSingleNode(MultiKeyClusterCommandCallback<S, T> cmd,
			RedisClusterNode node, byte[] key) {

		Assert.notNull(cmd, "MultiKeyCommandCallback must not be null!");
		Assert.notNull(node, "RedisClusterNode must not be null!");
		Assert.notNull(key, "Keys for execution must not be null!");

		S client = this.resourceProvider.getResourceForSpecificNode(node);
		Assert.notNull(client, "Could not acquire resource for node. Is your cluster info up to date?");

		try {
			return new NodeResult<T>(node, cmd.doInCluster(client, key), key);
		} catch (RuntimeException ex) {

			RuntimeException translatedException = convertToDataAccessExeption(ex);
			throw translatedException != null ? translatedException : ex;
		} finally {
			this.resourceProvider.returnResourceForSpecificNode(node, client);
		}
	}

	private ClusterTopology getClusterTopology() {
		return this.topologyProvider.getTopology();
	}

	private DataAccessException convertToDataAccessExeption(Exception e) {
		return exceptionTranslationStrategy.translate(e);
	}

	/**
	 * Set the maximum number of redirects to follow on {@code MOVED} or {@code ASK}.
	 *
	 * @param maxRedirects set to zero to suspend redirects.
	 */
	public void setMaxRedirects(int maxRedirects) {
		this.maxRedirects = maxRedirects;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.DisposableBean#destroy()
	 */
	@Override
	public void destroy() throws Exception {

		if (executor instanceof DisposableBean) {
			((DisposableBean) executor).destroy();
		}

		if (resourceProvider instanceof DisposableBean) {
			((DisposableBean) resourceProvider).destroy();
		}
	}

	/**
	 * Callback interface for Redis 'low level' code using the cluster client directly. To be used with
	 * {@link ClusterCommandExecutor} execution methods.
	 *
	 * @author Christoph Strobl
	 * @param <T> native driver connection
	 * @param <S>
	 * @since 1.7
	 */
	public static interface ClusterCommandCallback<T, S> {
		S doInCluster(T client);
	}

	/**
	 * Callback interface for Redis 'low level' code using the cluster client to execute multi key commands.
	 *
	 * @author Christoph Strobl
	 * @param <T> native driver connection
	 * @param <S>
	 */
	public static interface MultiKeyClusterCommandCallback<T, S> {
		S doInCluster(T client, byte[] key);
	}

	/**
	 * {@link NodeExecution} encapsulates the execution of a command on a specific node along with arguments, such as
	 * keys, involved.
	 *
	 * @author Christoph Strobl
	 * @since 1.7
	 */
	private static class NodeExecution {

		private RedisClusterNode node;
		private Object[] args;

		public NodeExecution(RedisClusterNode node, Object... args) {

			this.node = node;
			this.args = args;
		}

		/**
		 * Get the {@link RedisClusterNode} the execution happens on.
		 */
		public RedisClusterNode getNode() {
			return node;
		}

		/*
		 * (non-Javadoc)
		 * @see java.lang.Object#hashCode()
		 */
		@Override
		public int hashCode() {

			int result = ObjectUtils.nullSafeHashCode(node);
			return result + ObjectUtils.nullSafeHashCode(args);
		}

		/*
		 * (non-Javadoc)
		 * @see java.lang.Object#equals(java.lang.Object)
		 */
		@Override
		public boolean equals(Object obj) {

			if (this == obj) {
				return true;
			}

			if (obj == null) {
				return false;
			}

			if (!(obj instanceof NodeExecution)) {
				return false;
			}

			NodeExecution that = (NodeExecution) obj;

			if (!ObjectUtils.nullSafeEquals(this.node, that.node)) {
				return false;
			}

			return ObjectUtils.nullSafeEquals(this.args, that.args);
		}

	}

	/**
	 * {@link NodeResult} encapsules the actual value returned by a {@link ClusterCommandCallback} on a given
	 * {@link RedisClusterNode}.
	 *
	 * @author Christoph Strobl
	 * @param <T>
	 * @since 1.7
	 */
	public static class NodeResult<T> {

		private RedisClusterNode node;
		private T value;
		private ByteArrayWrapper key;

		/**
		 * Create new {@link NodeResult}.
		 *
		 * @param node
		 * @param value
		 */
		public NodeResult(RedisClusterNode node, T value) {
			this(node, value, new byte[] {});
		}

		/**
		 * Create new {@link NodeResult}.
		 *
		 * @param node
		 * @param value
		 * @parm key
		 */
		public NodeResult(RedisClusterNode node, T value, byte[] key) {

			this.node = node;
			this.value = value;

			this.key = new ByteArrayWrapper(key);
		}

		/**
		 * Get the actual value of the command execution.
		 *
		 * @return can be {@literal null}.
		 */
		public T getValue() {
			return value;
		}

		/**
		 * Get the {@link RedisClusterNode} the command was executed on.
		 *
		 * @return never {@literal null}.
		 */
		public RedisClusterNode getNode() {
			return node;
		}

		/**
		 * @return
		 */
		public byte[] getKey() {
			return key.getArray();
		}
	}

	/**
	 * {@link MulitNodeResult} holds all {@link NodeResult} of a command executed on multiple {@link RedisClusterNode}.
	 *
	 * @author Christoph Strobl
	 * @param <T>
	 * @since 1.7
	 */
	public static class MulitNodeResult<T> {

		List<NodeResult<T>> nodeResults = new ArrayList<NodeResult<T>>();

		private void add(NodeResult<T> result) {
			nodeResults.add(result);
		}

		/**
		 * @return never {@literal null}.
		 */
		public List<NodeResult<T>> getResults() {
			return Collections.unmodifiableList(nodeResults);
		}

		/**
		 * Get {@link List} of all individual {@link NodeResult#value}. <br />
		 * The resulting {@link List} may contain {@literal null} values.
		 *
		 * @return never {@literal null}.
		 */
		public List<T> resultsAsList() {
			return toList(nodeResults);
		}

		/**
		 * Get {@link List} of all individual {@link NodeResult#value}. <br />
		 * The resulting {@link List} may contain {@literal null} values.
		 *
		 * @return never {@literal null}.
		 */
		public List<T> resultsAsListSortBy(byte[]... keys) {

			ArrayList<NodeResult<T>> clone = new ArrayList<NodeResult<T>>(nodeResults);
			Collections.sort(clone, new ResultByReferenceKeyPositionComperator(keys));

			return toList(clone);
		}

		/**
		 * @param returnValue
		 * @return
		 */
		public T getFirstNonNullNotEmptyOrDefault(T returnValue) {

			for (NodeResult<T> nodeResult : nodeResults) {
				if (nodeResult.getValue() != null) {
					if (nodeResult.getValue() instanceof Map) {
						if (CollectionUtils.isEmpty((Map<?, ?>) nodeResult.getValue())) {
							return nodeResult.getValue();
						}
					} else if (CollectionUtils.isEmpty((Collection<?>) nodeResult.getValue())) {
						return nodeResult.getValue();
					} else {
						return nodeResult.getValue();
					}
				}
			}

			return returnValue;
		}

		private List<T> toList(Collection<NodeResult<T>> source) {

			ArrayList<T> result = new ArrayList<T>();
			for (NodeResult<T> nodeResult : source) {
				result.add(nodeResult.getValue());
			}
			return result;
		}

		/**
		 * {@link Comparator} for sorting {@link NodeResult} by reference keys.
		 *
		 * @author Christoph Strobl
		 */
		private static class ResultByReferenceKeyPositionComperator implements Comparator<NodeResult<?>> {

			List<ByteArrayWrapper> reference;

			public ResultByReferenceKeyPositionComperator(byte[]... keys) {
				reference = new ArrayList<ByteArrayWrapper>(new ByteArraySet(Arrays.asList(keys)));
			}

			@Override
			public int compare(NodeResult<?> o1, NodeResult<?> o2) {
				return Integer.compare(reference.indexOf(o1.key), reference.indexOf(o2.key));
			}
		}
	}
}
