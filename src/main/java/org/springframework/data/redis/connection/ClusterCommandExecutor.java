/*
 * Copyright 2015-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.redis.connection;

import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.ClusterRedirectException;
import org.springframework.data.redis.ClusterStateFailureException;
import org.springframework.data.redis.ExceptionTranslationStrategy;
import org.springframework.data.redis.TooManyClusterRedirectionsException;
import org.springframework.data.redis.connection.util.ByteArraySet;
import org.springframework.data.redis.connection.util.ByteArrayWrapper;
import org.springframework.lang.Nullable;
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

		Assert.notNull(topologyProvider, "ClusterTopologyProvider must not be null!");
		Assert.notNull(resourceProvider, "ClusterNodeResourceProvider must not be null!");
		Assert.notNull(exceptionTranslation, "ExceptionTranslationStrategy must not be null!");

		this.topologyProvider = topologyProvider;
		this.resourceProvider = resourceProvider;
		this.exceptionTranslationStrategy = exceptionTranslation;
	}

	/**
	 * @param topologyProvider must not be {@literal null}.
	 * @param resourceProvider must not be {@literal null}.
	 * @param exceptionTranslation must not be {@literal null}.
	 * @param executor can be {@literal null}. Defaulted to {@link ThreadPoolTaskExecutor}.
	 */
	public ClusterCommandExecutor(ClusterTopologyProvider topologyProvider, ClusterNodeResourceProvider resourceProvider,
			ExceptionTranslationStrategy exceptionTranslation, @Nullable AsyncTaskExecutor executor) {

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
	 * @return never {@literal null}.
	 */
	public <T> NodeResult<T> executeCommandOnArbitraryNode(ClusterCommandCallback<?, T> cmd) {

		Assert.notNull(cmd, "ClusterCommandCallback must not be null!");
		List<RedisClusterNode> nodes = new ArrayList<>(getClusterTopology().getActiveNodes());
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
			return new NodeResult<>(node, cmd.doInCluster(client));
		} catch (RuntimeException ex) {

			RuntimeException translatedException = convertToDataAccessException(ex);
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
	 * @param node must not be {@literal null}.
	 * @return never {@literal null}.
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
	 * @param cmd must not be {@literal null}.
	 * @return never {@literal null}.
	 * @throws ClusterCommandExecutionFailureException
	 */
	public <S, T> MultiNodeResult<T> executeCommandOnAllNodes(final ClusterCommandCallback<S, T> cmd) {
		return executeCommandAsyncOnNodes(cmd, getClusterTopology().getActiveMasterNodes());
	}

	/**
	 * @param callback must not be {@literal null}.
	 * @param nodes must not be {@literal null}.
	 * @return never {@literal null}.
	 * @throws ClusterCommandExecutionFailureException
	 * @throws IllegalArgumentException in case the node could not be resolved to a topology-known node
	 */
	public <S, T> MultiNodeResult<T> executeCommandAsyncOnNodes(ClusterCommandCallback<S, T> callback,
			Iterable<RedisClusterNode> nodes) {

		Assert.notNull(callback, "Callback must not be null!");
		Assert.notNull(nodes, "Nodes must not be null!");

		List<RedisClusterNode> resolvedRedisClusterNodes = new ArrayList<>();
		ClusterTopology topology = topologyProvider.getTopology();

		for (RedisClusterNode node : nodes) {
			try {
				resolvedRedisClusterNodes.add(topology.lookup(node));
			} catch (ClusterStateFailureException e) {
				throw new IllegalArgumentException(String.format("Node %s is unknown to cluster", node), e);
			}
		}

		Map<NodeExecution, Future<NodeResult<T>>> futures = new LinkedHashMap<>();
		for (RedisClusterNode node : resolvedRedisClusterNodes) {
			futures.put(new NodeExecution(node), executor.submit(() -> executeCommandOnSingleNode(callback, node)));
		}

		return collectResults(futures);
	}

	private <T> MultiNodeResult<T> collectResults(Map<NodeExecution, Future<NodeResult<T>>> futures) {

		boolean done = false;

		MultiNodeResult<T> result = new MultiNodeResult<>();
		Map<RedisClusterNode, Throwable> exceptions = new HashMap<>();

		Set<String> saveGuard = new HashSet<>();
		while (!done) {

			done = true;
			for (Map.Entry<NodeExecution, Future<NodeResult<T>>> entry : futures.entrySet()) {

				if (!entry.getValue().isDone() && !entry.getValue().isCancelled()) {
					done = false;
				} else {

					NodeExecution execution = entry.getKey();
					try {

						String futureId = ObjectUtils.getIdentityHexString(entry.getValue());
						if (!saveGuard.contains(futureId)) {

							if (execution.isPositional()) {
								result.add(execution.getPositionalKey(), entry.getValue().get());
							} else {
								result.add(entry.getValue().get());
							}
							saveGuard.add(futureId);
						}
					} catch (ExecutionException e) {

						RuntimeException ex = convertToDataAccessException((Exception) e.getCause());
						exceptions.put(execution.getNode(), ex != null ? ex : e.getCause());
					} catch (InterruptedException e) {

						Thread.currentThread().interrupt();

						RuntimeException ex = convertToDataAccessException((Exception) e.getCause());
						exceptions.put(execution.getNode(), ex != null ? ex : e.getCause());
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
			throw new ClusterCommandExecutionFailureException(new ArrayList<>(exceptions.values()));
		}
		return result;
	}

	/**
	 * Run {@link MultiKeyClusterCommandCallback} with on a curated set of nodes serving one or more keys.
	 *
	 * @param cmd must not be {@literal null}.
	 * @return never {@literal null}.
	 * @throws ClusterCommandExecutionFailureException
	 */
	public <S, T> MultiNodeResult<T> executeMultiKeyCommand(MultiKeyClusterCommandCallback<S, T> cmd,
			Iterable<byte[]> keys) {

		Map<RedisClusterNode, PositionalKeys> nodeKeyMap = new HashMap<>();

		int index = 0;
		for (byte[] key : keys) {
			for (RedisClusterNode node : getClusterTopology().getKeyServingNodes(key)) {
				nodeKeyMap.computeIfAbsent(node, val -> PositionalKeys.empty()).append(PositionalKey.of(key, index++));
			}
		}

		Map<NodeExecution, Future<NodeResult<T>>> futures = new LinkedHashMap<>();
		for (Entry<RedisClusterNode, PositionalKeys> entry : nodeKeyMap.entrySet()) {

			if (entry.getKey().isMaster()) {
				for (PositionalKey key : entry.getValue()) {
					futures.put(new NodeExecution(entry.getKey(), key),
							executor.submit(() -> executeMultiKeyCommandOnSingleNode(cmd, entry.getKey(), key.getBytes())));
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
			return new NodeResult<>(node, cmd.doInCluster(client, key), key);
		} catch (RuntimeException ex) {

			RuntimeException translatedException = convertToDataAccessException(ex);
			throw translatedException != null ? translatedException : ex;
		} finally {
			this.resourceProvider.returnResourceForSpecificNode(node, client);
		}
	}

	private ClusterTopology getClusterTopology() {
		return this.topologyProvider.getTopology();
	}

	@Nullable
	private DataAccessException convertToDataAccessException(Exception e) {
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
	public interface ClusterCommandCallback<T, S> {
		S doInCluster(T client);
	}

	/**
	 * Callback interface for Redis 'low level' code using the cluster client to execute multi key commands.
	 *
	 * @author Christoph Strobl
	 * @param <T> native driver connection
	 * @param <S>
	 */
	public interface MultiKeyClusterCommandCallback<T, S> {
		S doInCluster(T client, byte[] key);
	}

	/**
	 * {@link NodeExecution} encapsulates the execution of a command on a specific node along with arguments, such as
	 * keys, involved.
	 *
	 * @author Christoph Strobl
	 * @author Mark Paluch
	 * @since 1.7
	 */
	private static class NodeExecution {

		private final RedisClusterNode node;
		private final @Nullable PositionalKey positionalKey;

		NodeExecution(RedisClusterNode node) {
			this(node, null);
		}

		NodeExecution(RedisClusterNode node, @Nullable PositionalKey positionalKey) {

			this.node = node;
			this.positionalKey = positionalKey;
		}

		/**
		 * Get the {@link RedisClusterNode} the execution happens on.
		 */
		RedisClusterNode getNode() {
			return node;
		}

		/**
		 * Get the {@link PositionalKey} of this execution.
		 *
		 * @since 2.0.3
		 */
		PositionalKey getPositionalKey() {
			return positionalKey;
		}

		boolean isPositional() {
			return positionalKey != null;
		}
	}

	/**
	 * {@link NodeResult} encapsulates the actual value returned by a {@link ClusterCommandCallback} on a given
	 * {@link RedisClusterNode}.
	 *
	 * @author Christoph Strobl
	 * @param <T>
	 * @since 1.7
	 */
	public static class NodeResult<T> {

		private RedisClusterNode node;
		private @Nullable T value;
		private ByteArrayWrapper key;

		/**
		 * Create new {@link NodeResult}.
		 *
		 * @param node must not be {@literal null}.
		 * @param value can be {@literal null}.
		 */
		public NodeResult(RedisClusterNode node, @Nullable T value) {
			this(node, value, new byte[] {});
		}

		/**
		 * Create new {@link NodeResult}.
		 *
		 * @param node must not be {@literal null}.
		 * @param value can be {@literal null}.
		 * @param key must not be {@literal null}.
		 */
		public NodeResult(RedisClusterNode node, @Nullable T value, byte[] key) {

			this.node = node;
			this.value = value;

			this.key = new ByteArrayWrapper(key);
		}

		/**
		 * Get the actual value of the command execution.
		 *
		 * @return can be {@literal null}.
		 */
		@Nullable
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

		/**
		 * Apply the {@link Function mapper function} to the value and return the mapped value.
		 *
		 * @param mapper must not be {@literal null}.
		 * @param <U> type of the mapped value.
		 * @return the mapped value.
		 * @since 2.1
		 */
		@Nullable
		public <U> U mapValue(Function<? super T, ? extends U> mapper) {

			Assert.notNull(mapper, "Mapper function must not be null!");

			return mapper.apply(getValue());
		}
	}

	/**
	 * {@link MultiNodeResult} holds all {@link NodeResult} of a command executed on multiple {@link RedisClusterNode}.
	 *
	 * @author Christoph Strobl
	 * @author Mark Paluch
	 * @param <T>
	 * @since 1.7
	 */
	public static class MultiNodeResult<T> {

		List<NodeResult<T>> nodeResults = new ArrayList<>();
		Map<PositionalKey, NodeResult<T>> positionalResults = new LinkedHashMap<>();

		private void add(NodeResult<T> result) {
			nodeResults.add(result);
		}

		private void add(PositionalKey key, NodeResult<T> result) {

			positionalResults.put(key, result);
			add(result);
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

			if (positionalResults.isEmpty()) {

				List<NodeResult<T>> clone = new ArrayList<>(nodeResults);
				clone.sort(new ResultByReferenceKeyPositionComparator(keys));

				return toList(clone);
			}

			Map<PositionalKey, NodeResult<T>> result = new TreeMap<>(new ResultByKeyPositionComparator(keys));
			result.putAll(positionalResults);

			return result.values().stream().map(tNodeResult -> tNodeResult.value).collect(Collectors.toList());
		}

		/**
		 * @param returnValue can be {@literal null}.
		 * @return can be {@literal null}.
		 */
		@Nullable
		public T getFirstNonNullNotEmptyOrDefault(@Nullable T returnValue) {

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

			ArrayList<T> result = new ArrayList<>();
			for (NodeResult<T> nodeResult : source) {
				result.add(nodeResult.getValue());
			}
			return result;
		}

		/**
		 * {@link Comparator} for sorting {@link NodeResult} by reference keys.
		 *
		 * @author Christoph Strobl
		 * @author Mark Paluch
		 */
		private static class ResultByReferenceKeyPositionComparator implements Comparator<NodeResult<?>> {

			private final List<ByteArrayWrapper> reference;

			ResultByReferenceKeyPositionComparator(byte[]... keys) {
				reference = new ArrayList<>(new ByteArraySet(Arrays.asList(keys)));
			}

			@Override
			public int compare(NodeResult<?> o1, NodeResult<?> o2) {
				return Integer.compare(reference.indexOf(o1.key), reference.indexOf(o2.key));
			}
		}

		/**
		 * {@link Comparator} for sorting {@link PositionalKey} by external {@link PositionalKeys}.
		 *
		 * @author Mark Paluch
		 * @author Christoph Strobl
		 * @since 2.0.3
		 */
		private static class ResultByKeyPositionComparator implements Comparator<PositionalKey> {

			private final PositionalKeys reference;

			ResultByKeyPositionComparator(byte[]... keys) {
				reference = PositionalKeys.of(keys);
			}

			@Override
			public int compare(PositionalKey o1, PositionalKey o2) {
				return Integer.compare(reference.indexOf(o1), reference.indexOf(o2));
			}
		}
	}

	/**
	 * Value object representing a Redis key at a particular command position.
	 *
	 * @author Mark Paluch
	 * @author Christoph Strobl
	 * @since 2.0.3
	 */
	@Getter
	@EqualsAndHashCode
	@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
	private static class PositionalKey {

		private final ByteArrayWrapper key;
		private final int position;

		static PositionalKey of(byte[] key, int index) {
			return new PositionalKey(new ByteArrayWrapper(key), index);
		}

		/**
		 * @return binary key.
		 */
		byte[] getBytes() {
			return key.getArray();
		}
	}

	/**
	 * Mutable data structure to represent multiple {@link PositionalKey}s.
	 *
	 * @author Mark Paluch
	 * @author Christoph Strobl
	 * @since 2.0.3
	 */
	@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
	private static class PositionalKeys implements Iterable<PositionalKey> {

		private final List<PositionalKey> keys;

		/**
		 * Create an empty {@link PositionalKeys}.
		 */
		static PositionalKeys empty() {
			return new PositionalKeys(new ArrayList<>());
		}

		/**
		 * Create an {@link PositionalKeys} from {@code keys}.
		 */
		static PositionalKeys of(byte[]... keys) {

			List<PositionalKey> result = new ArrayList<>(keys.length);

			for (int i = 0; i < keys.length; i++) {
				result.add(PositionalKey.of(keys[i], i));
			}

			return new PositionalKeys(result);
		}

		/**
		 * Create an {@link PositionalKeys} from {@link PositionalKey}s.
		 */
		static PositionalKeys of(PositionalKey... keys) {

			PositionalKeys result = PositionalKeys.empty();
			result.append(keys);

			return result;
		}

		/**
		 * Append {@link PositionalKey}s to this object.
		 */
		void append(PositionalKey... keys) {
			this.keys.addAll(Arrays.asList(keys));
		}

		/**
		 * @return index of the {@link PositionalKey}.
		 */
		int indexOf(PositionalKey key) {
			return keys.indexOf(key);
		}

		@Override
		public Iterator<PositionalKey> iterator() {
			return keys.iterator();
		}
	}
}
