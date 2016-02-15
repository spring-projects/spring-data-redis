/*
 * Copyright 2015 the original author or authors.
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

import java.util.ArrayList;
import java.util.HashMap;
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
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.util.Assert;

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
	public <T> T executeCommandOnArbitraryNode(ClusterCommandCallback<?, T> cmd) {

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
	public <S, T> T executeCommandOnSingleNode(ClusterCommandCallback<S, T> cmd, RedisClusterNode node) {
		return executeCommandOnSingleNode(cmd, node, 0);
	}

	private <S, T> T executeCommandOnSingleNode(ClusterCommandCallback<S, T> cmd, RedisClusterNode node, int redirectCount) {

		Assert.notNull(cmd, "ClusterCommandCallback must not be null!");
		Assert.notNull(node, "RedisClusterNode must not be null!");

		if (redirectCount > maxRedirects) {
			throw new TooManyClusterRedirectionsException(
					String
							.format(
									"Cannot follow Cluster Redirects over more than %s legs. Please consider increasing the number of redirects to follow. Current value is: %s.",
									redirectCount, maxRedirects));
		}

		RedisClusterNode nodeToUse = lookupNode(node);

		S client = this.resourceProvider.getResourceForSpecificNode(nodeToUse);
		Assert.notNull(client, "Could not acquire resource for node. Is your cluster info up to date?");

		try {
			return cmd.doInCluster(client);
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
	 * @param node
	 * @return
	 * @throws IllegalArgumentException in case the node could not be resolved to a topology-known node
	 */
	private RedisClusterNode lookupNode(RedisClusterNode node) {
		try {
			return topologyProvider.getTopology().lookup(node);
		}catch (ClusterStateFailureException e){
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
	public <S, T> Map<RedisClusterNode, T> executeCommandOnAllNodes(final ClusterCommandCallback<S, T> cmd) {
		return executeCommandAsyncOnNodes(cmd, getClusterTopology().getActiveMasterNodes());
	}

	/**
	 * @param callback
	 * @param nodes
	 * @return
	 * @throws ClusterCommandExecutionFailureException
	 * @throws IllegalArgumentException in case the node could not be resolved to a topology-known node
	 */
	public <S, T> java.util.Map<RedisClusterNode, T> executeCommandAsyncOnNodes(
			final ClusterCommandCallback<S, T> callback, Iterable<RedisClusterNode> nodes) {

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

		Map<RedisClusterNode, Future<T>> futures = new LinkedHashMap<RedisClusterNode, Future<T>>();
		for (final RedisClusterNode node : resolvedRedisClusterNodes) {

			futures.put(node, executor.submit(new Callable<T>() {

				@Override
				public T call() throws Exception {
					return executeCommandOnSingleNode(callback, node);
				}
			}));
		}

		return collectResults(futures);
	}

	private <T> Map<RedisClusterNode, T> collectResults(Map<RedisClusterNode, Future<T>> futures) {

		boolean done = false;

		Map<RedisClusterNode, T> result = new HashMap<RedisClusterNode, T>();
		Map<RedisClusterNode, Throwable> exceptions = new HashMap<RedisClusterNode, Throwable>();
		while (!done) {

			done = true;
			for (Map.Entry<RedisClusterNode, Future<T>> entry : futures.entrySet()) {

				if (!entry.getValue().isDone() && !entry.getValue().isCancelled()) {
					done = false;
				} else {
					if (!result.containsKey(entry.getKey()) && !exceptions.containsKey(entry.getKey())) {
						try {
							result.put(entry.getKey(), entry.getValue().get());
						} catch (ExecutionException e) {

							RuntimeException ex = convertToDataAccessExeption((Exception) e.getCause());
							exceptions.put(entry.getKey(), ex != null ? ex : e.getCause());
						} catch (InterruptedException e) {

							Thread.currentThread().interrupt();

							RuntimeException ex = convertToDataAccessExeption((Exception) e.getCause());
							exceptions.put(entry.getKey(), ex != null ? ex : e.getCause());
							break;
						}
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
	public <S, T> Map<RedisClusterNode, T> executeMuliKeyCommand(final MultiKeyClusterCommandCallback<S, T> cmd,
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

		Map<RedisClusterNode, Future<T>> futures = new LinkedHashMap<RedisClusterNode, Future<T>>();
		for (final Entry<RedisClusterNode, Set<byte[]>> entry : nodeKeyMap.entrySet()) {

			if (entry.getKey().isMaster()) {
				for (final byte[] key : entry.getValue()) {
					futures.put(entry.getKey(), executor.submit(new Callable<T>() {

						@Override
						public T call() throws Exception {
							return (T) executeMultiKeyCommandOnSingleNode(cmd, entry.getKey(), key);
						}
					}));
				}
			}
		}
		return collectResults(futures);
	}

	private <S, T> T executeMultiKeyCommandOnSingleNode(MultiKeyClusterCommandCallback<S, T> cmd, RedisClusterNode node,
			byte[] key) {

		Assert.notNull(cmd, "MultiKeyCommandCallback must not be null!");
		Assert.notNull(node, "RedisClusterNode must not be null!");
		Assert.notNull(key, "Keys for execution must not be null!");

		S client = this.resourceProvider.getResourceForSpecificNode(node);
		Assert.notNull(client, "Could not acquire resource for node. Is your cluster info up to date?");

		try {
			return cmd.doInCluster(client, key);
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

}
