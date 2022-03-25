/*
 * Copyright 2015-2022 the original author or authors.
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
package org.springframework.data.redis.connection.jedis;

import redis.clients.jedis.Connection;
import redis.clients.jedis.ConnectionPool;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.providers.ClusterConnectionProvider;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.beans.DirectFieldAccessor;
import org.springframework.beans.PropertyAccessor;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.ClusterStateFailureException;
import org.springframework.data.redis.ExceptionTranslationStrategy;
import org.springframework.data.redis.FallbackExceptionTranslationStrategy;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.*;
import org.springframework.data.redis.connection.ClusterCommandExecutor.ClusterCommandCallback;
import org.springframework.data.redis.connection.ClusterCommandExecutor.MultiKeyClusterCommandCallback;
import org.springframework.data.redis.connection.ClusterCommandExecutor.NodeResult;
import org.springframework.data.redis.connection.RedisClusterNode.SlotRange;
import org.springframework.data.redis.connection.convert.Converters;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.util.DirectFieldAccessFallbackBeanWrapper;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * {@link RedisClusterConnection} implementation on top of {@link JedisCluster}.<br/>
 * Uses the native {@link JedisCluster} api where possible and falls back to direct node communication using
 * {@link Jedis} where needed.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Ninad Divadkar
 * @author Tao Chen
 * @author Chen Guanqun
 * @author Pavel Khokhlov
 * @author Liming Deng
 * @since 1.7
 */
public class JedisClusterConnection implements RedisClusterConnection {

	private static final ExceptionTranslationStrategy EXCEPTION_TRANSLATION = new FallbackExceptionTranslationStrategy(
			JedisExceptionConverter.INSTANCE);

	private final Log log = LogFactory.getLog(getClass());

	private final JedisCluster cluster;
	private final JedisClusterGeoCommands geoCommands = new JedisClusterGeoCommands(this);
	private final JedisClusterHashCommands hashCommands = new JedisClusterHashCommands(this);
	private final JedisClusterHyperLogLogCommands hllCommands = new JedisClusterHyperLogLogCommands(this);
	private final JedisClusterKeyCommands keyCommands = new JedisClusterKeyCommands(this);
	private final JedisClusterListCommands listCommands = new JedisClusterListCommands(this);
	private final JedisClusterSetCommands setCommands = new JedisClusterSetCommands(this);
	private final JedisClusterServerCommands serverCommands = new JedisClusterServerCommands(this);
	private final JedisClusterStreamCommands streamCommands = new JedisClusterStreamCommands(this);
	private final JedisClusterStringCommands stringCommands = new JedisClusterStringCommands(this);
	private final JedisClusterZSetCommands zSetCommands = new JedisClusterZSetCommands(this);

	private boolean closed;

	private final ClusterTopologyProvider topologyProvider;
	private final ClusterCommandExecutor clusterCommandExecutor;
	private final boolean disposeClusterCommandExecutorOnClose;

	private volatile @Nullable JedisSubscription subscription;

	/**
	 * Create new {@link JedisClusterConnection} utilizing native connections via {@link JedisCluster}.
	 *
	 * @param cluster must not be {@literal null}.
	 */
	public JedisClusterConnection(JedisCluster cluster) {

		Assert.notNull(cluster, "JedisCluster must not be null.");

		this.cluster = cluster;

		closed = false;
		topologyProvider = new JedisClusterTopologyProvider(cluster);
		clusterCommandExecutor = new ClusterCommandExecutor(topologyProvider,
				new JedisClusterNodeResourceProvider(cluster, topologyProvider), EXCEPTION_TRANSLATION);
		disposeClusterCommandExecutorOnClose = true;

		try {

			DirectFieldAccessor executorDfa = new DirectFieldAccessor(cluster);
			Object custerCommandExecutor = executorDfa.getPropertyValue("executor");
			DirectFieldAccessor dfa = new DirectFieldAccessor(custerCommandExecutor);
			clusterCommandExecutor.setMaxRedirects((Integer) dfa.getPropertyValue("maxRedirects"));
		} catch (Exception e) {
			// ignore it and work with the executor default
		}
	}

	/**
	 * Create new {@link JedisClusterConnection} utilizing native connections via {@link JedisCluster} running commands
	 * across the cluster via given {@link ClusterCommandExecutor}. Uses {@link JedisClusterTopologyProvider} by default.
	 *
	 * @param cluster must not be {@literal null}.
	 * @param executor must not be {@literal null}.
	 */
	public JedisClusterConnection(JedisCluster cluster, ClusterCommandExecutor executor) {
		this(cluster, executor, new JedisClusterTopologyProvider(cluster));
	}

	/**
	 * Create new {@link JedisClusterConnection} utilizing native connections via {@link JedisCluster} running commands
	 * across the cluster via given {@link ClusterCommandExecutor} and using the given {@link ClusterTopologyProvider}.
	 *
	 * @param cluster must not be {@literal null}.
	 * @param executor must not be {@literal null}.
	 * @param topologyProvider must not be {@literal null}.
	 * @since 2.2
	 */
	public JedisClusterConnection(JedisCluster cluster, ClusterCommandExecutor executor,
			ClusterTopologyProvider topologyProvider) {

		Assert.notNull(cluster, "JedisCluster must not be null.");
		Assert.notNull(executor, "ClusterCommandExecutor must not be null.");
		Assert.notNull(topologyProvider, "ClusterTopologyProvider must not be null.");

		this.closed = false;
		this.cluster = cluster;
		this.topologyProvider = topologyProvider;
		this.clusterCommandExecutor = executor;
		this.disposeClusterCommandExecutorOnClose = false;
	}

	@Nullable
	@Override
	public Object execute(String command, byte[]... args) {

		Assert.notNull(command, "Command must not be null!");
		Assert.notNull(args, "Args must not be null!");

		return clusterCommandExecutor.executeCommandOnArbitraryNode(
				(JedisClusterCommandCallback<Object>) client -> client.sendCommand(JedisClientUtils.getCommand(command), args))
				.getValue();
	}

	@Nullable
	@Override
	public <T> T execute(String command, byte[] key, Collection<byte[]> args) {

		Assert.notNull(command, "Command must not be null!");
		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(args, "Args must not be null!");

		byte[][] commandArgs = getCommandArguments(key, args);

		RedisClusterNode keyMaster = topologyProvider.getTopology().getKeyServingMasterNode(key);

		return clusterCommandExecutor.executeCommandOnSingleNode((JedisClusterCommandCallback<T>) client -> {
			return (T) client.sendCommand(JedisClientUtils.getCommand(command), commandArgs);
		}, keyMaster).getValue();
	}

	private static byte[][] getCommandArguments(byte[] key, Collection<byte[]> args) {

		byte[][] commandArgs = new byte[args.size() + 1][];

		commandArgs[0] = key;
		int targetIndex = 1;

		for (byte[] binaryArgument : args) {
			commandArgs[targetIndex++] = binaryArgument;
		}

		return commandArgs;
	}

	/**
	 * Execute the given command for each key in {@code keys} provided appending all {@code args} on each invocation.
	 * <br />
	 * This method, other than {@link #execute(String, byte[]...)}, dispatches the command to the {@code key} serving
	 * master node and appends the {@code key} as first command argument to the {@code command}. {@code keys} are not
	 * required to share the same slot for single-key commands. Multi-key commands carrying their keys in {@code args}
	 * still require to share the same slot as the {@code key}.
	 *
	 * <pre>
	 * <code>
	 * // SET foo bar EX 10 NX
	 * execute("SET", "foo".getBytes(), asBinaryList("bar", "EX", 10, "NX"))
	 * </code>
	 * </pre>
	 *
	 * @param command must not be {@literal null}.
	 * @param keys must not be {@literal null}.
	 * @param args must not be {@literal null}.
	 * @return command result as delivered by the underlying Redis driver. Can be {@literal null}.
	 * @since 2.1
	 */
	@Nullable
	public <T> List<T> execute(String command, Collection<byte[]> keys, Collection<byte[]> args) {

		Assert.notNull(command, "Command must not be null!");
		Assert.notNull(keys, "Key must not be null!");
		Assert.notNull(args, "Args must not be null!");

		return clusterCommandExecutor.executeMultiKeyCommand((JedisMultiKeyClusterCommandCallback<T>) (client, key) -> {
			return (T) client.sendCommand(JedisClientUtils.getCommand(command), getCommandArguments(key, args));
		}, keys).resultsAsList();

	}

	@Override
	public RedisCommands commands() {
		return this;
	}

	@Override
	public RedisClusterCommands clusterCommands() {
		return this;
	}

	@Override
	public RedisGeoCommands geoCommands() {
		return geoCommands;
	}

	@Override
	public RedisHashCommands hashCommands() {
		return hashCommands;
	}

	@Override
	public RedisHyperLogLogCommands hyperLogLogCommands() {
		return hllCommands;
	}

	@Override
	public RedisKeyCommands keyCommands() {
		return keyCommands;
	}

	@Override
	public RedisListCommands listCommands() {
		return listCommands;
	}

	@Override
	public RedisSetCommands setCommands() {
		return setCommands;
	}

	@Override
	public RedisClusterServerCommands serverCommands() {
		return serverCommands;
	}

	@Override
	public RedisStreamCommands streamCommands() {
		return streamCommands;
	}

	@Override
	public RedisStringCommands stringCommands() {
		return stringCommands;
	}

	@Override
	public RedisZSetCommands zSetCommands() {
		return zSetCommands;
	}

	@Override
	public RedisScriptingCommands scriptingCommands() {
		return new JedisClusterScriptingCommands(this);
	}

	@Override
	public Set<byte[]> keys(RedisClusterNode node, byte[] pattern) {
		return keyCommands.keys(node, pattern);
	}

	@Override
	public Cursor<byte[]> scan(RedisClusterNode node, ScanOptions options) {
		return keyCommands.scan(node, options);
	}

	@Override
	public byte[] randomKey(RedisClusterNode node) {
		return keyCommands.randomKey(node);
	}

	@Override
	public void multi() {
		throw new InvalidDataAccessApiUsageException("MULTI is currently not supported in cluster mode.");
	}

	@Override
	public List<Object> exec() {
		throw new InvalidDataAccessApiUsageException("EXEC is currently not supported in cluster mode.");
	}

	@Override
	public void discard() {
		throw new InvalidDataAccessApiUsageException("DISCARD is currently not supported in cluster mode.");
	}

	@Override
	public void watch(byte[]... keys) {
		throw new InvalidDataAccessApiUsageException("WATCH is currently not supported in cluster mode.");
	}

	@Override
	public void unwatch() {
		throw new InvalidDataAccessApiUsageException("UNWATCH is currently not supported in cluster mode.");
	}

	@Override
	public boolean isSubscribed() {
		return (subscription != null && subscription.isAlive());
	}

	@Override
	public Subscription getSubscription() {
		return subscription;
	}

	@Override
	public Long publish(byte[] channel, byte[] message) {
		try {
			return cluster.publish(channel, message);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public void subscribe(MessageListener listener, byte[]... channels) {

		if (isSubscribed()) {
			throw new RedisSubscribedConnectionException(
					"Connection already subscribed; use the connection Subscription to cancel or add new channels");
		}
		try {
			JedisMessageListener jedisPubSub = new JedisMessageListener(listener);
			subscription = new JedisSubscription(listener, jedisPubSub, channels, null);
			cluster.subscribe(jedisPubSub, channels);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public void pSubscribe(MessageListener listener, byte[]... patterns) {

		if (isSubscribed()) {
			throw new RedisSubscribedConnectionException(
					"Connection already subscribed; use the connection Subscription to cancel or add new channels");
		}
		try {
			JedisMessageListener jedisPubSub = new JedisMessageListener(listener);
			subscription = new JedisSubscription(listener, jedisPubSub, null, patterns);
			cluster.psubscribe(jedisPubSub, patterns);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public void select(int dbIndex) {

		if (dbIndex != 0) {
			throw new InvalidDataAccessApiUsageException("Cannot SELECT non zero index in cluster mode.");
		}
	}

	@Override
	public byte[] echo(byte[] message) {
		throw new InvalidDataAccessApiUsageException("Echo not supported in cluster mode.");
	}

	@Override
	public String ping() {

		return !clusterCommandExecutor.executeCommandOnAllNodes((JedisClusterCommandCallback<String>) Jedis::ping)
				.resultsAsList().isEmpty() ? "PONG" : null;

	}

	@Override
	public String ping(RedisClusterNode node) {

		return clusterCommandExecutor.executeCommandOnSingleNode((JedisClusterCommandCallback<String>) Jedis::ping, node)
				.getValue();
	}

	/*
	 * --> Cluster Commands
	 */

	@Override
	public void clusterSetSlot(RedisClusterNode node, int slot, AddSlots mode) {

		Assert.notNull(node, "Node must not be null.");
		Assert.notNull(mode, "AddSlots mode must not be null.");

		RedisClusterNode nodeToUse = topologyProvider.getTopology().lookup(node);
		String nodeId = nodeToUse.getId();

		clusterCommandExecutor.executeCommandOnSingleNode((JedisClusterCommandCallback<String>) client -> {

			switch (mode) {
				case IMPORTING:
					return client.clusterSetSlotImporting(slot, nodeId);
				case MIGRATING:
					return client.clusterSetSlotMigrating(slot, nodeId);
				case STABLE:
					return client.clusterSetSlotStable(slot);
				case NODE:
					return client.clusterSetSlotNode(slot, nodeId);
			}

			throw new IllegalArgumentException(String.format("Unknown AddSlots mode '%s'.", mode));
		}, node);

	}

	@Override
	public List<byte[]> clusterGetKeysInSlot(int slot, Integer count) {

		RedisClusterNode node = clusterGetNodeForSlot(slot);

		NodeResult<List<byte[]>> result = clusterCommandExecutor
				.executeCommandOnSingleNode(
						(JedisClusterCommandCallback<List<byte[]>>) client -> JedisConverters.stringListToByteList()
								.convert(client.clusterGetKeysInSlot(slot, count != null ? count.intValue() : Integer.MAX_VALUE)),
						node);

		return result.getValue();
	}

	@Override
	public void clusterAddSlots(RedisClusterNode node, int... slots) {

		clusterCommandExecutor.executeCommandOnSingleNode(
				(JedisClusterCommandCallback<String>) client -> client.clusterAddSlots(slots), node);
	}

	@Override
	public void clusterAddSlots(RedisClusterNode node, SlotRange range) {

		Assert.notNull(range, "Range must not be null.");

		clusterAddSlots(node, range.getSlotsArray());
	}

	@Override
	public Long clusterCountKeysInSlot(int slot) {

		RedisClusterNode node = clusterGetNodeForSlot(slot);

		return clusterCommandExecutor.executeCommandOnSingleNode(
				(JedisClusterCommandCallback<Long>) client -> client.clusterCountKeysInSlot(slot), node).getValue();
	}

	@Override
	public void clusterDeleteSlots(RedisClusterNode node, int... slots) {

		clusterCommandExecutor.executeCommandOnSingleNode(
				(JedisClusterCommandCallback<String>) client -> client.clusterDelSlots(slots), node);

	}

	@Override
	public void clusterDeleteSlotsInRange(RedisClusterNode node, SlotRange range) {

		Assert.notNull(range, "Range must not be null.");

		clusterDeleteSlots(node, range.getSlotsArray());
	}

	@Override
	public void clusterForget(RedisClusterNode node) {

		Set<RedisClusterNode> nodes = new LinkedHashSet<>(topologyProvider.getTopology().getActiveMasterNodes());
		RedisClusterNode nodeToRemove = topologyProvider.getTopology().lookup(node);
		nodes.remove(nodeToRemove);

		clusterCommandExecutor.executeCommandAsyncOnNodes(
				(JedisClusterCommandCallback<String>) client -> client.clusterForget(node.getId()), nodes);
	}

	@Override
	public void clusterMeet(RedisClusterNode node) {

		Assert.notNull(node, "Cluster node must not be null for CLUSTER MEET command!");
		Assert.hasText(node.getHost(), "Node to meet cluster must have a host!");
		Assert.isTrue(node.getPort() > 0, "Node to meet cluster must have a port greater 0!");

		clusterCommandExecutor.executeCommandOnAllNodes(
				(JedisClusterCommandCallback<String>) client -> client.clusterMeet(node.getHost(), node.getPort()));
	}

	@Override
	public void clusterReplicate(RedisClusterNode master, RedisClusterNode replica) {

		RedisClusterNode masterNode = topologyProvider.getTopology().lookup(master);

		clusterCommandExecutor.executeCommandOnSingleNode(
				(JedisClusterCommandCallback<String>) client -> client.clusterReplicate(masterNode.getId()), replica);

	}

	@Override
	public Integer clusterGetSlotForKey(byte[] key) {

		return clusterCommandExecutor
				.executeCommandOnArbitraryNode(
						(JedisClusterCommandCallback<Integer>) client -> (int) client.clusterKeySlot(JedisConverters.toString(key)))
				.getValue();
	}

	@Override
	public RedisClusterNode clusterGetNodeForKey(byte[] key) {
		return topologyProvider.getTopology().getKeyServingMasterNode(key);
	}

	@Override
	public RedisClusterNode clusterGetNodeForSlot(int slot) {

		for (RedisClusterNode node : topologyProvider.getTopology().getSlotServingNodes(slot)) {
			if (node.isMaster()) {
				return node;
			}
		}

		return null;
	}

	@Override
	public Set<RedisClusterNode> clusterGetNodes() {
		return topologyProvider.getTopology().getNodes();
	}

	@Override
	public Set<RedisClusterNode> clusterGetReplicas(RedisClusterNode master) {

		Assert.notNull(master, "Master cannot be null!");

		RedisClusterNode nodeToUse = topologyProvider.getTopology().lookup(master);

		return JedisConverters.toSetOfRedisClusterNodes(clusterCommandExecutor
				.executeCommandOnSingleNode(
						(JedisClusterCommandCallback<List<String>>) client -> client.clusterSlaves(nodeToUse.getId()), master)
				.getValue());
	}

	@Override
	public Map<RedisClusterNode, Collection<RedisClusterNode>> clusterGetMasterReplicaMap() {

		List<NodeResult<Collection<RedisClusterNode>>> nodeResults = clusterCommandExecutor.executeCommandAsyncOnNodes(
				(JedisClusterCommandCallback<Collection<RedisClusterNode>>) client -> JedisConverters
						.toSetOfRedisClusterNodes(client.clusterSlaves(client.clusterMyId())),
				topologyProvider.getTopology().getActiveMasterNodes()).getResults();

		Map<RedisClusterNode, Collection<RedisClusterNode>> result = new LinkedHashMap<>();

		for (NodeResult<Collection<RedisClusterNode>> nodeResult : nodeResults) {
			result.put(nodeResult.getNode(), nodeResult.getValue());
		}

		return result;
	}

	@Override
	public ClusterInfo clusterGetClusterInfo() {

		return new ClusterInfo(JedisConverters.toProperties(clusterCommandExecutor
				.executeCommandOnArbitraryNode((JedisClusterCommandCallback<String>) Jedis::clusterInfo).getValue()));
	}

	/*
	 * --> Little helpers to make it work
	 */

	protected DataAccessException convertJedisAccessException(Exception ex) {

		DataAccessException translated = EXCEPTION_TRANSLATION.translate(ex);

		return translated != null ? translated : new RedisSystemException(ex.getMessage(), ex);
	}

	@Override
	public void close() throws DataAccessException {

		if (!closed && disposeClusterCommandExecutorOnClose) {
			try {
				clusterCommandExecutor.destroy();
			} catch (Exception ex) {
				log.warn("Cannot properly close cluster command executor", ex);
			}
		}

		closed = true;
	}

	@Override
	public boolean isClosed() {
		return closed;
	}

	@Override
	public JedisCluster getNativeConnection() {
		return cluster;
	}

	@Override
	public boolean isQueueing() {
		return false;
	}

	@Override
	public boolean isPipelined() {
		return false;
	}

	@Override
	public void openPipeline() {
		throw new InvalidDataAccessApiUsageException("Pipeline is not supported for JedisClusterConnection.");
	}

	@Override
	public List<Object> closePipeline() throws RedisPipelineException {
		throw new InvalidDataAccessApiUsageException("Pipeline is not supported for JedisClusterConnection.");
	}

	@Override
	public RedisSentinelConnection getSentinelConnection() {
		throw new InvalidDataAccessApiUsageException("Sentinel is not supported for JedisClusterConnection.");
	}

	@Override
	public void rewriteConfig() {
		serverCommands().rewriteConfig();
	}

	/**
	 * {@link Jedis} specific {@link ClusterCommandCallback}.
	 *
	 * @author Christoph Strobl
	 * @param <T>
	 * @since 1.7
	 */
	protected interface JedisClusterCommandCallback<T> extends ClusterCommandCallback<Jedis, T> {}

	/**
	 * {@link Jedis} specific {@link MultiKeyClusterCommandCallback}.
	 *
	 * @author Christoph Strobl
	 * @param <T>
	 * @since 1.7
	 */
	protected interface JedisMultiKeyClusterCommandCallback<T> extends MultiKeyClusterCommandCallback<Jedis, T> {}

	/**
	 * Jedis specific implementation of {@link ClusterNodeResourceProvider}.
	 *
	 * @author Christoph Strobl
	 * @author Mark Paluch
	 * @since 1.7
	 */
	static class JedisClusterNodeResourceProvider implements ClusterNodeResourceProvider {

		private final JedisCluster cluster;
		private final ClusterTopologyProvider topologyProvider;
		private final ClusterConnectionProvider connectionHandler;

		/**
		 * Creates new {@link JedisClusterNodeResourceProvider}.
		 *
		 * @param cluster must not be {@literal null}.
		 * @param topologyProvider must not be {@literal null}.
		 */
		JedisClusterNodeResourceProvider(JedisCluster cluster, ClusterTopologyProvider topologyProvider) {

			this.cluster = cluster;
			this.topologyProvider = topologyProvider;

			if (cluster != null) {

				PropertyAccessor accessor = new DirectFieldAccessFallbackBeanWrapper(cluster);
				this.connectionHandler = accessor.isReadableProperty("connectionHandler")
						? (ClusterConnectionProvider) accessor.getPropertyValue("connectionHandler")
						: null;
			} else {
				this.connectionHandler = null;
			}
		}

		@Override
		@SuppressWarnings("unchecked")
		public Jedis getResourceForSpecificNode(RedisClusterNode node) {

			Assert.notNull(node, "Cannot get Pool for 'null' node!");

			ConnectionPool pool = getResourcePoolForSpecificNode(node);
			if (pool != null) {
				return new Jedis(pool.getResource());
			}

			Connection connection = getConnectionForSpecificNode(node);

			if (connection != null) {
				return new Jedis(connection);
			}

			throw new DataAccessResourceFailureException(String.format("Node %s is unknown to cluster", node));
		}

		private ConnectionPool getResourcePoolForSpecificNode(RedisClusterNode node) {

			Map<String, ConnectionPool> clusterNodes = cluster.getClusterNodes();
			if (clusterNodes.containsKey(node.asString())) {
				return clusterNodes.get(node.asString());
			}

			return null;
		}

		private Connection getConnectionForSpecificNode(RedisClusterNode node) {

			RedisClusterNode member = topologyProvider.getTopology().lookup(node);

			if (!member.hasValidHost()) {
				throw new DataAccessResourceFailureException(String
						.format("Cannot obtain connection to node %ss as it is not associated with a hostname!", node.getId()));
			}

			if (member != null && connectionHandler != null) {
				return connectionHandler.getConnection(new HostAndPort(member.getHost(), member.getPort()));
			}

			return null;
		}

		@Override
		public void returnResourceForSpecificNode(RedisClusterNode node, Object client) {
			((Jedis) client).close();
		}
	}

	/**
	 * Jedis specific implementation of {@link ClusterTopologyProvider}.
	 *
	 * @author Christoph Strobl
	 * @author Mark Paluch
	 * @since 1.7
	 */
	public static class JedisClusterTopologyProvider implements ClusterTopologyProvider {

		private final Object lock = new Object();
		private final JedisCluster cluster;
		private final long cacheTimeMs;
		private long time = 0;
		private @Nullable ClusterTopology cached;

		/**
		 * Create new {@link JedisClusterTopologyProvider}. Uses a default cache timeout of 100 milliseconds.
		 *
		 * @param cluster must not be {@literal null}.
		 */
		public JedisClusterTopologyProvider(JedisCluster cluster) {
			this(cluster, Duration.ofMillis(100));
		}

		/**
		 * Create new {@link JedisClusterTopologyProvider}.
		 *
		 * @param cluster must not be {@literal null}.
		 * @param cacheTimeout must not be {@literal null}.
		 * @since 2.2
		 */
		public JedisClusterTopologyProvider(JedisCluster cluster, Duration cacheTimeout) {

			Assert.notNull(cluster, "JedisCluster must not be null!");
			Assert.notNull(cacheTimeout, "Cache timeout must not be null!");
			Assert.isTrue(!cacheTimeout.isNegative(), "Cache timeout must not be negative.");

			this.cluster = cluster;
			this.cacheTimeMs = cacheTimeout.toMillis();
		}

		@Override
		public ClusterTopology getTopology() {

			if (cached != null && shouldUseCachedValue()) {
				return cached;
			}

			Map<String, Exception> errors = new LinkedHashMap<>();

			List<Entry<String, ConnectionPool>> list = new ArrayList<>(cluster.getClusterNodes().entrySet());
			Collections.shuffle(list);

			for (Entry<String, ConnectionPool> entry : list) {

				try (Connection connection = entry.getValue().getResource()) {

					time = System.currentTimeMillis();
					Set<RedisClusterNode> nodes = Converters.toSetOfRedisClusterNodes(new Jedis(connection).clusterNodes());

					synchronized (lock) {
						cached = new ClusterTopology(nodes);
					}
					return cached;
				} catch (Exception ex) {
					errors.put(entry.getKey(), ex);
				}
			}

			StringBuilder sb = new StringBuilder();

			for (Entry<String, Exception> entry : errors.entrySet()) {
				sb.append(String.format("\r\n\t- %s failed: %s", entry.getKey(), entry.getValue().getMessage()));
			}

			throw new ClusterStateFailureException(
					"Could not retrieve cluster information. CLUSTER NODES returned with error." + sb.toString());
		}

		/**
		 * Returns whether {@link #getTopology()} should return the cached {@link ClusterTopology}. Uses a time-based
		 * caching.
		 *
		 * @return {@literal true} to use the cached {@link ClusterTopology}; {@literal false} to fetch a new cluster
		 *         topology.
		 * @see #JedisClusterTopologyProvider(JedisCluster, Duration)
		 * @since 2.2
		 */
		protected boolean shouldUseCachedValue() {
			return time + cacheTimeMs > System.currentTimeMillis();
		}
	}

	protected JedisCluster getCluster() {
		return cluster;
	}

	protected ClusterCommandExecutor getClusterCommandExecutor() {
		return clusterCommandExecutor;
	}

	protected ClusterTopologyProvider getTopologyProvider() {
		return topologyProvider;
	}
}
