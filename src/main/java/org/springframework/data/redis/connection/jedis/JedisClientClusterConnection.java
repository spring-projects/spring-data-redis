/*
 * Copyright 2026-present the original author or authors.
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

import java.time.Duration;
import java.util.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.NullUnmarked;
import org.jspecify.annotations.Nullable;
import org.springframework.beans.PropertyAccessor;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
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
import org.springframework.util.Assert;

import redis.clients.jedis.*;
import redis.clients.jedis.providers.ClusterConnectionProvider;

/**
 * {@link RedisClusterConnection} implementation using Jedis 7.2+ {@link RedisClusterClient} API.
 * <p>
 * This implementation uses the new {@link RedisClusterClient} class introduced in Jedis 7.2.0 for managing Redis
 * Cluster operations. It follows the same pattern as {@link JedisClusterConnection} but uses the new client API.
 * <p>
 * This class is not Thread-safe and instances should not be shared across threads.
 *
 * @author Tihomir Mateev
 * @since 4.1
 * @see RedisClusterClient
 * @see JedisClusterConnection
 */
@NullUnmarked
public class JedisClientClusterConnection implements RedisClusterConnection {

	private static final ExceptionTranslationStrategy EXCEPTION_TRANSLATION = new FallbackExceptionTranslationStrategy(
			JedisExceptionConverter.INSTANCE);

	private final Log log = LogFactory.getLog(getClass());

	private final RedisClusterClient clusterClient;
	private final JedisClientClusterGeoCommands geoCommands = new JedisClientClusterGeoCommands(this);
	private final JedisClientClusterHashCommands hashCommands = new JedisClientClusterHashCommands(this);
	private final JedisClientClusterHyperLogLogCommands hllCommands = new JedisClientClusterHyperLogLogCommands(this);
	private final JedisClientClusterKeyCommands keyCommands = new JedisClientClusterKeyCommands(this);
	private final JedisClientClusterListCommands listCommands = new JedisClientClusterListCommands(this);
	private final JedisClientClusterSetCommands setCommands = new JedisClientClusterSetCommands(this);
	private final JedisClientClusterServerCommands serverCommands = new JedisClientClusterServerCommands(this);
	private final JedisClientClusterStreamCommands streamCommands = new JedisClientClusterStreamCommands(this);
	private final JedisClientClusterStringCommands stringCommands = new JedisClientClusterStringCommands(this);
	private final JedisClientClusterZSetCommands zSetCommands = new JedisClientClusterZSetCommands(this);

	private boolean closed;

	private final ClusterTopologyProvider topologyProvider;
	private final ClusterCommandExecutor clusterCommandExecutor;
	private final boolean disposeClusterCommandExecutorOnClose;

	private volatile @Nullable JedisSubscription subscription;

	/**
	 * Create new {@link JedisClientClusterConnection} utilizing native connections via {@link RedisClusterClient}.
	 *
	 * @param clusterClient must not be {@literal null}.
	 */
	public JedisClientClusterConnection(@NonNull RedisClusterClient clusterClient) {

		Assert.notNull(clusterClient, "RedisClusterClient must not be null");

		this.clusterClient = clusterClient;

		closed = false;
		topologyProvider = new JedisClientClusterTopologyProvider(clusterClient);
		clusterCommandExecutor = new ClusterCommandExecutor(topologyProvider,
				new JedisClientClusterNodeResourceProvider(clusterClient, topologyProvider), EXCEPTION_TRANSLATION);
		disposeClusterCommandExecutorOnClose = true;
	}

	/**
	 * Create new {@link JedisClientClusterConnection} utilizing native connections via {@link RedisClusterClient} running
	 * commands across the cluster via given {@link ClusterCommandExecutor}.
	 *
	 * @param clusterClient must not be {@literal null}.
	 * @param executor must not be {@literal null}.
	 */
	public JedisClientClusterConnection(@NonNull RedisClusterClient clusterClient,
			@NonNull ClusterCommandExecutor executor) {
		this(clusterClient, executor, new JedisClientClusterTopologyProvider(clusterClient));
	}

	/**
	 * Create new {@link JedisClientClusterConnection} utilizing native connections via {@link RedisClusterClient} running
	 * commands across the cluster via given {@link ClusterCommandExecutor} and using the given
	 * {@link ClusterTopologyProvider}.
	 *
	 * @param clusterClient must not be {@literal null}.
	 * @param executor must not be {@literal null}.
	 * @param topologyProvider must not be {@literal null}.
	 */
	public JedisClientClusterConnection(@NonNull RedisClusterClient clusterClient,
			@NonNull ClusterCommandExecutor executor, @NonNull ClusterTopologyProvider topologyProvider) {

		Assert.notNull(clusterClient, "RedisClusterClient must not be null");
		Assert.notNull(executor, "ClusterCommandExecutor must not be null");
		Assert.notNull(topologyProvider, "ClusterTopologyProvider must not be null");

		this.closed = false;
		this.clusterClient = clusterClient;
		this.topologyProvider = topologyProvider;
		this.clusterCommandExecutor = executor;
		this.disposeClusterCommandExecutorOnClose = false;
	}

	@Override
	public Object execute(@NonNull String command, byte @NonNull [] @NonNull... args) {

		Assert.notNull(command, "Command must not be null");
		Assert.notNull(args, "Args must not be null");

		JedisClientClusterCommandCallback<Object> commandCallback = jedis -> jedis
				.sendCommand(JedisClientUtils.getCommand(command), args);

		return this.clusterCommandExecutor.executeCommandOnArbitraryNode(commandCallback).getValue();
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T> T execute(@NonNull String command, byte @NonNull [] key, @NonNull Collection<byte @NonNull []> args) {

		Assert.notNull(command, "Command must not be null");
		Assert.notNull(key, "Key must not be null");
		Assert.notNull(args, "Args must not be null");

		byte[][] commandArgs = getCommandArguments(key, args);

		RedisClusterNode keyMaster = this.topologyProvider.getTopology().getKeyServingMasterNode(key);

		JedisClientClusterCommandCallback<T> commandCallback = jedis -> (T) jedis
				.sendCommand(JedisClientUtils.getCommand(command), commandArgs);

		return this.clusterCommandExecutor.executeCommandOnSingleNode(commandCallback, keyMaster).getValue();
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
	 */
	@SuppressWarnings("unchecked")
	public <T> List<T> execute(@NonNull String command, @NonNull Collection<byte @NonNull []> keys,
			@NonNull Collection<byte @NonNull []> args) {

		Assert.notNull(command, "Command must not be null");
		Assert.notNull(keys, "Key must not be null");
		Assert.notNull(args, "Args must not be null");

		JedisClientMultiKeyClusterCommandCallback<T> commandCallback = (jedis,
				key) -> (T) jedis.sendCommand(JedisClientUtils.getCommand(command), getCommandArguments(key, args));

		return this.clusterCommandExecutor.executeMultiKeyCommand(commandCallback, keys).resultsAsList();

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
		return new JedisClientClusterScriptingCommands(this);
	}

	@Override
	public Set<byte[]> keys(@NonNull RedisClusterNode node, byte @NonNull [] pattern) {
		return keyCommands.keys(node, pattern);
	}

	@Override
	public Cursor<byte[]> scan(@NonNull RedisClusterNode node, @NonNull ScanOptions options) {
		return keyCommands.scan(node, options);
	}

	@Override
	public byte[] randomKey(@NonNull RedisClusterNode node) {
		return keyCommands.randomKey(node);
	}

	@Override
	public void multi() {
		throw new InvalidDataAccessApiUsageException("MULTI is currently not supported in cluster mode");
	}

	@Override
	public List<Object> exec() {
		throw new InvalidDataAccessApiUsageException("EXEC is currently not supported in cluster mode");
	}

	@Override
	public void discard() {
		throw new InvalidDataAccessApiUsageException("DISCARD is currently not supported in cluster mode");
	}

	@Override
	public void watch(byte[] @NonNull... keys) {
		throw new InvalidDataAccessApiUsageException("WATCH is currently not supported in cluster mode");
	}

	@Override
	public void unwatch() {
		throw new InvalidDataAccessApiUsageException("UNWATCH is currently not supported in cluster mode");
	}

	@Override
	public boolean isSubscribed() {
		JedisSubscription subscription = this.subscription;
		return (subscription != null && subscription.isAlive());
	}

	@Override
	public Subscription getSubscription() {
		return this.subscription;
	}

	@Override
	public Long publish(byte @NonNull [] channel, byte @NonNull [] message) {

		try {
			return this.clusterClient.publish(channel, message);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public void subscribe(@NonNull MessageListener listener, byte @NonNull [] @NonNull... channels) {

		if (isSubscribed()) {
			String message = "Connection already subscribed; use the connection Subscription to cancel or add new channels";
			throw new RedisSubscribedConnectionException(message);
		}
		try {
			JedisMessageListener jedisPubSub = new JedisMessageListener(listener);
			subscription = new JedisSubscription(listener, jedisPubSub, channels, null);
			clusterClient.subscribe(jedisPubSub, channels);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public void pSubscribe(@NonNull MessageListener listener, byte @NonNull [] @NonNull... patterns) {

		if (isSubscribed()) {
			String message = "Connection already subscribed; use the connection Subscription to cancel or add new channels";
			throw new RedisSubscribedConnectionException(message);
		}

		try {
			JedisMessageListener jedisPubSub = new JedisMessageListener(listener);
			subscription = new JedisSubscription(listener, jedisPubSub, null, patterns);
			clusterClient.psubscribe(jedisPubSub, patterns);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public void select(int dbIndex) {

		if (dbIndex != 0) {
			throw new InvalidDataAccessApiUsageException("Cannot SELECT non zero index in cluster mode");
		}
	}

	@Override
	public byte[] echo(byte @NonNull [] message) {
		throw new InvalidDataAccessApiUsageException("Echo not supported in cluster mode");
	}

	@Override
	public String ping() {

		JedisClientClusterCommandCallback<String> command = Jedis::ping;

		return !this.clusterCommandExecutor.executeCommandOnAllNodes(command).resultsAsList().isEmpty() ? "PONG" : null;
	}

	@Override
	public String ping(@NonNull RedisClusterNode node) {

		JedisClientClusterCommandCallback<String> command = Jedis::ping;

		return this.clusterCommandExecutor.executeCommandOnSingleNode(command, node).getValue();
	}

	/*
	 * --> Cluster Commands
	 */

	@Override
	public void clusterSetSlot(@NonNull RedisClusterNode node, int slot, @NonNull AddSlots mode) {

		Assert.notNull(node, "Node must not be null");
		Assert.notNull(mode, "AddSlots mode must not be null");

		RedisClusterNode nodeToUse = this.topologyProvider.getTopology().lookup(node);
		String nodeId = nodeToUse.getId();

		JedisClientClusterCommandCallback<String> command = jedis -> switch (mode) {
			case IMPORTING -> jedis.clusterSetSlotImporting(slot, nodeId);
			case MIGRATING -> jedis.clusterSetSlotMigrating(slot, nodeId);
			case STABLE -> jedis.clusterSetSlotStable(slot);
			case NODE -> jedis.clusterSetSlotNode(slot, nodeId);
		};

		this.clusterCommandExecutor.executeCommandOnSingleNode(command, node);
	}

	@Override
	public List<byte[]> clusterGetKeysInSlot(int slot, @NonNull Integer count) {

		RedisClusterNode node = clusterGetNodeForSlot(slot);

		JedisClientClusterCommandCallback<List<byte[]>> command = jedis -> JedisConverters.stringListToByteList()
				.convert(jedis.clusterGetKeysInSlot(slot, nullSafeIntValue(count)));

		NodeResult<@NonNull List<byte[]>> result = this.clusterCommandExecutor.executeCommandOnSingleNode(command, node);

		return result.getValue();
	}

	private int nullSafeIntValue(@Nullable Integer value) {
		return value != null ? value : Integer.MAX_VALUE;
	}

	@Override
	public void clusterAddSlots(@NonNull RedisClusterNode node, int @NonNull... slots) {

		JedisClientClusterCommandCallback<String> command = jedis -> jedis.clusterAddSlots(slots);

		this.clusterCommandExecutor.executeCommandOnSingleNode(command, node);
	}

	@Override
	public void clusterAddSlots(@NonNull RedisClusterNode node, @NonNull SlotRange range) {

		Assert.notNull(range, "Range must not be null");

		clusterAddSlots(node, range.getSlotsArray());
	}

	@Override
	public Long clusterCountKeysInSlot(int slot) {

		RedisClusterNode node = clusterGetNodeForSlot(slot);

		JedisClientClusterCommandCallback<Long> command = jedis -> jedis.clusterCountKeysInSlot(slot);

		return this.clusterCommandExecutor.executeCommandOnSingleNode(command, node).getValue();
	}

	@Override
	public void clusterDeleteSlots(@NonNull RedisClusterNode node, int @NonNull... slots) {

		JedisClientClusterCommandCallback<String> command = jedis -> jedis.clusterDelSlots(slots);

		this.clusterCommandExecutor.executeCommandOnSingleNode(command, node);
	}

	@Override
	public void clusterDeleteSlotsInRange(@NonNull RedisClusterNode node, @NonNull SlotRange range) {

		Assert.notNull(range, "Range must not be null");

		clusterDeleteSlots(node, range.getSlotsArray());
	}

	@Override
	public void clusterForget(@NonNull RedisClusterNode node) {

		Set<RedisClusterNode> nodes = new LinkedHashSet<>(this.topologyProvider.getTopology().getActiveMasterNodes());
		RedisClusterNode nodeToRemove = this.topologyProvider.getTopology().lookup(node);

		nodes.remove(nodeToRemove);

		JedisClientClusterCommandCallback<String> command = jedis -> jedis.clusterForget(node.getId());

		this.clusterCommandExecutor.executeCommandAsyncOnNodes(command, nodes);
	}

	@Override
	@SuppressWarnings("all")
	public void clusterMeet(@NonNull RedisClusterNode node) {

		Assert.notNull(node, "Cluster node must not be null for CLUSTER MEET command");
		Assert.hasText(node.getHost(), "Node to meet cluster must have a host");
		Assert.isTrue(node.getPort() > 0, "Node to meet cluster must have a port greater 0");

		JedisClientClusterCommandCallback<String> command = jedis -> jedis.clusterMeet(node.getRequiredHost(),
				node.getRequiredPort());

		this.clusterCommandExecutor.executeCommandOnAllNodes(command);
	}

	@Override
	public void clusterReplicate(@NonNull RedisClusterNode master, @NonNull RedisClusterNode replica) {

		RedisClusterNode masterNode = this.topologyProvider.getTopology().lookup(master);

		JedisClientClusterCommandCallback<String> command = jedis -> jedis.clusterReplicate(masterNode.getId());

		this.clusterCommandExecutor.executeCommandOnSingleNode(command, replica);
	}

	@Override
	public Integer clusterGetSlotForKey(byte @NonNull [] key) {

		JedisClientClusterCommandCallback<Integer> command = jedis -> Long
				.valueOf(jedis.clusterKeySlot(JedisConverters.toString(key))).intValue();

		return this.clusterCommandExecutor.executeCommandOnArbitraryNode(command).getValue();
	}

	@Override
	public RedisClusterNode clusterGetNodeForKey(byte @NonNull [] key) {
		return this.topologyProvider.getTopology().getKeyServingMasterNode(key);
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
		return this.topologyProvider.getTopology().getNodes();
	}

	@Override
	public Set<RedisClusterNode> clusterGetReplicas(@NonNull RedisClusterNode master) {

		Assert.notNull(master, "Master cannot be null");

		RedisClusterNode nodeToUse = this.topologyProvider.getTopology().lookup(master);

		JedisClientClusterCommandCallback<List<String>> command = jedis -> jedis.clusterSlaves(nodeToUse.getId());

		List<String> clusterNodes = this.clusterCommandExecutor.executeCommandOnSingleNode(command, master).getValue();

		return JedisConverters.toSetOfRedisClusterNodes(clusterNodes);
	}

	@Override
	public Map<RedisClusterNode, Collection<RedisClusterNode>> clusterGetMasterReplicaMap() {

		JedisClientClusterCommandCallback<Collection<RedisClusterNode>> command = jedis -> JedisConverters
				.toSetOfRedisClusterNodes(jedis.clusterSlaves(jedis.clusterMyId()));

		Set<RedisClusterNode> activeMasterNodes = this.topologyProvider.getTopology().getActiveMasterNodes();

		List<NodeResult<@NonNull Collection<RedisClusterNode>>> nodeResults = this.clusterCommandExecutor
				.executeCommandAsyncOnNodes(command, activeMasterNodes).getResults();

		Map<RedisClusterNode, Collection<RedisClusterNode>> result = new LinkedHashMap<>();

		for (NodeResult<@NonNull Collection<RedisClusterNode>> nodeResult : nodeResults) {
			result.put(nodeResult.getNode(), nodeResult.getValue());
		}

		return result;
	}

	@Override
	public ClusterInfo clusterGetClusterInfo() {

		JedisClientClusterCommandCallback<String> command = Jedis::clusterInfo;

		String source = this.clusterCommandExecutor.executeCommandOnArbitraryNode(command).getValue();

		return new ClusterInfo(JedisConverters.toProperties(source));
	}

	/*
	 * Little helpers to make it work
	 */

	/**
	 * Converts the given Jedis exception to an appropriate Spring {@link DataAccessException}.
	 *
	 * @param cause the exception to convert, must not be {@literal null}.
	 * @return the converted {@link DataAccessException}.
	 */
	protected DataAccessException convertJedisAccessException(Exception cause) {

		DataAccessException translated = EXCEPTION_TRANSLATION.translate(cause);

		return translated != null ? translated : new RedisSystemException(cause.getMessage(), cause);
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
	public RedisClusterClient getNativeConnection() {
		return clusterClient;
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
		throw new InvalidDataAccessApiUsageException("Pipeline is not supported for JedisClientClusterConnection");
	}

	@Override
	public List<Object> closePipeline() throws RedisPipelineException {
		throw new InvalidDataAccessApiUsageException("Pipeline is not supported for JedisClientClusterConnection");
	}

	@Override
	public RedisSentinelConnection getSentinelConnection() {
		throw new InvalidDataAccessApiUsageException("Sentinel is not supported for JedisClientClusterConnection");
	}

	@Override
	public void rewriteConfig() {
		serverCommands().rewriteConfig();
	}

	/**
	 * {@link Jedis} specific {@link ClusterCommandCallback}.
	 *
	 * @author Tihomir Mateev
	 * @param <T>
	 * @since 4.1
	 */
	protected interface JedisClientClusterCommandCallback<T> extends ClusterCommandCallback<@NonNull Jedis, T> {}

	/**
	 * {@link Jedis} specific {@link MultiKeyClusterCommandCallback}.
	 *
	 * @author Tihomir Mateev
	 * @param <T>
	 * @since 4.1
	 */
	protected interface JedisClientMultiKeyClusterCommandCallback<T>
			extends MultiKeyClusterCommandCallback<@NonNull Jedis, T> {}

	/**
	 * Jedis specific implementation of {@link ClusterNodeResourceProvider}.
	 *
	 * @author Tihomir Mateev
	 * @since 4.1
	 */
	@NullMarked
	static class JedisClientClusterNodeResourceProvider implements ClusterNodeResourceProvider {

		private final RedisClusterClient clusterClient;
		private final ClusterTopologyProvider topologyProvider;
		private final @Nullable ClusterConnectionProvider connectionHandler;

		/**
		 * Creates new {@link JedisClientClusterNodeResourceProvider}.
		 *
		 * @param clusterClient should not be {@literal null}.
		 * @param topologyProvider must not be {@literal null}.
		 */
		JedisClientClusterNodeResourceProvider(RedisClusterClient clusterClient, ClusterTopologyProvider topologyProvider) {

			this.clusterClient = clusterClient;
			this.topologyProvider = topologyProvider;

			PropertyAccessor accessor = new DirectFieldAccessFallbackBeanWrapper(clusterClient);
			this.connectionHandler = accessor.isReadableProperty("connectionHandler")
					? (ClusterConnectionProvider) accessor.getPropertyValue("connectionHandler")
					: null;

		}

		@Override
		@SuppressWarnings("unchecked")
		public Jedis getResourceForSpecificNode(RedisClusterNode node) {

			Assert.notNull(node, "Cannot get Pool for 'null' node");

			ConnectionPool pool = getResourcePoolForSpecificNode(node);
			if (pool != null) {
				return new Jedis(pool.getResource());
			}

			Connection connection = getConnectionForSpecificNode(node);

			if (connection != null) {
				return new Jedis(connection);
			}

			throw new DataAccessResourceFailureException("Node %s is unknown to cluster".formatted(node));
		}

		private @Nullable ConnectionPool getResourcePoolForSpecificNode(RedisClusterNode node) {

			Map<String, ConnectionPool> clusterNodes = clusterClient.getClusterNodes();
			HostAndPort hap = JedisConverters.toHostAndPort(node);
			String key = JedisClusterInfoCache.getNodeKey(hap);

			if (clusterNodes.containsKey(key)) {
				return clusterNodes.get(key);
			}

			return null;
		}

		private @Nullable Connection getConnectionForSpecificNode(RedisClusterNode node) {

			RedisClusterNode member = topologyProvider.getTopology().lookup(node);

			if (!member.hasValidHost()) {
				throw new DataAccessResourceFailureException(
						"Cannot obtain connection to node %s; it is not associated with a hostname".formatted(node.getId()));
			}

			if (connectionHandler != null) {
				return connectionHandler.getConnection(JedisConverters.toHostAndPort(member));
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
	 * @author Tihomir Mateev
	 * @since 4.1
	 */
	@NullMarked
	public static class JedisClientClusterTopologyProvider implements ClusterTopologyProvider {

		private final RedisClusterClient clusterClient;

		private final long cacheTimeMs;

		private volatile @Nullable JedisClientClusterTopology cached;

		/**
		 * Create new {@link JedisClientClusterTopologyProvider}. Uses a default cache timeout of 100 milliseconds.
		 *
		 * @param clusterClient must not be {@literal null}.
		 */
		public JedisClientClusterTopologyProvider(RedisClusterClient clusterClient) {
			this(clusterClient, Duration.ofMillis(100));
		}

		/**
		 * Create new {@link JedisClientClusterTopologyProvider}.
		 *
		 * @param clusterClient must not be {@literal null}.
		 * @param cacheTimeout must not be {@literal null}.
		 */
		public JedisClientClusterTopologyProvider(RedisClusterClient clusterClient, Duration cacheTimeout) {

			Assert.notNull(clusterClient, "RedisClusterClient must not be null");
			Assert.notNull(cacheTimeout, "Cache timeout must not be null");
			Assert.isTrue(!cacheTimeout.isNegative(), "Cache timeout must not be negative");

			this.clusterClient = clusterClient;
			this.cacheTimeMs = cacheTimeout.toMillis();
		}

		@Override
		@SuppressWarnings("NullAway")
		public ClusterTopology getTopology() {

			JedisClientClusterTopology topology = cached;
			if (shouldUseCachedValue(topology)) {
				return topology;
			}

			Map<String, Exception> errors = new LinkedHashMap<>();
			List<Map.Entry<String, ConnectionPool>> list = new ArrayList<>(clusterClient.getClusterNodes().entrySet());

			Collections.shuffle(list);

			for (Map.Entry<String, ConnectionPool> entry : list) {

				try (Connection connection = entry.getValue().getResource()) {

					Set<RedisClusterNode> nodes = Converters.toSetOfRedisClusterNodes(new Jedis(connection).clusterNodes());
					topology = cached = new JedisClientClusterTopology(nodes, System.currentTimeMillis(), cacheTimeMs);
					return topology;

				} catch (Exception ex) {
					errors.put(entry.getKey(), ex);
				}
			}

			StringBuilder stringBuilder = new StringBuilder();

			for (Map.Entry<String, Exception> entry : errors.entrySet()) {
				stringBuilder.append("\r\n\t- %s failed: %s".formatted(entry.getKey(), entry.getValue().getMessage()));
			}

			throw new org.springframework.data.redis.ClusterStateFailureException(
					"Could not retrieve cluster information; CLUSTER NODES returned with error" + stringBuilder);
		}

		/**
		 * Returns whether {@link #getTopology()} should return the cached {@link JedisClientClusterTopology}. Uses a
		 * time-based caching.
		 *
		 * @return {@literal true} to use the cached {@link ClusterTopology}; {@literal false} to fetch a new cluster
		 *         topology.
		 * @see #JedisClientClusterTopologyProvider(RedisClusterClient, Duration)
		 */
		protected boolean shouldUseCachedValue(@Nullable JedisClientClusterTopology topology) {
			return topology != null && topology.getMaxTime() > System.currentTimeMillis();
		}
	}

	/**
	 * Extension of {@link ClusterTopology} that includes time-based caching information.
	 *
	 * @author Tihomir Mateev
	 * @since 4.1
	 */
	protected static class JedisClientClusterTopology extends ClusterTopology {

		private final long time;
		private final long timeoutMs;

		/**
		 * Creates a new {@link JedisClientClusterTopology}.
		 *
		 * @param nodes the cluster nodes, must not be {@literal null}.
		 * @param creationTimeMs the time in milliseconds when this topology was created.
		 * @param timeoutMs the timeout in milliseconds after which this topology should be refreshed.
		 */
		JedisClientClusterTopology(Set<RedisClusterNode> nodes, long creationTimeMs, long timeoutMs) {
			super(nodes);
			this.time = creationTimeMs;
			this.timeoutMs = timeoutMs;
		}

		/**
		 * Get the time in ms when the {@link ClusterTopology} was captured.
		 *
		 * @return ClusterTopology time.
		 */
		public long getTime() {
			return time;
		}

		/**
		 * Get the maximum time in ms the {@link ClusterTopology} should be used before a refresh is required.
		 *
		 * @return ClusterTopology maximum age.
		 */
		long getMaxTime() {
			return time + timeoutMs;
		}
	}

	/**
	 * Returns the underlying {@link RedisClusterClient}.
	 *
	 * @return the cluster client, never {@literal null}.
	 */
	protected RedisClusterClient getClusterClient() {
		return clusterClient;
	}

	/**
	 * Returns the {@link ClusterCommandExecutor} used to execute commands across the cluster.
	 *
	 * @return the cluster command executor, never {@literal null}.
	 */
	protected ClusterCommandExecutor getClusterCommandExecutor() {
		return clusterCommandExecutor;
	}

	/**
	 * Returns the {@link ClusterTopologyProvider} used to obtain cluster topology information.
	 *
	 * @return the topology provider, never {@literal null}.
	 */
	protected ClusterTopologyProvider getTopologyProvider() {
		return topologyProvider;
	}
}
