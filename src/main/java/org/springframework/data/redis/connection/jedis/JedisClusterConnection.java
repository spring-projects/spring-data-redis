/*
 * Copyright 2015-2017 the original author or authors.
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
package org.springframework.data.redis.connection.jedis;

import redis.clients.jedis.BinaryJedis;
import redis.clients.jedis.BinaryJedisPubSub;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisClusterConnectionHandler;
import redis.clients.jedis.JedisPool;

import java.util.Collection;
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
 * @since 1.7
 */
public class JedisClusterConnection implements DefaultedRedisClusterConnection {

	private static final ExceptionTranslationStrategy EXCEPTION_TRANSLATION = new FallbackExceptionTranslationStrategy(
			JedisConverters.exceptionConverter());

	private static final byte[][] EMPTY_2D_BYTE_ARRAY = new byte[0][];

	private final Log log = LogFactory.getLog(getClass());

	private final JedisCluster cluster;

	private boolean closed;

	private final JedisClusterTopologyProvider topologyProvider;
	private ClusterCommandExecutor clusterCommandExecutor;
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
			DirectFieldAccessor dfa = new DirectFieldAccessor(cluster);
			clusterCommandExecutor.setMaxRedirects((Integer) dfa.getPropertyValue("maxRedirections"));
		} catch (Exception e) {
			// ignore it and work with the executor default
		}
	}

	/**
	 * Create new {@link JedisClusterConnection} utilizing native connections via {@link JedisCluster} running commands
	 * across the cluster via given {@link ClusterCommandExecutor}.
	 *
	 * @param cluster must not be {@literal null}.
	 * @param executor must not be {@literal null}.
	 */
	public JedisClusterConnection(JedisCluster cluster, ClusterCommandExecutor executor) {

		Assert.notNull(cluster, "JedisCluster must not be null.");
		Assert.notNull(executor, "ClusterCommandExecutor must not be null.");

		this.closed = false;
		this.cluster = cluster;
		this.topologyProvider = new JedisClusterTopologyProvider(cluster);
		this.clusterCommandExecutor = executor;
		this.disposeClusterCommandExecutorOnClose = false;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisCommands#execute(java.lang.String, byte[][])
	 */
	@Nullable
	@Override
	public Object execute(String command, byte[]... args) {

		Assert.notNull(command, "Command must not be null!");
		Assert.notNull(args, "Args must not be null!");

		return clusterCommandExecutor
				.executeCommandOnArbitraryNode((JedisClusterCommandCallback<Object>) client -> JedisClientUtils.execute(command,
						EMPTY_2D_BYTE_ARRAY, args, () -> client))
				.getValue();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterConnection#execute(String, byte[], java.util.Collection)
	 */
	@Nullable
	@Override
	public <T> T execute(String command, byte[] key, Collection<byte[]> args) {

		Assert.notNull(command, "Command must not be null!");
		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(args, "Args must not be null!");

		byte[][] commandArgs = getCommandArguments(key, args);

		RedisClusterNode keyMaster = topologyProvider.getTopology().getKeyServingMasterNode(key);

		return clusterCommandExecutor.executeCommandOnSingleNode((JedisClusterCommandCallback<T>) client -> JedisClientUtils
				.execute(command, EMPTY_2D_BYTE_ARRAY, commandArgs, () -> client), keyMaster).getValue();
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
	 * Execute the given command for the {@code key} provided potentially appending args. <br />
	 * This method, other than {@link #execute(String, byte[]...)}, dispatches the command to the {@code key} serving
	 * master node.
	 *
	 * <pre>
	 * <code>
	 * // SET foo bar EX 10 NX
	 * execute("SET", "foo".getBytes(), asBinaryList("bar", "EX", 10, "NX")
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
			return JedisClientUtils.execute(command, new byte[][] { key }, args.toArray(new byte[args.size()][]),
					() -> client);
		}, keys).resultsAsList();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConnection#geoCommands()
	 */
	@Override
	public RedisGeoCommands geoCommands() {
		return new JedisClusterGeoCommands(this);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConnection#hashCommands()
	 */
	@Override
	public RedisHashCommands hashCommands() {
		return new JedisClusterHashCommands(this);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConnection#hyperLogLogCommands()
	 */
	@Override
	public RedisHyperLogLogCommands hyperLogLogCommands() {
		return new JedisClusterHyperLogLogCommands(this);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConnection#keyCommands()
	 */
	@Override
	public RedisKeyCommands keyCommands() {
		return doGetKeyCommands();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConnection#stringCommands()
	 */
	@Override
	public RedisStringCommands stringCommands() {
		return new JedisClusterStringCommands(this);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConnection#listCommands()
	 */
	@Override
	public RedisListCommands listCommands() {
		return new JedisClusterListCommands(this);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConnection#setCommands()
	 */
	@Override
	public RedisSetCommands setCommands() {
		return new JedisClusterSetCommands(this);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConnection#zSetCommands()
	 */
	@Override
	public RedisZSetCommands zSetCommands() {
		return new JedisClusterZSetCommands(this);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterConnection#serverCommands()
	 */
	@Override
	public RedisClusterServerCommands serverCommands() {
		return new JedisClusterServerCommands(this);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConnection#scriptingCommands()
	 */
	@Override
	public RedisScriptingCommands scriptingCommands() {
		return JedisClusterScriptingCommands.INSTANCE;
	}

	private JedisClusterKeyCommands doGetKeyCommands() {
		return new JedisClusterKeyCommands(this);
	}

	@Override
	public Set<byte[]> keys(RedisClusterNode node, byte[] pattern) {
		return doGetKeyCommands().keys(node, pattern);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterConnection#randomKey(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public byte[] randomKey(RedisClusterNode node) {
		return doGetKeyCommands().randomKey(node);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisTxCommands#multi()
	 */
	@Override
	public void multi() {
		throw new InvalidDataAccessApiUsageException("MUTLI is currently not supported in cluster mode.");
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisTxCommands#exec()
	 */
	@Override
	public List<Object> exec() {
		throw new InvalidDataAccessApiUsageException("EXEC is currently not supported in cluster mode.");
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisTxCommands#discard()
	 */
	@Override
	public void discard() {
		throw new InvalidDataAccessApiUsageException("DISCARD is currently not supported in cluster mode.");
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisTxCommands#watch(byte[][])
	 */
	@Override
	public void watch(byte[]... keys) {
		throw new InvalidDataAccessApiUsageException("WATCH is currently not supported in cluster mode.");
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisTxCommands#unwatch()
	 */
	@Override
	public void unwatch() {
		throw new InvalidDataAccessApiUsageException("UNWATCH is currently not supported in cluster mode.");
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisPubSubCommands#isSubscribed()
	 */
	@Override
	public boolean isSubscribed() {
		return (subscription != null && subscription.isAlive());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisPubSubCommands#getSubscription()
	 */
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
			BinaryJedisPubSub jedisPubSub = new JedisMessageListener(listener);
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
			BinaryJedisPubSub jedisPubSub = new JedisMessageListener(listener);
			subscription = new JedisSubscription(listener, jedisPubSub, null, patterns);
			cluster.psubscribe(jedisPubSub, patterns);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConnectionCommands#select(int)
	 */
	@Override
	public void select(int dbIndex) {

		if (dbIndex != 0) {
			throw new InvalidDataAccessApiUsageException("Cannot SELECT non zero index in cluster mode.");
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConnectionCommands#echo(byte[])
	 */
	@Override
	public byte[] echo(byte[] message) {

		try {
			return cluster.echo(message);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConnectionCommands#ping()
	 */
	@Override
	public String ping() {

		return !clusterCommandExecutor.executeCommandOnAllNodes((JedisClusterCommandCallback<String>) BinaryJedis::ping)
				.resultsAsList().isEmpty() ? "PONG" : null;

	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterConnection#ping(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public String ping(RedisClusterNode node) {

		return clusterCommandExecutor
				.executeCommandOnSingleNode((JedisClusterCommandCallback<String>) BinaryJedis::ping, node).getValue();
	}

	/*
	 * --> Cluster Commands
	 */

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterCommands#clusterSetSlot(org.springframework.data.redis.connection.RedisClusterNode, int, org.springframework.data.redis.connection.RedisClusterCommands.AddSlots)
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

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterCommands#clusterGetKeysInSlot(int, java.lang.Integer)
	 */
	@Override
	public List<byte[]> clusterGetKeysInSlot(int slot, Integer count) {

		RedisClusterNode node = clusterGetNodeForSlot(slot);

		clusterCommandExecutor
				.executeCommandOnSingleNode(
						(JedisClusterCommandCallback<List<byte[]>>) client -> JedisConverters.stringListToByteList()
								.convert(client.clusterGetKeysInSlot(slot, count != null ? count.intValue() : Integer.MAX_VALUE)),
						node);
		return null;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterCommands#clusterAddSlots(org.springframework.data.redis.connection.RedisClusterNode, int[])
	 */
	@Override
	public void clusterAddSlots(RedisClusterNode node, int... slots) {

		clusterCommandExecutor.executeCommandOnSingleNode(
				(JedisClusterCommandCallback<String>) client -> client.clusterAddSlots(slots), node);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterCommands#clusterAddSlots(org.springframework.data.redis.connection.RedisClusterNode, org.springframework.data.redis.connection.RedisClusterNode.SlotRange)
	 */
	@Override
	public void clusterAddSlots(RedisClusterNode node, SlotRange range) {

		Assert.notNull(range, "Range must not be null.");

		clusterAddSlots(node, range.getSlotsArray());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterCommands#clusterCountKeysInSlot(int)
	 */
	@Override
	public Long clusterCountKeysInSlot(int slot) {

		RedisClusterNode node = clusterGetNodeForSlot(slot);

		return clusterCommandExecutor.executeCommandOnSingleNode(
				(JedisClusterCommandCallback<Long>) client -> client.clusterCountKeysInSlot(slot), node).getValue();

	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterCommands#clusterDeleteSlots(org.springframework.data.redis.connection.RedisClusterNode, int[])
	 */
	@Override
	public void clusterDeleteSlots(RedisClusterNode node, int... slots) {

		clusterCommandExecutor.executeCommandOnSingleNode(
				(JedisClusterCommandCallback<String>) client -> client.clusterDelSlots(slots), node);

	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterCommands#clusterDeleteSlotsInRange(org.springframework.data.redis.connection.RedisClusterNode, org.springframework.data.redis.connection.RedisClusterNode.SlotRange)
	 */
	@Override
	public void clusterDeleteSlotsInRange(RedisClusterNode node, SlotRange range) {

		Assert.notNull(range, "Range must not be null.");

		clusterDeleteSlots(node, range.getSlotsArray());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterCommands#clusterForget(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public void clusterForget(RedisClusterNode node) {

		Set<RedisClusterNode> nodes = new LinkedHashSet<>(topologyProvider.getTopology().getActiveMasterNodes());
		RedisClusterNode nodeToRemove = topologyProvider.getTopology().lookup(node);
		nodes.remove(nodeToRemove);

		clusterCommandExecutor.executeCommandAsyncOnNodes(
				(JedisClusterCommandCallback<String>) client -> client.clusterForget(node.getId()), nodes);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterCommands#clusterMeet(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public void clusterMeet(RedisClusterNode node) {

		Assert.notNull(node, "Cluster node must not be null for CLUSTER MEET command!");
		Assert.hasText(node.getHost(), "Node to meet cluster must have a host!");
		Assert.isTrue(node.getPort() > 0, "Node to meet cluster must have a port greater 0!");

		clusterCommandExecutor.executeCommandOnAllNodes(
				(JedisClusterCommandCallback<String>) client -> client.clusterMeet(node.getHost(), node.getPort()));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterCommands#clusterReplicate(org.springframework.data.redis.connection.RedisClusterNode, org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public void clusterReplicate(RedisClusterNode master, RedisClusterNode slave) {

		RedisClusterNode masterNode = topologyProvider.getTopology().lookup(master);

		clusterCommandExecutor.executeCommandOnSingleNode(
				(JedisClusterCommandCallback<String>) client -> client.clusterReplicate(masterNode.getId()), slave);

	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterCommands#getClusterSlotForKey(byte[])
	 */
	@Override
	public Integer clusterGetSlotForKey(byte[] key) {

		return clusterCommandExecutor.executeCommandOnArbitraryNode((JedisClusterCommandCallback<Integer>) client -> client
				.clusterKeySlot(JedisConverters.toString(key)).intValue()).getValue();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterCommands#clusterGetNodeForSlot(int)
	 */
	@Override
	public RedisClusterNode clusterGetNodeForSlot(int slot) {

		for (RedisClusterNode node : topologyProvider.getTopology().getSlotServingNodes(slot)) {
			if (node.isMaster()) {
				return node;
			}
		}

		return null;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterCommands#clusterGetNodes()
	 */
	@Override
	public Set<RedisClusterNode> clusterGetNodes() {
		return topologyProvider.getTopology().getNodes();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterCommands#clusterGetSlaves(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public Set<RedisClusterNode> clusterGetSlaves(RedisClusterNode master) {

		Assert.notNull(master, "Master cannot be null!");

		RedisClusterNode nodeToUse = topologyProvider.getTopology().lookup(master);

		return JedisConverters.toSetOfRedisClusterNodes(clusterCommandExecutor
				.executeCommandOnSingleNode(
						(JedisClusterCommandCallback<List<String>>) client -> client.clusterSlaves(nodeToUse.getId()), master)
				.getValue());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterCommands#clusterGetMasterSlaveMap()
	 */
	@Override
	public Map<RedisClusterNode, Collection<RedisClusterNode>> clusterGetMasterSlaveMap() {

		List<NodeResult<Collection<RedisClusterNode>>> nodeResults = clusterCommandExecutor
				.executeCommandAsyncOnNodes((JedisClusterCommandCallback<Collection<RedisClusterNode>>) client -> {

					// TODO: remove client.eval as soon as Jedis offers support for myid
					return JedisConverters.toSetOfRedisClusterNodes(
							client.clusterSlaves((String) client.eval("return redis.call('cluster', 'myid')", 0)));
				}, topologyProvider.getTopology().getActiveMasterNodes()).getResults();

		Map<RedisClusterNode, Collection<RedisClusterNode>> result = new LinkedHashMap<>();

		for (NodeResult<Collection<RedisClusterNode>> nodeResult : nodeResults) {
			result.put(nodeResult.getNode(), nodeResult.getValue());
		}

		return result;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterCommands#clusterGetNodeForKey(byte[])
	 */
	@Override
	public RedisClusterNode clusterGetNodeForKey(byte[] key) {
		return clusterGetNodeForSlot(clusterGetSlotForKey(key));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterCommands#clusterGetClusterInfo()
	 */
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

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConnection#close()
	 */
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

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConnection#isClosed()
	 */
	@Override
	public boolean isClosed() {
		return closed;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConnection#getNativeConnection()
	 */
	@Override
	public JedisCluster getNativeConnection() {
		return cluster;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConnection#isQueueing()
	 */
	@Override
	public boolean isQueueing() {
		return false;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConnection#isPipelined()
	 */
	@Override
	public boolean isPipelined() {
		return false;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConnection#openPipeline()
	 */
	@Override
	public void openPipeline() {
		throw new UnsupportedOperationException("Pipeline is currently not supported for JedisClusterConnection.");
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConnection#closePipeline()
	 */
	@Override
	public List<Object> closePipeline() throws RedisPipelineException {
		throw new UnsupportedOperationException("Pipeline is currently not supported for JedisClusterConnection.");
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConnection#getSentinelConnection()
	 */
	@Override
	public RedisSentinelConnection getSentinelConnection() {
		throw new UnsupportedOperationException("Sentinel is currently not supported for JedisClusterConnection.");
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
		private final JedisClusterConnectionHandler connectionHandler;

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
						? (JedisClusterConnectionHandler) accessor.getPropertyValue("connectionHandler") : null;
			} else {
				this.connectionHandler = null;
			}
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.connection.ClusterNodeResourceProvider#getResourceForSpecificNode(org.springframework.data.redis.connection.RedisClusterNode)
		 */
		@Override
		@SuppressWarnings("unchecked")
		public Jedis getResourceForSpecificNode(RedisClusterNode node) {

			Assert.notNull(node, "Cannot get Pool for 'null' node!");

			JedisPool pool = getResourcePoolForSpecificNode(node);
			if (pool != null) {
				return pool.getResource();
			}

			Jedis connection = getConnectionForSpecificNode(node);

			if (connection != null) {
				return connection;
			}

			throw new DataAccessResourceFailureException(String.format("Node %s is unknown to cluster", node));
		}

		private JedisPool getResourcePoolForSpecificNode(RedisClusterNode node) {

			Map<String, JedisPool> clusterNodes = cluster.getClusterNodes();
			if (clusterNodes.containsKey(node.asString())) {
				return clusterNodes.get(node.asString());
			}

			return null;
		}

		private Jedis getConnectionForSpecificNode(RedisClusterNode node) {

			RedisClusterNode member = topologyProvider.getTopology().lookup(node);

			if (member != null && connectionHandler != null) {
				return connectionHandler.getConnectionFromNode(new HostAndPort(member.getHost(), member.getPort()));
			}

			return null;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.connection.ClusterNodeResourceProvider#returnResourceForSpecificNode(org.springframework.data.redis.connection.RedisClusterNode, java.lang.Object)
		 */
		@Override
		public void returnResourceForSpecificNode(RedisClusterNode node, Object client) {
			((Jedis) client).close();
		}
	}

	/**
	 * Jedis specific implementation of {@link ClusterTopologyProvider}.
	 *
	 * @author Christoph Strobl
	 * @since 1.7
	 */
	static class JedisClusterTopologyProvider implements ClusterTopologyProvider {

		private final Object lock = new Object();
		private final JedisCluster cluster;
		private long time = 0;
		private @Nullable ClusterTopology cached;

		/**
		 * Create new {@link JedisClusterTopologyProvider}.s
		 *
		 * @param cluster
		 */
		public JedisClusterTopologyProvider(JedisCluster cluster) {
			this.cluster = cluster;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.connection.ClusterTopologyProvider#getTopology()
		 */
		@Override
		public ClusterTopology getTopology() {

			if (cached != null && time + 100 > System.currentTimeMillis()) {
				return cached;
			}

			Map<String, Exception> errors = new LinkedHashMap<>();

			for (Entry<String, JedisPool> entry : cluster.getClusterNodes().entrySet()) {

				Jedis jedis = null;

				try {
					jedis = entry.getValue().getResource();

					time = System.currentTimeMillis();
					Set<RedisClusterNode> nodes = Converters.toSetOfRedisClusterNodes(jedis.clusterNodes());

					synchronized (lock) {
						cached = new ClusterTopology(nodes);
					}
					return cached;
				} catch (Exception ex) {
					errors.put(entry.getKey(), ex);
				} finally {
					if (jedis != null) {
						jedis.close();
					}
				}
			}

			StringBuilder sb = new StringBuilder();
			for (Entry<String, Exception> entry : errors.entrySet()) {
				sb.append(String.format("\r\n\t- %s failed: %s", entry.getKey(), entry.getValue().getMessage()));
			}
			throw new ClusterStateFailureException(
					"Could not retrieve cluster information. CLUSTER NODES returned with error." + sb.toString());
		}
	}

	protected JedisCluster getCluster() {
		return cluster;
	}

	protected ClusterCommandExecutor getClusterCommandExecutor() {
		return clusterCommandExecutor;
	}

	protected JedisClusterTopologyProvider getTopologyProvider() {
		return topologyProvider;
	}
}
