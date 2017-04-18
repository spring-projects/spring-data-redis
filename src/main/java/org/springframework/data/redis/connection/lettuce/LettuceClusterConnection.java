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
package org.springframework.data.redis.connection.lettuce;

import io.lettuce.core.RedisException;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.SlotHash;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;
import io.lettuce.core.cluster.models.partitions.Partitions;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.springframework.beans.DirectFieldAccessor;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.ExceptionTranslationStrategy;
import org.springframework.data.redis.PassThroughExceptionTranslationStrategy;
import org.springframework.data.redis.connection.ClusterCommandExecutor;
import org.springframework.data.redis.connection.ClusterCommandExecutor.ClusterCommandCallback;
import org.springframework.data.redis.connection.ClusterCommandExecutor.MultiKeyClusterCommandCallback;
import org.springframework.data.redis.connection.ClusterCommandExecutor.NodeResult;
import org.springframework.data.redis.connection.ClusterInfo;
import org.springframework.data.redis.connection.ClusterNodeResourceProvider;
import org.springframework.data.redis.connection.ClusterTopology;
import org.springframework.data.redis.connection.ClusterTopologyProvider;
import org.springframework.data.redis.connection.RedisClusterNode;
import org.springframework.data.redis.connection.RedisClusterNode.SlotRange;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.connection.RedisHashCommands;
import org.springframework.data.redis.connection.RedisHyperLogLogCommands;
import org.springframework.data.redis.connection.RedisKeyCommands;
import org.springframework.data.redis.connection.RedisListCommands;
import org.springframework.data.redis.connection.RedisSetCommands;
import org.springframework.data.redis.connection.RedisStringCommands;
import org.springframework.data.redis.connection.RedisZSetCommands;
import org.springframework.data.redis.connection.convert.Converters;
import org.springframework.data.redis.core.types.RedisClientInfo;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 1.7
 */
public class LettuceClusterConnection extends LettuceConnection
		implements org.springframework.data.redis.connection.RedisClusterConnection {

	static final ExceptionTranslationStrategy exceptionConverter = new PassThroughExceptionTranslationStrategy(
			new LettuceExceptionConverter());
	static final RedisCodec<byte[], byte[]> CODEC = ByteArrayCodec.INSTANCE;

	private final RedisClusterClient clusterClient;
	private ClusterCommandExecutor clusterCommandExecutor;
	private ClusterTopologyProvider topologyProvider;

	/**
	 * Creates new {@link LettuceClusterConnection} using {@link RedisClusterClient}.
	 * 
	 * @param clusterClient must not be {@literal null}.
	 */
	public LettuceClusterConnection(RedisClusterClient clusterClient) {

		super(null, 100, clusterClient, null, 0);

		Assert.notNull(clusterClient, "RedisClusterClient must not be null.");

		this.clusterClient = clusterClient;
		topologyProvider = new LettuceClusterTopologyProvider(clusterClient);
		clusterCommandExecutor = new ClusterCommandExecutor(topologyProvider,
				new LettuceClusterNodeResourceProvider(clusterClient), exceptionConverter);
	}

	/**
	 * Creates new {@link LettuceClusterConnection} using {@link RedisClusterClient} running commands across the cluster
	 * via given {@link ClusterCommandExecutor}.
	 *
	 * @param clusterClient must not be {@literal null}.
	 * @param executor must not be {@literal null}.
	 */
	public LettuceClusterConnection(RedisClusterClient clusterClient, ClusterCommandExecutor executor) {

		super(null, 100, clusterClient, null, 0);

		Assert.notNull(clusterClient, "RedisClusterClient must not be null.");
		Assert.notNull(executor, "ClusterCommandExecutor must not be null.");

		this.clusterClient = clusterClient;
		topologyProvider = new LettuceClusterTopologyProvider(clusterClient);
		clusterCommandExecutor = executor;
	}

	@Override
	public RedisGeoCommands geoCommands() {
		return new LettuceClusterGeoCommands(this);
	}

	@Override
	public RedisHashCommands hashCommands() {
		return new LettuceClusterHashCommands(this);
	}

	@Override
	public RedisHyperLogLogCommands hyperLogLogCommands() {
		return new LettuceClusterHyperLogLogCommands(this);
	}

	@Override
	public RedisKeyCommands keyCommands() {
		return doGetClusterKeyCommands();
	}

	private LettuceClusterKeyCommands doGetClusterKeyCommands() {
		return new LettuceClusterKeyCommands(this);
	}

	@Override
	public RedisListCommands listCommands() {
		return new LettuceClusterListCommands(this);
	}

	@Override
	public RedisStringCommands stringCommands() {
		return new LettuceClusterStringCommands(this);
	}

	@Override
	public RedisSetCommands setCommands() {
		return new LettuceClusterSetCommands(this);
	}

	@Override
	public RedisZSetCommands zSetCommands() {
		return new LettuceClusterZSetCommands(this);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnection#flushAll()
	 */
	@Override
	public void flushAll() {

		clusterCommandExecutor.executeCommandOnAllNodes(new LettuceClusterCommandCallback<String>() {

			@Override
			public String doInCluster(RedisClusterCommands<byte[], byte[]> client) {
				return client.flushall();
			}
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnection#flushDb()
	 */
	@Override
	public void flushDb() {

		clusterCommandExecutor.executeCommandOnAllNodes(new LettuceClusterCommandCallback<String>() {

			@Override
			public String doInCluster(RedisClusterCommands<byte[], byte[]> client) {
				return client.flushdb();
			}
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnection#dbSize()
	 */
	@Override
	public Long dbSize() {

		Collection<Long> dbSizes = clusterCommandExecutor
				.executeCommandOnAllNodes(new LettuceClusterCommandCallback<Long>() {

					@Override
					public Long doInCluster(RedisClusterCommands<byte[], byte[]> client) {
						return client.dbsize();
					}

				}).resultsAsList();

		if (CollectionUtils.isEmpty(dbSizes)) {
			return 0L;
		}

		Long size = 0L;
		for (Long value : dbSizes) {
			size += value;
		}
		return size;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnection#info()
	 */
	@Override
	public Properties info() {

		Properties infos = new Properties();

		List<NodeResult<Properties>> nodeResults = clusterCommandExecutor
				.executeCommandOnAllNodes(new LettuceClusterCommandCallback<Properties>() {

					@Override
					public Properties doInCluster(RedisClusterCommands<byte[], byte[]> client) {
						return LettuceConverters.toProperties(client.info());
					}
				}).getResults();

		for (NodeResult<Properties> nodePorperties : nodeResults) {
			for (Entry<Object, Object> entry : nodePorperties.getValue().entrySet()) {
				infos.put(nodePorperties.getNode().asString() + "." + entry.getKey(), entry.getValue());
			}
		}

		return infos;
	}

	@Override
	public Properties info(final String section) {

		Properties infos = new Properties();
		List<NodeResult<Properties>> nodeResults = clusterCommandExecutor
				.executeCommandOnAllNodes(new LettuceClusterCommandCallback<Properties>() {

					@Override
					public Properties doInCluster(RedisClusterCommands<byte[], byte[]> client) {
						return LettuceConverters.toProperties(client.info(section));
					}
				}).getResults();

		for (NodeResult<Properties> nodePorperties : nodeResults) {
			for (Entry<Object, Object> entry : nodePorperties.getValue().entrySet()) {
				infos.put(nodePorperties.getNode().asString() + "." + entry.getKey(), entry.getValue());
			}
		}

		return infos;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterConnection#info(org.springframework.data.redis.connection.RedisClusterNode, java.lang.String)
	 */
	@Override
	public Properties info(RedisClusterNode node, final String section) {

		return LettuceConverters
				.toProperties(clusterCommandExecutor.executeCommandOnSingleNode(new LettuceClusterCommandCallback<String>() {

					@Override
					public String doInCluster(RedisClusterCommands<byte[], byte[]> client) {
						return client.info(section);
					}
				}, node).getValue());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterCommands#getClusterSlaves(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public Set<RedisClusterNode> clusterGetSlaves(final RedisClusterNode master) {

		Assert.notNull(master, "Master must not be null!");

		final RedisClusterNode nodeToUse = topologyProvider.getTopology().lookup(master);

		return clusterCommandExecutor
				.executeCommandOnSingleNode(new LettuceClusterCommandCallback<Set<RedisClusterNode>>() {

					@Override
					public Set<RedisClusterNode> doInCluster(RedisClusterCommands<byte[], byte[]> client) {
						return LettuceConverters.toSetOfRedisClusterNodes(client.clusterSlaves(nodeToUse.getId()));
					}
				}, master).getValue();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterCommands#getClusterSlotForKey(byte[])
	 */
	@Override
	public Integer clusterGetSlotForKey(byte[] key) {
		return SlotHash.getSlot(key);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterCommands#getClusterNodeForSlot(int)
	 */
	@Override
	public RedisClusterNode clusterGetNodeForSlot(int slot) {

		DirectFieldAccessor accessor = new DirectFieldAccessor(clusterClient);
		return LettuceConverters
				.toRedisClusterNode(((Partitions) accessor.getPropertyValue("partitions")).getPartitionBySlot(slot));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterCommands#getClusterNodeForKey(byte[])
	 */
	@Override
	public RedisClusterNode clusterGetNodeForKey(byte[] key) {
		return clusterGetNodeForSlot(clusterGetSlotForKey(key));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterCommands#getClusterInfo()
	 */
	@Override
	public ClusterInfo clusterGetClusterInfo() {

		return clusterCommandExecutor.executeCommandOnArbitraryNode(new LettuceClusterCommandCallback<ClusterInfo>() {

			@Override
			public ClusterInfo doInCluster(RedisClusterCommands<byte[], byte[]> client) {
				return new ClusterInfo(LettuceConverters.toProperties(client.clusterInfo()));
			}
		}).getValue();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterCommands#addSlots(org.springframework.data.redis.connection.RedisClusterNode, int[])
	 */
	@Override
	public void clusterAddSlots(RedisClusterNode node, final int... slots) {

		clusterCommandExecutor.executeCommandOnSingleNode(new LettuceClusterCommandCallback<String>() {

			@Override
			public String doInCluster(RedisClusterCommands<byte[], byte[]> client) {
				return client.clusterAddSlots(slots);
			}
		}, node);

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
	 * @see org.springframework.data.redis.connection.RedisClusterCommands#deleteSlots(org.springframework.data.redis.connection.RedisClusterNode, int[])
	 */
	@Override
	public void clusterDeleteSlots(RedisClusterNode node, final int... slots) {

		clusterCommandExecutor.executeCommandOnSingleNode(new LettuceClusterCommandCallback<String>() {

			@Override
			public String doInCluster(RedisClusterCommands<byte[], byte[]> client) {
				return client.clusterDelSlots(slots);
			}
		}, node);
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
	public void clusterForget(final RedisClusterNode node) {

		List<RedisClusterNode> nodes = new ArrayList<RedisClusterNode>(clusterGetNodes());
		final RedisClusterNode nodeToRemove = topologyProvider.getTopology().lookup(node);
		nodes.remove(nodeToRemove);

		this.clusterCommandExecutor.executeCommandAsyncOnNodes(new LettuceClusterCommandCallback<String>() {

			@Override
			public String doInCluster(RedisClusterCommands<byte[], byte[]> client) {
				return client.clusterForget(nodeToRemove.getId());
			}

		}, nodes);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterCommands#clusterMeet(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public void clusterMeet(final RedisClusterNode node) {

		Assert.notNull(node, "Cluster node must not be null for CLUSTER MEET command!");
		Assert.hasText(node.getHost(), "Node to meet cluster must have a host!");
		Assert.isTrue(node.getPort() > 0, "Node to meet cluster must have a port greater 0!");

		this.clusterCommandExecutor.executeCommandOnAllNodes(new LettuceClusterCommandCallback<String>() {

			@Override
			public String doInCluster(RedisClusterCommands<byte[], byte[]> client) {
				return client.clusterMeet(node.getHost(), node.getPort());
			}
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterCommands#clusterSetSlot(org.springframework.data.redis.connection.RedisClusterNode, int, org.springframework.data.redis.connection.RedisClusterCommands.AddSlots)
	 */
	@Override
	public void clusterSetSlot(final RedisClusterNode node, final int slot, final AddSlots mode) {

		Assert.notNull(node, "Node must not be null.");
		Assert.notNull(mode, "AddSlots mode must not be null.");

		final RedisClusterNode nodeToUse = topologyProvider.getTopology().lookup(node);
		final String nodeId = nodeToUse.getId();

		clusterCommandExecutor.executeCommandOnSingleNode(new LettuceClusterCommandCallback<String>() {

			@Override
			public String doInCluster(RedisClusterCommands<byte[], byte[]> client) {
				switch (mode) {
					case MIGRATING:
						return client.clusterSetSlotMigrating(slot, nodeId);
					case IMPORTING:
						return client.clusterSetSlotImporting(slot, nodeId);
					case NODE:
						return client.clusterSetSlotNode(slot, nodeId);
					case STABLE:
						return client.clusterSetSlotStable(slot);
					default:
						throw new InvalidDataAccessApiUsageException("Invalid import mode for cluster slot: " + slot);
				}
			}
		}, node);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterCommands#getKeysInSlot(int, java.lang.Integer)
	 */
	@Override
	public List<byte[]> clusterGetKeysInSlot(int slot, Integer count) {

		try {
			return getConnection().clusterGetKeysInSlot(slot, count);
		} catch (Exception ex) {
			throw exceptionConverter.translate(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterCommands#countKeys(int)
	 */
	@Override
	public Long clusterCountKeysInSlot(int slot) {

		try {
			return getConnection().clusterCountKeysInSlot(slot);
		} catch (Exception ex) {
			throw exceptionConverter.translate(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterCommands#clusterReplicate(org.springframework.data.redis.connection.RedisClusterNode, org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public void clusterReplicate(final RedisClusterNode master, RedisClusterNode slave) {

		final RedisClusterNode masterNode = topologyProvider.getTopology().lookup(master);
		clusterCommandExecutor.executeCommandOnSingleNode(new LettuceClusterCommandCallback<String>() {

			@Override
			public String doInCluster(RedisClusterCommands<byte[], byte[]> client) {
				return client.clusterReplicate(masterNode.getId());
			}
		}, slave);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnection#ping()
	 */
	@Override
	public String ping() {
		Collection<String> ping = clusterCommandExecutor
				.executeCommandOnAllNodes(new LettuceClusterCommandCallback<String>() {

					@Override
					public String doInCluster(RedisClusterCommands<byte[], byte[]> connection) {
						return connection.ping();
					}
				}).resultsAsList();

		for (String result : ping) {
			if (!ObjectUtils.nullSafeEquals("PONG", result)) {
				return "";
			}
		}

		return "PONG";
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterConnection#ping(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public String ping(RedisClusterNode node) {

		return clusterCommandExecutor.executeCommandOnSingleNode(new LettuceClusterCommandCallback<String>() {

			@Override
			public String doInCluster(RedisClusterCommands<byte[], byte[]> client) {
				return client.ping();
			}
		}, node).getValue();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterConnection#bgReWriteAof(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public void bgReWriteAof(RedisClusterNode node) {

		clusterCommandExecutor.executeCommandOnSingleNode(new LettuceClusterCommandCallback<String>() {

			@Override
			public String doInCluster(RedisClusterCommands<byte[], byte[]> client) {
				return client.bgrewriteaof();
			}
		}, node);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterConnection#bgSave(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public void bgSave(RedisClusterNode node) {

		clusterCommandExecutor.executeCommandOnSingleNode(new LettuceClusterCommandCallback<String>() {

			@Override
			public String doInCluster(RedisClusterCommands<byte[], byte[]> client) {
				return client.bgsave();
			}
		}, node);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterConnection#lastSave(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public Long lastSave(RedisClusterNode node) {

		return clusterCommandExecutor.executeCommandOnSingleNode(new LettuceClusterCommandCallback<Long>() {

			@Override
			public Long doInCluster(RedisClusterCommands<byte[], byte[]> client) {
				return client.lastsave().getTime();
			}
		}, node).getValue();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterConnection#save(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public void save(RedisClusterNode node) {

		clusterCommandExecutor.executeCommandOnSingleNode(new LettuceClusterCommandCallback<String>() {

			@Override
			public String doInCluster(RedisClusterCommands<byte[], byte[]> client) {
				return client.save();
			}
		}, node);

	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterConnection#dbSize(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public Long dbSize(RedisClusterNode node) {

		return clusterCommandExecutor.executeCommandOnSingleNode(new LettuceClusterCommandCallback<Long>() {

			@Override
			public Long doInCluster(RedisClusterCommands<byte[], byte[]> client) {
				return client.dbsize();
			}
		}, node).getValue();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterConnection#flushDb(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public void flushDb(RedisClusterNode node) {

		clusterCommandExecutor.executeCommandOnSingleNode(new LettuceClusterCommandCallback<String>() {

			@Override
			public String doInCluster(RedisClusterCommands<byte[], byte[]> client) {
				return client.flushdb();
			}
		}, node);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterConnection#flushAll(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public void flushAll(RedisClusterNode node) {

		clusterCommandExecutor.executeCommandOnSingleNode(new LettuceClusterCommandCallback<String>() {

			@Override
			public String doInCluster(RedisClusterCommands<byte[], byte[]> client) {
				return client.flushall();
			}
		}, node);

	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterConnection#info(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public Properties info(RedisClusterNode node) {

		return LettuceConverters
				.toProperties(clusterCommandExecutor.executeCommandOnSingleNode(new LettuceClusterCommandCallback<String>() {

					@Override
					public String doInCluster(RedisClusterCommands<byte[], byte[]> client) {
						return client.info();
					}
				}, node).getValue());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterConnection#keys(org.springframework.data.redis.connection.RedisClusterNode, byte[])
	 */
	@Override
	public Set<byte[]> keys(RedisClusterNode node, final byte[] pattern) {
		return doGetClusterKeyCommands().keys(node, pattern);
	}

	public byte[] randomKey(RedisClusterNode node) {
		return doGetClusterKeyCommands().randomKey(node);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterConnection#shutdown(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public void shutdown(RedisClusterNode node) {

		clusterCommandExecutor.executeCommandOnSingleNode(new LettuceClusterCommandCallback<Void>() {

			@Override
			public Void doInCluster(RedisClusterCommands<byte[], byte[]> client) {
				client.shutdown(true);
				return null;
			}
		}, node);
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
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnection#getAsyncDedicatedConnection()
	 */
	@Override
	protected StatefulConnection<byte[], byte[]> doGetAsyncDedicatedConnection() {
		return clusterClient.connect(CODEC);
	}

	// --> cluster node stuff

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterCommands#getClusterNodes()
	 */
	@Override
	public List<RedisClusterNode> clusterGetNodes() {
		return LettuceConverters.partitionsToClusterNodes(clusterClient.getPartitions());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnection#watch(byte[][])
	 */
	@Override
	public void watch(byte[]... keys) {
		throw new InvalidDataAccessApiUsageException("WATCH is currently not supported in cluster mode.");
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnection#unwatch()
	 */
	@Override
	public void unwatch() {
		throw new InvalidDataAccessApiUsageException("UNWATCH is currently not supported in cluster mode.");
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnection#multi()
	 */
	@Override
	public void multi() {
		throw new InvalidDataAccessApiUsageException("MULTI is currently not supported in cluster mode.");
	}

	/*
	 *
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnection#getConfig(java.lang.String)
	 */
	@Override
	public List<String> getConfig(final String pattern) {

		List<NodeResult<List<String>>> mapResult = clusterCommandExecutor
				.executeCommandOnAllNodes(new LettuceClusterCommandCallback<List<String>>() {

					@Override
					public List<String> doInCluster(RedisClusterCommands<byte[], byte[]> client) {
						return client.configGet(pattern);
					}
				}).getResults();

		List<String> result = new ArrayList<String>();
		for (NodeResult<List<String>> entry : mapResult) {

			String prefix = entry.getNode().asString();
			int i = 0;
			for (String value : entry.getValue()) {
				result.add((i++ % 2 == 0 ? (prefix + ".") : "") + value);
			}
		}

		return result;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterConnection#getConfig(org.springframework.data.redis.connection.RedisClusterNode, java.lang.String)
	 */
	@Override
	public List<String> getConfig(RedisClusterNode node, final String pattern) {

		return clusterCommandExecutor.executeCommandOnSingleNode(new LettuceClusterCommandCallback<List<String>>() {

			@Override
			public List<String> doInCluster(RedisClusterCommands<byte[], byte[]> client) {
				return client.configGet(pattern);
			}
		}, node).getValue();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnection#setConfig(java.lang.String, java.lang.String)
	 */
	@Override
	public void setConfig(final String param, final String value) {

		clusterCommandExecutor.executeCommandOnAllNodes(new LettuceClusterCommandCallback<String>() {

			@Override
			public String doInCluster(RedisClusterCommands<byte[], byte[]> client) {
				return client.configSet(param, value);
			}
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterConnection#setConfig(org.springframework.data.redis.connection.RedisClusterNode, java.lang.String, java.lang.String)
	 */
	@Override
	public void setConfig(RedisClusterNode node, final String param, final String value) {

		clusterCommandExecutor.executeCommandOnSingleNode(new LettuceClusterCommandCallback<String>() {

			@Override
			public String doInCluster(RedisClusterCommands<byte[], byte[]> client) {
				return client.configSet(param, value);
			}
		}, node);

	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnection#resetConfigStats()
	 */
	@Override
	public void resetConfigStats() {

		clusterCommandExecutor.executeCommandOnAllNodes(new LettuceClusterCommandCallback<String>() {

			@Override
			public String doInCluster(RedisClusterCommands<byte[], byte[]> client) {
				return client.configResetstat();
			}
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterConnection#resetConfigStats(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public void resetConfigStats(RedisClusterNode node) {

		clusterCommandExecutor.executeCommandOnSingleNode(new LettuceClusterCommandCallback<String>() {

			@Override
			public String doInCluster(RedisClusterCommands<byte[], byte[]> client) {
				return client.configResetstat();
			}
		}, node);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnection#time()
	 */
	@Override
	public Long time() {

		return convertListOfStringToTime(
				clusterCommandExecutor.executeCommandOnArbitraryNode(new LettuceClusterCommandCallback<List<byte[]>>() {

					@Override
					public List<byte[]> doInCluster(RedisClusterCommands<byte[], byte[]> client) {
						return client.time();
					}
				}).getValue());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterConnection#time(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public Long time(RedisClusterNode node) {

		return convertListOfStringToTime(
				clusterCommandExecutor.executeCommandOnSingleNode(new LettuceClusterCommandCallback<List<byte[]>>() {

					@Override
					public List<byte[]> doInCluster(RedisClusterCommands<byte[], byte[]> client) {
						return client.time();
					}
				}, node).getValue());
	}

	private Long convertListOfStringToTime(List<byte[]> serverTimeInformation) {

		Assert.notEmpty(serverTimeInformation, "Received invalid result from server. Expected 2 items in collection.");
		Assert.isTrue(serverTimeInformation.size() == 2,
				"Received invalid number of arguments from redis server. Expected 2 received " + serverTimeInformation.size());

		return Converters.toTimeMillis(LettuceConverters.toString(serverTimeInformation.get(0)),
				LettuceConverters.toString(serverTimeInformation.get(1)));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnection#getClientList()
	 */
	@Override
	public List<RedisClientInfo> getClientList() {

		List<String> map = clusterCommandExecutor.executeCommandOnAllNodes(new LettuceClusterCommandCallback<String>() {

			@Override
			public String doInCluster(RedisClusterCommands<byte[], byte[]> client) {
				return client.clientList();
			}
		}).resultsAsList();

		ArrayList<RedisClientInfo> result = new ArrayList<RedisClientInfo>();
		for (String infos : map) {
			result.addAll(LettuceConverters.toListOfRedisClientInformation(infos));
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterConnection#getClientList(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public List<RedisClientInfo> getClientList(RedisClusterNode node) {

		return LettuceConverters.toListOfRedisClientInformation(
				clusterCommandExecutor.executeCommandOnSingleNode(new LettuceClusterCommandCallback<String>() {

					@Override
					public String doInCluster(RedisClusterCommands<byte[], byte[]> client) {
						return client.clientList();
					}
				}, node).getValue());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterCommands#clusterGetMasterSlaveMap()
	 */
	@Override
	public Map<RedisClusterNode, Collection<RedisClusterNode>> clusterGetMasterSlaveMap() {

		List<NodeResult<Collection<RedisClusterNode>>> nodeResults = clusterCommandExecutor
				.executeCommandAsyncOnNodes(new LettuceClusterCommandCallback<Collection<RedisClusterNode>>() {

					@Override
					public Set<RedisClusterNode> doInCluster(RedisClusterCommands<byte[], byte[]> client) {
						return Converters.toSetOfRedisClusterNodes(client.clusterSlaves(client.clusterMyId()));
					}
				}, topologyProvider.getTopology().getActiveMasterNodes()).getResults();

		Map<RedisClusterNode, Collection<RedisClusterNode>> result = new LinkedHashMap<RedisClusterNode, Collection<RedisClusterNode>>();

		for (NodeResult<Collection<RedisClusterNode>> nodeResult : nodeResults) {
			result.put(nodeResult.getNode(), nodeResult.getValue());
		}

		return result;
	}

	public ClusterCommandExecutor getClusterCommandExecutor() {
		return clusterCommandExecutor;
	}

	/**
	 * Lettuce specific implementation of {@link ClusterCommandCallback}.
	 *
	 * @author Christoph Strobl
	 * @param <T>
	 * @since 1.7
	 */
	protected interface LettuceClusterCommandCallback<T>
			extends ClusterCommandCallback<RedisClusterCommands<byte[], byte[]>, T> {}

	/**
	 * Lettuce specific implementation of {@link MultiKeyClusterCommandCallback}.
	 *
	 * @author Christoph Strobl
	 * @param <T>
	 * @since 1.7
	 */
	protected interface LettuceMultiKeyClusterCommandCallback<T>
			extends MultiKeyClusterCommandCallback<RedisClusterCommands<byte[], byte[]>, T> {

	}

	/**
	 * Lettuce specific implementation of {@link ClusterNodeResourceProvider}.
	 *
	 * @author Christoph Strobl
	 * @since 1.7
	 */
	static class LettuceClusterNodeResourceProvider implements ClusterNodeResourceProvider, DisposableBean {

		private final RedisClusterClient client;
		private volatile StatefulRedisClusterConnection<byte[], byte[]> connection;

		public LettuceClusterNodeResourceProvider(RedisClusterClient client) {

			this.client = client;
		}

		@Override
		@SuppressWarnings("unchecked")
		public RedisClusterCommands<byte[], byte[]> getResourceForSpecificNode(RedisClusterNode node) {

			Assert.notNull(node, "Node must not be null!");

			if (connection == null) {
				synchronized (this) {
					if (connection == null) {
						this.connection = client.connect(CODEC);
					}
				}
			}

			try {
				return connection.getConnection(node.getHost(), node.getPort()).sync();
			} catch (RedisException e) {

				// unwrap cause when cluster node not known in cluster
				if (e.getCause() instanceof IllegalArgumentException) {
					throw (IllegalArgumentException) e.getCause();
				}
				throw e;
			}
		}

		@Override
		@SuppressWarnings("unchecked")
		public void returnResourceForSpecificNode(RedisClusterNode node, Object resource) {}

		@Override
		public void destroy() throws Exception {
			if (connection != null) {
				connection.close();
			}
		}
	}

	/**
	 * Lettuce specific implementation of {@link ClusterTopologyProvider}.
	 *
	 * @author Christoph Strobl
	 * @since 1.7
	 */
	static class LettuceClusterTopologyProvider implements ClusterTopologyProvider {

		private final RedisClusterClient client;

		/**
		 * @param client must not be {@literal null}.
		 */
		public LettuceClusterTopologyProvider(RedisClusterClient client) {

			Assert.notNull(client, "RedisClusterClient must not be null.");
			this.client = client;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.connection.ClusterTopologyProvider#getTopology()
		 */
		@Override
		public ClusterTopology getTopology() {
			return new ClusterTopology(
					new LinkedHashSet<RedisClusterNode>(LettuceConverters.partitionsToClusterNodes(client.getPartitions())));
		}
	}

}
