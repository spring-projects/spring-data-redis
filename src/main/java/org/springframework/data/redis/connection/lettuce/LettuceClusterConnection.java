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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.DirectFieldAccessor;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.ExceptionTranslationStrategy;
import org.springframework.data.redis.PassThroughExceptionTranslationStrategy;
import org.springframework.data.redis.connection.ClusterCommandExecutor;
import org.springframework.data.redis.connection.ClusterCommandExecutor.ClusterCommandCallback;
import org.springframework.data.redis.connection.ClusterCommandExecutor.MultiKeyClusterCommandCallback;
import org.springframework.data.redis.connection.ClusterCommandExecutor.NodeResult;
import org.springframework.data.redis.connection.ClusterInfo;
import org.springframework.data.redis.connection.ClusterNodeResourceProvider;
import org.springframework.data.redis.connection.ClusterSlotHashUtil;
import org.springframework.data.redis.connection.ClusterTopology;
import org.springframework.data.redis.connection.ClusterTopologyProvider;
import org.springframework.data.redis.connection.RedisClusterNode;
import org.springframework.data.redis.connection.RedisClusterNode.SlotRange;
import org.springframework.data.redis.connection.SortParameters;
import org.springframework.data.redis.connection.convert.Converters;
import org.springframework.data.redis.connection.util.ByteArraySet;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.types.RedisClientInfo;
import org.springframework.data.redis.util.ByteUtils;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;

import com.lambdaworks.redis.KeyValue;
import com.lambdaworks.redis.RedisException;
import com.lambdaworks.redis.RedisURI;
import com.lambdaworks.redis.api.StatefulConnection;
import com.lambdaworks.redis.cluster.RedisClusterClient;
import com.lambdaworks.redis.cluster.SlotHash;
import com.lambdaworks.redis.cluster.api.StatefulRedisClusterConnection;
import com.lambdaworks.redis.cluster.api.sync.RedisClusterCommands;
import com.lambdaworks.redis.cluster.models.partitions.Partitions;
import com.lambdaworks.redis.codec.ByteArrayCodec;
import com.lambdaworks.redis.codec.RedisCodec;

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

	private final Log log = LogFactory.getLog(getClass());

	private final RedisClusterClient clusterClient;
	private ClusterCommandExecutor clusterCommandExecutor;
	private ClusterTopologyProvider topologyProvider;
	private boolean disposeClusterCommandExecutorOnClose;

	/**
	 * Creates new {@link LettuceClusterConnection} using {@link RedisClusterClient} with default
	 * {@link RedisURI#DEFAULT_TIMEOUT timeout} and a fresh {@link ClusterCommandExecutor} that gets destroyed on close.
	 *
	 * @param clusterClient must not be {@literal null}.
	 */
	public LettuceClusterConnection(RedisClusterClient clusterClient) {

		this(clusterClient, RedisURI.DEFAULT_TIMEOUT,
				new ClusterCommandExecutor(new LettuceClusterTopologyProvider(clusterClient),
						new LettuceClusterNodeResourceProvider(clusterClient), exceptionConverter));

		this.disposeClusterCommandExecutorOnClose = true;
	}

	/**
	 * Creates new {@link LettuceClusterConnection} with default {@link RedisURI#DEFAULT_TIMEOUT timeout} using
	 * {@link RedisClusterClient} running commands across the cluster via given {@link ClusterCommandExecutor}.
	 *
	 * @param clusterClient must not be {@literal null}.
	 * @param executor must not be {@literal null}.
	 */
	public LettuceClusterConnection(RedisClusterClient clusterClient, ClusterCommandExecutor executor) {
		this(clusterClient, RedisURI.DEFAULT_TIMEOUT, executor);
	}

	/**
	 * Creates new {@link LettuceClusterConnection} with given command {@code timeout} using {@link RedisClusterClient}
	 * running commands across the cluster via given {@link ClusterCommandExecutor}.
	 *
	 * @param clusterClient must not be {@literal null}.
	 * @param timeout command timeout (in milliseconds)
	 * @param executor must not be {@literal null}.
	 * @since 1.8.7
	 */
	public LettuceClusterConnection(RedisClusterClient clusterClient, long timeout, ClusterCommandExecutor executor) {

		super(null, timeout, clusterClient, null, 0);

		Assert.notNull(clusterClient, "RedisClusterClient must not be null.");
		Assert.notNull(executor, "ClusterCommandExecutor must not be null.");

		this.clusterClient = clusterClient;
		this.topologyProvider = new LettuceClusterTopologyProvider(clusterClient);
		this.clusterCommandExecutor = executor;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnection#scan(long, org.springframework.data.redis.core.ScanOptions)
	 */
	@Override
	public Cursor<byte[]> scan(long cursorId, ScanOptions options) {
		throw new InvalidDataAccessApiUsageException("Scan is not supported across multiple nodes within a cluster.");
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnection#keys(byte[])
	 */
	@Override
	public Set<byte[]> keys(final byte[] pattern) {

		Assert.notNull(pattern, "Pattern must not be null!");

		Collection<List<byte[]>> keysPerNode = clusterCommandExecutor
				.executeCommandOnAllNodes(new LettuceClusterCommandCallback<List<byte[]>>() {

					@Override
					public List<byte[]> doInCluster(RedisClusterCommands<byte[], byte[]> connection) {
						return connection.keys(pattern);
					}
				}).resultsAsList();

		Set<byte[]> keys = new HashSet<byte[]>();

		for (List<byte[]> keySet : keysPerNode) {
			keys.addAll(keySet);
		}
		return keys;
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
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnection#move(byte[], int)
	 */
	@Override
	public Boolean move(byte[] key, int dbIndex) {
		throw new UnsupportedOperationException("MOVE not supported in CLUSTER mode!");
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnection#del(byte[][])
	 */
	@Override
	public Long del(byte[]... keys) {

		Assert.noNullElements(keys, "Keys must not be null or contain null key!");

		// Routing for mget is handled by lettuce.
		return super.del(keys);
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

		return LettuceConverters.toBytesSet(
				clusterCommandExecutor.executeCommandOnSingleNode(new LettuceClusterCommandCallback<List<byte[]>>() {

					@Override
					public List<byte[]> doInCluster(RedisClusterCommands<byte[], byte[]> client) {
						return client.keys(pattern);
					}
				}, node).getValue());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterConnection#randomKey(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public byte[] randomKey(RedisClusterNode node) {

		return clusterCommandExecutor.executeCommandOnSingleNode(new LettuceClusterCommandCallback<byte[]>() {

			@Override
			public byte[] doInCluster(RedisClusterCommands<byte[], byte[]> client) {
				return client.randomkey();
			}
		}, node).getValue();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnection#randomKey()
	 */
	@Override
	public byte[] randomKey() {

		List<RedisClusterNode> nodes = clusterGetNodes();
		Set<RedisClusterNode> inspectedNodes = new HashSet<RedisClusterNode>(nodes.size());

		do {

			RedisClusterNode node = nodes.get(new Random().nextInt(nodes.size()));

			while (inspectedNodes.contains(node)) {
				node = nodes.get(new Random().nextInt(nodes.size()));
			}
			inspectedNodes.add(node);
			byte[] key = randomKey(node);

			if (key != null && key.length > 0) {
				return key;
			}
		} while (nodes.size() != inspectedNodes.size());

		return null;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnection#rename(byte[], byte[])
	 */
	@Override
	public void rename(byte[] oldName, byte[] newName) {

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(oldName, newName)) {
			super.rename(oldName, newName);
			return;
		}

		byte[] value = dump(oldName);

		if (value != null && value.length > 0) {

			restore(newName, 0, value);
			del(oldName);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnection#renameNX(byte[], byte[])
	 */
	@Override
	public Boolean renameNX(byte[] oldName, byte[] newName) {

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(oldName, newName)) {
			return super.renameNX(oldName, newName);
		}

		byte[] value = dump(oldName);

		if (value != null && value.length > 0 && !exists(newName)) {

			restore(newName, 0, value);
			del(oldName);
			return Boolean.TRUE;
		}
		return Boolean.FALSE;
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
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnection#sort(byte[], org.springframework.data.redis.connection.SortParameters, byte[])
	 */
	@Override
	public Long sort(byte[] key, SortParameters params, byte[] storeKey) {

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(key, storeKey)) {
			return super.sort(key, params, storeKey);
		}

		List<byte[]> sorted = sort(key, params);
		if (!CollectionUtils.isEmpty(sorted)) {

			byte[][] arr = new byte[sorted.size()][];
			switch (type(key)) {

				case SET:
					sAdd(storeKey, sorted.toArray(arr));
					return 1L;
				case LIST:
					lPush(storeKey, sorted.toArray(arr));
					return 1L;
				default:
					throw new IllegalArgumentException("sort and store is only supported for SET and LIST");
			}
		}
		return 0L;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnection#mGet(byte[][])
	 */
	@Override
	public List<byte[]> mGet(byte[]... keys) {

		Assert.notNull(keys, "Keys must not be null!");

		// Routing for mget is handled by lettuce.
		return super.mGet(keys);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnection#mSet(java.util.Map)
	 */
	@Override
	public void mSet(Map<byte[], byte[]> tuples) {

		Assert.notNull(tuples, "Tuples must not be null!");

		// Routing for mset is handled by lettuce.
		super.mSet(tuples);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnection#mSetNX(java.util.Map)
	 */
	@Override
	public Boolean mSetNX(Map<byte[], byte[]> tuples) {

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(tuples.keySet().toArray(new byte[tuples.keySet().size()][]))) {
			return super.mSetNX(tuples);
		}

		boolean result = true;
		for (Map.Entry<byte[], byte[]> entry : tuples.entrySet()) {
			if (!setNX(entry.getKey(), entry.getValue()) && result) {
				result = false;
			}
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnection#bLPop(int, byte[][])
	 */
	@Override
	public List<byte[]> bLPop(final int timeout, byte[]... keys) {

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(keys)) {
			return super.bLPop(timeout, keys);
		}

		List<KeyValue<byte[], byte[]>> resultList = this.clusterCommandExecutor
				.executeMuliKeyCommand(new LettuceMultiKeyClusterCommandCallback<KeyValue<byte[], byte[]>>() {

					@Override
					public KeyValue<byte[], byte[]> doInCluster(RedisClusterCommands<byte[], byte[]> client, byte[] key) {
						return client.blpop(timeout, key);
					}
				}, Arrays.asList(keys)).resultsAsList();

		for (KeyValue<byte[], byte[]> kv : resultList) {
			if (kv != null) {
				return LettuceConverters.toBytesList(kv);
			}
		}

		return Collections.emptyList();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnection#bRPop(int, byte[][])
	 */
	@Override
	public List<byte[]> bRPop(final int timeout, byte[]... keys) {

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(keys)) {
			return super.bRPop(timeout, keys);
		}

		List<KeyValue<byte[], byte[]>> resultList = this.clusterCommandExecutor
				.executeMuliKeyCommand(new LettuceMultiKeyClusterCommandCallback<KeyValue<byte[], byte[]>>() {

					@Override
					public KeyValue<byte[], byte[]> doInCluster(RedisClusterCommands<byte[], byte[]> client, byte[] key) {
						return client.brpop(timeout, key);
					}
				}, Arrays.asList(keys)).resultsAsList();

		for (KeyValue<byte[], byte[]> kv : resultList) {
			if (kv != null) {
				return LettuceConverters.toBytesList(kv);
			}
		}

		return Collections.emptyList();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnection#rPopLPush(byte[], byte[])
	 */
	@Override
	public byte[] rPopLPush(byte[] srcKey, byte[] dstKey) {

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(srcKey, dstKey)) {
			return super.rPopLPush(srcKey, dstKey);
		}

		byte[] val = rPop(srcKey);
		lPush(dstKey, val);
		return val;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnection#bRPopLPush(int, byte[], byte[])
	 */
	@Override
	public byte[] bRPopLPush(int timeout, byte[] srcKey, byte[] dstKey) {

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(srcKey, dstKey)) {
			return super.bRPopLPush(timeout, srcKey, dstKey);
		}

		List<byte[]> val = bRPop(timeout, srcKey);
		if (!CollectionUtils.isEmpty(val)) {
			lPush(dstKey, val.get(1));
			return val.get(1);
		}

		return null;
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
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnection#sMove(byte[], byte[], byte[])
	 */
	@Override
	public Boolean sMove(byte[] srcKey, byte[] destKey, byte[] value) {

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(srcKey, destKey)) {
			return super.sMove(srcKey, destKey, value);
		}

		if (exists(srcKey)) {
			if (sRem(srcKey, value) > 0 && !sIsMember(destKey, value)) {
				return LettuceConverters.toBoolean(sAdd(destKey, value));
			}
		}
		return Boolean.FALSE;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnection#sInter(byte[][])
	 */
	@Override
	public Set<byte[]> sInter(byte[]... keys) {

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(keys)) {
			return super.sInter(keys);
		}

		Collection<Set<byte[]>> nodeResult = this.clusterCommandExecutor
				.executeMuliKeyCommand(new LettuceMultiKeyClusterCommandCallback<Set<byte[]>>() {

					@Override
					public Set<byte[]> doInCluster(RedisClusterCommands<byte[], byte[]> client, byte[] key) {
						return client.smembers(key);
					}
				}, Arrays.asList(keys)).resultsAsList();

		ByteArraySet result = null;
		for (Set<byte[]> entry : nodeResult) {

			ByteArraySet tmp = new ByteArraySet(entry);
			if (result == null) {
				result = tmp;
			} else {
				result.retainAll(tmp);
				if (result.isEmpty()) {
					break;
				}
			}
		}

		if (result.isEmpty()) {
			return Collections.emptySet();
		}

		return result.asRawSet();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnection#sInterStore(byte[], byte[][])
	 */
	@Override
	public Long sInterStore(byte[] destKey, byte[]... keys) {

		byte[][] allKeys = ByteUtils.mergeArrays(destKey, keys);

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(allKeys)) {
			return super.sInterStore(destKey, keys);
		}

		Set<byte[]> result = sInter(keys);
		if (result.isEmpty()) {
			return 0L;
		}
		return sAdd(destKey, result.toArray(new byte[result.size()][]));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnection#sUnion(byte[][])
	 */
	@Override
	public Set<byte[]> sUnion(byte[]... keys) {

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(keys)) {
			return super.sUnion(keys);
		}

		Collection<Set<byte[]>> nodeResult = this.clusterCommandExecutor
				.executeMuliKeyCommand(new LettuceMultiKeyClusterCommandCallback<Set<byte[]>>() {

					@Override
					public Set<byte[]> doInCluster(RedisClusterCommands<byte[], byte[]> client, byte[] key) {
						return client.smembers(key);
					}
				}, Arrays.asList(keys)).resultsAsList();

		ByteArraySet result = new ByteArraySet();
		for (Set<byte[]> entry : nodeResult) {
			result.addAll(entry);
		}

		if (result.isEmpty()) {
			return Collections.emptySet();
		}

		return result.asRawSet();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnection#sUnionStore(byte[], byte[][])
	 */
	@Override
	public Long sUnionStore(byte[] destKey, byte[]... keys) {

		byte[][] allKeys = ByteUtils.mergeArrays(destKey, keys);

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(allKeys)) {
			return super.sUnionStore(destKey, keys);
		}

		Set<byte[]> result = sUnion(keys);
		if (result.isEmpty()) {
			return 0L;
		}
		return sAdd(destKey, result.toArray(new byte[result.size()][]));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnection#sDiff(byte[][])
	 */
	@Override
	public Set<byte[]> sDiff(byte[]... keys) {

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(keys)) {
			return super.sDiff(keys);
		}

		byte[] source = keys[0];
		byte[][] others = Arrays.copyOfRange(keys, 1, keys.length);

		ByteArraySet values = new ByteArraySet(sMembers(source));
		Collection<Set<byte[]>> nodeResult = clusterCommandExecutor
				.executeMuliKeyCommand(new LettuceMultiKeyClusterCommandCallback<Set<byte[]>>() {

					@Override
					public Set<byte[]> doInCluster(RedisClusterCommands<byte[], byte[]> client, byte[] key) {
						return client.smembers(key);
					}
				}, Arrays.asList(others)).resultsAsList();

		if (values.isEmpty()) {
			return Collections.emptySet();
		}

		for (Set<byte[]> toSubstract : nodeResult) {
			values.removeAll(toSubstract);
		}

		return values.asRawSet();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnection#sDiffStore(byte[], byte[][])
	 */
	@Override
	public Long sDiffStore(byte[] destKey, byte[]... keys) {

		byte[][] allKeys = ByteUtils.mergeArrays(destKey, keys);

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(allKeys)) {
			return super.sDiffStore(destKey, keys);
		}

		Set<byte[]> diff = sDiff(keys);
		if (diff.isEmpty()) {
			return 0L;
		}

		return sAdd(destKey, diff.toArray(new byte[diff.size()][]));
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
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnection#pfCount(byte[][])
	 */
	@Override
	public Long pfCount(byte[]... keys) {

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(keys)) {

			try {
				return super.pfCount(keys);
			} catch (Exception ex) {
				throw convertLettuceAccessException(ex);
			}

		}
		throw new InvalidDataAccessApiUsageException("All keys must map to same slot for pfcount in cluster mode.");
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnection#pfMerge(byte[], byte[][])
	 */
	@Override
	public void pfMerge(byte[] destinationKey, byte[]... sourceKeys) {

		byte[][] allKeys = ByteUtils.mergeArrays(destinationKey, sourceKeys);

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(allKeys)) {
			try {
				super.pfMerge(destinationKey, sourceKeys);
				return;
			} catch (Exception ex) {
				throw convertLettuceAccessException(ex);
			}

		}
		throw new InvalidDataAccessApiUsageException("All keys must map to same slot for pfmerge in cluster mode.");
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

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConnection#close()
	 */
	@Override
	public void close() throws DataAccessException {

		if (!isClosed() && disposeClusterCommandExecutorOnClose) {
			try {
				clusterCommandExecutor.destroy();
			} catch (Exception ex) {
				log.warn("Cannot properly close cluster command executor", ex);
			}
		}

		super.close();
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
