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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.DirectFieldAccessor;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.geo.Circle;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.geo.Metric;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.ClusterStateFailureException;
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
import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.RedisClusterConnection;
import org.springframework.data.redis.connection.RedisClusterNode;
import org.springframework.data.redis.connection.RedisClusterNode.SlotRange;
import org.springframework.data.redis.connection.RedisNode;
import org.springframework.data.redis.connection.RedisPipelineException;
import org.springframework.data.redis.connection.RedisSentinelConnection;
import org.springframework.data.redis.connection.RedisSubscribedConnectionException;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.connection.SortParameters;
import org.springframework.data.redis.connection.Subscription;
import org.springframework.data.redis.connection.convert.Converters;
import org.springframework.data.redis.connection.util.ByteArraySet;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanCursor;
import org.springframework.data.redis.core.ScanIteration;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.data.redis.core.types.RedisClientInfo;
import org.springframework.data.redis.util.ByteUtils;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;

import redis.clients.jedis.BinaryJedisPubSub;
import redis.clients.jedis.GeoCoordinate;
import redis.clients.jedis.GeoUnit;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ZParams;
import redis.clients.jedis.params.geo.GeoRadiusParam;

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
public class JedisClusterConnection implements RedisClusterConnection {

	private static final ExceptionTranslationStrategy EXCEPTION_TRANSLATION = new PassThroughExceptionTranslationStrategy(
			JedisConverters.exceptionConverter());

	private final JedisCluster cluster;

	private boolean closed;

	private final JedisClusterTopologyProvider topologyProvider;
	private ClusterCommandExecutor clusterCommandExecutor;

	private volatile JedisSubscription subscription;

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
		clusterCommandExecutor = new ClusterCommandExecutor(topologyProvider, new JedisClusterNodeResourceProvider(cluster),
				EXCEPTION_TRANSLATION);

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
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisCommands#execute(java.lang.String, byte[][])
	 */
	@Override
	public Object execute(String command, byte[]... args) {

		// TODO: execute command on all nodes? or throw exception requiring to execute command on a specific node
		throw new UnsupportedOperationException("Execute is currently not supported in cluster mode.");
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisKeyCommands#del(byte[][])
	 */
	@Override
	public Long del(byte[]... keys) {

		Assert.noNullElements(keys, "Keys must not be null or contain null key!");

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(keys)) {
			try {
				return cluster.del(keys);
			} catch (Exception ex) {
				throw convertJedisAccessException(ex);
			}
		}

		return Long
				.valueOf(this.clusterCommandExecutor.executeMuliKeyCommand(new JedisMultiKeyClusterCommandCallback<Long>() {

					@Override
					public Long doInCluster(Jedis client, byte[] key) {
						return client.del(key);
					}
				}, Arrays.asList(keys)).resultsAsList().size());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisKeyCommands#type(byte[])
	 */
	@Override
	public DataType type(byte[] key) {

		try {
			return JedisConverters.toDataType(cluster.type(key));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisKeyCommands#keys(byte[])
	 */
	@Override
	public Set<byte[]> keys(final byte[] pattern) {

		Assert.notNull(pattern, "Pattern must not be null!");

		Collection<Set<byte[]>> keysPerNode = clusterCommandExecutor
				.executeCommandOnAllNodes(new JedisClusterCommandCallback<Set<byte[]>>() {

					@Override
					public Set<byte[]> doInCluster(Jedis client) {
						return client.keys(pattern);
					}
				}).resultsAsList();

		Set<byte[]> keys = new HashSet<byte[]>();
		for (Set<byte[]> keySet : keysPerNode) {
			keys.addAll(keySet);
		}
		return keys;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterConnection#keys(org.springframework.data.redis.connection.RedisClusterNode, byte[])
	 */
	@Override
	public Set<byte[]> keys(RedisClusterNode node, final byte[] pattern) {

		Assert.notNull(pattern, "Pattern must not be null!");

		return clusterCommandExecutor.executeCommandOnSingleNode(new JedisClusterCommandCallback<Set<byte[]>>() {

			@Override
			public Set<byte[]> doInCluster(Jedis client) {
				return client.keys(pattern);
			}
		}, node).getValue();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisKeyCommands#scan(org.springframework.data.redis.core.ScanOptions)
	 */
	@Override
	public Cursor<byte[]> scan(ScanOptions options) {
		throw new InvalidDataAccessApiUsageException("Scan is not supported accros multiple nodes within a cluster");
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisKeyCommands#randomKey()
	 */
	@Override
	public byte[] randomKey() {

		List<RedisClusterNode> nodes = new ArrayList<RedisClusterNode>(
				topologyProvider.getTopology().getActiveMasterNodes());
		Set<RedisNode> inspectedNodes = new HashSet<RedisNode>(nodes.size());

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
	 * @see org.springframework.data.redis.connection.RedisClusterConnection#randomKey(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public byte[] randomKey(RedisClusterNode node) {

		return clusterCommandExecutor.executeCommandOnSingleNode(new JedisClusterCommandCallback<byte[]>() {

			@Override
			public byte[] doInCluster(Jedis client) {
				return client.randomBinaryKey();
			}
		}, node).getValue();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisKeyCommands#rename(byte[], byte[])
	 */
	@Override
	public void rename(final byte[] sourceKey, final byte[] targetKey) {

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(sourceKey, targetKey)) {

			try {
				cluster.rename(sourceKey, targetKey);
				return;
			} catch (Exception ex) {
				throw convertJedisAccessException(ex);
			}
		}

		byte[] value = dump(sourceKey);

		if (value != null && value.length > 0) {

			restore(targetKey, 0, value);
			del(sourceKey);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisKeyCommands#renameNX(byte[], byte[])
	 */
	@Override
	public Boolean renameNX(final byte[] sourceKey, final byte[] targetKey) {

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(sourceKey, targetKey)) {

			try {
				return JedisConverters.toBoolean(cluster.renamenx(sourceKey, targetKey));
			} catch (Exception ex) {
				throw convertJedisAccessException(ex);
			}
		}

		byte[] value = dump(sourceKey);

		if (value != null && value.length > 0 && !exists(targetKey)) {

			restore(targetKey, 0, value);
			del(sourceKey);
			return Boolean.TRUE;
		}
		return Boolean.FALSE;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisKeyCommands#expire(byte[], long)
	 */
	@Override
	public Boolean expire(byte[] key, long seconds) {

		if (seconds > Integer.MAX_VALUE) {
			throw new UnsupportedOperationException("Jedis does not support seconds exceeding Integer.MAX_VALUE.");
		}
		try {
			return JedisConverters.toBoolean(cluster.expire(key, Long.valueOf(seconds).intValue()));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisKeyCommands#pExpire(byte[], long)
	 */
	@Override
	public Boolean pExpire(final byte[] key, final long millis) {

		try {
			return JedisConverters.toBoolean(cluster.pexpire(key, millis));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisKeyCommands#expireAt(byte[], long)
	 */
	@Override
	public Boolean expireAt(byte[] key, long unixTime) {

		try {
			return JedisConverters.toBoolean(cluster.expireAt(key, unixTime));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisKeyCommands#pExpireAt(byte[], long)
	 */
	@Override
	public Boolean pExpireAt(byte[] key, long unixTimeInMillis) {

		try {
			return JedisConverters.toBoolean(cluster.pexpireAt(key, unixTimeInMillis));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisKeyCommands#persist(byte[])
	 */
	@Override
	public Boolean persist(byte[] key) {

		try {
			return JedisConverters.toBoolean(cluster.persist(key));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisKeyCommands#move(byte[], int)
	 */
	@Override
	public Boolean move(byte[] key, int dbIndex) {
		throw new UnsupportedOperationException("Cluster mode does not allow moving keys.");
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisKeyCommands#ttl(byte[])
	 */
	@Override
	public Long ttl(byte[] key) {

		try {
			return cluster.ttl(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}
	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisKeyCommands#ttl(byte[], java.util.concurrent.TimeUnit)
	 */
	@Override
	public Long ttl(byte[] key, TimeUnit timeUnit) {

		try {
			return Converters.secondsToTimeUnit(cluster.ttl(key), timeUnit);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisKeyCommands#pTtl(byte[])
	 */
	@Override
	public Long pTtl(final byte[] key) {

		return clusterCommandExecutor.executeCommandOnSingleNode(new JedisClusterCommandCallback<Long>() {

			@Override
			public Long doInCluster(Jedis client) {
				return client.pttl(key);
			}
		}, topologyProvider.getTopology().getKeyServingMasterNode(key)).getValue();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisKeyCommands#pTtl(byte[], java.util.concurrent.TimeUnit)
	 */
	@Override
	public Long pTtl(final byte[] key, final TimeUnit timeUnit) {

		return clusterCommandExecutor.executeCommandOnSingleNode(new JedisClusterCommandCallback<Long>() {

			@Override
			public Long doInCluster(Jedis client) {
				return Converters.millisecondsToTimeUnit(client.pttl(key), timeUnit);
			}
		}, topologyProvider.getTopology().getKeyServingMasterNode(key)).getValue();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisKeyCommands#sort(byte[], org.springframework.data.redis.connection.SortParameters)
	 */
	@Override
	public List<byte[]> sort(byte[] key, SortParameters params) {

		try {
			return cluster.sort(key, JedisConverters.toSortingParams(params));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisKeyCommands#sort(byte[], org.springframework.data.redis.connection.SortParameters, byte[])
	 */
	@Override
	public Long sort(byte[] key, SortParameters params, byte[] storeKey) {

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
	 * @see org.springframework.data.redis.connection.RedisKeyCommands#dump(byte[])
	 */
	@Override
	public byte[] dump(final byte[] key) {

		return clusterCommandExecutor.executeCommandOnSingleNode(new JedisClusterCommandCallback<byte[]>() {

			@Override
			public byte[] doInCluster(Jedis client) {
				return client.dump(key);
			}
		}, topologyProvider.getTopology().getKeyServingMasterNode(key)).getValue();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisKeyCommands#restore(byte[], long, byte[])
	 */
	@Override
	public void restore(final byte[] key, final long ttlInMillis, final byte[] serializedValue) {

		if (ttlInMillis > Integer.MAX_VALUE) {
			throw new UnsupportedOperationException("Jedis does not support ttlInMillis exceeding Integer.MAX_VALUE.");
		}

		this.clusterCommandExecutor.executeCommandOnSingleNode(new JedisClusterCommandCallback<String>() {

			@Override
			public String doInCluster(Jedis client) {
				return client.restore(key, Long.valueOf(ttlInMillis).intValue(), serializedValue);
			}
		}, clusterGetNodeForKey(key));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStringCommands#get(byte[])
	 */
	@Override
	public byte[] get(byte[] key) {

		try {
			return cluster.get(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStringCommands#getSet(byte[], byte[])
	 */
	@Override
	public byte[] getSet(byte[] key, byte[] value) {

		try {
			return cluster.getSet(key, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStringCommands#mGet(byte[][])
	 */
	@Override
	public List<byte[]> mGet(byte[]... keys) {

		Assert.noNullElements(keys, "Keys must not contain null elements!");

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(keys)) {
			return cluster.mget(keys);
		}

		return this.clusterCommandExecutor.executeMuliKeyCommand(new JedisMultiKeyClusterCommandCallback<byte[]>() {

			@Override
			public byte[] doInCluster(Jedis client, byte[] key) {
				return client.get(key);
			}
		}, Arrays.asList(keys)).resultsAsListSortBy(keys);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStringCommands#set(byte[], byte[])
	 */
	@Override
	public void set(byte[] key, byte[] value) {

		try {
			cluster.set(key, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStringCommands#set(byte[], byte[], org.springframework.data.redis.core.types.Expiration, org.springframework.data.redis.connection.RedisStringCommands.SetOptions)
	 */
	@Override
	public void set(byte[] key, byte[] value, Expiration expiration, SetOption option) {

		if (expiration == null || expiration.isPersistent()) {

			if (option == null || ObjectUtils.nullSafeEquals(SetOption.UPSERT, option)) {
				set(key, value);
			} else {

				// BinaryCluster does not support set with nxxx and binary key/value pairs.
				if (ObjectUtils.nullSafeEquals(SetOption.SET_IF_PRESENT, option)) {
					throw new UnsupportedOperationException("Jedis does not support SET XX without PX or EX on BinaryCluster.");
				}

				setNX(key, value);
			}
		} else {

			if (option == null || ObjectUtils.nullSafeEquals(SetOption.UPSERT, option)) {

				if (ObjectUtils.nullSafeEquals(TimeUnit.MILLISECONDS, expiration.getTimeUnit())) {
					pSetEx(key, expiration.getExpirationTime(), value);
				} else {
					setEx(key, expiration.getExpirationTime(), value);
				}
			} else {

				byte[] nxxx = JedisConverters.toSetCommandNxXxArgument(option);
				byte[] expx = JedisConverters.toSetCommandExPxArgument(expiration);

				try {
					cluster.set(key, value, nxxx, expx, expiration.getExpirationTime());
				} catch (Exception ex) {
					throw convertJedisAccessException(ex);
				}
			}
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStringCommands#setNX(byte[], byte[])
	 */
	@Override
	public Boolean setNX(byte[] key, byte[] value) {

		try {
			return JedisConverters.toBoolean(cluster.setnx(key, value));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStringCommands#setEx(byte[], long, byte[])
	 */
	@Override
	public void setEx(byte[] key, long seconds, byte[] value) {

		if (seconds > Integer.MAX_VALUE) {
			throw new IllegalArgumentException("Seconds have cannot exceed Integer.MAX_VALUE!");
		}

		try {
			cluster.setex(key, Long.valueOf(seconds).intValue(), value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStringCommands#pSetEx(byte[], long, byte[])
	 */
	@Override
	public void pSetEx(final byte[] key, final long milliseconds, final byte[] value) {

		if (milliseconds > Integer.MAX_VALUE) {
			throw new IllegalArgumentException("Milliseconds have cannot exceed Integer.MAX_VALUE!");
		}

		this.clusterCommandExecutor.executeCommandOnSingleNode(new JedisClusterCommandCallback<String>() {

			@Override
			public String doInCluster(Jedis client) {
				return client.psetex(key, milliseconds, value);
			}
		}, topologyProvider.getTopology().getKeyServingMasterNode(key));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStringCommands#mSet(java.util.Map)
	 */
	@Override
	public void mSet(Map<byte[], byte[]> tuples) {

		Assert.notNull(tuples, "Tuples must not be null!");

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(tuples.keySet().toArray(new byte[tuples.keySet().size()][]))) {
			try {
				cluster.mset(JedisConverters.toByteArrays(tuples));
				return;
			} catch (Exception ex) {
				throw convertJedisAccessException(ex);
			}
		}

		for (Map.Entry<byte[], byte[]> entry : tuples.entrySet()) {
			set(entry.getKey(), entry.getValue());
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStringCommands#mSetNX(java.util.Map)
	 */
	@Override
	public Boolean mSetNX(Map<byte[], byte[]> tuples) {

		Assert.notNull(tuples, "Tuple must not be null!");

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(tuples.keySet().toArray(new byte[tuples.keySet().size()][]))) {
			try {
				return JedisConverters.toBoolean(cluster.msetnx(JedisConverters.toByteArrays(tuples)));
			} catch (Exception ex) {
				throw convertJedisAccessException(ex);
			}
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
	 * @see org.springframework.data.redis.connection.RedisStringCommands#incr(byte[])
	 */
	@Override
	public Long incr(byte[] key) {

		try {
			return cluster.incr(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStringCommands#incrBy(byte[], long)
	 */
	@Override
	public Long incrBy(byte[] key, long value) {

		try {
			return cluster.incrBy(key, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStringCommands#incrBy(byte[], double)
	 */
	@Override
	public Double incrBy(byte[] key, double value) {

		try {
			return cluster.incrByFloat(key, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStringCommands#decr(byte[])
	 */
	@Override
	public Long decr(byte[] key) {

		try {
			return cluster.decr(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStringCommands#decrBy(byte[], long)
	 */
	@Override
	public Long decrBy(byte[] key, long value) {

		try {
			return cluster.decrBy(key, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStringCommands#append(byte[], byte[])
	 */
	@Override
	public Long append(byte[] key, byte[] value) {

		try {
			return cluster.append(key, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStringCommands#getRange(byte[], long, long)
	 */
	@Override
	public byte[] getRange(byte[] key, long begin, long end) {

		try {
			return cluster.getrange(key, begin, end);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStringCommands#setRange(byte[], byte[], long)
	 */
	@Override
	public void setRange(byte[] key, byte[] value, long offset) {

		try {
			cluster.setrange(key, offset, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Boolean getBit(byte[] key, long offset) {

		try {
			return cluster.getbit(key, offset);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Boolean setBit(byte[] key, long offset, boolean value) {

		try {
			return cluster.setbit(key, offset, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long bitCount(byte[] key) {

		try {
			return cluster.bitcount(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long bitCount(byte[] key, long begin, long end) {

		try {
			return cluster.bitcount(key, begin, end);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long bitOp(BitOperation op, byte[] destination, byte[]... keys) {

		byte[][] allKeys = ByteUtils.mergeArrays(destination, keys);

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(allKeys)) {
			try {
				return cluster.bitop(JedisConverters.toBitOp(op), destination, keys);
			} catch (Exception ex) {
				throw convertJedisAccessException(ex);
			}
		}

		throw new InvalidDataAccessApiUsageException("BITOP is only supported for same slot keys in cluster mode.");
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStringCommands#strLen(byte[])
	 */
	@Override
	public Long strLen(byte[] key) {

		try {
			return cluster.strlen(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisListCommands#rPush(byte[], byte[][])
	 */
	@Override
	public Long rPush(byte[] key, byte[]... values) {

		try {
			return cluster.rpush(key, values);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisListCommands#lPush(byte[], byte[][])
	 */
	@Override
	public Long lPush(byte[] key, byte[]... values) {

		try {
			return cluster.lpush(key, values);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisListCommands#rPushX(byte[], byte[])
	 */
	@Override
	public Long rPushX(byte[] key, byte[] value) {

		try {
			return cluster.rpushx(key, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisListCommands#lPushX(byte[], byte[])
	 */
	@Override
	public Long lPushX(byte[] key, byte[] value) {

		try {
			return cluster.lpushx(key, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisListCommands#lLen(byte[])
	 */
	@Override
	public Long lLen(byte[] key) {

		try {
			return cluster.llen(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisListCommands#lRange(byte[], long, long)
	 */
	@Override
	public List<byte[]> lRange(byte[] key, long begin, long end) {

		try {
			return cluster.lrange(key, begin, end);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisListCommands#lTrim(byte[], long, long)
	 */
	@Override
	public void lTrim(final byte[] key, final long begin, final long end) {

		try {
			cluster.ltrim(key, begin, end);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisListCommands#lIndex(byte[], long)
	 */
	@Override
	public byte[] lIndex(byte[] key, long index) {

		try {
			return cluster.lindex(key, index);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisListCommands#lInsert(byte[], org.springframework.data.redis.connection.RedisListCommands.Position, byte[], byte[])
	 */
	@Override
	public Long lInsert(byte[] key, Position where, byte[] pivot, byte[] value) {

		try {
			return cluster.linsert(key, JedisConverters.toListPosition(where), pivot, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisListCommands#lSet(byte[], long, byte[])
	 */
	@Override
	public void lSet(byte[] key, long index, byte[] value) {

		try {
			cluster.lset(key, index, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisListCommands#lRem(byte[], long, byte[])
	 */
	@Override
	public Long lRem(byte[] key, long count, byte[] value) {

		try {
			return cluster.lrem(key, count, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisListCommands#lPop(byte[])
	 */
	@Override
	public byte[] lPop(byte[] key) {

		try {
			return cluster.lpop(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisListCommands#rPop(byte[])
	 */
	@Override
	public byte[] rPop(byte[] key) {

		try {
			return cluster.rpop(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisListCommands#bLPop(int, byte[][])
	 */
	@Override
	public List<byte[]> bLPop(final int timeout, final byte[]... keys) {

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(keys)) {
			try {
				return cluster.blpop(timeout, keys);
			} catch (Exception ex) {
				throw convertJedisAccessException(ex);
			}
		}

		return this.clusterCommandExecutor.executeMuliKeyCommand(new JedisMultiKeyClusterCommandCallback<List<byte[]>>() {

			@Override
			public List<byte[]> doInCluster(Jedis client, byte[] key) {
				return client.blpop(timeout, key);
			}
		}, Arrays.asList(keys)).getFirstNonNullNotEmptyOrDefault(Collections.<byte[]> emptyList());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisListCommands#bRPop(int, byte[][])
	 */
	@Override
	public List<byte[]> bRPop(final int timeout, byte[]... keys) {

		return this.clusterCommandExecutor.executeMuliKeyCommand(new JedisMultiKeyClusterCommandCallback<List<byte[]>>() {

			@Override
			public List<byte[]> doInCluster(Jedis client, byte[] key) {
				return client.brpop(timeout, key);
			}
		}, Arrays.asList(keys)).getFirstNonNullNotEmptyOrDefault(Collections.<byte[]> emptyList());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisListCommands#rPopLPush(byte[], byte[])
	 */
	@Override
	public byte[] rPopLPush(byte[] srcKey, byte[] dstKey) {

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(srcKey, dstKey)) {
			try {
				return cluster.rpoplpush(srcKey, dstKey);
			} catch (Exception ex) {
				throw convertJedisAccessException(ex);
			}
		}

		byte[] val = rPop(srcKey);
		lPush(dstKey, val);
		return val;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisListCommands#bRPopLPush(int, byte[], byte[])
	 */
	@Override
	public byte[] bRPopLPush(int timeout, byte[] srcKey, byte[] dstKey) {

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(srcKey, dstKey)) {
			try {
				return cluster.brpoplpush(srcKey, dstKey, timeout);
			} catch (Exception ex) {
				throw convertJedisAccessException(ex);
			}
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
	 * @see org.springframework.data.redis.connection.RedisSetCommands#sAdd(byte[], byte[][])
	 */
	@Override
	public Long sAdd(byte[] key, byte[]... values) {

		try {
			return cluster.sadd(key, values);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisSetCommands#sRem(byte[], byte[][])
	 */
	@Override
	public Long sRem(byte[] key, byte[]... values) {

		try {
			return cluster.srem(key, values);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisSetCommands#sPop(byte[])
	 */
	@Override
	public byte[] sPop(byte[] key) {
		try {
			return cluster.spop(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisSetCommands#sMove(byte[], byte[], byte[])
	 */
	@Override
	public Boolean sMove(byte[] srcKey, byte[] destKey, byte[] value) {

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(srcKey, destKey)) {
			try {
				return JedisConverters.toBoolean(cluster.smove(srcKey, destKey, value));
			} catch (Exception ex) {
				throw convertJedisAccessException(ex);
			}
		}

		if (exists(srcKey)) {
			if (sRem(srcKey, value) > 0 && !sIsMember(destKey, value)) {
				return JedisConverters.toBoolean(sAdd(destKey, value));
			}
		}
		return Boolean.FALSE;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisSetCommands#sCard(byte[])
	 */
	@Override
	public Long sCard(byte[] key) {

		try {
			return cluster.scard(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisSetCommands#sIsMember(byte[], byte[])
	 */
	@Override
	public Boolean sIsMember(byte[] key, byte[] value) {

		try {
			return cluster.sismember(key, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisSetCommands#sInter(byte[][])
	 */
	@Override
	public Set<byte[]> sInter(byte[]... keys) {

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(keys)) {
			try {
				return cluster.sinter(keys);
			} catch (Exception ex) {
				throw convertJedisAccessException(ex);
			}
		}

		Collection<Set<byte[]>> resultList = this.clusterCommandExecutor
				.executeMuliKeyCommand(new JedisMultiKeyClusterCommandCallback<Set<byte[]>>() {

					@Override
					public Set<byte[]> doInCluster(Jedis client, byte[] key) {
						return client.smembers(key);
					}
				}, Arrays.asList(keys)).resultsAsList();

		ByteArraySet result = null;

		for (Set<byte[]> value : resultList) {

			ByteArraySet tmp = new ByteArraySet(value);
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
	 * @see org.springframework.data.redis.connection.RedisSetCommands#sInterStore(byte[], byte[][])
	 */
	@Override
	public Long sInterStore(byte[] destKey, byte[]... keys) {

		byte[][] allKeys = ByteUtils.mergeArrays(destKey, keys);

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(allKeys)) {
			try {
				return cluster.sinterstore(destKey, keys);
			} catch (Exception ex) {
				throw convertJedisAccessException(ex);
			}
		}

		Set<byte[]> result = sInter(keys);
		if (result.isEmpty()) {
			return 0L;
		}
		return sAdd(destKey, result.toArray(new byte[result.size()][]));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisSetCommands#sUnion(byte[][])
	 */
	@Override
	public Set<byte[]> sUnion(byte[]... keys) {

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(keys)) {
			try {
				return cluster.sunion(keys);
			} catch (Exception ex) {
				throw convertJedisAccessException(ex);
			}
		}

		Collection<Set<byte[]>> resultList = this.clusterCommandExecutor
				.executeMuliKeyCommand(new JedisMultiKeyClusterCommandCallback<Set<byte[]>>() {

					@Override
					public Set<byte[]> doInCluster(Jedis client, byte[] key) {
						return client.smembers(key);
					}
				}, Arrays.asList(keys)).resultsAsList();

		ByteArraySet result = new ByteArraySet();
		for (Set<byte[]> entry : resultList) {
			result.addAll(entry);
		}

		if (result.isEmpty()) {
			return Collections.emptySet();
		}

		return result.asRawSet();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisSetCommands#sUnionStore(byte[], byte[][])
	 */
	@Override
	public Long sUnionStore(byte[] destKey, byte[]... keys) {

		byte[][] allKeys = ByteUtils.mergeArrays(destKey, keys);

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(allKeys)) {
			try {
				return cluster.sunionstore(destKey, keys);
			} catch (Exception ex) {
				throw convertJedisAccessException(ex);
			}
		}

		Set<byte[]> result = sUnion(keys);
		if (result.isEmpty()) {
			return 0L;
		}
		return sAdd(destKey, result.toArray(new byte[result.size()][]));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisSetCommands#sDiff(byte[][])
	 */
	@Override
	public Set<byte[]> sDiff(byte[]... keys) {

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(keys)) {
			try {
				return cluster.sdiff(keys);
			} catch (Exception ex) {
				throw convertJedisAccessException(ex);
			}
		}

		byte[] source = keys[0];
		byte[][] others = Arrays.copyOfRange(keys, 1, keys.length);

		ByteArraySet values = new ByteArraySet(sMembers(source));
		Collection<Set<byte[]>> resultList = clusterCommandExecutor
				.executeMuliKeyCommand(new JedisMultiKeyClusterCommandCallback<Set<byte[]>>() {

					@Override
					public Set<byte[]> doInCluster(Jedis client, byte[] key) {
						return client.smembers(key);
					}
				}, Arrays.asList(others)).resultsAsList();

		if (values.isEmpty()) {
			return Collections.emptySet();
		}

		for (Set<byte[]> singleNodeValue : resultList) {
			values.removeAll(singleNodeValue);
		}

		return values.asRawSet();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisSetCommands#sDiffStore(byte[], byte[][])
	 */
	@Override
	public Long sDiffStore(byte[] destKey, byte[]... keys) {

		byte[][] allKeys = ByteUtils.mergeArrays(destKey, keys);

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(allKeys)) {
			try {
				return cluster.sdiffstore(destKey, keys);
			} catch (Exception ex) {
				throw convertJedisAccessException(ex);
			}
		}

		Set<byte[]> diff = sDiff(keys);
		if (diff.isEmpty()) {
			return 0L;
		}

		return sAdd(destKey, diff.toArray(new byte[diff.size()][]));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisSetCommands#sMembers(byte[])
	 */
	@Override
	public Set<byte[]> sMembers(byte[] key) {

		try {
			return cluster.smembers(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisSetCommands#sRandMember(byte[])
	 */
	@Override
	public byte[] sRandMember(byte[] key) {

		try {
			return cluster.srandmember(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisSetCommands#sRandMember(byte[], long)
	 */
	@Override
	public List<byte[]> sRandMember(byte[] key, long count) {

		if (count > Integer.MAX_VALUE) {
			throw new IllegalArgumentException("Count cannot exceed Integer.MAX_VALUE!");
		}

		try {
			return cluster.srandmember(key, Long.valueOf(count).intValue());
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisSetCommands#sScan(byte[], org.springframework.data.redis.core.ScanOptions)
	 */
	@Override
	public Cursor<byte[]> sScan(final byte[] key, ScanOptions options) {

		return new ScanCursor<byte[]>(options) {

			@Override
			protected ScanIteration<byte[]> doScan(long cursorId, ScanOptions options) {

				ScanParams params = JedisConverters.toScanParams(options);
				redis.clients.jedis.ScanResult<byte[]> result = cluster.sscan(key, JedisConverters.toBytes(cursorId), params);
				return new ScanIteration<byte[]>(Long.valueOf(result.getStringCursor()), result.getResult());
			}
		}.open();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zAdd(byte[], double, byte[])
	 */
	@Override
	public Boolean zAdd(byte[] key, double score, byte[] value) {

		try {
			return JedisConverters.toBoolean(cluster.zadd(key, score, value));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long zAdd(byte[] key, Set<Tuple> tuples) {

		// TODO: need to move the tuple conversion form jedisconnection.
		throw new UnsupportedOperationException();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRem(byte[], byte[][])
	 */
	@Override
	public Long zRem(byte[] key, byte[]... values) {

		try {
			return cluster.zrem(key, values);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}

	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zIncrBy(byte[], double, byte[])
	 */
	@Override
	public Double zIncrBy(byte[] key, double increment, byte[] value) {
		try {
			return cluster.zincrby(key, increment, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRank(byte[], byte[])
	 */
	@Override
	public Long zRank(byte[] key, byte[] value) {

		try {
			return cluster.zrank(key, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRevRank(byte[], byte[])
	 */
	@Override
	public Long zRevRank(byte[] key, byte[] value) {

		try {
			return cluster.zrevrank(key, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRange(byte[], long, long)
	 */
	@Override
	public Set<byte[]> zRange(byte[] key, long begin, long end) {

		try {
			return cluster.zrange(key, begin, end);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRangeByScoreWithScores(byte[], org.springframework.data.redis.connection.RedisZSetCommands.Range)
	 */
	@Override
	public Set<Tuple> zRangeByScoreWithScores(byte[] key, Range range) {
		return zRangeByScoreWithScores(key, range, null);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRangeByScoreWithScores(byte[], org.springframework.data.redis.connection.RedisZSetCommands.Range, org.springframework.data.redis.connection.RedisZSetCommands.Limit)
	 */
	@Override
	public Set<Tuple> zRangeByScoreWithScores(byte[] key, Range range, Limit limit) {

		Assert.notNull(range, "Range cannot be null for ZRANGEBYSCOREWITHSCORES.");

		byte[] min = JedisConverters.boundaryToBytesForZRange(range.getMin(), JedisConverters.NEGATIVE_INFINITY_BYTES);
		byte[] max = JedisConverters.boundaryToBytesForZRange(range.getMax(), JedisConverters.POSITIVE_INFINITY_BYTES);

		try {
			if (limit != null) {
				return JedisConverters
						.toTupleSet(cluster.zrangeByScoreWithScores(key, min, max, limit.getOffset(), limit.getCount()));
			}
			return JedisConverters.toTupleSet(cluster.zrangeByScoreWithScores(key, min, max));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRevRangeByScore(byte[], org.springframework.data.redis.connection.RedisZSetCommands.Range)
	 */
	@Override
	public Set<byte[]> zRevRangeByScore(byte[] key, Range range) {
		return zRevRangeByScore(key, range, null);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRevRangeByScore(byte[], org.springframework.data.redis.connection.RedisZSetCommands.Range, org.springframework.data.redis.connection.RedisZSetCommands.Limit)
	 */
	@Override
	public Set<byte[]> zRevRangeByScore(byte[] key, Range range, Limit limit) {

		Assert.notNull(range, "Range cannot be null for ZREVRANGEBYSCORE.");
		byte[] min = JedisConverters.boundaryToBytesForZRange(range.getMin(), JedisConverters.NEGATIVE_INFINITY_BYTES);
		byte[] max = JedisConverters.boundaryToBytesForZRange(range.getMax(), JedisConverters.POSITIVE_INFINITY_BYTES);

		try {
			if (limit != null) {
				return cluster.zrevrangeByScore(key, max, min, limit.getOffset(), limit.getCount());
			}
			return cluster.zrevrangeByScore(key, max, min);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRevRangeByScoreWithScores(byte[], org.springframework.data.redis.connection.RedisZSetCommands.Range)
	 */
	@Override
	public Set<Tuple> zRevRangeByScoreWithScores(byte[] key, Range range) {
		return zRevRangeByScoreWithScores(key, range, null);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRevRangeByScoreWithScores(byte[], org.springframework.data.redis.connection.RedisZSetCommands.Range, org.springframework.data.redis.connection.RedisZSetCommands.Limit)
	 */
	@Override
	public Set<Tuple> zRevRangeByScoreWithScores(byte[] key, Range range, Limit limit) {

		Assert.notNull(range, "Range cannot be null for ZREVRANGEBYSCOREWITHSCORES.");

		byte[] min = JedisConverters.boundaryToBytesForZRange(range.getMin(), JedisConverters.NEGATIVE_INFINITY_BYTES);
		byte[] max = JedisConverters.boundaryToBytesForZRange(range.getMax(), JedisConverters.POSITIVE_INFINITY_BYTES);

		try {
			if (limit != null) {
				return JedisConverters
						.toTupleSet(cluster.zrevrangeByScoreWithScores(key, max, min, limit.getOffset(), limit.getCount()));
			}
			return JedisConverters.toTupleSet(cluster.zrevrangeByScoreWithScores(key, max, min));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zCount(byte[], org.springframework.data.redis.connection.RedisZSetCommands.Range)
	 */
	@Override
	public Long zCount(byte[] key, Range range) {

		Assert.notNull(range, "Range cannot be null for ZCOUNT.");

		byte[] min = JedisConverters.boundaryToBytesForZRange(range.getMin(), JedisConverters.NEGATIVE_INFINITY_BYTES);
		byte[] max = JedisConverters.boundaryToBytesForZRange(range.getMax(), JedisConverters.POSITIVE_INFINITY_BYTES);

		try {
			return cluster.zcount(key, min, max);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRemRangeByScore(byte[], org.springframework.data.redis.connection.RedisZSetCommands.Range)
	 */
	@Override
	public Long zRemRangeByScore(byte[] key, Range range) {

		Assert.notNull(range, "Range cannot be null for ZREMRANGEBYSCORE.");

		byte[] min = JedisConverters.boundaryToBytesForZRange(range.getMin(), JedisConverters.NEGATIVE_INFINITY_BYTES);
		byte[] max = JedisConverters.boundaryToBytesForZRange(range.getMax(), JedisConverters.POSITIVE_INFINITY_BYTES);

		try {
			return cluster.zremrangeByScore(key, min, max);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}

	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRangeByScore(byte[], org.springframework.data.redis.connection.RedisZSetCommands.Range)
	 */
	@Override
	public Set<byte[]> zRangeByScore(byte[] key, Range range) {
		return zRangeByScore(key, range, null);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRangeByScore(byte[], org.springframework.data.redis.connection.RedisZSetCommands.Range, org.springframework.data.redis.connection.RedisZSetCommands.Limit)
	 */
	@Override
	public Set<byte[]> zRangeByScore(byte[] key, Range range, Limit limit) {

		Assert.notNull(range, "Range cannot be null for ZRANGEBYSCORE.");

		byte[] min = JedisConverters.boundaryToBytesForZRange(range.getMin(), JedisConverters.NEGATIVE_INFINITY_BYTES);
		byte[] max = JedisConverters.boundaryToBytesForZRange(range.getMax(), JedisConverters.POSITIVE_INFINITY_BYTES);

		try {
			if (limit != null) {
				return cluster.zrangeByScore(key, min, max, limit.getOffset(), limit.getCount());
			}
			return cluster.zrangeByScore(key, min, max);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRangeByLex(byte[])
	 */
	@Override
	public Set<byte[]> zRangeByLex(byte[] key) {
		return zRangeByLex(key, Range.unbounded());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRangeByLex(byte[], org.springframework.data.redis.connection.RedisZSetCommands.Range)
	 */
	@Override
	public Set<byte[]> zRangeByLex(byte[] key, Range range) {
		return zRangeByLex(key, range, null);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRangeByLex(byte[], org.springframework.data.redis.connection.RedisZSetCommands.Range, org.springframework.data.redis.connection.RedisZSetCommands.Limit)
	 */
	@Override
	public Set<byte[]> zRangeByLex(byte[] key, Range range, Limit limit) {

		Assert.notNull(range, "Range cannot be null for ZRANGEBYLEX.");

		byte[] min = JedisConverters.boundaryToBytesForZRangeByLex(range.getMin(), JedisConverters.toBytes("-"));
		byte[] max = JedisConverters.boundaryToBytesForZRangeByLex(range.getMax(), JedisConverters.toBytes("+"));

		try {
			if (limit != null) {
				return cluster.zrangeByLex(key, min, max, limit.getOffset(), limit.getCount());
			}
			return cluster.zrangeByLex(key, min, max);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRangeWithScores(byte[], long, long)
	 */
	@Override
	public Set<Tuple> zRangeWithScores(byte[] key, long begin, long end) {

		try {
			return JedisConverters.toTupleSet(cluster.zrangeWithScores(key, begin, end));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRangeByScore(byte[], double, double)
	 */
	@Override
	public Set<byte[]> zRangeByScore(byte[] key, double min, double max) {

		try {
			return cluster.zrangeByScore(key, min, max);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRangeByScoreWithScores(byte[], double, double)
	 */
	@Override
	public Set<Tuple> zRangeByScoreWithScores(byte[] key, double min, double max) {

		try {
			return JedisConverters.toTupleSet(cluster.zrangeByScoreWithScores(key, min, max));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRangeByScore(byte[], double, double, long, long)
	 */
	@Override
	public Set<byte[]> zRangeByScore(byte[] key, double min, double max, long offset, long count) {

		if (offset > Integer.MAX_VALUE || count > Integer.MAX_VALUE) {
			throw new IllegalArgumentException("Count/Offset cannot exceed Integer.MAX_VALUE!");
		}

		try {
			return cluster.zrangeByScore(key, min, max, Long.valueOf(offset).intValue(), Long.valueOf(count).intValue());
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRangeByScoreWithScores(byte[], double, double, long, long)
	 */
	@Override
	public Set<Tuple> zRangeByScoreWithScores(byte[] key, double min, double max, long offset, long count) {

		if (offset > Integer.MAX_VALUE || count > Integer.MAX_VALUE) {
			throw new IllegalArgumentException("Count/Offset cannot exceed Integer.MAX_VALUE!");
		}

		try {
			return JedisConverters.toTupleSet(cluster.zrangeByScoreWithScores(key, min, max, Long.valueOf(offset).intValue(),
					Long.valueOf(count).intValue()));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRevRange(byte[], long, long)
	 */
	@Override
	public Set<byte[]> zRevRange(byte[] key, long begin, long end) {

		try {
			return cluster.zrevrange(key, begin, end);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRevRangeWithScores(byte[], long, long)
	 */
	@Override
	public Set<Tuple> zRevRangeWithScores(byte[] key, long begin, long end) {

		try {
			return JedisConverters.toTupleSet(cluster.zrevrangeWithScores(key, begin, end));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRevRangeByScore(byte[], double, double)
	 */
	@Override
	public Set<byte[]> zRevRangeByScore(byte[] key, double min, double max) {

		try {
			return cluster.zrevrangeByScore(key, max, min);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRevRangeByScoreWithScores(byte[], double, double)
	 */
	@Override
	public Set<Tuple> zRevRangeByScoreWithScores(byte[] key, double min, double max) {

		try {
			return JedisConverters.toTupleSet(cluster.zrevrangeByScoreWithScores(key, max, min));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRevRangeByScore(byte[], double, double, long, long)
	 */
	@Override
	public Set<byte[]> zRevRangeByScore(byte[] key, double min, double max, long offset, long count) {

		if (offset > Integer.MAX_VALUE || count > Integer.MAX_VALUE) {
			throw new IllegalArgumentException("Count/Offset cannot exceed Integer.MAX_VALUE!");
		}

		try {
			return cluster.zrevrangeByScore(key, max, min, Long.valueOf(offset).intValue(), Long.valueOf(count).intValue());
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRevRangeByScoreWithScores(byte[], double, double, long, long)
	 */
	@Override
	public Set<Tuple> zRevRangeByScoreWithScores(byte[] key, double min, double max, long offset, long count) {

		if (offset > Integer.MAX_VALUE || count > Integer.MAX_VALUE) {
			throw new IllegalArgumentException("Count/Offset cannot exceed Integer.MAX_VALUE!");
		}

		try {
			return JedisConverters.toTupleSet(cluster.zrevrangeByScoreWithScores(key, max, min,
					Long.valueOf(offset).intValue(), Long.valueOf(count).intValue()));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zCount(byte[], double, double)
	 */
	@Override
	public Long zCount(byte[] key, double min, double max) {

		try {
			return cluster.zcount(key, min, max);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zCard(byte[])
	 */
	@Override
	public Long zCard(byte[] key) {

		try {
			return cluster.zcard(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zScore(byte[], byte[])
	 */
	@Override
	public Double zScore(byte[] key, byte[] value) {

		try {
			return cluster.zscore(key, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRemRange(byte[], long, long)
	 */
	@Override
	public Long zRemRange(byte[] key, long begin, long end) {

		try {
			return cluster.zremrangeByRank(key, begin, end);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRemRangeByScore(byte[], double, double)
	 */
	@Override
	public Long zRemRangeByScore(byte[] key, double min, double max) {

		try {
			return cluster.zremrangeByScore(key, min, max);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zUnionStore(byte[], byte[][])
	 */
	@Override
	public Long zUnionStore(byte[] destKey, byte[]... sets) {

		byte[][] allKeys = ByteUtils.mergeArrays(destKey, sets);

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(allKeys)) {

			try {
				return cluster.zunionstore(destKey, sets);
			} catch (Exception ex) {
				throw convertJedisAccessException(ex);
			}
		}

		throw new InvalidDataAccessApiUsageException("ZUNIONSTORE can only be executed when all keys map to the same slot");
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zUnionStore(byte[], org.springframework.data.redis.connection.RedisZSetCommands.Aggregate, int[], byte[][])
	 */
	@Override
	public Long zUnionStore(byte[] destKey, Aggregate aggregate, int[] weights, byte[]... sets) {

		byte[][] allKeys = ByteUtils.mergeArrays(destKey, sets);

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(allKeys)) {

			ZParams zparams = new ZParams().weights(weights).aggregate(ZParams.Aggregate.valueOf(aggregate.name()));

			try {
				return cluster.zunionstore(destKey, zparams, sets);
			} catch (Exception ex) {
				throw convertJedisAccessException(ex);
			}
		}

		throw new InvalidDataAccessApiUsageException("ZUNIONSTORE can only be executed when all keys map to the same slot");
	}

	@Override
	public Long zInterStore(byte[] destKey, byte[]... sets) {

		byte[][] allKeys = ByteUtils.mergeArrays(destKey, sets);

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(allKeys)) {

			try {
				return cluster.zinterstore(destKey, sets);
			} catch (Exception ex) {
				throw convertJedisAccessException(ex);
			}
		}

		throw new InvalidDataAccessApiUsageException("ZINTERSTORE can only be executed when all keys map to the same slot");
	}

	@Override
	public Long zInterStore(byte[] destKey, Aggregate aggregate, int[] weights, byte[]... sets) {

		byte[][] allKeys = ByteUtils.mergeArrays(destKey, sets);

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(allKeys)) {

			ZParams zparams = new ZParams().weights(weights).aggregate(ZParams.Aggregate.valueOf(aggregate.name()));

			try {
				return cluster.zinterstore(destKey, zparams, sets);
			} catch (Exception ex) {
				throw convertJedisAccessException(ex);
			}
		}

		throw new IllegalArgumentException("ZINTERSTORE can only be executed when all keys map to the same slot");
	}

	@Override
	public Cursor<Tuple> zScan(final byte[] key, final ScanOptions options) {
		return new ScanCursor<Tuple>(options) {

			@Override
			protected ScanIteration<Tuple> doScan(long cursorId, ScanOptions options) {

				ScanParams params = JedisConverters.toScanParams(options);

				redis.clients.jedis.ScanResult<redis.clients.jedis.Tuple> result = cluster.zscan(key,
						JedisConverters.toBytes(cursorId), params);
				return new ScanIteration<Tuple>(Long.valueOf(result.getStringCursor()),
						JedisConverters.tuplesToTuples().convert(result.getResult()));
			}
		}.open();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRangeByScore(byte[], java.lang.String, java.lang.String)
	 */
	@Override
	public Set<byte[]> zRangeByScore(byte[] key, String min, String max) {

		try {
			return cluster.zrangeByScore(key, JedisConverters.toBytes(min), JedisConverters.toBytes(max));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRangeByScore(byte[], java.lang.String, java.lang.String, long, long)
	 */
	@Override
	public Set<byte[]> zRangeByScore(byte[] key, String min, String max, long offset, long count) {

		if (offset > Integer.MAX_VALUE || count > Integer.MAX_VALUE) {
			throw new IllegalArgumentException("Count/Offset cannot exceed Integer.MAX_VALUE!");
		}

		try {
			return cluster.zrangeByScore(key, JedisConverters.toBytes(min), JedisConverters.toBytes(max),
					Long.valueOf(offset).intValue(), Long.valueOf(count).intValue());
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisHashCommands#hSet(byte[], byte[], byte[])
	 */
	@Override
	public Boolean hSet(byte[] key, byte[] field, byte[] value) {

		try {
			return JedisConverters.toBoolean(cluster.hset(key, field, value));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisHashCommands#hSetNX(byte[], byte[], byte[])
	 */
	@Override
	public Boolean hSetNX(byte[] key, byte[] field, byte[] value) {

		try {
			return JedisConverters.toBoolean(cluster.hsetnx(key, field, value));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisHashCommands#hGet(byte[], byte[])
	 */
	@Override
	public byte[] hGet(byte[] key, byte[] field) {

		try {
			return cluster.hget(key, field);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisHashCommands#hMGet(byte[], byte[][])
	 */
	@Override
	public List<byte[]> hMGet(byte[] key, byte[]... fields) {

		try {
			return cluster.hmget(key, fields);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisHashCommands#hMSet(byte[], java.util.Map)
	 */
	@Override
	public void hMSet(byte[] key, Map<byte[], byte[]> hashes) {

		try {
			cluster.hmset(key, hashes);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisHashCommands#hIncrBy(byte[], byte[], long)
	 */
	@Override
	public Long hIncrBy(byte[] key, byte[] field, long delta) {

		try {
			return cluster.hincrBy(key, field, delta);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisHashCommands#hIncrBy(byte[], byte[], double)
	 */
	@Override
	public Double hIncrBy(byte[] key, byte[] field, double delta) {
		try {
			return cluster.hincrByFloat(key, field, delta);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisHashCommands#hExists(byte[], byte[])
	 */
	@Override
	public Boolean hExists(byte[] key, byte[] field) {

		try {
			return cluster.hexists(key, field);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisHashCommands#hDel(byte[], byte[][])
	 */
	@Override
	public Long hDel(byte[] key, byte[]... fields) {

		try {
			return cluster.hdel(key, fields);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisHashCommands#hLen(byte[])
	 */
	@Override
	public Long hLen(byte[] key) {

		try {
			return cluster.hlen(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisHashCommands#hKeys(byte[])
	 */
	@Override
	public Set<byte[]> hKeys(byte[] key) {

		try {
			return cluster.hkeys(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisHashCommands#hVals(byte[])
	 */
	@Override
	public List<byte[]> hVals(byte[] key) {

		try {
			return new ArrayList<byte[]>(cluster.hvals(key));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisHashCommands#hGetAll(byte[])
	 */
	@Override
	public Map<byte[], byte[]> hGetAll(byte[] key) {

		try {
			return cluster.hgetAll(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisHashCommands#hScan(byte[], org.springframework.data.redis.core.ScanOptions)
	 */
	@Override
	public Cursor<Entry<byte[], byte[]>> hScan(final byte[] key, ScanOptions options) {

		return new ScanCursor<Map.Entry<byte[], byte[]>>(options) {

			@Override
			protected ScanIteration<Entry<byte[], byte[]>> doScan(long cursorId, ScanOptions options) {

				ScanParams params = JedisConverters.toScanParams(options);

				redis.clients.jedis.ScanResult<Map.Entry<byte[], byte[]>> result = cluster.hscan(key,
						JedisConverters.toBytes(cursorId), params);
				return new ScanIteration<Map.Entry<byte[], byte[]>>(Long.valueOf(result.getStringCursor()), result.getResult());
			}
		}.open();
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
	 * @see org.springframework.data.redis.connection.RedisGeoCommands#geoAdd(byte[], org.springframework.data.geo.Point, byte[])
	 */
	@Override
	public Long geoAdd(byte[] key, Point point, byte[] member) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(point, "Point must not be null!");
		Assert.notNull(member, "Member must not be null!");

		try {
			return cluster.geoadd(key, point.getX(), point.getY(), member);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisGeoCommands#geoAdd(byte[], org.springframework.data.redis.connection.RedisGeoCommands.GeoLocation)
	 */
	public Long geoAdd(byte[] key, GeoLocation<byte[]> location) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(location, "Location must not be null!");

		return geoAdd(key, location.getPoint(), location.getName());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisGeoCommands#geoAdd(byte[], java.util.Map)
	 */
	@Override
	public Long geoAdd(byte[] key, Map<byte[], Point> memberCoordinateMap) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(memberCoordinateMap, "MemberCoordinateMap must not be null!");

		Map<byte[], GeoCoordinate> redisGeoCoordinateMap = new HashMap<byte[], GeoCoordinate>();
		for (byte[] mapKey : memberCoordinateMap.keySet()) {
			redisGeoCoordinateMap.put(mapKey, JedisConverters.toGeoCoordinate(memberCoordinateMap.get(mapKey)));
		}

		try {
			return cluster.geoadd(key, redisGeoCoordinateMap);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisGeoCommands#geoAdd(byte[], java.lang.Iterable)
	 */
	@Override
	public Long geoAdd(byte[] key, Iterable<GeoLocation<byte[]>> locations) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(locations, "Locations must not be null!");

		Map<byte[], redis.clients.jedis.GeoCoordinate> redisGeoCoordinateMap = new HashMap<byte[], GeoCoordinate>();
		for (GeoLocation<byte[]> location : locations) {
			redisGeoCoordinateMap.put(location.getName(), JedisConverters.toGeoCoordinate(location.getPoint()));
		}

		try {
			return cluster.geoadd(key, redisGeoCoordinateMap);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisGeoCommands#geoDist(byte[], byte[], byte[])
	 */
	@Override
	public Distance geoDist(byte[] key, byte[] member1, byte[] member2) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(member1, "Member1 must not be null!");
		Assert.notNull(member2, "Member2 must not be null!");

		try {
			return JedisConverters.distanceConverterForMetric(DistanceUnit.METERS)
					.convert(cluster.geodist(key, member1, member2));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisGeoCommands#geoDist(byte[], byte[], byte[], org.springframework.data.geo.Metric)
	 */
	@Override
	public Distance geoDist(byte[] key, byte[] member1, byte[] member2, Metric metric) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(member1, "Member1 must not be null!");
		Assert.notNull(member2, "Member2 must not be null!");
		Assert.notNull(metric, "Metric must not be null!");

		GeoUnit geoUnit = JedisConverters.toGeoUnit(metric);
		try {
			return JedisConverters.distanceConverterForMetric(metric)
					.convert(cluster.geodist(key, member1, member2, geoUnit));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisGeoCommands#geoHash(byte[], byte[][])
	 */
	@Override
	public List<String> geoHash(byte[] key, byte[]... members) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(members, "Members must not be null!");
		Assert.noNullElements(members, "Members must not contain null!");

		try {
			return JedisConverters.toStrings(cluster.geohash(key, members));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisGeoCommands#geoPos(byte[], byte[][])
	 */
	@Override
	public List<Point> geoPos(byte[] key, byte[]... members) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(members, "Members must not be null!");
		Assert.noNullElements(members, "Members must not contain null!");

		try {
			return JedisConverters.geoCoordinateToPointConverter().convert(cluster.geopos(key, members));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisGeoCommands#geoRadius(byte[], org.springframework.data.geo.Circle)
	 */
	@Override
	public GeoResults<GeoLocation<byte[]>> geoRadius(byte[] key, Circle within) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(within, "Within must not be null!");

		try {
			return JedisConverters.geoRadiusResponseToGeoResultsConverter(within.getRadius().getMetric())
					.convert(cluster.georadius(key, within.getCenter().getX(), within.getCenter().getY(),
							within.getRadius().getValue(), JedisConverters.toGeoUnit(within.getRadius().getMetric())));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisGeoCommands#geoRadius(byte[], org.springframework.data.geo.Circle, org.springframework.data.redis.core.GeoRadiusCommandArgs)
	 */
	@Override
	public GeoResults<GeoLocation<byte[]>> geoRadius(byte[] key, Circle within, GeoRadiusCommandArgs args) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(within, "Within must not be null!");
		Assert.notNull(args, "Args must not be null!");

		GeoRadiusParam geoRadiusParam = JedisConverters.toGeoRadiusParam(args);

		try {
			return JedisConverters.geoRadiusResponseToGeoResultsConverter(within.getRadius().getMetric())
					.convert(cluster.georadius(key, within.getCenter().getX(), within.getCenter().getY(),
							within.getRadius().getValue(), JedisConverters.toGeoUnit(within.getRadius().getMetric()),
							geoRadiusParam));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisGeoCommands#geoRadiusByMember(byte[], byte[], double)
	 */
	@Override
	public GeoResults<GeoLocation<byte[]>> geoRadiusByMember(byte[] key, byte[] member, double radius) {
		return geoRadiusByMember(key, member, new Distance(radius, DistanceUnit.METERS));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisGeoCommands#geoRadiusByMember(byte[], byte[], org.springframework.data.geo.Distance)
	 */
	@Override
	public GeoResults<GeoLocation<byte[]>> geoRadiusByMember(byte[] key, byte[] member, Distance radius) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(member, "Member must not be null!");
		Assert.notNull(radius, "Radius must not be null!");

		GeoUnit geoUnit = JedisConverters.toGeoUnit(radius.getMetric());
		try {
			return JedisConverters.geoRadiusResponseToGeoResultsConverter(radius.getMetric())
					.convert(cluster.georadiusByMember(key, member, radius.getValue(), geoUnit));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisGeoCommands#geoRadiusByMember(byte[], byte[], org.springframework.data.geo.Distance, org.springframework.data.redis.core.GeoRadiusCommandArgs)
	 */
	@Override
	public GeoResults<GeoLocation<byte[]>> geoRadiusByMember(byte[] key, byte[] member, Distance radius,
			GeoRadiusCommandArgs args) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(member, "Member must not be null!");
		Assert.notNull(radius, "Radius must not be null!");
		Assert.notNull(args, "Args must not be null!");

		GeoUnit geoUnit = JedisConverters.toGeoUnit(radius.getMetric());
		redis.clients.jedis.params.geo.GeoRadiusParam geoRadiusParam = JedisConverters.toGeoRadiusParam(args);

		try {
			return JedisConverters.geoRadiusResponseToGeoResultsConverter(radius.getMetric())
					.convert(cluster.georadiusByMember(key, member, radius.getValue(), geoUnit, geoRadiusParam));

		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisGeoCommands#geoRemove(byte[], byte[][])
	 */
	@Override
	public Long geoRemove(byte[] key, byte[]... members) {
		return zRem(key, members);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConnectionCommands#select(int)
	 */
	@Override
	public void select(final int dbIndex) {

		if (dbIndex != 0) {
			throw new InvalidDataAccessApiUsageException("Cannot SELECT non zero index in cluster mode.");
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConnectionCommands#echo(byte[])
	 */
	@Override
	public byte[] echo(final byte[] message) {

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

		return !clusterCommandExecutor.executeCommandOnAllNodes(new JedisClusterCommandCallback<String>() {

			@Override
			public String doInCluster(Jedis client) {
				return client.ping();
			}
		}).resultsAsList().isEmpty() ? "PONG" : null;

	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterConnection#ping(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public String ping(RedisClusterNode node) {

		return clusterCommandExecutor.executeCommandOnSingleNode(new JedisClusterCommandCallback<String>() {

			@Override
			public String doInCluster(Jedis client) {
				return client.ping();
			}
		}, node).getValue();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#bgWriteAof()
	 */
	@Override
	public void bgWriteAof() {

		clusterCommandExecutor.executeCommandOnAllNodes(new JedisClusterCommandCallback<String>() {

			@Override
			public String doInCluster(Jedis client) {
				return client.bgrewriteaof();
			}
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterConnection#bgReWriteAof(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public void bgReWriteAof(RedisClusterNode node) {

		clusterCommandExecutor.executeCommandOnSingleNode(new JedisClusterCommandCallback<String>() {

			@Override
			public String doInCluster(Jedis client) {
				return client.bgrewriteaof();
			}
		}, node);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#bgReWriteAof()
	 */
	@Override
	public void bgReWriteAof() {

		clusterCommandExecutor.executeCommandOnAllNodes(new JedisClusterCommandCallback<String>() {

			@Override
			public String doInCluster(Jedis client) {
				return client.bgrewriteaof();
			}
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#bgSave()
	 */
	@Override
	public void bgSave() {

		clusterCommandExecutor.executeCommandOnAllNodes(new JedisClusterCommandCallback<String>() {

			@Override
			public String doInCluster(Jedis client) {
				return client.bgsave();
			}
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterConnection#bgSave(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public void bgSave(RedisClusterNode node) {

		clusterCommandExecutor.executeCommandOnSingleNode(new JedisClusterCommandCallback<String>() {

			@Override
			public String doInCluster(Jedis client) {
				return client.bgsave();
			}
		}, node);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#lastSave()
	 */
	@Override
	public Long lastSave() {

		List<Long> result = new ArrayList<Long>(
				clusterCommandExecutor.executeCommandOnAllNodes(new JedisClusterCommandCallback<Long>() {

					@Override
					public Long doInCluster(Jedis client) {
						return client.lastsave();
					}
				}).resultsAsList());

		if (CollectionUtils.isEmpty(result)) {
			return null;
		}

		Collections.sort(result, Collections.reverseOrder());
		return result.get(0);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterConnection#lastSave(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public Long lastSave(RedisClusterNode node) {

		return clusterCommandExecutor.executeCommandOnSingleNode(new JedisClusterCommandCallback<Long>() {

			@Override
			public Long doInCluster(Jedis client) {
				return client.lastsave();
			}
		}, node).getValue();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#save()
	 */
	@Override
	public void save() {

		clusterCommandExecutor.executeCommandOnAllNodes(new JedisClusterCommandCallback<String>() {

			@Override
			public String doInCluster(Jedis client) {
				return client.save();
			}
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterConnection#save(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public void save(RedisClusterNode node) {

		clusterCommandExecutor.executeCommandOnSingleNode(new JedisClusterCommandCallback<String>() {

			@Override
			public String doInCluster(Jedis client) {
				return client.save();
			}
		}, node);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#dbSize()
	 */
	@Override
	public Long dbSize() {

		Collection<Long> dbSizes = clusterCommandExecutor.executeCommandOnAllNodes(new JedisClusterCommandCallback<Long>() {

			@Override
			public Long doInCluster(Jedis client) {
				return client.dbSize();
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
	 * @see org.springframework.data.redis.connection.RedisClusterConnection#dbSize(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public Long dbSize(RedisClusterNode node) {

		return clusterCommandExecutor.executeCommandOnSingleNode(new JedisClusterCommandCallback<Long>() {

			@Override
			public Long doInCluster(Jedis client) {
				return client.dbSize();
			}
		}, node).getValue();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#flushDb()
	 */
	@Override
	public void flushDb() {

		clusterCommandExecutor.executeCommandOnAllNodes(new JedisClusterCommandCallback<String>() {

			@Override
			public String doInCluster(Jedis client) {
				return client.flushDB();
			}
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterConnection#flushDb(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public void flushDb(RedisClusterNode node) {

		clusterCommandExecutor.executeCommandOnSingleNode(new JedisClusterCommandCallback<String>() {

			@Override
			public String doInCluster(Jedis client) {
				return client.flushDB();
			}
		}, node);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#flushAll()
	 */
	@Override
	public void flushAll() {

		clusterCommandExecutor.executeCommandOnAllNodes(new JedisClusterCommandCallback<String>() {

			@Override
			public String doInCluster(Jedis client) {
				return client.flushAll();
			}
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterConnection#flushAll(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public void flushAll(RedisClusterNode node) {

		clusterCommandExecutor.executeCommandOnSingleNode(new JedisClusterCommandCallback<String>() {

			@Override
			public String doInCluster(Jedis client) {
				return client.flushAll();
			}
		}, node);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#info()
	 */
	@Override
	public Properties info() {

		Properties infos = new Properties();

		List<NodeResult<Properties>> nodeResults = clusterCommandExecutor
				.executeCommandOnAllNodes(new JedisClusterCommandCallback<Properties>() {

					@Override
					public Properties doInCluster(Jedis client) {
						return JedisConverters.toProperties(client.info());
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
	 * @see org.springframework.data.redis.connection.RedisClusterConnection#info(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public Properties info(RedisClusterNode node) {

		return JedisConverters
				.toProperties(clusterCommandExecutor.executeCommandOnSingleNode(new JedisClusterCommandCallback<String>() {

					@Override
					public String doInCluster(Jedis client) {
						return client.info();
					}
				}, node).getValue());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#info(java.lang.String)
	 */
	@Override
	public Properties info(final String section) {

		Properties infos = new Properties();

		List<NodeResult<Properties>> nodeResults = clusterCommandExecutor
				.executeCommandOnAllNodes(new JedisClusterCommandCallback<Properties>() {

					@Override
					public Properties doInCluster(Jedis client) {
						return JedisConverters.toProperties(client.info(section));
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

		return JedisConverters
				.toProperties(clusterCommandExecutor.executeCommandOnSingleNode(new JedisClusterCommandCallback<String>() {

					@Override
					public String doInCluster(Jedis client) {
						return client.info(section);
					}
				}, node).getValue());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#shutdown()
	 */
	@Override
	public void shutdown() {

		clusterCommandExecutor.executeCommandOnAllNodes(new JedisClusterCommandCallback<String>() {

			@Override
			public String doInCluster(Jedis client) {
				return client.shutdown();
			}
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterConnection#shutdown(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public void shutdown(RedisClusterNode node) {

		clusterCommandExecutor.executeCommandOnSingleNode(new JedisClusterCommandCallback<String>() {

			@Override
			public String doInCluster(Jedis client) {
				return client.shutdown();
			}
		}, node);

	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#shutdown(org.springframework.data.redis.connection.RedisServerCommands.ShutdownOption)
	 */
	@Override
	public void shutdown(ShutdownOption option) {

		if (option == null) {
			shutdown();
			return;
		}

		throw new IllegalArgumentException("Shutdown with options is not supported for jedis.");
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#getConfig(java.lang.String)
	 */
	@Override
	public List<String> getConfig(final String pattern) {

		List<NodeResult<List<String>>> mapResult = clusterCommandExecutor
				.executeCommandOnAllNodes(new JedisClusterCommandCallback<List<String>>() {

					@Override
					public List<String> doInCluster(Jedis client) {
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

		return clusterCommandExecutor.executeCommandOnSingleNode(new JedisClusterCommandCallback<List<String>>() {

			@Override
			public List<String> doInCluster(Jedis client) {
				return client.configGet(pattern);
			}
		}, node).getValue();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#setConfig(java.lang.String, java.lang.String)
	 */
	@Override
	public void setConfig(final String param, final String value) {

		clusterCommandExecutor.executeCommandOnAllNodes(new JedisClusterCommandCallback<String>() {

			@Override
			public String doInCluster(Jedis client) {
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

		clusterCommandExecutor.executeCommandOnSingleNode(new JedisClusterCommandCallback<String>() {

			@Override
			public String doInCluster(Jedis client) {
				return client.configSet(param, value);
			}
		}, node);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#resetConfigStats()
	 */
	@Override
	public void resetConfigStats() {

		clusterCommandExecutor.executeCommandOnAllNodes(new JedisClusterCommandCallback<String>() {

			@Override
			public String doInCluster(Jedis client) {
				return client.configResetStat();
			}
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterConnection#resetConfigStats(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public void resetConfigStats(RedisClusterNode node) {

		clusterCommandExecutor.executeCommandOnSingleNode(new JedisClusterCommandCallback<String>() {

			@Override
			public String doInCluster(Jedis client) {
				return client.configResetStat();
			}
		}, node);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#time()
	 */
	@Override
	public Long time() {

		return convertListOfStringToTime(
				clusterCommandExecutor.executeCommandOnArbitraryNode(new JedisClusterCommandCallback<List<String>>() {

					@Override
					public List<String> doInCluster(Jedis client) {
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
				clusterCommandExecutor.executeCommandOnSingleNode(new JedisClusterCommandCallback<List<String>>() {

					@Override
					public List<String> doInCluster(Jedis client) {
						return client.time();
					}
				}, node).getValue());
	}

	private Long convertListOfStringToTime(List<String> serverTimeInformation) {

		Assert.notEmpty(serverTimeInformation, "Received invalid result from server. Expected 2 items in collection.");
		Assert.isTrue(serverTimeInformation.size() == 2,
				"Received invalid number of arguments from redis server. Expected 2 received " + serverTimeInformation.size());

		return Converters.toTimeMillis(serverTimeInformation.get(0), serverTimeInformation.get(1));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#killClient(java.lang.String, int)
	 */
	@Override
	public void killClient(String host, int port) {

		final String hostAndPort = String.format("%s:%s", host, port);

		clusterCommandExecutor.executeCommandOnAllNodes(new JedisClusterCommandCallback<String>() {

			@Override
			public String doInCluster(Jedis client) {
				return client.clientKill(hostAndPort);
			}
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#setClientName(byte[])
	 */
	@Override
	public void setClientName(byte[] name) {
		throw new InvalidDataAccessApiUsageException("CLIENT SETNAME is not supported in cluster environment.");
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#getClientName()
	 */
	@Override
	public String getClientName() {
		throw new InvalidDataAccessApiUsageException("CLIENT GETNAME is not supported in cluster environment.");
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#getClientList()
	 */
	@Override
	public List<RedisClientInfo> getClientList() {

		Collection<String> map = clusterCommandExecutor.executeCommandOnAllNodes(new JedisClusterCommandCallback<String>() {

			@Override
			public String doInCluster(Jedis client) {
				return client.clientList();
			}
		}).resultsAsList();

		ArrayList<RedisClientInfo> result = new ArrayList<RedisClientInfo>();
		for (String infos : map) {
			result.addAll(JedisConverters.toListOfRedisClientInformation(infos));
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterConnection#getClientList(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public List<RedisClientInfo> getClientList(RedisClusterNode node) {

		return JedisConverters.toListOfRedisClientInformation(
				clusterCommandExecutor.executeCommandOnSingleNode(new JedisClusterCommandCallback<String>() {

					@Override
					public String doInCluster(Jedis client) {
						return client.clientList();
					}
				}, node).getValue());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#slaveOf(java.lang.String, int)
	 */
	@Override
	public void slaveOf(String host, int port) {
		throw new InvalidDataAccessApiUsageException(
				"SlaveOf is not supported in cluster environment. Please use CLUSTER REPLICATE.");
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#slaveOfNoOne()
	 */
	@Override
	public void slaveOfNoOne() {
		throw new InvalidDataAccessApiUsageException(
				"SlaveOf is not supported in cluster environment. Please use CLUSTER REPLICATE.");
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisScriptingCommands#scriptFlush()
	 */
	@Override
	public void scriptFlush() {
		throw new InvalidDataAccessApiUsageException("ScriptFlush is not supported in cluster environment.");
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisScriptingCommands#scriptKill()
	 */
	@Override
	public void scriptKill() {
		throw new InvalidDataAccessApiUsageException("ScriptKill is not supported in cluster environment.");
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisScriptingCommands#scriptLoad(byte[])
	 */
	@Override
	public String scriptLoad(byte[] script) {
		throw new InvalidDataAccessApiUsageException("ScriptLoad is not supported in cluster environment.");
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisScriptingCommands#scriptExists(java.lang.String[])
	 */
	@Override
	public List<Boolean> scriptExists(String... scriptShas) {
		throw new InvalidDataAccessApiUsageException("ScriptExists is not supported in cluster environment.");
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisScriptingCommands#eval(byte[], org.springframework.data.redis.connection.ReturnType, int, byte[][])
	 */
	@Override
	public <T> T eval(byte[] script, ReturnType returnType, int numKeys, byte[]... keysAndArgs) {
		throw new InvalidDataAccessApiUsageException("Eval is not supported in cluster environment.");
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisScriptingCommands#evalSha(java.lang.String, org.springframework.data.redis.connection.ReturnType, int, byte[][])
	 */
	@Override
	public <T> T evalSha(String scriptSha, ReturnType returnType, int numKeys, byte[]... keysAndArgs) {
		throw new InvalidDataAccessApiUsageException("EvalSha is not supported in cluster environment.");
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisScriptingCommands#evalSha(byte[], org.springframework.data.redis.connection.ReturnType, int, byte[][])
	 */
	@Override
	public <T> T evalSha(byte[] scriptSha, ReturnType returnType, int numKeys, byte[]... keysAndArgs) {
		throw new InvalidDataAccessApiUsageException("EvalSha is not supported in cluster environment.");
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.HyperLogLogCommands#pfAdd(byte[], byte[][])
	 */
	@Override
	public Long pfAdd(byte[] key, byte[]... values) {

		try {
			return cluster.pfadd(key, values);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.HyperLogLogCommands#pfCount(byte[][])
	 */
	@Override
	public Long pfCount(byte[]... keys) {

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(keys)) {

			try {
				return cluster.pfcount(keys);
			} catch (Exception ex) {
				throw convertJedisAccessException(ex);
			}

		}
		throw new InvalidDataAccessApiUsageException("All keys must map to same slot for pfcount in cluster mode.");
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.HyperLogLogCommands#pfMerge(byte[], byte[][])
	 */
	@Override
	public void pfMerge(byte[] destinationKey, byte[]... sourceKeys) {

		byte[][] allKeys = ByteUtils.mergeArrays(destinationKey, sourceKeys);

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(allKeys)) {
			try {
				cluster.pfmerge(destinationKey, sourceKeys);
				return;
			} catch (Exception ex) {
				throw convertJedisAccessException(ex);
			}
		}

		throw new InvalidDataAccessApiUsageException("All keys must map to same slot for pfmerge in cluster mode.");
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisKeyCommands#exists(byte[])
	 */
	@Override
	public Boolean exists(final byte[] key) {

		try {
			return cluster.exists(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * --> Cluster Commands
	 */

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

		clusterCommandExecutor.executeCommandOnSingleNode(new JedisClusterCommandCallback<String>() {

			@Override
			public String doInCluster(Jedis client) {

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
			}
		}, node);

	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterCommands#clusterGetKeysInSlot(int, java.lang.Integer)
	 */
	@Override
	public List<byte[]> clusterGetKeysInSlot(final int slot, final Integer count) {

		RedisClusterNode node = clusterGetNodeForSlot(slot);

		clusterCommandExecutor.executeCommandOnSingleNode(new JedisClusterCommandCallback<List<byte[]>>() {

			@Override
			public List<byte[]> doInCluster(Jedis client) {
				return JedisConverters.stringListToByteList()
						.convert(client.clusterGetKeysInSlot(slot, count != null ? count.intValue() : Integer.MAX_VALUE));
			}
		}, node);
		return null;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterCommands#clusterAddSlots(org.springframework.data.redis.connection.RedisClusterNode, int[])
	 */
	@Override
	public void clusterAddSlots(RedisClusterNode node, final int... slots) {

		clusterCommandExecutor.executeCommandOnSingleNode(new JedisClusterCommandCallback<String>() {

			@Override
			public String doInCluster(Jedis client) {

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
	 * @see org.springframework.data.redis.connection.RedisClusterCommands#clusterCountKeysInSlot(int)
	 */
	@Override
	public Long clusterCountKeysInSlot(final int slot) {

		RedisClusterNode node = clusterGetNodeForSlot(slot);

		return clusterCommandExecutor.executeCommandOnSingleNode(new JedisClusterCommandCallback<Long>() {

			@Override
			public Long doInCluster(Jedis client) {

				return client.clusterCountKeysInSlot(slot);
			}
		}, node).getValue();

	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterCommands#clusterDeleteSlots(org.springframework.data.redis.connection.RedisClusterNode, int[])
	 */
	@Override
	public void clusterDeleteSlots(RedisClusterNode node, final int... slots) {

		clusterCommandExecutor.executeCommandOnSingleNode(new JedisClusterCommandCallback<String>() {

			@Override
			public String doInCluster(Jedis client) {
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

		Set<RedisClusterNode> nodes = new LinkedHashSet<RedisClusterNode>(
				topologyProvider.getTopology().getActiveMasterNodes());
		final RedisClusterNode nodeToRemove = topologyProvider.getTopology().lookup(node);
		nodes.remove(nodeToRemove);

		clusterCommandExecutor.executeCommandAsyncOnNodes(new JedisClusterCommandCallback<String>() {

			@Override
			public String doInCluster(Jedis client) {
				return client.clusterForget(node.getId());
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

		clusterCommandExecutor.executeCommandOnAllNodes(new JedisClusterCommandCallback<String>() {

			@Override
			public String doInCluster(Jedis client) {

				return client.clusterMeet(node.getHost(), node.getPort());
			}
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterCommands#clusterReplicate(org.springframework.data.redis.connection.RedisClusterNode, org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public void clusterReplicate(final RedisClusterNode master, RedisClusterNode slave) {

		final RedisClusterNode masterNode = topologyProvider.getTopology().lookup(master);

		clusterCommandExecutor.executeCommandOnSingleNode(new JedisClusterCommandCallback<String>() {

			@Override
			public String doInCluster(Jedis client) {

				return client.clusterReplicate(masterNode.getId());
			}
		}, slave);

	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterCommands#getClusterSlotForKey(byte[])
	 */
	@Override
	public Integer clusterGetSlotForKey(final byte[] key) {

		return clusterCommandExecutor.executeCommandOnArbitraryNode(new JedisClusterCommandCallback<Integer>() {

			@Override
			public Integer doInCluster(Jedis client) {
				return client.clusterKeySlot(JedisConverters.toString(key)).intValue();
			}
		}).getValue();
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
	public Set<RedisClusterNode> clusterGetSlaves(final RedisClusterNode master) {

		Assert.notNull(master, "Master cannot be null!");

		final RedisClusterNode nodeToUse = topologyProvider.getTopology().lookup(master);

		return JedisConverters.toSetOfRedisClusterNodes(
				clusterCommandExecutor.executeCommandOnSingleNode(new JedisClusterCommandCallback<List<String>>() {

					@Override
					public List<String> doInCluster(Jedis client) {
						return client.clusterSlaves(nodeToUse.getId());
					}
				}, master).getValue());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterCommands#clusterGetMasterSlaveMap()
	 */
	@Override
	public Map<RedisClusterNode, Collection<RedisClusterNode>> clusterGetMasterSlaveMap() {

		List<NodeResult<Collection<RedisClusterNode>>> nodeResults = clusterCommandExecutor
				.executeCommandAsyncOnNodes(new JedisClusterCommandCallback<Collection<RedisClusterNode>>() {

					@Override
					public Set<RedisClusterNode> doInCluster(Jedis client) {

						// TODO: remove client.eval as soon as Jedis offers support for myid
						return JedisConverters.toSetOfRedisClusterNodes(
								client.clusterSlaves((String) client.eval("return redis.call('cluster', 'myid')", 0)));
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

		return new ClusterInfo(JedisConverters
				.toProperties(clusterCommandExecutor.executeCommandOnArbitraryNode(new JedisClusterCommandCallback<String>() {

					@Override
					public String doInCluster(Jedis client) {
						return client.clusterInfo();
					}
				}).getValue()));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#migrate(byte[], org.springframework.data.redis.connection.RedisNode, int, org.springframework.data.redis.connection.RedisServerCommands.MigrateOption)
	 */
	@Override
	public void migrate(byte[] key, RedisNode target, int dbIndex, MigrateOption option) {
		migrate(key, target, dbIndex, option, Long.MAX_VALUE);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#migrate(byte[], org.springframework.data.redis.connection.RedisNode, int, org.springframework.data.redis.connection.RedisServerCommands.MigrateOption, long)
	 */
	@Override
	public void migrate(final byte[] key, final RedisNode target, final int dbIndex, final MigrateOption option,
			final long timeout) {

		final int timeoutToUse = timeout <= Integer.MAX_VALUE ? (int) timeout : Integer.MAX_VALUE;

		RedisClusterNode node = topologyProvider.getTopology().lookup(target.getHost(), target.getPort());

		clusterCommandExecutor.executeCommandOnSingleNode(new JedisClusterCommandCallback<String>() {

			@Override
			public String doInCluster(Jedis client) {
				return client.migrate(JedisConverters.toBytes(target.getHost()), target.getPort(), key, dbIndex, timeoutToUse);
			}
		}, node);
	}

	/*
	 * --> Little helpers to make it work
	 */

	protected DataAccessException convertJedisAccessException(Exception ex) {
		return EXCEPTION_TRANSLATION.translate(ex);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConnection#close()
	 */
	@Override
	public void close() throws DataAccessException {
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
	 * @since 1.7
	 */
	static class JedisClusterNodeResourceProvider implements ClusterNodeResourceProvider {

		private final JedisCluster cluster;

		/**
		 * Creates new {@link JedisClusterNodeResourceProvider}.
		 *
		 * @param cluster must not be {@literal null}.
		 */
		public JedisClusterNodeResourceProvider(JedisCluster cluster) {
			this.cluster = cluster;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.connection.ClusterNodeResourceProvider#getResourceForSpecificNode(org.springframework.data.redis.connection.RedisClusterNode)
		 */
		@Override
		@SuppressWarnings("unchecked")
		public Jedis getResourceForSpecificNode(RedisClusterNode node) {

			JedisPool pool = getResourcePoolForSpecificNode(node);
			if (pool != null) {
				return pool.getResource();
			}

			throw new IllegalArgumentException(String.format("Node %s is unknown to cluster", node));
		}

		protected JedisPool getResourcePoolForSpecificNode(RedisNode node) {

			Assert.notNull(node, "Cannot get Pool for 'null' node!");

			Map<String, JedisPool> clusterNodes = cluster.getClusterNodes();
			if (clusterNodes.containsKey(node.asString())) {
				return clusterNodes.get(node.asString());
			}

			return null;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.connection.ClusterNodeResourceProvider#returnResourceForSpecificNode(org.springframework.data.redis.connection.RedisClusterNode, java.lang.Object)
		 */
		@Override
		public void returnResourceForSpecificNode(RedisClusterNode node, Object client) {
			getResourcePoolForSpecificNode(node).returnResource((Jedis) client);
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
		private ClusterTopology cached;

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

			Map<String, Exception> errors = new LinkedHashMap<String, Exception>();

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
						entry.getValue().returnResource(jedis);
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

}
