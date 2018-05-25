/*
 * Copyright 2017-2018 the original author or authors.
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

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import redis.clients.jedis.BinaryJedis;
import redis.clients.jedis.ScanParams;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.springframework.dao.DataAccessException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.connection.ClusterSlotHashUtil;
import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.connection.RedisClusterNode;
import org.springframework.data.redis.connection.RedisKeyCommands;
import org.springframework.data.redis.connection.RedisNode;
import org.springframework.data.redis.connection.SortParameters;
import org.springframework.data.redis.connection.ValueEncoding;
import org.springframework.data.redis.connection.convert.Converters;
import org.springframework.data.redis.connection.jedis.JedisClusterConnection.JedisClusterCommandCallback;
import org.springframework.data.redis.connection.jedis.JedisClusterConnection.JedisMultiKeyClusterCommandCallback;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanCursor;
import org.springframework.data.redis.core.ScanIteration;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.0
 */
@RequiredArgsConstructor
class JedisClusterKeyCommands implements RedisKeyCommands {

	private final @NonNull JedisClusterConnection connection;

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisKeyCommands#del(byte[][])
	 */
	@Override
	public Long del(byte[]... keys) {

		Assert.notNull(keys, "Keys must not be null!");
		Assert.noNullElements(keys, "Keys must not contain null elements!");

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(keys)) {
			try {
				return connection.getCluster().del(keys);
			} catch (Exception ex) {
				throw convertJedisAccessException(ex);
			}
		}

		return (long) connection.getClusterCommandExecutor()
				.executeMultiKeyCommand((JedisMultiKeyClusterCommandCallback<Long>) (client, key) -> client.del(key),
						Arrays.asList(keys))
				.resultsAsList().size();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisKeyCommands#unlink(byte[][])
	 */
	@Nullable
	@Override
	public Long unlink(byte[]... keys) {

		Assert.notNull(keys, "Keys must not be null!");

		return connection.<Long> execute("UNLINK", Arrays.asList(keys), Collections.emptyList()).stream()
				.mapToLong(val -> val).sum();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisKeyCommands#type(byte[])
	 */
	@Override
	public DataType type(byte[] key) {

		Assert.notNull(key, "Key must not be null!");

		try {
			return JedisConverters.toDataType(connection.getCluster().type(key));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisKeyCommands#touch(byte[][])
	 */
	@Nullable
	@Override
	public Long touch(byte[]... keys) {

		Assert.notNull(keys, "Keys must not be null!");

		return connection.<Long> execute("TOUCH", Arrays.asList(keys), Collections.emptyList()).stream()
				.mapToLong(val -> val).sum();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisKeyCommands#keys(byte[])
	 */
	@Override
	public Set<byte[]> keys(byte[] pattern) {

		Assert.notNull(pattern, "Pattern must not be null!");

		Collection<Set<byte[]>> keysPerNode = connection.getClusterCommandExecutor()
				.executeCommandOnAllNodes((JedisClusterCommandCallback<Set<byte[]>>) client -> client.keys(pattern))
				.resultsAsList();

		Set<byte[]> keys = new HashSet<>();
		for (Set<byte[]> keySet : keysPerNode) {
			keys.addAll(keySet);
		}
		return keys;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterConnection#keys(org.springframework.data.redis.connection.RedisClusterNode, byte[])
	 */
	public Set<byte[]> keys(RedisClusterNode node, byte[] pattern) {

		Assert.notNull(node, "RedisClusterNode must not be null!");
		Assert.notNull(pattern, "Pattern must not be null!");

		return connection.getClusterCommandExecutor()
				.executeCommandOnSingleNode((JedisClusterCommandCallback<Set<byte[]>>) client -> client.keys(pattern), node)
				.getValue();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisKeyCommands#scan(org.springframework.data.redis.core.ScanOptions)
	 */
	@Override
	public Cursor<byte[]> scan(ScanOptions options) {
		throw new InvalidDataAccessApiUsageException("Scan is not supported across multiple nodes within a cluster");
	}

	/**
	 * Use a {@link Cursor} to iterate over keys stored at the given {@link RedisClusterNode}.
	 *
	 * @param node must not be {@literal null}.
	 * @param options must not be {@literal null}.
	 * @return never {@literal null}.
	 * @since 2.1
	 */
	Cursor<byte[]> scan(RedisClusterNode node, ScanOptions options) {

		Assert.notNull(node, "RedisClusterNode must not be null!");
		Assert.notNull(options, "Options must not be null!");

		return connection.getClusterCommandExecutor()
				.executeCommandOnSingleNode((JedisClusterCommandCallback<Cursor<byte[]>>) client -> {

					return new ScanCursor<byte[]>(0, options) {

						@Override
						protected ScanIteration<byte[]> doScan(long cursorId, ScanOptions options) {

							ScanParams params = JedisConverters.toScanParams(options);
							redis.clients.jedis.ScanResult<String> result = client.scan(Long.toString(cursorId), params);
							return new ScanIteration<>(Long.valueOf(result.getStringCursor()),
									JedisConverters.stringListToByteList().convert(result.getResult()));
						}
					}.open();
				}, node).getValue();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisKeyCommands#randomKey()
	 */
	@Override
	public byte[] randomKey() {

		List<RedisClusterNode> nodes = new ArrayList<>(
				connection.getTopologyProvider().getTopology().getActiveMasterNodes());
		Set<RedisNode> inspectedNodes = new HashSet<>(nodes.size());

		do {

			RedisClusterNode node = nodes.get(ThreadLocalRandom.current().nextInt(nodes.size()));

			while (inspectedNodes.contains(node)) {
				node = nodes.get(ThreadLocalRandom.current().nextInt(nodes.size()));
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
	public byte[] randomKey(RedisClusterNode node) {

		Assert.notNull(node, "RedisClusterNode must not be null!");

		return connection.getClusterCommandExecutor()
				.executeCommandOnSingleNode((JedisClusterCommandCallback<byte[]>) client -> client.randomBinaryKey(), node)
				.getValue();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisKeyCommands#rename(byte[], byte[])
	 */
	@Override
	public void rename(byte[] sourceKey, byte[] targetKey) {

		Assert.notNull(sourceKey, "Source key must not be null!");
		Assert.notNull(targetKey, "Target key must not be null!");

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(sourceKey, targetKey)) {

			try {
				connection.getCluster().rename(sourceKey, targetKey);
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
	public Boolean renameNX(byte[] sourceKey, byte[] targetKey) {

		Assert.notNull(sourceKey, "Source key must not be null!");
		Assert.notNull(targetKey, "Target key must not be null!");

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(sourceKey, targetKey)) {

			try {
				return JedisConverters.toBoolean(connection.getCluster().renamenx(sourceKey, targetKey));
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

		Assert.notNull(key, "Key must not be null!");

		if (seconds > Integer.MAX_VALUE) {
			throw new UnsupportedOperationException("Jedis does not support seconds exceeding Integer.MAX_VALUE.");
		}
		try {
			return JedisConverters.toBoolean(connection.getCluster().expire(key, Long.valueOf(seconds).intValue()));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisKeyCommands#pExpire(byte[], long)
	 */
	@Override
	public Boolean pExpire(byte[] key, long millis) {

		Assert.notNull(key, "Key must not be null!");

		try {
			return JedisConverters.toBoolean(connection.getCluster().pexpire(key, millis));
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

		Assert.notNull(key, "Key must not be null!");

		try {
			return JedisConverters.toBoolean(connection.getCluster().expireAt(key, unixTime));
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

		Assert.notNull(key, "Key must not be null!");

		try {
			return JedisConverters.toBoolean(connection.getCluster().pexpireAt(key, unixTimeInMillis));
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

		Assert.notNull(key, "Key must not be null!");

		try {
			return JedisConverters.toBoolean(connection.getCluster().persist(key));
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

		Assert.notNull(key, "Key must not be null!");

		try {
			return connection.getCluster().ttl(key);
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

		Assert.notNull(key, "Key must not be null!");

		try {
			return Converters.secondsToTimeUnit(connection.getCluster().ttl(key), timeUnit);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisKeyCommands#pTtl(byte[])
	 */
	@Override
	public Long pTtl(byte[] key) {

		Assert.notNull(key, "Key must not be null!");

		return connection.getClusterCommandExecutor()
				.executeCommandOnSingleNode((JedisClusterCommandCallback<Long>) client -> client.pttl(key),
						connection.getTopologyProvider().getTopology().getKeyServingMasterNode(key))
				.getValue();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisKeyCommands#pTtl(byte[], java.util.concurrent.TimeUnit)
	 */
	@Override
	public Long pTtl(byte[] key, TimeUnit timeUnit) {

		Assert.notNull(key, "Key must not be null!");

		return connection.getClusterCommandExecutor()
				.executeCommandOnSingleNode(
						(JedisClusterCommandCallback<Long>) client -> Converters.millisecondsToTimeUnit(client.pttl(key), timeUnit),
						connection.getTopologyProvider().getTopology().getKeyServingMasterNode(key))
				.getValue();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisKeyCommands#dump(byte[])
	 */
	@Override
	public byte[] dump(byte[] key) {

		Assert.notNull(key, "Key must not be null!");

		return connection.getClusterCommandExecutor()
				.executeCommandOnSingleNode((JedisClusterCommandCallback<byte[]>) client -> client.dump(key),
						connection.getTopologyProvider().getTopology().getKeyServingMasterNode(key))
				.getValue();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisKeyCommands#restore(byte[], long, byte[], boolean)
	 */
	@Override
	public void restore(byte[] key, long ttlInMillis, byte[] serializedValue, boolean replace) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(serializedValue, "Serialized value must not be null!");

		if (ttlInMillis > Integer.MAX_VALUE) {
			throw new UnsupportedOperationException("Jedis does not support ttlInMillis exceeding Integer.MAX_VALUE.");
		}

		connection.getClusterCommandExecutor().executeCommandOnSingleNode((JedisClusterCommandCallback<String>) client -> {

			if (!replace) {
				return client.restore(key, Long.valueOf(ttlInMillis).intValue(), serializedValue);
			}

			return JedisConverters.toString(this.connection.execute("RESTORE", key,
					Arrays.asList(JedisConverters.toBytes(ttlInMillis), serializedValue, JedisConverters.toBytes
							("REPLACE"))));

		}, connection.clusterGetNodeForKey(key));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisKeyCommands#sort(byte[], org.springframework.data.redis.connection.SortParameters)
	 */
	@Override
	public List<byte[]> sort(byte[] key, SortParameters params) {

		Assert.notNull(key, "Key must not be null!");

		try {
			return connection.getCluster().sort(key, JedisConverters.toSortingParams(params));
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

		Assert.notNull(key, "Key must not be null!");

		List<byte[]> sorted = sort(key, params);
		if (!CollectionUtils.isEmpty(sorted)) {

			byte[][] arr = new byte[sorted.size()][];
			switch (type(key)) {

				case SET:
					connection.setCommands().sAdd(storeKey, sorted.toArray(arr));
					return 1L;
				case LIST:
					connection.listCommands().lPush(storeKey, sorted.toArray(arr));
					return 1L;
				default:
					throw new IllegalArgumentException("sort and store is only supported for SET and LIST");
			}
		}
		return 0L;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisKeyCommands#exists(byte[][])
	 */
	@Nullable
	@Override
	public Long exists(byte[]... keys) {

		Assert.notNull(keys, "Keys must not be null!");
		Assert.noNullElements(keys, "Keys must not contain null elements!");

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(keys)) {
			try {
				return connection.getCluster().exists(keys);
			} catch (Exception ex) {
				throw convertJedisAccessException(ex);
			}
		}

		return connection.getClusterCommandExecutor()
				.executeMultiKeyCommand((JedisMultiKeyClusterCommandCallback<Boolean>) BinaryJedis::exists, Arrays.asList(keys))
				.resultsAsList().stream().mapToLong(val -> ObjectUtils.nullSafeEquals(val, Boolean.TRUE) ? 1 : 0).sum();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisKeyCommands#encoding(byte[])
	 */
	@Nullable
	@Override
	public ValueEncoding encodingOf(byte[] key) {

		Assert.notNull(key, "Key must not be null!");

		return connection.getClusterCommandExecutor()
				.executeCommandOnSingleNode((JedisClusterCommandCallback<byte[]>) client -> client.objectEncoding(key),
						connection.clusterGetNodeForKey(key))
				.mapValue(JedisConverters::toEncoding);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisKeyCommands#idletime(byte[])
	 */
	@Nullable
	@Override
	public Duration idletime(byte[] key) {

		Assert.notNull(key, "Key must not be null!");

		return connection.getClusterCommandExecutor()
				.executeCommandOnSingleNode((JedisClusterCommandCallback<Long>) client -> client.objectIdletime(key),
						connection.clusterGetNodeForKey(key))
				.mapValue(Converters::secondsToDuration);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisKeyCommands#refcount(byte[])
	 */
	@Nullable
	@Override
	public Long refcount(byte[] key) {

		Assert.notNull(key, "Key must not be null!");

		return connection.getClusterCommandExecutor()
				.executeCommandOnSingleNode((JedisClusterCommandCallback<Long>) client -> client.objectRefcount(key),
						connection.clusterGetNodeForKey(key))
				.getValue();

	}

	private DataAccessException convertJedisAccessException(Exception ex) {
		return connection.convertJedisAccessException(ex);
	}
}
