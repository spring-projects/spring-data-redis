/*
 * Copyright 2017-2025 the original author or authors.
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

import redis.clients.jedis.Jedis;
import redis.clients.jedis.args.ExpiryOption;
import redis.clients.jedis.params.RestoreParams;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

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
import org.springframework.data.redis.connection.ExpirationOptions;
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
import org.springframework.util.ObjectUtils;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author ihaohong
 * @author Dan Smith
 * @since 2.0
 */
class JedisClusterKeyCommands implements RedisKeyCommands {

	private final JedisClusterConnection connection;

	JedisClusterKeyCommands(JedisClusterConnection connection) {
		this.connection = connection;
	}

	@Override
	public Boolean copy(byte[] sourceKey, byte[] targetKey, boolean replace) {

		Assert.notNull(sourceKey, "source key must not be null");
		Assert.notNull(targetKey, "target key must not be null");

		return connection.getCluster().copy(sourceKey, targetKey, replace);
	}

	@Override
	public Long del(byte[]... keys) {

		Assert.notNull(keys, "Keys must not be null");
		Assert.noNullElements(keys, "Keys must not contain null elements");

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

	@Nullable
	@Override
	public Long unlink(byte[]... keys) {

		Assert.notNull(keys, "Keys must not be null");

		return connection.<Long> execute("UNLINK", Arrays.asList(keys), Collections.emptyList()).stream()
				.mapToLong(val -> val).sum();
	}

	@Override
	public DataType type(byte[] key) {

		Assert.notNull(key, "Key must not be null");

		try {
			return JedisConverters.toDataType(connection.getCluster().type(key));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Nullable
	@Override
	public Long touch(byte[]... keys) {

		Assert.notNull(keys, "Keys must not be null");

		return connection.<Long> execute("TOUCH", Arrays.asList(keys), Collections.emptyList()).stream()
				.mapToLong(val -> val).sum();
	}

	@Override
	public Set<byte[]> keys(byte[] pattern) {

		Assert.notNull(pattern, "Pattern must not be null");

		Collection<Set<byte[]>> keysPerNode = connection.getClusterCommandExecutor()
				.executeCommandOnAllNodes((JedisClusterCommandCallback<Set<byte[]>>) client -> client.keys(pattern))
				.resultsAsList();

		Set<byte[]> keys = new HashSet<>();
		for (Set<byte[]> keySet : keysPerNode) {
			keys.addAll(keySet);
		}
		return keys;
	}

	public Set<byte[]> keys(RedisClusterNode node, byte[] pattern) {

		Assert.notNull(node, "RedisClusterNode must not be null");
		Assert.notNull(pattern, "Pattern must not be null");

		return connection.getClusterCommandExecutor()
				.executeCommandOnSingleNode((JedisClusterCommandCallback<Set<byte[]>>) client -> client.keys(pattern), node)
				.getValue();
	}

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

		Assert.notNull(node, "RedisClusterNode must not be null");
		Assert.notNull(options, "Options must not be null");

		return connection.getClusterCommandExecutor()
				.executeCommandOnSingleNode((JedisClusterCommandCallback<Cursor<byte[]>>) client -> {

					return new ScanCursor<byte[]>(0, options) {

						@Override
						protected ScanIteration<byte[]> doScan(CursorId cursorId, ScanOptions options) {

							ScanParams params = JedisConverters.toScanParams(options);
							ScanResult<String> result = client.scan(cursorId.getCursorId(), params);
							return new ScanIteration<>(CursorId.of(result.getCursor()),
									JedisConverters.stringListToByteList().convert(result.getResult()));
						}
					}.open();
				}, node).getValue();
	}

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

	public byte[] randomKey(RedisClusterNode node) {

		Assert.notNull(node, "RedisClusterNode must not be null");

		return connection.getClusterCommandExecutor()
				.executeCommandOnSingleNode((JedisClusterCommandCallback<byte[]>) client -> client.randomBinaryKey(), node)
				.getValue();
	}

	@Override
	public void rename(byte[] oldKey, byte[] newKey) {

		Assert.notNull(oldKey, "Old key must not be null");
		Assert.notNull(newKey, "New key must not be null");

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(oldKey, newKey)) {

			try {
				connection.getCluster().rename(oldKey, newKey);
				return;
			} catch (Exception ex) {
				throw convertJedisAccessException(ex);
			}
		}

		byte[] value = dump(oldKey);

		if (value != null && value.length > 0) {

			restore(newKey, 0, value, true);
			del(oldKey);
		}
	}

	@Override
	public Boolean renameNX(byte[] sourceKey, byte[] targetKey) {

		Assert.notNull(sourceKey, "Source key must not be null");
		Assert.notNull(targetKey, "Target key must not be null");

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

	@Override
	public Boolean expire(byte[] key, long seconds, ExpirationOptions.Condition condition) {

		Assert.notNull(key, "Key must not be null");

		try {
			if (condition == ExpirationOptions.Condition.ALWAYS) {
				return JedisConverters.toBoolean(connection.getCluster().expire(key, seconds));
			}

			return JedisConverters
					.toBoolean(connection.getCluster().expire(key, seconds, ExpiryOption.valueOf(condition.name())));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Boolean pExpire(byte[] key, long millis, ExpirationOptions.Condition condition) {

		Assert.notNull(key, "Key must not be null");

		try {
			if (condition == ExpirationOptions.Condition.ALWAYS) {
				return JedisConverters.toBoolean(connection.getCluster().pexpire(key, millis));
			}
			return JedisConverters
					.toBoolean(connection.getCluster().pexpire(key, millis, ExpiryOption.valueOf(condition.name())));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Boolean expireAt(byte[] key, long unixTime, ExpirationOptions.Condition condition) {

		Assert.notNull(key, "Key must not be null");

		try {
			if (condition == ExpirationOptions.Condition.ALWAYS) {
				return JedisConverters.toBoolean(connection.getCluster().expireAt(key, unixTime));
			}

			return JedisConverters
					.toBoolean(connection.getCluster().expireAt(key, unixTime, ExpiryOption.valueOf(condition.name())));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Boolean pExpireAt(byte[] key, long unixTimeInMillis, ExpirationOptions.Condition condition) {

		Assert.notNull(key, "Key must not be null");

		try {
			if (condition == ExpirationOptions.Condition.ALWAYS) {
				return JedisConverters.toBoolean(connection.getCluster().pexpireAt(key, unixTimeInMillis));
			}

			return JedisConverters
					.toBoolean(connection.getCluster().pexpireAt(key, unixTimeInMillis, ExpiryOption.valueOf(condition.name())));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Boolean persist(byte[] key) {

		Assert.notNull(key, "Key must not be null");

		try {
			return JedisConverters.toBoolean(connection.getCluster().persist(key));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Boolean move(byte[] key, int dbIndex) {
		throw new InvalidDataAccessApiUsageException("Cluster mode does not allow moving keys");
	}

	@Override
	public Long ttl(byte[] key) {

		Assert.notNull(key, "Key must not be null");

		try {
			return connection.getCluster().ttl(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long ttl(byte[] key, TimeUnit timeUnit) {

		Assert.notNull(key, "Key must not be null");

		try {
			return Converters.secondsToTimeUnit(connection.getCluster().ttl(key), timeUnit);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long pTtl(byte[] key) {

		Assert.notNull(key, "Key must not be null");

		try {
			return connection.getCluster().pttl(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long pTtl(byte[] key, TimeUnit timeUnit) {

		Assert.notNull(key, "Key must not be null");

		try {
			return Converters.millisecondsToTimeUnit(connection.getCluster().pttl(key), timeUnit);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public byte[] dump(byte[] key) {

		Assert.notNull(key, "Key must not be null");

		try {
			return connection.getCluster().dump(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public void restore(byte[] key, long ttlInMillis, byte[] serializedValue, boolean replace) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(serializedValue, "Serialized value must not be null");

		RestoreParams restoreParams = RestoreParams.restoreParams();

		if (replace) {
			restoreParams = restoreParams.replace();
		}
		try {
			connection.getCluster().restore(key, ttlInMillis, serializedValue, restoreParams);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public List<byte[]> sort(byte[] key, SortParameters params) {

		Assert.notNull(key, "Key must not be null");

		try {
			return connection.getCluster().sort(key, JedisConverters.toSortingParams(params));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long sort(byte[] key, SortParameters params, byte[] storeKey) {

		Assert.notNull(key, "Key must not be null");

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(key, storeKey)) {
			try {
				return connection.getCluster().sort(key, JedisConverters.toSortingParams(params), storeKey);
			} catch (Exception ex) {
				throw convertJedisAccessException(ex);
			}
		}

		List<byte[]> sorted = sort(key, params);
		byte[][] arr = new byte[sorted.size()][];
		connection.keyCommands().unlink(storeKey);
		connection.listCommands().lPush(storeKey, sorted.toArray(arr));
		return (long) sorted.size();
	}

	@Nullable
	@Override
	public Long exists(byte[]... keys) {

		Assert.notNull(keys, "Keys must not be null");
		Assert.noNullElements(keys, "Keys must not contain null elements");

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(keys)) {
			try {
				return connection.getCluster().exists(keys);
			} catch (Exception ex) {
				throw convertJedisAccessException(ex);
			}
		}

		return connection.getClusterCommandExecutor()
				.executeMultiKeyCommand((JedisMultiKeyClusterCommandCallback<Boolean>) Jedis::exists, Arrays.asList(keys))
				.resultsAsList().stream().mapToLong(val -> ObjectUtils.nullSafeEquals(val, Boolean.TRUE) ? 1 : 0).sum();
	}

	@Nullable
	@Override
	public ValueEncoding encodingOf(byte[] key) {

		Assert.notNull(key, "Key must not be null");

		try {
			return JedisConverters.toEncoding(connection.getCluster().objectEncoding(key));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Nullable
	@Override
	public Duration idletime(byte[] key) {

		Assert.notNull(key, "Key must not be null");

		try {
			return Converters.secondsToDuration(connection.getCluster().objectIdletime(key));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Nullable
	@Override
	public Long refcount(byte[] key) {

		Assert.notNull(key, "Key must not be null");

		try {
			return connection.getCluster().objectRefcount(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}

	}

	private DataAccessException convertJedisAccessException(Exception ex) {
		return connection.convertJedisAccessException(ex);
	}
}
