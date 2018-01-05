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
import redis.clients.jedis.ScanParams;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.ClusterSlotHashUtil;
import org.springframework.data.redis.connection.RedisSetCommands;
import org.springframework.data.redis.connection.jedis.JedisClusterConnection.JedisMultiKeyClusterCommandCallback;
import org.springframework.data.redis.connection.util.ByteArraySet;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanCursor;
import org.springframework.data.redis.core.ScanIteration;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.util.ByteUtils;
import org.springframework.util.Assert;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.0
 */
@RequiredArgsConstructor
class JedisClusterSetCommands implements RedisSetCommands {

	private final @NonNull JedisClusterConnection connection;

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisSetCommands#sAdd(byte[], byte[][])
	 */
	@Override
	public Long sAdd(byte[] key, byte[]... values) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(values, "Values must not be null!");
		Assert.noNullElements(values, "Values must not contain null elements!");

		try {
			return connection.getCluster().sadd(key, values);
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

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(values, "Values must not be null!");
		Assert.noNullElements(values, "Values must not contain null elements!");

		try {
			return connection.getCluster().srem(key, values);
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

		Assert.notNull(key, "Key must not be null!");

		try {
			return connection.getCluster().spop(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisSetCommands#sPop(byte[], long)
	 */
	@Override
	public List<byte[]> sPop(byte[] key, long count) {

		Assert.notNull(key, "Key must not be null!");

		try {
			return new ArrayList<>(connection.getCluster().spop(key, count));
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

		Assert.notNull(srcKey, "Source key must not be null!");
		Assert.notNull(destKey, "Destination key must not be null!");
		Assert.notNull(value, "Value must not be null!");

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(srcKey, destKey)) {
			try {
				return JedisConverters.toBoolean(connection.getCluster().smove(srcKey, destKey, value));
			} catch (Exception ex) {
				throw convertJedisAccessException(ex);
			}
		}

		if (connection.keyCommands().exists(srcKey)) {
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

		Assert.notNull(key, "Key must not be null!");

		try {
			return connection.getCluster().scard(key);
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

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(value, "Value must not be null!");

		try {
			return connection.getCluster().sismember(key, value);
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

		Assert.notNull(keys, "Keys must not be null!");
		Assert.noNullElements(keys, "Keys must not contain null elements!");

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(keys)) {
			try {
				return connection.getCluster().sinter(keys);
			} catch (Exception ex) {
				throw convertJedisAccessException(ex);
			}
		}

		Collection<Set<byte[]>> resultList = connection.getClusterCommandExecutor()
				.executeMultiKeyCommand(
						(JedisMultiKeyClusterCommandCallback<Set<byte[]>>) (client, key) -> client.smembers(key),
						Arrays.asList(keys))
				.resultsAsList();

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

		Assert.notNull(destKey, "Destination key must not be null!");
		Assert.notNull(keys, "Source keys must not be null!");
		Assert.noNullElements(keys, "Source keys must not contain null elements!");

		byte[][] allKeys = ByteUtils.mergeArrays(destKey, keys);

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(allKeys)) {
			try {
				return connection.getCluster().sinterstore(destKey, keys);
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

		Assert.notNull(keys, "Keys must not be null!");
		Assert.noNullElements(keys, "Keys must not contain null elements!");

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(keys)) {
			try {
				return connection.getCluster().sunion(keys);
			} catch (Exception ex) {
				throw convertJedisAccessException(ex);
			}
		}

		Collection<Set<byte[]>> resultList = connection.getClusterCommandExecutor()
				.executeMultiKeyCommand(
						(JedisMultiKeyClusterCommandCallback<Set<byte[]>>) (client, key) -> client.smembers(key),
						Arrays.asList(keys))
				.resultsAsList();

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

		Assert.notNull(destKey, "Destination key must not be null!");
		Assert.notNull(keys, "Source keys must not be null!");
		Assert.noNullElements(keys, "Source keys must not contain null elements!");

		byte[][] allKeys = ByteUtils.mergeArrays(destKey, keys);

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(allKeys)) {
			try {
				return connection.getCluster().sunionstore(destKey, keys);
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

		Assert.notNull(keys, "Keys must not be null!");
		Assert.noNullElements(keys, "Keys must not contain null elements!");

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(keys)) {
			try {
				return connection.getCluster().sdiff(keys);
			} catch (Exception ex) {
				throw convertJedisAccessException(ex);
			}
		}

		byte[] source = keys[0];
		byte[][] others = Arrays.copyOfRange(keys, 1, keys.length);

		ByteArraySet values = new ByteArraySet(sMembers(source));
		Collection<Set<byte[]>> resultList = connection.getClusterCommandExecutor()
				.executeMultiKeyCommand(
						(JedisMultiKeyClusterCommandCallback<Set<byte[]>>) (client, key) -> client.smembers(key),
						Arrays.asList(others))
				.resultsAsList();

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

		Assert.notNull(destKey, "Destination key must not be null!");
		Assert.notNull(keys, "Source keys must not be null!");
		Assert.noNullElements(keys, "Source keys must not contain null elements!");

		byte[][] allKeys = ByteUtils.mergeArrays(destKey, keys);

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(allKeys)) {
			try {
				return connection.getCluster().sdiffstore(destKey, keys);
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

		Assert.notNull(key, "Key must not be null!");

		try {
			return connection.getCluster().smembers(key);
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

		Assert.notNull(key, "Key must not be null!");

		try {
			return connection.getCluster().srandmember(key);
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

		Assert.notNull(key, "Key must not be null!");

		if (count > Integer.MAX_VALUE) {
			throw new IllegalArgumentException("Count cannot exceed Integer.MAX_VALUE!");
		}

		try {
			return connection.getCluster().srandmember(key, Long.valueOf(count).intValue());
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisSetCommands#sScan(byte[], org.springframework.data.redis.core.ScanOptions)
	 */
	@Override
	public Cursor<byte[]> sScan(byte[] key, ScanOptions options) {

		Assert.notNull(key, "Key must not be null!");

		return new ScanCursor<byte[]>(options) {

			@Override
			protected ScanIteration<byte[]> doScan(long cursorId, ScanOptions options) {

				ScanParams params = JedisConverters.toScanParams(options);
				redis.clients.jedis.ScanResult<byte[]> result = connection.getCluster().sscan(key,
						JedisConverters.toBytes(cursorId), params);
				return new ScanIteration<>(Long.valueOf(result.getStringCursor()), result.getResult());
			}
		}.open();
	}

	private DataAccessException convertJedisAccessException(Exception ex) {
		return connection.convertJedisAccessException(ex);
	}

}
