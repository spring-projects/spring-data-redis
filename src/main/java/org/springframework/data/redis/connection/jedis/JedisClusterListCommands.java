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

import redis.clients.jedis.args.ListDirection;
import redis.clients.jedis.params.LPosParams;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.NullUnmarked;
import org.jspecify.annotations.Nullable;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.ClusterSlotHashUtil;
import org.springframework.data.redis.connection.RedisListCommands;
import org.springframework.data.redis.connection.jedis.JedisClusterConnection.JedisMultiKeyClusterCommandCallback;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Jot Zhao
 * @author dengliming
 * @since 2.0
 */
@NullUnmarked
class JedisClusterListCommands implements RedisListCommands {

	private final JedisClusterConnection connection;

	JedisClusterListCommands(@NonNull JedisClusterConnection connection) {
		this.connection = connection;
	}

	@Override
	public Long rPush(byte @NonNull [] key, byte @NonNull [] @NonNull... values) {

		Assert.notNull(key, "Key must not be null");

		try {
			return connection.getCluster().rpush(key, values);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public List<Long> lPos(byte @NonNull [] key, byte @NonNull [] element, @Nullable Integer rank,
			@Nullable Integer count) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(element, "Element must not be null");

		LPosParams params = new LPosParams();
		if (rank != null) {
			params.rank(rank);
		}

		try {

			if (count != null) {
				return connection.getCluster().lpos(key, element, params, count);
			}

			Long value = connection.getCluster().lpos(key, element, params);
			return value != null ? Collections.singletonList(value) : Collections.emptyList();
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long lPush(byte @NonNull [] key, byte @NonNull [] @NonNull... values) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(values, "Values must not be null");
		Assert.noNullElements(values, "Values must not contain null elements");

		try {
			return connection.getCluster().lpush(key, values);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long rPushX(byte @NonNull [] key, byte @NonNull [] value) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(value, "Value must not be null");

		try {
			return connection.getCluster().rpushx(key, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long lPushX(byte @NonNull [] key, byte @NonNull [] value) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(value, "Value must not be null");

		try {
			return connection.getCluster().lpushx(key, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long lLen(byte @NonNull [] key) {

		Assert.notNull(key, "Key must not be null");

		try {
			return connection.getCluster().llen(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public List<byte @NonNull []> lRange(byte @NonNull [] key, long start, long end) {

		Assert.notNull(key, "Key must not be null");

		try {
			return connection.getCluster().lrange(key, start, end);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public void lTrim(byte @NonNull [] key, long start, long end) {

		Assert.notNull(key, "Key must not be null");

		try {
			connection.getCluster().ltrim(key, start, end);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public byte[] lIndex(byte @NonNull [] key, long index) {

		Assert.notNull(key, "Key must not be null");

		try {
			return connection.getCluster().lindex(key, index);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long lInsert(byte @NonNull [] key, @NonNull Position where, byte @NonNull [] pivot, byte @NonNull [] value) {

		Assert.notNull(key, "Key must not be null");

		try {
			return connection.getCluster().linsert(key, JedisConverters.toListPosition(where), pivot, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public byte[] lMove(byte @NonNull [] sourceKey, byte @NonNull [] destinationKey, @NonNull Direction from,
			@NonNull Direction to) {

		Assert.notNull(sourceKey, "Source key must not be null");
		Assert.notNull(destinationKey, "Destination key must not be null");
		Assert.notNull(from, "From direction must not be null");
		Assert.notNull(to, "To direction must not be null");

		try {
			return connection.getCluster().lmove(sourceKey, destinationKey, ListDirection.valueOf(from.name()),
					ListDirection.valueOf(to.name()));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public byte[] bLMove(byte @NonNull [] sourceKey, byte @NonNull [] destinationKey, @NonNull Direction from,
			@NonNull Direction to, double timeout) {

		Assert.notNull(sourceKey, "Source key must not be null");
		Assert.notNull(destinationKey, "Destination key must not be null");
		Assert.notNull(from, "From direction must not be null");
		Assert.notNull(to, "To direction must not be null");

		try {
			return connection.getCluster().blmove(sourceKey, destinationKey, ListDirection.valueOf(from.name()),
					ListDirection.valueOf(to.name()), timeout);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public void lSet(byte @NonNull [] key, long index, byte @NonNull [] value) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(value, "Value must not be null");

		try {
			connection.getCluster().lset(key, index, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long lRem(byte @NonNull [] key, long count, byte @NonNull [] value) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(value, "Value must not be null");

		try {
			return connection.getCluster().lrem(key, count, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public byte[] lPop(byte @NonNull [] key) {

		Assert.notNull(key, "Key must not be null");

		try {
			return connection.getCluster().lpop(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public List<byte @NonNull []> lPop(byte @NonNull [] key, long count) {

		Assert.notNull(key, "Key must not be null");

		try {
			return connection.getCluster().lpop(key, (int) count);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public byte[] rPop(byte[] key) {

		Assert.notNull(key, "Key must not be null");

		try {
			return connection.getCluster().rpop(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public List<byte @NonNull []> rPop(byte @NonNull [] key, long count) {

		Assert.notNull(key, "Key must not be null");

		try {
			return connection.getCluster().rpop(key, (int) count);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public List<byte @NonNull []> bLPop(double timeout, byte @NonNull [] @NonNull... keys) {

		Assert.notNull(keys, "Key must not be null");
		Assert.noNullElements(keys, "Keys must not contain null elements");

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(keys)) {
			try {
				return JedisConverters.kvToList(connection.getCluster().blpop(timeout, keys));
			} catch (Exception ex) {
				throw convertJedisAccessException(ex);
			}
		}

		return connection.getClusterCommandExecutor()
				.executeMultiKeyCommand(
						(JedisMultiKeyClusterCommandCallback<List<byte[]>>) (client, key) -> JedisConverters.kvToList(client.blpop(timeout, key)),
						Arrays.asList(keys))
				.getFirstNonNullNotEmptyOrDefault(Collections.<byte[]> emptyList());
	}

	@Override
	public List<byte @NonNull []> bRPop(double timeout, byte @NonNull [] @NonNull... keys) {

		Assert.notNull(keys, "Key must not be null");
		Assert.noNullElements(keys, "Keys must not contain null elements");

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(keys)) {
			try {
				return JedisConverters.kvToList(connection.getCluster().brpop(timeout, keys));
			} catch (Exception ex) {
				throw convertJedisAccessException(ex);
			}
		}

		return connection.getClusterCommandExecutor()
				.executeMultiKeyCommand(
						(JedisMultiKeyClusterCommandCallback<List<byte[]>>) (client, key) -> JedisConverters.kvToList(client.brpop(timeout, key)),
						Arrays.asList(keys))
				.getFirstNonNullNotEmptyOrDefault(Collections.<byte[]> emptyList());
	}

	@Override
	public byte[] rPopLPush(byte @NonNull [] srcKey, byte @NonNull [] dstKey) {

		Assert.notNull(srcKey, "Source key must not be null");
		Assert.notNull(dstKey, "Destination key must not be null");

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(srcKey, dstKey)) {
			try {
				return connection.getCluster().rpoplpush(srcKey, dstKey);
			} catch (Exception ex) {
				throw convertJedisAccessException(ex);
			}
		}

		byte[] val = rPop(srcKey);
		lPush(dstKey, val);
		return val;
	}

	@Override
	public byte[] bRPopLPush(double timeout, byte @NonNull [] srcKey, byte @NonNull [] dstKey) {

		Assert.notNull(srcKey, "Source key must not be null");
		Assert.notNull(dstKey, "Destination key must not be null");

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(srcKey, dstKey)) {
			try {
				return connection.getCluster().brpoplpush(srcKey, dstKey, Double.valueOf(timeout).intValue());
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

	private DataAccessException convertJedisAccessException(Exception ex) {
		return connection.convertJedisAccessException(ex);
	}
}
