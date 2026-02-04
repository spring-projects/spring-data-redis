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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.NullUnmarked;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.BitFieldSubCommands;
import org.springframework.data.redis.connection.ClusterSlotHashUtil;
import org.springframework.data.redis.connection.RedisStringCommands;
import org.springframework.data.redis.connection.convert.Converters;
import org.springframework.data.redis.connection.jedis.JedisClientClusterConnection.JedisClientMultiKeyClusterCommandCallback;
import org.springframework.data.redis.connection.lettuce.LettuceConverters;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.data.redis.util.ByteUtils;
import org.springframework.util.Assert;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.SetParams;

/**
 * @author Tihomir Mateev
 * @since 4.1
 */
@NullUnmarked
class JedisClientClusterStringCommands implements RedisStringCommands {

	private final JedisClientClusterConnection connection;

	JedisClientClusterStringCommands(@NonNull JedisClientClusterConnection connection) {
		this.connection = connection;
	}

	@Override
	public byte[] get(byte @NonNull [] key) {

		Assert.notNull(key, "Key must not be null");

		try {
			return connection.getClusterClient().get(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public byte[] getDel(byte @NonNull [] key) {

		Assert.notNull(key, "Key must not be null");

		try {
			return connection.getClusterClient().getDel(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public byte[] getEx(byte @NonNull [] key, @NonNull Expiration expiration) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(expiration, "Expiration must not be null");

		try {
			return connection.getClusterClient().getEx(key, JedisConverters.toGetExParams(expiration));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public byte[] getSet(byte @NonNull [] key, byte @NonNull [] value) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(value, "Value must not be null");

		try {
			return connection.getClusterClient().setGet(key, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public List<byte[]> mGet(byte @NonNull [] @NonNull... keys) {

		Assert.notNull(keys, "Keys must not be null");
		Assert.noNullElements(keys, "Keys must not contain null elements");

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(keys)) {
			return connection.getClusterClient().mget(keys);
		}

		return connection.getClusterCommandExecutor()
				.executeMultiKeyCommand((JedisClientMultiKeyClusterCommandCallback<byte[]>) Jedis::get, Arrays.asList(keys))
				.resultsAsListSortBy(keys);
	}

	@Override
	public Boolean set(byte @NonNull [] key, byte @NonNull [] value) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(value, "Value must not be null");

		try {
			return Converters.stringToBoolean(connection.getClusterClient().set(key, value));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Boolean set(byte @NonNull [] key, byte @NonNull [] value, @NonNull Expiration expiration,
			@NonNull SetOption option) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(value, "Value must not be null");
		Assert.notNull(expiration, "Expiration must not be null");
		Assert.notNull(option, "Option must not be null");

		SetParams setParams = JedisConverters.toSetCommandExPxArgument(expiration,
				JedisConverters.toSetCommandNxXxArgument(option));

		try {
			return Converters.stringToBoolean(connection.getClusterClient().set(key, value, setParams));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public byte[] setGet(byte @NonNull [] key, byte @NonNull [] value, @NonNull Expiration expiration,
			@NonNull SetOption option) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(value, "Value must not be null");
		Assert.notNull(expiration, "Expiration must not be null");
		Assert.notNull(option, "Option must not be null");

		SetParams setParams = JedisConverters.toSetCommandExPxArgument(expiration,
				JedisConverters.toSetCommandNxXxArgument(option));

		try {
			return connection.getClusterClient().setGet(key, value, setParams);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Boolean setNX(byte @NonNull [] key, byte @NonNull [] value) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(value, "Value must not be null");

		try {
			return JedisConverters.toBoolean(connection.getClusterClient().setnx(key, value));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Boolean setEx(byte @NonNull [] key, long seconds, byte @NonNull [] value) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(value, "Value must not be null");

		if (seconds > Integer.MAX_VALUE) {
			throw new IllegalArgumentException("Seconds have cannot exceed Integer.MAX_VALUE");
		}

		try {
			return Converters
					.stringToBoolean(connection.getClusterClient().setex(key, Long.valueOf(seconds).intValue(), value));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Boolean pSetEx(byte @NonNull [] key, long milliseconds, byte @NonNull [] value) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(value, "Value must not be null");

		try {
			return Converters.stringToBoolean(connection.getClusterClient().psetex(key, milliseconds, value));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Boolean mSet(@NonNull Map<byte @NonNull [], byte @NonNull []> tuples) {

		Assert.notNull(tuples, "Tuples must not be null");

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(tuples.keySet().toArray(new byte[tuples.size()][]))) {
			try {
				return Converters.stringToBoolean(connection.getClusterClient().mset(JedisConverters.toByteArrays(tuples)));
			} catch (Exception ex) {
				throw convertJedisAccessException(ex);
			}
		}

		boolean result = true;
		for (Map.Entry<byte[], byte[]> entry : tuples.entrySet()) {
			if (!set(entry.getKey(), entry.getValue())) {
				result = false;
			}
		}
		return result;
	}

	@Override
	public Boolean mSetNX(@NonNull Map<byte @NonNull [], byte @NonNull []> tuples) {

		Assert.notNull(tuples, "Tuples must not be null");

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(tuples.keySet().toArray(new byte[tuples.size()][]))) {
			try {
				return JedisConverters.toBoolean(connection.getClusterClient().msetnx(JedisConverters.toByteArrays(tuples)));
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

	@Override
	public Long incr(byte @NonNull [] key) {

		Assert.notNull(key, "Key must not be null");

		try {
			return connection.getClusterClient().incr(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long incrBy(byte @NonNull [] key, long value) {

		Assert.notNull(key, "Key must not be null");

		try {
			return connection.getClusterClient().incrBy(key, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Double incrBy(byte @NonNull [] key, double value) {

		Assert.notNull(key, "Key must not be null");

		try {
			return connection.getClusterClient().incrByFloat(key, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long decr(byte @NonNull [] key) {

		Assert.notNull(key, "Key must not be null");

		try {
			return connection.getClusterClient().decr(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long decrBy(byte @NonNull [] key, long value) {

		Assert.notNull(key, "Key must not be null");

		try {
			return connection.getClusterClient().decrBy(key, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long append(byte @NonNull [] key, byte[] value) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(value, "Value must not be null");

		try {
			return connection.getClusterClient().append(key, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public byte[] getRange(byte @NonNull [] key, long start, long end) {

		Assert.notNull(key, "Key must not be null");

		try {
			return connection.getClusterClient().getrange(key, start, end);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public void setRange(byte @NonNull [] key, byte @NonNull [] value, long offset) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(value, "Value must not be null");

		try {
			connection.getClusterClient().setrange(key, offset, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Boolean getBit(byte @NonNull [] key, long offset) {

		Assert.notNull(key, "Key must not be null");

		try {
			return connection.getClusterClient().getbit(key, offset);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Boolean setBit(byte @NonNull [] key, long offset, boolean value) {

		Assert.notNull(key, "Key must not be null");

		try {
			return connection.getClusterClient().setbit(key, offset, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long bitCount(byte @NonNull [] key) {

		Assert.notNull(key, "Key must not be null");

		try {
			return connection.getClusterClient().bitcount(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long bitCount(byte @NonNull [] key, long start, long end) {

		Assert.notNull(key, "Key must not be null");

		try {
			return connection.getClusterClient().bitcount(key, start, end);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public List<Long> bitField(byte @NonNull [] key, @NonNull BitFieldSubCommands subCommands) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(subCommands, "Command must not be null");

		byte[][] args = JedisConverters.toBitfieldCommandArguments(subCommands);

		try {
			return connection.getClusterClient().bitfield(key, args);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long bitOp(@NonNull BitOperation op, byte @NonNull [] destination, byte @NonNull [] @NonNull... keys) {

		Assert.notNull(op, "BitOperation must not be null");
		Assert.notNull(destination, "Destination key must not be null");

		byte[][] allKeys = ByteUtils.mergeArrays(destination, keys);

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(allKeys)) {
			try {
				return connection.getClusterClient().bitop(JedisConverters.toBitOp(op), destination, keys);
			} catch (Exception ex) {
				throw convertJedisAccessException(ex);
			}
		}

		throw new InvalidDataAccessApiUsageException("BITOP is only supported for same slot keys in cluster mode");
	}

	@Override
	public Long bitPos(byte @NonNull [] key, boolean bit, @NonNull Range<@NonNull Long> range) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(range, "Range must not be null Use Range.unbounded() instead");

		List<byte[]> args = new ArrayList<>(3);
		args.add(LettuceConverters.toBit(bit));

		if (range.getLowerBound().isBounded()) {
			args.add(range.getLowerBound().getValue().map(LettuceConverters::toBytes).orElseGet(() -> new byte[0]));
		}
		if (range.getUpperBound().isBounded()) {
			args.add(range.getUpperBound().getValue().map(LettuceConverters::toBytes).orElseGet(() -> new byte[0]));
		}

		return connection.execute("BITPOS", key, args);
	}

	@Override
	public Long strLen(byte @NonNull [] key) {
		Assert.notNull(key, "Key must not be null");

		try {
			return connection.getClusterClient().strlen(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	private DataAccessException convertJedisAccessException(Exception ex) {
		return connection.convertJedisAccessException(ex);
	}

}
