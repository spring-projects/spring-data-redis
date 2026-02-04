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

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.NullUnmarked;
import org.jspecify.annotations.Nullable;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.BitFieldSubCommands;
import org.springframework.data.redis.connection.RedisStringCommands;
import org.springframework.data.redis.connection.convert.Converters;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.util.Assert;

import redis.clients.jedis.params.BitPosParams;
import redis.clients.jedis.params.SetParams;

import static org.springframework.data.redis.connection.jedis.JedisConverters.toBitOp;
import static org.springframework.data.redis.connection.jedis.JedisConverters.toBitfieldCommandArguments;

/**
 * @author Tihomir Mateev
 * @since 4.1
 */
@NullUnmarked
class JedisClientStringCommands implements RedisStringCommands {

	private final JedisClientConnection connection;

	JedisClientStringCommands(JedisClientConnection connection) {
		this.connection = connection;
	}

	@Override
	public byte[] get(byte @NonNull [] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.execute(client -> client.get(key), pipeline -> pipeline.get(key));
	}

	@Override
	public byte[] getDel(byte @NonNull [] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.execute(client -> client.getDel(key), pipeline -> pipeline.getDel(key));
	}

	@Override
	public byte[] getEx(byte @NonNull [] key, @NonNull Expiration expiration) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(expiration, "Expiration must not be null");

		return connection.execute(client -> client.getEx(key, JedisConverters.toGetExParams(expiration)),
				pipeline -> pipeline.getEx(key, JedisConverters.toGetExParams(expiration)));
	}

	@Override
	public byte[] getSet(byte @NonNull [] key, byte @NonNull [] value) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(value, "Value must not be null");

		return connection.execute(client -> client.setGet(key, value), pipeline -> pipeline.setGet(key, value));
	}

	@Override
	public List<byte[]> mGet(byte @NonNull [] @NonNull... keys) {

		Assert.notNull(keys, "Keys must not be null");
		Assert.noNullElements(keys, "Keys must not contain null elements");

		return connection.execute(client -> client.mget(keys), pipeline -> pipeline.mget(keys));
	}

	@Override
	public Boolean set(byte @NonNull [] key, byte @NonNull [] value) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(value, "Value must not be null");

		return connection.execute(client -> client.set(key, value), pipeline -> pipeline.set(key, value),
				Converters.stringToBooleanConverter());
	}

	@Override
	public Boolean set(byte @NonNull [] key, byte @NonNull [] value, @NonNull Expiration expiration,
			@NonNull SetOption option) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(value, "Value must not be null");
		Assert.notNull(expiration, "Expiration must not be null");
		Assert.notNull(option, "Option must not be null");

		SetParams params = JedisConverters.toSetCommandExPxArgument(expiration,
				JedisConverters.toSetCommandNxXxArgument(option));

		return connection.execute(client -> client.set(key, value, params), pipeline -> pipeline.set(key, value, params),
				Converters.stringToBooleanConverter(), () -> false);
	}

	@Override
	public byte @Nullable [] setGet(byte @NonNull [] key, byte @NonNull [] value, @NonNull Expiration expiration,
			@NonNull SetOption option) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(value, "Value must not be null");
		Assert.notNull(expiration, "Expiration must not be null");
		Assert.notNull(option, "Option must not be null");

		SetParams params = JedisConverters.toSetCommandExPxArgument(expiration,
				JedisConverters.toSetCommandNxXxArgument(option));

		return connection.execute(client -> client.setGet(key, value, params),
				pipeline -> pipeline.setGet(key, value, params));
	}

	@Override
	public Boolean setNX(byte @NonNull [] key, byte @NonNull [] value) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(value, "Value must not be null");

		return connection.execute(client -> client.setnx(key, value), pipeline -> pipeline.setnx(key, value),
				Converters.longToBoolean());
	}

	@Override
	public Boolean setEx(byte @NonNull [] key, long seconds, byte @NonNull [] value) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(value, "Value must not be null");

		if (seconds > Integer.MAX_VALUE) {
			throw new IllegalArgumentException("Time must be less than Integer.MAX_VALUE for setEx in Jedis");
		}

		return connection.execute(client -> client.setex(key, seconds, value),
				pipeline -> pipeline.setex(key, seconds, value), Converters.stringToBooleanConverter(), () -> false);
	}

	@Override
	public Boolean pSetEx(byte @NonNull [] key, long milliseconds, byte @NonNull [] value) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(value, "Value must not be null");

		return connection.execute(client -> client.psetex(key, milliseconds, value),
				pipeline -> pipeline.psetex(key, milliseconds, value), Converters.stringToBooleanConverter(), () -> false);
	}

	@Override
	public Boolean mSet(@NonNull Map<byte @NonNull [], byte @NonNull []> tuples) {

		Assert.notNull(tuples, "Tuples must not be null");

		return connection.execute(client -> client.mset(JedisConverters.toByteArrays(tuples)),
				pipeline -> pipeline.mset(JedisConverters.toByteArrays(tuples)), Converters.stringToBooleanConverter());
	}

	@Override
	public Boolean mSetNX(@NonNull Map<byte @NonNull [], byte @NonNull []> tuples) {

		Assert.notNull(tuples, "Tuples must not be null");

		return connection.execute(client -> client.msetnx(JedisConverters.toByteArrays(tuples)),
				pipeline -> pipeline.msetnx(JedisConverters.toByteArrays(tuples)), Converters.longToBoolean());
	}

	@Override
	public Long incr(byte @NonNull [] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.execute(client -> client.incr(key), pipeline -> pipeline.incr(key));
	}

	@Override
	public Long incrBy(byte @NonNull [] key, long value) {

		Assert.notNull(key, "Key must not be null");

		return connection.execute(client -> client.incrBy(key, value), pipeline -> pipeline.incrBy(key, value));
	}

	@Override
	public Double incrBy(byte @NonNull [] key, double value) {

		Assert.notNull(key, "Key must not be null");

		return connection.execute(client -> client.incrByFloat(key, value), pipeline -> pipeline.incrByFloat(key, value));
	}

	@Override
	public Long decr(byte @NonNull [] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.execute(client -> client.decr(key), pipeline -> pipeline.decr(key));
	}

	@Override
	public Long decrBy(byte @NonNull [] key, long value) {

		Assert.notNull(key, "Key must not be null");

		return connection.execute(client -> client.decrBy(key, value), pipeline -> pipeline.decrBy(key, value));
	}

	@Override
	public Long append(byte @NonNull [] key, byte @NonNull [] value) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(value, "Value must not be null");

		return connection.execute(client -> client.append(key, value), pipeline -> pipeline.append(key, value));
	}

	@Override
	public byte[] getRange(byte @NonNull [] key, long start, long end) {

		Assert.notNull(key, "Key must not be null");

		return connection.execute(client -> client.getrange(key, start, end),
				pipeline -> pipeline.getrange(key, start, end));
	}

	@Override
	public void setRange(byte @NonNull [] key, byte @NonNull [] value, long offset) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(value, "Value must not be null");

		connection.executeStatus(client -> client.setrange(key, offset, value),
				pipeline -> pipeline.setrange(key, offset, value));
	}

	@Override
	public Boolean getBit(byte @NonNull [] key, long offset) {

		Assert.notNull(key, "Key must not be null");

		return connection.execute(client -> client.getbit(key, offset), pipeline -> pipeline.getbit(key, offset));
	}

	@Override
	public Boolean setBit(byte @NonNull [] key, long offset, boolean value) {

		Assert.notNull(key, "Key must not be null");

		return connection.execute(client -> client.setbit(key, offset, value),
				pipeline -> pipeline.setbit(key, offset, value));
	}

	@Override
	public Long bitCount(byte @NonNull [] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.execute(client -> client.bitcount(key), pipeline -> pipeline.bitcount(key));
	}

	@Override
	public Long bitCount(byte @NonNull [] key, long start, long end) {

		Assert.notNull(key, "Key must not be null");

		return connection.execute(client -> client.bitcount(key, start, end),
				pipeline -> pipeline.bitcount(key, start, end));
	}

	@Override
	public List<Long> bitField(byte @NonNull [] key, @NonNull BitFieldSubCommands subCommands) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(subCommands, "Command must not be null");

		return connection.execute(client -> client.bitfield(key, toBitfieldCommandArguments(subCommands)),
				pipeline -> pipeline.bitfield(key, toBitfieldCommandArguments(subCommands)));
	}

	@Override
	public Long bitOp(@NonNull BitOperation op, byte @NonNull [] destination, byte @NonNull [] @NonNull... keys) {

		Assert.notNull(op, "BitOperation must not be null");
		Assert.notNull(destination, "Destination key must not be null");

		if (op == BitOperation.NOT && keys.length > 1) {
			throw new IllegalArgumentException("Bitop NOT should only be performed against one key");
		}

		return connection.execute(client -> client.bitop(toBitOp(op), destination, keys),
				pipeline -> pipeline.bitop(toBitOp(op), destination, keys));
	}

	@Override
	public Long bitPos(byte @NonNull [] key, boolean bit, @NonNull Range<@NonNull Long> range) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(range, "Range must not be null Use Range.unbounded() instead");

		if (range.getLowerBound().isBounded()) {

			Optional<@NonNull Long> lower = range.getLowerBound().getValue();
			Range.Bound<@NonNull Long> upper = range.getUpperBound();
			BitPosParams params = upper.isBounded() ? new BitPosParams(lower.orElse(0L), upper.getValue().orElse(0L))
					: new BitPosParams(lower.orElse(0L));

			return connection.execute(client -> client.bitpos(key, bit, params),
					pipeline -> pipeline.bitpos(key, bit, params));
		}

		return connection.execute(client -> client.bitpos(key, bit), pipeline -> pipeline.bitpos(key, bit));
	}

	@Override
	public Long strLen(byte @NonNull [] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.execute(client -> client.strlen(key), pipeline -> pipeline.strlen(key));
	}

}
