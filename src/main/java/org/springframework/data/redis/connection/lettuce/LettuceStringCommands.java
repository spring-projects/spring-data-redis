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
package org.springframework.data.redis.connection.lettuce;

import io.lettuce.core.BitFieldArgs;
import io.lettuce.core.api.async.RedisStringAsyncCommands;

import java.util.List;
import java.util.Map;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.NullUnmarked;
import org.jspecify.annotations.Nullable;

import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.BitFieldSubCommands;
import org.springframework.data.redis.connection.RedisStringCommands;
import org.springframework.data.redis.connection.convert.Converters;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.data.redis.util.KeyUtils;
import org.springframework.util.Assert;

/**
 * {@link RedisStringCommands} implementation for {@literal Lettuce}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author dengliming
 * @author John Blum
 * @author Marcin Grzejszczak
 * @author Viktoriya Kutsarova
 * @since 2.0
 */
@NullUnmarked
class LettuceStringCommands implements RedisStringCommands {

	private final LettuceConnection connection;

	LettuceStringCommands(@NonNull LettuceConnection connection) {
		this.connection = connection;
	}

	@Override
	public byte[] get(byte @NonNull [] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().just(RedisStringAsyncCommands::get, key);
	}

	@Override
	public byte[] getDel(byte @NonNull [] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().just(RedisStringAsyncCommands::getdel, key);
	}

	@Override
	public byte[] getEx(byte @NonNull [] key, @NonNull Expiration expiration) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(expiration, "Expiration must not be null");

		return connection.invoke().just(RedisStringAsyncCommands::getex, key, LettuceConverters.toGetExArgs(expiration));
	}

	@Override
	public byte[] getSet(byte @NonNull [] key, byte @NonNull [] value) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(value, "Value must not be null");

		return connection.invoke().just(RedisStringAsyncCommands::getset, key, value);
	}

	@Override
	public List<byte[]> mGet(byte @NonNull [] @NonNull... keys) {

		Assert.notNull(keys, "Keys must not be null");
		Assert.noNullElements(keys, "Keys must not contain null elements");

		return connection.invoke().fromMany(RedisStringAsyncCommands::mget, keys)
				.toList(source -> source.getValueOrElse(null));
	}

	@Override
	public Boolean set(byte @NonNull [] key, byte @NonNull [] value) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(value, "Value must not be null");

		return connection.invoke().from(RedisStringAsyncCommands::set, key, value)
				.get(Converters.stringToBooleanConverter());
	}

	@Override
	public Boolean set(byte @NonNull [] key, byte @NonNull [] value, @NonNull Expiration expiration,
			@NonNull SetOption option) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(value, "Value must not be null");
		Assert.notNull(expiration, "Expiration must not be null");
		Assert.notNull(option, "Option must not be null");

		return connection.invoke()
				.from(RedisStringAsyncCommands::set, key, value, LettuceConverters.toSetArgs(expiration, option))
				.orElse(LettuceConverters.stringToBooleanConverter(), false);
	}

	@Override
	public byte @Nullable [] setGet(byte @NonNull [] key, byte @NonNull [] value, @NonNull Expiration expiration,
			@NonNull SetOption option) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(value, "Value must not be null");
		Assert.notNull(expiration, "Expiration must not be null");
		Assert.notNull(option, "Option must not be null");

		return connection.invoke().just(RedisStringAsyncCommands::setGet, key, value,
				LettuceConverters.toSetArgs(expiration, option));
	}

	@Override
	public Boolean setNX(byte @NonNull [] key, byte @NonNull [] value) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(value, "Value must not be null");

		return connection.invoke().just(RedisStringAsyncCommands::setnx, key, value);
	}

	@Override
	public Boolean setEx(byte @NonNull [] key, long seconds, byte @NonNull [] value) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(value, "Value must not be null");

		return connection.invoke().from(RedisStringAsyncCommands::setex, key, seconds, value)
				.get(Converters.stringToBooleanConverter());
	}

	@Override
	public Boolean pSetEx(byte @NonNull [] key, long milliseconds, byte @NonNull [] value) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(value, "Value must not be null");

		return connection.invoke().from(RedisStringAsyncCommands::psetex, key, milliseconds, value)
				.get(Converters.stringToBooleanConverter());
	}

	@Override
	public Boolean mSet(@NonNull Map<byte @NonNull [], byte @NonNull []> tuples) {

		Assert.notNull(tuples, "Tuples must not be null");

		return connection.invoke().from(RedisStringAsyncCommands::mset, tuples).get(Converters.stringToBooleanConverter());
	}

	@Override
	public Boolean mSetNX(@NonNull Map<byte @NonNull [], byte @NonNull []> tuples) {

		Assert.notNull(tuples, "Tuples must not be null");

		return connection.invoke().just(RedisStringAsyncCommands::msetnx, tuples);
	}

	@Override
	public Long incr(byte @NonNull [] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().just(RedisStringAsyncCommands::incr, key);
	}

	@Override
	public Long incrBy(byte @NonNull [] key, long value) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().just(RedisStringAsyncCommands::incrby, key, value);
	}

	@Override
	public Double incrBy(byte @NonNull [] key, double value) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().just(RedisStringAsyncCommands::incrbyfloat, key, value);
	}

	@Override
	public Long decr(byte @NonNull [] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().just(RedisStringAsyncCommands::decr, key);
	}

	@Override
	public Long decrBy(byte @NonNull [] key, long value) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().just(RedisStringAsyncCommands::decrby, key, value);
	}

	@Override
	public Long append(byte @NonNull [] key, byte @NonNull [] value) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(value, "Value must not be null");

		return connection.invoke().just(RedisStringAsyncCommands::append, key, value);
	}

	@Override
	public byte[] getRange(byte @NonNull [] key, long start, long end) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().just(RedisStringAsyncCommands::getrange, key, start, end);
	}

	@Override
	public void setRange(byte @NonNull [] key, byte @NonNull [] value, long offset) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(value, "Value must not be null");

		connection.invokeStatus().just(RedisStringAsyncCommands::setrange, key, offset, value);
	}

	@Override
	public Boolean getBit(byte @NonNull [] key, long offset) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().from(RedisStringAsyncCommands::getbit, key, offset)
				.get(LettuceConverters.longToBoolean());
	}

	@Override
	public Boolean setBit(byte @NonNull [] key, long offset, boolean value) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().from(RedisStringAsyncCommands::setbit, key, offset, LettuceConverters.toInt(value))
				.get(LettuceConverters.longToBoolean());
	}

	@Override
	public Long bitCount(byte @NonNull [] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().just(RedisStringAsyncCommands::bitcount, key);
	}

	@Override
	public Long bitCount(byte @NonNull [] key, long start, long end) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().just(RedisStringAsyncCommands::bitcount, key, start, end);
	}

	@Override
	public List<Long> bitField(byte @NonNull [] key, @NonNull BitFieldSubCommands subCommands) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(subCommands, "Command must not be null");

		BitFieldArgs args = LettuceConverters.toBitFieldArgs(subCommands);

		return connection.invoke().just(RedisStringAsyncCommands::bitfield, key, args);
	}

	@Override
	public Long bitOp(@NonNull BitOperation op, byte @NonNull [] destination, byte @NonNull [] @NonNull... keys) {

		Assert.notNull(op, "BitOperation must not be null");
		Assert.notNull(destination, "Destination key must not be null");

		if (op == BitOperation.NOT && keys.length > 1) {
			throw new IllegalArgumentException("Bitop NOT should only be performed against one key");
		}

		return connection.invoke().just(it -> switch (op) {
			case AND -> it.bitopAnd(destination, keys);
			case OR -> it.bitopOr(destination, keys);
			case XOR -> it.bitopXor(destination, keys);
			case NOT -> {
				if (keys.length != 1) {
					throw new IllegalArgumentException("Bitop NOT should only be performed against one key");
				}
				yield it.bitopNot(destination, keys[0]);
			}
			case DIFF -> KeyUtils.splitKeys(keys, (first, remaining) -> it.bitopDiff(destination, first, remaining));
			case DIFF1 -> KeyUtils.splitKeys(keys, (first, remaining) -> it.bitopDiff1(destination, first, remaining));
			case ANDOR -> KeyUtils.splitKeys(keys, (first, remaining) -> it.bitopAndor(destination, first, remaining));
			case ONE -> it.bitopOne(destination, keys);
		});
	}

	@Override
	public Long bitPos(byte @NonNull [] key, boolean bit, @NonNull Range<Long> range) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(range, "Range must not be null Use Range.unbounded() instead");

		if (range.getLowerBound().isBounded()) {

			if (range.getUpperBound().isBounded()) {
				return connection.invoke().just(RedisStringAsyncCommands::bitpos, key, bit, getLowerValue(range),
						getUpperValue(range));
			}

			return connection.invoke().just(RedisStringAsyncCommands::bitpos, key, bit, getLowerValue(range));
		}

		return connection.invoke().just(RedisStringAsyncCommands::bitpos, key, bit);
	}

	@Override
	public Long strLen(byte @NonNull [] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().just(RedisStringAsyncCommands::strlen, key);
	}

	private static <T extends Comparable<T>> T getUpperValue(Range<T> range) {

		return range.getUpperBound().getValue()
				.orElseThrow(() -> new IllegalArgumentException("Range does not contain upper bound value"));
	}

	private static <T extends Comparable<T>> T getLowerValue(Range<T> range) {

		return range.getLowerBound().getValue()
				.orElseThrow(() -> new IllegalArgumentException("Range does not contain lower bound value"));
	}
}
