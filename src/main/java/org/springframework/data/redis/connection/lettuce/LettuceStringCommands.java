/*
 * Copyright 2017-2022 the original author or authors.
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

import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.BitFieldSubCommands;
import org.springframework.data.redis.connection.RedisStringCommands;
import org.springframework.data.redis.connection.convert.Converters;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author dengliming
 * @since 2.0
 */
class LettuceStringCommands implements RedisStringCommands {

	private final LettuceConnection connection;

	LettuceStringCommands(LettuceConnection connection) {
		this.connection = connection;
	}

	@Override
	public byte[] get(byte[] key) {

		Assert.notNull(key, "Key must not be null!");

		return connection.invoke().just(RedisStringAsyncCommands::get, key);
	}

	@Nullable
	@Override
	public byte[] getDel(byte[] key) {

		Assert.notNull(key, "Key must not be null!");

		return connection.invoke().just(RedisStringAsyncCommands::getdel, key);
	}

	@Nullable
	@Override
	public byte[] getEx(byte[] key, Expiration expiration) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(expiration, "Expiration must not be null!");

		return connection.invoke().just(RedisStringAsyncCommands::getex, key, LettuceConverters.toGetExArgs(expiration));
	}

	@Override
	public byte[] getSet(byte[] key, byte[] value) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(value, "Value must not be null!");

		return connection.invoke().just(RedisStringAsyncCommands::getset, key, value);
	}

	@Override
	public List<byte[]> mGet(byte[]... keys) {

		Assert.notNull(keys, "Keys must not be null!");
		Assert.noNullElements(keys, "Keys must not contain null elements!");

		return connection.invoke().fromMany(RedisStringAsyncCommands::mget, keys)
				.toList(source -> source.getValueOrElse(null));
	}

	@Override
	public Boolean set(byte[] key, byte[] value) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(value, "Value must not be null!");

		return connection.invoke().from(RedisStringAsyncCommands::set, key, value)
				.get(Converters.stringToBooleanConverter());
	}

	@Override
	public Boolean set(byte[] key, byte[] value, Expiration expiration, SetOption option) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(value, "Value must not be null!");
		Assert.notNull(expiration, "Expiration must not be null!");
		Assert.notNull(option, "Option must not be null!");

		return connection.invoke()
				.from(RedisStringAsyncCommands::set, key, value, LettuceConverters.toSetArgs(expiration, option))
				.orElse(LettuceConverters.stringToBooleanConverter(), false);
	}

	@Override
	public Boolean setNX(byte[] key, byte[] value) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(value, "Value must not be null!");

		return connection.invoke().just(RedisStringAsyncCommands::setnx, key, value);
	}

	@Override
	public Boolean setEx(byte[] key, long seconds, byte[] value) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(value, "Value must not be null!");

		return connection.invoke().from(RedisStringAsyncCommands::setex, key, seconds, value)
				.get(Converters.stringToBooleanConverter());
	}

	@Override
	public Boolean pSetEx(byte[] key, long milliseconds, byte[] value) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(value, "Value must not be null!");

		return connection.invoke().from(RedisStringAsyncCommands::psetex, key, milliseconds, value)
				.get(Converters.stringToBooleanConverter());
	}

	@Override
	public Boolean mSet(Map<byte[], byte[]> tuples) {

		Assert.notNull(tuples, "Tuples must not be null!");

		return connection.invoke().from(RedisStringAsyncCommands::mset, tuples).get(Converters.stringToBooleanConverter());
	}

	@Override
	public Boolean mSetNX(Map<byte[], byte[]> tuples) {

		Assert.notNull(tuples, "Tuples must not be null!");

		return connection.invoke().just(RedisStringAsyncCommands::msetnx, tuples);
	}

	@Override
	public Long incr(byte[] key) {

		Assert.notNull(key, "Key must not be null!");

		return connection.invoke().just(RedisStringAsyncCommands::incr, key);
	}

	@Override
	public Long incrBy(byte[] key, long value) {

		Assert.notNull(key, "Key must not be null!");

		return connection.invoke().just(RedisStringAsyncCommands::incrby, key, value);
	}

	@Override
	public Double incrBy(byte[] key, double value) {

		Assert.notNull(key, "Key must not be null!");

		return connection.invoke().just(RedisStringAsyncCommands::incrbyfloat, key, value);
	}

	@Override
	public Long decr(byte[] key) {

		Assert.notNull(key, "Key must not be null!");

		return connection.invoke().just(RedisStringAsyncCommands::decr, key);
	}

	@Override
	public Long decrBy(byte[] key, long value) {

		Assert.notNull(key, "Key must not be null!");

		return connection.invoke().just(RedisStringAsyncCommands::decrby, key, value);
	}

	@Override
	public Long append(byte[] key, byte[] value) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(value, "Value must not be null!");

		return connection.invoke().just(RedisStringAsyncCommands::append, key, value);
	}

	@Override
	public byte[] getRange(byte[] key, long start, long end) {

		Assert.notNull(key, "Key must not be null!");

		return connection.invoke().just(RedisStringAsyncCommands::getrange, key, start, end);
	}

	@Override
	public void setRange(byte[] key, byte[] value, long offset) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(value, "Value must not be null!");

		connection.invokeStatus().just(RedisStringAsyncCommands::setrange, key, offset, value);
	}

	@Override
	public Boolean getBit(byte[] key, long offset) {

		Assert.notNull(key, "Key must not be null!");

		return connection.invoke().from(RedisStringAsyncCommands::getbit, key, offset)
				.get(LettuceConverters.longToBoolean());
	}

	@Override
	public Boolean setBit(byte[] key, long offset, boolean value) {

		Assert.notNull(key, "Key must not be null!");

		return connection.invoke().from(RedisStringAsyncCommands::setbit, key, offset, LettuceConverters.toInt(value))
				.get(LettuceConverters.longToBoolean());
	}

	@Override
	public Long bitCount(byte[] key) {

		Assert.notNull(key, "Key must not be null!");

		return connection.invoke().just(RedisStringAsyncCommands::bitcount, key);
	}

	@Override
	public Long bitCount(byte[] key, long start, long end) {

		Assert.notNull(key, "Key must not be null!");

		return connection.invoke().just(RedisStringAsyncCommands::bitcount, key, start, end);
	}

	@Override
	public List<Long> bitField(byte[] key, BitFieldSubCommands subCommands) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(subCommands, "Command must not be null!");

		BitFieldArgs args = LettuceConverters.toBitFieldArgs(subCommands);

		return connection.invoke().just(RedisStringAsyncCommands::bitfield, key, args);
	}

	@Override
	public Long bitOp(BitOperation op, byte[] destination, byte[]... keys) {

		Assert.notNull(op, "BitOperation must not be null!");
		Assert.notNull(destination, "Destination key must not be null!");

		if (op == BitOperation.NOT && keys.length > 1) {
			throw new IllegalArgumentException("Bitop NOT should only be performed against one key");
		}

		return connection.invoke().just(it -> {

			switch (op) {
				case AND:
					return it.bitopAnd(destination, keys);
				case OR:
					return it.bitopOr(destination, keys);
				case XOR:
					return it.bitopXor(destination, keys);
				case NOT:
					if (keys.length != 1) {
						throw new IllegalArgumentException("Bitop NOT should only be performed against one key");
					}
					return it.bitopNot(destination, keys[0]);
				default:
					throw new UnsupportedOperationException("Bit operation " + op + " is not supported");
			}
		});
	}

	@Nullable
	@Override
	public Long bitPos(byte[] key, boolean bit, Range<Long> range) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(range, "Range must not be null! Use Range.unbounded() instead.");

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
	public Long strLen(byte[] key) {

		Assert.notNull(key, "Key must not be null!");

		return connection.invoke().just(RedisStringAsyncCommands::strlen, key);
	}

	private static <T extends Comparable<T>> T getUpperValue(Range<T> range) {
		return range.getUpperBound().getValue()
				.orElseThrow(() -> new IllegalArgumentException("Range does not contain upper bound value!"));
	}

	private static <T extends Comparable<T>> T getLowerValue(Range<T> range) {
		return range.getLowerBound().getValue()
				.orElseThrow(() -> new IllegalArgumentException("Range does not contain lower bound value!"));
	}
}
