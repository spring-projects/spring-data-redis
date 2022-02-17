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
package org.springframework.data.redis.connection.jedis;

import redis.clients.jedis.BinaryJedis;
import redis.clients.jedis.BitPosParams;
import redis.clients.jedis.MultiKeyPipelineBase;
import redis.clients.jedis.params.SetParams;

import java.util.List;
import java.util.Map;

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
class JedisStringCommands implements RedisStringCommands {

	private final JedisConnection connection;

	JedisStringCommands(JedisConnection connection) {
		this.connection = connection;
	}

	@Override
	public byte[] get(byte[] key) {

		Assert.notNull(key, "Key must not be null!");

		return connection.invoke().just(BinaryJedis::get, MultiKeyPipelineBase::get, key);
	}

	@Nullable
	@Override
	public byte[] getDel(byte[] key) {

		Assert.notNull(key, "Key must not be null!");

		return connection.invoke().just(BinaryJedis::getDel, MultiKeyPipelineBase::getDel, key);
	}

	@Nullable
	@Override
	public byte[] getEx(byte[] key, Expiration expiration) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(expiration, "Expiration must not be null!");

		return connection.invoke().just(BinaryJedis::getEx, MultiKeyPipelineBase::getEx, key,
				JedisConverters.toGetExParams(expiration));
	}

	@Override
	public byte[] getSet(byte[] key, byte[] value) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(value, "Value must not be null!");

		return connection.invoke().just(BinaryJedis::getSet, MultiKeyPipelineBase::getSet, key, value);
	}

	@Override
	public List<byte[]> mGet(byte[]... keys) {

		Assert.notNull(keys, "Keys must not be null!");
		Assert.noNullElements(keys, "Keys must not contain null elements!");

		return connection.invoke().just(BinaryJedis::mget, MultiKeyPipelineBase::mget, keys);
	}

	@Override
	public Boolean set(byte[] key, byte[] value) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(value, "Value must not be null!");

		return connection.invoke().from(BinaryJedis::set, MultiKeyPipelineBase::set, key, value)
				.get(Converters.stringToBooleanConverter());
	}

	@Override
	public Boolean set(byte[] key, byte[] value, Expiration expiration, SetOption option) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(value, "Value must not be null!");
		Assert.notNull(expiration, "Expiration must not be null!");
		Assert.notNull(option, "Option must not be null!");

		SetParams params = JedisConverters.toSetCommandExPxArgument(expiration,
				JedisConverters.toSetCommandNxXxArgument(option));

		return connection.invoke().from(BinaryJedis::set, MultiKeyPipelineBase::set, key, value, params)
				.getOrElse(Converters.stringToBooleanConverter(), () -> false);
	}

	@Override
	public Boolean setNX(byte[] key, byte[] value) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(value, "Value must not be null!");

		return connection.invoke().from(BinaryJedis::setnx, MultiKeyPipelineBase::setnx, key, value)
				.get(Converters.longToBoolean());
	}

	@Override
	public Boolean setEx(byte[] key, long seconds, byte[] value) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(value, "Value must not be null!");

		if (seconds > Integer.MAX_VALUE) {
			throw new IllegalArgumentException("Time must be less than Integer.MAX_VALUE for setEx in Jedis.");
		}

		return connection.invoke().from(BinaryJedis::setex, MultiKeyPipelineBase::setex, key, (int) seconds, value)
				.getOrElse(Converters.stringToBooleanConverter(), () -> false);
	}

	@Override
	public Boolean pSetEx(byte[] key, long milliseconds, byte[] value) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(value, "Value must not be null!");

		return connection.invoke().from(BinaryJedis::psetex, MultiKeyPipelineBase::psetex, key, milliseconds, value)
				.getOrElse(Converters.stringToBooleanConverter(), () -> false);
	}

	@Override
	public Boolean mSet(Map<byte[], byte[]> tuples) {

		Assert.notNull(tuples, "Tuples must not be null!");

		return connection.invoke().from(BinaryJedis::mset, MultiKeyPipelineBase::mset, JedisConverters.toByteArrays(tuples))
				.get(Converters.stringToBooleanConverter());
	}

	@Override
	public Boolean mSetNX(Map<byte[], byte[]> tuples) {

		Assert.notNull(tuples, "Tuples must not be null!");

		return connection.invoke()
				.from(BinaryJedis::msetnx, MultiKeyPipelineBase::msetnx, JedisConverters.toByteArrays(tuples))
				.get(Converters.longToBoolean());
	}

	@Override
	public Long incr(byte[] key) {

		Assert.notNull(key, "Key must not be null!");

		return connection.invoke().just(BinaryJedis::incr, MultiKeyPipelineBase::incr, key);
	}

	@Override
	public Long incrBy(byte[] key, long value) {

		Assert.notNull(key, "Key must not be null!");

		return connection.invoke().just(BinaryJedis::incrBy, MultiKeyPipelineBase::incrBy, key, value);
	}

	@Override
	public Double incrBy(byte[] key, double value) {

		Assert.notNull(key, "Key must not be null!");

		return connection.invoke().just(BinaryJedis::incrByFloat, MultiKeyPipelineBase::incrByFloat, key, value);
	}

	@Override
	public Long decr(byte[] key) {

		Assert.notNull(key, "Key must not be null!");

		return connection.invoke().just(BinaryJedis::decr, MultiKeyPipelineBase::decr, key);
	}

	@Override
	public Long decrBy(byte[] key, long value) {

		Assert.notNull(key, "Key must not be null!");

		return connection.invoke().just(BinaryJedis::decrBy, MultiKeyPipelineBase::decrBy, key, value);
	}

	@Override
	public Long append(byte[] key, byte[] value) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(value, "Value must not be null!");

		return connection.invoke().just(BinaryJedis::append, MultiKeyPipelineBase::append, key, value);
	}

	@Override
	public byte[] getRange(byte[] key, long start, long end) {

		Assert.notNull(key, "Key must not be null!");

		return connection.invoke().just(BinaryJedis::getrange, MultiKeyPipelineBase::getrange, key, start, end);
	}

	@Override
	public void setRange(byte[] key, byte[] value, long offset) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(value, "Value must not be null!");

		connection.invokeStatus().just(BinaryJedis::setrange, MultiKeyPipelineBase::setrange, key, offset, value);
	}

	@Override
	public Boolean getBit(byte[] key, long offset) {

		Assert.notNull(key, "Key must not be null!");

		return connection.invoke().just(BinaryJedis::getbit, MultiKeyPipelineBase::getbit, key, offset);
	}

	@Override
	public Boolean setBit(byte[] key, long offset, boolean value) {

		Assert.notNull(key, "Key must not be null!");

		return connection.invoke().just(BinaryJedis::setbit, MultiKeyPipelineBase::setbit, key, offset,
				JedisConverters.toBit(value));
	}

	@Override
	public Long bitCount(byte[] key) {

		Assert.notNull(key, "Key must not be null!");

		return connection.invoke().just(BinaryJedis::bitcount, MultiKeyPipelineBase::bitcount, key);
	}

	@Override
	public Long bitCount(byte[] key, long start, long end) {

		Assert.notNull(key, "Key must not be null!");

		return connection.invoke().just(BinaryJedis::bitcount, MultiKeyPipelineBase::bitcount, key, start, end);
	}

	@Override
	public List<Long> bitField(byte[] key, BitFieldSubCommands subCommands) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(subCommands, "Command must not be null!");

		return connection.invoke().just(BinaryJedis::bitfield, MultiKeyPipelineBase::bitfield, key,
				JedisConverters.toBitfieldCommandArguments(subCommands));
	}

	@Override
	public Long bitOp(BitOperation op, byte[] destination, byte[]... keys) {

		Assert.notNull(op, "BitOperation must not be null!");
		Assert.notNull(destination, "Destination key must not be null!");

		if (op == BitOperation.NOT && keys.length > 1) {
			throw new UnsupportedOperationException("Bitop NOT should only be performed against one key");
		}

		return connection.invoke().just(BinaryJedis::bitop, MultiKeyPipelineBase::bitop, JedisConverters.toBitOp(op),
				destination, keys);
	}

	@Nullable
	@Override
	public Long bitPos(byte[] key, boolean bit, Range<Long> range) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(range, "Range must not be null! Use Range.unbounded() instead.");

		if (range.getLowerBound().isBounded()) {

			BitPosParams params = range.getUpperBound().isBounded()
					? new BitPosParams(range.getLowerBound().getValue().get(), range.getUpperBound().getValue().get())
					: new BitPosParams(range.getLowerBound().getValue().get());

			return connection.invoke().just(BinaryJedis::bitpos, MultiKeyPipelineBase::bitpos, key, bit, params);
		}

		return connection.invoke().just(BinaryJedis::bitpos, MultiKeyPipelineBase::bitpos, key, bit);
	}

	@Override
	public Long strLen(byte[] key) {

		Assert.notNull(key, "Key must not be null!");

		return connection.invoke().just(BinaryJedis::strlen, MultiKeyPipelineBase::strlen, key);
	}

}
