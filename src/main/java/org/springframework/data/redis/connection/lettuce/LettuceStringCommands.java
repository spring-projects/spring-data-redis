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
package org.springframework.data.redis.connection.lettuce;

import io.lettuce.core.BitFieldArgs;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import org.springframework.dao.DataAccessException;
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
 * @since 2.0
 */
@RequiredArgsConstructor
class LettuceStringCommands implements RedisStringCommands {

	private final @NonNull LettuceConnection connection;

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStringCommands#get(byte[])
	 */
	@Override
	public byte[] get(byte[] key) {

		Assert.notNull(key, "Key must not be null!");

		try {
			if (isPipelined()) {
				pipeline(connection.newLettuceResult(getAsyncConnection().get(key)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceResult(getAsyncConnection().get(key)));
				return null;
			}
			return getConnection().get(key);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStringCommands#getSet(byte[], byte[])
	 */
	@Override
	public byte[] getSet(byte[] key, byte[] value) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(value, "Value must not be null!");

		try {
			if (isPipelined()) {
				pipeline(connection.newLettuceResult(getAsyncConnection().getset(key, value)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceResult(getAsyncConnection().getset(key, value)));
				return null;
			}
			return getConnection().getset(key, value);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStringCommands#mGet(byte[][])
	 */
	@Override
	public List<byte[]> mGet(byte[]... keys) {

		Assert.notNull(keys, "Keys must not be null!");
		Assert.noNullElements(keys, "Keys must not contain null elements!");

		try {
			if (isPipelined()) {
				pipeline(
						connection.newLettuceResult(getAsyncConnection().mget(keys), LettuceConverters.keyValueListUnwrapper()));
				return null;
			}
			if (isQueueing()) {
				transaction(
						connection.newLettuceResult(getAsyncConnection().mget(keys), LettuceConverters.keyValueListUnwrapper()));
				return null;
			}

			return LettuceConverters.<byte[], byte[]> keyValueListUnwrapper().convert(getConnection().mget(keys));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStringCommands#set(byte[], byte[])
	 */
	@Override
	public Boolean set(byte[] key, byte[] value) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(value, "Value must not be null!");

		try {
			if (isPipelined()) {
				pipeline(
						connection.newLettuceResult(getAsyncConnection().set(key, value), Converters.stringToBooleanConverter()));
				return null;
			}
			if (isQueueing()) {
				transaction(
						connection.newLettuceResult(getAsyncConnection().set(key, value), Converters.stringToBooleanConverter()));
				return null;
			}
			return Converters.stringToBoolean(getConnection().set(key, value));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStringCommands#set(byte[], byte[], org.springframework.data.redis.core.types.Expiration, org.springframework.data.redis.connection.RedisStringCommands.SetOption)
	 */
	@Override
	public Boolean set(byte[] key, byte[] value, Expiration expiration, SetOption option) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(value, "Value must not be null!");
		Assert.notNull(expiration, "Expiration must not be null!");
		Assert.notNull(option, "Option must not be null!");

		try {
			if (isPipelined()) {
				pipeline(connection.newLettuceResult(
						getAsyncConnection().set(key, value, LettuceConverters.toSetArgs(expiration, option)),
						Converters.stringToBooleanConverter(), () -> false));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceResult(
						getAsyncConnection().set(key, value, LettuceConverters.toSetArgs(expiration, option)),
						Converters.stringToBooleanConverter(), () -> false));
				return null;
			}
			return Converters
					.stringToBoolean(getConnection().set(key, value, LettuceConverters.toSetArgs(expiration, option)));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStringCommands#setNX(byte[], byte[])
	 */
	@Override
	public Boolean setNX(byte[] key, byte[] value) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(value, "Value must not be null!");

		try {
			if (isPipelined()) {
				pipeline(connection.newLettuceResult(getAsyncConnection().setnx(key, value)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceResult(getAsyncConnection().setnx(key, value)));
				return null;
			}
			return getConnection().setnx(key, value);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStringCommands#setEx(byte[], long, byte[])
	 */
	@Override
	public Boolean setEx(byte[] key, long seconds, byte[] value) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(value, "Value must not be null!");

		try {
			if (isPipelined()) {
				pipeline(connection.newLettuceResult(getAsyncConnection().setex(key, seconds, value),
						Converters.stringToBooleanConverter()));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceResult(getAsyncConnection().setex(key, seconds, value),
						Converters.stringToBooleanConverter()));
				return null;
			}
			return Converters.stringToBoolean(getConnection().setex(key, seconds, value));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStringCommands#pSetEx(byte[], long, byte[])
	 */
	@Override
	public Boolean pSetEx(byte[] key, long milliseconds, byte[] value) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(value, "Value must not be null!");

		try {
			if (isPipelined()) {
				pipeline(connection.newLettuceResult(getAsyncConnection().psetex(key, milliseconds, value),
						Converters.stringToBooleanConverter()));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceResult(getAsyncConnection().psetex(key, milliseconds, value),
						Converters.stringToBooleanConverter()));
				return null;
			}
			return Converters.stringToBoolean(getConnection().psetex(key, milliseconds, value));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStringCommands#mSet(java.util.Map)
	 */
	@Override
	public Boolean mSet(Map<byte[], byte[]> tuples) {

		Assert.notNull(tuples, "Tuples must not be null!");

		try {
			if (isPipelined()) {
				pipeline(connection.newLettuceResult(getAsyncConnection().mset(tuples), Converters.stringToBooleanConverter()));
				return null;
			}
			if (isQueueing()) {
				transaction(
						connection.newLettuceResult(getAsyncConnection().mset(tuples), Converters.stringToBooleanConverter()));
				return null;
			}
			return Converters.stringToBoolean(getConnection().mset(tuples));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStringCommands#mSetNX(java.util.Map)
	 */
	@Override
	public Boolean mSetNX(Map<byte[], byte[]> tuples) {

		Assert.notNull(tuples, "Tuples must not be null!");

		try {
			if (isPipelined()) {
				pipeline(connection.newLettuceResult(getAsyncConnection().msetnx(tuples)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceResult(getAsyncConnection().msetnx(tuples)));
				return null;
			}
			return getConnection().msetnx(tuples);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStringCommands#incr(byte[])
	 */
	@Override
	public Long incr(byte[] key) {

		Assert.notNull(key, "Key must not be null!");

		try {
			if (isPipelined()) {
				pipeline(connection.newLettuceResult(getAsyncConnection().incr(key)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceResult(getAsyncConnection().incr(key)));
				return null;
			}
			return getConnection().incr(key);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStringCommands#incrBy(byte[], long)
	 */
	@Override
	public Long incrBy(byte[] key, long value) {

		Assert.notNull(key, "Key must not be null!");

		try {
			if (isPipelined()) {
				pipeline(connection.newLettuceResult(getAsyncConnection().incrby(key, value)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceResult(getAsyncConnection().incrby(key, value)));
				return null;
			}
			return getConnection().incrby(key, value);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStringCommands#incrBy(byte[], double)
	 */
	@Override
	public Double incrBy(byte[] key, double value) {

		Assert.notNull(key, "Key must not be null!");

		try {
			if (isPipelined()) {
				pipeline(connection.newLettuceResult(getAsyncConnection().incrbyfloat(key, value)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceResult(getAsyncConnection().incrbyfloat(key, value)));
				return null;
			}
			return getConnection().incrbyfloat(key, value);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStringCommands#decr(byte[])
	 */
	@Override
	public Long decr(byte[] key) {

		Assert.notNull(key, "Key must not be null!");

		try {
			if (isPipelined()) {
				pipeline(connection.newLettuceResult(getAsyncConnection().decr(key)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceResult(getAsyncConnection().decr(key)));
				return null;
			}
			return getConnection().decr(key);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStringCommands#decrBy(byte[], long)
	 */
	@Override
	public Long decrBy(byte[] key, long value) {

		Assert.notNull(key, "Key must not be null!");

		try {
			if (isPipelined()) {
				pipeline(connection.newLettuceResult(getAsyncConnection().decrby(key, value)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceResult(getAsyncConnection().decrby(key, value)));
				return null;
			}
			return getConnection().decrby(key, value);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStringCommands#append(byte[], byte[])
	 */
	@Override
	public Long append(byte[] key, byte[] value) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(value, "Value must not be null!");

		try {
			if (isPipelined()) {
				pipeline(connection.newLettuceResult(getAsyncConnection().append(key, value)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceResult(getAsyncConnection().append(key, value)));
				return null;
			}
			return getConnection().append(key, value);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStringCommands#getRange(byte[], long, long)
	 */
	@Override
	public byte[] getRange(byte[] key, long start, long end) {

		Assert.notNull(key, "Key must not be null!");

		try {
			if (isPipelined()) {
				pipeline(connection.newLettuceResult(getAsyncConnection().getrange(key, start, end)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceResult(getAsyncConnection().getrange(key, start, end)));
				return null;
			}
			return getConnection().getrange(key, start, end);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStringCommands#setRange(byte[], byte[], long)
	 */
	@Override
	public void setRange(byte[] key, byte[] value, long offset) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(value, "Value must not be null!");

		try {
			if (isPipelined()) {
				pipeline(connection.newLettuceStatusResult(getAsyncConnection().setrange(key, offset, value)));
				return;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceStatusResult(getAsyncConnection().setrange(key, offset, value)));
				return;
			}
			getConnection().setrange(key, offset, value);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStringCommands#getBit(byte[], long)
	 */
	@Override
	public Boolean getBit(byte[] key, long offset) {

		Assert.notNull(key, "Key must not be null!");

		try {
			if (isPipelined()) {
				pipeline(
						connection.newLettuceResult(getAsyncConnection().getbit(key, offset), LettuceConverters.longToBoolean()));
				return null;
			}
			if (isQueueing()) {
				transaction(
						connection.newLettuceResult(getAsyncConnection().getbit(key, offset), LettuceConverters.longToBoolean()));
				return null;
			}
			return LettuceConverters.toBoolean(getConnection().getbit(key, offset));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStringCommands#setBit(byte[], long, boolean)
	 */
	@Override
	public Boolean setBit(byte[] key, long offset, boolean value) {

		Assert.notNull(key, "Key must not be null!");

		try {
			if (isPipelined()) {
				pipeline(connection.newLettuceResult(getAsyncConnection().setbit(key, offset, LettuceConverters.toInt(value)),
						LettuceConverters.longToBooleanConverter()));
				return null;
			}
			if (isQueueing()) {
				transaction(
						connection.newLettuceResult(getAsyncConnection().setbit(key, offset, LettuceConverters.toInt(value)),
								LettuceConverters.longToBooleanConverter()));
				return null;
			}
			return LettuceConverters.longToBooleanConverter()
					.convert(getConnection().setbit(key, offset, LettuceConverters.toInt(value)));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStringCommands#bitCount(byte[])
	 */
	@Override
	public Long bitCount(byte[] key) {

		Assert.notNull(key, "Key must not be null!");

		try {
			if (isPipelined()) {
				pipeline(connection.newLettuceResult(getAsyncConnection().bitcount(key)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceResult(getAsyncConnection().bitcount(key)));
				return null;
			}
			return getConnection().bitcount(key);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStringCommands#bitCount(byte[], long, long)
	 */
	@Override
	public Long bitCount(byte[] key, long start, long end) {

		Assert.notNull(key, "Key must not be null!");

		try {
			if (isPipelined()) {
				pipeline(connection.newLettuceResult(getAsyncConnection().bitcount(key, start, end)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceResult(getAsyncConnection().bitcount(key, start, end)));
				return null;
			}
			return getConnection().bitcount(key, start, end);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStringCommands#bitfield(byte[], BitfieldCommand)
	 */
	@Override
	public List<Long> bitField(byte[] key, BitFieldSubCommands subCommands) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(subCommands, "Command must not be null!");

		BitFieldArgs args = LettuceConverters.toBitFieldArgs(subCommands);

		try {
			if (isPipelined()) {
				pipeline(connection.newLettuceResult(getAsyncConnection().bitfield(key, args)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceResult(getAsyncConnection().bitfield(key, args)));
				return null;
			}
			return getConnection().bitfield(key, args);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStringCommands#bitOp(org.springframework.data.redis.connection.RedisStringCommands.BitOperation, byte[], byte[][])
	 */
	@Override
	public Long bitOp(BitOperation op, byte[] destination, byte[]... keys) {

		Assert.notNull(op, "BitOperation must not be null!");
		Assert.notNull(destination, "Destination key must not be null!");

		if (op == BitOperation.NOT && keys.length > 1) {
			throw new UnsupportedOperationException("Bitop NOT should only be performed against one key");
		}
		try {
			if (isPipelined()) {
				pipeline(connection.newLettuceResult(asyncBitOp(op, destination, keys)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceResult(asyncBitOp(op, destination, keys)));
				return null;
			}
			return syncBitOp(op, destination, keys);
		} catch (UnsupportedOperationException ex) {
			throw ex;
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	private Future<Long> asyncBitOp(BitOperation op, byte[] destination, byte[]... keys) {
		switch (op) {
			case AND:
				return getAsyncConnection().bitopAnd(destination, keys);
			case OR:
				return getAsyncConnection().bitopOr(destination, keys);
			case XOR:
				return getAsyncConnection().bitopXor(destination, keys);
			case NOT:
				if (keys.length != 1) {
					throw new UnsupportedOperationException("Bitop NOT should only be performed against one key");
				}
				return getAsyncConnection().bitopNot(destination, keys[0]);
			default:
				throw new UnsupportedOperationException("Bit operation " + op + " is not supported");
		}
	}

	private Long syncBitOp(BitOperation op, byte[] destination, byte[]... keys) {
		switch (op) {
			case AND:
				return getConnection().bitopAnd(destination, keys);
			case OR:
				return getConnection().bitopOr(destination, keys);
			case XOR:
				return getConnection().bitopXor(destination, keys);
			case NOT:
				if (keys.length != 1) {
					throw new UnsupportedOperationException("Bitop NOT should only be performed against one key");
				}
				return getConnection().bitopNot(destination, keys[0]);
			default:
				throw new UnsupportedOperationException("Bit operation " + op + " is not supported");
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStringCommands#bitPos(byte[], boolean, org.springframework.data.domain.Range)
	 */
	@Nullable
	@Override
	public Long bitPos(byte[] key, boolean bit, Range<Long> range) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(range, "Range must not be null! Use Range.unbounded() instead.");

		try {
			if (isPipelined() || isQueueing()) {

				RedisFuture<Long> futureResult;
				RedisClusterAsyncCommands<byte[], byte[]> connection = getAsyncConnection();

				if (range.getLowerBound().isBounded()) {
					if (range.getUpperBound().isBounded()) {
						futureResult = connection.bitpos(key, bit, getLowerValue(range), getUpperValue(range));
					} else {
						futureResult = connection.bitpos(key, bit, getLowerValue(range));
					}
				} else {
					futureResult = connection.bitpos(key, bit);
				}

				if (isPipelined()) {
					pipeline(this.connection.newLettuceResult(futureResult));
				}
				else if (isQueueing()) {
					transaction(this.connection.newLettuceResult(futureResult));
				}
				return null;
			}

			if (range.getLowerBound().isBounded()) {
				if (range.getUpperBound().isBounded()) {
					return getConnection().bitpos(key, bit, getLowerValue(range), getUpperValue(range));
				}
				return getConnection().bitpos(key, bit, getLowerValue(range));
			}

			return getConnection().bitpos(key, bit);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStringCommands#strLen(byte[])
	 */
	@Override
	public Long strLen(byte[] key) {

		Assert.notNull(key, "Key must not be null!");

		try {
			if (isPipelined()) {
				pipeline(connection.newLettuceResult(getAsyncConnection().strlen(key)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceResult(getAsyncConnection().strlen(key)));
				return null;
			}
			return getConnection().strlen(key);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	private boolean isPipelined() {
		return connection.isPipelined();
	}

	private boolean isQueueing() {
		return connection.isQueueing();
	}

	private void pipeline(LettuceResult result) {
		connection.pipeline(result);
	}

	private void transaction(LettuceResult result) {
		connection.transaction(result);
	}

	RedisClusterAsyncCommands<byte[], byte[]> getAsyncConnection() {
		return connection.getAsyncConnection();
	}

	public RedisClusterCommands<byte[], byte[]> getConnection() {
		return connection.getConnection();
	}

	private DataAccessException convertLettuceAccessException(Exception ex) {
		return connection.convertLettuceAccessException(ex);
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
