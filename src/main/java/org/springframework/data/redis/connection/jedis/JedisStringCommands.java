/*
 * Copyright 2017 the original author or authors.
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

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.springframework.data.redis.connection.RedisStringCommands;
import org.springframework.data.redis.connection.convert.Converters;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.0
 */
@RequiredArgsConstructor
class JedisStringCommands implements RedisStringCommands {

	private final @NonNull JedisConnection connection;

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStringCommands#get(byte[])
	 */
	@Override
	public byte[] get(byte[] key) {

		Assert.notNull(key, "Key must not be null!");

		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getRequiredPipeline().get(key)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getRequiredTransaction().get(key)));
				return null;
			}

			return connection.getJedis().get(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
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
				pipeline(connection.newJedisResult(connection.getRequiredPipeline().getSet(key, value)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getRequiredTransaction().getSet(key, value)));
				return null;
			}
			return connection.getJedis().getSet(key, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
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
				pipeline(connection.newJedisResult(connection.getRequiredPipeline().mget(keys)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getRequiredTransaction().mget(keys)));
				return null;
			}
			return connection.getJedis().mget(keys);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
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
				pipeline(connection.newJedisResult(connection.getRequiredPipeline().set(key, value),
						Converters.stringToBooleanConverter()));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getRequiredTransaction().set(key, value),
						Converters.stringToBooleanConverter()));
				return null;
			}
			return Converters.stringToBoolean(connection.getJedis().set(key, value));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
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

		if (expiration.isPersistent()) {

			if (ObjectUtils.nullSafeEquals(SetOption.UPSERT, option)) {
				return set(key, value);
			} else {

				try {

					byte[] nxxx = JedisConverters.toSetCommandNxXxArgument(option);

					if (isPipelined()) {

						pipeline(connection.newJedisResult(connection.getRequiredPipeline().set(key, value, nxxx),
								Converters.stringToBooleanConverter(), () -> false));
						return null;
					}
					if (isQueueing()) {

						transaction(connection.newJedisResult(connection.getRequiredTransaction().set(key, value, nxxx),
								Converters.stringToBooleanConverter(), () -> false));
						return null;
					}

					return Converters.stringToBoolean(connection.getJedis().set(key, value, nxxx));
				} catch (Exception ex) {
					throw convertJedisAccessException(ex);
				}
			}

		} else {

			if (ObjectUtils.nullSafeEquals(SetOption.UPSERT, option)) {

				if (ObjectUtils.nullSafeEquals(TimeUnit.MILLISECONDS, expiration.getTimeUnit())) {
					return pSetEx(key, expiration.getExpirationTime(), value);
				} else {
					return setEx(key, expiration.getExpirationTime(), value);
				}
			} else {

				byte[] nxxx = JedisConverters.toSetCommandNxXxArgument(option);
				byte[] expx = JedisConverters.toSetCommandExPxArgument(expiration);

				try {
					if (isPipelined()) {

						if (expiration.getExpirationTime() > Integer.MAX_VALUE) {

							throw new IllegalArgumentException(
									"Expiration.expirationTime must be less than Integer.MAX_VALUE for pipeline in Jedis.");
						}

						pipeline(connection.newJedisResult(
								connection.getRequiredPipeline().set(key, value, nxxx, expx, (int) expiration.getExpirationTime()),
								Converters.stringToBooleanConverter(), () -> false));
						return null;
					}
					if (isQueueing()) {

						if (expiration.getExpirationTime() > Integer.MAX_VALUE) {
							throw new IllegalArgumentException(
									"Expiration.expirationTime must be less than Integer.MAX_VALUE for transactions in Jedis.");
						}

						transaction(connection.newJedisResult(
								connection.getRequiredTransaction().set(key, value, nxxx, expx, (int) expiration.getExpirationTime()),
								Converters.stringToBooleanConverter(), () -> false));
						return null;
					}

					return Converters
							.stringToBoolean(connection.getJedis().set(key, value, nxxx, expx, expiration.getExpirationTime()));

				} catch (Exception ex) {
					throw convertJedisAccessException(ex);
				}
			}
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
				pipeline(connection.newJedisResult(connection.getRequiredPipeline().setnx(key, value),
						JedisConverters.longToBoolean()));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getRequiredTransaction().setnx(key, value),
						JedisConverters.longToBoolean()));
				return null;
			}
			return JedisConverters.toBoolean(connection.getJedis().setnx(key, value));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
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

		if (seconds > Integer.MAX_VALUE) {
			throw new IllegalArgumentException("Time must be less than Integer.MAX_VALUE for setEx in Jedis.");
		}

		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getRequiredPipeline().setex(key, (int) seconds, value),
						Converters.stringToBooleanConverter(), () -> false));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getRequiredTransaction().setex(key, (int) seconds, value),
						Converters.stringToBooleanConverter(), () -> false));
				return null;
			}
			return Converters.stringToBoolean(connection.getJedis().setex(key, (int) seconds, value));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
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
				pipeline(connection.newJedisResult(connection.getRequiredPipeline().psetex(key, milliseconds, value),
						Converters.stringToBooleanConverter(), () -> false));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getRequiredTransaction().psetex(key, milliseconds, value),
						Converters.stringToBooleanConverter(), () -> false));
				return null;
			}
			return Converters.stringToBoolean(connection.getJedis().psetex(key, milliseconds, value));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
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
				pipeline(connection.newJedisResult(connection.getRequiredPipeline().mset(JedisConverters.toByteArrays(tuples)),
						Converters.stringToBooleanConverter()));
				return null;
			}
			if (isQueueing()) {
				transaction(
						connection.newJedisResult(connection.getRequiredTransaction().mset(JedisConverters.toByteArrays(tuples)),
								Converters.stringToBooleanConverter()));
				return null;
			}
			return Converters.stringToBoolean(connection.getJedis().mset(JedisConverters.toByteArrays(tuples)));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
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
				pipeline(
						connection.newJedisResult(connection.getRequiredPipeline().msetnx(JedisConverters.toByteArrays(tuples)),
								JedisConverters.longToBoolean()));
				return null;
			}
			if (isQueueing()) {
				transaction(
						connection.newJedisResult(connection.getRequiredTransaction().msetnx(JedisConverters.toByteArrays(tuples)),
								JedisConverters.longToBoolean()));
				return null;
			}
			return JedisConverters.toBoolean(connection.getJedis().msetnx(JedisConverters.toByteArrays(tuples)));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
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
				pipeline(connection.newJedisResult(connection.getRequiredPipeline().incr(key)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getRequiredTransaction().incr(key)));
				return null;
			}
			return connection.getJedis().incr(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
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
				pipeline(connection.newJedisResult(connection.getRequiredPipeline().incrBy(key, value)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getRequiredTransaction().incrBy(key, value)));
				return null;
			}
			return connection.getJedis().incrBy(key, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
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
				pipeline(connection.newJedisResult(connection.getRequiredPipeline().incrByFloat(key, value)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getRequiredTransaction().incrByFloat(key, value)));
				return null;
			}
			return connection.getJedis().incrByFloat(key, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
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
				pipeline(connection.newJedisResult(connection.getRequiredPipeline().decr(key)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getRequiredTransaction().decr(key)));
				return null;
			}
			return connection.getJedis().decr(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
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
				pipeline(connection.newJedisResult(connection.getRequiredPipeline().decrBy(key, value)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getRequiredTransaction().decrBy(key, value)));
				return null;
			}
			return connection.getJedis().decrBy(key, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
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
				pipeline(connection.newJedisResult(connection.getRequiredPipeline().append(key, value)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getRequiredTransaction().append(key, value)));
				return null;
			}
			return connection.getJedis().append(key, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStringCommands#getRange(byte[], long, long)
	 */
	@Override
	public byte[] getRange(byte[] key, long start, long end) {

		Assert.notNull(key, "Key must not be null!");

		if (start > Integer.MAX_VALUE || end > Integer.MAX_VALUE) {
			throw new IllegalArgumentException("Start and end must be less than Integer.MAX_VALUE for getRange in Jedis.");
		}

		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getRequiredPipeline().substr(key, (int) start, (int) end),
						JedisConverters.stringToBytes()));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getRequiredTransaction().substr(key, (int) start, (int) end),
						JedisConverters.stringToBytes()));
				return null;
			}
			return connection.getJedis().substr(key, (int) start, (int) end);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
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
				pipeline(connection.newStatusResult(connection.getRequiredPipeline().setrange(key, offset, value)));
				return;
			}
			if (isQueueing()) {
				transaction(connection.newStatusResult(connection.getRequiredTransaction().setrange(key, offset, value)));
				return;
			}
			connection.getJedis().setrange(key, offset, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
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
				pipeline(connection.newJedisResult(connection.getRequiredPipeline().getbit(key, offset)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getRequiredTransaction().getbit(key, offset)));
				return null;
			}
			// compatibility check for Jedis 2.0.0
			Object getBit = connection.getJedis().getbit(key, offset);
			// Jedis 2.0
			if (getBit instanceof Long) {
				return (((Long) getBit) == 0 ? Boolean.FALSE : Boolean.TRUE);
			}
			// Jedis 2.1
			return ((Boolean) getBit);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
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

				pipeline(connection
						.newJedisResult(connection.getRequiredPipeline().setbit(key, offset, JedisConverters.toBit(value))));
				return null;
			}
			if (isQueueing()) {
				transaction(connection
						.newJedisResult(connection.getRequiredTransaction().setbit(key, offset, JedisConverters.toBit(value))));
				return null;
			}
			return connection.getJedis().setbit(key, offset, JedisConverters.toBit(value));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
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
				pipeline(connection.newJedisResult(connection.getRequiredPipeline().bitcount(key)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getRequiredTransaction().bitcount(key)));
				return null;
			}
			return connection.getJedis().bitcount(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
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
				pipeline(connection.newJedisResult(connection.getRequiredPipeline().bitcount(key, start, end)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getRequiredTransaction().bitcount(key, start, end)));
				return null;
			}
			return connection.getJedis().bitcount(key, start, end);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
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
				pipeline(connection
						.newJedisResult(connection.getRequiredPipeline().bitop(JedisConverters.toBitOp(op), destination, keys)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection
						.newJedisResult(connection.getRequiredTransaction().bitop(JedisConverters.toBitOp(op), destination, keys)));
				return null;
			}
			return connection.getJedis().bitop(JedisConverters.toBitOp(op), destination, keys);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
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
				pipeline(connection.newJedisResult(connection.getRequiredPipeline().strlen(key)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getRequiredTransaction().strlen(key)));
				return null;
			}
			return connection.getJedis().strlen(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	private boolean isPipelined() {
		return connection.isPipelined();
	}

	private void pipeline(JedisResult result) {
		connection.pipeline(result);
	}

	private boolean isQueueing() {
		return connection.isQueueing();
	}

	private void transaction(JedisResult result) {
		connection.transaction(result);
	}

	private RuntimeException convertJedisAccessException(Exception ex) {
		return connection.convertJedisAccessException(ex);
	}
}
