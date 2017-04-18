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

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.springframework.data.redis.connection.RedisStringCommands;
import org.springframework.data.redis.connection.jedis.JedisConnection.JedisResult;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.util.ObjectUtils;

/**
 * @author Christoph Strobl
 * @since 2.0
 */
class JedisStringCommands implements RedisStringCommands {

	private final JedisConnection connection;

	public JedisStringCommands(JedisConnection connection) {
		this.connection = connection;
	}

	@Override
	public byte[] get(byte[] key) {

		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getPipeline().get(key)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getTransaction().get(key)));
				return null;
			}

			return connection.getJedis().get(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public byte[] getSet(byte[] key, byte[] value) {

		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getPipeline().getSet(key, value)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getTransaction().getSet(key, value)));
				return null;
			}
			return connection.getJedis().getSet(key, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}

	}

	@Override
	public List<byte[]> mGet(byte[]... keys) {


		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getPipeline().mget(keys)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getTransaction().mget(keys)));
				return null;
			}
			return connection.getJedis().mget(keys);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public void set(byte[] key, byte[] value) {

		try {
			if (isPipelined()) {
				pipeline(connection.newStatusResult(connection.getPipeline().set(key, value)));
				return;
			}
			if (isQueueing()) {
				transaction(connection.newStatusResult(connection.getTransaction().set(key, value)));
				return;
			}
			connection.getJedis().set(key, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public void set(byte[] key, byte[] value, Expiration expiration, SetOption option) {

		if (expiration == null || expiration.isPersistent()) {

			if (option == null || ObjectUtils.nullSafeEquals(SetOption.UPSERT, option)) {
				set(key, value);
			} else {

				try {

					byte[] nxxx = JedisConverters.toSetCommandNxXxArgument(option);

					if (isPipelined()) {

						pipeline(connection.newStatusResult(connection.getPipeline().set(key, value, nxxx)));
						return;
					}
					if (isQueueing()) {

						transaction(connection.newStatusResult(connection.getTransaction().set(key, value, nxxx)));
						return;
					}

					connection.getJedis().set(key, value, nxxx);
				} catch (Exception ex) {
					throw convertJedisAccessException(ex);
				}
			}

		} else {

			if (option == null || ObjectUtils.nullSafeEquals(SetOption.UPSERT, option)) {

				if (ObjectUtils.nullSafeEquals(TimeUnit.MILLISECONDS, expiration.getTimeUnit())) {
					pSetEx(key, expiration.getExpirationTime(), value);
				} else {
					setEx(key, expiration.getExpirationTime(), value);
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

						pipeline(connection.newStatusResult(connection.getPipeline().set(key, value, nxxx, expx, (int) expiration.getExpirationTime())));
						return;
					}
					if (isQueueing()) {

						if (expiration.getExpirationTime() > Integer.MAX_VALUE) {
							throw new IllegalArgumentException(
									"Expiration.expirationTime must be less than Integer.MAX_VALUE for transactions in Jedis.");
						}

						transaction(
								connection.newStatusResult(connection.getTransaction().set(key, value, nxxx, expx, (int) expiration.getExpirationTime())));
						return;
					}

					connection.getJedis().set(key, value, nxxx, expx, expiration.getExpirationTime());

				} catch (Exception ex) {
					throw convertJedisAccessException(ex);
				}
			}
		}
	}



	@Override
	public Boolean setNX(byte[] key, byte[] value) {

		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getPipeline().setnx(key, value), JedisConverters.longToBoolean()));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getTransaction().setnx(key, value), JedisConverters.longToBoolean()));
				return null;
			}
			return JedisConverters.toBoolean(connection.getJedis().setnx(key, value));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}

	}

	@Override
	public void setEx(byte[] key, long seconds, byte[] value) {
		if (seconds > Integer.MAX_VALUE) {
			throw new IllegalArgumentException("Time must be less than Integer.MAX_VALUE for setEx in Jedis.");
		}

		try {
			if (isPipelined()) {
				pipeline(connection.newStatusResult(connection.getPipeline().setex(key, (int) seconds, value)));
				return;
			}
			if (isQueueing()) {
				transaction(connection.newStatusResult(connection.getTransaction().setex(key, (int) seconds, value)));
				return;
			}
			connection.getJedis().setex(key, (int) seconds, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public void pSetEx(byte[] key, long milliseconds, byte[] value) {

		try {
			if (isPipelined()) {
				pipeline(connection.newStatusResult(connection.getPipeline().psetex(key, milliseconds, value)));
				return;
			}
			if (isQueueing()) {
				transaction(connection.newStatusResult(connection.getTransaction().psetex(key, milliseconds, value)));
				return;
			}
			connection.getJedis().psetex(key, milliseconds, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public void mSet(Map<byte[], byte[]> tuples) {

		try {
			if (isPipelined()) {
				pipeline(connection.newStatusResult(connection.getPipeline().mset(JedisConverters.toByteArrays(tuples))));
				return;
			}
			if (isQueueing()) {
				transaction(connection.newStatusResult(connection.getTransaction().mset(JedisConverters.toByteArrays(tuples))));
				return;
			}
			connection.getJedis().mset(JedisConverters.toByteArrays(tuples));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Boolean mSetNX(Map<byte[], byte[]> tuples) {

		try {
			if (isPipelined()) {
				pipeline(
						connection.newJedisResult(connection.getPipeline().msetnx(JedisConverters.toByteArrays(tuples)), JedisConverters.longToBoolean()));
				return null;
			}
			if (isQueueing()) {
				transaction(
						connection.newJedisResult(connection.getTransaction().msetnx(JedisConverters.toByteArrays(tuples)), JedisConverters.longToBoolean()));
				return null;
			}
			return JedisConverters.toBoolean(connection.getJedis().msetnx(JedisConverters.toByteArrays(tuples)));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long incr(byte[] key) {

		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getPipeline().incr(key)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getTransaction().incr(key)));
				return null;
			}
			return connection.getJedis().incr(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}

	}

	@Override
	public Long incrBy(byte[] key, long value) {

		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getPipeline().incrBy(key, value)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getTransaction().incrBy(key, value)));
				return null;
			}
			return connection.getJedis().incrBy(key, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}

	}

	@Override
	public Double incrBy(byte[] key, double value) {

		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getPipeline().incrByFloat(key, value)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getTransaction().incrByFloat(key, value)));
				return null;
			}
			return connection.getJedis().incrByFloat(key, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long decr(byte[] key) {

		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getPipeline().decr(key)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getTransaction().decr(key)));
				return null;
			}
			return connection.getJedis().decr(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}

	}

	@Override
	public Long decrBy(byte[] key, long value) {

		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getPipeline().decrBy(key, value)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getTransaction().decrBy(key, value)));
				return null;
			}
			return connection.getJedis().decrBy(key, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long append(byte[] key, byte[] value) {

		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getPipeline().append(key, value)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getTransaction().append(key, value)));
				return null;
			}
			return connection.getJedis().append(key, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public byte[] getRange(byte[] key, long start, long end) {

		if (start > Integer.MAX_VALUE || end > Integer.MAX_VALUE) {
			throw new IllegalArgumentException("Start and end must be less than Integer.MAX_VALUE for getRange in Jedis.");
		}

		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getPipeline().substr(key, (int) start, (int) end), JedisConverters.stringToBytes()));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getTransaction().substr(key, (int) start, (int) end), JedisConverters.stringToBytes()));
				return null;
			}
			return connection.getJedis().substr(key, (int) start, (int) end);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public void setRange(byte[] key, byte[] value, long start) {

		try {
			if (isPipelined()) {
				pipeline(connection.newStatusResult(connection.getPipeline().setrange(key, start, value)));
				return;
			}
			if (isQueueing()) {
				transaction(connection.newStatusResult(connection.getTransaction().setrange(key, start, value)));
				return;
			}
			connection.getJedis().setrange(key, start, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Boolean getBit(byte[] key, long offset) {

		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getPipeline().getbit(key, offset)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getTransaction().getbit(key, offset)));
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

	@Override
	public Boolean setBit(byte[] key, long offset, boolean value) {

		try {
			if (isPipelined()) {

				pipeline(connection.newJedisResult(connection.getPipeline().setbit(key, offset, JedisConverters.toBit(value))));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getTransaction().setbit(key, offset, JedisConverters.toBit(value))));
				return null;
			}
			return connection.getJedis().setbit(key, offset, JedisConverters.toBit(value));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}

	}

	@Override
	public Long bitCount(byte[] key) {

		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getPipeline().bitcount(key)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getTransaction().bitcount(key)));
				return null;
			}
			return connection.getJedis().bitcount(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long bitCount(byte[] key, long begin, long end) {

		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getPipeline().bitcount(key, begin, end)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getTransaction().bitcount(key, begin, end)));
				return null;
			}
			return connection.getJedis().bitcount(key, begin, end);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long bitOp(BitOperation op, byte[] destination, byte[]... keys) {


		if (op == BitOperation.NOT && keys.length > 1) {
			throw new UnsupportedOperationException("Bitop NOT should only be performed against one key");
		}
		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getPipeline().bitop(JedisConverters.toBitOp(op), destination, keys)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getTransaction().bitop(JedisConverters.toBitOp(op), destination, keys)));
				return null;
			}
			return connection.getJedis().bitop(JedisConverters.toBitOp(op), destination, keys);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long strLen(byte[] key) {

		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getPipeline().strlen(key)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getTransaction().strlen(key)));
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
