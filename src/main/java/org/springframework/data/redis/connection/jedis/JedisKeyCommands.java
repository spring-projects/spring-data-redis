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

import redis.clients.jedis.ScanParams;
import redis.clients.jedis.SortingParams;

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.connection.RedisKeyCommands;
import org.springframework.data.redis.connection.SortParameters;
import org.springframework.data.redis.connection.convert.Converters;
import org.springframework.data.redis.connection.jedis.JedisConnection.JedisResult;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanCursor;
import org.springframework.data.redis.core.ScanIteration;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.util.Assert;

/**
 * @author Christoph Strobl
 * @since 2.0
 */
class JedisKeyCommands implements RedisKeyCommands {

	private final JedisConnection connection;

	public JedisKeyCommands(JedisConnection connection) {
		this.connection = connection;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisKeyCommands#exists(byte[])
	 */
	@Override
	public Boolean exists(byte[] key) {

		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getPipeline().exists(key)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getTransaction().exists(key)));
				return null;
			}
			return connection.getJedis().exists(key);
		} catch (Exception ex) {
			throw connection.convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisKeyCommands#del(byte[][])
	 */
	@Override
	public Long del(byte[]... keys) {

		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getPipeline().del(keys)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getTransaction().del(keys)));
				return null;
			}
			return connection.getJedis().del(keys);
		} catch (Exception ex) {
			throw connection.convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisKeyCommands#type(byte[])
	 */
	@Override
	public DataType type(byte[] key) {

		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getPipeline().type(key), JedisConverters.stringToDataType()));
				return null;
			}
			if (isQueueing()) {
				transaction(
						connection.newJedisResult(connection.getTransaction().type(key), JedisConverters.stringToDataType()));
				return null;
			}
			return JedisConverters.toDataType(connection.getJedis().type(key));
		} catch (Exception ex) {
			throw connection.convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisKeyCommands#keys(byte[])
	 */
	@Override
	public Set<byte[]> keys(byte[] pattern) {

		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getPipeline().keys(pattern)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getTransaction().keys(pattern)));
				return null;
			}
			return connection.getJedis().keys(pattern);
		} catch (Exception ex) {
			throw connection.convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisKeyCommands#scan(org.springframework.data.redis.core.ScanOptions)
	 */
	public Cursor<byte[]> scan(ScanOptions options) {
		return scan(0, options != null ? options : ScanOptions.NONE);
	}

	/**
	 * @since 1.4
	 * @param cursorId
	 * @param options
	 * @return
	 */
	public Cursor<byte[]> scan(long cursorId, ScanOptions options) {

		return new ScanCursor<byte[]>(cursorId, options) {

			@Override
			protected ScanIteration<byte[]> doScan(long cursorId, ScanOptions options) {

				if (isQueueing() || isPipelined()) {
					throw new UnsupportedOperationException("'SCAN' cannot be called in pipeline / transaction mode.");
				}

				ScanParams params = JedisConverters.toScanParams(options);
				redis.clients.jedis.ScanResult<String> result = connection.getJedis().scan(Long.toString(cursorId), params);
				return new ScanIteration<>(Long.valueOf(result.getStringCursor()),
						JedisConverters.stringListToByteList().convert(result.getResult()));
			}

			protected void doClose() {
				JedisKeyCommands.this.connection.close();
			};

		}.open();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisKeyCommands#randomKey()
	 */
	@Override
	public byte[] randomKey() {

		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getPipeline().randomKeyBinary()));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getTransaction().randomKeyBinary()));
				return null;
			}
			return connection.getJedis().randomBinaryKey();
		} catch (Exception ex) {
			throw connection.convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisKeyCommands#rename(byte[], byte[])
	 */
	@Override
	public void rename(byte[] oldName, byte[] newName) {

		try {
			if (isPipelined()) {
				pipeline(connection.newStatusResult(connection.getPipeline().rename(oldName, newName)));
				return;
			}
			if (isQueueing()) {
				transaction(connection.newStatusResult(connection.getTransaction().rename(oldName, newName)));
				return;
			}
			connection.getJedis().rename(oldName, newName);
		} catch (Exception ex) {
			throw connection.convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisKeyCommands#renameNX(byte[], byte[])
	 */
	@Override
	public Boolean renameNX(byte[] oldName, byte[] newName) {

		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getPipeline().renamenx(oldName, newName),
						JedisConverters.longToBoolean()));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getTransaction().renamenx(oldName, newName),
						JedisConverters.longToBoolean()));
				return null;
			}
			return JedisConverters.toBoolean(connection.getJedis().renamenx(oldName, newName));
		} catch (Exception ex) {
			throw connection.convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisKeyCommands#expire(byte[], long)
	 */
	@Override
	public Boolean expire(byte[] key, long seconds) {

		Assert.notNull(key, "Key must not be null!");

		if (seconds > Integer.MAX_VALUE) {
			return pExpire(key, TimeUnit.SECONDS.toMillis(seconds));
		}

		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getPipeline().expire(key, (int) seconds),
						JedisConverters.longToBoolean()));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getTransaction().expire(key, (int) seconds),
						JedisConverters.longToBoolean()));
				return null;
			}
			return JedisConverters.toBoolean(connection.getJedis().expire(key, (int) seconds));
		} catch (Exception ex) {
			throw connection.convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisKeyCommands#expireAt(byte[], long)
	 */
	@Override
	public Boolean expireAt(byte[] key, long unixTime) {

		Assert.notNull(key, "Key must not be null!");

		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getPipeline().expireAt(key, unixTime),
						JedisConverters.longToBoolean()));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getTransaction().expireAt(key, unixTime),
						JedisConverters.longToBoolean()));
				return null;
			}
			return JedisConverters.toBoolean(connection.getJedis().expireAt(key, unixTime));
		} catch (Exception ex) {
			throw connection.convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisKeyCommands#pExpire(byte[], long)
	 */
	@Override
	public Boolean pExpire(byte[] key, long millis) {

		Assert.notNull(key, "Key must not be null!");

		try {
			if (isPipelined()) {
				pipeline(
						connection.newJedisResult(connection.getPipeline().pexpire(key, millis), JedisConverters.longToBoolean()));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getTransaction().pexpire(key, millis),
						JedisConverters.longToBoolean()));
				return null;
			}
			return JedisConverters.toBoolean(connection.getJedis().pexpire(key, millis));
		} catch (Exception ex) {
			throw connection.convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisKeyCommands#pExpireAt(byte[], long)
	 */
	@Override
	public Boolean pExpireAt(byte[] key, long unixTimeInMillis) {

		Assert.notNull(key, "Key must not be null!");

		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getPipeline().pexpireAt(key, unixTimeInMillis),
						JedisConverters.longToBoolean()));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getTransaction().pexpireAt(key, unixTimeInMillis),
						JedisConverters.longToBoolean()));
				return null;
			}
			return JedisConverters.toBoolean(connection.getJedis().pexpireAt(key, unixTimeInMillis));
		} catch (Exception ex) {
			throw connection.convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisKeyCommands#persist(byte[])
	 */
	@Override
	public Boolean persist(byte[] key) {

		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getPipeline().persist(key), JedisConverters.longToBoolean()));
				return null;
			}
			if (isQueueing()) {
				transaction(
						connection.newJedisResult(connection.getTransaction().persist(key), JedisConverters.longToBoolean()));
				return null;
			}
			return JedisConverters.toBoolean(connection.getJedis().persist(key));
		} catch (Exception ex) {
			throw connection.convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisKeyCommands#move(byte[], int)
	 */
	@Override
	public Boolean move(byte[] key, int dbIndex) {

		try {
			if (isPipelined()) {
				pipeline(
						connection.newJedisResult(connection.getPipeline().move(key, dbIndex), JedisConverters.longToBoolean()));
				return null;
			}
			if (isQueueing()) {
				transaction(
						connection.newJedisResult(connection.getTransaction().move(key, dbIndex), JedisConverters.longToBoolean()));
				return null;
			}
			return JedisConverters.toBoolean(connection.getJedis().move(key, dbIndex));
		} catch (Exception ex) {
			throw connection.convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisKeyCommands#ttl(byte[])
	 */
	@Override
	public Long ttl(byte[] key) {

		Assert.notNull(key, "Key must not be null!");

		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getPipeline().ttl(key)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getTransaction().ttl(key)));
				return null;
			}

			return connection.getJedis().ttl(key);
		} catch (Exception ex) {
			throw connection.convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisKeyCommands#ttl(byte[], java.util.concurrent.TimeUnit)
	 */
	@Override
	public Long ttl(byte[] key, TimeUnit timeUnit) {

		Assert.notNull(key, "Key must not be null!");

		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getPipeline().ttl(key), Converters.secondsToTimeUnit(timeUnit)));
				return null;
			}
			if (isQueueing()) {
				transaction(
						connection.newJedisResult(connection.getTransaction().ttl(key), Converters.secondsToTimeUnit(timeUnit)));
				return null;
			}

			return Converters.secondsToTimeUnit(connection.getJedis().ttl(key), timeUnit);
		} catch (Exception ex) {
			throw connection.convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisKeyCommands#pTtl(byte[])
	 */
	@Override
	public Long pTtl(byte[] key) {

		Assert.notNull(key, "Key must not be null!");

		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getPipeline().pttl(key)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getTransaction().pttl(key)));
				return null;
			}

			return connection.getJedis().pttl(key);
		} catch (Exception ex) {
			throw connection.convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisKeyCommands#pTtl(byte[], java.util.concurrent.TimeUnit)
	 */
	@Override
	public Long pTtl(byte[] key, TimeUnit timeUnit) {

		Assert.notNull(key, "Key must not be null!");

		try {
			if (isPipelined()) {
				pipeline(
						connection.newJedisResult(connection.getPipeline().pttl(key), Converters.millisecondsToTimeUnit(timeUnit)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getTransaction().pttl(key),
						Converters.millisecondsToTimeUnit(timeUnit)));
				return null;
			}

			return Converters.millisecondsToTimeUnit(connection.getJedis().pttl(key), timeUnit);
		} catch (Exception ex) {
			throw connection.convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisKeyCommands#sort(byte[], org.springframework.data.redis.connection.SortParameters)
	 */
	@Override
	public List<byte[]> sort(byte[] key, SortParameters params) {

		SortingParams sortParams = JedisConverters.toSortingParams(params);

		try {
			if (isPipelined()) {
				if (sortParams != null) {
					pipeline(connection.newJedisResult(connection.getPipeline().sort(key, sortParams)));
				} else {
					pipeline(connection.newJedisResult(connection.getPipeline().sort(key)));
				}

				return null;
			}
			if (isQueueing()) {
				if (sortParams != null) {
					transaction(connection.newJedisResult(connection.getTransaction().sort(key, sortParams)));
				} else {
					transaction(connection.newJedisResult(connection.getTransaction().sort(key)));
				}

				return null;
			}
			return (sortParams != null ? connection.getJedis().sort(key, sortParams) : connection.getJedis().sort(key));
		} catch (Exception ex) {
			throw connection.convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisKeyCommands#sort(byte[], org.springframework.data.redis.connection.SortParameters, byte[])
	 */
	@Override
	public Long sort(byte[] key, SortParameters params, byte[] storeKey) {

		SortingParams sortParams = JedisConverters.toSortingParams(params);

		try {
			if (isPipelined()) {
				if (sortParams != null) {
					pipeline(connection.newJedisResult(connection.getPipeline().sort(key, sortParams, storeKey)));
				} else {
					pipeline(connection.newJedisResult(connection.getPipeline().sort(key, storeKey)));
				}

				return null;
			}
			if (isQueueing()) {
				if (sortParams != null) {
					transaction(connection.newJedisResult(connection.getTransaction().sort(key, sortParams, storeKey)));
				} else {
					transaction(connection.newJedisResult(connection.getTransaction().sort(key, storeKey)));
				}

				return null;
			}
			return (sortParams != null ? connection.getJedis().sort(key, sortParams, storeKey)
					: connection.getJedis().sort(key, storeKey));
		} catch (Exception ex) {
			throw connection.convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisKeyCommands#dump(byte[])
	 */
	@Override
	public byte[] dump(byte[] key) {

		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getPipeline().dump(key)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getTransaction().dump(key)));
				return null;
			}
			return connection.getJedis().dump(key);
		} catch (Exception ex) {
			throw connection.convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisKeyCommands#restore(byte[], long, byte[])
	 */
	@Override
	public void restore(byte[] key, long ttlInMillis, byte[] serializedValue) {

		if (ttlInMillis > Integer.MAX_VALUE) {
			throw new IllegalArgumentException("TtlInMillis must be less than Integer.MAX_VALUE for restore in Jedis.");
		}
		try {
			if (isPipelined()) {
				pipeline(connection.newStatusResult(connection.getPipeline().restore(key, (int) ttlInMillis, serializedValue)));
				return;
			}
			if (isQueueing()) {
				transaction(
						connection.newStatusResult(connection.getTransaction().restore(key, (int) ttlInMillis, serializedValue)));
				return;
			}
			connection.getJedis().restore(key, (int) ttlInMillis, serializedValue);
		} catch (Exception ex) {
			throw connection.convertJedisAccessException(ex);
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
}
