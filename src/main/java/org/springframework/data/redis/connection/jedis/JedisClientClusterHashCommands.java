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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.ExpirationOptions;
import org.springframework.data.redis.connection.RedisHashCommands;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanCursor;
import org.springframework.data.redis.core.ScanIteration;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.util.Assert;

import redis.clients.jedis.args.ExpiryOption;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

/**
 * Cluster {@link RedisHashCommands} implementation for Jedis.
 *
 * @author Tihomir Mateev
 * @since 4.1
 */
class JedisClientClusterHashCommands implements RedisHashCommands {

	private final JedisClientClusterConnection connection;

	JedisClientClusterHashCommands(JedisClientClusterConnection connection) {
		this.connection = connection;
	}

	@Override
	public Boolean hSet(byte[] key, byte[] field, byte[] value) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(field, "Field must not be null");
		Assert.notNull(value, "Value must not be null");

		try {
			return JedisConverters.toBoolean(connection.getClusterClient().hset(key, field, value));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Boolean hSetNX(byte[] key, byte[] field, byte[] value) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(field, "Field must not be null");
		Assert.notNull(value, "Value must not be null");

		try {
			return JedisConverters.toBoolean(connection.getClusterClient().hsetnx(key, field, value));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public byte[] hGet(byte[] key, byte[] field) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(field, "Field must not be null");

		try {
			return connection.getClusterClient().hget(key, field);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public List<byte[]> hMGet(byte[] key, byte[]... fields) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(fields, "Fields must not be null");

		try {
			return connection.getClusterClient().hmget(key, fields);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public void hMSet(byte[] key, Map<byte[], byte[]> hashes) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(hashes, "Hashes must not be null");

		try {
			connection.getClusterClient().hmset(key, hashes);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long hIncrBy(byte[] key, byte[] field, long delta) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(field, "Field must not be null");

		try {
			return connection.getClusterClient().hincrBy(key, field, delta);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Double hIncrBy(byte[] key, byte[] field, double delta) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(field, "Field must not be null");

		try {
			return connection.getClusterClient().hincrByFloat(key, field, delta);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public byte @Nullable [] hRandField(byte[] key) {

		Assert.notNull(key, "Key must not be null");

		try {
			return connection.getClusterClient().hrandfield(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Nullable
	@Override
	public Entry<byte[], byte[]> hRandFieldWithValues(byte[] key) {

		Assert.notNull(key, "Key must not be null");

		try {
			List<Entry<byte[], byte[]>> mapEntryList = connection.getClusterClient().hrandfieldWithValues(key, 1);
			return mapEntryList.isEmpty() ? null : mapEntryList.get(0);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Nullable
	@Override
	public List<byte[]> hRandField(byte[] key, long count) {

		Assert.notNull(key, "Key must not be null");

		try {
			return connection.getClusterClient().hrandfield(key, count);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Nullable
	@Override
	public List<Entry<byte[], byte[]>> hRandFieldWithValues(byte[] key, long count) {

		try {
			return connection.getClusterClient().hrandfieldWithValues(key, count);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Boolean hExists(byte[] key, byte[] field) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(field, "Field must not be null");

		try {
			return connection.getClusterClient().hexists(key, field);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long hDel(byte[] key, byte[]... fields) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(fields, "Fields must not be null");

		try {
			return connection.getClusterClient().hdel(key, fields);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long hLen(byte[] key) {

		Assert.notNull(key, "Key must not be null");

		try {
			return connection.getClusterClient().hlen(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Set<byte[]> hKeys(byte[] key) {

		Assert.notNull(key, "Key must not be null");

		try {
			return connection.getClusterClient().hkeys(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public List<byte[]> hVals(byte[] key) {

		Assert.notNull(key, "Key must not be null");

		try {
			return new ArrayList<>(connection.getClusterClient().hvals(key));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Map<byte[], byte[]> hGetAll(byte[] key) {

		Assert.notNull(key, "Key must not be null");

		try {
			return connection.getClusterClient().hgetAll(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Cursor<Entry<byte[], byte[]>> hScan(byte[] key, ScanOptions options) {

		Assert.notNull(key, "Key must not be null");

		return new ScanCursor<Entry<byte[], byte[]>>(options) {

			@Override
			protected ScanIteration<Entry<byte[], byte[]>> doScan(CursorId cursorId, ScanOptions options) {

				ScanParams params = JedisConverters.toScanParams(options);

				ScanResult<Entry<byte[], byte[]>> result = connection.getClusterClient().hscan(key,
						JedisConverters.toBytes(cursorId), params);
				return new ScanIteration<>(CursorId.of(result.getCursor()), result.getResult());
			}
		}.open();
	}

	@Override
	public List<Long> hExpire(byte[] key, long seconds, ExpirationOptions.Condition condition, byte[]... fields) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(fields, "Fields must not be null");

		try {
			if (condition == ExpirationOptions.Condition.ALWAYS) {
				return connection.getClusterClient().hexpire(key, seconds, fields);
			}

			return connection.getClusterClient().hexpire(key, seconds, ExpiryOption.valueOf(condition.name()), fields);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public List<Long> hpExpire(byte[] key, long millis, ExpirationOptions.Condition condition, byte[]... fields) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(fields, "Fields must not be null");

		try {
			if (condition == ExpirationOptions.Condition.ALWAYS) {
				return connection.getClusterClient().hpexpire(key, millis, fields);
			}

			return connection.getClusterClient().hpexpire(key, millis, ExpiryOption.valueOf(condition.name()), fields);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public List<Long> hExpireAt(byte[] key, long unixTime, ExpirationOptions.Condition condition, byte[]... fields) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(fields, "Fields must not be null");

		try {

			if (condition == ExpirationOptions.Condition.ALWAYS) {
				return connection.getClusterClient().hexpireAt(key, unixTime, fields);
			}

			return connection.getClusterClient().hexpireAt(key, unixTime, ExpiryOption.valueOf(condition.name()), fields);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public List<Long> hpExpireAt(byte[] key, long unixTimeInMillis, ExpirationOptions.Condition condition,
			byte[]... fields) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(fields, "Fields must not be null");

		try {

			if (condition == ExpirationOptions.Condition.ALWAYS) {
				return connection.getClusterClient().hpexpireAt(key, unixTimeInMillis, fields);
			}

			return connection.getClusterClient().hpexpireAt(key, unixTimeInMillis, ExpiryOption.valueOf(condition.name()),
					fields);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public List<Long> hPersist(byte[] key, byte[]... fields) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(fields, "Fields must not be null");

		try {
			return connection.getClusterClient().hpersist(key, fields);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public List<Long> hTtl(byte[] key, byte[]... fields) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(fields, "Fields must not be null");

		try {
			return connection.getClusterClient().httl(key, fields);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public List<Long> hTtl(byte[] key, TimeUnit timeUnit, byte[]... fields) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(fields, "Fields must not be null");

		try {
			return connection.getClusterClient().httl(key, fields).stream()
					.map(it -> it != null ? timeUnit.convert(it, TimeUnit.SECONDS) : null).toList();
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public List<Long> hpTtl(byte[] key, byte[]... fields) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(fields, "Fields must not be null");

		try {
			return connection.getClusterClient().hpttl(key, fields);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public List<byte[]> hGetDel(byte[] key, byte[]... fields) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(fields, "Fields must not be null");

		try {
			return connection.getClusterClient().hgetdel(key, fields);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public List<byte[]> hGetEx(byte[] key, @Nullable Expiration expiration, byte[]... fields) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(fields, "Fields must not be null");

		try {
			return connection.getClusterClient().hgetex(key, JedisConverters.toHGetExParams(expiration), fields);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Boolean hSetEx(byte[] key, Map<byte[], byte[]> hashes, @NonNull HashFieldSetOption condition,
			@Nullable Expiration expiration) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(hashes, "Fields must not be null");
		Assert.notNull(condition, "Condition must not be null");

		try {
			return JedisConverters.toBoolean(
					connection.getClusterClient().hsetex(key, JedisConverters.toHSetExParams(condition, expiration), hashes));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Nullable
	@Override
	public Long hStrLen(byte[] key, byte[] field) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(field, "Field must not be null");

		return connection.getClusterClient().hstrlen(key, field);
	}

	private DataAccessException convertJedisAccessException(Exception ex) {
		return connection.convertJedisAccessException(ex);
	}

}
