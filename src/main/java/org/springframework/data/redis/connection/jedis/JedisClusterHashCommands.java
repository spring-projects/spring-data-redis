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
package org.springframework.data.redis.connection.jedis;

import redis.clients.jedis.args.ExpiryOption;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.jspecify.annotations.Nullable;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.ExpirationOptions;
import org.springframework.data.redis.connection.RedisHashCommands;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanCursor;
import org.springframework.data.redis.core.ScanIteration;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.util.Assert;

/**
 * Cluster {@link RedisHashCommands} implementation for Jedis.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author John Blum
 * @author Tihomir Mateev
 * @since 2.0
 */
class JedisClusterHashCommands implements RedisHashCommands {

	private final JedisClusterConnection connection;

	JedisClusterHashCommands(JedisClusterConnection connection) {
		this.connection = connection;
	}

	@Override
	public Boolean hSet(byte[] key, byte[] field, byte[] value) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(field, "Field must not be null");
		Assert.notNull(value, "Value must not be null");

		try {
			return JedisConverters.toBoolean(connection.getCluster().hset(key, field, value));
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
			return JedisConverters.toBoolean(connection.getCluster().hsetnx(key, field, value));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public byte[] hGet(byte[] key, byte[] field) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(field, "Field must not be null");

		try {
			return connection.getCluster().hget(key, field);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public List<byte[]> hMGet(byte[] key, byte[]... fields) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(fields, "Fields must not be null");

		try {
			return connection.getCluster().hmget(key, fields);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public void hMSet(byte[] key, Map<byte[], byte[]> hashes) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(hashes, "Hashes must not be null");

		try {
			connection.getCluster().hmset(key, hashes);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long hIncrBy(byte[] key, byte[] field, long delta) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(field, "Field must not be null");

		try {
			return connection.getCluster().hincrBy(key, field, delta);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Double hIncrBy(byte[] key, byte[] field, double delta) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(field, "Field must not be null");

		try {
			return connection.getCluster().hincrByFloat(key, field, delta);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}


	@Override
	public byte @Nullable[] hRandField(byte[] key) {

		Assert.notNull(key, "Key must not be null");

		try {
			return connection.getCluster().hrandfield(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Nullable
	@Override
	public Entry<byte[], byte[]> hRandFieldWithValues(byte[] key) {

		Assert.notNull(key, "Key must not be null");

		try {
			List<Entry<byte[], byte[]>> mapEntryList = connection.getCluster().hrandfieldWithValues(key, 1);
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
			return connection.getCluster().hrandfield(key, count);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Nullable
	@Override
	public List<Entry<byte[], byte[]>> hRandFieldWithValues(byte[] key, long count) {

		try {
			return connection.getCluster().hrandfieldWithValues(key, count);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Boolean hExists(byte[] key, byte[] field) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(field, "Field must not be null");

		try {
			return connection.getCluster().hexists(key, field);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long hDel(byte[] key, byte[]... fields) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(fields, "Fields must not be null");

		try {
			return connection.getCluster().hdel(key, fields);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long hLen(byte[] key) {

		Assert.notNull(key, "Key must not be null");

		try {
			return connection.getCluster().hlen(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Set<byte[]> hKeys(byte[] key) {

		Assert.notNull(key, "Key must not be null");

		try {
			return connection.getCluster().hkeys(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public List<byte[]> hVals(byte[] key) {

		Assert.notNull(key, "Key must not be null");

		try {
			return new ArrayList<>(connection.getCluster().hvals(key));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Map<byte[], byte[]> hGetAll(byte[] key) {

		Assert.notNull(key, "Key must not be null");

		try {
			return connection.getCluster().hgetAll(key);
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

				ScanResult<Entry<byte[], byte[]>> result = connection.getCluster().hscan(key, JedisConverters.toBytes(cursorId),
						params);
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
				return connection.getCluster().hexpire(key, seconds, fields);
			}

			return connection.getCluster().hexpire(key, seconds, ExpiryOption.valueOf(condition.name()), fields);
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
				return connection.getCluster().hpexpire(key, millis, fields);
			}

			return connection.getCluster().hpexpire(key, millis, ExpiryOption.valueOf(condition.name()), fields);
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
				return connection.getCluster().hexpireAt(key, unixTime, fields);
			}

			return connection.getCluster().hexpireAt(key, unixTime, ExpiryOption.valueOf(condition.name()), fields);
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
				return connection.getCluster().hpexpireAt(key, unixTimeInMillis, fields);
			}

			return connection.getCluster().hpexpireAt(key, unixTimeInMillis, ExpiryOption.valueOf(condition.name()), fields);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public List<Long> hPersist(byte[] key, byte[]... fields) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(fields, "Fields must not be null");

		try {
			return connection.getCluster().hpersist(key, fields);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public List<Long> hTtl(byte[] key, byte[]... fields) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(fields, "Fields must not be null");

		try {
			return connection.getCluster().httl(key, fields);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public List<Long> hTtl(byte[] key, TimeUnit timeUnit, byte[]... fields) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(fields, "Fields must not be null");

		try {
			return connection.getCluster().httl(key, fields).stream()
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
			return connection.getCluster().hpttl(key, fields);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Nullable
	@Override
	public Long hStrLen(byte[] key, byte[] field) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(field, "Field must not be null");

		return connection.getCluster().hstrlen(key, field);
	}

	private DataAccessException convertJedisAccessException(Exception ex) {
		return connection.convertJedisAccessException(ex);
	}

}
