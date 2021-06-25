/*
 * Copyright 2017-2021 the original author or authors.
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

import redis.clients.jedis.ScanParams;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisHashCommands;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanCursor;
import org.springframework.data.redis.core.ScanIteration;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.util.Streamable;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.0
 */
class JedisClusterHashCommands implements RedisHashCommands {

	private final JedisClusterConnection connection;

	JedisClusterHashCommands(JedisClusterConnection connection) {
		this.connection = connection;
	}

	/*
	* (non-Javadoc)
	* @see org.springframework.data.redis.connection.RedisHashCommands#hSet(byte[], byte[], byte[])
	*/
	@Override
	public Boolean hSet(byte[] key, byte[] field, byte[] value) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(field, "Field must not be null!");
		Assert.notNull(value, "Value must not be null!");

		try {
			return JedisConverters.toBoolean(connection.getCluster().hset(key, field, value));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisHashCommands#hSetNX(byte[], byte[], byte[])
	 */
	@Override
	public Boolean hSetNX(byte[] key, byte[] field, byte[] value) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(field, "Field must not be null!");
		Assert.notNull(value, "Value must not be null!");

		try {
			return JedisConverters.toBoolean(connection.getCluster().hsetnx(key, field, value));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisHashCommands#hGet(byte[], byte[])
	 */
	@Override
	public byte[] hGet(byte[] key, byte[] field) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(field, "Field must not be null!");

		try {
			return connection.getCluster().hget(key, field);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisHashCommands#hMGet(byte[], byte[][])
	 */
	@Override
	public List<byte[]> hMGet(byte[] key, byte[]... fields) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(fields, "Fields must not be null!");

		try {
			return connection.getCluster().hmget(key, fields);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisHashCommands#hMSet(byte[], java.util.Map)
	 */
	@Override
	public void hMSet(byte[] key, Map<byte[], byte[]> hashes) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(hashes, "Hashes must not be null!");

		try {
			connection.getCluster().hmset(key, hashes);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisHashCommands#hIncrBy(byte[], byte[], long)
	 */
	@Override
	public Long hIncrBy(byte[] key, byte[] field, long delta) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(field, "Field must not be null!");

		try {
			return connection.getCluster().hincrBy(key, field, delta);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisHashCommands#hIncrBy(byte[], byte[], double)
	 */
	@Override
	public Double hIncrBy(byte[] key, byte[] field, double delta) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(field, "Field must not be null!");

		try {
			return connection.getCluster().hincrByFloat(key, field, delta);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisHashCommands#hRandField(byte[])
	 */
	@Nullable
	@Override
	public byte[] hRandField(byte[] key) {

		Assert.notNull(key, "Key must not be null!");

		try {
			return connection.getCluster().hrandfield(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisHashCommands#hRandFieldWithValues(byte[])
	 */
	@Nullable
	@Override
	public Entry<byte[], byte[]> hRandFieldWithValues(byte[] key) {

		Assert.notNull(key, "Key must not be null!");

		try {
			Map<byte[], byte[]> map = connection.getCluster().hrandfieldWithValues(key, 1);
			return map.isEmpty() ? null : map.entrySet().iterator().next();
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisHashCommands#hRandField(byte[], long)
	 */
	@Nullable
	@Override
	public List<byte[]> hRandField(byte[] key, long count) {

		Assert.notNull(key, "Key must not be null!");

		try {
			return connection.getCluster().hrandfield(key, count);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisHashCommands#hRandFieldWithValues(byte[], long)
	 */
	@Nullable
	@Override
	public List<Entry<byte[], byte[]>> hRandFieldWithValues(byte[] key, long count) {

		try {
			Map<byte[], byte[]> map = connection.getCluster().hrandfieldWithValues(key, count);
			return Streamable.of(() -> map.entrySet().iterator()).toList();
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisHashCommands#hExists(byte[], byte[])
	 */
	@Override
	public Boolean hExists(byte[] key, byte[] field) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(field, "Field must not be null!");

		try {
			return connection.getCluster().hexists(key, field);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisHashCommands#hDel(byte[], byte[][])
	 */
	@Override
	public Long hDel(byte[] key, byte[]... fields) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(fields, "Fields must not be null!");

		try {
			return connection.getCluster().hdel(key, fields);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisHashCommands#hLen(byte[])
	 */
	@Override
	public Long hLen(byte[] key) {

		Assert.notNull(key, "Key must not be null!");

		try {
			return connection.getCluster().hlen(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisHashCommands#hKeys(byte[])
	 */
	@Override
	public Set<byte[]> hKeys(byte[] key) {

		Assert.notNull(key, "Key must not be null!");

		try {
			return connection.getCluster().hkeys(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisHashCommands#hVals(byte[])
	 */
	@Override
	public List<byte[]> hVals(byte[] key) {

		Assert.notNull(key, "Key must not be null!");

		try {
			return new ArrayList<>(connection.getCluster().hvals(key));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisHashCommands#hGetAll(byte[])
	 */
	@Override
	public Map<byte[], byte[]> hGetAll(byte[] key) {

		Assert.notNull(key, "Key must not be null!");

		try {
			return connection.getCluster().hgetAll(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisHashCommands#hScan(byte[], org.springframework.data.redis.core.ScanOptions)
	 */
	@Override
	public Cursor<Entry<byte[], byte[]>> hScan(byte[] key, ScanOptions options) {

		Assert.notNull(key, "Key must not be null!");

		return new ScanCursor<Entry<byte[], byte[]>>(options) {

			@Override
			protected ScanIteration<Entry<byte[], byte[]>> doScan(long cursorId, ScanOptions options) {

				ScanParams params = JedisConverters.toScanParams(options);

				redis.clients.jedis.ScanResult<Map.Entry<byte[], byte[]>> result = connection.getCluster().hscan(key,
						JedisConverters.toBytes(cursorId), params);
				return new ScanIteration<>(Long.valueOf(result.getCursor()), result.getResult());
			}
		}.open();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisHashCommands#hStrLen(byte[], byte[])
	 */
	@Nullable
	@Override
	public Long hStrLen(byte[] key, byte[] field) {
		return Long.class.cast(connection.execute("HSTRLEN", key, Collections.singleton(field)));
	}

	private DataAccessException convertJedisAccessException(Exception ex) {

		return connection.convertJedisAccessException(ex);
	}
}
