/*
 * Copyright 2010 the original author or authors.
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
package org.springframework.datastore.redis.core.connection.jedis;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Collection;

import org.springframework.dao.DataAccessException;
import org.springframework.datastore.keyvalue.UncategorizedKeyvalueStoreException;
import org.springframework.datastore.redis.UncategorizedRedisException;
import org.springframework.datastore.redis.core.connection.DataType;
import org.springframework.datastore.redis.core.connection.RedisConnection;
import org.springframework.util.ReflectionUtils;

import redis.clients.jedis.Client;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisException;

/**
 * Jedis based {@link RedisConnection}.
 * 
 * @author Costin Leau
 */
public class JedisConnection implements RedisConnection<Jedis> {

	private static final Field CLIENT_FIELD;

	static {
		CLIENT_FIELD = ReflectionUtils.findField(Jedis.class, "client", Client.class);
		ReflectionUtils.makeAccessible(CLIENT_FIELD);
	}

	private final Jedis jedis;
	private final Client client;

	public JedisConnection(Jedis jedis) {
		this.jedis = jedis;
		// extract underlying client for batch operations
		client = (Client) ReflectionUtils.getField(CLIENT_FIELD, jedis);
	}

	protected DataAccessException convertJedisAccessException(Exception ex) {
		if (ex instanceof JedisException) {
			return JedisUtils.convertJedisAccessException((JedisException) ex);
		}
		if (ex instanceof IOException) {
			return JedisUtils.convertJedisAccessException((IOException) ex);
		}

		throw new UncategorizedKeyvalueStoreException("Unknown jedis exception", ex);
	}

	@Override
	public void close() throws UncategorizedRedisException {
		try {
			jedis.disconnect();
			jedis.quit();
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public String getCharset() {
		return "UTF-8";
	}

	@Override
	public Jedis getNativeConnection() {
		return jedis;
	}

	@Override
	public boolean isClosed() {
		try {
			return !jedis.isConnected();
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public int dbSize() {
		try {
			return jedis.dbSize();
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public int del(String... keys) {
		try {
			return jedis.del(keys);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public void discard() {
		try {
			client.discard();
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public void exec() {
		try {
			client.exec();
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public boolean exists(String key) {
		try {
			return (jedis.exists(key) == 1);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public boolean expire(String key, long seconds) {
		try {
			return (jedis.expire(key, (int) seconds) == 1);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Collection<String> keys(String pattern) {
		try {
			return (jedis.keys(pattern));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public void multi() {
		try {
			jedis.multi();
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public boolean persist(String key) {
		try {
			return (jedis.persist(key) == 1);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public String randomKey() {
		try {
			return jedis.randomKey();
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public boolean rename(String oldName, String newName) {
		try {
			return (JedisUtils.OK_CODE.equals(jedis.rename(oldName, newName)));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public boolean renameNx(String oldName, String newName) {
		try {
			return (JedisUtils.OK_CODE.equals(jedis.renamenx(oldName, newName)));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public void select(int dbIndex) {
		try {
			jedis.select(dbIndex);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public int ttl(String key) {
		try {
			return jedis.ttl(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public DataType type(String key) {
		try {
			return DataType.fromCode(jedis.type(key));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public void unwatch() {
		try {
			jedis.unwatch();
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public void watch(String... keys) {
		try {
			for (String key : keys) {
				jedis.watch(key);
			}
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public int hSet(String key, String field, String value) {
		try {
			return jedis.hset(key, field, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public int lPush(String key, String value) {
		try {
			return jedis.lpush(key, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public int rPush(String key, String value) {
		try {
			return jedis.rpush(key, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public String get(String key) {
		try {
			return jedis.get(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public void set(String key, String value) {
		try {
			jedis.set(key, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}
}