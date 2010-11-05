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
package org.springframework.datastore.redis.connection.jedis;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Collection;

import org.springframework.dao.DataAccessException;
import org.springframework.datastore.keyvalue.UncategorizedKeyvalueStoreException;
import org.springframework.datastore.redis.UncategorizedRedisException;
import org.springframework.datastore.redis.connection.DataType;
import org.springframework.datastore.redis.connection.RedisConnection;
import org.springframework.util.ReflectionUtils;

import redis.clients.jedis.Client;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisException;
import redis.clients.jedis.Transaction;

/**
 * Jedis based {@link RedisConnection}.
 * 
 * @author Costin Leau
 */
public class JedisConnection implements RedisConnection {

	private static final Field CLIENT_FIELD;

	static {
		CLIENT_FIELD = ReflectionUtils.findField(Jedis.class, "client", Client.class);
		ReflectionUtils.makeAccessible(CLIENT_FIELD);
	}

	private final Jedis jedis;
	private final Client client;
	private final Transaction transaction;

	public JedisConnection(Jedis jedis) {
		this.jedis = jedis;
		// extract underlying connection for batch operations
		client = (Client) ReflectionUtils.getField(CLIENT_FIELD, jedis);
		transaction = new Transaction(client);
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
			if (isQueueing()) {
				client.quit();
				client.disconnect();
			}
			jedis.quit();
			jedis.disconnect();
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public String getEncoding() {
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
	public boolean isQueueing() {
		return client.isInMulti();
	}

	@Override
	public Integer dbSize() {
		try {
			if (isQueueing()) {
				transaction.dbSize();
				return null;
			}
			return jedis.dbSize();
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Integer del(String... keys) {
		try {
			if (isQueueing()) {
				transaction.del(keys);
				return null;
			}
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
	public Boolean exists(String key) {
		try {
			if (isQueueing()) {
				transaction.exists(key);
				return null;
			}
			return (jedis.exists(key) == 1);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Boolean expire(String key, int seconds) {
		try {
			if (isQueueing()) {
				transaction.expire(key, seconds);
				return null;
			}
			return (jedis.expire(key, (int) seconds) == 1);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Collection<String> keys(String pattern) {
		try {
			if (isQueueing()) {
				transaction.keys(pattern);
				return null;
			}
			return (jedis.keys(pattern));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public void multi() {
		try {
			client.multi();
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Boolean persist(String key) {
		try {
			if (isQueueing()) {
				client.persist(key);
				return null;
			}
			return (jedis.persist(key) == 1);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public String randomKey() {
		try {
			if (isQueueing()) {
				transaction.randomKey();
				return null;
			}
			return jedis.randomKey();
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Boolean rename(String oldName, String newName) {
		try {
			if (isQueueing()) {
				transaction.rename(oldName, newName);
				return null;
			}
			return (JedisUtils.isStatusOk(jedis.rename(oldName, newName)));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Boolean renameNx(String oldName, String newName) {
		try {
			if (isQueueing()) {
				transaction.renamenx(oldName, newName);
				return null;
			}
			return (jedis.renamenx(oldName, newName) == 1);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public void select(int dbIndex) {
		try {
			if (isQueueing()) {
				transaction.select(dbIndex);
			}
			jedis.select(dbIndex);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Integer ttl(String key) {
		try {
			if (isQueueing()) {
				transaction.ttl(key);
				return null;
			}
			return jedis.ttl(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public DataType type(String key) {
		try {
			if (isQueueing()) {
				transaction.type(key);
				return null;
			}
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
		if (isQueueing()) {
			// ignore (as watch not allowed in multi)
			return;
		}

		try {
			for (String key : keys) {
				jedis.watch(key);
			}
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Integer hSet(String key, String field, String value) {
		try {
			if (isQueueing()) {
				transaction.hset(key, field, value);
				return null;
			}
			return jedis.hset(key, field, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Integer lPush(String key, String value) {
		try {
			if (isQueueing()) {
				transaction.lpush(key, value);
				return null;
			}
			return jedis.lpush(key, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Integer rPush(String key, String value) {
		try {
			if (isQueueing()) {
				transaction.rpush(key, value);
				return null;
			}
			return jedis.rpush(key, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public String get(String key) {
		try {
			if (isQueueing()) {
				transaction.get(key);
				return null;
			}

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