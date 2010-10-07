/*
 * Copyright 2002-2010 the original author or authors.
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
package org.springframework.datastore.redis.core.jedis;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.dao.DataAccessException;
import org.springframework.datastore.redis.core.AbstractRedisClient;
import org.springframework.datastore.redis.support.RedisPersistenceExceptionTranslator;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import redis.clients.jedis.Jedis;

/**
 * Jedis based implementation of Spring's RedisClient interface. Presents a low
 * level API where method names map onto Redis commands.
 * 
 * @author Mark Pollack
 * 
 */
public class JedisClient extends AbstractRedisClient {

	private Jedis _jedis;
	private RedisPersistenceExceptionTranslator exceptionTranslator;

	public JedisClient(Jedis jedis,
			RedisPersistenceExceptionTranslator exceptionTranslator) {
		this._jedis = jedis;
		this.exceptionTranslator = exceptionTranslator;
	}

	public <T> T execute(JedisClientCallback<T> action) {
		Assert.notNull(action, "Callback object must not be null");

		// TODO jredisClient resource mgmt.
		try {
			if (logger.isDebugEnabled()) {
				logger.debug("Executing callback on Jedis : " + _jedis);
			}
			return action.doInJedis(_jedis);
		} catch (Exception e) {
			throw convertJedisAccessException(e);
		}

	}

	protected DataAccessException convertJedisAccessException(Exception ex) {
		return exceptionTranslator.translateException(ex);
	}

	public void disconnect() throws IOException {
		execute(new JedisClientCallback<Object>() {
			public Object doInJedis(Jedis jedis) throws Exception {
				jedis.disconnect();
				return null;
			}
		});
	}

	// Database control commands

	public String save() {
		return execute(new JedisClientCallback<String>() {
			public String doInJedis(Jedis jedis) throws Exception {
				return jedis.save();
			}
		});
	}

	public String bgsave() {
		return execute(new JedisClientCallback<String>() {
			public String doInJedis(Jedis jedis) throws Exception {
				return jedis.bgsave();
			}
		});
	}

	public String bgrewriteaof() {
		return execute(new JedisClientCallback<String>() {
			public String doInJedis(Jedis jedis) throws Exception {
				return jedis.bgrewriteaof();
			}
		});
	}

	public Integer lastsave() {
		return execute(new JedisClientCallback<Integer>() {
			public Integer doInJedis(Jedis jedis) throws Exception {
				return jedis.lastsave();
			}
		});
	}

	public String shutdown() {
		return execute(new JedisClientCallback<String>() {
			public String doInJedis(Jedis jedis) throws Exception {
				return jedis.shutdown();
			}
		});
	}

	public Map<String, String> info() {
		return execute(new JedisClientCallback<Map<String, String>>() {
			public Map<String, String> doInJedis(Jedis jedis) throws Exception {
				String[] response = StringUtils.delimitedListToStringArray(
						jedis.info(), "\r\n");
				Map<String, String> responseMap = new HashMap<String, String>();
				for (String responseLine : response) {
					if (!responseLine.isEmpty()) {
						String[] keyValue = StringUtils
								.split(responseLine, ":");
						if (keyValue == null) {
							logger.warn("Could not parse info reponse line ["
									+ responseLine + "]");
							continue;
						}
						responseMap.put(keyValue[0], keyValue[1]);
					}
				}
				return responseMap;
			}
		});
	}

	public String slaveof(final String host, final int port) {
		return execute(new JedisClientCallback<String>() {
			public String doInJedis(Jedis jedis) throws Exception {
				return jedis.slaveof(host, port);
			}
		});
	}

	public String slaveofNoOne() {
		return execute(new JedisClientCallback<String>() {
			public String doInJedis(Jedis jedis) throws Exception {
				return jedis.slaveofNoOne();
			}
		});
	}

	public String select(final int index) {
		return execute(new JedisClientCallback<String>() {
			public String doInJedis(Jedis jedis) throws Exception {
				return jedis.select(index);
			}
		});
	}

	public String flushDb() {
		return execute(new JedisClientCallback<String>() {
			public String doInJedis(Jedis jedis) throws Exception {
				return jedis.flushDB();
			}
		});
	}

	public String flushAll() {
		return execute(new JedisClientCallback<String>() {
			public String doInJedis(Jedis jedis) throws Exception {
				return jedis.flushAll();
			}
		});
	}

	public Integer move(final String key, final int dbIndex) {
		return execute(new JedisClientCallback<Integer>() {
			public Integer doInJedis(Jedis jedis) throws Exception {
				return jedis.move(key, dbIndex);
			}
		});
	}

	public String auth(final String password) {
		return execute(new JedisClientCallback<String>() {
			public String doInJedis(Jedis jedis) throws Exception {
				return jedis.auth(password);
			}
		});
	}

	public Integer dbSize() {
		return execute(new JedisClientCallback<Integer>() {
			public Integer doInJedis(Jedis jedis) throws Exception {
				return jedis.dbSize();
			}
		});
	}

	// Commands operating on string value types "StringOperations" or
	// "Operations"

	public void set(final String key, final String value) {
		execute(new JedisClientCallback<String>() {
			public String doInJedis(Jedis jedis) throws Exception {
				return jedis.set(key, value);
			}
		});
	}

	public void set(String key, byte[] value) {
		set(key, byteToString(value));
	}

	public String get(final String key) {
		return execute(new JedisClientCallback<String>() {
			public String doInJedis(Jedis jedis) throws Exception {
				return jedis.get(key);
			}
		});
	}

	public byte[] getAsBytes(String key) {
		return stringToByte(get(key));
	}

	public String getSet(final String key, final String value) {
		return execute(new JedisClientCallback<String>() {
			public String doInJedis(Jedis jedis) throws Exception {
				return jedis.getSet(key, value);
			}
		});
	}

	public List<String> mget(final String... keys) {
		return execute(new JedisClientCallback<List<String>>() {
			public List<String> doInJedis(Jedis jedis) throws Exception {
				return jedis.mget(keys);
			}
		});
	}

	public Integer setnx(final String key, final String value) {
		return execute(new JedisClientCallback<Integer>() {
			public Integer doInJedis(Jedis jedis) throws Exception {
				return jedis.setnx(key, value);
			}
		});
	}

	public String setex(final String key, final int seconds, final String value) {
		return execute(new JedisClientCallback<String>() {
			public String doInJedis(Jedis jedis) throws Exception {
				return jedis.setex(key, seconds, value);
			}
		});
	}

	public String mset(final String... keysvalues) {
		return execute(new JedisClientCallback<String>() {
			public String doInJedis(Jedis jedis) throws Exception {
				return jedis.mset(keysvalues);
			}
		});
	}

	public Integer msetnx(final String... keysvalues) {
		return execute(new JedisClientCallback<Integer>() {
			public Integer doInJedis(Jedis jedis) throws Exception {
				return jedis.msetnx(keysvalues);
			}
		});
	}

	public Integer incrBy(final String key, final int increment) {
		return execute(new JedisClientCallback<Integer>() {
			public Integer doInJedis(Jedis jedis) throws Exception {
				return jedis.incrBy(key, increment);
			}
		});
	}

	public Integer incr(final String key) {
		return execute(new JedisClientCallback<Integer>() {
			public Integer doInJedis(Jedis jedis) throws Exception {
				return jedis.incr(key);
			}
		});
	}

	public Integer decr(final String key) {
		return execute(new JedisClientCallback<Integer>() {
			public Integer doInJedis(Jedis jedis) throws Exception {
				return jedis.decr(key);
			}
		});
	}

	public Integer decrBy(final String key, final int increment) {
		return execute(new JedisClientCallback<Integer>() {
			public Integer doInJedis(Jedis jedis) throws Exception {
				return jedis.decrBy(key, increment);
			}
		});
	}

	public Integer append(final String key, final String value) {
		return execute(new JedisClientCallback<Integer>() {
			public Integer doInJedis(Jedis jedis) throws Exception {
				return jedis.append(key, value);
			}
		});
	}

	public String substr(final String key, final int start, final int end) {
		return execute(new JedisClientCallback<String>() {
			public String doInJedis(Jedis jedis) throws Exception {
				return jedis.substr(key, start, end);
			}
		});
	}

	// Commands operating on all value types "KeySpaceOperations"

	public Integer exists(final String key) {
		return execute(new JedisClientCallback<Integer>() {
			public Integer doInJedis(Jedis jedis) throws Exception {
				return jedis.exists(key);
			}
		});
	}

	public Integer del(final String... keys) {
		return execute(new JedisClientCallback<Integer>() {
			public Integer doInJedis(Jedis jedis) throws Exception {
				return jedis.del(keys);
			}
		});
	}

	public String type(final String key) {
		return execute(new JedisClientCallback<String>() {
			public String doInJedis(Jedis jedis) throws Exception {
				return jedis.type(key);
			}
		});
	}

	public List<String> keys(final String pattern) {
		return execute(new JedisClientCallback<List<String>>() {
			public List<String> doInJedis(Jedis jedis) throws Exception {
				return jedis.keys(pattern);
			}
		});
	}

	public String randomKey() {
		return execute(new JedisClientCallback<String>() {
			public String doInJedis(Jedis jedis) throws Exception {
				return jedis.randomKey();
			}
		});
	}

	public String rename(final String oldkey, final String newkey) {
		return execute(new JedisClientCallback<String>() {
			public String doInJedis(Jedis jedis) throws Exception {
				return jedis.rename(oldkey, newkey);
			}
		});
	}

	public Integer renamenx(final String oldkey, final String newkey) {
		return execute(new JedisClientCallback<Integer>() {
			public Integer doInJedis(Jedis jedis) throws Exception {
				return jedis.renamenx(oldkey, newkey);
			}
		});
	}

	public Integer expire(final String key, final int seconds) {
		return execute(new JedisClientCallback<Integer>() {
			public Integer doInJedis(Jedis jedis) throws Exception {
				return jedis.expire(key, seconds);
			}
		});
	}

	public Integer expireAt(final String key, final long unixTime) {
		return execute(new JedisClientCallback<Integer>() {
			public Integer doInJedis(Jedis jedis) throws Exception {
				return jedis.expireAt(key, unixTime);
			}
		});
	}

	public Integer ttl(final String key) {
		return execute(new JedisClientCallback<Integer>() {
			public Integer doInJedis(Jedis jedis) throws Exception {
				return jedis.ttl(key);
			}
		});
	}

	public Integer persist(final String key) {
		return execute(new JedisClientCallback<Integer>() {
			public Integer doInJedis(Jedis jedis) throws Exception {
				return jedis.persist(key);
			}
		});
	}

	// Commands operating on Sets

	public Integer sadd(final String key, final String member) {
		return execute(new JedisClientCallback<Integer>() {
			public Integer doInJedis(Jedis jedis) throws Exception {
				return jedis.sadd(key, member);
			}
		});
	}

	public Set<String> smembers(final String key) {
		return execute(new JedisClientCallback<Set<String>>() {
			public Set<String> doInJedis(Jedis jedis) throws Exception {
				return jedis.smembers(key);
			}
		});
	}

	public Integer srem(final String key, final String member) {
		return execute(new JedisClientCallback<Integer>() {
			public Integer doInJedis(Jedis jedis) throws Exception {
				return jedis.srem(key, member);
			}
		});
	}

	public String spop(final String key) {
		return execute(new JedisClientCallback<String>() {
			public String doInJedis(Jedis jedis) throws Exception {
				return jedis.spop(key);
			}
		});
	}

	public Integer smove(final String srckey, final String dstkey,
			final String member) {
		return execute(new JedisClientCallback<Integer>() {
			public Integer doInJedis(Jedis jedis) throws Exception {
				return jedis.smove(srckey, dstkey, member);
			}
		});
	}

	public Integer scard(final String key) {
		return execute(new JedisClientCallback<Integer>() {
			public Integer doInJedis(Jedis jedis) throws Exception {
				return jedis.scard(key);
			}
		});
	}

	public Integer sismember(final String key, final String member) {
		return execute(new JedisClientCallback<Integer>() {
			public Integer doInJedis(Jedis jedis) throws Exception {
				return jedis.sismember(key, member);
			}
		});
	}

	public Set<String> sinter(final String... keys) {
		return execute(new JedisClientCallback<Set<String>>() {
			public Set<String> doInJedis(Jedis jedis) throws Exception {
				return jedis.sinter(keys);
			}
		});
	}

	public Integer sinterstore(final String dstkey, final String... keys) {
		return execute(new JedisClientCallback<Integer>() {
			public Integer doInJedis(Jedis jedis) throws Exception {
				return jedis.sinterstore(dstkey, keys);
			}
		});
	}

	public Set<String> sunion(final String... keys) {
		return execute(new JedisClientCallback<Set<String>>() {
			public Set<String> doInJedis(Jedis jedis) throws Exception {
				return jedis.sunion(keys);
			}
		});
	}

	public Integer sunionstore(final String dstkey, final String... keys) {
		return execute(new JedisClientCallback<Integer>() {
			public Integer doInJedis(Jedis jedis) throws Exception {
				return jedis.sunionstore(dstkey, keys);	
			}
		});
	}

	public Set<String> sdiff(final String... keys) {
		return execute(new JedisClientCallback<Set<String>>() {
			public Set<String> doInJedis(Jedis jedis) throws Exception {
				return jedis.sdiff(keys);
			}
		});
	}

	public Integer sdiffstore(final String dstkey, final String... keys) {
		return execute(new JedisClientCallback<Integer>() {
			public Integer doInJedis(Jedis jedis) throws Exception {
				return jedis.sdiffstore(dstkey, keys);
			}
		});
	}

	public String srandmember(final String key) {
		return execute(new JedisClientCallback<String>() {
			public String doInJedis(Jedis jedis) throws Exception {
				return jedis.srandmember(key);
			}
		});
	}

}
