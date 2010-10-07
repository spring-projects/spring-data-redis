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
package org.springframework.datastore.redis.core.jredis;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jredis.ri.alphazero.JRedisClient;
import org.jredis.ri.alphazero.support.DefaultCodec;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataRetrievalFailureException;
import org.springframework.datastore.redis.core.AbstractRedisClient;
import org.springframework.datastore.redis.support.RedisPersistenceExceptionTranslator;
import org.springframework.util.Assert;

/**
 * JRedis implementation of RedisClient. Name has 'Spring' in it to avoid naming
 * conflict with classes in JRedis itself.
 * 
 * @author Mark Pollack
 * 
 */
public class JRedisSpringClient extends AbstractRedisClient {

	/** Logger available to subclasses */
	protected final Log logger = LogFactory.getLog(getClass());

	private JRedisClient _jredisClient;
	private RedisPersistenceExceptionTranslator exceptionTranslator;

	public JRedisSpringClient(JRedisClient jredisClient,
			RedisPersistenceExceptionTranslator exceptionTransator) {
		this._jredisClient = jredisClient;
		this.exceptionTranslator = exceptionTransator; 
		this.setDefaultCharset(DefaultCodec.SUPPORTED_CHARSET_NAME);
	}
	

	protected Integer convertToInteger(long longTime) {
		if (longTime < Integer.MIN_VALUE
				|| longTime > Integer.MAX_VALUE) {
			throw new DataRetrievalFailureException(
					longTime
							+ " cannot be cast to int without changing its value.");
		}
		return (int) longTime;
	}

	public <T> T execute(JRedisClientCallback<T> action) {
		Assert.notNull(action, "Callback object must not be null");

		// TODO jredisClient resource mgmt.
		try {
			if (logger.isDebugEnabled()) {
				logger.debug("Executing callback on JRedisClient : "
						+ _jredisClient);
			}
			return action.doInJRedis(_jredisClient);
		} catch (Exception e) {
			throw convertJRedisAccessException(e);
		}

	}

	protected DataAccessException convertJRedisAccessException(Exception ex) {
		return exceptionTranslator.translateException(ex);
	}

	public void disconnect() {
		// TODO look at disconnect exception translation
		execute(new JRedisClientCallback<Object>() {
			public Object doInJRedis(JRedisClient jredisClient)
					throws Exception {
				jredisClient.quit();
				return null;
			}
		});
	}

	public String get(final String key) {
		return execute(new JRedisClientCallback<String>() {
			public String doInJRedis(JRedisClient jredisClient)
					throws Exception {
				return byteToString(jredisClient.get(key));
			}
		});
	}

	public byte[] getAsBytes(final String key) {
		return execute(new JRedisClientCallback<byte[]>() {
			public byte[] doInJRedis(JRedisClient jredisClient)
					throws Exception {
				return jredisClient.get(key);
			}
		});
	}

	public void set(final String key, final String value) {
		execute(new JRedisClientCallback<Object>() {
			public String doInJRedis(JRedisClient jredisClient)
					throws Exception {
				jredisClient.set(key, value);
				return null;
			}
		});
	}

	public void set(final String key, final byte[] value) {
		execute(new JRedisClientCallback<Object>() {
			public String doInJRedis(JRedisClient jredisClient)
					throws Exception {
				jredisClient.set(key, value);
				return null;
			}
		});
	}

	public String save() {
		return execute(new JRedisClientCallback<String>() {
			public String doInJRedis(JRedisClient jredisClient)
					throws Exception {
				jredisClient.save();
				return "OK";
			}
		});
	}

	public String bgsave() {
		return execute(new JRedisClientCallback<String>() {
			public String doInJRedis(JRedisClient jredisClient)
					throws Exception {
				jredisClient.bgsave();
				return "Background saving started";
			}
		});
	}

	public String bgrewriteaof() {
		return execute(new JRedisClientCallback<String>() {
			public String doInJRedis(JRedisClient jredisClient)
					throws Exception {
				jredisClient.bgrewriteaof();
				return "Background append only file rewriting started";
			}
		});
	}

	public Integer lastsave() {
		return execute(new JRedisClientCallback<Integer>() {
			public Integer doInJRedis(JRedisClient jredisClient)
					throws Exception {
				long longTime = jredisClient.lastsave();
				// odd that JRedis return long when the Redis command spec says
				// int.
				return convertToInteger(longTime);
			}
		});
	}

	public String shutdown() {
		return execute(new JRedisClientCallback<String>() {
			public String doInJRedis(JRedisClient jredisClient)
					throws Exception {
				throw new UnsupportedOperationException("JRedis does not implement SHUTDOWN command"); 				
			}
		});
	}

	public Map<String, String> info() {
		return execute(new JRedisClientCallback<Map<String, String>>() {
			public Map<String, String> doInJRedis(JRedisClient jredisClient)
					throws Exception {
				return jredisClient.info();
			}
		});
	}

	public String slaveof(final String host, final int port) {
		return execute(new JRedisClientCallback<String>() {
			public String doInJRedis(JRedisClient jredisClient)
					throws Exception {
				jredisClient.slaveof(host,port);
				return "TODO - EXTRACT CORRECT STRING RESPONSE FROM REDIS FOR COMMAND SLAVEOF";
			}
		});
	}

	public String slaveofNoOne() {
		return execute(new JRedisClientCallback<String>() {
			public String doInJRedis(JRedisClient jredisClient)
					throws Exception {
				jredisClient.slaveofnone();
				return "TODO - EXTRACT CORRECT STRING RESPONSE FROM REDIS FOR COMMAND SLAVEOF NO ONE";
			}
		});
	}

	public String select(int index) {
		return execute(new JRedisClientCallback<String>() {
			public String doInJRedis(JRedisClient jredisClient)
					throws Exception {
				throw new UnsupportedOperationException("JRedis does not implement SELECT command"); 	
			}
		});
	}

	public String flushDb() {
		return execute(new JRedisClientCallback<String>() {
			public String doInJRedis(JRedisClient jredisClient)
					throws Exception {
				jredisClient.flushdb();
				//TODO why does flushdb() return JRedis interface? 
				return "OK";
			}
		});
	}

	public String flushAll() {
		return execute(new JRedisClientCallback<String>() {
			public String doInJRedis(JRedisClient jredisClient)
					throws Exception {
				jredisClient.flushall();
				//TODO why does flushdb() return JRedis interface? 
				return "OK";
			}
		});
	}

	public Integer move(final String key, final int dbIndex) {
		return execute(new JRedisClientCallback<Integer>() {
			public Integer doInJRedis(JRedisClient jredisClient)
					throws Exception {
				return jredisClient.move(key, dbIndex) ? 1 : 0;				
			}
		});
	}

	public String auth(String password) {
		return execute(new JRedisClientCallback<String>() {
			public String doInJRedis(JRedisClient jredisClient)
					throws Exception {
				throw new UnsupportedOperationException("JRedis does not implement AUTH command"); 
			}
		});
	}
	
	public Integer dbSize() {
		return execute(new JRedisClientCallback<Integer>() {
			public Integer doInJRedis(JRedisClient jredisClient)
					throws Exception {
				return convertToInteger(jredisClient.dbsize());				
			}
		});
	}


	public String getSet(String key, String value) {
		// TODO Auto-generated method stub
		return null;
	}


	public List<String> mget(String... keys) {
		// TODO Auto-generated method stub
		return null;
	}


	public Integer setnx(String key, String value) {
		// TODO Auto-generated method stub
		return null;
	}


	public String setex(String key, int seconds, String value) {
		// TODO Auto-generated method stub
		return null;
	}


	public String mset(String... keysvalues) {
		// TODO Auto-generated method stub
		return null;
	}


	public Integer msetnx(String... keysvalues) {
		// TODO Auto-generated method stub
		return null;
	}


	public Integer incrBy(String key, int increment) {
		// TODO Auto-generated method stub
		return null;
	}


	public Integer incr(String key) {
		// TODO Auto-generated method stub
		return null;
	}


	public Integer decr(String key) {
		// TODO Auto-generated method stub
		return null;
	}


	public Integer decrBy(String key, int decrement) {
		// TODO Auto-generated method stub
		return null;
	}


	public Integer append(String key, String value) {
		// TODO Auto-generated method stub
		return null;
	}


	public String substr(String key, int start, int end) {
		// TODO Auto-generated method stub
		return null;
	}


	public Integer exists(String key) {
		// TODO Auto-generated method stub
		return null;
	}


	public Integer del(String... keys) {
		// TODO Auto-generated method stub
		return null;
	}


	public String type(String key) {
		// TODO Auto-generated method stub
		return null;
	}


	public List<String> keys(String pattern) {
		// TODO Auto-generated method stub
		return null;
	}


	public String randomKey() {
		// TODO Auto-generated method stub
		return null;
	}


	public String rename(String oldkey, String newkey) {
		// TODO Auto-generated method stub
		return null;
	}


	public Integer renamenx(String oldkey, String newkey) {
		// TODO Auto-generated method stub
		return null;
	}


	public Integer expire(String key, int seconds) {
		// TODO Auto-generated method stub
		return null;
	}


	public Integer expireAt(String key, long unixTime) {
		// TODO Auto-generated method stub
		return null;
	}


	public Integer ttl(String key) {
		// TODO Auto-generated method stub
		return null;
	}


	public Integer persist(String key) {
		// TODO Auto-generated method stub
		return null;
	}


	public Integer sadd(String key, String member) {
		// TODO Auto-generated method stub
		return null;
	}


	public Set<String> smembers(String key) {
		// TODO Auto-generated method stub
		return null;
	}


	public Integer srem(String key, String member) {
		// TODO Auto-generated method stub
		return null;
	}


	public String spop(String key) {
		// TODO Auto-generated method stub
		return null;
	}


	public Integer smove(String srckey, String dstkey, String member) {
		// TODO Auto-generated method stub
		return null;
	}


	public Integer scard(String key) {
		// TODO Auto-generated method stub
		return null;
	}


	public Integer sismember(String key, String member) {
		// TODO Auto-generated method stub
		return null;
	}


	public Set<String> sinter(String... keys) {
		// TODO Auto-generated method stub
		return null;
	}


	public Integer sinterstore(String dstkey, String... keys) {
		// TODO Auto-generated method stub
		return null;
	}


	public Set<String> sunion(String... keys) {
		// TODO Auto-generated method stub
		return null;
	}


	public Integer sunionstore(String dstkey, String... keys) {
		// TODO Auto-generated method stub
		return null;
	}


	public Set<String> sdiff(String... keys) {
		// TODO Auto-generated method stub
		return null;
	}


	public Integer sdiffstore(String dstkey, String... keys) {
		// TODO Auto-generated method stub
		return null;
	}


	public String srandmember(String key) {
		// TODO Auto-generated method stub
		return null;
	}

}
