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

import java.util.concurrent.TimeoutException;

import org.springframework.datastore.redis.CannotGetRedisConnectionException;
import org.springframework.datastore.redis.core.AbstractRedisClientFactory;
import org.springframework.datastore.redis.core.RedisClient;
import org.springframework.datastore.redis.support.RedisPersistenceExceptionTranslator;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 * A RedisClientFactory implementation that uses Jedis's native connection/client
 * caching features.
 * 
 * @author Mark Pollack
 *
 */
public class CachingJedisClientFactory extends AbstractRedisClientFactory {

	private JedisPool pool;
	
	private int timeout;

	private int clientCacheSize;

	private long maxWaitTime;

	/**
	 * 
	 * @param clientCacheSize
	 */
	public CachingJedisClientFactory(int clientCacheSize) {
		this.clientCacheSize = clientCacheSize;
	}

	public CachingJedisClientFactory(JedisPool pool) {
		this.pool = pool;
	}

	public int getClientCacheSize() {
		return this.clientCacheSize;
	}

	public JedisPool getJedisPool() {
		return this.pool;
	}
	
	public int getTimeout() {
		return timeout;
	}

	protected void setTimeout(int timeout) {
		this.timeout = timeout;
	}

	public long getMaxWaitTime() {
		return this.maxWaitTime;
	}

	/**
	 * Sets the maximum amount of time (in milliseconds) the getResource() method 
	 * should block before throwing an TimeoutException. 
	 * @param maxWaitTime The maximum time you would like to wait for the resource.
	 */
	public void setMaxWaitTime(long maxWaitTime) {
		this.maxWaitTime = maxWaitTime;
	}

	@Override
	public RedisClient doGetClient() {
		Jedis jedis;
		if (getClientCacheSize() != 0) {
			pool = new JedisPool(getHostName(), getPort(), getTimeout());
			pool.setResourcesNumber(getClientCacheSize());
		}
		try {
			if (getMaxWaitTime() != 0)
				jedis = pool.getResource(getMaxWaitTime());
			else {
				jedis = pool.getResource();
			}
		} catch (TimeoutException e) {
			throw new CannotGetRedisConnectionException(
					"Timed out.  Could not get Redis Connection", e);
		}
		return new JedisClient(jedis, getExceptionTranslator() );
	}

	@Override
	public RedisPersistenceExceptionTranslator getExceptionTranslator() {
		return new JedisPersistenceExceptionTranslator();
	}

}
