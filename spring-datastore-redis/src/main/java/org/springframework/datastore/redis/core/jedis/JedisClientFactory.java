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
import java.net.UnknownHostException;

import org.springframework.datastore.redis.CannotGetRedisConnectionException;
import org.springframework.datastore.redis.core.AbstractRedisClientFactory;
import org.springframework.datastore.redis.core.RedisClient;
import org.springframework.datastore.redis.core.RedisClientFactory;
import org.springframework.datastore.redis.core.jredis.JRedisPersistenceExceptionTranslator;
import org.springframework.datastore.redis.support.RedisPersistenceExceptionTranslator;

import redis.clients.jedis.Jedis;
import redis.clients.util.ShardInfo;

/**
 * A {@link RedisClientFactory} implementation that returns a new instance of a
 * Jedis backed RedisClient from call {@link #createClient()} calls.
 * 
 * @author Mark Pollack
 * 
 */
public class JedisClientFactory extends AbstractRedisClientFactory {

	private ShardInfo shardInfo;
	
	private int timeout;
	
	private RedisPersistenceExceptionTranslator exceptionTranslator = new JRedisPersistenceExceptionTranslator();
	
	public JedisClientFactory() {
		setHostName(getDefaultHostName());
	}
		
	public JedisClientFactory(String hostname) {
		setHostName(hostname);
	}
	
	public JedisClientFactory(String hostname, int port)
	{
		setHostName(hostname);
		setPort(port);
	}
	
	public JedisClientFactory(String hostname, int port, int timeout)
	{
		setHostName(hostname);
		setPort(port);
		setTimeout(timeout);
	}
	
	public JedisClientFactory(ShardInfo shardInfo) {
		this.shardInfo = shardInfo;
	}
	
	protected ShardInfo getShardInfo() {
		return this.shardInfo;
	}
	
	public int getTimeout() {
		return timeout;
	}

	protected void setTimeout(int timeout) {
		this.timeout = timeout;
	}
	
	@Override
	public RedisClient doGetClient() {
		Jedis jedis;
		if (getShardInfo() != null) {
			jedis = new Jedis(getShardInfo());
		}
		if (getPort() != 0 && getTimeout() != 0) {
			jedis = new Jedis(getHostName(), getPort(), getTimeout());
		} else if (getPort() != 0) {
			jedis = new Jedis(getHostName(), getPort());
		} else {
			jedis = new Jedis(getHostName());
		}		
		try {
			jedis.connect();
			if (getPassword() != null) {
				jedis.auth(getPassword());
			}			
		} catch (UnknownHostException e) {
			throw new CannotGetRedisConnectionException(
					"Could not get Redis Connection", e);
		} catch (IOException e) {
			throw new CannotGetRedisConnectionException(
					"Could not get Redis Connection", e);
		}
		return new JedisClient(jedis, getExceptionTranslator());
	}

	@Override
	public RedisPersistenceExceptionTranslator getExceptionTranslator() {
		return exceptionTranslator;
	}

	public void setExceptionTranslator(
			RedisPersistenceExceptionTranslator exceptionTranslator) {
		this.exceptionTranslator = exceptionTranslator;
	}

}
