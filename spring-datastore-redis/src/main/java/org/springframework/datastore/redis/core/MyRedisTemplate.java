/*
 * Copyright 2006-2009 the original author or authors.
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
package org.springframework.datastore.redis.core;

import org.springframework.datastore.redis.core.connection.RedisConnection;
import org.springframework.datastore.redis.core.connection.RedisConnectionFactory;

/**
 * 
 * Helper class that simplifies Redis data access code. Automatically converts Redis client exceptions into 
 * DataAccessExceptions, following the org.springframework.dao exception hierarchy.
 *
 * The central method is execute, supporting Redis access code implementing the {@link MyRedisCallback} interface.
 * It provides {@link RedisConnection} handling such that neither the {@link MyRedisCallback} implementation nor 
 * the calling code needs to explicitly care about retrieving/closing Redis connections, or handling Session 
 * lifecycle exceptions. For typical single step actions, there are various convenience methods.
 *  
 * <b>This is the central class in Redis support</b>.
 * Simplifies the use of Redis and helps avoid common errors.
 * 
 * @author Costin Leau
 */
public class MyRedisTemplate extends MyRedisAccessor {

	private boolean exposeConnection = false;

	public MyRedisTemplate() {
	}

	public MyRedisTemplate(RedisConnectionFactory connectionFactory) {
		this.setConnectionFactory(connectionFactory);
		afterPropertiesSet();
	}

	public <T> T execute(MyRedisCallback<T> action) {
		throw new UnsupportedOperationException();
	}

	/**
	 * Sets whether to expose the Redis connection to {@link MyRedisCallback} code.
	 * 
	 * Default is "false": a proxy will be returned, suppressing <tt>quit</tt> and <tt>disconnect</tt> calls.
	 *  
	 * @param exposeConnection
	 */
	public void setExposeConnection(boolean exposeConnection) {
		this.exposeConnection = exposeConnection;
	}
}
