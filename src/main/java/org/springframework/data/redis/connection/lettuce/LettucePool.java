/*
 * Copyright 2013 the original author or authors.
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
package org.springframework.data.redis.connection.lettuce;

import java.util.concurrent.TimeUnit;

import org.apache.commons.pool.BasePoolableObjectFactory;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.commons.pool.impl.GenericObjectPool.Config;
import org.springframework.data.redis.connection.Pool;
import org.springframework.data.redis.connection.PoolException;

import com.lambdaworks.redis.RedisAsyncConnection;
import com.lambdaworks.redis.RedisClient;

/**
 * Lettuce implementation of {@link Pool}
 * 
 * @author Jennifer Hickey
 * 
 */
public class LettucePool implements Pool<RedisAsyncConnection<byte[], byte[]>> {
	private final GenericObjectPool internalPool;
	private static final int DEFAULT_TIMEOUT = (int) TimeUnit.MILLISECONDS.convert(60, TimeUnit.SECONDS);

	/**
	 * Uses the {@link Config} and {@link RedisClient} defaults for configuring
	 * the connection pool
	 * 
	 * @param hostName
	 *            The Redis host
	 * @param port
	 *            The Redis port
	 */
	public LettucePool(String hostName, int port) {
		this(hostName, port, 0, DEFAULT_TIMEOUT, new Config());
	}

	/**
	 * Uses the {@link RedisClient} defaults for configuring the connection pool
	 * 
	 * @param hostName
	 *            The Redis host
	 * @param port
	 *            The Redis port
	 * @param poolConfig
	 *            The pool {@link Config}
	 */
	public LettucePool(String hostName, int port, Config poolConfig) {
		this(hostName, port, 0, DEFAULT_TIMEOUT, poolConfig);
	}

	/**
	 * 
	 * Uses the {@link Config} defaults for configuring the connection pool
	 * 
	 * @param client
	 *            The {@link RedisClient} for connecting to Redis
	 * 
	 */
	public LettucePool(RedisClient client) {
		this.internalPool = new GenericObjectPool(new LettuceFactory(client, 0), new Config());
	}

	/**
	 * 
	 * @param client
	 *            The {@link RedisClient} for connecting to Redis
	 * 
	 * @param poolConfig
	 *            The pool {@link Config}
	 * @param dbIndex
	 *            The index of the database all connections should use. The
	 *            database will be selected when connections are activated.
	 */
	public LettucePool(RedisClient client, Config poolConfig, int dbIndex) {
		this.internalPool = new GenericObjectPool(new LettuceFactory(client, dbIndex), poolConfig);
	}

	/**
	 * Uses the {@link Config} defaults for configuring the connection pool
	 * 
	 * @param hostName
	 *            The Redis host
	 * @param port
	 *            The Redis port
	 * @param dbIndex
	 *            The index of the database all connections should use. The
	 *            database will be selected when connections are activated.
	 * @param password
	 *            The password used for authenticating with the Redis server or
	 *            null if no password required
	 * @param timeout
	 *            The socket timeout or 0 to use the default socket timeout
	 */
	public LettucePool(String hostName, int port, int dbIndex, int timeout) {
		this(hostName, port, dbIndex, timeout, new Config());
	}

	/**
	 * 
	 * @param hostName
	 *            The Redis host
	 * @param port
	 *            The Redis port
	 * @param dbIndex
	 *            The index of the database all connections should use. The
	 *            database will be selected when connections are activated.
	 * @param password
	 *            The password used for authenticating with the Redis server or
	 *            null if no password required
	 * @param timeout
	 *            The socket timeout or 0 to use the default socket timeout
	 * @param poolConfig
	 *            The pool {@link COnfig}
	 */
	public LettucePool(String hostName, int port, int dbIndex, int timeout, Config poolConfig) {
		RedisClient client = new RedisClient(hostName, port);
		client.setDefaultTimeout(timeout, TimeUnit.MILLISECONDS);
		this.internalPool = new GenericObjectPool(new LettuceFactory(client, dbIndex), poolConfig);
	}

	@SuppressWarnings("unchecked")
	public RedisAsyncConnection<byte[], byte[]> getResource() {
		try {
			return (RedisAsyncConnection<byte[], byte[]>) internalPool.borrowObject();
		} catch (Exception e) {
			throw new PoolException("Could not get a resource from the pool", e);
		}
	}

	public void returnBrokenResource(final RedisAsyncConnection<byte[], byte[]> resource) {
		try {
			internalPool.invalidateObject(resource);
		} catch (Exception e) {
			throw new PoolException("Could not invalidate the broken resource", e);
		}
	}

	public void returnResource(final RedisAsyncConnection<byte[], byte[]> resource) {
		try {
			internalPool.returnObject(resource);
		} catch (Exception e) {
			throw new PoolException("Could not return the resource to the pool", e);
		}
	}

	public void destroy() {
		try {
			internalPool.close();
		} catch (Exception e) {
			throw new PoolException("Could not destroy the pool", e);
		}
	}

	private static class LettuceFactory extends BasePoolableObjectFactory {

		private final RedisClient client;

		private int dbIndex;

		public LettuceFactory(RedisClient client, int dbIndex) {
			super();
			this.client = client;
			this.dbIndex = dbIndex;
		}

		public Object makeObject() throws Exception {
			return client.connectAsync(LettuceUtils.CODEC);
		}

		@SuppressWarnings("rawtypes")
		@Override
		public void activateObject(Object obj) throws Exception {
			if (obj instanceof RedisAsyncConnection) {
				((RedisAsyncConnection) obj).select(dbIndex);
			}
		}

		@SuppressWarnings("rawtypes")
		public void destroyObject(final Object obj) throws Exception {
			if (obj instanceof RedisAsyncConnection) {
				try {
					((RedisAsyncConnection) obj).close();
				} catch (Exception e) {
					// Errors may happen if returning a broken resource
				}
			}
		}

		@SuppressWarnings("rawtypes")
		public boolean validateObject(final Object obj) {
			if (obj instanceof RedisAsyncConnection) {
				try {
					((RedisAsyncConnection) obj).ping();
					return true;
				} catch (Exception e) {
					return false;
				}
			} else {
				return false;
			}
		}
	}
}
