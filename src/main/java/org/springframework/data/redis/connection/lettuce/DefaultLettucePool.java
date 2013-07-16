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
import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.redis.connection.PoolException;
import org.springframework.util.Assert;

import com.lambdaworks.redis.RedisAsyncConnection;
import com.lambdaworks.redis.RedisClient;

/**
 * Default implementation of {@link LettucePool}
 * 
 * @author Jennifer Hickey
 * 
 */
public class DefaultLettucePool implements LettucePool, InitializingBean {
	private GenericObjectPool internalPool;
	private RedisClient client;
	private int dbIndex = 0;
	private Config poolConfig = new Config();
	private String hostName = "localhost";
	private int port = 6379;
	private String password;
	private long timeout = TimeUnit.MILLISECONDS.convert(60, TimeUnit.SECONDS);

	/**
	 * Constructs a new <code>DefaultLettucePool</code> instance with
	 * default settings.
	 */
	public DefaultLettucePool() {
	}

	/**
	 * Uses the {@link Config} and {@link RedisClient} defaults for configuring
	 * the connection pool
	 * 
	 * @param hostName
	 *            The Redis host
	 * @param port
	 *            The Redis port
	 */
	public DefaultLettucePool(String hostName, int port) {
		this.hostName = hostName;
		this.port = port;
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
	public DefaultLettucePool(String hostName, int port, Config poolConfig) {
		this.hostName = hostName;
		this.port = port;
		this.poolConfig = poolConfig;
	}

	public void afterPropertiesSet() {
		this.client = password != null ? new AuthenticatingRedisClient(hostName, port, password) :
			new RedisClient(hostName, port);
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

	public RedisClient getClient() {
		return client;
	}

	/**
	 *
	 * @return The pool configuration
	 */
	public Config getPoolConfig() {
		return poolConfig;
	}

	/**
	 *
	 * @param poolConfig The pool configuration to use
	 */
	public void setPoolConfig(Config poolConfig) {
		this.poolConfig = poolConfig;
	}

	/**
	 * Returns the index of the database.
	 *
	 * @return Returns the database index
	 */
	public int getDatabase() {
		return dbIndex;
	}

	/**
	 * Sets the index of the database used by this connection pool. Default
	 * is 0.
	 *
	 * @param index
	 *            database index
	 */
	public void setDatabase(int index) {
		Assert.isTrue(index >= 0, "invalid DB index (a positive index required)");
		this.dbIndex = index;
	}

	/**
	 * Returns the password used for authenticating with the Redis server.
	 *
	 * @return password for authentication
	 */
	public String getPassword() {
		return password;
	}

	/**
	 * Sets the password used for authenticating with the Redis server.
	 *
	 * @param password the password to set
	 */
	public void setPassword(String password) {
		this.password = password;
	}

	/**
	 * Returns the current host.
	 *
	 * @return the host
	 */
	public String getHostName() {
		return hostName;
	}

	/**
	 * Sets the host.
	 *
	 * @param host
	 *            the host to set
	 */
	public void setHostName(String host) {
		this.hostName = host;
	}

	/**
	 * Returns the current port.
	 *
	 * @return the port
	 */
	public int getPort() {
		return port;
	}

	/**
	 * Sets the port.
	 *
	 * @param port
	 *            the port to set
	 */
	public void setPort(int port) {
		this.port = port;
	}

	/**
	 * Returns the connection timeout (in milliseconds).
	 *
	 * @return connection timeout
	 */
	public long getTimeout() {
		return timeout;
	}

	/**
	 * Sets the connection timeout (in milliseconds).
	 *
	 * @param timeout
	 *            connection timeout
	 */
	public void setTimeout(long timeout) {
		this.timeout = timeout;
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
			return client.connectAsync(LettuceConnection.CODEC);
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
