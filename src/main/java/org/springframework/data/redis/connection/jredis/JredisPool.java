/*
 * Copyright 2011-2019 the original author or authors.
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
package org.springframework.data.redis.connection.jredis;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.jredis.JRedis;
import org.jredis.connector.Connection;
import org.jredis.connector.Connection.Socket.Property;
import org.jredis.connector.ConnectionSpec;
import org.jredis.ri.alphazero.JRedisClient;
import org.jredis.ri.alphazero.connection.DefaultConnectionSpec;
import org.springframework.data.redis.connection.Pool;
import org.springframework.data.redis.connection.PoolException;
import org.springframework.util.StringUtils;

/**
 * JRedis implementation of {@link Pool}
 * 
 * @author Jennifer Hickey
 * @author Christoph Strobl
 * @deprecated since 1.7. Will be removed in subsequent version.
 */
@Deprecated
public class JredisPool implements Pool<JRedis> {

	private final GenericObjectPool<JRedis> internalPool;

	/**
	 * Uses the {@link Config} and {@link ConnectionSpec} defaults for configuring the connection pool
	 * 
	 * @param hostName The Redis host
	 * @param port The Redis port
	 */
	public JredisPool(String hostName, int port) {
		this(hostName, port, 0, null, 0, new GenericObjectPoolConfig());
	}

	/**
	 * Uses the {@link ConnectionSpec} defaults for configuring the connection pool
	 * 
	 * @param hostName The Redis host
	 * @param port The Redis port
	 * @param poolConfig The pool {@link Config}
	 */
	public JredisPool(String hostName, int port, GenericObjectPoolConfig poolConfig) {
		this(hostName, port, 0, null, 0, poolConfig);
	}

	/**
	 * Uses the {@link Config} defaults for configuring the connection pool
	 * 
	 * @param connectionSpec The {@link ConnectionSpec} for connecting to Redis
	 */
	public JredisPool(ConnectionSpec connectionSpec) {
		this.internalPool = new GenericObjectPool<JRedis>(new JredisFactory(connectionSpec), new GenericObjectPoolConfig());
	}

	/**
	 * @param connectionSpec The {@link ConnectionSpec} for connecting to Redis
	 * @param poolConfig The pool {@link Config}
	 */
	public JredisPool(ConnectionSpec connectionSpec, GenericObjectPoolConfig poolConfig) {
		this.internalPool = new GenericObjectPool<JRedis>(new JredisFactory(connectionSpec), poolConfig);
	}

	/**
	 * Uses the {@link Config} defaults for configuring the connection pool
	 * 
	 * @param hostName The Redis host
	 * @param port The Redis port
	 * @param dbIndex The index of the database all connections should use. The database will only be selected on initial
	 *          creation of the pooled {@link JRedis} instances. Since calling select directly on {@link JRedis} is not
	 *          supported, it is assumed that connections can be re-used without subsequent selects.
	 * @param password The password used for authenticating with the Redis server or null if no password required
	 * @param timeout The socket timeout or 0 to use the default socket timeout
	 */
	public JredisPool(String hostName, int port, int dbIndex, String password, int timeout) {
		this(hostName, port, dbIndex, password, timeout, new GenericObjectPoolConfig());
	}

	/**
	 * @param hostName The Redis host
	 * @param port The Redis port
	 * @param dbIndex The index of the database all connections should use
	 * @param password The password used for authenticating with the Redis server or null if no password required
	 * @param timeout The socket timeout or 0 to use the default socket timeout
	 * @param poolConfig The pool {@link Config}
	 */
	public JredisPool(String hostName, int port, int dbIndex, String password, int timeout,
			GenericObjectPoolConfig poolConfig) {
		ConnectionSpec connectionSpec = DefaultConnectionSpec.newSpec(hostName, port, dbIndex, null);
		connectionSpec.setConnectionFlag(Connection.Flag.RELIABLE, false);
		if (StringUtils.hasLength(password)) {
			connectionSpec.setCredentials(password);
		}
		if (timeout > 0) {
			connectionSpec.setSocketProperty(Property.SO_TIMEOUT, timeout);
		}
		this.internalPool = new GenericObjectPool<JRedis>(new JredisFactory(connectionSpec), poolConfig);
	}

	public JRedis getResource() {
		try {
			return internalPool.borrowObject();
		} catch (Exception e) {
			throw new PoolException("Could not get a resource from the pool", e);
		}
	}

	public void returnBrokenResource(final JRedis resource) {
		try {
			internalPool.invalidateObject(resource);
		} catch (Exception e) {
			throw new PoolException("Could not invalidate the broken resource", e);
		}
	}

	public void returnResource(final JRedis resource) {
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

	private static class JredisFactory extends BasePooledObjectFactory<JRedis> {

		private final ConnectionSpec connectionSpec;

		public JredisFactory(ConnectionSpec connectionSpec) {
			super();
			this.connectionSpec = connectionSpec;
		}

		@Override
		public void destroyObject(final PooledObject<JRedis> obj) throws Exception {
			try {
				obj.getObject().quit();
			} catch (Exception e) {
				// Errors may happen if returning a broken resource
			}
		}

		@Override
		public boolean validateObject(final PooledObject<JRedis> obj) {
			try {
				obj.getObject().ping();
				return true;
			} catch (Exception e) {
				return false;
			}
		}

		@Override
		public JRedis create() throws Exception {
			return new JRedisClient(connectionSpec);
		}

		@Override
		public PooledObject<JRedis> wrap(JRedis obj) {
			return new DefaultPooledObject<JRedis>(obj);
		}
	}

}
