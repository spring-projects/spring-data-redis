/*
 * Copyright 2011-2013 the original author or authors.
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

import org.apache.commons.pool.BasePoolableObjectFactory;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.commons.pool.impl.GenericObjectPool.Config;
import org.jredis.ClientRuntimeException;
import org.jredis.JRedis;
import org.jredis.connector.Connection;
import org.jredis.connector.ConnectionSpec;
import org.jredis.connector.Connection.Socket.Property;
import org.jredis.ri.alphazero.JRedisClient;
import org.jredis.ri.alphazero.connection.DefaultConnectionSpec;
import org.springframework.util.StringUtils;

/**
 * Default implementation of {@link JredisPool}
 * 
 * @author Jennifer Hickey
 * 
 */
public class DefaultJredisPool implements JredisPool {

	private final GenericObjectPool internalPool;

	/**
	 * Uses the {@link Config} and {@link ConnectionSpec} defaults for
	 * configuring the connection pool
	 * 
	 * @param hostName
	 *            The Redis host
	 * @param port
	 *            The Redis port
	 */
	public DefaultJredisPool(String hostName, int port) {
		this(hostName, port, 0, null, 0, new Config());
	}

	/**
	 * Uses the {@link ConnectionSpec} defaults for configuring the connection
	 * pool
	 * 
	 * @param hostName
	 *            The Redis host
	 * @param port
	 *            The Redis port
	 * @param poolConfig
	 *            The pool {@link Config}
	 */
	public DefaultJredisPool(String hostName, int port, Config poolConfig) {
		this(hostName, port, 0, null, 0, poolConfig);
	}

	/**
	 * 
	 * Uses the {@link Config} defaults for configuring the connection pool
	 * 
	 * @param connectionSpec
	 *            The {@link ConnectionSpec} for connecting to Redis
	 * 
	 */
	public DefaultJredisPool(ConnectionSpec connectionSpec) {
		this.internalPool = new GenericObjectPool(new JredisFactory(connectionSpec), new Config());
	}

	/**
	 * 
	 * @param connectionSpec
	 *            The {@link ConnectionSpec} for connecting to Redis
	 * 
	 * @param poolConfig
	 *            The pool {@link Config}
	 */
	public DefaultJredisPool(ConnectionSpec connectionSpec, Config poolConfig) {
		this.internalPool = new GenericObjectPool(new JredisFactory(connectionSpec), poolConfig);
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
	 *            database will only be selected on initial creation of the
	 *            pooled {@link JRedis} instances. Since calling select directly
	 *            on {@link JRedis} is not supported, it is assumed that
	 *            connections can be re-used without subsequent selects.
	 * @param password
	 *            The password used for authenticating with the Redis server or
	 *            null if no password required
	 * @param timeout
	 *            The socket timeout or 0 to use the default socket timeout
	 */
	public DefaultJredisPool(String hostName, int port, int dbIndex, String password, int timeout) {
		this(hostName, port, dbIndex, password, timeout, new Config());
	}

	/**
	 * 
	 * @param hostName
	 *            The Redis host
	 * @param port
	 *            The Redis port
	 * @param dbIndex
	 *            The index of the database all connections should use
	 * @param password
	 *            The password used for authenticating with the Redis server or
	 *            null if no password required
	 * @param timeout
	 *            The socket timeout or 0 to use the default socket timeout
	 * @param poolConfig
	 *            The pool {@link COnfig}
	 */
	public DefaultJredisPool(String hostName, int port, int dbIndex, String password, int timeout,
			Config poolConfig) {
		ConnectionSpec connectionSpec = DefaultConnectionSpec.newSpec(hostName, port, dbIndex, null);
		connectionSpec.setConnectionFlag(Connection.Flag.RELIABLE, false);
		if (StringUtils.hasLength(password)) {
			connectionSpec.setCredentials(password);
		}
		if (timeout > 0) {
			connectionSpec.setSocketProperty(Property.SO_TIMEOUT, timeout);
		}
		this.internalPool = new GenericObjectPool(new JredisFactory(connectionSpec), poolConfig);
	}

	public JRedis getResource() {
		try {
			return (JRedis) internalPool.borrowObject();
		} catch (Exception e) {
			throw new ClientRuntimeException("Could not get a resource from the pool", e);
		}
	}

	public void returnBrokenResource(final JRedis resource) {
		try {
			internalPool.invalidateObject(resource);
		} catch (Exception e) {
			throw new ClientRuntimeException("Could not invalidate the broken resource", e);
		}
	}

	public void returnResource(final JRedis resource) {
		try {
			internalPool.returnObject(resource);
		} catch (Exception e) {
			throw new ClientRuntimeException("Could not return the resource to the pool", e);
		}
	}

	public void destroy() {
		try {
			internalPool.close();
		} catch (Exception e) {
			throw new ClientRuntimeException("Could not destroy the pool", e);
		}
	}

	private static class JredisFactory extends BasePoolableObjectFactory {

		private final ConnectionSpec connectionSpec;

		public JredisFactory(ConnectionSpec connectionSpec) {
			super();
			this.connectionSpec = connectionSpec;
		}

		public Object makeObject() throws Exception {
			return new JRedisClient(connectionSpec);
		}

		public void destroyObject(final Object obj) throws Exception {
			if (obj instanceof JRedis) {
				try {
					((JRedis) obj).quit();
				}catch(Exception e) {
					// Errors may happen if returning a broken resource
				}
			}
		}

		public boolean validateObject(final Object obj) {
			if (obj instanceof JRedis) {
				try {
					((JRedis) obj).ping();
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
