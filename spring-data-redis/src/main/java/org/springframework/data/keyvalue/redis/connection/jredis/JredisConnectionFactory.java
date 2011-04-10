/*
 * Copyright 2010-2011 the original author or authors.
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
package org.springframework.data.keyvalue.redis.connection.jredis;

import org.jredis.ClientRuntimeException;
import org.jredis.connector.Connection;
import org.jredis.connector.ConnectionSpec;
import org.jredis.connector.Connection.Socket.Property;
import org.jredis.ri.alphazero.JRedisClient;
import org.jredis.ri.alphazero.JRedisService;
import org.jredis.ri.alphazero.connection.DefaultConnectionSpec;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.dao.DataAccessException;
import org.springframework.data.keyvalue.redis.connection.RedisConnection;
import org.springframework.data.keyvalue.redis.connection.RedisConnectionFactory;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Connection factory using creating <a href="http://github.com/alphazero/jredis">JRedis</a> based connections. 
 * 
 * @author Costin Leau
 */
public class JredisConnectionFactory implements InitializingBean, DisposableBean, RedisConnectionFactory {

	private ConnectionSpec connectionSpec;

	private String hostName = "localhost";
	private int port = DEFAULT_REDIS_PORT;
	private String password = null;
	private int timeout;

	private boolean usePool = true;
	private int dbIndex = DEFAULT_REDIS_DB;

	private JRedisService pool = null;
	// taken from JRedis code
	private int poolSize = 5;

	private static final int DEFAULT_REDIS_PORT = 6379;
	private static final int DEFAULT_REDIS_DB = 0;
	private static final byte[] DEFAULT_REDIS_PASSWORD = null;


	/**
	 * Constructs a new <code>JredisConnectionFactory</code> instance.
	 */
	public JredisConnectionFactory() {
	}

	/**
	 * Constructs a new <code>JredisConnectionFactory</code> instance.
	 * Will override the other connection parameters passed to the factory. 
	 *
	 * @param connectionSpec already configured connection.
	 */
	public JredisConnectionFactory(ConnectionSpec connectionSpec) {
		this.connectionSpec = connectionSpec;
	}

	@Override
	public void afterPropertiesSet() {
		if (connectionSpec == null) {
			Assert.hasText(hostName);
			connectionSpec = DefaultConnectionSpec.newSpec(hostName, port, dbIndex, DEFAULT_REDIS_PASSWORD);
			connectionSpec.setConnectionFlag(Connection.Flag.RELIABLE, false);

			if (StringUtils.hasLength(password)) {
				connectionSpec.setCredentials(password);
			}

			if (timeout > 0) {
				connectionSpec.setSocketProperty(Property.SO_TIMEOUT, timeout);
			}
		}

		if (usePool) {
			int size = getPoolSize();
			pool = new JRedisService(connectionSpec, size);
		}
	}


	@Override
	public void destroy() {
		if (usePool && pool != null) {
			pool.quit();
			pool = null;
		}
	}


	@Override
	public RedisConnection getConnection() {
		return postProcessConnection(new JredisConnection((usePool ? pool : new JRedisClient(connectionSpec))));
	}


	/**
	 * Post process a newly retrieved connection. Useful for decorating or executing
	 * initialization commands on a new connection.
	 * This implementation simply returns the connection.
	 * 
	 * @param connection
	 * @return processed connection
	 */
	protected RedisConnection postProcessConnection(JredisConnection connection) {
		return connection;
	}

	@Override
	public DataAccessException translateExceptionIfPossible(RuntimeException ex) {
		if (ex instanceof ClientRuntimeException) {
			return JredisUtils.convertJredisAccessException((ClientRuntimeException) ex);
		}
		return null;
	}


	/**
	 * Returns the Redis host name of this factory.
	 *
	 * @return Returns the hostName
	 */
	public String getHostName() {
		return hostName;
	}

	/**
	 * Sets the Redis host name for this factory.
	 * 
	 * @param hostName The hostName to set.
	 */
	public void setHostName(String hostName) {
		this.hostName = hostName;
	}


	/**
	 * Returns the Redis port.
	 *
	 * @return Returns the port
	 */
	public int getPort() {
		return port;
	}

	/**
	 * Sets the Redis port.
	 * 
	 * @param port The port to set.
	 */
	public void setPort(int port) {
		this.port = port;
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
	 * Indicates the use of a connection pool.
	 *
	 * @return Returns the use of connection pooling.
	 */
	public boolean getUsePool() {
		return usePool;
	}

	/**
	 * Turns on or off the use of connection pooling.
	 * 
	 * @param usePool The usePool to set.
	 */
	public void setUsePool(boolean usePool) {
		this.usePool = usePool;
	}

	/**
	 * Returns the pool size of this factory.
	 *
	 * @return Returns the poolSize
	 */
	public int getPoolSize() {
		return poolSize;
	}

	/**
	 * Sets the connection pool size of the underlying factory.
	 * 
	 * @param poolSize The poolSize to set.
	 */
	public void setPoolSize(int poolSize) {
		Assert.isTrue(poolSize > 0, "pool size needs to be bigger then zero");
		this.poolSize = poolSize;
		usePool = true;
	}

	/**
	 * Sets the index of the database used by this connection factory.
	 * Can be between 0 (default) and 15.
	 * 
	 * @param index database index
	 */
	public void setDatabase(int index) {
		Assert.isTrue(index >= 0, "invalid DB index (a positive index required)");
		this.dbIndex = index;
	}
}