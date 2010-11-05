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
package org.springframework.datastore.redis.connection.jredis;

import org.jredis.JRedis;
import org.jredis.connector.ConnectionSpec;
import org.jredis.connector.Connection.Socket.Property;
import org.jredis.ri.alphazero.JRedisClient;
import org.jredis.ri.alphazero.JRedisService;
import org.jredis.ri.alphazero.connection.DefaultConnectionSpec;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.dao.DataAccessException;
import org.springframework.datastore.redis.connection.RedisConnection;
import org.springframework.datastore.redis.connection.RedisConnectionFactory;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Connection factory on top of {@link JRedis} connection. 
 * 
 * @author Costin Leau
 */
public class JredisConnectionFactory implements InitializingBean, DisposableBean, RedisConnectionFactory {

	private String encoding = "UTF-8";
	private ConnectionSpec connectionSpec;

	private String password;
	private int timeout;

	private boolean usePool = true;

	private JRedisService pool = null;
	// taken from JRedis code
	private int poolSize = 5;


	/**
	 * Constructs a new <code>JredisConnectionFactory</code> instance.
	 */
	public JredisConnectionFactory() {
		this(DefaultConnectionSpec.newSpec());
	}


	/**
	 * Constructs a new <code>JredisConnectionFactory</code> instance.
	 *
	 * @param hostName
	 */
	public JredisConnectionFactory(String hostName) {
		Assert.hasText(hostName);
		throw new UnsupportedOperationException();
	}


	/**
	 * Constructs a new <code>JredisConnectionFactory</code> instance.
	 *
	 * @param hostName
	 * @param port
	 */
	public JredisConnectionFactory(String hostName, int port) {
		Assert.hasText(hostName);
		throw new UnsupportedOperationException();
	}

	/**
	 * Constructs a new <code>JredisConnectionFactory</code> instance.
	 *
	 * @param connectionSpec
	 */
	public JredisConnectionFactory(ConnectionSpec connectionSpec) {
		this.connectionSpec = connectionSpec;
	}


	@Override
	public void afterPropertiesSet() {
		if (StringUtils.hasLength(password)) {
			connectionSpec.setCredentials(password);
		}

		if (timeout > 0) {
			connectionSpec.setSocketProperty(Property.SO_TIMEOUT, timeout);
		}

		if (usePool) {
			int size = getPoolSize();
			pool = new JRedisService(connectionSpec, size);
		}
	}


	@Override
	public void destroy() {
		if (usePool && pool != null) {
			//pool.quit();
			pool = null;
		}
	}


	@Override
	public RedisConnection getConnection() {
		return new JredisConnection((usePool ? pool : new JRedisClient(connectionSpec)), getEncoding());
	}


	@Override
	public DataAccessException translateExceptionIfPossible(RuntimeException ex) {
		return null;
	}

	/**
	 * Returns the encoding.
	 *
	 * @return Returns the encoding
	 */
	public String getEncoding() {
		return encoding;
	}

	/**
	 * @param encoding The encoding to set.
	 */
	public void setEncoding(String encoding) {
		this.encoding = encoding;
	}

	/**
	 * @return the password
	 */
	public String getPassword() {
		return password;
	}

	/**
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
	public boolean isPooling() {
		return usePool;
	}

	/**
	 * Turns on or off the use of connection pooling.
	 * 
	 * @param usePool The usePool to set.
	 */
	public void setPooling(boolean usePool) {
		this.usePool = usePool;
	}

	/**
	 * Returns the poolSize.
	 *
	 * @return Returns the poolSize
	 */
	public int getPoolSize() {
		return poolSize;
	}

	/**
	 * @param poolSize The poolSize to set.
	 */
	public void setPoolSize(int poolSize) {
		Assert.isTrue(poolSize > 0, "pool size needs to be bigger then zero");
		this.poolSize = poolSize;
		usePool = true;
	}
}