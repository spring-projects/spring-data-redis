/*
 * Copyright 2010 the original author or authors.
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

package org.springframework.data.keyvalue.redis.connection.jedis;

import java.util.concurrent.TimeoutException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.dao.DataAccessException;
import org.springframework.data.keyvalue.redis.connection.RedisConnection;
import org.springframework.data.keyvalue.redis.connection.RedisConnectionFactory;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisShardInfo;

/**
 * Connection factory using Jedis underneath. 
 * 
 * @author Costin Leau
 */
public class JedisConnectionFactory implements InitializingBean, DisposableBean, RedisConnectionFactory {

	private final static Log log = LogFactory.getLog(JedisConnectionFactory.class);

	private JedisShardInfo shardInfo;
	private String password;
	private int timeout;

	private boolean usePool = true;

	private JedisPool pool = null;
	// taken from Jedis code
	private int poolSize = 10;

	/**
	 * Constructs a new <code>JedisConnectionFactory</code> instance.
	 */
	public JedisConnectionFactory() {
		this(getDefaultHostName());
	}

	/**
	 * Constructs a new <code>JedisConnectionFactory</code> instance.
	 *
	 * @param hostname
	 */
	public JedisConnectionFactory(String hostName) {
		Assert.hasText(hostName);
		shardInfo = new JedisShardInfo(hostName);
	}

	/**
	 * Constructs a new <code>JedisConnectionFactory</code> instance.
	 *
	 * @param hostname
	 * @param port
	 */
	public JedisConnectionFactory(String hostName, int port) {
		shardInfo = new JedisShardInfo(hostName, port);
	}

	/**
	 * Constructs a new <code>JedisConnectionFactory</code> instance.
	 *
	 * @param shardInfo
	 */
	public JedisConnectionFactory(JedisShardInfo shardInfo) {
		this.shardInfo = shardInfo;
	}

	/**
	 * Returns a Jedis instance to be used as a Redis connection.
	 * The instance can be newly created or retrieved from a pool.
	 * 
	 * @return Jedis instance ready for wrapping into a {@link RedisConnection}.
	 */
	protected Jedis fetchJedisConnector() {
		try {
			if (usePool) {
				return pool.getResource();
			}
			return new Jedis(getShardInfo());
		} catch (TimeoutException ex) {
			throw JedisUtils.convertJedisAccessException(ex);
		}
	}

	public void afterPropertiesSet() {
		if (StringUtils.hasLength(password)) {
			shardInfo.setPassword(password);
		}

		if (timeout > 0) {
			shardInfo.setTimeout(timeout);
		}

		if (usePool) {
			int size = getPoolSize();
			pool = new JedisPool(shardInfo);
			pool.setResourcesNumber(size);
			pool.init();
		}
	}

	public void destroy() {
		if (usePool && pool != null) {
			pool.destroy();
			pool = null;
		}
	}

	public JedisConnection getConnection() {
		return new JedisConnection(fetchJedisConnector());
	}

	@Override
	public DataAccessException translateExceptionIfPossible(RuntimeException ex) {
		return JedisUtils.convertJedisAccessException(ex);
	}

	private static String getDefaultHostName() {
		return "localhost";
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
	 * Returns the shardInfo.
	 *
	 * @return Returns the shardInfo
	 */
	public JedisShardInfo getShardInfo() {
		return shardInfo;
	}

	/**
	 * @param shardInfo The shardInfo to set.
	 */
	public void setShardInfo(JedisShardInfo shardInfo) {
		this.shardInfo = shardInfo;
	}

	/**
	 * Returns the timeout.
	 *
	 * @return Returns the timeout
	 */
	public int getTimeout() {
		return timeout;
	}

	/**
	 * @param timeout The timeout to set.
	 */
	public void setTimeout(int timeout) {
		this.timeout = timeout;
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