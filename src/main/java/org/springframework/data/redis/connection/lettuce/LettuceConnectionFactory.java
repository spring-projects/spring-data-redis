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

package org.springframework.data.redis.connection.lettuce;

import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.connection.Pool;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.util.Assert;

import com.lambdaworks.redis.RedisAsyncConnection;
import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisException;

/**
 * Connection factory creating <a
 * href="http://github.com/wg/lettuce">Lettuce</a>-based connections.
 * <p>
 * This factory creates a new {@link LettuceConnection} on each call to
 * {@link #getConnection()}. Multiple {@link LettuceConnection}s share a single
 * thread-safe native connection by default.
 *
 * <p>
 * The shared native connection is never closed by {@link LettuceConnection},
 * therefore it is not validated by default on {@link #getConnection()}. Use
 * {@link #setValidateConnection(boolean)} to change this behavior if necessary.
 *
 * Inject a {@link Pool} to pool dedicated connections. If shareNativeConnection is
 * true, the pool will be used to select a connection for blocking and tx operations only,
 * which should not share a connection. If native connection sharing is disabled,
 * the selected connection will be used for all operations.
 *
 * @author Costin Leau
 * @author Jennifer Hickey
 */
public class LettuceConnectionFactory implements InitializingBean, DisposableBean,
		RedisConnectionFactory {

	private final Log log = LogFactory.getLog(getClass());

	private String hostName = "localhost";
	private int port = 6379;
	private RedisClient client;
	private long timeout = TimeUnit.MILLISECONDS.convert(60, TimeUnit.SECONDS);
	private boolean validateConnection = false;
	private boolean shareNativeConnection = true;
	private RedisAsyncConnection<byte[], byte[]> connection;
	private Pool<RedisAsyncConnection<byte[], byte[]>> pool;
	private int dbIndex = 0;
	/** Synchronization monitor for the shared Connection */
	private final Object connectionMonitor = new Object();

	/**
	 * Constructs a new <code>LettuceConnectionFactory</code> instance with
	 * default settings.
	 */
	public LettuceConnectionFactory() {
	}

	/**
	 * Constructs a new <code>LettuceConnectionFactory</code> instance with
	 * default settings.
	 */
	public LettuceConnectionFactory(String host, int port) {
		this.hostName = host;
		this.port = port;
	}

	public LettuceConnectionFactory(String host, int port, Pool<RedisAsyncConnection<byte[], byte[]>> pool) {
		this.hostName = host;
		this.port = port;
		this.pool = pool;
	}

	public void afterPropertiesSet() {
		client = new RedisClient(hostName, port);
		client.setDefaultTimeout(timeout, TimeUnit.MILLISECONDS);
		resetConnection();
		if (shareNativeConnection) {
			initConnection();
		}
	}

	public void destroy() {
		resetConnection();
		client.shutdown();
	}

	public RedisConnection getConnection() {
		return new LettuceConnection(getSharedConnection(), createLettuceConnector(false), timeout, client, pool);
	}

	public void initConnection() {
		synchronized (this.connectionMonitor) {
			if (this.connection != null) {
				resetConnection();
			}
			this.connection = createLettuceConnector(true);
		}
	}

	/**
	 * Reset the underlying shared Connection, to be reinitialized on next
	 * access.
	 */
	public void resetConnection() {
		synchronized (this.connectionMonitor) {
			if(this.connection != null) {
				this.connection.close();
			}
			this.connection = null;
		}
	}

	/**
	 * Validate the shared Connection and reinitialize if invalid
	 */
	public void validateConnection() {
		synchronized (this.connectionMonitor) {
			try {
				new com.lambdaworks.redis.RedisConnection<byte[], byte[]>(connection).ping();
			} catch (RedisException e) {
				log.warn("Validation of shared connection failed. Creating a new connection.");
				initConnection();
			}
		}
	}

	public DataAccessException translateExceptionIfPossible(RuntimeException ex) {
		return LettuceUtils.convertRedisAccessException(ex);
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

	/**
	 * Indicates if validation of the native Lettuce connection is enabled
	 *
	 * @return connection validation enabled
	 */
	public boolean getValidateConnection() {
		return validateConnection;
	}

	/**
	 * Enables validation of the shared native Lettuce connection on calls to
	 * {@link #getConnection()}. A new connection will be created and used if
	 * validation fails.
	 * <p>
	 * Lettuce will automatically reconnect until close is called, which should
	 * never happen through {@link LettuceConnection} if a shared native
	 * connection is used, therefore the default is false.
	 * <p>
	 * Setting this to true will result in a round-trip call to the server on
	 * each new connection, so this setting should only be used if connection
	 * sharing is enabled and there is code that is actively closing the native
	 * Lettuce connection.
	 *
	 * @param validateConnection
	 *            enable connection validation
	 */
	public void setValidateConnection(boolean validateConnection) {
		this.validateConnection = validateConnection;
	}

	/**
	 * Indicates if multiple {@link LettuceConnection}s should share a single
	 * native connection.
	 *
	 * @return native connection shared
	 */
	public boolean getShareNativeConnection() {
		return shareNativeConnection;
	}

	/**
	 * Enables multiple {@link LettuceConnection}s to share a single native
	 * connection. If set to false, every operation on {@link LettuceConnection}
	 * will open and close a socket.
	 *
	 * @param shareNativeConnection
	 *            enable connection sharing
	 */
	public void setShareNativeConnection(boolean shareNativeConnection) {
		this.shareNativeConnection = shareNativeConnection;
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
	 * Sets the index of the database used by this connection factory. Default
	 * is 0.
	 *
	 * @param index
	 *            database index
	 */
	public void setDatabase(int index) {
		Assert.isTrue(index >= 0, "invalid DB index (a positive index required)");
		this.dbIndex = index;
	}

	protected RedisAsyncConnection<byte[], byte[]> getSharedConnection() {
		if (shareNativeConnection) {
			synchronized (this.connectionMonitor) {
				if (this.connection == null) {
					initConnection();
				}
				if (validateConnection) {
					validateConnection();
				}
				return this.connection;
			}
		} else {
			return null;
		}
	}

	protected RedisAsyncConnection<byte[], byte[]> createLettuceConnector(boolean shared) {
		if(pool != null && !shared) {
			return pool.getResource();
		}
		try {
			RedisAsyncConnection<byte[], byte[]> dedicatedConnection = client.connectAsync(LettuceUtils.CODEC);
			if(dbIndex > 0) {
				dedicatedConnection.select(dbIndex);
			}
			return dedicatedConnection;
		} catch (RedisException e) {
			throw new RedisConnectionFailureException("Unable to connect to Redis on " +
					getHostName() + ":" + getPort(), e);
		}
	}
}