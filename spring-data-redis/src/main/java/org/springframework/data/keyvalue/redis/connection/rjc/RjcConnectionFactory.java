/*
 * Copyright 2011 the original author or authors.
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
package org.springframework.data.keyvalue.redis.connection.rjc;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.idevlab.rjc.ds.DataSource;
import org.idevlab.rjc.ds.PoolableDataSource;
import org.idevlab.rjc.ds.SimpleDataSource;
import org.idevlab.rjc.protocol.Protocol;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.dao.DataAccessException;
import org.springframework.data.keyvalue.redis.connection.RedisConnection;
import org.springframework.data.keyvalue.redis.connection.RedisConnectionFactory;
import org.springframework.data.keyvalue.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.util.Assert;

/**
 * Connection factory creating <a href="http://github.com/e-mzungu/rjc/">rjc</a> based connections.
 * 
 * @author Costin Leau
 */
public class RjcConnectionFactory implements InitializingBean, DisposableBean, RedisConnectionFactory {

	private final static Log log = LogFactory.getLog(JedisConnectionFactory.class);

	private String hostName = "localhost";
	private int port = Protocol.DEFAULT_PORT;
	private int timeout = Protocol.DEFAULT_TIMEOUT;
	private String password;

	private boolean usePool = true;
	private int dbIndex = 0;
	private DataSource dataSource;


	/**
	 * Constructs a new <code>RjcConnectionFactory</code> instance
	 * with default settings (default connection pooling, no shard information).
	 */
	public RjcConnectionFactory() {
	}


	public void afterPropertiesSet() {
		if (usePool) {
			PoolableDataSource pool = new PoolableDataSource();
			pool.setHost(hostName);
			pool.setPort(port);
			pool.setPassword(password);
			pool.setTimeout(timeout);

			pool.init();

			dataSource = pool;

		}
		else {
			dataSource = new SimpleDataSource(hostName, port, timeout, password);
		}
	}

	public void destroy() {
		if (usePool && dataSource != null) {
			try {
				((PoolableDataSource) dataSource).close();
			} catch (Exception ex) {
				log.warn("Cannot properly close Rjc pool", ex);
			}
			dataSource = null;
		}
	}

	@Override
	public RedisConnection getConnection() {
		return postProcessConnection(new RjcConnection(dataSource.getConnection(), dbIndex));
	}

	/**
	 * Post process a newly retrieved connection. Useful for decorating or executing
	 * initialization commands on a new connection.
	 * This implementation simply returns the connection.
	 * 
	 * @param connection
	 * @return processed connection
	 */
	protected RjcConnection postProcessConnection(RjcConnection connection) {
		return connection;
	}

	@Override
	public DataAccessException translateExceptionIfPossible(RuntimeException ex) {
		return RjcUtils.convertRjcAccessException(ex);
	}


	/**
	 * Returns the Redis hostName.
	 *
	 * @return Returns the hostName
	 */
	public String getHostName() {
		return hostName;
	}

	/**
	 * Sets the Redis hostName.
	 * 
	 * @param hostName The hostName to set.
	 */
	public void setHostName(String hostName) {
		this.hostName = hostName;
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
	 * Returns the port used to connect to the Redis instance.
	 * 
	 * @return Redis port.
	 */
	public int getPort() {
		return port;

	}

	/**
	 * Sets the port used to connect to the Redis instance.
	 * 
	 * @param port Redis port
	 */
	public void setPort(int port) {
		this.port = port;
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
	 * Returns the index of the database.
	 *
	 * @return Returns the database index
	 */
	public int getDatabase() {
		return dbIndex;
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