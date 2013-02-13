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

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;

import com.lambdaworks.redis.RedisClient;

/**
 * Connection factory creating <a href="http://github.com/wg/lettuce">Lettuce</a>-based connections.
 * 
 * @author Costin Leau
 */
public class LettuceConnectionFactory implements InitializingBean, DisposableBean, RedisConnectionFactory {

	private String hostName = "localhost";
	private int port = 6379;
	private RedisClient client;
	private long timeout = TimeUnit.MILLISECONDS.convert(60, TimeUnit.SECONDS);
		
	/**
	 * Constructs a new <code>LettuceConnectionFactory</code> instance
	 * with default settings.
	 */
	public LettuceConnectionFactory() {
	}

	/**
	 * Constructs a new <code>LettuceConnectionFactory</code> instance
	 * with default settings.
	 */
	public LettuceConnectionFactory(String host, int port) {
		this.hostName = host;
		this.port = port;
	}

	public void afterPropertiesSet() {
		client = new RedisClient(hostName, port);
		client.setDefaultTimeout(timeout, TimeUnit.MILLISECONDS);
	}

	public void destroy() {
		client.shutdown();
	}

	public RedisConnection getConnection() {
		return new LettuceConnection(client.connectAsync(LettuceUtils.CODEC), timeout, client);
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
	 * @param host the host to set
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
	 * @param port the port to set
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
	 * @param timeout connection timeout
	 */
	public void setTimeout(long timeout) {
		this.timeout = timeout;
	}
}