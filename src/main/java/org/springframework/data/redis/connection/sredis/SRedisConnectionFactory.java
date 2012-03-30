/*
 * Copyright 2011-2012 the original author or authors.
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

package org.springframework.data.redis.connection.sredis;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.Socket;
import java.util.Queue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.util.ReflectionUtils;

import redis.client.SocketPool;

/**
 * Connection factory creating <a href="http://github.com/spullara/redis-protocol">Redis Protocol</a> based connections.
 * 
 * @author Costin Leau
 */
public class SRedisConnectionFactory implements InitializingBean, DisposableBean, RedisConnectionFactory {

	private final static Log log = LogFactory.getLog(JedisConnectionFactory.class);

	private String hostName = "localhost";
	private int port = 6379;
	private SocketPool pool;

	/**
	 * Constructs a new <code>SRedisConnectionFactory</code> instance
	 * with default settings.
	 */
	public SRedisConnectionFactory() {
	}

	/**
	 * Constructs a new <code>SRedisConnectionFactory</code> instance
	 * with default settings.
	 */
	public SRedisConnectionFactory(String host, int port) {
		this.hostName = host;
		this.port = port;
	}

	public void afterPropertiesSet() {
		pool = new SocketPool(hostName, port);
	}

	public void destroy() {
		Field f = ReflectionUtils.findField(SocketPool.class, "queue");
		ReflectionUtils.makeAccessible(f);
		Queue<Socket> queue = (Queue<Socket>) ReflectionUtils.getField(f, pool);
		Socket s = null;
		do {
			s = queue.poll();
			if (s != null) {
				try {
					s.close();
				} catch (IOException ex) {
					// ignore
				}
			}
		} while (s != null);
		pool = null;
	}

	public RedisConnection getConnection() {
		return new SRedisConnection(pool);
	}

	public DataAccessException translateExceptionIfPossible(RuntimeException ex) {
		return SRedisUtils.convertSRedisAccessException(ex);
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
}