/*
 * Copyright 2014 the original author or authors.
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
package org.springframework.data.redis.connection;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.dao.DataAccessException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.dao.InvalidDataAccessResourceUsageException;
import org.springframework.data.redis.RedisSystemException;

/**
 * @author Christoph Strobl
 * @since 1.4
 */
public abstract class AbstractRedisConnection implements RedisConnection {

	private RedisSentinelConfiguration sentinelConfiguration;
	private ConcurrentHashMap<RedisNode, RedisSentinelConnection> connectionCache = new ConcurrentHashMap<RedisNode, RedisSentinelConnection>();

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConnection#getSentinelCommands()
	 */
	@Override
	public RedisSentinelConnection getSentinelConnection() {

		if (!hasRedisSentinelConfigured()) {
			throw new InvalidDataAccessResourceUsageException("No sentinels configured.");
		}

		RedisNode node = selectActiveSentinel();
		RedisSentinelConnection connection = connectionCache.get(node);
		if (connection == null || !connection.isOpen()) {
			connection = getSentinelConnection(node);
			connectionCache.putIfAbsent(node, connection);
		}
		return connection;
	}

	public void setSentinelConfiguration(RedisSentinelConfiguration sentinelConfiguration) {
		this.sentinelConfiguration = sentinelConfiguration;
	}

	public boolean hasRedisSentinelConfigured() {
		return this.sentinelConfiguration != null;
	}

	private RedisNode selectActiveSentinel() {

		for (RedisNode node : this.sentinelConfiguration.getSentinels()) {
			if (isActive(node)) {
				return node;
			}
		}

		throw new InvalidDataAccessApiUsageException("Could not find any active sentinels");
	}

	/**
	 * Check if node is active by sending ping.
	 * 
	 * @param node
	 * @return
	 */
	protected boolean isActive(RedisNode node) {
		return false;
	}

	/**
	 * Get {@link RedisSentinelCommands} connected to given node.
	 * 
	 * @param sentinel
	 * @return
	 */
	protected RedisSentinelConnection getSentinelConnection(RedisNode sentinel) {
		throw new UnsupportedOperationException("Sentinel is not supported by this client.");
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisConnection#close()
	 */
	@Override
	public void close() throws DataAccessException {

		if (!connectionCache.isEmpty()) {
			for (RedisNode node : connectionCache.keySet()) {
				RedisSentinelConnection connection = connectionCache.remove(node);
				if (connection.isOpen()) {
					try {
						connection.close();
					} catch (IOException e) {
						throw new RedisSystemException("Failed to close sentinel connection", e);
					}
				}
			}
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public Object zRange(byte[] key, TargetMethod method) {
		try {
			method.getValueList().add(0, key);
			Method targetMethod = this.getClass().getMethod(method.getMethodName(), method.getTypeList());
			return targetMethod.invoke(this, method.getValueList().toArray(new Object[0]));
		}
		catch (NoSuchMethodException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		catch (SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		catch (InvocationTargetException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return new HashSet<Object>();

	}

}
