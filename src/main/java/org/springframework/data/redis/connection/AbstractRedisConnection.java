/*
 * Copyright 2014-2018 the original author or authors.
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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.dao.DataAccessException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.dao.InvalidDataAccessResourceUsageException;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 1.4
 */
public abstract class AbstractRedisConnection implements DefaultedRedisConnection {

	private @Nullable RedisSentinelConfiguration sentinelConfiguration;
	private final Map<RedisNode, RedisSentinelConnection> connectionCache = new ConcurrentHashMap<>();

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

		Assert.state(hasRedisSentinelConfigured(), "Sentinel configuration missing!");

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

}
