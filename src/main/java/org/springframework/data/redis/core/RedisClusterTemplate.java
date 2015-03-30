/*
 * Copyright 2015 the original author or authors.
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
package org.springframework.data.redis.core;

import org.springframework.data.redis.connection.RedisClusterConnection;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.util.Assert;

/**
 * @author Christoph Strobl
 * @since 1.6
 */
public class RedisClusterTemplate<K, V> extends RedisTemplate<K, V> {

	/**
	 * Executed wrapped command upon {@link RedisClusterConnection}.
	 * 
	 * @param callback
	 * @return
	 */
	public <T> T execute(RedisClusterCallback<T> callback) {

		Assert.notNull(callback, "ClusterCallback must not be null!");
		return callback.doInRedis(getConnection());
	}

	/**
	 * Get {@link RedisClusterOperations} operating upon {@link RedisClusterTemplate}.
	 * 
	 * @return
	 */
	public RedisClusterOperations<K, V> getClusterOps() {
		return new DefaultRedisClusterOperations<K, V>(this);
	}

	private RedisClusterConnection getConnection() {

		RedisConnection connection = getConnectionFactory().getConnection();
		Assert.isInstanceOf(RedisClusterConnection.class, connection);

		return (RedisClusterConnection) connection;
	}

}
