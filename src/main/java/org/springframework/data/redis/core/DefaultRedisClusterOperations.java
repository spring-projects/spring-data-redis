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

import java.util.Set;

import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisClusterConnection;
import org.springframework.data.redis.connection.RedisNode;

/**
 * @author Christoph Strobl
 * @since 1.6
 * @param <K>
 * @param <V>
 */
public class DefaultRedisClusterOperations<K, V> extends AbstractOperations<K, V> implements
		RedisClusterOperations<K, V> {

	private final RedisClusterTemplate<K, V> template;

	public DefaultRedisClusterOperations(RedisClusterTemplate<K, V> template) {
		super(template);
		this.template = template;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.RedisClusterOperations#keys(org.springframework.data.redis.connection.RedisNode, byte[])
	 */
	@Override
	public Set<K> keys(final RedisNode node, final byte[] pattern) {

		return template.execute(new RedisClusterCallback<Set<K>>() {

			@Override
			public Set<K> doInRedis(RedisClusterConnection connection) throws DataAccessException {
				return deserializeKeys(connection.keys(node, pattern));
			}
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.RedisClusterOperations#randomKey(org.springframework.data.redis.connection.RedisNode)
	 */
	@Override
	public K randomKey(final RedisNode node) {
		return template.execute(new RedisClusterCallback<K>() {

			@Override
			public K doInRedis(RedisClusterConnection connection) throws DataAccessException {
				return deserializeKey(connection.randomKey(node));
			}
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.RedisClusterOperations#ping(org.springframework.data.redis.connection.RedisNode)
	 */
	@Override
	public String ping(final RedisNode node) {
		return template.execute(new RedisClusterCallback<String>() {

			@Override
			public String doInRedis(RedisClusterConnection connection) throws DataAccessException {
				return connection.ping(node);
			}
		});
	}

}
