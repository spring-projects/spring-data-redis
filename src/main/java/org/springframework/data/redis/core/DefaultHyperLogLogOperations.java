/*
 * Copyright 2014-2016 the original author or authors.
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

import java.util.Arrays;

import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 1.5
 * @param <K>
 * @param <V>
 */
public class DefaultHyperLogLogOperations<K, V> extends AbstractOperations<K, V>
		implements HyperLogLogOperations<K, V> {

	/**
	 * Constructs a new {@link DefaultHyperLogLogOperations} instance.
	 * 
	 * @param template must not be {@literal null}.
	 */
	public DefaultHyperLogLogOperations(RedisTemplate<K, V> template) {
		super(template);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.HyperLogLogOperations#add(java.lang.Object, java.lang.Object[])
	 */
	@Override
	public Long add(K key, V... values) {

		final byte[] rawKey = rawKey(key);
		final byte[][] rawValues = rawValues(values);

		return execute(new RedisCallback<Long>() {

			@Override
			public Long doInRedis(RedisConnection connection) throws DataAccessException {
				return connection.pfAdd(rawKey, rawValues);
			}
		}, true);

	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.HyperLogLogOperations#size(java.lang.Object[])
	 */
	@Override
	public Long size(K... keys) {

		final byte[][] rawKeys = rawKeys(Arrays.asList(keys));

		return execute(new RedisCallback<Long>() {

			@Override
			public Long doInRedis(RedisConnection connection) throws DataAccessException {
				return connection.pfCount(rawKeys);
			}
		}, true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.HyperLogLogOperations#union(java.lang.Object, java.lang.Object[])
	 */
	@Override
	public Long union(K destination, K... sourceKeys) {

		final byte[] rawDestinationKey = rawKey(destination);
		final byte[][] rawSourceKeys = rawKeys(Arrays.asList(sourceKeys));

		return execute(new RedisCallback<Long>() {

			@Override
			public Long doInRedis(RedisConnection connection) throws DataAccessException {

				connection.pfMerge(rawDestinationKey, rawSourceKeys);
				return connection.pfCount(rawDestinationKey);
			}
		}, true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.HyperLogLogOperations#delete(java.lang.Object)
	 */
	@Override
	public void delete(K key) {
		template.delete(key);
	}
}
