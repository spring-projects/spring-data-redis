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
package org.springframework.data.redis.core;

import java.util.Arrays;

/**
 * @author Christoph Strobl
 * @since 1.5
 * @param <K>
 * @param <V>
 */
class DefaultHyperLogLogOperations<K, V> extends AbstractOperations<K, V> implements HyperLogLogOperations<K, V> {

	DefaultHyperLogLogOperations(RedisTemplate<K, V> template) {
		super(template);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.HyperLogLogOperations#add(java.lang.Object, java.lang.Object[])
	 */
	@Override
	public Long add(K key, V... values) {

		byte[] rawKey = rawKey(key);
		byte[][] rawValues = rawValues(values);
		return execute(connection -> connection.pfAdd(rawKey, rawValues), true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.HyperLogLogOperations#size(java.lang.Object[])
	 */
	@Override
	public Long size(K... keys) {

		byte[][] rawKeys = rawKeys(Arrays.asList(keys));
		return execute(connection -> connection.pfCount(rawKeys), true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.HyperLogLogOperations#union(java.lang.Object, java.lang.Object[])
	 */
	@Override
	public Long union(K destination, K... sourceKeys) {

		byte[] rawDestinationKey = rawKey(destination);
		byte[][] rawSourceKeys = rawKeys(Arrays.asList(sourceKeys));
		return execute(connection -> {

			connection.pfMerge(rawDestinationKey, rawSourceKeys);
			return connection.pfCount(rawDestinationKey);
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
