/*
 * Copyright 2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.redis.core.convert;

import java.io.Serializable;
import java.util.Map;

import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisKeyValueAdapter;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.convert.BinaryConverters.StringToBytesConverter;
import org.springframework.util.Assert;

/**
 * {@link ReferenceResolver} using {@link RedisKeyValueAdapter} to read raw data.
 * 
 * @author Christoph Strobl
 * @since 1.7
 */
public class ReferenceResolverImpl implements ReferenceResolver {

	private final RedisOperations<?, ?> redisOps;
	private final StringToBytesConverter converter;

	/**
	 * @param redisOperations must not be {@literal null}.
	 */
	public ReferenceResolverImpl(RedisOperations<?, ?> redisOperations) {

		Assert.notNull(redisOperations);

		this.redisOps = redisOperations;
		this.converter = new StringToBytesConverter();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.convert.ReferenceResolver#resolveReference(java.io.Serializable, java.io.Serializable, java.lang.Class)
	 */
	@Override
	public Map<byte[], byte[]> resolveReference(Serializable id, String keyspace) {

		final byte[] key = converter.convert(keyspace + ":" + id);

		return redisOps.execute(new RedisCallback<Map<byte[], byte[]>>() {

			@Override
			public Map<byte[], byte[]> doInRedis(RedisConnection connection) throws DataAccessException {
				return connection.hGetAll(key);
			}
		});
	}
}
