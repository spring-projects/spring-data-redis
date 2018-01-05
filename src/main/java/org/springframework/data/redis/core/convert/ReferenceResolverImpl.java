/*
 * Copyright 2016-2018 the original author or authors.
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
package org.springframework.data.redis.core.convert;

import java.util.Map;

import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisKeyValueAdapter;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.convert.BinaryConverters.StringToBytesConverter;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * {@link ReferenceResolver} using {@link RedisKeyValueAdapter} to read raw data.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 1.7
 */
public class ReferenceResolverImpl implements ReferenceResolver {

	private final RedisOperations<?, ?> redisOps;
	private final StringToBytesConverter converter;

	/**
	 * @param redisOperations must not be {@literal null}.
	 */
	public ReferenceResolverImpl(RedisOperations<?, ?> redisOperations) {

		Assert.notNull(redisOperations, "RedisOperations must not be null!");

		this.redisOps = redisOperations;
		this.converter = new StringToBytesConverter();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.convert.ReferenceResolver#resolveReference(java.lang.Object, java.lang.String)
	 */
	@Override
	@Nullable
	public Map<byte[], byte[]> resolveReference(Object id, String keyspace) {

		byte[] key = converter.convert(keyspace + ":" + id);

		return redisOps.execute((RedisCallback<Map<byte[], byte[]>>) connection -> connection.hGetAll(key));
	}
}
