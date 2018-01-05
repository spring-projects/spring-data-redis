/*
 * Copyright 2017-2018 the original author or authors.
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
package org.springframework.data.redis.serializer;

import org.springframework.data.redis.serializer.RedisSerializationContext.SerializationPair;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Adapter to delegate serialization/deserialization to {@link RedisSerializer}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.0
 */
class RedisSerializerToSerializationPairAdapter<T> implements SerializationPair<T> {

	private final static RedisSerializerToSerializationPairAdapter<?> RAW = new RedisSerializerToSerializationPairAdapter<>(
			null);

	private final DefaultSerializationPair pair;

	protected RedisSerializerToSerializationPairAdapter(@Nullable RedisSerializer<T> serializer) {
		pair = new DefaultSerializationPair(new DefaultRedisElementReader<>(serializer),
				new DefaultRedisElementWriter<>(serializer));
	}

	@SuppressWarnings("unchecked")
	public static <T> SerializationPair<T> raw() {
		return (SerializationPair) RAW;
	}

	/**
	 * Create a {@link SerializationPair} from given {@link RedisSerializer}.
	 *
	 * @param redisSerializer must not be {@literal null}.
	 * @param <T>
	 * @return
	 */
	public static <T> SerializationPair<T> from(RedisSerializer<T> redisSerializer) {

		Assert.notNull(redisSerializer, "RedisSerializer must not be null!");

		return new RedisSerializerToSerializationPairAdapter<>(redisSerializer);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.serializer.RedisSerializationContext.SerializationPair#reader()
	 */
	@Override
	public RedisElementReader<T> getReader() {
		return pair.getReader();
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.serializer.RedisSerializationContext.SerializationPair#writer()
	 */
	@Override
	public RedisElementWriter<T> getWriter() {
		return pair.getWriter();
	}
}
