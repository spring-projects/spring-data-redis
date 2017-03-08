/*
 * Copyright 2017 the original author or authors.
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

import org.springframework.data.redis.serializer.ReactiveSerializationContext.SerializationTuple;

/**
 * Adapter to delegate serialization/deserialization to {@link RedisSerializer}.
 *
 * @author Mark Paluch
 * @since 2.0
 */
class RedisSerializerTupleAdapter<T> implements SerializationTuple<T> {

	private final static RedisSerializerTupleAdapter<?> RAW = new RedisSerializerTupleAdapter<>(null);

	private final RedisElementReader<T> reader;
	private final RedisElementWriter<T> writer;

	protected RedisSerializerTupleAdapter(RedisSerializer<T> serializer) {

		reader = new DefaultRedisElementReader<>(serializer);
		writer = new DefaultRedisElementWriter<>(serializer);
	}

	@SuppressWarnings("unchecked")
	public static <T> SerializationTuple<T> raw() {
		return (SerializationTuple) RAW;
	}

	public static <T> SerializationTuple<T> from(RedisSerializer<T> redisSerializer) {
		return new RedisSerializerTupleAdapter<>(redisSerializer);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.serializer.ReactiveSerializationContext.SerializationTuple#reader()
	 */
	@Override
	public RedisElementReader<T> getReader() {
		return reader;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.serializer.ReactiveSerializationContext.SerializationTuple#writer()
	 */
	@Override
	public RedisElementWriter<T> getWriter() {
		return writer;
	}
}
