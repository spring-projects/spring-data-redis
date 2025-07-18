/*
 * Copyright 2017-2025 the original author or authors.
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
package org.springframework.data.redis.serializer;

import java.nio.ByteBuffer;

import org.jspecify.annotations.Nullable;
import org.springframework.data.redis.serializer.RedisSerializationContext.SerializationPair;
import org.springframework.util.Assert;

/**
 * Adapter to delegate serialization/deserialization to {@link RedisSerializer}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @author John Blum
 * @since 2.0
 */
class RedisSerializerToSerializationPairAdapter<T> implements SerializationPair<T> {

	private static final RedisSerializerToSerializationPairAdapter<?> BYTE_BUFFER =
			new RedisSerializerToSerializationPairAdapter<>(null);

	private static final RedisSerializerToSerializationPairAdapter<byte[]> BYTE_ARRAY =
			new RedisSerializerToSerializationPairAdapter<>(RedisSerializer.byteArray());

	private final DefaultSerializationPair<T> pair;

	RedisSerializerToSerializationPairAdapter(@Nullable RedisSerializer<T> serializer) {
		pair = new DefaultSerializationPair<>(new DefaultRedisElementReader<>(serializer),
				new DefaultRedisElementWriter<>(serializer));
	}

	/**
	 * @return the {@link RedisSerializerToSerializationPairAdapter} for {@link ByteBuffer}.
	 * @deprecated since 2.2. Please use {@link #byteBuffer()} instead.
	 */
	@SuppressWarnings("unchecked")
	@Deprecated
	static <T> SerializationPair<T> raw() {
		return (SerializationPair<T>) byteBuffer();
	}

	/**
	 * @return the {@link RedisSerializerToSerializationPairAdapter} for {@code byte[]}.
	 * @since 2.2
	 */
	static SerializationPair<byte[]> byteArray() {
		return BYTE_ARRAY;
	}

	/**
	 * @return the {@link RedisSerializerToSerializationPairAdapter} for {@link ByteBuffer}.
	 * @since 2.2
	 */
	@SuppressWarnings("unchecked")
	static SerializationPair<ByteBuffer> byteBuffer() {
		return (SerializationPair<ByteBuffer>) BYTE_BUFFER;
	}

	/**
	 * Create a {@link SerializationPair} from given {@link RedisSerializer}.
	 *
	 * @param <T> {@link Class type} of {@link Object} handled by the {@link RedisSerializer}.
	 * @param redisSerializer must not be {@literal null}.
	 * @return the given {@link RedisSerializer} adapted as a {@link SerializationPair}.
	 */
	public static <T> SerializationPair<T> from(RedisSerializer<T> redisSerializer) {

		Assert.notNull(redisSerializer, "RedisSerializer must not be null");

		return new RedisSerializerToSerializationPairAdapter<>(redisSerializer);
	}

	@Override
	public RedisElementReader<T> getReader() {
		return pair.getReader();
	}

	@Override
	public RedisElementWriter<T> getWriter() {
		return pair.getWriter();
	}

}
