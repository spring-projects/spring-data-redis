/*
 * Copyright 2011-2024 the original author or authors.
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

import org.springframework.core.convert.converter.Converter;
import org.springframework.core.serializer.DefaultDeserializer;
import org.springframework.core.serializer.DefaultSerializer;
import org.springframework.core.serializer.support.DeserializingConverter;
import org.springframework.core.serializer.support.SerializingConverter;
import org.springframework.data.redis.util.RedisAssertions;
import org.springframework.lang.Nullable;

/**
 * Java Serialization {@link RedisSerializer}.
 * <p>
 * Delegates to the default (Java-based) {@link DefaultSerializer serializer}
 * and {@link DefaultDeserializer deserializer}.
 * <p>
 * This {@link RedisSerializer serializer} can be constructed with either a custom {@link ClassLoader}
 * or custom {@link Converter converters}.
 *
 * @author Mark Pollack
 * @author Costin Leau
 * @author Mark Paluch
 * @author Christoph Strobl
 * @author John Blum
 */
public class JdkSerializationRedisSerializer implements RedisSerializer<Object> {

	private final Converter<Object, byte[]> serializer;
	private final Converter<byte[], Object> deserializer;

	/**
	 * Creates a new {@link JdkSerializationRedisSerializer} using the default {@link ClassLoader}.
	 */
	public JdkSerializationRedisSerializer() {
		this(new SerializingConverter(), new DeserializingConverter());
	}

	/**
	 * Creates a new {@link JdkSerializationRedisSerializer} with the given {@link ClassLoader} used to
	 * resolve {@link Class types} during deserialization.
	 *
	 * @param classLoader {@link ClassLoader} used to resolve {@link Class types} for deserialization;
	 * can be {@literal null}.
	 * @since 1.7
	 */
	public JdkSerializationRedisSerializer(@Nullable ClassLoader classLoader) {
		this(new SerializingConverter(), new DeserializingConverter(classLoader));
	}

	/**
	 * Creates a new {@link JdkSerializationRedisSerializer} using {@link Converter converters} to serialize and
	 * deserialize {@link Object objects}.
	 *
	 * @param serializer {@link Converter} used to serialize an {@link Object} to a byte array;
	 * must not be {@literal null}.
	 * @param deserializer {@link Converter} used to deserialize and convert a byte arra into an {@link Object};
	 * must not be {@literal null}
	 * @throws IllegalArgumentException if either the given {@code serializer} or {@code deserializer}
	 * are {@literal null}.
	 * @since 1.7
	 */
	public JdkSerializationRedisSerializer(Converter<Object, byte[]> serializer,
			Converter<byte[], Object> deserializer) {

		this.serializer = RedisAssertions.requireNonNull(serializer, "Serializer must not be null");
		this.deserializer = RedisAssertions.requireNonNull(deserializer, "Deserializer must not be null");
	}

	@Override
	public byte[] serialize(@Nullable Object value) {

		if (value == null) {
			return SerializationUtils.EMPTY_ARRAY;
		}

		try {
			return serializer.convert(value);
		} catch (Exception ex) {
			throw new SerializationException("Cannot serialize", ex);
		}
	}

	@Override
	public Object deserialize(@Nullable byte[] bytes) {

		if (SerializationUtils.isEmpty(bytes)) {
			return null;
		}

		try {
			return deserializer.convert(bytes);
		} catch (Exception ex) {
			throw new SerializationException("Cannot deserialize", ex);
		}
	}
}
