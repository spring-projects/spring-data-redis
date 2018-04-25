/*
 * Copyright 2011-2018 the original author or authors.
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

import org.springframework.core.convert.converter.Converter;
import org.springframework.core.serializer.DefaultDeserializer;
import org.springframework.core.serializer.DefaultSerializer;
import org.springframework.core.serializer.support.DeserializingConverter;
import org.springframework.core.serializer.support.SerializingConverter;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Java Serialization Redis serializer. Delegates to the default (Java based) {@link DefaultSerializer serializer} and
 * {@link DefaultDeserializer}. This {@link RedisSerializer serializer} can be constructed with either custom
 * {@link ClassLoader} or own {@link Converter converters}.
 *
 * @author Mark Pollack
 * @author Costin Leau
 * @author Mark Paluch
 * @author Christoph Strobl
 */
public class JdkSerializationRedisSerializer implements RedisSerializer<Object> {

	private final Converter<Object, byte[]> serializer;
	private final Converter<byte[], Object> deserializer;

	/**
	 * Creates a new {@link JdkSerializationRedisSerializer} using the default class loader.
	 */
	public JdkSerializationRedisSerializer() {
		this(new SerializingConverter(), new DeserializingConverter());
	}

	/**
	 * Creates a new {@link JdkSerializationRedisSerializer} using a {@link ClassLoader}.
	 *
	 * @param classLoader the {@link ClassLoader} to use for deserialization. Can be {@literal null}.
	 * @since 1.7
	 */
	public JdkSerializationRedisSerializer(@Nullable ClassLoader classLoader) {
		this(new SerializingConverter(), new DeserializingConverter(classLoader));
	}

	/**
	 * Creates a new {@link JdkSerializationRedisSerializer} using a {@link Converter converters} to serialize and
	 * deserialize objects.
	 *
	 * @param serializer must not be {@literal null}
	 * @param deserializer must not be {@literal null}
	 * @since 1.7
	 */
	public JdkSerializationRedisSerializer(Converter<Object, byte[]> serializer, Converter<byte[], Object> deserializer) {

		Assert.notNull(serializer, "Serializer must not be null!");
		Assert.notNull(deserializer, "Deserializer must not be null!");

		this.serializer = serializer;
		this.deserializer = deserializer;
	}

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

	@Override
	public byte[] serialize(@Nullable Object object) {
		if (object == null) {
			return SerializationUtils.EMPTY_ARRAY;
		}
		try {
			return serializer.convert(object);
		} catch (Exception ex) {
			throw new SerializationException("Cannot serialize", ex);
		}
	}
}
