/*
 * Copyright 2015-2018 the original author or authors.
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

import java.io.IOException;

import org.springframework.cache.support.NullValue;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.As;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectMapper.DefaultTyping;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.SerializerFactory;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

/**
 * @author Christoph Strobl
 * @since 1.6
 */
public class GenericJackson2JsonRedisSerializer implements RedisSerializer<Object> {

	private final ObjectMapper mapper;

	/**
	 * Creates {@link GenericJackson2JsonRedisSerializer} and configures {@link ObjectMapper} for default typing.
	 */
	public GenericJackson2JsonRedisSerializer() {
		this((String) null);
	}

	/**
	 * Creates {@link GenericJackson2JsonRedisSerializer} and configures {@link ObjectMapper} for default typing using the
	 * given {@literal name}. In case of an {@literal empty} or {@literal null} String the default
	 * {@link JsonTypeInfo.Id#CLASS} will be used.
	 *
	 * @param classPropertyTypeName Name of the JSON property holding type information. Can be {@literal null}.
	 */
	public GenericJackson2JsonRedisSerializer(@Nullable String classPropertyTypeName) {

		this(new ObjectMapper());

		// simply setting {@code mapper.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS)} does not help here since we need
		// the type hint embedded for deserialization using the default typing feature.
		mapper.registerModule(new SimpleModule().addSerializer(new NullValueSerializer(classPropertyTypeName)));

		if (StringUtils.hasText(classPropertyTypeName)) {
			mapper.enableDefaultTypingAsProperty(DefaultTyping.NON_FINAL, classPropertyTypeName);
		} else {
			mapper.enableDefaultTyping(DefaultTyping.NON_FINAL, As.PROPERTY);
		}
	}

	/**
	 * Setting a custom-configured {@link ObjectMapper} is one way to take further control of the JSON serialization
	 * process. For example, an extended {@link SerializerFactory} can be configured that provides custom serializers for
	 * specific types.
	 *
	 * @param mapper must not be {@literal null}.
	 */
	public GenericJackson2JsonRedisSerializer(ObjectMapper mapper) {

		Assert.notNull(mapper, "ObjectMapper must not be null!");
		this.mapper = mapper;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.serializer.RedisSerializer#serialize(java.lang.Object)
	 */
	@Override
	public byte[] serialize(@Nullable Object source) throws SerializationException {

		if (source == null) {
			return SerializationUtils.EMPTY_ARRAY;
		}

		try {
			return mapper.writeValueAsBytes(source);
		} catch (JsonProcessingException e) {
			throw new SerializationException("Could not write JSON: " + e.getMessage(), e);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.serializer.RedisSerializer#deserialize(byte[])
	 */
	@Override
	public Object deserialize(@Nullable byte[] source) throws SerializationException {
		return deserialize(source, Object.class);
	}

	/**
	 * @param source can be {@literal null}.
	 * @param type must not be {@literal null}.
	 * @return {@literal null} for empty source.
	 * @throws SerializationException
	 */
	@Nullable
	public <T> T deserialize(@Nullable byte[] source, Class<T> type) throws SerializationException {

		Assert.notNull(type,
				"Deserialization type must not be null! Pleaes provide Object.class to make use of Jackson2 default typing.");

		if (SerializationUtils.isEmpty(source)) {
			return null;
		}

		try {
			return mapper.readValue(source, type);
		} catch (Exception ex) {
			throw new SerializationException("Could not read JSON: " + ex.getMessage(), ex);
		}
	}

	/**
	 * {@link StdSerializer} adding class information required by default typing. This allows de-/serialization of
	 * {@link NullValue}.
	 *
	 * @author Christoph Strobl
	 * @since 1.8
	 */
	private class NullValueSerializer extends StdSerializer<NullValue> {

		private static final long serialVersionUID = 1999052150548658808L;
		private final String classIdentifier;

		/**
		 * @param classIdentifier can be {@literal null} and will be defaulted to {@code @class}.
		 */
		NullValueSerializer(@Nullable String classIdentifier) {

			super(NullValue.class);
			this.classIdentifier = StringUtils.hasText(classIdentifier) ? classIdentifier : "@class";
		}

		/*
		 * (non-Javadoc)
		 * @see com.fasterxml.jackson.databind.ser.std.StdSerializer#serialize(java.lang.Object, com.fasterxml.jackson.core.JsonGenerator, com.fasterxml.jackson.databind.SerializerProvider)
		 */
		@Override
		public void serialize(NullValue value, JsonGenerator jgen, SerializerProvider provider)
				throws IOException {

			jgen.writeStartObject();
			jgen.writeStringField(classIdentifier, NullValue.class.getName());
			jgen.writeEndObject();
		}
	}
}
