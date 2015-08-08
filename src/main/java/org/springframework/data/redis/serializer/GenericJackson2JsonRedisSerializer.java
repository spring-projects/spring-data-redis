/*
 * Copyright 2015 the original author or authors.
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

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ser.SerializerFactory;

/**
 * {@link RedisSerializer} that can read and write JSON for arbitrary input types at the expense of additional type
 * information stored with with value by using <a href="https://github.com/FasterXML/jackson-core">Jackson's</a> and <a
 * href="https://github.com/FasterXML/jackson-databind">Jackson Databind</a> {@link ObjectMapper}.
 * <p>
 * This converter can be used to bind to typed beans, or untyped {@link java.util.HashMap HashMap} instances.
 * <b>Note:</b>Null objects are serialized as empty arrays and vice versa.
 * <p>
 * The serialized form consists of:
 * <ol>
 * <li>Payload type name length (short, 2 bytes)</li>
 * <li>Payload type name in bytes encoded with UTF-8</li>
 * <li>Payload data bytes</li>
 * </ol>
 * 
 * @author Thomas Darimont
 * @since 1.6
 */
public class GenericJackson2JsonRedisSerializer implements RedisSerializer<Object> {

	public static final Charset DEFAULT_CHARSET = Charset.forName("UTF-8");

	private ObjectMapper objectMapper = new ObjectMapper();

	private final Map<String, Class<?>> typeKeyToClass;
	private final Map<Class<?>, String> classToTypeKey;

	/**
	 * Creates a new {@link GenericJackson2JsonRedisSerializer}.
	 */
	public GenericJackson2JsonRedisSerializer() {
		this(Collections.<Class<?>, String> emptyMap());
	}

	/**
	 * Creates a new {@link GenericJackson2JsonRedisSerializer} with support for type aliases.
	 * <p>
	 * The type alias mappings based on the given {@link Map} of {@link Class} to {@link String} aliases will be honored
	 * during serialization and deserialization.
	 * 
	 * @param classAliases must not be {@literal null}
	 */
	public GenericJackson2JsonRedisSerializer(Map<Class<?>, String> classAliases) {

		Assert.notNull(classAliases, "Class aliases mus not be null!");

		this.classToTypeKey = new HashMap<Class<?>, String>(classAliases);
		this.typeKeyToClass = new HashMap<String, Class<?>>(classAliases.size());

		for (Map.Entry<Class<?>, String> entry : classAliases.entrySet()) {
			this.typeKeyToClass.put(entry.getValue(), entry.getKey());
		}
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.serializer.RedisSerializer#deserialize(byte[])
	 */
	@Override
	public Object deserialize(byte[] bytes) throws SerializationException {

		if (SerializationUtils.isEmpty(bytes)) {
			return null;
		}

		try {

			ByteBuffer buffer = ByteBuffer.wrap(bytes);

			int typeLength = buffer.getShort();
			byte[] typeBytes = new byte[typeLength];
			buffer.get(typeBytes);

			// resolve valuetype first to allow early exit on unresolveable types
			String typeKey = new String(typeBytes, DEFAULT_CHARSET);
			Class<?> valueType = this.typeKeyToClass.get(typeKey);
			if (valueType == null) {
				valueType = ClassUtils.resolveClassName(typeKey, getClass().getClassLoader());
			}

			byte[] valueBytes = new byte[buffer.remaining()];
			buffer.get(valueBytes);

			return this.objectMapper.readValue(valueBytes, 0, valueBytes.length, valueType);
		} catch (Exception ex) {
			throw new SerializationException("Could not read JSON: " + ex.getMessage(), ex);
		}
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.serializer.RedisSerializer#serialize(java.lang.Object)
	 */
	@Override
	public byte[] serialize(Object value) throws SerializationException {

		if (value == null) {
			return SerializationUtils.EMPTY_ARRAY;
		}

		try {

			byte[] valueBytes = this.objectMapper.writeValueAsBytes(value);
			String typeKey = this.classToTypeKey.get(value.getClass());
			if (typeKey == null) {
				typeKey = value.getClass().getName();
			}

			byte[] typeBytes = typeKey.getBytes(DEFAULT_CHARSET);

			ByteBuffer buffer = ByteBuffer.allocate(Short.BYTES + typeBytes.length + valueBytes.length);
			buffer.putShort((short) typeBytes.length);
			buffer.put(typeBytes);
			buffer.put(valueBytes);

			return buffer.array();
		} catch (Exception ex) {
			throw new SerializationException("Could not write JSON: " + ex.getMessage(), ex);
		}
	}

	/**
	 * Sets the {@code ObjectMapper} for this view. If not set, a default {@link ObjectMapper#ObjectMapper() ObjectMapper}
	 * is used.
	 * <p>
	 * Setting a custom-configured {@code ObjectMapper} is one way to take further control of the JSON serialization
	 * process. For example, an extended {@link SerializerFactory} can be configured that provides custom serializers for
	 * specific types. The other option for refining the serialization process is to use Jackson's provided annotations on
	 * the types to be serialized, in which case a custom-configured ObjectMapper is unnecessary.
	 */
	public void setObjectMapper(ObjectMapper objectMapper) {

		Assert.notNull(objectMapper, "'objectMapper' must not be null");
		this.objectMapper = objectMapper;
	}
}
