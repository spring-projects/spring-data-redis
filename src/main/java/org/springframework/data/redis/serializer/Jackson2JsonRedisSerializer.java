/*
 * Copyright 2011-2025 the original author or authors.
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

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import org.jspecify.annotations.Nullable;

import org.springframework.util.Assert;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ser.SerializerFactory;
import com.fasterxml.jackson.databind.type.TypeFactory;

/**
 * {@link RedisSerializer} that can read and write JSON using
 * <a href="https://github.com/FasterXML/jackson-core">Jackson's</a> and
 * <a href="https://github.com/FasterXML/jackson-databind">Jackson Databind</a> {@link ObjectMapper}.
 * <p>
 * This serializer can be used to bind to typed beans, or untyped {@link java.util.HashMap HashMap} instances.
 * <b>Note:</b>Null objects are serialized as empty arrays and vice versa.
 * <p>
 * JSON reading and writing can be customized by configuring {@link Jackson2ObjectReader} respective
 * {@link Jackson2ObjectWriter}.
 *
 * @author Thomas Darimont
 * @author Mark Paluch
 * @since 1.2
 * @deprecated since 4.0 in favor of {@link JacksonJsonRedisSerializer}.
 */
@SuppressWarnings("removal")
@Deprecated(since = "4.0", forRemoval = true)
public class Jackson2JsonRedisSerializer<T> implements RedisSerializer<T> {

	/**
	 * @deprecated since 3.0 for removal.
	 */
	@Deprecated(since = "3.0", forRemoval = true) //
	public static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;

	private final JavaType javaType;

	private ObjectMapper mapper;

	private final Jackson2ObjectReader reader;

	private final Jackson2ObjectWriter writer;

	/**
	 * Creates a new {@link Jackson2JsonRedisSerializer} for the given target {@link Class}.
	 *
	 * @param type must not be {@literal null}.
	 */
	public Jackson2JsonRedisSerializer(Class<T> type) {
		this(new ObjectMapper(), type);
	}

	/**
	 * Creates a new {@link Jackson2JsonRedisSerializer} for the given target {@link JavaType}.
	 *
	 * @param javaType must not be {@literal null}.
	 */
	public Jackson2JsonRedisSerializer(JavaType javaType) {
		this(new ObjectMapper(), javaType);
	}

	/**
	 * Creates a new {@link Jackson2JsonRedisSerializer} for the given target {@link Class}.
	 *
	 * @param mapper must not be {@literal null}.
	 * @param type must not be {@literal null}.
	 * @since 3.0
	 */
	public Jackson2JsonRedisSerializer(ObjectMapper mapper, Class<T> type) {

		Assert.notNull(mapper, "ObjectMapper must not be null");
		Assert.notNull(type, "Java type must not be null");

		this.javaType = getJavaType(type);
		this.mapper = mapper;
		this.reader = Jackson2ObjectReader.create();
		this.writer = Jackson2ObjectWriter.create();
	}

	/**
	 * Creates a new {@link Jackson2JsonRedisSerializer} for the given target {@link JavaType}.
	 *
	 * @param mapper must not be {@literal null}.
	 * @param javaType must not be {@literal null}.
	 * @since 3.0
	 */
	public Jackson2JsonRedisSerializer(ObjectMapper mapper, JavaType javaType) {
		this(mapper, javaType, Jackson2ObjectReader.create(), Jackson2ObjectWriter.create());
	}

	/**
	 * Creates a new {@link Jackson2JsonRedisSerializer} for the given target {@link JavaType}.
	 *
	 * @param mapper must not be {@literal null}.
	 * @param javaType must not be {@literal null}.
	 * @param reader the {@link Jackson2ObjectReader} function to read objects using {@link ObjectMapper}.
	 * @param writer the {@link Jackson2ObjectWriter} function to write objects using {@link ObjectMapper}.
	 * @since 3.0
	 */
	public Jackson2JsonRedisSerializer(ObjectMapper mapper, JavaType javaType, Jackson2ObjectReader reader,
			Jackson2ObjectWriter writer) {

		Assert.notNull(mapper, "ObjectMapper must not be null!");
		Assert.notNull(reader, "Reader must not be null!");
		Assert.notNull(writer, "Writer must not be null!");

		this.mapper = mapper;
		this.reader = reader;
		this.writer = writer;
		this.javaType = javaType;
	}

	/**
	 * Sets the {@code ObjectMapper} for this view. If not set, a default {@link ObjectMapper#ObjectMapper() ObjectMapper}
	 * is used.
	 * <p>
	 * Setting a custom-configured {@code ObjectMapper} is one way to take further control of the JSON serialization
	 * process. For example, an extended {@link SerializerFactory} can be configured that provides custom serializers for
	 * specific types. The other option for refining the serialization process is to use Jackson's provided annotations on
	 * the types to be serialized, in which case a custom-configured ObjectMapper is unnecessary.
	 *
	 * @deprecated since 3.0, use {@link #Jackson2JsonRedisSerializer(ObjectMapper, Class) constructor creation} to
	 *             configure the object mapper.
	 */
	@Deprecated(since = "3.0", forRemoval = true)
	public void setObjectMapper(ObjectMapper mapper) {

		Assert.notNull(mapper, "'objectMapper' must not be null");
		this.mapper = mapper;
	}

	@Override
	public byte[] serialize(@Nullable T value) throws SerializationException {

		if (value == null) {
			return SerializationUtils.EMPTY_ARRAY;
		}
		try {
			return this.writer.write(this.mapper, value);
		} catch (Exception ex) {
			throw new SerializationException("Could not write JSON: " + ex.getMessage(), ex);
		}
	}

	@Nullable
	@Override
	@SuppressWarnings("unchecked")
	public T deserialize(byte @Nullable[] bytes) throws SerializationException {

		if (SerializationUtils.isEmpty(bytes)) {
			return null;
		}
		try {
			return (T) this.reader.read(this.mapper, bytes, javaType);
		} catch (Exception ex) {
			throw new SerializationException("Could not read JSON: " + ex.getMessage(), ex);
		}
	}

	/**
	 * Returns the Jackson {@link JavaType} for the specific class.
	 * <p>
	 * Default implementation returns {@link TypeFactory#constructType(java.lang.reflect.Type)}, but this can be
	 * overridden in subclasses, to allow for custom generic collection handling. For instance:
	 *
	 * <pre class="code">
	 * protected JavaType getJavaType(Class&lt;?&gt; clazz) {
	 * 	if (List.class.isAssignableFrom(clazz)) {
	 * 		return TypeFactory.defaultInstance().constructCollectionType(ArrayList.class, MyBean.class);
	 * 	} else {
	 * 		return super.getJavaType(clazz);
	 * 	}
	 * }
	 * </pre>
	 *
	 * @param clazz the class to return the java type for
	 * @return the java type
	 */
	protected JavaType getJavaType(Class<?> clazz) {
		return TypeFactory.defaultInstance().constructType(clazz);
	}

}
