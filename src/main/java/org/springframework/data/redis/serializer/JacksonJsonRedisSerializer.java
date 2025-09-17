/*
 * Copyright 2025 the original author or authors.
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

import tools.jackson.databind.JavaType;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.type.TypeFactory;

import org.jspecify.annotations.Nullable;

import org.springframework.util.Assert;

/**
 * {@link RedisSerializer} that can read and write JSON using
 * <a href="https://github.com/FasterXML/jackson-core">Jackson 3</a> and
 * <a href="https://github.com/FasterXML/jackson-databind">Jackson 3 Databind</a> {@link ObjectMapper}.
 * <p>
 * This serializer can be used to bind to typed beans, or untyped {@link java.util.HashMap HashMap} instances.
 * <b>Note:</b>Null objects are serialized as empty arrays and vice versa.
 * <p>
 * JSON reading and writing can be customized by configuring {@link JacksonObjectReader} respective
 * {@link JacksonObjectWriter}.
 *
 * @author Christoph Strobl
 * @author Thomas Darimont
 * @author Mark Paluch
 * @since 4.0
 */
public class JacksonJsonRedisSerializer<T> implements RedisSerializer<T> {

	private final JavaType javaType;

	private final ObjectMapper mapper;

	private final JacksonObjectReader reader;

	private final JacksonObjectWriter writer;

	/**
	 * Creates a new {@link JacksonJsonRedisSerializer} for the given target {@link Class}.
	 *
	 * @param type must not be {@literal null}.
	 */
	public JacksonJsonRedisSerializer(Class<T> type) {
		this(JsonMapper.shared(), type);
	}

	/**
	 * Creates a new {@link JacksonJsonRedisSerializer} for the given target {@link JavaType}.
	 *
	 * @param javaType must not be {@literal null}.
	 */
	public JacksonJsonRedisSerializer(JavaType javaType) {
		this(JsonMapper.shared(), javaType);
	}

	/**
	 * Creates a new {@link JacksonJsonRedisSerializer} for the given target {@link Class}.
	 *
	 * @param mapper must not be {@literal null}.
	 * @param type must not be {@literal null}.
	 */
	public JacksonJsonRedisSerializer(ObjectMapper mapper, Class<T> type) {

		Assert.notNull(mapper, "ObjectMapper must not be null");
		Assert.notNull(type, "Java type must not be null");

		this.javaType = getJavaType(type);
		this.mapper = mapper;
		this.reader = JacksonObjectReader.create();
		this.writer = JacksonObjectWriter.create();
	}

	/**
	 * Creates a new {@link JacksonJsonRedisSerializer} for the given target {@link JavaType}.
	 *
	 * @param mapper must not be {@literal null}.
	 * @param javaType must not be {@literal null}.
	 */
	public JacksonJsonRedisSerializer(ObjectMapper mapper, JavaType javaType) {
		this(mapper, javaType, JacksonObjectReader.create(), JacksonObjectWriter.create());
	}

	/**
	 * Creates a new {@link JacksonJsonRedisSerializer} for the given target {@link JavaType}.
	 *
	 * @param mapper must not be {@literal null}.
	 * @param javaType must not be {@literal null}.
	 * @param reader the {@link JacksonObjectReader} function to read objects using {@link ObjectMapper}.
	 * @param writer the {@link JacksonObjectWriter} function to write objects using {@link ObjectMapper}.
	 */
	public JacksonJsonRedisSerializer(ObjectMapper mapper, JavaType javaType, JacksonObjectReader reader,
			JacksonObjectWriter writer) {

		Assert.notNull(mapper, "ObjectMapper must not be null");
		Assert.notNull(reader, "Reader must not be null");
		Assert.notNull(writer, "Writer must not be null");

		this.mapper = mapper;
		this.reader = reader;
		this.writer = writer;
		this.javaType = javaType;
	}

	@Override
	public byte[] serialize(@Nullable T value) throws SerializationException {

		if (value == null) {
			return SerializationUtils.EMPTY_ARRAY;
		}
		try {
			return this.writer.write(this.mapper, value);
		} catch (RuntimeException ex) {
			throw new SerializationException("Could not write JSON: " + ex.getMessage(), ex);
		}
	}

	@Nullable
	@Override
	@SuppressWarnings("unchecked")
	public T deserialize(byte @Nullable [] bytes) throws SerializationException {

		if (SerializationUtils.isEmpty(bytes)) {
			return null;
		}
		try {
			return (T) this.reader.read(this.mapper, bytes, javaType);
		} catch (RuntimeException ex) {
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
		return TypeFactory.unsafeSimpleType(clazz);
	}

}
