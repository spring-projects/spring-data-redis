/*
 * Copyright 2015-2023 the original author or authors.
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

import java.io.IOException;
import java.io.Serial;
import java.util.Collections;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.springframework.cache.support.NullValue;
import org.springframework.core.KotlinDetector;
import org.springframework.data.util.Lazy;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.As;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.DeserializationConfig;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectMapper.DefaultTyping;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.PolymorphicTypeValidator;
import com.fasterxml.jackson.databind.jsontype.TypeDeserializer;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.fasterxml.jackson.databind.jsontype.impl.StdTypeResolverBuilder;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.TextNode;
import com.fasterxml.jackson.databind.ser.SerializerFactory;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.fasterxml.jackson.databind.type.TypeFactory;

/**
 * Generic Jackson 2-based {@link RedisSerializer} that maps {@link Object objects} to and from {@literal JSON} using
 * dynamic typing.
 * <p>
 * {@literal JSON} reading and writing can be customized by configuring a {@link JacksonObjectReader} and
 * {@link JacksonObjectWriter}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Mao Shuai
 * @author John Blum
 * @see org.springframework.data.redis.serializer.JacksonObjectReader
 * @see org.springframework.data.redis.serializer.JacksonObjectWriter
 * @see com.fasterxml.jackson.databind.ObjectMapper
 * @since 1.6
 */
public class GenericJackson2JsonRedisSerializer implements RedisSerializer<Object> {

	private final JacksonObjectReader reader;

	private final JacksonObjectWriter writer;

	private final Lazy<Boolean> defaultTypingEnabled;

	private final ObjectMapper mapper;

	private final TypeResolver typeResolver;

	/**
	 * Creates {@link GenericJackson2JsonRedisSerializer} initialized with an {@link ObjectMapper} configured for default
	 * typing.
	 */
	public GenericJackson2JsonRedisSerializer() {
		this((String) null);
	}

	/**
	 * Creates {@link GenericJackson2JsonRedisSerializer} initialized with an {@link ObjectMapper} configured for default
	 * typing using the given {@link String name}.
	 * <p>
	 * In case {@link String name} is {@literal empty} or {@literal null}, then {@link JsonTypeInfo.Id#CLASS} will be
	 * used.
	 *
	 * @param classPropertyTypeName {@link String name} of the JSON property holding type information; can be
	 *          {@literal null}.
	 * @see ObjectMapper#activateDefaultTypingAsProperty(PolymorphicTypeValidator, DefaultTyping, String)
	 * @see ObjectMapper#activateDefaultTyping(PolymorphicTypeValidator, DefaultTyping, As)
	 */
	public GenericJackson2JsonRedisSerializer(@Nullable String classPropertyTypeName) {
		this(classPropertyTypeName, JacksonObjectReader.create(), JacksonObjectWriter.create());
	}

	/**
	 * Creates {@link GenericJackson2JsonRedisSerializer} initialized with an {@link ObjectMapper} configured for default
	 * typing using the given {@link String name} along with the given, required {@link JacksonObjectReader} and
	 * {@link JacksonObjectWriter} used to read/write {@link Object Objects} de/serialized as JSON.
	 * <p>
	 * In case {@link String name} is {@literal empty} or {@literal null}, then {@link JsonTypeInfo.Id#CLASS} will be
	 * used.
	 *
	 * @param classPropertyTypeName {@link String name} of the JSON property holding type information; can be
	 *          {@literal null}.
	 * @param reader {@link JacksonObjectReader} function to read objects using {@link ObjectMapper}.
	 * @param writer {@link JacksonObjectWriter} function to write objects using {@link ObjectMapper}.
	 * @see ObjectMapper#activateDefaultTypingAsProperty(PolymorphicTypeValidator, DefaultTyping, String)
	 * @see ObjectMapper#activateDefaultTyping(PolymorphicTypeValidator, DefaultTyping, As)
	 * @since 3.0
	 */
	public GenericJackson2JsonRedisSerializer(@Nullable String classPropertyTypeName, JacksonObjectReader reader,
			JacksonObjectWriter writer) {

		this(new ObjectMapper(), reader, writer, classPropertyTypeName);

		registerNullValueSerializer(this.mapper, classPropertyTypeName);

		StdTypeResolverBuilder typer = TypeResolverBuilder.forEverything(this.mapper).init(JsonTypeInfo.Id.CLASS, null)
				.inclusion(JsonTypeInfo.As.PROPERTY);

		if (StringUtils.hasText(classPropertyTypeName)) {
			typer = typer.typeProperty(classPropertyTypeName);
		}

		this.mapper.setDefaultTyping(typer);
	}

	/**
	 * Setting a custom-configured {@link ObjectMapper} is one way to take further control of the JSON serialization
	 * process. For example, an extended {@link SerializerFactory} can be configured that provides custom serializers for
	 * specific types.
	 *
	 * @param mapper must not be {@literal null}.
	 */
	public GenericJackson2JsonRedisSerializer(ObjectMapper mapper) {
		this(mapper, JacksonObjectReader.create(), JacksonObjectWriter.create());
	}

	/**
	 * Setting a custom-configured {@link ObjectMapper} is one way to take further control of the JSON serialization
	 * process. For example, an extended {@link SerializerFactory} can be configured that provides custom serializers for
	 * specific types.
	 *
	 * @param mapper must not be {@literal null}.
	 * @param reader the {@link JacksonObjectReader} function to read objects using {@link ObjectMapper}.
	 * @param writer the {@link JacksonObjectWriter} function to write objects using {@link ObjectMapper}.
	 * @since 3.0
	 */
	public GenericJackson2JsonRedisSerializer(ObjectMapper mapper, JacksonObjectReader reader,
			JacksonObjectWriter writer) {

		this(mapper, reader, writer, null);
	}

	private GenericJackson2JsonRedisSerializer(ObjectMapper mapper, JacksonObjectReader reader,
			JacksonObjectWriter writer, @Nullable String typeHintPropertyName) {

		Assert.notNull(mapper, "ObjectMapper must not be null");
		Assert.notNull(reader, "Reader must not be null");
		Assert.notNull(writer, "Writer must not be null");

		this.mapper = mapper;
		this.reader = reader;
		this.writer = writer;

		this.defaultTypingEnabled = Lazy.of(() -> mapper.getSerializationConfig().getDefaultTyper(null) != null);

		this.typeResolver = newTypeResolver(mapper, typeHintPropertyName, this.defaultTypingEnabled);
	}

	private TypeResolver newTypeResolver(ObjectMapper mapper, @Nullable String typeHintPropertyName,
			Lazy<Boolean> defaultTypingEnabled) {

		Lazy<TypeFactory> lazyTypeFactory = Lazy.of(mapper::getTypeFactory);

		Lazy<String> lazyTypeHintPropertyName = typeHintPropertyName != null ? Lazy.of(typeHintPropertyName)
				: newLazyTypeHintPropertyName(mapper, defaultTypingEnabled);

		return new TypeResolver(lazyTypeFactory, lazyTypeHintPropertyName);
	}

	private Lazy<String> newLazyTypeHintPropertyName(ObjectMapper mapper, Lazy<Boolean> defaultTypingEnabled) {

		Lazy<String> configuredTypeDeserializationPropertyName = getConfiguredTypeDeserializationPropertyName(mapper);

		Lazy<String> resolvedLazyTypeHintPropertyName = Lazy.of(() -> defaultTypingEnabled.get() ? null
				: configuredTypeDeserializationPropertyName.get());

		resolvedLazyTypeHintPropertyName = resolvedLazyTypeHintPropertyName.or("@class");

		return resolvedLazyTypeHintPropertyName;
	}

	private Lazy<String> getConfiguredTypeDeserializationPropertyName(ObjectMapper mapper) {

		return Lazy.of(() -> {

			DeserializationConfig deserializationConfig = mapper.getDeserializationConfig();

			JavaType objectType = mapper.getTypeFactory().constructType(Object.class);

			TypeDeserializer typeDeserializer = deserializationConfig.getDefaultTyper(null)
					.buildTypeDeserializer(deserializationConfig, objectType, Collections.emptyList());

			return typeDeserializer.getPropertyName();
		});
	}

	/**
	 * Register {@link NullValueSerializer} in the given {@link ObjectMapper} with an optional
	 * {@code classPropertyTypeName}. This method should be called by code that customizes
	 * {@link GenericJackson2JsonRedisSerializer} by providing an external {@link ObjectMapper}.
	 *
	 * @param objectMapper the object mapper to customize.
	 * @param classPropertyTypeName name of the type property. Defaults to {@code @class} if {@literal null}/empty.
	 * @since 2.2
	 */
	public static void registerNullValueSerializer(ObjectMapper objectMapper, @Nullable String classPropertyTypeName) {

		// Simply setting {@code mapper.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS)} does not help here
		// since we need the type hint embedded for deserialization using the default typing feature.
		objectMapper.registerModule(new SimpleModule().addSerializer(new NullValueSerializer(classPropertyTypeName)));
	}

	/**
	 * Gets the configured {@link ObjectMapper} used internally by this {@link GenericJackson2JsonRedisSerializer} to
	 * de/serialize {@link Object objects} as {@literal JSON}.
	 *
	 * @return the configured {@link ObjectMapper}.
	 */
	protected ObjectMapper getObjectMapper() {
		return this.mapper;
	}

	@Override
	public byte[] serialize(@Nullable Object value) throws SerializationException {

		if (value == null) {
			return SerializationUtils.EMPTY_ARRAY;
		}

		try {
			return writer.write(mapper, value);
		} catch (IOException cause) {
			String message = String.format("Could not write JSON: %s", cause.getMessage());
			throw new SerializationException(message, cause);
		}
	}

	@Override
	public Object deserialize(@Nullable byte[] source) throws SerializationException {
		return deserialize(source, Object.class);
	}

	/**
	 * Deserialized the array of bytes containing {@literal JSON} as an {@link Object} of the given, required {@link Class
	 * type}.
	 *
	 * @param source array of bytes containing the {@literal JSON} to deserialize; can be {@literal null}.
	 * @param type {@link Class type} of {@link Object} from which the {@literal JSON} will be deserialized; must not be
	 *          {@literal null}.
	 * @return {@literal null} for an empty source, or an {@link Object} of the given {@link Class type} deserialized from
	 *         the array of bytes containing {@literal JSON}.
	 * @throws IllegalArgumentException if the given {@link Class type} is {@literal null}.
	 * @throws SerializationException if the array of bytes cannot be deserialized as an instance of the given
	 *           {@link Class type}
	 */
	@Nullable
	@SuppressWarnings("unchecked")
	public <T> T deserialize(@Nullable byte[] source, Class<T> type) throws SerializationException {

		Assert.notNull(type, "Deserialization type must not be null;"
				+ " Please provide Object.class to make use of Jackson2 default typing.");

		if (SerializationUtils.isEmpty(source)) {
			return null;
		}

		try {
			return (T) reader.read(mapper, source, resolveType(source, type));
		} catch (Exception cause) {
			String message = String.format("Could not read JSON:%s ", cause.getMessage());
			throw new SerializationException(message, cause);
		}
	}

	/**
	 * Builder method used to configure and customize the internal Jackson {@link ObjectMapper} created by this
	 * {@link GenericJackson2JsonRedisSerializer} and used to de/serialize {@link Object objects} as {@literal JSON}.
	 *
	 * @param objectMapperConfigurer {@link Consumer} used to configure and customize the internal {@link ObjectMapper};
	 *          must not be {@literal null}.
	 * @return this {@link GenericJackson2JsonRedisSerializer}.
	 * @throws IllegalArgumentException if the {@link Consumer} used to configure and customize the internal
	 *           {@link ObjectMapper} is {@literal null}.
	 * @since 3.1.5
	 */
	public GenericJackson2JsonRedisSerializer configure(Consumer<ObjectMapper> objectMapperConfigurer) {

		Assert.notNull(objectMapperConfigurer, "Consumer used to configure and customize ObjectMapper must not be null");

		objectMapperConfigurer.accept(getObjectMapper());

		return this;
	}

	protected JavaType resolveType(byte[] source, Class<?> type) throws IOException {

		if (!type.equals(Object.class) || !defaultTypingEnabled.get()) {
			return typeResolver.constructType(type);
		}

		return typeResolver.resolveType(source, type);
	}

	/**
	 * @since 3.0
	 */
	static class TypeResolver {

		// need a separate instance to bypass class hint checks
		private final ObjectMapper mapper = new ObjectMapper();

		private final Supplier<TypeFactory> typeFactory;
		private final Supplier<String> hintName;

		TypeResolver(Supplier<TypeFactory> typeFactory, Supplier<String> hintName) {

			this.typeFactory = typeFactory;
			this.hintName = hintName;
		}

		protected JavaType constructType(Class<?> type) {
			return typeFactory.get().constructType(type);
		}

		protected JavaType resolveType(byte[] source, Class<?> type) throws IOException {

			JsonNode root = mapper.readTree(source);
			JsonNode jsonNode = root.get(hintName.get());

			if (jsonNode instanceof TextNode && jsonNode.asText() != null) {
				return typeFactory.get().constructFromCanonical(jsonNode.asText());
			}

			return constructType(type);
		}
	}

	/**
	 * {@link StdSerializer} adding class information required by default typing. This allows de-/serialization of
	 * {@link NullValue}.
	 *
	 * @author Christoph Strobl
	 * @since 1.8
	 */
	private static class NullValueSerializer extends StdSerializer<NullValue> {

		@Serial
		private static final long serialVersionUID = 1999052150548658808L;

		private final String classIdentifier;

		/**
		 * @param classIdentifier can be {@literal null} and will be defaulted to {@code @class}.
		 */
		NullValueSerializer(@Nullable String classIdentifier) {

			super(NullValue.class);
			this.classIdentifier = StringUtils.hasText(classIdentifier) ? classIdentifier : "@class";
		}

		@Override
		public void serialize(NullValue value, JsonGenerator jsonGenerator, SerializerProvider provider)
				throws IOException {

			jsonGenerator.writeStartObject();
			jsonGenerator.writeStringField(classIdentifier, NullValue.class.getName());
			jsonGenerator.writeEndObject();
		}

		@Override
		public void serializeWithType(NullValue value, JsonGenerator jsonGenerator, SerializerProvider serializers,
				TypeSerializer typeSerializer) throws IOException {

			serialize(value, jsonGenerator, serializers);
		}
	}

	/**
	 * Custom {@link StdTypeResolverBuilder} that considers typing for non-primitive types. Primitives, their wrappers and
	 * primitive arrays do not require type hints. The default {@code DefaultTyping#EVERYTHING} typing does not satisfy
	 * those requirements.
	 *
	 * @author Mark Paluch
	 * @since 2.7.2
	 */
	private static class TypeResolverBuilder extends ObjectMapper.DefaultTypeResolverBuilder {

		static TypeResolverBuilder forEverything(ObjectMapper mapper) {
			return new TypeResolverBuilder(DefaultTyping.EVERYTHING, mapper.getPolymorphicTypeValidator());
		}

		public TypeResolverBuilder(DefaultTyping typing, PolymorphicTypeValidator polymorphicTypeValidator) {
			super(typing, polymorphicTypeValidator);
		}

		@Override
		public ObjectMapper.DefaultTypeResolverBuilder withDefaultImpl(Class<?> defaultImpl) {
			return this;
		}

		/**
		 * Method called to check if the default type handler should be used for given type. Note: "natural types" (String,
		 * Boolean, Integer, Double) will never use typing; that is both due to them being concrete and final, and since
		 * actual serializers and deserializers will also ignore any attempts to enforce typing.
		 */
		public boolean useForType(JavaType javaType) {

			if (javaType.isJavaLangObject()) {
				return true;
			}

			javaType = resolveArrayOrWrapper(javaType);

			if (javaType.isEnumType() || ClassUtils.isPrimitiveOrWrapper(javaType.getRawClass())) {
				return false;
			}

			if (javaType.isFinal() && !KotlinDetector.isKotlinType(javaType.getRawClass())
					&& javaType.getRawClass().getPackageName().startsWith("java")) {
				return false;
			}

			// [databind#88] Should not apply to JSON tree models:
			return !TreeNode.class.isAssignableFrom(javaType.getRawClass());
		}

		private JavaType resolveArrayOrWrapper(JavaType type) {

			while (type.isArrayType()) {
				type = type.getContentType();
				if (type.isReferenceType()) {
					type = resolveArrayOrWrapper(type);
				}
			}

			while (type.isReferenceType()) {
				type = type.getReferencedType();
				if (type.isArrayType()) {
					type = resolveArrayOrWrapper(type);
				}
			}

			return type;
		}
	}
}
