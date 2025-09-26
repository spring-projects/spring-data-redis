/*
 * Copyright 2015-2025 the original author or authors.
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

import org.jspecify.annotations.Nullable;

import org.springframework.cache.support.NullValue;
import org.springframework.core.KotlinDetector;
import org.springframework.data.util.Lazy;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.As;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.DeserializationConfig;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectMapper.DefaultTyping;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.BeanDeserializerFactory;
import com.fasterxml.jackson.databind.deser.DefaultDeserializationContext;
import com.fasterxml.jackson.databind.deser.std.JsonNodeDeserializer;
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
 * {@literal JSON} reading and writing can be customized by configuring a {@link Jackson2ObjectReader} and
 * {@link Jackson2ObjectWriter}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Mao Shuai
 * @author John Blum
 * @author Anne Lee
 * @see Jackson2ObjectReader
 * @see Jackson2ObjectWriter
 * @see com.fasterxml.jackson.databind.ObjectMapper
 * @since 1.6
 * @deprecated since 4.0 in favor of {@link GenericJacksonJsonRedisSerializer}
 */
@SuppressWarnings("removal")
@Deprecated(since = "4.0", forRemoval = true)
public class GenericJackson2JsonRedisSerializer implements RedisSerializer<Object> {

	private final Jackson2ObjectReader reader;

	private final Jackson2ObjectWriter writer;

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
	 * @param typeHintPropertyName {@link String name} of the JSON property holding type information; can be
	 *          {@literal null}.
	 * @see ObjectMapper#activateDefaultTypingAsProperty(PolymorphicTypeValidator, DefaultTyping, String)
	 * @see ObjectMapper#activateDefaultTyping(PolymorphicTypeValidator, DefaultTyping, As)
	 */
	public GenericJackson2JsonRedisSerializer(@Nullable String typeHintPropertyName) {
		this(typeHintPropertyName, Jackson2ObjectReader.create(), Jackson2ObjectWriter.create());
	}

	/**
	 * Creates {@link GenericJackson2JsonRedisSerializer} initialized with an {@link ObjectMapper} configured for default
	 * typing using the given {@link String name} along with the given, required {@link Jackson2ObjectReader} and
	 * {@link Jackson2ObjectWriter} used to read/write {@link Object Objects} de/serialized as JSON.
	 * <p>
	 * In case {@link String name} is {@literal empty} or {@literal null}, then {@link JsonTypeInfo.Id#CLASS} will be
	 * used.
	 *
	 * @param typeHintPropertyName {@link String name} of the JSON property holding type information; can be
	 *          {@literal null}.
	 * @param reader {@link Jackson2ObjectReader} function to read objects using {@link ObjectMapper}.
	 * @param writer {@link Jackson2ObjectWriter} function to write objects using {@link ObjectMapper}.
	 * @see ObjectMapper#activateDefaultTypingAsProperty(PolymorphicTypeValidator, DefaultTyping, String)
	 * @see ObjectMapper#activateDefaultTyping(PolymorphicTypeValidator, DefaultTyping, As)
	 * @since 3.0
	 */
	public GenericJackson2JsonRedisSerializer(@Nullable String typeHintPropertyName, Jackson2ObjectReader reader,
			Jackson2ObjectWriter writer) {

		this(new ObjectMapper(), reader, writer, typeHintPropertyName);

		registerNullValueSerializer(this.mapper, typeHintPropertyName);

		this.mapper.setDefaultTyping(createDefaultTypeResolverBuilder(getObjectMapper(), typeHintPropertyName));
	}

	/**
	 * Setting a custom-configured {@link ObjectMapper} is one way to take further control of the JSON serialization
	 * process. For example, an extended {@link SerializerFactory} can be configured that provides custom serializers for
	 * specific types.
	 *
	 * @param mapper must not be {@literal null}.
	 */
	public GenericJackson2JsonRedisSerializer(ObjectMapper mapper) {
		this(mapper, Jackson2ObjectReader.create(), Jackson2ObjectWriter.create());
	}

	/**
	 * Setting a custom-configured {@link ObjectMapper} is one way to take further control of the JSON serialization
	 * process. For example, an extended {@link SerializerFactory} can be configured that provides custom serializers for
	 * specific types.
	 *
	 * @param mapper must not be {@literal null}.
	 * @param reader the {@link Jackson2ObjectReader} function to read objects using {@link ObjectMapper}.
	 * @param writer the {@link Jackson2ObjectWriter} function to write objects using {@link ObjectMapper}.
	 * @since 3.0
	 */
	public GenericJackson2JsonRedisSerializer(ObjectMapper mapper, Jackson2ObjectReader reader,
			Jackson2ObjectWriter writer) {
		this(mapper, reader, writer, null);
	}

	private GenericJackson2JsonRedisSerializer(ObjectMapper mapper, Jackson2ObjectReader reader,
			Jackson2ObjectWriter writer, @Nullable String typeHintPropertyName) {

		Assert.notNull(mapper, "ObjectMapper must not be null");
		Assert.notNull(reader, "Reader must not be null");
		Assert.notNull(writer, "Writer must not be null");

		this.mapper = mapper;
		this.reader = reader;
		this.writer = writer;

		this.defaultTypingEnabled = Lazy.of(() -> mapper.getSerializationConfig().getDefaultTyper(null) != null);

		this.typeResolver = newTypeResolver(mapper, typeHintPropertyName, this.defaultTypingEnabled);
	}

	private static TypeResolver newTypeResolver(ObjectMapper mapper, @Nullable String typeHintPropertyName,
			Lazy<Boolean> defaultTypingEnabled) {

		Lazy<TypeFactory> lazyTypeFactory = Lazy.of(mapper::getTypeFactory);

		Lazy<String> lazyTypeHintPropertyName = typeHintPropertyName != null ? Lazy.of(typeHintPropertyName)
				: newLazyTypeHintPropertyName(mapper, defaultTypingEnabled);

		return new TypeResolver(mapper, lazyTypeFactory, lazyTypeHintPropertyName);
	}

	private static Lazy<String> newLazyTypeHintPropertyName(ObjectMapper mapper, Lazy<Boolean> defaultTypingEnabled) {

		Lazy<String> configuredTypeDeserializationPropertyName = getConfiguredTypeDeserializationPropertyName(mapper);

		Lazy<String> resolvedLazyTypeHintPropertyName = Lazy
				.of(() -> defaultTypingEnabled.get() ? null : configuredTypeDeserializationPropertyName.get());

		return resolvedLazyTypeHintPropertyName.or("@class");
	}

	private static Lazy<String> getConfiguredTypeDeserializationPropertyName(ObjectMapper mapper) {

		return Lazy.of(() -> {

			DeserializationConfig deserializationConfig = mapper.getDeserializationConfig();

			JavaType objectType = mapper.getTypeFactory().constructType(Object.class);

			TypeDeserializer typeDeserializer = deserializationConfig.getDefaultTyper(null)
					.buildTypeDeserializer(deserializationConfig, objectType, Collections.emptyList());

			return typeDeserializer.getPropertyName();
		});
	}

	private static StdTypeResolverBuilder createDefaultTypeResolverBuilder(ObjectMapper objectMapper,
			@Nullable String typeHintPropertyName) {

		StdTypeResolverBuilder typer = TypeResolverBuilder.forEverything(objectMapper).init(JsonTypeInfo.Id.CLASS, null)
				.inclusion(As.PROPERTY);

		if (StringUtils.hasText(typeHintPropertyName)) {
			typer = typer.typeProperty(typeHintPropertyName);
		}
		return typer;
	}

	/**
	 * Factory method returning a {@literal Builder} used to construct and configure a
	 * {@link GenericJackson2JsonRedisSerializer}.
	 *
	 * @return new {@link GenericJackson2JsonRedisSerializer.GenericJackson2JsonRedisSerializerBuilder}.
	 * @since 3.3.1
	 */
	public static GenericJackson2JsonRedisSerializerBuilder builder() {
		return new GenericJackson2JsonRedisSerializerBuilder();
	}

	/**
	 * Register {@link NullValueSerializer} in the given {@link ObjectMapper} with an optional
	 * {@code typeHintPropertyName}. This method should be called by code that customizes
	 * {@link GenericJackson2JsonRedisSerializer} by providing an external {@link ObjectMapper}.
	 *
	 * @param objectMapper the object mapper to customize.
	 * @param typeHintPropertyName name of the type property. Defaults to {@code @class} if {@literal null}/empty.
	 * @since 2.2
	 */
	public static void registerNullValueSerializer(ObjectMapper objectMapper, @Nullable String typeHintPropertyName) {

		// Simply setting {@code mapper.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS)} does not help here
		// since we need the type hint embedded for deserialization using the default typing feature.
		objectMapper.registerModule(new SimpleModule().addSerializer(new NullValueSerializer(typeHintPropertyName)));
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
		} catch (IOException ex) {
			throw new SerializationException("Could not write JSON: %s".formatted(ex.getMessage()), ex);
		}
	}

	@Override
	public @Nullable Object deserialize(byte @Nullable[] source) throws SerializationException {
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
	@SuppressWarnings("unchecked")
	public <T> @Nullable T deserialize(byte @Nullable[] source, Class<T> type) throws SerializationException {

		Assert.notNull(type, "Deserialization type must not be null;"
				+ " Please provide Object.class to make use of Jackson2 default typing.");

		if (SerializationUtils.isEmpty(source)) {
			return null;
		}

		try {
			return (T) reader.read(mapper, source, resolveType(source, type));
		} catch (Exception ex) {
			throw new SerializationException("Could not read JSON:%s ".formatted(ex.getMessage()), ex);
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

		private final ObjectMapper mapper;
		private final Supplier<TypeFactory> typeFactory;
		private final Supplier<String> hintName;

		TypeResolver(ObjectMapper mapper, Supplier<TypeFactory> typeFactory, Supplier<String> hintName) {

			this.mapper = mapper;
			this.typeFactory = typeFactory;
			this.hintName = hintName;
		}

		protected JavaType constructType(Class<?> type) {
			return typeFactory.get().constructType(type);
		}

		protected JavaType resolveType(byte[] source, Class<?> type) throws IOException {

			JsonNode root = readTree(source);
			JsonNode jsonNode = root.get(hintName.get());

			if (jsonNode instanceof TextNode && jsonNode.asText() != null) {
				return typeFactory.get().constructFromCanonical(jsonNode.asText());
			}

			return constructType(type);
		}

		/**
		 * Lenient variant of ObjectMapper._readTreeAndClose using a strict {@link JsonNodeDeserializer}.
		 */
		private JsonNode readTree(byte[] source) throws IOException {

			JsonDeserializer<? extends JsonNode> deserializer = JsonNodeDeserializer.getDeserializer(JsonNode.class);
			DeserializationConfig cfg = mapper.getDeserializationConfig();

			try (JsonParser parser = createParser(source, cfg)) {

				JsonToken t = parser.currentToken();
				if (t == null) {
					t = parser.nextToken();
					if (t == null) {
						return cfg.getNodeFactory().missingNode();
					}
				}

				/*
				 * Hokey pokey! Oh my.
				 */
				DefaultDeserializationContext ctxt = new DefaultDeserializationContext.Impl(BeanDeserializerFactory.instance)
						.createInstance(cfg, parser, mapper.getInjectableValues());
				if (t == JsonToken.VALUE_NULL) {
					return cfg.getNodeFactory().nullNode();
				} else {
					return deserializer.deserialize(parser, ctxt);
				}
			}
		}

		private JsonParser createParser(byte[] source, DeserializationConfig cfg) throws IOException {

			JsonParser parser = mapper.createParser(source);
			cfg.initialize(parser);
			return parser;
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

		@Serial private static final long serialVersionUID = 1999052150548658808L;

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
	 * Builder for configuring and creating a {@link GenericJackson2JsonRedisSerializer}.
	 *
	 * @author Anne Lee
	 * @author Mark Paluch
	 * @since 3.3.1
	 */
	public static class GenericJackson2JsonRedisSerializerBuilder {

		private @Nullable String typeHintPropertyName;

		private Jackson2ObjectReader reader = Jackson2ObjectReader.create();

		private Jackson2ObjectWriter writer = Jackson2ObjectWriter.create();

		private @Nullable ObjectMapper objectMapper;

		private @Nullable Boolean defaultTyping;

		private boolean registerNullValueSerializer = true;

		private @Nullable StdSerializer<NullValue> nullValueSerializer;

		private GenericJackson2JsonRedisSerializerBuilder() {}

		/**
		 * Enable or disable default typing. Enabling default typing will override
		 * {@link ObjectMapper#setDefaultTyping(com.fasterxml.jackson.databind.jsontype.TypeResolverBuilder)} for a given
		 * {@link ObjectMapper}. Default typing is enabled by default if no {@link ObjectMapper} is provided.
		 *
		 * @param defaultTyping whether to enable/disable default typing. Enabled by default if the {@link ObjectMapper} is
		 *          not provided.
		 * @return this {@link GenericJackson2JsonRedisSerializer.GenericJackson2JsonRedisSerializerBuilder}.
		 */
		public GenericJackson2JsonRedisSerializerBuilder defaultTyping(boolean defaultTyping) {
			this.defaultTyping = defaultTyping;
			return this;
		}

		/**
		 * Configure a property name to that represents the type hint.
		 *
		 * @param typeHintPropertyName {@link String name} of the JSON property holding type information.
		 * @return this {@link GenericJackson2JsonRedisSerializer.GenericJackson2JsonRedisSerializerBuilder}.
		 */
		public GenericJackson2JsonRedisSerializerBuilder typeHintPropertyName(String typeHintPropertyName) {

			Assert.hasText(typeHintPropertyName, "Type hint property name must bot be null or empty");

			this.typeHintPropertyName = typeHintPropertyName;
			return this;
		}

		/**
		 * Configure a provided {@link ObjectMapper}. Note that the provided {@link ObjectMapper} can be reconfigured with a
		 * {@link #nullValueSerializer} or default typing depending on the builder configuration.
		 *
		 * @param objectMapper must not be {@literal null}.
		 * @return this {@link GenericJackson2JsonRedisSerializer.GenericJackson2JsonRedisSerializerBuilder}.
		 */
		public GenericJackson2JsonRedisSerializerBuilder objectMapper(ObjectMapper objectMapper) {

			Assert.notNull(objectMapper, "ObjectMapper must not be null");

			this.objectMapper = objectMapper;
			return this;
		}

		/**
		 * Configure {@link Jackson2ObjectReader}.
		 *
		 * @param reader must not be {@literal null}.
		 * @return this {@link GenericJackson2JsonRedisSerializer.GenericJackson2JsonRedisSerializerBuilder}.
		 */
		public GenericJackson2JsonRedisSerializerBuilder reader(Jackson2ObjectReader reader) {

			Assert.notNull(reader, "JacksonObjectReader must not be null");

			this.reader = reader;
			return this;
		}

		/**
		 * Configure {@link Jackson2ObjectWriter}.
		 *
		 * @param writer must not be {@literal null}.
		 * @return this {@link GenericJackson2JsonRedisSerializer.GenericJackson2JsonRedisSerializerBuilder}.
		 */
		public GenericJackson2JsonRedisSerializerBuilder writer(Jackson2ObjectWriter writer) {

			Assert.notNull(writer, "JacksonObjectWriter must not be null");

			this.writer = writer;
			return this;
		}

		/**
		 * Register a {@link StdSerializer serializer} for {@link NullValue}.
		 *
		 * @param nullValueSerializer the {@link StdSerializer} to use for {@link NullValue} serialization, must not be
		 *          {@literal null}.
		 * @return this {@link GenericJackson2JsonRedisSerializer.GenericJackson2JsonRedisSerializerBuilder}.
		 */
		public GenericJackson2JsonRedisSerializerBuilder nullValueSerializer(StdSerializer<NullValue> nullValueSerializer) {

			Assert.notNull(nullValueSerializer, "Null value serializer must not be null");

			this.nullValueSerializer = nullValueSerializer;
			return this;
		}

		/**
		 * Configure whether to register a {@link StdSerializer serializer} for {@link NullValue} serialization. The default
		 * serializer considers {@link #typeHintPropertyName(String)}.
		 *
		 * @param registerNullValueSerializer {@code true} to register the default serializer; {@code false} otherwise.
		 * @return this {@link GenericJackson2JsonRedisSerializer.GenericJackson2JsonRedisSerializerBuilder}.
		 */
		public GenericJackson2JsonRedisSerializerBuilder registerNullValueSerializer(boolean registerNullValueSerializer) {
			this.registerNullValueSerializer = registerNullValueSerializer;
			return this;
		}

		/**
		 * Creates a new instance of {@link GenericJackson2JsonRedisSerializer} with configuration options applied. Creates
		 * also a new {@link ObjectMapper} if none was provided.
		 *
		 * @return a new instance of {@link GenericJackson2JsonRedisSerializer}.
		 */
		public GenericJackson2JsonRedisSerializer build() {

			ObjectMapper objectMapper = this.objectMapper;
			boolean providedObjectMapper = objectMapper != null;

			if (objectMapper == null) {
				objectMapper = new ObjectMapper();
			}

			if (registerNullValueSerializer) {
				objectMapper.registerModule(new SimpleModule("GenericJackson2JsonRedisSerializerBuilder")
						.addSerializer(this.nullValueSerializer != null ? this.nullValueSerializer
								: new NullValueSerializer(this.typeHintPropertyName)));
			}

			if ((!providedObjectMapper && (defaultTyping == null || defaultTyping))
					|| (defaultTyping != null && defaultTyping)) {
				objectMapper.setDefaultTyping(createDefaultTypeResolverBuilder(objectMapper, typeHintPropertyName));
			}

			return new GenericJackson2JsonRedisSerializer(objectMapper, this.reader, this.writer, this.typeHintPropertyName);
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
		@Override
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
