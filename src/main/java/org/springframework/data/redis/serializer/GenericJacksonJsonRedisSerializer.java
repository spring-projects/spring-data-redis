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

import tools.jackson.core.JacksonException;
import tools.jackson.core.JsonGenerator;
import tools.jackson.core.JsonParser;
import tools.jackson.core.JsonToken;
import tools.jackson.core.TreeNode;
import tools.jackson.core.Version;
import tools.jackson.databind.DefaultTyping;
import tools.jackson.databind.DeserializationConfig;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.JacksonModule;
import tools.jackson.databind.JavaType;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.SerializationContext;
import tools.jackson.databind.cfg.MapperBuilder;
import tools.jackson.databind.deser.jackson.BaseNodeDeserializer;
import tools.jackson.databind.deser.jackson.JsonNodeDeserializer;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.jsontype.BasicPolymorphicTypeValidator;
import tools.jackson.databind.jsontype.PolymorphicTypeValidator;
import tools.jackson.databind.jsontype.TypeDeserializer;
import tools.jackson.databind.jsontype.TypeSerializer;
import tools.jackson.databind.jsontype.impl.DefaultTypeResolverBuilder;
import tools.jackson.databind.jsontype.impl.StdTypeResolverBuilder;
import tools.jackson.databind.module.SimpleSerializers;
import tools.jackson.databind.ser.std.StdSerializer;
import tools.jackson.databind.type.TypeFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.jspecify.annotations.Nullable;

import org.springframework.cache.support.NullValue;
import org.springframework.core.KotlinDetector;
import org.springframework.data.util.Lazy;
import org.springframework.lang.Contract;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Generic Jackson 3-based {@link RedisSerializer} that maps {@link Object objects} to and from {@literal JSON}.
 * <p>
 * {@literal JSON} reading and writing can be customized by configuring a {@link JacksonObjectReader} and
 * {@link JacksonObjectWriter}.
 *
 * @author Christoph Strobl
 * @see JacksonObjectReader
 * @see JacksonObjectWriter
 * @see ObjectMapper
 * @since 4.0
 */
public class GenericJacksonJsonRedisSerializer implements RedisSerializer<Object> {

	private final JacksonObjectReader reader;

	private final JacksonObjectWriter writer;

	private final Lazy<Boolean> defaultTypingEnabled;

	private final ObjectMapper mapper;

	private final TypeResolver typeResolver;

	/**
	 * Create a {@link GenericJacksonJsonRedisSerializer} with a custom-configured {@link ObjectMapper}.
	 *
	 * @param mapper must not be {@literal null}.
	 */
	public GenericJacksonJsonRedisSerializer(ObjectMapper mapper) {
		this(mapper, JacksonObjectReader.create(), JacksonObjectWriter.create());
	}

	/**
	 * Create a {@link GenericJacksonJsonRedisSerializer} with a custom-configured {@link ObjectMapper} considering
	 * potential Object/{@link JacksonObjectReader -reader} and {@link JacksonObjectWriter -writer}.
	 *
	 * @param mapper must not be {@literal null}.
	 * @param reader the {@link JacksonObjectReader} function to read objects using {@link ObjectMapper}.
	 * @param writer the {@link JacksonObjectWriter} function to write objects using {@link ObjectMapper}.
	 */
	protected GenericJacksonJsonRedisSerializer(ObjectMapper mapper, JacksonObjectReader reader,
			JacksonObjectWriter writer) {

		Assert.notNull(mapper, "ObjectMapper must not be null");
		Assert.notNull(reader, "Reader must not be null");
		Assert.notNull(writer, "Writer must not be null");

		this.mapper = mapper;
		this.reader = reader;
		this.writer = writer;

		this.defaultTypingEnabled = Lazy.of(() -> mapper.serializationConfig().getDefaultTyper(null) != null);

		Lazy<String> lazyTypeHintPropertyName = newLazyTypeHintPropertyName(mapper, this.defaultTypingEnabled);
		this.typeResolver = newTypeResolver(mapper, lazyTypeHintPropertyName);
	}

	/**
	 * Prepare a new {@link GenericJacksonJsonRedisSerializer} instance.
	 *
	 * @param configurer the configurer for {@link GenericJacksonJsonRedisSerializerBuilder}.
	 * @return new instance of {@link GenericJacksonJsonRedisSerializer}.
	 */
	public static GenericJacksonJsonRedisSerializer create(
			Consumer<GenericJacksonJsonRedisSerializerBuilder<JsonMapper.Builder>> configurer) {

		Assert.notNull(configurer, "Builder configurer must not be null");

		GenericJacksonJsonRedisSerializerBuilder<JsonMapper.Builder> builder = builder();
		configurer.accept(builder);
		return builder.build();
	}

	/**
	 * Creates a new {@link GenericJacksonJsonRedisSerializerBuilder} to configure and build a
	 * {@link GenericJacksonJsonRedisSerializer} using {@link JsonMapper}.
	 *
	 * @return a new {@link GenericJacksonJsonRedisSerializerBuilder}.
	 */
	public static GenericJacksonJsonRedisSerializerBuilder<JsonMapper.Builder> builder() {
		return builder(JsonMapper::builder);
	}

	/**
	 * Creates a new {@link GenericJacksonJsonRedisSerializerBuilder} to configure and build a
	 * {@link GenericJacksonJsonRedisSerializer}.
	 *
	 * @param builderFactory factory to create a {@link MapperBuilder} for the {@link ObjectMapper}.
	 * @param <B> type of the {@link MapperBuilder} to use.
	 * @return a new {@link GenericJacksonJsonRedisSerializerBuilder}.
	 */
	public static <B extends MapperBuilder<? extends ObjectMapper, ? extends MapperBuilder<?, ?>>> GenericJacksonJsonRedisSerializerBuilder<B> builder(
			Supplier<B> builderFactory) {

		Assert.notNull(builderFactory, "MapperBuilder Factory must not be null");

		return new GenericJacksonJsonRedisSerializerBuilder<>(builderFactory);
	}

	@Override
	@Contract("_ -> !null")
	public byte[] serialize(@Nullable Object value) throws SerializationException {

		if (value == null) {
			return SerializationUtils.EMPTY_ARRAY;
		}

		try {
			return writer.write(mapper, value);
		} catch (RuntimeException ex) {
			throw new SerializationException("Could not write JSON: %s".formatted(ex.getMessage()), ex);
		}
	}

	@Override
	@Contract("null -> null")
	public @Nullable Object deserialize(byte @Nullable [] source) throws SerializationException {
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
	@Contract("null, _ -> null")
	public <T> @Nullable T deserialize(byte @Nullable [] source, Class<T> type) throws SerializationException {

		Assert.notNull(type, "Deserialization type must not be null;"
				+ " Please provide Object.class to make use of Jackson3 default typing.");

		if (SerializationUtils.isEmpty(source)) {
			return null;
		}

		try {
			return (T) reader.read(mapper, source, resolveType(source, type));
		} catch (Exception ex) {
			throw new SerializationException("Could not read JSON:%s ".formatted(ex.getMessage()), ex);
		}
	}

	protected JavaType resolveType(byte[] source, Class<?> type) throws IOException {

		if (!type.equals(Object.class) || !defaultTypingEnabled.get()) {
			return typeResolver.constructType(type);
		}

		return typeResolver.resolveType(source, type);
	}

	private static TypeResolver newTypeResolver(ObjectMapper mapper, Lazy<String> typeHintPropertyName) {

		Lazy<TypeFactory> lazyTypeFactory = Lazy.of(mapper::getTypeFactory);
		return new TypeResolver(mapper, lazyTypeFactory, typeHintPropertyName);
	}

	private static Lazy<String> newLazyTypeHintPropertyName(ObjectMapper mapper, Lazy<Boolean> defaultTypingEnabled) {

		Lazy<String> configuredTypeDeserializationPropertyName = getConfiguredTypeDeserializationPropertyName(mapper);

		Lazy<String> resolvedLazyTypeHintPropertyName = Lazy
				.of(() -> defaultTypingEnabled.get() ? configuredTypeDeserializationPropertyName.get() : null);

		return resolvedLazyTypeHintPropertyName.or("@class");
	}

	private static Lazy<String> getConfiguredTypeDeserializationPropertyName(ObjectMapper mapper) {

		return Lazy.of(() -> {

			DeserializationConfig deserializationConfig = mapper.deserializationConfig();

			JavaType objectType = mapper.getTypeFactory().constructType(Object.class);

			TypeDeserializer typeDeserializer = deserializationConfig.getDefaultTyper(null)
					.buildTypeDeserializer(mapper._deserializationContext(), objectType, Collections.emptyList());

			return typeDeserializer.getPropertyName();
		});
	}

	/**
	 * {@link GenericJacksonJsonRedisSerializerBuilder} wraps around a {@link JsonMapper.Builder} providing dedicated
	 * methods to configure aspects like {@link NullValue} serialization strategy for the resulting {@link ObjectMapper}
	 * to be used with {@link GenericJacksonJsonRedisSerializer} as well as potential Object/{@link JacksonObjectReader
	 * -reader} and {@link JacksonObjectWriter -writer} settings.
	 *
	 * @param <B> type of the {@link MapperBuilder}.
	 */
	public static class GenericJacksonJsonRedisSerializerBuilder<B extends MapperBuilder<? extends ObjectMapper, ? extends MapperBuilder<?, ?>>> {

		private final Supplier<B> builderFactory;

		private boolean cacheNullValueSupportEnabled = false;
		private boolean defaultTyping = false;
		private @Nullable String typePropertyName;
		private PolymorphicTypeValidator typeValidator = BasicPolymorphicTypeValidator.builder()
				.allowIfBaseType(Object.class).allowIfSubType((ctx, clazz) -> true).build();
		private Consumer<B> mapperBuilderCustomizer = (b) -> {};
		private JacksonObjectWriter writer = JacksonObjectWriter.create();
		private JacksonObjectReader reader = JacksonObjectReader.create();

		private GenericJacksonJsonRedisSerializerBuilder(Supplier<B> builderFactory) {
			this.builderFactory = builderFactory;
		}

		/**
		 * Registers a {@link StdSerializer} capable of serializing Spring Cache {@link NullValue} using the mappers default
		 * type property. Please make sure to active
		 * {@link JsonMapper.Builder#activateDefaultTypingAsProperty(PolymorphicTypeValidator, DefaultTyping, String)
		 * default typing} accordingly.
		 *
		 * @return this.
		 */
		@Contract("-> this")
		public GenericJacksonJsonRedisSerializerBuilder<B> enableSpringCacheNullValueSupport() {

			this.cacheNullValueSupportEnabled = true;
			return this;
		}

		/**
		 * Registers a {@link StdSerializer} capable of serializing Spring Cache {@link NullValue} using the given type
		 * property name. Please make sure to active
		 * {@link JsonMapper.Builder#activateDefaultTypingAsProperty(PolymorphicTypeValidator, DefaultTyping, String)
		 * default typing} accordingly.
		 *
		 * @return {@code this} builder.
		 */
		@Contract("_ -> this")
		public GenericJacksonJsonRedisSerializerBuilder<B> enableSpringCacheNullValueSupport(String typePropertyName) {

			typePropertyName(typePropertyName);
			return enableSpringCacheNullValueSupport();
		}

		/**
		 * Enables
		 * {@link JsonMapper.Builder#activateDefaultTypingAsProperty(PolymorphicTypeValidator, DefaultTyping, String)
		 * default typing} without any type validation constraints.
		 * <p>
		 * <strong>WARNING</strong>: without restrictions of the {@link PolymorphicTypeValidator} deserialization is
		 * vulnerable to arbitrary code execution when reading from untrusted sources.
		 *
		 * @return {@code this} builder.
		 * @see <a href=
		 *      "https://owasp.org/www-community/vulnerabilities/Deserialization_of_untrusted_data">https://owasp.org/www-community/vulnerabilities/Deserialization_of_untrusted_data</a>
		 */
		@Contract("-> this")
		public GenericJacksonJsonRedisSerializerBuilder<B> enableUnsafeDefaultTyping() {

			this.defaultTyping = true;
			return this;
		}

		/**
		 * Enables
		 * {@link JsonMapper.Builder#activateDefaultTypingAsProperty(PolymorphicTypeValidator, DefaultTyping, String)
		 * default typing} using the given {@link PolymorphicTypeValidator}.
		 *
		 * @return {@code this} builder.
		 */
		@Contract("_ -> this")
		public GenericJacksonJsonRedisSerializerBuilder<B> enableDefaultTyping(PolymorphicTypeValidator typeValidator) {

			typeValidator(typeValidator);

			this.defaultTyping = true;
			return this;
		}

		/**
		 * Provide a {@link PolymorphicTypeValidator} to validate polymorphic types during deserialization.
		 *
		 * @param typeValidator the validator to use, defaults to a permissive validator that allows all types.
		 * @return {@code this} builder.
		 */
		@Contract("_ -> this")
		public GenericJacksonJsonRedisSerializerBuilder<B> typeValidator(PolymorphicTypeValidator typeValidator) {

			Assert.notNull(typeValidator, "Type validator must not be null");

			this.typeValidator = typeValidator;
			return this;
		}

		/**
		 * Configure the type property name used for default typing and {@link #enableSpringCacheNullValueSupport()}.
		 *
		 * @return {@code this} builder.
		 */
		@Contract("_ -> this")
		public GenericJacksonJsonRedisSerializerBuilder<B> typePropertyName(String typePropertyName) {

			Assert.hasText(typePropertyName, "Property name must not be null or empty");

			this.typePropertyName = typePropertyName;
			return this;
		}

		/**
		 * Configures the {@link JacksonObjectWriter}.
		 *
		 * @param writer must not be {@literal null}.
		 * @return {@code this} builder.
		 */
		@Contract("_ -> this")
		public GenericJacksonJsonRedisSerializerBuilder<B> writer(JacksonObjectWriter writer) {

			Assert.notNull(writer, "Jackson3ObjectWriter must not be null");

			this.writer = writer;
			return this;
		}

		/**
		 * Configures the {@link JacksonObjectReader}.
		 *
		 * @param reader must not be {@literal null}.
		 * @return {@code this} builder.
		 */
		@Contract("_ -> this")
		public GenericJacksonJsonRedisSerializerBuilder<B> reader(JacksonObjectReader reader) {

			Assert.notNull(reader, "Jackson3ObjectReader must not be null");

			this.reader = reader;
			return this;
		}

		/**
		 * Provide a {@link Consumer customizer} to configure the {@link ObjectMapper} through its {@link MapperBuilder}.
		 *
		 * @param mapperBuilderCustomizer the configurer to apply to the {@link ObjectMapper} builder.
		 * @return {@code this} builder.
		 */
		@Contract("_ -> this")
		public GenericJacksonJsonRedisSerializerBuilder<B> customize(Consumer<B> mapperBuilderCustomizer) {

			Assert.notNull(mapperBuilderCustomizer, "JSON mapper configurer must not be null");

			this.mapperBuilderCustomizer = mapperBuilderCustomizer;
			return this;
		}

		/**
		 * Build a new {@link GenericJacksonJsonRedisSerializer} instance using the configured settings.
		 *
		 * @return a new {@link GenericJacksonJsonRedisSerializer} instance.
		 */
		@Contract("-> new")
		public GenericJacksonJsonRedisSerializer build() {

			B mapperBuilder = builderFactory.get();

			if (cacheNullValueSupportEnabled) {

				String typePropertyName = StringUtils.hasText(this.typePropertyName) ? this.typePropertyName : "@class";
				mapperBuilder.addModules(new GenericJacksonRedisSerializerModule(() -> {
					tools.jackson.databind.jsontype.TypeResolverBuilder<?> defaultTyper = mapperBuilder.baseSettings()
							.getDefaultTyper();
					if (defaultTyper instanceof StdTypeResolverBuilder stdTypeResolverBuilder) {
						return stdTypeResolverBuilder.getTypeProperty();
					}
					return typePropertyName;
				}));
			}

			if (defaultTyping) {

				GenericJacksonJsonRedisSerializer.TypeResolverBuilder resolver = new GenericJacksonJsonRedisSerializer.TypeResolverBuilder(
						typeValidator, DefaultTyping.NON_FINAL, JsonTypeInfo.As.PROPERTY, JsonTypeInfo.Id.CLASS, typePropertyName);

				mapperBuilder.configure(DeserializationFeature.FAIL_ON_MISSING_EXTERNAL_TYPE_ID_PROPERTY, false)
						.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
						.setDefaultTyping(resolver);
			}

			mapperBuilderCustomizer.accept(mapperBuilder);

			return new GenericJacksonJsonRedisSerializer(mapperBuilder.build(), reader, writer);
		}

	}

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

			if (jsonNode != null && jsonNode.isString() && jsonNode.asString() != null) {
				return typeFactory.get().constructFromCanonical(jsonNode.asString());
			}

			return constructType(type);
		}

		/**
		 * Lenient variant of ObjectMapper._readTreeAndClose using a strict {@link JsonNodeDeserializer}.
		 */
		private JsonNode readTree(byte[] source) throws IOException {

			BaseNodeDeserializer<?> deserializer = JsonNodeDeserializer.getDeserializer(JsonNode.class);
			DeserializationConfig cfg = mapper.deserializationConfig();

			try (JsonParser parser = createParser(source)) {

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
				DeserializationContext ctxt = mapper._deserializationContext();

				if (t == JsonToken.VALUE_NULL) {
					return cfg.getNodeFactory().nullNode();
				} else {
					return deserializer.deserialize(parser, ctxt);
				}
			}
		}

		private JsonParser createParser(byte[] source) throws IOException {
			return mapper.createParser(source);
		}
	}

	/**
	 * {@link StdSerializer} adding class information required by default typing. This allows de-/serialization of
	 * {@link NullValue}.
	 *
	 * @author Christoph Strobl
	 */
	private static class NullValueSerializer extends StdSerializer<NullValue> {

		private final Lazy<String> classIdentifier;

		/**
		 * @param classIdentifier can be {@literal null} and will be defaulted to {@code @class}.
		 */
		NullValueSerializer(Supplier<String> classIdentifier) {

			super(NullValue.class);

			this.classIdentifier = Lazy.of(() -> {
				String identifier = classIdentifier.get();
				return StringUtils.hasText(identifier) ? identifier : "@class";
			});
		}

		@Override
		public void serialize(NullValue value, JsonGenerator gen, SerializationContext provider) throws JacksonException {

			if (gen.canWriteTypeId()) {

				gen.writeTypeId(classIdentifier.get());
				gen.writeString(NullValue.class.getName());
			} else if (StringUtils.hasText(classIdentifier.get())) {

				gen.writeStartObject();
				gen.writeName(classIdentifier.get());
				gen.writeString(NullValue.class.getName());
				gen.writeEndObject();
			} else {
				gen.writeNull();
			}
		}

		@Override
		public void serializeWithType(NullValue value, JsonGenerator gen, SerializationContext ctxt, TypeSerializer typeSer)
				throws JacksonException {
			serialize(value, gen, ctxt);
		}
	}

	private static class GenericJacksonRedisSerializerModule extends JacksonModule {

		private final Supplier<String> classIdentifier;

		GenericJacksonRedisSerializerModule(Supplier<String> classIdentifier) {
			this.classIdentifier = classIdentifier;
		}

		@Override
		public String getModuleName() {
			return "spring-data-redis-generic-serializer-module";
		}

		@Override
		public Version version() {
			return new Version(4, 0, 0, null, "org.springframework.data", "spring-data-redis");
		}

		@Override
		public void setupModule(SetupContext context) {
			context.addSerializers(new SimpleSerializers().addSerializer(new NullValueSerializer(classIdentifier)));
		}

	}

	private static class TypeResolverBuilder extends DefaultTypeResolverBuilder {

		public TypeResolverBuilder(PolymorphicTypeValidator subtypeValidator, DefaultTyping t, JsonTypeInfo.As includeAs) {
			super(subtypeValidator, t, includeAs);
		}

		public TypeResolverBuilder(PolymorphicTypeValidator subtypeValidator, DefaultTyping t, String propertyName) {
			super(subtypeValidator, t, propertyName);
		}

		public TypeResolverBuilder(PolymorphicTypeValidator subtypeValidator, DefaultTyping t, JsonTypeInfo.As includeAs,
				JsonTypeInfo.Id idType, @Nullable String propertyName) {
			super(subtypeValidator, t, includeAs, idType, propertyName);
		}

		@Override
		public DefaultTypeResolverBuilder withDefaultImpl(Class<?> defaultImpl) {
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
