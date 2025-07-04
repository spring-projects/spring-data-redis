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
import tools.jackson.core.Version;
import tools.jackson.core.exc.JacksonIOException;
import tools.jackson.databind.DefaultTyping;
import tools.jackson.databind.DeserializationConfig;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.JacksonModule;
import tools.jackson.databind.JavaType;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.SerializationContext;
import tools.jackson.databind.ValueSerializer;
import tools.jackson.databind.deser.jackson.BaseNodeDeserializer;
import tools.jackson.databind.deser.jackson.JsonNodeDeserializer;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.json.JsonMapper.Builder;
import tools.jackson.databind.jsontype.BasicPolymorphicTypeValidator;
import tools.jackson.databind.jsontype.PolymorphicTypeValidator;
import tools.jackson.databind.jsontype.TypeDeserializer;
import tools.jackson.databind.jsontype.TypeResolverBuilder;
import tools.jackson.databind.jsontype.impl.StdTypeResolverBuilder;
import tools.jackson.databind.module.SimpleSerializers;
import tools.jackson.databind.ser.std.StdSerializer;
import tools.jackson.databind.type.TypeFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.jspecify.annotations.Nullable;
import org.springframework.cache.support.NullValue;
import org.springframework.data.util.Lazy;
import org.springframework.lang.Contract;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Generic Jackson 2-based {@link RedisSerializer} that maps {@link Object objects} to and from {@literal JSON}.
 * <p>
 * {@literal JSON} reading and writing can be customized by configuring a {@link Jackson3ObjectReader} and
 * {@link Jackson3ObjectWriter}.
 *
 * @author Christoph Strobl
 * @see Jackson3ObjectReader
 * @see Jackson3ObjectWriter
 * @see ObjectMapper
 * @since 4.0
 */
public class GenericJackson3JsonRedisSerializer implements RedisSerializer<Object> {

	private final Jackson3ObjectReader reader;

	private final Jackson3ObjectWriter writer;

	private final Lazy<Boolean> defaultTypingEnabled;

	private final Lazy<String> lazyTypeHintPropertyName;

	private final ObjectMapper mapper;

	private final TypeResolver typeResolver;

	// internal shortcut for testing
	GenericJackson3JsonRedisSerializer() {
		this(((Supplier<ObjectMapper>) () -> {
			Builder builder = JsonMapper.builder();
			new JsonMapperConfigurer(builder).unsafeDefaultTyping().enableSpringCacheNullValueSupport();
			return builder.build();
		}).get());
	}

	/**
	 * Prepare a new {@link GenericJackson3JsonRedisSerializer} instance.
	 *
	 * @param configurer configuration helper callback to apply customizations to the {@link JsonMapper.Builder json
	 *          mapper}.
	 * @return new instance of {@link GenericJackson3JsonRedisSerializer}.
	 */
	public static GenericJackson3JsonRedisSerializer create(Consumer<JsonMapperConfigurer> configurer) {

		Builder configurationBuilder = JsonMapper.builder();
		JsonMapperConfigurer configHelper = new JsonMapperConfigurer(configurationBuilder);
		configurer.accept(configHelper);
		return new GenericJackson3JsonRedisSerializer(configurationBuilder.build(), configHelper.getReader(),
				configHelper.getWriter());
	}

	/**
	 * Create a {@link GenericJackson3JsonRedisSerializer} with a custom-configured {@link ObjectMapper}.
	 *
	 * @param mapper must not be {@literal null}.
	 */
	public GenericJackson3JsonRedisSerializer(ObjectMapper mapper) {
		this(mapper, Jackson3ObjectReader.create(), Jackson3ObjectWriter.create());
	}

	/**
	 * Create a {@link GenericJackson3JsonRedisSerializer} with a custom-configured {@link ObjectMapper} considering
	 * potential Object/{@link Jackson3ObjectReader -reader} and {@link Jackson3ObjectWriter -writer}.
	 *
	 * @param mapper must not be {@literal null}.
	 * @param reader the {@link Jackson3ObjectReader} function to read objects using {@link ObjectMapper}.
	 * @param writer the {@link Jackson3ObjectWriter} function to write objects using {@link ObjectMapper}.
	 */
	public GenericJackson3JsonRedisSerializer(ObjectMapper mapper, Jackson3ObjectReader reader,
			Jackson3ObjectWriter writer) {

		Assert.notNull(mapper, "ObjectMapper must not be null");
		Assert.notNull(reader, "Reader must not be null");
		Assert.notNull(writer, "Writer must not be null");

		this.mapper = mapper;
		this.reader = reader;
		this.writer = writer;

		this.defaultTypingEnabled = Lazy.of(() -> mapper.serializationConfig().getDefaultTyper(null) != null);
		this.lazyTypeHintPropertyName = newLazyTypeHintPropertyName(mapper, this.defaultTypingEnabled);

		this.typeResolver = newTypeResolver(mapper, this.lazyTypeHintPropertyName);
	}

	@Override
	public byte[] serialize(@Nullable Object value) throws SerializationException {

		if (value == null) {
			return SerializationUtils.EMPTY_ARRAY;
		}

		try {
			return writer.write(mapper, value);
		} catch (IOException | JacksonIOException ex) {
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
	 * {@link JsonMapperConfigurer} wraps around a {@link JsonMapper.Builder} providing dedicated methods to configure
	 * aspects like {@link NullValue} serialization strategy for the resulting {@link ObjectMapper} to be used with
	 * {@link GenericJackson3JsonRedisSerializer} as well as potential Object/{@link Jackson3ObjectReader -reader} and
	 * {@link Jackson3ObjectWriter -writer} settings.
	 * 
	 * @since 4.0
	 */
	public static class JsonMapperConfigurer {

		private final JsonMapper.Builder builder;
		private @Nullable Jackson3ObjectWriter writer;
		private @Nullable Jackson3ObjectReader reader;

		public JsonMapperConfigurer(Builder builder) {
			this.builder = builder;
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
		public JsonMapperConfigurer enableSpringCacheNullValueSupport() {

			builder.addModules(new GenericJackson3RedisSerializerModule(() -> {
				TypeResolverBuilder<?> defaultTyper = builder.baseSettings().getDefaultTyper();
				if (defaultTyper instanceof StdTypeResolverBuilder stdTypeResolverBuilder) {
					return stdTypeResolverBuilder.getTypeProperty();
				}
				return "@class";
			}));
			return this;
		}

		/**
		 * Registers a {@link StdSerializer} capable of serializing Spring Cache {@link NullValue} using the given type
		 * property name. Please make sure to active
		 * {@link JsonMapper.Builder#activateDefaultTypingAsProperty(PolymorphicTypeValidator, DefaultTyping, String)
		 * default typing} accordingly.
		 *
		 * @return this.
		 */
		@Contract("_ -> this")
		public JsonMapperConfigurer enableSpringCacheNullValueSupport(String typePropertyName) {

			builder.addModules(new GenericJackson3RedisSerializerModule(() -> typePropertyName));
			return this;
		}

		/**
		 * Enables
		 * {@link JsonMapper.Builder#activateDefaultTypingAsProperty(PolymorphicTypeValidator, DefaultTyping, String)
		 * default typing} without any type validation constraints.
		 * <p>
		 * <strong>WARNING</strong>: without restrictions of the {@link PolymorphicTypeValidator} deserialization is
		 * vulnerable to arbitrary code execution when reading from untrusted sources.
		 *
		 * @return this.
		 * @see <a href=
		 *      "https://owasp.org/www-community/vulnerabilities/Deserialization_of_untrusted_data">https://owasp.org/www-community/vulnerabilities/Deserialization_of_untrusted_data</a>
		 */
		@Contract("-> this")
		public JsonMapperConfigurer unsafeDefaultTyping() {

			builder.configure(DeserializationFeature.FAIL_ON_MISSING_EXTERNAL_TYPE_ID_PROPERTY, false)
					.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
					.activateDefaultTypingAsProperty(BasicPolymorphicTypeValidator.builder().allowIfBaseType(Object.class)
							.allowIfSubType((ctx, clazz) -> true).build(), DefaultTyping.NON_FINAL, "@class");
			return this;
		}

		/**
		 * Configures the {@link Jackson3ObjectWriter}.
		 * 
		 * @param writer must not be {@literal null}.
		 * @return this.
		 */
		@Contract("_ -> this")
		public JsonMapperConfigurer writer(Jackson3ObjectWriter writer) {
			this.writer = writer;
			return this;
		}

		/**
		 * Configures the {@link Jackson3ObjectReader}.
		 *
		 * @param reader must not be {@literal null}.
		 * @return this.
		 */
		@Contract("_ -> this")
		public JsonMapperConfigurer reader(Jackson3ObjectReader reader) {

			this.reader = reader;
			return this;
		}

		/**
		 * Callback hook to interact with the raw {@link Builder}.
		 *
		 * @param builderCustomizer
		 * @return this.
		 */
		@Contract("_ -> this")
		public JsonMapperConfigurer customize(Consumer<Builder> builderCustomizer) {

			builderCustomizer.accept(builder);
			return this;
		}

		Jackson3ObjectReader getReader() {
			return reader != null ? reader : Jackson3ObjectReader.create();
		}

		Jackson3ObjectWriter getWriter() {
			return writer != null ? writer : Jackson3ObjectWriter.create();
		}
	}

	/**
	 * @since 4.0
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

			if (jsonNode.isString() && jsonNode.asString() != null) {
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
	 * @since 4.0
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
	}

	/**
	 * @since 4.0
	 */
	private static class GenericJackson3RedisSerializerModule extends JacksonModule {

		private final Supplier<String> classIdentifier;

		GenericJackson3RedisSerializerModule(Supplier<String> classIdentifier) {
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

			List<ValueSerializer<?>> valueSerializers = new ArrayList<>();
			valueSerializers.add(new NullValueSerializer(classIdentifier));
			context.addSerializers(new SimpleSerializers(valueSerializers));
		}
	}
}
