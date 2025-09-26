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
package org.springframework.data.redis.hash;

import tools.jackson.core.JacksonException;
import tools.jackson.core.JsonGenerator;
import tools.jackson.core.JsonParser;
import tools.jackson.core.TreeNode;
import tools.jackson.core.Version;
import tools.jackson.databind.*;
import tools.jackson.databind.cfg.MapperBuilder;
import tools.jackson.databind.deser.jdk.JavaUtilCalendarDeserializer;
import tools.jackson.databind.deser.jdk.JavaUtilDateDeserializer;
import tools.jackson.databind.deser.jdk.NumberDeserializers.BigDecimalDeserializer;
import tools.jackson.databind.deser.jdk.NumberDeserializers.BigIntegerDeserializer;
import tools.jackson.databind.deser.std.StdDeserializer;
import tools.jackson.databind.exc.MismatchedInputException;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.jsontype.BasicPolymorphicTypeValidator;
import tools.jackson.databind.jsontype.PolymorphicTypeValidator;
import tools.jackson.databind.jsontype.TypeDeserializer;
import tools.jackson.databind.jsontype.TypeResolverBuilder;
import tools.jackson.databind.jsontype.TypeSerializer;
import tools.jackson.databind.jsontype.impl.AsPropertyTypeDeserializer;
import tools.jackson.databind.jsontype.impl.DefaultTypeResolverBuilder;
import tools.jackson.databind.module.SimpleDeserializers;
import tools.jackson.databind.module.SimpleSerializers;
import tools.jackson.databind.ser.Serializers;
import tools.jackson.databind.ser.jdk.JavaUtilCalendarSerializer;
import tools.jackson.databind.ser.jdk.JavaUtilDateSerializer;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TimeZone;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.jspecify.annotations.Nullable;

import org.springframework.data.mapping.MappingException;
import org.springframework.data.util.DirectFieldAccessFallbackBeanWrapper;
import org.springframework.data.util.Lazy;
import org.springframework.lang.Contract;
import org.springframework.util.Assert;

import com.fasterxml.jackson.annotation.JsonInclude.Include;

/**
 * {@link ObjectMapper} based {@link HashMapper} implementation that allows flattening. Given an entity {@code Person}
 * with an {@code Address} like below the flattening will create individual hash entries for all nested properties and
 * resolve complex types into simple types, as far as possible.
 * <p>
 * Creation can be configured using {@link #builder()} to enable Jackson 2 compatibility mode (when migrating existing
 * data from Jackson 2) or to attach a custom {@link MapperBuilder} configurer.
 * <p>
 * By default, JSON mapping uses default typing. Make sure to configure an appropriate {@link PolymorphicTypeValidator}
 * to prevent instantiation of unwanted types.
 * <p>
 * Flattening requires all property names to not interfere with JSON paths. Using dots or brackets in map keys or as
 * property names is not supported using flattening. The resulting hash cannot be mapped back into an Object.
 * <h3>Example</h3>
 *
 * <pre class="code">
 * class Person {
 * 	String firstname;
 * 	String lastname;
 * 	Address address;
 * 	Date date;
 * 	LocalDateTime localDateTime;
 * }
 *
 * class Address {
 * 	String city;
 * 	String country;
 * }
 * </pre>
 *
 * <h3>Normal</h3>
 * <table>
 * <tr>
 * <th>Hash field</th>
 * <th>Value</th>
 * </tr>
 * <tr>
 * <td>firstname</td>
 * <td>Jon</td>
 * </tr>
 * <tr>
 * <td>lastname</td>
 * <td>Snow</td>
 * </tr>
 * <tr>
 * <td>address</td>
 * <td>{ "city" : "Castle Black", "country" : "The North" }</td>
 * </tr>
 * <tr>
 * <td>date</td>
 * <td>1561543964015</td>
 * </tr>
 * <tr>
 * <td>localDateTime</td>
 * <td>2018-01-02T12:13:14</td>
 * </tr>
 * </table>
 * <h3>Flat</h3>
 * <table>
 * <tr>
 * <th>Hash field</th>
 * <th>Value</th>
 * </tr>
 * <tr>
 * <td>firstname</td>
 * <td>Jon</td>
 * </tr>
 * <tr>
 * <td>lastname</td>
 * <td>Snow</td>
 * </tr>
 * <tr>
 * <td>address.city</td>
 * <td>Castle Black</td>
 * </tr>
 * <tr>
 * <td>address.country</td>
 * <td>The North</td>
 * </tr>
 * <tr>
 * <td>date</td>
 * <td>1561543964015</td>
 * </tr>
 * <tr>
 * <td>localDateTime</td>
 * <td>2018-01-02T12:13:14</td>
 * </tr>
 * </table>
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 4.0
 */
public class JacksonHashMapper implements HashMapper<Object, String, Object> {

	private static final Lazy<JacksonHashMapper> sharedFlattening = Lazy
			.of(() -> create(JacksonHashMapperBuilder::flatten));
	private static final Lazy<JacksonHashMapper> sharedHierarchical = Lazy
			.of(() -> create(JacksonHashMapperBuilder::hierarchical));

	private final ObjectMapper typingMapper;
	private final ObjectMapper untypedMapper;
	private final boolean flatten;

	/**
	 * Creates a new {@link JacksonHashMapper} initialized with a custom Jackson {@link ObjectMapper}.
	 *
	 * @param mapper Jackson {@link ObjectMapper} used to de/serialize hashed {@link Object objects}; must not be
	 *          {@literal null}.
	 * @param flatten boolean used to configure whether JSON de/serialized {@link Object} properties will be un/flattened
	 *          using {@literal dot notation}, or whether to retain the hierarchical node structure created by Jackson.
	 */
	public JacksonHashMapper(ObjectMapper mapper, boolean flatten) {

		Assert.notNull(mapper, "Mapper must not be null");

		this.flatten = flatten;
		this.typingMapper = mapper;
		this.untypedMapper = JsonMapper.shared();
	}

	/**
	 * Returns a flattening {@link JacksonHashMapper} using {@literal dot notation} for properties.
	 *
	 * @return a flattening {@link JacksonHashMapper} instance.
	 */
	public static JacksonHashMapper flattening() {
		return sharedFlattening.get();
	}

	/**
	 * Returns a {@link JacksonHashMapper} retain the hierarchical node structure created by Jackson.
	 *
	 * @return a hierarchical {@link JacksonHashMapper} instance.
	 */
	public static JacksonHashMapper hierarchical() {
		return sharedHierarchical.get();
	}

	/**
	 * Creates a new {@link JacksonHashMapper} allowing further configuration through {@code configurer}.
	 *
	 * @param configurer the configurer for {@link JacksonHashMapperBuilder}.
	 * @return a new {@link JacksonHashMapper} instance.
	 */
	public static JacksonHashMapper create(Consumer<JacksonHashMapperBuilder<JsonMapper.Builder>> configurer) {

		Assert.notNull(configurer, "Builder configurer must not be null");

		JacksonHashMapperBuilder<JsonMapper.Builder> builder = builder();
		configurer.accept(builder);

		return builder.build();
	}

	/**
	 * Creates a {@link JacksonHashMapperBuilder} to build a {@link JacksonHashMapper} instance using {@link JsonMapper}.
	 *
	 * @return a {@link JacksonHashMapperBuilder} to build a {@link JacksonHashMapper} instance.
	 */
	public static JacksonHashMapperBuilder<JsonMapper.Builder> builder() {
		return builder(JsonMapper::builder);
	}

	/**
	 * Creates a new {@link JacksonHashMapperBuilder} to configure and build a {@link JacksonHashMapper}.
	 *
	 * @param builderFactory factory to create a {@link MapperBuilder} for the {@link ObjectMapper}.
	 * @param <B> type of the {@link MapperBuilder} to use.
	 * @return a new {@link JacksonHashMapperBuilder}.
	 */
	public static <B extends MapperBuilder<? extends ObjectMapper, ? extends MapperBuilder<?, ?>>> JacksonHashMapperBuilder<B> builder(
			Supplier<B> builderFactory) {

		Assert.notNull(builderFactory, "MapperBuilder Factory must not be null");

		return new JacksonHashMapperBuilder<>(builderFactory);
	}

	/**
	 * Preconfigures the given {@link MapperBuilder} to create a Jackson {@link ObjectMapper} that is suitable for
	 * HashMapper use.
	 *
	 * @param builder the {@link MapperBuilder} to preconfigure.
	 * @param jackson2Compatibility whether to apply Jackson 2.x compatibility settings to read values written by
	 *          {@link Jackson2HashMapper}.
	 */
	public static void preconfigure(MapperBuilder<? extends ObjectMapper, ? extends MapperBuilder<?, ?>> builder,
			boolean jackson2Compatibility) {

		builder.findAndAddModules() //
				.addModules(new HashMapperModule(jackson2Compatibility)) //
				.disable(MapperFeature.REQUIRE_TYPE_ID_FOR_SUBTYPES) //
				.configure(DeserializationFeature.FAIL_ON_MISSING_EXTERNAL_TYPE_ID_PROPERTY, false)
				.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
				.changeDefaultPropertyInclusion(value -> value.withValueInclusion(Include.NON_NULL));
	}

	@Override
	@SuppressWarnings("unchecked")
	public Map<String, Object> toHash(@Nullable Object source) {

		JsonNode tree = this.typingMapper.valueToTree(source);
		return this.flatten ? FlatEric.flatten(JacksonAdapterFactory.INSTANCE, tree.properties())
				: this.untypedMapper.convertValue(tree, Map.class);
	}

	@Override
	@SuppressWarnings("all")
	public Object fromHash(Map<String, Object> hash) {

		try {
			if (this.flatten) {

				Map<String, Object> unflattenedHash = FlatEric.unflatten(hash);
				byte[] unflattenedHashedBytes = this.untypedMapper.writeValueAsBytes(unflattenedHash);
				Object hashedObject = this.typingMapper.reader().forType(Object.class).readValue(unflattenedHashedBytes);

				return hashedObject;
			}

			return this.typingMapper.treeToValue(this.untypedMapper.valueToTree(hash), Object.class);

		} catch (Exception ex) {
			throw new MappingException(ex.getMessage(), ex);
		}
	}


	/**
	 * Builder to create a {@link JacksonHashMapper} instance.
	 *
	 * @param <B> type of the {@link MapperBuilder}.
	 */
	public static class JacksonHashMapperBuilder<B extends MapperBuilder<? extends ObjectMapper, ? extends MapperBuilder<?, ?>>> {

		private final Supplier<B> builderFactory;

		private PolymorphicTypeValidator typeValidator = BasicPolymorphicTypeValidator.builder()
				.allowIfBaseType(Object.class).allowIfSubType((ctx, clazz) -> true).build();
		private boolean jackson2CompatibilityMode = false;
		private boolean flatten = false;
		private Consumer<B> mapperBuilderCustomizer = builder -> {};

		private JacksonHashMapperBuilder(Supplier<B> builderFactory) {
			this.builderFactory = builderFactory;
		}

		/**
		 * Use a flattened representation using {@literal dot notation}. The default is to use {@link #hierarchical()}.
		 *
		 * @return {@code this} builder.
		 */
		@Contract("-> this")
		public JacksonHashMapperBuilder<B> flatten() {
			return flatten(true);
		}

		/**
		 * Use a hierarchical node structure as created by Jackson. This is the default behavior.
		 *
		 * @return {@code this} builder.
		 */
		@Contract("-> this")
		public JacksonHashMapperBuilder<B> hierarchical() {
			return flatten(false);
		}

		/**
		 * Configure whether to flatten the resulting hash using {@literal dot notation} for properties. The default is to
		 * use {@link #hierarchical()}.
		 *
		 * @param flatten boolean used to configure whether JSON de/serialized {@link Object} properties will be
		 *          un/flattened using {@literal dot notation}, or whether to retain the hierarchical node structure created
		 *          by Jackson.
		 * @return {@code this} builder.
		 */
		@Contract("_ -> this")
		public JacksonHashMapperBuilder<B> flatten(boolean flatten) {
			this.flatten = flatten;
			return this;
		}

		/**
		 * Enable Jackson 2 compatibility mode. This enables reading values written by {@link Jackson2HashMapper} and
		 * writing values that can be read by {@link Jackson2HashMapper}.
		 *
		 * @return {@code this} builder.
		 */
		@Contract("-> this")
		public JacksonHashMapperBuilder<B> jackson2CompatibilityMode() {
			this.jackson2CompatibilityMode = true;
			return this;
		}

		/**
		 * Provide a {@link PolymorphicTypeValidator} to validate polymorphic types during deserialization.
		 *
		 * @param typeValidator the validator to use, defaults to a permissive validator that allows all types.
		 * @return {@code this} builder.
		 */
		@Contract("_ -> this")
		public JacksonHashMapperBuilder<B> typeValidator(PolymorphicTypeValidator typeValidator) {

			Assert.notNull(typeValidator, "Type validator must not be null");

			this.typeValidator = typeValidator;
			return this;
		}

		/**
		 * Provide a {@link Consumer customizer} to configure the {@link ObjectMapper} through its {@link MapperBuilder}.
		 *
		 * @param mapperBuilderCustomizer the configurer to apply to the {@link ObjectMapper} builder.
		 * @return {@code this} builder.
		 */
		@Contract("_ -> this")
		public JacksonHashMapperBuilder<B> customize(Consumer<B> mapperBuilderCustomizer) {

			Assert.notNull(mapperBuilderCustomizer, "JSON mapper customizer must not be null");

			this.mapperBuilderCustomizer = mapperBuilderCustomizer;
			return this;
		}

		/**
		 * Build a new {@link JacksonHashMapper} instance with the configured settings.
		 *
		 * @return a new {@link JacksonHashMapper} instance.
		 */
		@Contract("-> new")
		public JacksonHashMapper build() {

			B mapperBuilder = builderFactory.get();

			preconfigure(mapperBuilder, jackson2CompatibilityMode);
			mapperBuilder.setDefaultTyping(getDefaultTyping(typeValidator, flatten, "@class"));

			mapperBuilderCustomizer.accept(mapperBuilder);

			return new JacksonHashMapper(mapperBuilder.build(), flatten);
		}

		private static TypeResolverBuilder<?> getDefaultTyping(PolymorphicTypeValidator typeValidator, boolean flatten,
				String typePropertyName) {

			return new DefaultTypeResolverBuilder(typeValidator, DefaultTyping.NON_FINAL, typePropertyName) {

				@Override
				public boolean useForType(JavaType type) {

					if (type.isPrimitive()) {
						return false;
					}

					if (flatten && (type.isTypeOrSubTypeOf(Number.class) || type.isEnumType())) {
						return false;
					}

					return !TreeNode.class.isAssignableFrom(type.getRawClass());
				}
			};
		}
	}

	private static class HashMapperModule extends JacksonModule {

		private final boolean useCalendarTimestamps;

		private HashMapperModule(boolean useCalendarTimestamps) {
			this.useCalendarTimestamps = useCalendarTimestamps;
		}

		@Override
		public String getModuleName() {
			return "spring-data-redis-hash-mapper-module";
		}

		@Override
		public Version version() {
			return new Version(4, 0, 0, null, "org.springframework.data", "spring-data-redis");
		}

		@Override
		public void setupModule(SetupContext context) {

			List<ValueSerializer<?>> valueSerializers = new ArrayList<>();
			valueSerializers.add(new JavaUtilDateSerializer(true, null) {
				@Override
				public void serializeWithType(Date value, JsonGenerator g, SerializationContext ctxt, TypeSerializer typeSer)
						throws JacksonException {
					serialize(value, g, ctxt);
				}
			});
			valueSerializers.add(new UTCCalendarSerializer(useCalendarTimestamps));

			Serializers serializers = new SimpleSerializers(valueSerializers);
			context.addSerializers(serializers);

			Map<Class<?>, ValueDeserializer<?>> valueDeserializers = new LinkedHashMap<>();
			valueDeserializers.put(java.util.Calendar.class,
					new UntypedFallbackDeserializer<>(new UntypedUTCCalendarDeserializer()));
			valueDeserializers.put(java.util.Date.class, new UntypedFallbackDeserializer<>(new JavaUtilDateDeserializer()));
			valueDeserializers.put(BigInteger.class, new UntypedFallbackDeserializer<>(new BigIntegerDeserializer()));
			valueDeserializers.put(BigDecimal.class, new UntypedFallbackDeserializer<>(new BigDecimalDeserializer()));

			context.addDeserializers(new SimpleDeserializers(valueDeserializers));
		}

	}

	static class UntypedFallbackDeserializer<T> extends StdDeserializer<T> {

		private final StdDeserializer<?> delegate;

		protected UntypedFallbackDeserializer(StdDeserializer<?> delegate) {
			super(Object.class);
			this.delegate = delegate;
		}

		@Override
		public Object deserializeWithType(JsonParser p, DeserializationContext ctxt, TypeDeserializer typeDeserializer)
				throws JacksonException {

			if (!(typeDeserializer instanceof AsPropertyTypeDeserializer asPropertySerializer)) {
				return super.deserializeWithType(p, ctxt, typeDeserializer);
			}

			try {
				return super.deserializeWithType(p, ctxt, typeDeserializer);
			} catch (MismatchedInputException e) {
				if (!asPropertySerializer.baseType().isTypeOrSuperTypeOf(delegate.handledType())) {
					throw e;
				}
			}

			return deserialize(p, ctxt);

		}

		@Override
		@SuppressWarnings("unchecked")
		public T deserialize(JsonParser p, DeserializationContext ctxt) throws JacksonException {
			return (T) delegate.deserialize(p, ctxt);
		}
	}

	static class UTCCalendarSerializer extends JavaUtilCalendarSerializer {

		private static final TimeZone UTC = TimeZone.getTimeZone("UTC");

		public final boolean useTimestamps;

		public UTCCalendarSerializer(boolean useTimestamps) {
			this.useTimestamps = useTimestamps;
		}

		@Override
		public void serialize(Calendar value, JsonGenerator g, SerializationContext provider) throws JacksonException {

			Calendar utc = Calendar.getInstance();
			utc.setTimeInMillis(value.getTimeInMillis());
			utc.setTimeZone(UTC);
			super.serialize(utc, g, provider);
		}

		@Override
		public void serializeWithType(Calendar value, JsonGenerator g, SerializationContext ctxt, TypeSerializer typeSer)
				throws JacksonException {
			serialize(value, g, ctxt);
		}

		protected boolean _asTimestamp(SerializationContext serializers) {
			return useTimestamps;
		}

	}

	static class UntypedUTCCalendarDeserializer extends JavaUtilCalendarDeserializer {

		private static final TimeZone UTC = TimeZone.getTimeZone("UTC");

		@Override
		public Calendar deserialize(JsonParser p, DeserializationContext ctxt) throws JacksonException {

			Calendar cal = super.deserialize(p, ctxt);

			Calendar utc = Calendar.getInstance(UTC);
			utc.setTimeInMillis(cal.getTimeInMillis());
			utc.setTimeZone(TimeZone.getTimeZone(ZoneId.systemDefault()));

			return utc;
		}
	}

	private enum JacksonAdapterFactory implements FlatEric.JsonNodeAdapterFactory {

		INSTANCE;


		@Override
		public FlatEric.JsonNodeAdapter adapt(Object node) {
			return node instanceof FlatEric.JsonNodeAdapter na ? na : new JacksonJsonNodeAdapter((JsonNode) node);
		}

		@Override
		public boolean isJsonNode(Object value) {
			return value instanceof JsonNode || value instanceof FlatEric.JsonNodeAdapter;
		}
	}

	private record JacksonJsonNodeAdapter(JsonNode node) implements FlatEric.JsonNodeAdapter {

		@Override
		public FlatEric.JsonNodeType getNodeType() {
			return FlatEric.JsonNodeType.valueOf(node().getNodeType().name());
		}

		@Override
		public boolean isArray() {
			return node().isArray();
		}

		@Override
		public Collection<? extends FlatEric.JsonNodeAdapter> values() {
			return node().valueStream().map(JacksonJsonNodeAdapter::new).toList();
		}

		@Override
		public String asString() {
			return node().asString();
		}

		@Override
		public boolean isNumber() {
			return node().isNumber();
		}

		@Override
		public Number numberValue() {
			return node().numberValue();
		}

		@Override
		public boolean isString() {
			return node().isString();
		}

		@Override
		public String stringValue() {
			return node().stringValue();
		}

		@Override
		public boolean isBoolean() {
			return node().isBoolean();
		}

		@Override
		public boolean booleanValue() {
			return node().booleanValue();
		}

		@Override
		public boolean isBinary() {
			return node().isBinary();
		}

		@Override
		public byte[] binaryValue() {
			return node().binaryValue();
		}

		@Override
		public boolean isObject() {
			return node().isObject();
		}

		@Override
		public Collection<Entry<String, FlatEric.JsonNodeAdapter>> properties() {
			return node().propertyStream()
					.map(it -> Map.entry(it.getKey(), (FlatEric.JsonNodeAdapter) new JacksonJsonNodeAdapter(it.getValue())))
					.toList();
		}

		@Override
		public @Nullable Object getDirectValue() {
			return new DirectFieldAccessFallbackBeanWrapper(node()).getPropertyValue("_value");
		}

	}

}
