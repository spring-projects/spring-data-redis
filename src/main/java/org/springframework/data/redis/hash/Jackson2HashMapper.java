/*
 * Copyright 2016-2025 the original author or authors.
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

import static com.fasterxml.jackson.databind.ObjectMapper.DefaultTyping.*;

import java.io.IOException;
import java.text.ParseException;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;

import org.jspecify.annotations.Nullable;

import org.springframework.data.mapping.MappingException;
import org.springframework.data.util.DirectFieldAccessFallbackBeanWrapper;
import org.springframework.util.Assert;
import org.springframework.util.NumberUtils;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonTypeInfo.As;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectMapper.DefaultTyping;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.UntypedObjectDeserializer;
import com.fasterxml.jackson.databind.jsontype.PolymorphicTypeValidator;
import com.fasterxml.jackson.databind.jsontype.TypeDeserializer;
import com.fasterxml.jackson.databind.jsontype.TypeResolverBuilder;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.CalendarSerializer;
import com.fasterxml.jackson.databind.ser.std.DateSerializer;

/**
 * {@link ObjectMapper} based {@link HashMapper} implementation that allows flattening. Given an entity {@code Person}
 * with an {@code Address} like below the flattening will create individual hash entries for all nested properties and
 * resolve complex types into simple types, as far as possible.
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
 * @author John Blum
 * @since 1.8
 * @deprecated since 4.0 in favor of {@link JacksonHashMapper}.
 */
@Deprecated(since = "4.0", forRemoval = true)
public class Jackson2HashMapper implements HashMapper<Object, String, Object> {

	private final ObjectMapper typingMapper;
	private final ObjectMapper untypedMapper;
	private final boolean flatten;

	/**
	 * Creates new {@link Jackson2HashMapper} with a default {@link ObjectMapper}.
	 *
	 * @param flatten boolean used to configure whether JSON de/serialized {@link Object} properties
	 * will be un/flattened using {@literal dot notation}, or whether to retain the hierarchical node structure
	 * created by Jackson.
	 */
	public Jackson2HashMapper(boolean flatten) {

		this(new ObjectMapper() {

			@Override
			protected TypeResolverBuilder<?> _constructDefaultTypeResolverBuilder(DefaultTyping applicability,
					PolymorphicTypeValidator typeValidator) {

				return new DefaultTypeResolverBuilder(applicability, typeValidator) {

					public boolean useForType(JavaType type) {

						if (type.isPrimitive()) {
							return false;
						}

						if (flatten && (type.isTypeOrSubTypeOf(Number.class) || type.isEnumType())) {
							return false;
						}

						if (EVERYTHING.equals(_appliesFor)) {
							return !TreeNode.class.isAssignableFrom(type.getRawClass());
						}

						return super.useForType(type);
					}
				};
			}
		}.findAndRegisterModules(), flatten);

		this.typingMapper.activateDefaultTyping(this.typingMapper.getPolymorphicTypeValidator(),
				DefaultTyping.EVERYTHING, As.PROPERTY);
		this.typingMapper.configure(SerializationFeature.WRITE_NULL_MAP_VALUES, false);

		if(flatten) {
			this.typingMapper.disable(MapperFeature.REQUIRE_TYPE_ID_FOR_SUBTYPES);
		}

		// Prevent splitting time types into arrays. E
		this.typingMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
		this.typingMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		this.typingMapper.setSerializationInclusion(Include.NON_NULL);
		this.typingMapper.registerModule(new HashMapperModule());
	}

	/**
	 * Creates new {@link Jackson2HashMapper} initialized with a custom Jackson {@link ObjectMapper}.
	 *
	 * @param mapper Jackson {@link ObjectMapper} used to de/serialize hashed {@link Object objects};
	 * must not be {@literal null}.
	 * @param flatten boolean used to configure whether JSON de/serialized {@link Object} properties
	 * will be un/flattened using {@literal dot notation}, or whether to retain the hierarchical node structure
	 * created by Jackson.
	 */
	public Jackson2HashMapper(ObjectMapper mapper, boolean flatten) {

		Assert.notNull(mapper, "Mapper must not be null");

		this.flatten = flatten;
		this.typingMapper = mapper;
		this.untypedMapper = new ObjectMapper();
		this.untypedMapper.configure(SerializationFeature.WRITE_NULL_MAP_VALUES, false);
		this.untypedMapper.setSerializationInclusion(Include.NON_NULL);
		this.untypedMapper.findAndRegisterModules();
	}

	@Override
	@SuppressWarnings("unchecked")
	public Map<String, Object> toHash(@Nullable Object source) {

		JsonNode tree = this.typingMapper.valueToTree(source);

		return this.flatten ? FlatEric.flatten(Jackson2AdapterFactory.INSTANCE, tree.properties())
				: this.untypedMapper.convertValue(tree, Map.class);
	}

	@Override
	@SuppressWarnings("all")
	public @Nullable Object fromHash(Map<String, Object> hash) {

		try {
			if (this.flatten) {

				Map<String, Object> unflattenedHash = FlatEric.unflatten(hash);
				byte[] unflattenedHashedBytes = this.untypedMapper.writeValueAsBytes(unflattenedHash);
				Object hashedObject = this.typingMapper.reader().forType(Object.class)
						.readValue(unflattenedHashedBytes);

				return hashedObject;
			}

			return this.typingMapper.treeToValue(this.untypedMapper.valueToTree(hash), Object.class);

		} catch (IOException ex) {
			throw new MappingException(ex.getMessage(), ex);
		}
	}

	private static class HashMapperModule extends SimpleModule {

		HashMapperModule() {

			addSerializer(java.util.Date.class, new UntypedSerializer<>(new DateToTimestampSerializer()));
			addSerializer(java.util.Calendar.class, new UntypedSerializer<>(new CalendarToTimestampSerializer()));

			addDeserializer(java.util.Date.class, new UntypedDateDeserializer());
			addDeserializer(java.util.Calendar.class, new UntypedCalendarDeserializer());
		}
	}

	/**
	 * {@link JsonDeserializer} for {@link Date} objects without considering type hints.
	 */
	private static class UntypedDateDeserializer extends JsonDeserializer<Date> {

		private final JsonDeserializer<?> delegate = new UntypedObjectDeserializer(null, null);

		@Override
		public Object deserializeWithType(JsonParser p, DeserializationContext ctxt, TypeDeserializer typeDeserializer)
				throws IOException {
			return deserialize(p, ctxt);
		}

		@Override
		public Date deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {

			Object value = delegate.deserialize(p, ctxt);

			if (value instanceof Date) {
				return (Date) value;
			}

			try {
				return ctxt.getConfig().getDateFormat().parse(value.toString());
			} catch (ParseException ignore) {
				return new Date(NumberUtils.parseNumber(value.toString(), Long.class));
			}
		}

	}

	/**
	 * {@link JsonDeserializer} for {@link Calendar} objects without considering type hints.
	 */
	private static class UntypedCalendarDeserializer extends JsonDeserializer<Calendar> {

		private final UntypedDateDeserializer dateDeserializer = new UntypedDateDeserializer();

		@Override
		public @Nullable Object deserializeWithType(JsonParser p, DeserializationContext ctxt, TypeDeserializer typeDeserializer)
				throws IOException {
			return deserialize(p, ctxt);
		}

		@Override
		public @Nullable Calendar deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {

			Date date = dateDeserializer.deserialize(p, ctxt);

			if (date != null) {
				Calendar calendar = Calendar.getInstance();
				calendar.setTime(date);
				return calendar;
			}

			return null;
		}

	}

	/**
	 * Untyped {@link JsonSerializer} to serialize plain values without writing JSON type hints.
	 *
	 * @param <T>
	 */
	private static class UntypedSerializer<T> extends JsonSerializer<T> {

		private final JsonSerializer<T> delegate;

		UntypedSerializer(JsonSerializer<T> delegate) {
			this.delegate = delegate;
		}

		@Override
		public void serializeWithType(T value, JsonGenerator jsonGenerator, SerializerProvider serializers,
				TypeSerializer typeSerializer) throws IOException {

			serialize(value, jsonGenerator, serializers);
		}

		@Override
		public void serialize(@Nullable T value, JsonGenerator jsonGenerator, SerializerProvider serializers)
				throws IOException {

			if (value != null) {
				delegate.serialize(value, jsonGenerator, serializers);
			} else {
				serializers.defaultSerializeNull(jsonGenerator);
			}
		}

	}

	private static class DateToTimestampSerializer extends DateSerializer {

		// Prevent splitting to array.
		@Override
		protected boolean _asTimestamp(SerializerProvider serializers) {
			return true;
		}

	}

	private static class CalendarToTimestampSerializer extends CalendarSerializer {

		// Prevent splitting to array.
		@Override
		protected boolean _asTimestamp(SerializerProvider serializers) {
			return true;
		}

	}

	private enum Jackson2AdapterFactory implements FlatEric.JsonNodeAdapterFactory {

		INSTANCE;

		@Override
		public FlatEric.JsonNodeAdapter adapt(Object node) {
			return node instanceof FlatEric.JsonNodeAdapter na ? na : new Jackson2JsonNodeAdapter((JsonNode) node);
		}

		@Override
		public boolean isJsonNode(Object value) {
			return value instanceof JsonNode || value instanceof FlatEric.JsonNodeAdapter;
		}
	}

	private record Jackson2JsonNodeAdapter(JsonNode node) implements FlatEric.JsonNodeAdapter {

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
			return node().valueStream().map(Jackson2JsonNodeAdapter::new).toList();
		}

		@Override
		public String asString() {
			return node().asText();
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
			return node().isTextual();
		}

		@Override
		public String stringValue() {
			return node().asText();
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

			try {
				return node().binaryValue();
			} catch (IOException e) {
				throw new IllegalStateException(e);
			}
		}

		@Override
		public boolean isObject() {
			return node().isObject();
		}

		@Override
		public Collection<Entry<String, FlatEric.JsonNodeAdapter>> properties() {
			return node().propertyStream()
					.map(it -> Map.entry(it.getKey(), (FlatEric.JsonNodeAdapter) new Jackson2JsonNodeAdapter(it.getValue())))
					.toList();
		}

		@Override
		public @Nullable Object getDirectValue() {
			return new DirectFieldAccessFallbackBeanWrapper(node()).getPropertyValue("_value");
		}

	}

}
