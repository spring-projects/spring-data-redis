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

import tools.jackson.core.JacksonException;
import tools.jackson.core.JsonGenerator;
import tools.jackson.core.JsonParser;
import tools.jackson.core.Version;
import tools.jackson.databind.DefaultTyping;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.JacksonModule;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.SerializationContext;
import tools.jackson.databind.ValueDeserializer;
import tools.jackson.databind.ValueSerializer;
import tools.jackson.databind.cfg.MapperBuilder;
import tools.jackson.databind.deser.jdk.JavaUtilCalendarDeserializer;
import tools.jackson.databind.deser.jdk.JavaUtilDateDeserializer;
import tools.jackson.databind.deser.jdk.NumberDeserializers.BigDecimalDeserializer;
import tools.jackson.databind.deser.jdk.NumberDeserializers.BigIntegerDeserializer;
import tools.jackson.databind.deser.std.StdDeserializer;
import tools.jackson.databind.exc.MismatchedInputException;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.json.JsonMapper.Builder;
import tools.jackson.databind.jsontype.BasicPolymorphicTypeValidator;
import tools.jackson.databind.jsontype.TypeDeserializer;
import tools.jackson.databind.jsontype.TypeSerializer;
import tools.jackson.databind.jsontype.impl.AsPropertyTypeDeserializer;
import tools.jackson.databind.module.SimpleDeserializers;
import tools.jackson.databind.module.SimpleSerializers;
import tools.jackson.databind.ser.Serializers;
import tools.jackson.databind.ser.jdk.JavaUtilCalendarSerializer;
import tools.jackson.databind.ser.jdk.JavaUtilDateSerializer;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TimeZone;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.springframework.data.mapping.MappingException;
import org.springframework.data.redis.support.collections.CollectionUtils;
import org.springframework.data.util.DirectFieldAccessFallbackBeanWrapper;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

import com.fasterxml.jackson.annotation.JsonInclude.Include;

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
 * @since 4.0
 */
public class Jackson3HashMapper implements HashMapper<Object, String, Object> {

	private static final boolean SOURCE_VERSION_PRESENT = ClassUtils.isPresent("javax.lang.model.SourceVersion",
			Jackson3HashMapper.class.getClassLoader());

	private final ObjectMapper typingMapper;
	private final ObjectMapper untypedMapper;
	private final boolean flatten;

	public Jackson3HashMapper(
			Consumer<MapperBuilder<? extends ObjectMapper, ? extends MapperBuilder<?, ?>>> jsonMapperBuilder,
			boolean flatten) {
		this(((Supplier<JsonMapper>) () -> {
			Builder builder = JsonMapper.builder();
			jsonMapperBuilder.accept(builder);
			return builder.build();
		}).get(), flatten);
	}

	public static void preconfigure(MapperBuilder<? extends ObjectMapper, ? extends MapperBuilder<?, ?>> builder) {
		builder.findAndAddModules().addModules(new HashMapperModule())
				.activateDefaultTypingAsProperty(BasicPolymorphicTypeValidator.builder().allowIfBaseType(Object.class)
						.allowIfSubType((ctx, clazz) -> true).build(), DefaultTyping.NON_FINAL, "@class")
				.configure(DeserializationFeature.FAIL_ON_MISSING_EXTERNAL_TYPE_ID_PROPERTY, false)
				.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
				.changeDefaultPropertyInclusion(value -> value.withValueInclusion(Include.NON_NULL));
	}

	/**
	 * Creates new {@link Jackson3HashMapper} initialized with a custom Jackson {@link ObjectMapper}.
	 *
	 * @param mapper Jackson {@link ObjectMapper} used to de/serialize hashed {@link Object objects}; must not be
	 *          {@literal null}.
	 * @param flatten boolean used to configure whether JSON de/serialized {@link Object} properties will be un/flattened
	 *          using {@literal dot notation}, or whether to retain the hierarchical node structure created by Jackson.
	 */
	public Jackson3HashMapper(ObjectMapper mapper, boolean flatten) {

		Assert.notNull(mapper, "Mapper must not be null");

		this.flatten = flatten;
		this.typingMapper = mapper;
		this.untypedMapper = JsonMapper.shared();
	}

	@Override
	@SuppressWarnings("unchecked")
	public Map<String, Object> toHash(Object source) {

		JsonNode tree = this.typingMapper.valueToTree(source);
		return this.flatten ? flattenMap(tree.properties()) : this.untypedMapper.convertValue(tree, Map.class);
	}

	@Override
	@SuppressWarnings("all")
	public Object fromHash(Map<String, Object> hash) {

		try {
			if (this.flatten) {

				Map<String, Object> unflattenedHash = doUnflatten(hash);
				byte[] unflattenedHashedBytes = this.untypedMapper.writeValueAsBytes(unflattenedHash);
				Object hashedObject = this.typingMapper.reader().forType(Object.class).readValue(unflattenedHashedBytes);

				return hashedObject;
			}

			return this.typingMapper.treeToValue(this.untypedMapper.valueToTree(hash), Object.class);

		} catch (Exception ex) {
			throw new MappingException(ex.getMessage(), ex);
		}
	}

	@SuppressWarnings("unchecked")
	private Map<String, Object> doUnflatten(Map<String, Object> source) {

		Map<String, Object> result = org.springframework.util.CollectionUtils.newLinkedHashMap(source.size());
		Set<String> treatSeparate = org.springframework.util.CollectionUtils.newLinkedHashSet(source.size());

		for (Entry<String, Object> entry : source.entrySet()) {

			String key = entry.getKey();
			String[] keyParts = key.split("\\.");

			if (keyParts.length == 1 && isNotIndexed(keyParts[0])) {
				result.put(key, entry.getValue());
			} else if (keyParts.length == 1 && isIndexed(keyParts[0])) {

				String indexedKeyName = keyParts[0];
				String nonIndexedKeyName = stripIndex(indexedKeyName);

				int index = getIndex(indexedKeyName);

				if (result.containsKey(nonIndexedKeyName)) {
					addValueToTypedListAtIndex((List<Object>) result.get(nonIndexedKeyName), index, entry.getValue());
				} else {
					result.put(nonIndexedKeyName, createTypedListWithValue(index, entry.getValue()));
				}
			} else {
				treatSeparate.add(keyParts[0]);
			}
		}

		for (String partial : treatSeparate) {

			Map<String, Object> newSource = new LinkedHashMap<>();

			// Copies all nested, dot properties from the source Map to the new Map beginning from
			// the next nested (dot) property
			for (Entry<String, Object> entry : source.entrySet()) {
				String key = entry.getKey();
				if (key.startsWith(partial)) {
					String keyAfterDot = key.substring(partial.length() + 1);
					newSource.put(keyAfterDot, entry.getValue());
				}
			}

			if (isNonNestedIndexed(partial)) {

				String nonIndexPartial = stripIndex(partial);
				int index = getIndex(partial);

				if (result.containsKey(nonIndexPartial)) {
					addValueToTypedListAtIndex((List<Object>) result.get(nonIndexPartial), index, doUnflatten(newSource));
				} else {
					result.put(nonIndexPartial, createTypedListWithValue(index, doUnflatten(newSource)));
				}
			} else {
				result.put(partial, doUnflatten(newSource));
			}
		}

		return result;
	}

	private boolean isIndexed(String value) {
		return value.indexOf('[') > -1;
	}

	private boolean isNotIndexed(String value) {
		return !isIndexed(value);
	}

	private boolean isNonNestedIndexed(String value) {
		return value.endsWith("]");
	}

	private int getIndex(String indexedValue) {
		return Integer.parseInt(indexedValue.substring(indexedValue.indexOf('[') + 1, indexedValue.length() - 1));
	}

	private String stripIndex(String indexedValue) {

		int indexOfLeftBracket = indexedValue.indexOf("[");

		return indexOfLeftBracket > -1 ? indexedValue.substring(0, indexOfLeftBracket) : indexedValue;
	}

	private Map<String, Object> flattenMap(Set<Entry<String, JsonNode>> source) {

		Map<String, Object> resultMap = new HashMap<>();
		doFlatten("", source, resultMap);
		return resultMap;
	}

	private void doFlatten(String propertyPrefix, Set<Entry<String, JsonNode>> inputMap, Map<String, Object> resultMap) {

		if (StringUtils.hasText(propertyPrefix)) {
			propertyPrefix = propertyPrefix + ".";
		}

		for (Entry<String, JsonNode> entry : inputMap) {
			flattenElement(propertyPrefix + entry.getKey(), entry.getValue(), resultMap);
		}
	}

	private void flattenElement(String propertyPrefix, Object source, Map<String, Object> resultMap) {

		if (!(source instanceof JsonNode element)) {
			resultMap.put(propertyPrefix, source);
			return;
		}

		if (element.isArray()) {

			Iterator<JsonNode> nodes = element.values().iterator();

			while (nodes.hasNext()) {

				JsonNode currentNode = nodes.next();

				if (currentNode.isArray()) {
					flattenCollection(propertyPrefix, currentNode.values(), resultMap);
				} else if (nodes.hasNext() && mightBeJavaType(currentNode)) {

					JsonNode next = nodes.next();

					if (next.isArray()) {
						flattenCollection(propertyPrefix, next.values(), resultMap);
					}
					if (currentNode.asString().equals("java.util.Date")) {
						resultMap.put(propertyPrefix, next.asString());
						break;
					}
					if (next.isNumber()) {
						resultMap.put(propertyPrefix, next.numberValue());
						break;
					}
					if (next.isString()) {
						resultMap.put(propertyPrefix, next.stringValue());
						break;
					}
					if (next.isBoolean()) {
						resultMap.put(propertyPrefix, next.booleanValue());
						break;
					}
					if (next.isBinary()) {

						try {
							resultMap.put(propertyPrefix, next.binaryValue());
						} catch (Exception ex) {
							throw new IllegalStateException("Cannot read binary value '%s'".formatted(propertyPrefix), ex);
						}

						break;
					}
				}
			}
		} else if (element.isObject()) {
			doFlatten(propertyPrefix, element.properties(), resultMap);
		} else {

			switch (element.getNodeType()) {
				case STRING -> resultMap.put(propertyPrefix, element.stringValue());
				case NUMBER -> resultMap.put(propertyPrefix, element.numberValue());
				case BOOLEAN -> resultMap.put(propertyPrefix, element.booleanValue());
				case BINARY -> {
					try {
						resultMap.put(propertyPrefix, element.binaryValue());
					} catch (Exception e) {
						throw new IllegalStateException(e);
					}
				}
				default ->
					resultMap.put(propertyPrefix, new DirectFieldAccessFallbackBeanWrapper(element).getPropertyValue("_value"));
			}
		}
	}

	private boolean mightBeJavaType(JsonNode node) {

		String textValue = node.asString();

		if (!SOURCE_VERSION_PRESENT) {
			return Arrays.asList("java.util.Date", "java.math.BigInteger", "java.math.BigDecimal").contains(textValue);
		}

		return javax.lang.model.SourceVersion.isName(textValue);
	}

	private void flattenCollection(String propertyPrefix, Collection<JsonNode> list, Map<String, Object> resultMap) {

		Iterator<JsonNode> iterator = list.iterator();
		for (int counter = 0; iterator.hasNext(); counter++) {
			JsonNode element = iterator.next();
			flattenElement(propertyPrefix + "[" + counter + "]", element, resultMap);
		}
	}

	@SuppressWarnings("unchecked")
	private void addValueToTypedListAtIndex(List<Object> listWithTypeHint, int index, Object value) {

		List<Object> valueList = (List<Object>) listWithTypeHint.get(1);

		if (index >= valueList.size()) {
			int initialCapacity = index + 1;
			List<Object> newValueList = new ArrayList<>(initialCapacity);
			Collections.copy(CollectionUtils.initializeList(newValueList, initialCapacity), valueList);
			listWithTypeHint.set(1, newValueList);
			valueList = newValueList;
		}

		valueList.set(index, value);
	}

	private List<Object> createTypedListWithValue(int index, Object value) {

		int initialCapacity = index + 1;

		List<Object> valueList = CollectionUtils.initializeList(new ArrayList<>(initialCapacity), initialCapacity);
		valueList.set(index, value);

		List<Object> listWithTypeHint = new ArrayList<>();
		listWithTypeHint.add(ArrayList.class.getName());
		listWithTypeHint.add(valueList);

		return listWithTypeHint;
	}

	private static class HashMapperModule extends JacksonModule {

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
			valueSerializers.add(new UTCCalendarSerializer());

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
}
