/*
 * Copyright 2016-2018 the original author or authors.
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
package org.springframework.data.redis.hash;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.springframework.data.mapping.MappingException;
import org.springframework.data.util.DirectFieldAccessFallbackBeanWrapper;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonTypeInfo.As;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectMapper.DefaultTyping;
import com.fasterxml.jackson.databind.SerializationFeature;

/**
 * {@link ObjectMapper} based {@link HashMapper} implementation that allows flattening. Given an entity {@code Person}
 * with an {@code Address} like below the flattening will create individual hash entries for all nested properties and
 * resolve complex types into simple types, as far as possible.
 * <p>
 * Flattening requires all property names to not interfere with JSON paths. Using dots or brackets in map keys or as
 * property names is not supported using flattening. The resulting hash cannot be mapped back into an Object.
 * <strong>Example</strong>
 *
 * <pre>
 * <code>
 * class Person {
 *   String firstname;
 *   String lastname;
 *   Address address;
 * }
 *
 * class Address {
 *   String city;
 *   String country;
 * }
 * </code>
 * </pre>
 *
 * <strong>Normal</strong>
 * <table>
 *   <tr><th>Hash field</th><th>Value<th></tr>
 *   <tr><td>firstname</td><td>Jon<td></tr>
 *   <tr><td>lastname</td><td>Snow<td></tr>
 *   <tr><td>address</td><td>{ "city" : "Castle Black", "country" : "The North" }<td></tr>
 * </table>
 * <br />
 * <strong>Flat</strong>:
 * <table>
 *   <tr><th>Hash field</th><th>Value<th></tr>
 *   <tr><td>firstname</td><td>Jon<td></tr>
 *   <tr><td>lastname</td><td>Snow<td></tr>
 *   <tr><td>address.city</td><td>Castle Black<td></tr>
 *   <tr><td>address.country</td><td>The North<td></tr>
 * </table>
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 1.8
 */
public class Jackson2HashMapper implements HashMapper<Object, String, Object> {

	private final ObjectMapper typingMapper;
	private final ObjectMapper untypedMapper;
	private final boolean flatten;

	/**
	 * Creates new {@link Jackson2HashMapper} with default {@link ObjectMapper}.
	 *
	 * @param flatten
	 */
	public Jackson2HashMapper(boolean flatten) {

		this(new ObjectMapper(), flatten);

		typingMapper.enableDefaultTyping(DefaultTyping.NON_FINAL, As.PROPERTY);
		typingMapper.configure(SerializationFeature.WRITE_NULL_MAP_VALUES, false);
		typingMapper.setSerializationInclusion(Include.NON_NULL);
		typingMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
	}

	/**
	 * Creates new {@link Jackson2HashMapper}.
	 *
	 * @param mapper must not be {@literal null}.
	 * @param flatten
	 */
	public Jackson2HashMapper(ObjectMapper mapper, boolean flatten) {

		Assert.notNull(mapper, "Mapper must not be null!");

		this.typingMapper = mapper;
		this.flatten = flatten;

		this.untypedMapper = new ObjectMapper();
		this.untypedMapper.configure(SerializationFeature.WRITE_NULL_MAP_VALUES, false);
		this.untypedMapper.setSerializationInclusion(Include.NON_NULL);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.hash.HashMapper#toHash(java.lang.Object)
	 */
	@Override
	@SuppressWarnings("unchecked")
	public Map<String, Object> toHash(Object source) {

		JsonNode tree = typingMapper.valueToTree(source);
		return flatten ? flattenMap(tree.fields()) : untypedMapper.convertValue(tree, Map.class);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.hash.HashMapper#fromHash(java.util.Map)
	 */
	@Override
	public Object fromHash(Map<String, Object> hash) {

		try {

			if (flatten) {

				return typingMapper.reader().forType(Object.class)
						.readValue(untypedMapper.writeValueAsBytes(doUnflatten(hash)));
			}

			return typingMapper.treeToValue(untypedMapper.valueToTree(hash), Object.class);

		} catch (IOException e) {
			throw new MappingException(e.getMessage(), e);
		}
	}

	@SuppressWarnings("unchecked")
	private Map<String, Object> doUnflatten(Map<String, Object> source) {

		Map<String, Object> result = new LinkedHashMap<>();
		Set<String> treatSeperate = new LinkedHashSet<>();
		for (Entry<String, Object> entry : source.entrySet()) {

			String key = entry.getKey();
			String[] args = key.split("\\.");

			if (args.length == 1 && !args[0].contains("[")) {
				result.put(entry.getKey(), entry.getValue());
				continue;
			}

			if (args.length == 1 && args[0].contains("[")) {

				String prunedKey = args[0].substring(0, args[0].indexOf('['));
				if (result.containsKey(prunedKey)) {
					appendValueToTypedList(args[0], entry.getValue(), (List<Object>) result.get(prunedKey));
				} else {
					result.put(prunedKey, createTypedListWithValue(entry.getValue()));
				}
			} else {
				treatSeperate.add(key.substring(0, key.indexOf('.')));
			}
		}

		for (String partial : treatSeperate) {

			Map<String, Object> newSource = new LinkedHashMap<>();

			for (Entry<String, Object> entry : source.entrySet()) {
				if (entry.getKey().startsWith(partial)) {
					newSource.put(entry.getKey().substring(partial.length() + 1), entry.getValue());
				}
			}

			if (partial.endsWith("]")) {

				String prunedKey = partial.substring(0, partial.indexOf('['));

				if (result.containsKey(prunedKey)) {
					appendValueToTypedList(partial, doUnflatten(newSource), (List<Object>) result.get(prunedKey));
				} else {
					result.put(prunedKey, createTypedListWithValue(doUnflatten(newSource)));
				}
			} else {
				result.put(partial, doUnflatten(newSource));
			}
		}

		return result;
	}

	private Map<String, Object> flattenMap(Iterator<Entry<String, JsonNode>> source) {

		Map<String, Object> resultMap = new HashMap<>();
		this.doFlatten("", source, resultMap);
		return resultMap;
	}

	private void doFlatten(String propertyPrefix, Iterator<Entry<String, JsonNode>> inputMap,
			Map<String, Object> resultMap) {

		if (StringUtils.hasText(propertyPrefix)) {
			propertyPrefix = propertyPrefix + ".";
		}

		while (inputMap.hasNext()) {

			Entry<String, JsonNode> entry = inputMap.next();
			flattenElement(propertyPrefix + entry.getKey(), entry.getValue(), resultMap);
		}
	}

	private void flattenElement(String propertyPrefix, Object source, Map<String, Object> resultMap) {

		if (!(source instanceof JsonNode)) {

			resultMap.put(propertyPrefix, source);
			return;
		}

		JsonNode element = (JsonNode) source;
		if (element.isArray()) {

			Iterator<JsonNode> nodes = element.elements();

			while (nodes.hasNext()) {

				JsonNode cur = nodes.next();
				if (cur.isArray()) {
					this.falttenCollection(propertyPrefix, cur.elements(), resultMap);
				}
			}

		} else if (element.isContainerNode()) {
			this.doFlatten(propertyPrefix, element.fields(), resultMap);
		} else {
			resultMap.put(propertyPrefix, new DirectFieldAccessFallbackBeanWrapper(element).getPropertyValue("_value"));
		}
	}

	private void falttenCollection(String propertyPrefix, Iterator<JsonNode> list, Map<String, Object> resultMap) {

		int counter = 0;
		while (list.hasNext()) {
			JsonNode element = list.next();
			flattenElement(propertyPrefix + "[" + counter + "]", element, resultMap);
			counter++;
		}
	}

	@SuppressWarnings("unchecked")
	private void appendValueToTypedList(String key, Object value, List<Object> destination) {

		int index = Integer.valueOf(key.substring(key.indexOf('[') + 1, key.length() - 1));
		List<Object> resultList = ((List<Object>) destination.get(1));
		if (resultList.size() < index) {
			resultList.add(value);
		} else {
			resultList.add(index, value);
		}
	}

	private List<Object> createTypedListWithValue(Object value) {

		List<Object> listWithTypeHint = new ArrayList<>();
		listWithTypeHint.add(ArrayList.class.getName()); // why jackson? why?
		List<Object> values = new ArrayList<>();
		values.add(value);
		listWithTypeHint.add(values);
		return listWithTypeHint;
	}
}
