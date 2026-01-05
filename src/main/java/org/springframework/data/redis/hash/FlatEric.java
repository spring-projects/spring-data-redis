/*
 * Copyright 2025-present the original author or authors.
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

import org.jspecify.annotations.Nullable;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

/**
 * Mr. Oizo calling.
 *
 * <pre>
 * +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++-++++++++-+#####
 * +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++---++++++++++#####
 * -+++++++++++++++++++++++++++++++-+++++++++++++-+++-------+++++++++++++++++++++++++---+---++++++######
 * ++++++++++-+++++++++++++++++++++++++++++++++++-----------+++++++++++++++++++++++++---++++-++--+######
 * -+++++++++++++++++++++++++++++++++++++++++++++------------++++++++++++++++++++++++------++---++#####+
 * +++++++++++++++++++++++++++++++++++++++++++-------------------++++++++++++++++++++++-----++---+#####+
 * +++++++++++++++++++++++++++++++++++++++++++---+#+-------+#------++++++++++++++++++++----------+#####+
 * +++++++++++++++++++++++++++++++++++++++++++----+---------+------++++++++++++++++++++----------+####++
 * ++++++++++++++++++++++++++++###########+++----------------------+++++++++++++++++++++---------+#####+
 * +++++++++++++++++++++++++++##############++----------------------++++++++++++++++++++---------+####++
 * ++++++++++++++++++++++++++##############+++----------------------++++++++++++++++++++++-------######+
 * ++++++++++++++++++++++++++###############+-----------------------++++++++++++++++++++--------+######+
 * ++++++++++++++++++++++++++##############+++--------###+----------++++++++++++++++++++--------+######+
 * ++++++#+++++++++++++++++++###############++++-------##-----------+++++++++++++++++++++-------+######+
 * ++++++#++++++++++++++++++++##############+++++++-----------------++++++++++++++++++++++++++++########
 * ++++++#++++++++++++++++++++##############++++----+++------------++++++++++++++++++++++++++++#########
 * +++++++++++++++++++++++++++############+++-+++++-----------------+++++++++++++++++++++++++###########
 * +++++++#+++++++++++++++++++###########+++++++-+-----+-------------++++++++++++++++++++++#############
 * +++++++#+++++++++++++++++#########++++++++---++--------------------++++++++++++++++++################
 * ++++++++#+++++++++++++++########+---+++++----++--------+------------++++++++++++++###################
 * ++++++++#++++++++++++++#######+----++++++--------+---------------------++++++########################
 * ++++++++#++++++++++++#######+-----+++++++----+--+----------------------++############################
 * ++++++++#++++++++++########-------+++++++--------------------------------############################
 * +++++++++#++++++++########+-----+###++++++--------------------------------###########################
 * +++++++++#++++++##########------+###++----+--------------------------------##########################
 * +++++++++#++++++#########+------###++++-+---------------------------+-------#########################
 * +++++++++##+++###########+------##+++++-----------------------------++------+########################
 * +++++++++###+############+------###+++++-----------------------------+++-----+#######################
 * ++++++++++###############+-----+##++++++-----------------------------+#+-----+#######################
 * ++++++++++###############+------##+++++------------------------------+#+------#######################
 * +++++++++################+-------+#++++-----------------------------+##++-----+######################
 * +++++++++#################+-------+#++++---------------------------++##++++---+######################
 * +++++++++##################+-------+#++++------------------------+++####++----+######################
 * ++++++++####################++-------++++++++++------+---++-+++++++###++------+######################
 * ++++++++#####################+++-------++++++++++++++++++++++++++####+--------#######################
 * +++++++########################+---------####++++++++++++++++++++------------########################
 * ++++++#########################++---------++#######++###+------------------+#########################
 * ++++############################++---------++++######++++++---------------+##########################
 * ++##################################++---+-++++###+------++---------------###########################
 * ####################################++++++++++++-----------++-+-----------+##########################
 * ####################################++++++++----------------+##+----#+----###########################
 * #####################################+++----------------++###########+---+###########################
 * ###################################++---------------+++------+++#######+++###########################
 * ##############################+++----------------+++++++-++--------++################################
 * #########################++------------------+++++++++++++++-+-------+####+#+++######################
 * ######################+--------------------+#####+++++++++++++--------++#++#+++++++#+################
 * ####################+--------------------##############++++++----------+++++#+++++++#+###############
 * ####################-------------------+##################++++---------++++++++++++++++++############
 * ####################-----------------+++++++++++++##########++--------+++++++++++++++++++++++########
 * #############+######+-------------++++++++++++++++++++#######+-------+++++++++++++++++++++++++++#####
 * #########+###+#++++#++---------++++++++++++++++++++++++########+++-+++++++++++++++++++++++++++++++###
 * #######++++++++++#++##++-----+++++++++++++++++++++++++++++++###++++++++++++++++++++++++++++++++++++++
 * #####+++##++#+++#+++##+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 * </pre>
 */
class FlatEric {

	/**
	 * Un-Flatten a Map with dot notation keys into a Map with nested Maps and Lists.
	 *
	 * @param source source map.
	 * @return
	 */
	@SuppressWarnings("unchecked")
	static Map<String, Object> unflatten(Map<String, Object> source) {

		Map<String, Object> result = CollectionUtils.newLinkedHashMap(source.size());
		Set<String> treatSeparate = CollectionUtils.newLinkedHashSet(source.size());

		for (Map.Entry<String, Object> entry : source.entrySet()) {

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
			for (Map.Entry<String, Object> entry : source.entrySet()) {
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
					addValueToTypedListAtIndex((List<Object>) result.get(nonIndexPartial), index, unflatten(newSource));
				} else {
					result.put(nonIndexPartial, createTypedListWithValue(index, unflatten(newSource)));
				}
			} else {
				result.put(partial, unflatten(newSource));
			}
		}

		return result;
	}

	@SuppressWarnings("unchecked")
	private static void addValueToTypedListAtIndex(List<Object> listWithTypeHint, int index, Object value) {

		List<Object> valueList = (List<Object>) listWithTypeHint.get(1);

		if (index >= valueList.size()) {
			int initialCapacity = index + 1;
			List<Object> newValueList = new ArrayList<>(initialCapacity);
			Collections.copy(org.springframework.data.redis.support.collections.CollectionUtils.initializeList(newValueList,
					initialCapacity), valueList);
			listWithTypeHint.set(1, newValueList);
			valueList = newValueList;
		}

		valueList.set(index, value);
	}

	private static List<Object> createTypedListWithValue(int index, Object value) {

		int initialCapacity = index + 1;

		List<Object> valueList = org.springframework.data.redis.support.collections.CollectionUtils
				.initializeList(new ArrayList<>(initialCapacity), initialCapacity);
		valueList.set(index, value);

		List<Object> listWithTypeHint = new ArrayList<>();
		listWithTypeHint.add(ArrayList.class.getName());
		listWithTypeHint.add(valueList);

		return listWithTypeHint;
	}

	private static boolean isIndexed(String value) {
		return value.indexOf('[') > -1;
	}

	private static boolean isNotIndexed(String value) {
		return !isIndexed(value);
	}

	private static boolean isNonNestedIndexed(String value) {
		return value.endsWith("]");
	}

	private static int getIndex(String indexedValue) {
		return Integer.parseInt(indexedValue.substring(indexedValue.indexOf('[') + 1, indexedValue.length() - 1));
	}

	private static String stripIndex(String indexedValue) {

		int indexOfLeftBracket = indexedValue.indexOf("[");

		return indexOfLeftBracket > -1 ? indexedValue.substring(0, indexOfLeftBracket) : indexedValue;
	}

	/**
	 * Flatten a {@link Map} containing nested values, Maps and Lists into a map with property path (dot-notation) keys.
	 *
	 * @param adapterFactory
	 * @param source
	 * @return
	 * @param
	 */
	static Map<String, Object> flatten(FlatEric.JsonNodeAdapterFactory adapterFactory,
			Collection<? extends Map.Entry<String, ?>> source) {

		Map<String, Object> resultMap = new HashMap<>(source.size());
		flatten(adapterFactory, "", source, resultMap::put);
		return resultMap;
	}

	private static void flatten(FlatEric.JsonNodeAdapterFactory adapterFactory, String propertyPrefix,
			Collection<? extends Map.Entry<String, ?>> inputMap, BiConsumer<String, Object> sink) {

		if (StringUtils.hasText(propertyPrefix)) {
			propertyPrefix = propertyPrefix + ".";
		}

		for (Map.Entry<String, ?> entry : inputMap) {
			flattenElement(adapterFactory, propertyPrefix + entry.getKey(), entry.getValue(), sink);
		}
	}

	private static void flattenElement(FlatEric.JsonNodeAdapterFactory adapterFactory, String propertyPrefix,
			Object source, BiConsumer<String, Object> sink) {

		if (!adapterFactory.isJsonNode(source)) {
			sink.accept(propertyPrefix, source);
			return;
		}

		FlatEric.JsonNodeAdapter adapter = adapterFactory.adapt(source);

		if (adapter.isArray()) {

			Iterator<? extends JsonNodeAdapter> nodes = adapter.values().iterator();

			while (nodes.hasNext()) {

				FlatEric.JsonNodeAdapter currentNode = nodes.next();

				if (currentNode.isArray()) {
					flattenCollection(adapterFactory, propertyPrefix, currentNode.values(), sink);
				} else if (nodes.hasNext() && mightBeJavaType(currentNode)) {

					FlatEric.JsonNodeAdapter next = nodes.next();

					if (next.isArray()) {
						flattenCollection(adapterFactory, propertyPrefix, next.values(), sink);
					}
					if (currentNode.asString().equals("java.util.Date")) {
						sink.accept(propertyPrefix, next.asString());
						break;
					}
					if (next.isNumber()) {
						sink.accept(propertyPrefix, next.numberValue());
						break;
					}
					if (next.isString()) {
						sink.accept(propertyPrefix, next.stringValue());
						break;
					}
					if (next.isBoolean()) {
						sink.accept(propertyPrefix, next.booleanValue());
						break;
					}
					if (next.isBinary()) {

						try {
							sink.accept(propertyPrefix, next.binaryValue());
						} catch (Exception ex) {
							throw new IllegalStateException("Cannot read binary value '%s'".formatted(propertyPrefix), ex);
						}

						break;
					}
				}
			}
		} else if (adapter.isObject()) {
			flatten(adapterFactory, propertyPrefix, adapter.properties(), sink);
		} else {

			switch (adapter.getNodeType()) {
				case STRING -> sink.accept(propertyPrefix, adapter.stringValue());
				case NUMBER -> sink.accept(propertyPrefix, adapter.numberValue());
				case BOOLEAN -> sink.accept(propertyPrefix, adapter.booleanValue());
				case BINARY -> {
					try {
						sink.accept(propertyPrefix, adapter.binaryValue());
					} catch (Exception e) {
						throw new IllegalStateException(e);
					}
				}
				default -> sink.accept(propertyPrefix, adapter.getDirectValue());
			}
		}
	}

	private static boolean mightBeJavaType(FlatEric.JsonNodeAdapter node) {

		String textValue = node.asString();
		return javax.lang.model.SourceVersion.isName(textValue);
	}

	private static void flattenCollection(FlatEric.JsonNodeAdapterFactory adapterFactory, String propertyPrefix,
			Collection<? extends FlatEric.JsonNodeAdapter> list, BiConsumer<String, Object> sink) {

		Iterator<? extends FlatEric.JsonNodeAdapter> iterator = list.iterator();

		for (int counter = 0; iterator.hasNext(); counter++) {
			FlatEric.JsonNodeAdapter element = iterator.next();
			flattenElement(adapterFactory, propertyPrefix + "[" + counter + "]", element, sink);
		}
	}

	/**
	 * Factory to create a {@link JsonNodeAdapter} from a given node type.
	 */
	public interface JsonNodeAdapterFactory {

		JsonNodeAdapter adapt(Object node);

		boolean isJsonNode(Object value);
	}

	public enum JsonNodeType {
		ARRAY, BINARY, BOOLEAN, MISSING, NULL, NUMBER, OBJECT, POJO, STRING
	}

	/**
	 * Wrapper around a JSON node to adapt between Jackson 2 and 3 JSON nodes.
	 */
	public interface JsonNodeAdapter {

		JsonNodeType getNodeType();

		boolean isArray();

		Collection<? extends JsonNodeAdapter> values();

		String asString();

		boolean isNumber();

		Number numberValue();

		boolean isString();

		String stringValue();

		boolean isBoolean();

		boolean booleanValue();

		boolean isBinary();

		byte[] binaryValue();

		boolean isObject();

		Collection<Map.Entry<String, JsonNodeAdapter>> properties();

		@Nullable
		Object getDirectValue();
	}

}
