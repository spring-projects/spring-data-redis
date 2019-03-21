/*
 * Copyright 2013-2019 the original author or authors.
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
package org.springframework.data.redis.connection.convert;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.core.convert.converter.Converter;

/**
 * Converts a Map of values of one key/value type to a Map of values of another type
 * 
 * @author Jennifer Hickey
 * @param <S> The type of keys and values in the Map to convert
 * @param <T> The type of keys and values in the converted Map
 */
public class MapConverter<S, T> implements Converter<Map<S, S>, Map<T, T>> {

	private Converter<S, T> itemConverter;

	/**
	 * @param itemConverter The {@link Converter} to use for converting individual Map keys and values
	 */
	public MapConverter(Converter<S, T> itemConverter) {
		this.itemConverter = itemConverter;
	}

	public Map<T, T> convert(Map<S, S> source) {
		if (source == null) {
			return null;
		}
		Map<T, T> results;
		if (source instanceof LinkedHashMap) {
			results = new LinkedHashMap<T, T>();
		} else {
			results = new HashMap<T, T>();
		}
		for (Map.Entry<S, S> result : source.entrySet()) {
			results.put(itemConverter.convert(result.getKey()), itemConverter.convert(result.getValue()));
		}
		return results;
	}

}
