/*
 * Copyright 2013 the original author or authors.
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
package org.springframework.data.redis.connection.convert;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

import org.springframework.core.convert.converter.Converter;

/**
 * Converts a Set of values of one type to a Set of values of another type
 *
 * @author Jennifer Hickey
 *
 * @param <S>
 *            The type of elements in the Set to convert
 * @param <T>
 *            The type of elements in the converted Set
 */
public class SetConverter<S, T> implements Converter<Set<S>, Set<T>> {

	private Converter<S, T> itemConverter;

	/**
	 *
	 * @param itemConverter
	 *            The {@link Converter} to use for converting individual Set
	 *            items
	 */
	public SetConverter(Converter<S, T> itemConverter) {
		this.itemConverter = itemConverter;
	}

	public Set<T> convert(Set<S> source) {
		if (source == null) {
			return null;
		}
		Set<T> results;
		if (source instanceof LinkedHashSet) {
			results = new LinkedHashSet<T>();
		} else {
			results = new HashSet<T>();
		}
		for (S result : source) {
			results.add(itemConverter.convert(result));
		}
		return results;
	}

}
