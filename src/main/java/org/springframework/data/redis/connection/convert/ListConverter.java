/*
 * Copyright 2013-2018 the original author or authors.
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

import java.util.ArrayList;
import java.util.List;

import org.springframework.core.convert.converter.Converter;

/**
 * Converts a List of values of one type to a List of values of another type
 *
 * @author Jennifer Hickey
 * @author Mark Paluch
 * @author Christoph Strobl
 * @param <S> The type of elements in the List to convert
 * @param <T> The type of elements in the converted List
 */
public class ListConverter<S, T> implements Converter<List<S>, List<T>> {

	private Converter<S, T> itemConverter;

	/**
	 * @param itemConverter The {@link Converter} to use for converting individual List items
	 */
	public ListConverter(Converter<S, T> itemConverter) {
		this.itemConverter = itemConverter;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.core.convert.converter.Converter#convert(Object)
	 */
	@Override
	public List<T> convert(List<S> source) {

		List<T> results = new ArrayList<>(source.size());

		for (S result : source) {
			results.add(itemConverter.convert(result));
		}

		return results;
	}
}
