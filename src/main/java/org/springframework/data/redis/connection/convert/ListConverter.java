package org.springframework.data.redis.connection.convert;

import java.util.ArrayList;
import java.util.List;

import org.springframework.core.convert.converter.Converter;

/**
 * Converts a List of values of one type to a List of values of another type
 * 
 * @author Jennifer Hickey
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

	public List<T> convert(List<S> source) {
		if (source == null) {
			return null;
		}
		List<T> results = new ArrayList<T>();
		for (S result : source) {
			results.add(itemConverter.convert(result));
		}
		return results;
	}

}
