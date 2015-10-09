/*
 * Copyright 2015 the original author or authors.
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
package org.springframework.data.redis.core.convert;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.springframework.core.GenericTypeResolver;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.convert.converter.ConverterFactory;
import org.springframework.core.convert.converter.GenericConverter;
import org.springframework.core.convert.converter.GenericConverter.ConvertiblePair;
import org.springframework.core.convert.support.GenericConversionService;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.convert.WritingConverter;

/**
 * @author Christoph Strobl
 */
public class CustomConversions {

	private final Set<ConvertiblePair> readingPairs;
	private final Set<ConvertiblePair> writingPairs;

	private final List<Object> converters;

	public CustomConversions() {
		this(Collections.emptyList());
	}

	public CustomConversions(List<?> converters) {

		this.readingPairs = new LinkedHashSet<ConvertiblePair>();
		this.writingPairs = new LinkedHashSet<ConvertiblePair>();

		List<Object> toRegister = new ArrayList<Object>(converters);

		toRegister.add(new BinaryConverters.StringToBytesConverter());
		toRegister.add(new BinaryConverters.BytesToStringConverter());
		toRegister.add(new BinaryConverters.NumberToBytesConverter());
		toRegister.add(new BinaryConverters.BytesToNumberConverterFactory());
		toRegister.add(new BinaryConverters.EnumToBytesConverter());
		toRegister.add(new BinaryConverters.BytesToEnumConverterFactory());
		toRegister.add(new BinaryConverters.BooleanToBytesConverter());
		toRegister.add(new BinaryConverters.BytesToBooleanConverter());
		toRegister.add(new BinaryConverters.DateToBytesConverter());
		toRegister.add(new BinaryConverters.BytesToDateConverter());

		for (Object c : toRegister) {
			registerConversion(c);
		}

		Collections.reverse(toRegister);

		this.converters = Collections.unmodifiableList(toRegister);
	}

	/**
	 * Populates the given {@link GenericConversionService} with the convertes registered.
	 * 
	 * @param conversionService
	 */
	public void registerConvertersIn(GenericConversionService conversionService) {

		for (Object converter : converters) {

			boolean added = false;

			if (converter instanceof Converter) {
				conversionService.addConverter((Converter<?, ?>) converter);
				added = true;
			}

			if (converter instanceof ConverterFactory) {
				conversionService.addConverterFactory((ConverterFactory<?, ?>) converter);
				added = true;
			}

			if (converter instanceof GenericConverter) {
				conversionService.addConverter((GenericConverter) converter);
				added = true;
			}

			if (!added) {
				throw new IllegalArgumentException("Given set contains element that is neither Converter nor ConverterFactory!");
			}
		}
	}

	/**
	 * Registers a conversion for the given converter. Inspects either generics or the {@link ConvertiblePair}s returned
	 * by a {@link GenericConverter}.
	 * 
	 * @param converter
	 */
	private void registerConversion(Object converter) {

		Class<?> type = converter.getClass();
		boolean isWriting = type.isAnnotationPresent(WritingConverter.class);
		boolean isReading = type.isAnnotationPresent(ReadingConverter.class);

		if (converter instanceof GenericConverter) {
			GenericConverter genericConverter = (GenericConverter) converter;
			for (ConvertiblePair pair : genericConverter.getConvertibleTypes()) {
				register(pair, isReading, isWriting);
			}
		} else if (converter instanceof Converter) {
			Class<?>[] arguments = GenericTypeResolver.resolveTypeArguments(converter.getClass(), Converter.class);
			register(new ConvertiblePair(arguments[0], arguments[1]), isReading, isWriting);
		} else if (converter instanceof ConverterFactory) {

			Class<?>[] arguments = GenericTypeResolver.resolveTypeArguments(converter.getClass(), ConverterFactory.class);
			register(new ConvertiblePair(arguments[0], arguments[1]), isReading, isWriting);
		}

		else {
			throw new IllegalArgumentException("Unsupported Converter type!");
		}
	}

	private void register(ConvertiblePair pair, boolean isReading, boolean isWriting) {

		if (isReading) {
			readingPairs.add(pair);
		}

		if (isWriting) {
			writingPairs.add(pair);
		}
	}

}
