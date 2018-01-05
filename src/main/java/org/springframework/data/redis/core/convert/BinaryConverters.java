/*
 * Copyright 2015-2018 the original author or authors.
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

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Date;

import org.springframework.core.convert.converter.Converter;
import org.springframework.core.convert.converter.ConverterFactory;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.convert.WritingConverter;
import org.springframework.util.NumberUtils;
import org.springframework.util.ObjectUtils;

/**
 * Set of {@link ReadingConverter} and {@link WritingConverter} used to convert Objects into binary format.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 1.7
 */
final class BinaryConverters {

	/**
	 * Use {@literal UTF-8} as default charset.
	 */
	public static final Charset CHARSET = StandardCharsets.UTF_8;

	private BinaryConverters() {}

	/**
	 * @author Christoph Strobl
	 * @since 1.7
	 */
	static class StringBasedConverter {

		byte[] fromString(String source) {
			return source.getBytes(CHARSET);
		}

		String toString(byte[] source) {
			return new String(source, CHARSET);
		}
	}

	/**
	 * @author Christoph Strobl
	 * @since 1.7
	 */
	@WritingConverter
	static class StringToBytesConverter extends StringBasedConverter implements Converter<String, byte[]> {

		@Override
		public byte[] convert(String source) {
			return fromString(source);
		}
	}

	/**
	 * @author Christoph Strobl
	 * @since 1.7
	 */
	@ReadingConverter
	static class BytesToStringConverter extends StringBasedConverter implements Converter<byte[], String> {

		@Override
		public String convert(byte[] source) {
			return toString(source);
		}

	}

	/**
	 * @author Christoph Strobl
	 * @since 1.7
	 */
	@WritingConverter
	static class NumberToBytesConverter extends StringBasedConverter implements Converter<Number, byte[]> {

		@Override
		public byte[] convert(Number source) {
			return fromString(source.toString());
		}
	}

	/**
	 * @author Christoph Strobl
	 * @author Mark Paluch
	 * @since 1.7
	 */
	@WritingConverter
	static class EnumToBytesConverter extends StringBasedConverter implements Converter<Enum<?>, byte[]> {

		@Override
		public byte[] convert(Enum<?> source) {
			return fromString(source.name());
		}
	}

	/**
	 * @author Christoph Strobl
	 * @since 1.7
	 */
	@ReadingConverter
	static final class BytesToEnumConverterFactory implements ConverterFactory<byte[], Enum<?>> {

		@Override
		@SuppressWarnings({ "unchecked", "rawtypes" })
		public <T extends Enum<?>> Converter<byte[], T> getConverter(Class<T> targetType) {

			Class<?> enumType = targetType;
			while (enumType != null && !enumType.isEnum()) {
				enumType = enumType.getSuperclass();
			}
			if (enumType == null) {
				throw new IllegalArgumentException("The target type " + targetType.getName() + " does not refer to an enum");
			}
			return new BytesToEnum(enumType);
		}

		/**
		 * @author Christoph Strobl
		 * @since 1.7
		 */
		private class BytesToEnum<T extends Enum<T>> extends StringBasedConverter implements Converter<byte[], T> {

			private final Class<T> enumType;

			public BytesToEnum(Class<T> enumType) {
				this.enumType = enumType;
			}

			@Override
			public T convert(byte[] source) {

				if (ObjectUtils.isEmpty(source)) {
					return null;
				}

				return Enum.valueOf(this.enumType, toString(source).trim());
			}
		}
	}

	/**
	 * @author Christoph Strobl
	 * @since 1.7
	 */
	@ReadingConverter
	static class BytesToNumberConverterFactory implements ConverterFactory<byte[], Number> {

		@Override
		public <T extends Number> Converter<byte[], T> getConverter(Class<T> targetType) {
			return new BytesToNumberConverter<>(targetType);
		}

		private static final class BytesToNumberConverter<T extends Number> extends StringBasedConverter
				implements Converter<byte[], T> {

			private final Class<T> targetType;

			public BytesToNumberConverter(Class<T> targetType) {
				this.targetType = targetType;
			}

			@Override
			public T convert(byte[] source) {

				if (ObjectUtils.isEmpty(source)) {
					return null;
				}

				return NumberUtils.parseNumber(toString(source), targetType);
			}
		}
	}

	/**
	 * @author Christoph Strobl
	 * @since 1.7
	 */
	@WritingConverter
	static class BooleanToBytesConverter extends StringBasedConverter implements Converter<Boolean, byte[]> {

		byte[] _true = fromString("1");
		byte[] _false = fromString("0");

		@Override
		public byte[] convert(Boolean source) {
			return source.booleanValue() ? _true : _false;
		}
	}

	/**
	 * @author Christoph Strobl
	 * @since 1.7
	 */
	@ReadingConverter
	static class BytesToBooleanConverter extends StringBasedConverter implements Converter<byte[], Boolean> {

		@Override
		public Boolean convert(byte[] source) {

			if (ObjectUtils.isEmpty(source)) {
				return null;
			}

			String value = toString(source);
			return ("1".equals(value) || "true".equalsIgnoreCase(value)) ? Boolean.TRUE : Boolean.FALSE;
		}
	}

	/**
	 * @author Christoph Strobl
	 * @since 1.7
	 */
	@WritingConverter
	static class DateToBytesConverter extends StringBasedConverter implements Converter<Date, byte[]> {

		@Override
		public byte[] convert(Date source) {
			return fromString(Long.toString(source.getTime()));
		}
	}

	/**
	 * @author Christoph Strobl
	 * @since 1.7
	 */
	@ReadingConverter
	static class BytesToDateConverter extends StringBasedConverter implements Converter<byte[], Date> {

		@Override
		public Date convert(byte[] source) {

			if (ObjectUtils.isEmpty(source)) {
				return null;
			}

			String value = toString(source);
			try {
				return new Date(NumberUtils.parseNumber(value, Long.class));
			} catch (NumberFormatException nfe) {
				// ignore
			}

			try {
				return DateFormat.getInstance().parse(value);
			} catch (ParseException e) {
				// ignore
			}

			throw new IllegalArgumentException(String.format("Cannot parse date out of %s", Arrays.toString(source)));
		}
	}
}
