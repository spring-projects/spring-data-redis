/*
 * Copyright 2011-2025 the original author or authors.
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
package org.springframework.data.redis.serializer;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import org.springframework.beans.BeansException;
import org.springframework.beans.TypeConverter;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Generic String to byte[] (and back) serializer. Relies on the Spring {@link ConversionService} to transform objects
 * into String and vice versa. The Strings are convert into bytes and vice-versa using the specified charset (by default
 * UTF-8). <b>Note:</b> The conversion service initialization happens automatically if the class is defined as a Spring
 * bean. <b>Note:</b> Does not handle nulls in any special way delegating everything to the container.
 *
 * @author Costin Leau
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public class GenericToStringSerializer<T> implements RedisSerializer<T>, BeanFactoryAware {

	private final Class<T> type;
	private final Charset charset;
	private Converter converter;

	public GenericToStringSerializer(Class<T> type) {
		this(type, StandardCharsets.UTF_8);
	}

	public GenericToStringSerializer(Class<T> type, Charset charset) {

		Assert.notNull(type, "Type must not be null");

		this.type = type;
		this.charset = charset;
		this.converter = new Converter(DefaultConversionService.getSharedInstance());
	}

	/**
	 * Set the {@link ConversionService} to be used.
	 *
	 * @param conversionService the conversion service to be used, must not be {@literal null}.
	 */
	public void setConversionService(ConversionService conversionService) {

		Assert.notNull(conversionService, "ConversionService must not be null");

		converter = new Converter(conversionService);
	}

	/**
	 * Set the {@link TypeConverter} to be used.
	 *
	 * @param typeConverter the conversion service to be used, must not be {@literal null}.
	 */
	public void setTypeConverter(TypeConverter typeConverter) {

		Assert.notNull(typeConverter, "TypeConverter must not be null");

		converter = new Converter(typeConverter);
	}

	@Override
	public byte[] serialize(@Nullable T value) {

		if (value == null) {
			return null;
		}

		String string = converter.convert(value, String.class);
		return string.getBytes(charset);
	}

	@Override
	public T deserialize(@Nullable byte[] bytes) {

		if (bytes == null) {
			return null;
		}

		String string = new String(bytes, charset);
		return converter.convert(string, type);
	}

	@Override
	public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
		// no-op
	}

	private final static class Converter {

		private final @Nullable ConversionService conversionService;
		private final @Nullable TypeConverter typeConverter;

		public Converter(ConversionService conversionService) {
			this.conversionService = conversionService;
			this.typeConverter = null;
		}

		public Converter(TypeConverter typeConverter) {
			this.conversionService = null;
			this.typeConverter = typeConverter;
		}

		@Nullable
		<E> E convert(Object value, Class<E> targetType) {

			return conversionService != null ? conversionService.convert(value, targetType)
					: typeConverter.convertIfNecessary(value, targetType);
		}
	}
}
