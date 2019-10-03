/*
 * Copyright 2018-2019 the original author or authors.
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
package org.springframework.data.redis.cache;

import static org.assertj.core.api.Assertions.*;

import org.junit.Test;
import org.springframework.beans.DirectFieldAccessor;
import org.springframework.core.convert.converter.Converter;
import org.springframework.instrument.classloading.ShadowingClassLoader;
import org.springframework.lang.Nullable;

/**
 * Unit tests for {@link RedisCacheConfiguration}.
 *
 * @author Mark Paluch
 */
public class RedisCacheConfigurationUnitTests {

	@Test // DATAREDIS-763
	public void shouldSetClassLoader() {

		ShadowingClassLoader classLoader = new ShadowingClassLoader(getClass().getClassLoader());

		RedisCacheConfiguration config = RedisCacheConfiguration.defaultCacheConfig(classLoader);

		Object adapter = new DirectFieldAccessor(config.getValueSerializationPair().getReader())
				.getPropertyValue("serializer");
		Object deserializerConverter = new DirectFieldAccessor(adapter).getPropertyValue("deserializer");
		Object deserializer = new DirectFieldAccessor(deserializerConverter).getPropertyValue("deserializer");
		Object usedClassLoader = new DirectFieldAccessor(deserializer).getPropertyValue("classLoader");

		assertThat(usedClassLoader).isSameAs(classLoader);
	}

	@Test // DATAREDIS-1032
	public void shouldAllowConverterRegistration() {

		RedisCacheConfiguration config = RedisCacheConfiguration.defaultCacheConfig();
		config.configureKeyConverters(registry -> registry.addConverter(new DomainTypeConverter()));

		assertThat(config.getConversionService().canConvert(DomainType.class, String.class)).isTrue();
	}

	@Test // DATAREDIS-1041
	public void shouldAppendCacheNameAfterKeyPrefix() {

		RedisCacheConfiguration config = RedisCacheConfiguration.defaultCacheConfig()
				.prefixKeysWith("prefix");

		assertThat(config.getKeyPrefixFor("cacheName")).isEqualTo("prefix::cacheName::");
	}

	private static class DomainType {

	}

	static class DomainTypeConverter implements Converter<DomainType, String> {

		@Nullable
		@Override
		public String convert(DomainType source) {
			return null;
		}
	}
}
