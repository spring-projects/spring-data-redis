/*
 * Copyright 2018-2023 the original author or authors.
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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

import org.springframework.beans.DirectFieldAccessor;
import org.springframework.core.convert.converter.Converter;
import org.springframework.instrument.classloading.ShadowingClassLoader;
import org.springframework.lang.Nullable;

import java.time.Duration;
import java.util.function.BiFunction;

/**
 * Unit tests for {@link RedisCacheConfiguration}.
 *
 * @author Mark Paluch
 */
class RedisCacheConfigurationUnitTests {

	@Test // DATAREDIS-763
	void shouldSetClassLoader() {

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
	void shouldAllowConverterRegistration() {

		RedisCacheConfiguration config = RedisCacheConfiguration.defaultCacheConfig();
		config.configureKeyConverters(registry -> registry.addConverter(new DomainTypeConverter()));

		assertThat(config.getConversionService().canConvert(DomainType.class, String.class)).isTrue();
	}


	@Test
	void shouldGetDynamicTtlGivenTtlProvider() {

		BiFunction<Object, Object, Duration> ttlProvider = (key, val) ->
				Duration.ofSeconds(Integer.parseInt(key + String.valueOf(val)));

		RedisCacheConfiguration defaultCacheConfiguration = RedisCacheConfiguration.defaultCacheConfig()
				.entryTtlProvider(ttlProvider);

		assertThat(defaultCacheConfiguration.getTtl(1, 12)).isEqualTo(Duration.ofSeconds(112));
		assertThat(defaultCacheConfiguration.getTtl(15, 22)).isEqualTo(Duration.ofSeconds(1522));
		assertThat(defaultCacheConfiguration.getTtl(77, 0)).isEqualTo(Duration.ofSeconds(770));
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
