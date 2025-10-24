/*
 * Copyright 2018-2025 the original author or authors.
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
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.time.Duration;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;

import org.springframework.beans.DirectFieldAccessor;
import org.springframework.core.convert.converter.Converter;
import org.springframework.instrument.classloading.ShadowingClassLoader;

/**
 * Unit tests for {@link RedisCacheConfiguration}.
 *
 * @author Mark Paluch
 * @author John Blum
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

	@Test // GH-2628
	void getTtlReturnsFixedDuration() {

		Duration sixtySeconds = Duration.ofSeconds(60);

		RedisCacheConfiguration cacheConfiguration = RedisCacheConfiguration.defaultCacheConfig().entryTtl(sixtySeconds);

		assertThat(cacheConfiguration).isNotNull();
		assertThat(cacheConfiguration.getTtlFunction().getTimeToLive("foo", null)).isEqualByComparingTo(sixtySeconds);
		assertThat(cacheConfiguration.getTtlFunction().getTimeToLive("foo", null)).isEqualByComparingTo(sixtySeconds); // does
																																																										// not
																																																										// change!
	}

	@Test // GH-2628
	void getTtlReturnsDynamicDuration() {

		Duration thirtyMinutes = Duration.ofMinutes(30);
		Duration twoHours = Duration.ofHours(2);

		RedisCacheWriter.TtlFunction mockTtlFunction = mock(RedisCacheWriter.TtlFunction.class);

		doReturn(thirtyMinutes).doReturn(twoHours).when(mockTtlFunction).getTimeToLive(any(), any());

		RedisCacheConfiguration cacheConfiguration = RedisCacheConfiguration.defaultCacheConfig().entryTtl(mockTtlFunction);

		assertThat(cacheConfiguration.getTtlFunction().getTimeToLive("foo", null)).isEqualTo(thirtyMinutes);
		assertThat(cacheConfiguration.getTtlFunction().getTimeToLive("foo", null)).isEqualTo(twoHours);

		verify(mockTtlFunction, times(2)).getTimeToLive(any(), isNull());
		verifyNoMoreInteractions(mockTtlFunction);
	}

	@Test // GH-2351
	void enableTtiExpirationShouldConfigureTti() {

		RedisCacheConfiguration cacheConfiguration = RedisCacheConfiguration.defaultCacheConfig();

		assertThat(cacheConfiguration).isNotNull();
		assertThat(cacheConfiguration.isTimeToIdleEnabled()).isFalse();

		RedisCacheConfiguration ttiEnabledCacheConfiguration = cacheConfiguration.enableTimeToIdle();

		assertThat(ttiEnabledCacheConfiguration).isNotNull();
		assertThat(ttiEnabledCacheConfiguration).isNotSameAs(cacheConfiguration);
		assertThat(ttiEnabledCacheConfiguration.isTimeToIdleEnabled()).isTrue();
	}

	private static class DomainType {

	}

	static class DomainTypeConverter implements Converter<DomainType, String> {

		@Override
		public @Nullable String convert(DomainType source) {
			return null;
		}
	}
}
