/*
 * Copyright 2017-2018 the original author or authors.
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
package org.springframework.data.redis.cache;

import static org.assertj.core.api.Assertions.*;

import java.util.Collections;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.cache.Cache;
import org.springframework.cache.transaction.TransactionAwareCacheDecorator;
import org.springframework.test.util.ReflectionTestUtils;

/**
 * Unit tests for {@link RedisCacheManager}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@RunWith(MockitoJUnitRunner.class)
public class RedisCacheManagerUnitTests {

	@Mock RedisCacheWriter cacheWriter;

	@Test // DATAREDIS-481
	public void missingCacheShouldBeCreatedWithDefaultConfiguration() {

		RedisCacheConfiguration configuration = RedisCacheConfiguration.defaultCacheConfig().disableKeyPrefix();

		RedisCacheManager cm = RedisCacheManager.builder(cacheWriter).cacheDefaults(configuration).build();
		cm.afterPropertiesSet();

		assertThat(cm.getMissingCache("new-cache").getCacheConfiguration()).isEqualTo(configuration);
	}

	@Test // DATAREDIS-481
	public void appliesDefaultConfigurationToInitialCache() {

		RedisCacheConfiguration withPrefix = RedisCacheConfiguration.defaultCacheConfig().disableKeyPrefix();
		RedisCacheConfiguration withoutPrefix = RedisCacheConfiguration.defaultCacheConfig().disableKeyPrefix();

		RedisCacheManager cm = RedisCacheManager.builder(cacheWriter).cacheDefaults(withPrefix) //
				.initialCacheNames(Collections.singleton("first-cache")) //
				.cacheDefaults(withoutPrefix) //
				.initialCacheNames(Collections.singleton("second-cache")) //
				.build();

		cm.afterPropertiesSet();

		assertThat(((RedisCache) cm.getCache("first-cache")).getCacheConfiguration()).isEqualTo(withPrefix);
		assertThat(((RedisCache) cm.getCache("second-cache")).getCacheConfiguration()).isEqualTo(withoutPrefix);
		assertThat(((RedisCache) cm.getCache("other-cache")).getCacheConfiguration()).isEqualTo(withoutPrefix);
	}

	@Test // DATAREDIS-481, DATAREDIS-728
	public void predefinedCacheShouldBeCreatedWithSpecificConfig() {

		RedisCacheConfiguration configuration = RedisCacheConfiguration.defaultCacheConfig().disableKeyPrefix();

		RedisCacheManager cm = RedisCacheManager.builder(cacheWriter)
				.withInitialCacheConfigurations(Collections.singletonMap("predefined-cache", configuration))
				.withInitialCacheConfigurations(Collections.singletonMap("another-predefined-cache", configuration)).build();
		cm.afterPropertiesSet();

		assertThat(((RedisCache) cm.getCache("predefined-cache")).getCacheConfiguration()).isEqualTo(configuration);
		assertThat(((RedisCache) cm.getCache("another-predefined-cache")).getCacheConfiguration()).isEqualTo(configuration);
		assertThat(cm.getMissingCache("new-cache").getCacheConfiguration()).isNotEqualTo(configuration);
	}

	@Test // DATAREDIS-481
	public void transactionAwareCacheManagerShouldDecoracteCache() {

		Cache cache = RedisCacheManager.builder(cacheWriter).transactionAware().build().getCache("decoracted-cache");

		assertThat(cache).isInstanceOfAny(TransactionAwareCacheDecorator.class);
		assertThat(ReflectionTestUtils.getField(cache, "targetCache")).isInstanceOf(RedisCache.class);
	}

	@Test // DATAREDIS-767
	public void lockedCacheManagerShouldPreventInFlightCacheCreation() {

		RedisCacheManager cacheManager = RedisCacheManager.builder(cacheWriter).disableCreateOnMissingCache().build();
		cacheManager.afterPropertiesSet();

		assertThat(cacheManager.getCache("not-configured")).isNull();
	}

	@Test // DATAREDIS-767
	public void lockedCacheManagerShouldStillReturnPreconfiguredCaches() {

		RedisCacheManager cacheManager = RedisCacheManager.builder(cacheWriter)
				.initialCacheNames(Collections.singleton("configured")).disableCreateOnMissingCache().build();
		cacheManager.afterPropertiesSet();

		assertThat(cacheManager.getCache("configured")).isNotNull();
	}
}
