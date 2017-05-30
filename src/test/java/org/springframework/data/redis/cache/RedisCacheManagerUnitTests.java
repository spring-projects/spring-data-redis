/*
 * Copyright 2017 the original author or authors.
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
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.test.util.ReflectionTestUtils;

/**
 * @author Christoph Strobl
 */
@RunWith(MockitoJUnitRunner.class)
public class RedisCacheManagerUnitTests {

	@Mock RedisCacheWriter cacheWriter;

	@Test // DATAREDIS-481
	public void missingCacheShouldBeCreatedWithDefaultConfiguration() {

		RedisCacheConfiguration configuration = RedisCacheConfiguration.defaultCacheConfig().disableKeyPrefix();

		RedisCacheManager cm = RedisCacheManager.usingCacheWriter(cacheWriter).withCacheDefaults(configuration).createAndGet();
		assertThat(cm.getMissingCache("new-cache").getCacheConfiguration()).isEqualTo(configuration);
	}

	@Test // DATAREDIS-481
	public void predefinedCacheShouldBeCreatedWithSpecificConfig() {

		RedisCacheConfiguration configuration = RedisCacheConfiguration.defaultCacheConfig().disableKeyPrefix();

		RedisCacheManager cm = RedisCacheManager.usingCacheWriter(cacheWriter)
				.withInitialCacheConfigurations(Collections.singletonMap("predefined-cache", configuration)).createAndGet();

		assertThat(((RedisCache) cm.getCache("predefined-cache")).getCacheConfiguration()).isEqualTo(configuration);
		assertThat(cm.getMissingCache("new-cache").getCacheConfiguration()).isNotEqualTo(configuration);
	}

	@Test // DATAREDIS-481
	public void transactionAwareCacheManagerShouldDecoracteCache() {

		Cache cache = RedisCacheManager.usingCacheWriter(cacheWriter).transactionAware().createAndGet()
				.getCache("decoracted-cache");

		assertThat(cache).isInstanceOfAny(TransactionAwareCacheDecorator.class);
		assertThat(ReflectionTestUtils.getField(cache, "targetCache")).isInstanceOf(RedisCache.class);
	}

	@Bean
	public RedisCacheManager cacheManager(RedisConnectionFactory connectionFactory) {
		return RedisCacheManager.usingRawConnectionFactory(connectionFactory)
				.withCacheDefaults(RedisCacheConfiguration.defaultCacheConfig())
				.withInitialCacheConfigurations(Collections.singletonMap("predefined", RedisCacheConfiguration.defaultCacheConfig().disableCachingNullValues()))
				.transactionAware()
				.createAndGet();
	}

}
