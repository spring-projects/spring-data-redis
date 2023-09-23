/*
 * Copyright 2017-2023 the original author or authors.
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
 *  limitations under the License.
 */
package org.springframework.data.redis.cache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Test;

import org.springframework.cache.support.NullValue;
import org.springframework.data.redis.util.ByteUtils;

/**
 * Unit tests for {@link RedisCache}.
 *
 * @author John Blum
 */
class RedisCacheUnitTests {

	@Test // GH-2650
	void cacheRetrieveValueCallsCacheWriterRetrieveCorrectly() throws Exception {

		RedisCacheWriter mockCacheWriter = mock(RedisCacheWriter.class);

		doReturn(CompletableFuture.completedFuture("TEST".getBytes()))
				.when(mockCacheWriter).retrieve(anyString(), any(byte[].class));

		RedisCache cache = new RedisCache("TestCache", mockCacheWriter,
				RedisCacheConfiguration.defaultCacheConfig());

		CompletableFuture<byte[]> value = cache.retrieveValue("TestKey");

		assertThat(value).isNotNull();
		assertThat(new String(value.get())).isEqualTo("TEST");

		verify(mockCacheWriter, times(1)).retrieve(eq("TestCache"), isA(byte[].class));
		verifyNoMoreInteractions(mockCacheWriter);
	}

	@Test // GH-2650
	void nullSafeDeserializedStoreValueWithNullValueIsNullSafe() {

		RedisCacheConfiguration cacheConfiguration = RedisCacheConfiguration.defaultCacheConfig();
		RedisCacheWriter mockCacheWriter = mock(RedisCacheWriter.class);
		RedisCache cache = new RedisCache("TestCache", mockCacheWriter, cacheConfiguration);

		assertThat(cache.nullSafeDeserializedStoreValue(null)).isNull();

		verifyNoInteractions(mockCacheWriter);
	}

	@Test // GH-2650
	void nullSafeDeserializedStoreValueWithBinaryNullValueAllowingNullValues() {

		RedisCacheConfiguration cacheConfiguration = RedisCacheConfiguration.defaultCacheConfig();
		RedisCacheWriter mockCacheWriter = mock(RedisCacheWriter.class);
		RedisCache cache = new RedisCache("TestCache", mockCacheWriter, cacheConfiguration);

		assertThat(cacheConfiguration.getAllowCacheNullValues()).isTrue();
		assertThat(cache.nullSafeDeserializedStoreValue(RedisCache.BINARY_NULL_VALUE)).isNull();

		verifyNoInteractions(mockCacheWriter);
	}

	@Test // GH-2650
	void nullSafeDeserializedStoreValueWithBinaryNullValueDisablingNullValues() {

		RedisCacheConfiguration cacheConfiguration =
				RedisCacheConfiguration.defaultCacheConfig().disableCachingNullValues();

		RedisCacheWriter mockCacheWriter = mock(RedisCacheWriter.class);

		RedisCache cache = new RedisCache("TestCache", mockCacheWriter, cacheConfiguration);

		assertThat(cacheConfiguration.getAllowCacheNullValues()).isFalse();
		assertThat(cache.nullSafeDeserializedStoreValue(RedisCache.BINARY_NULL_VALUE)).isEqualTo(NullValue.INSTANCE);

		verifyNoInteractions(mockCacheWriter);
	}

	@Test // GH-2650
	void nullSafeDeserializedStoreValueWithNonNullValue() {

		RedisCacheConfiguration cacheConfiguration = RedisCacheConfiguration.defaultCacheConfig();

		byte[] serializedValue = ByteUtils.getBytes(cacheConfiguration.getValueSerializationPair()
				.write("TestValue"));

		RedisCacheWriter mockCacheWriter = mock(RedisCacheWriter.class);

		RedisCache cache = new RedisCache("TestCache", mockCacheWriter, cacheConfiguration);

		assertThat(cache.nullSafeDeserializedStoreValue(serializedValue)).isEqualTo("TestValue");

		verifyNoInteractions(mockCacheWriter);
	}
}
