/*
 * Copyright 2023-2025 the original author or authors.
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

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link RedisCacheWriter}.
 *
 * @author John Blum
 */
class RedisCacheWriterUnitTests {

	@Test // GH-2351
	void defaultGetWithNameKeyAndTtlCallsGetWithNameAndKeyDiscardingTtl() {

		byte[] key = "TestKey".getBytes();
		byte[] value = "TestValue".getBytes();

		Duration thirtyMinutes = Duration.ofMinutes(30);

		RedisCacheWriter cacheWriter = mock(RedisCacheWriter.class);

		doCallRealMethod().when(cacheWriter).get(anyString(), any(), any());
		doReturn(value).when(cacheWriter).get(anyString(), any());

		assertThat(cacheWriter.get("TestCacheName", key, thirtyMinutes)).isEqualTo(value);

		verify(cacheWriter, times(1)).get(eq("TestCacheName"), eq(key), eq(thirtyMinutes));
		verify(cacheWriter, times(1)).get(eq("TestCacheName"), eq(key));
		verifyNoMoreInteractions(cacheWriter);
	}

	@Test // GH-2650
	void defaultRetrieveWithNameAndKeyCallsRetrieveWithNameKeyAndTtl() throws Exception {

		byte[] key = "TestKey".getBytes();
		byte[] value = "TestValue".getBytes();

		RedisCacheWriter cacheWriter = mock(RedisCacheWriter.class);

		doCallRealMethod().when(cacheWriter).retrieve(anyString(), any());
		doReturn(CompletableFuture.completedFuture(value)).when(cacheWriter).retrieve(anyString(), any(), any());

		assertThat(cacheWriter.retrieve("TestCacheName", key).thenApply(String::new).get()).isEqualTo("TestValue");

		verify(cacheWriter, times(1)).retrieve(eq("TestCacheName"), eq(key));
		verify(cacheWriter, times(1)).retrieve(eq("TestCacheName"), eq(key), isNull());
		verifyNoMoreInteractions(cacheWriter);
	}
}
