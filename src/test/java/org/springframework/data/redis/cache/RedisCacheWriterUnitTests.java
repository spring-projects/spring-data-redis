/*
 * Copyright 2023 the original author or authors.
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
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.time.Duration;

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
}
