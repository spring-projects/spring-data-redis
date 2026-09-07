/*
 * Copyright 2025-present the original author or authors.
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
package org.springframework.data.redis.core;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisHashCommands;
import org.springframework.data.redis.core.types.Expirations;

/**
 * Unit tests for {@link DefaultHashOperations}.
 *
 * @author Seonghun Lee
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class DefaultHashOperationsUnitTests {

	@Mock RedisConnectionFactory connectionFactoryMock;
	@Mock RedisConnection connectionMock;
	@Mock RedisHashCommands hashCommandsMock;

	StringRedisTemplate template;

	@BeforeEach
	void setUp() {

		when(connectionFactoryMock.getConnection()).thenReturn(connectionMock);
		when(connectionMock.hashCommands()).thenReturn(hashCommandsMock);

		template = new StringRedisTemplate(connectionFactoryMock);
		template.afterPropertiesSet();
	}

	@Test // GH-3427
	void getTimeToLiveShouldConsiderTimeUnit() {

		// HTTL returns 120 (seconds); drivers convert to 2 when asked for MINUTES
		when(hashCommandsMock.hTtl(any(byte[].class), any(byte[][].class))).thenReturn(List.of(120L));
		when(hashCommandsMock.hTtl(any(byte[].class), eq(TimeUnit.MINUTES), any(byte[][].class))).thenReturn(List.of(2L));

		Expirations<String> expirations = template.<String, String> opsForHash().getTimeToLive("key", TimeUnit.MINUTES,
				List.of("field"));

		assertThat(expirations.ttlOf("field")).isEqualTo(Duration.ofMinutes(2));
		assertThat(expirations.expirationOf("field").value()).isEqualTo(2L);
	}
}
