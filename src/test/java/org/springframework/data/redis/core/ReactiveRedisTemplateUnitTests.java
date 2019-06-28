/*
 * Copyright 2019 the original author or authors.
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

import static org.mockito.Mockito.*;

import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import org.junit.Test;

import org.springframework.data.redis.connection.ReactiveRedisConnection;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.serializer.RedisSerializationContext;

/**
 * Unit tests for {@link ReactiveRedisTemplate}.
 *
 * @author Mark Paluch
 */
public class ReactiveRedisTemplateUnitTests {

	ReactiveRedisConnectionFactory connectionFactoryMock = mock(ReactiveRedisConnectionFactory.class);
	ReactiveRedisConnection connectionMock = mock(ReactiveRedisConnection.class);

	@Test // DATAREDIS-999
	public void closeShouldUseAsyncRelease() {

		when(connectionFactoryMock.getReactiveConnection()).thenReturn(connectionMock);
		when(connectionMock.closeLater()).thenReturn(Mono.empty());

		ReactiveRedisTemplate<String, String> template = new ReactiveRedisTemplate<>(connectionFactoryMock,
				RedisSerializationContext.string());

		template.execute(connection -> Mono.empty()) //
				.as(StepVerifier::create) //
				.verifyComplete();

		verify(connectionMock).closeLater();
		verifyNoMoreInteractions(connectionMock);
	}
}
