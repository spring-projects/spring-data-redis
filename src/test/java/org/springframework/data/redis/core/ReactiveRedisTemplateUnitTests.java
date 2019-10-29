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

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;
import org.springframework.data.redis.connection.ReactivePubSubCommands;
import org.springframework.data.redis.connection.ReactiveRedisConnection;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.connection.ReactiveSubscription;
import org.springframework.data.redis.serializer.RedisSerializationContext;

/**
 * Unit tests for {@link ReactiveRedisTemplate}.
 *
 * @author Mark Paluch
 */
public class ReactiveRedisTemplateUnitTests {

	ReactiveRedisConnectionFactory connectionFactoryMock = mock(ReactiveRedisConnectionFactory.class);
	ReactiveRedisConnection connectionMock = mock(ReactiveRedisConnection.class);

	@Test // DATAREDIS-1053
	public void listenToShouldSubscribeToChannel() {

		AtomicBoolean closed = new AtomicBoolean();
		when(connectionFactoryMock.getReactiveConnection()).thenReturn(connectionMock);
		when(connectionMock.closeLater()).thenReturn(Mono.<Void> empty().doOnSubscribe(ignore -> closed.set(true)));

		ReactivePubSubCommands pubSubCommands = mock(ReactivePubSubCommands.class);
		ReactiveSubscription subscription = mock(ReactiveSubscription.class);

		when(connectionMock.pubSubCommands()).thenReturn(pubSubCommands);
		when(pubSubCommands.subscribe(any())).thenReturn(Mono.empty());
		when(pubSubCommands.createSubscription()).thenReturn(Mono.just(subscription));
		when(subscription.receive()).thenReturn(Flux.create(sink -> {}));

		ReactiveRedisTemplate<String, String> template = new ReactiveRedisTemplate<>(connectionFactoryMock,
				RedisSerializationContext.string());

		template.listenToChannel("channel") //
				.as(StepVerifier::create) //
				.thenAwait() //
				.thenCancel() //
				.verify();

		assertThat(closed).isTrue();
	}
}
