/*
 * Copyright 2022-present the original author or authors.
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
package org.springframework.data.redis.connection.lettuce;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.lettuce.core.RedisCommandTimeoutException;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.pubsub.api.reactive.RedisPubSubReactiveCommands;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.nio.ByteBuffer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import org.springframework.dao.QueryTimeoutException;
import org.springframework.data.redis.connection.util.ByteArrayWrapper;

/**
 * Unit tests for {@link LettuceReactivePubSubCommands}.
 *
 * @author Mark Paluch
 * @author Daeryeon Choi
 */
@MockitoSettings(strictness = Strictness.LENIENT)
class LettuceReactivePubSubCommandsUnitTests {

	LettuceReactivePubSubCommands sut;

	@Mock LettuceReactiveRedisConnection connection;

	@Mock StatefulRedisPubSubConnection<ByteBuffer, ByteBuffer> lettuceConnection;
	@Mock RedisPubSubReactiveCommands<ByteBuffer, ByteBuffer> reactiveCommands;

	@SuppressWarnings("unchecked")
	@BeforeEach
	void setUp() {

		when(connection.getPubSubConnection()).thenReturn(Mono.just(lettuceConnection));
		when(connection.translateException()).thenReturn(throwable -> {
			if (throwable instanceof RedisCommandTimeoutException) {
				return new QueryTimeoutException("timeout", throwable);
			}
			return throwable;
		});
		when(lettuceConnection.reactive()).thenReturn(reactiveCommands);
		sut = new LettuceReactivePubSubCommands(connection);
	}

	@Test // GH-2386
	void shouldSubscribeChannelMultipleTimes() {

		when(reactiveCommands.subscribe(any())).thenReturn(Mono.empty());

		sut.subscribe(wrap("channel")) //
				.as(StepVerifier::create) //
				.verifyComplete();

		sut.subscribe(wrap("channel")) //
				.as(StepVerifier::create) //
				.verifyComplete();

		verify(reactiveCommands, times(2)).subscribe(wrap("channel"));
		assertThat(sut.getChannels()).hasSize(1);
		assertThat(sut.getPatterns()).isEmpty();
	}

	@Test // GH-2386
	void shouldNotUnsubscribeChannelIfUsedMultipleTimes() {

		when(reactiveCommands.subscribe(any())).thenReturn(Mono.empty());
		when(reactiveCommands.unsubscribe(any())).thenReturn(Mono.empty());

		sut.subscribe(wrap("channel")) //
				.as(StepVerifier::create) //
				.verifyComplete();

		sut.subscribe(wrap("channel")) //
				.as(StepVerifier::create) //
				.verifyComplete();

		sut.unsubscribe(wrap("channel")) //
				.as(StepVerifier::create) //
				.verifyComplete();

		verify(reactiveCommands, times(2)).subscribe(wrap("channel"));
		verifyNoMoreInteractions(reactiveCommands);
		assertThat(sut.getChannels()).hasSize(1);
		assertThat(sut.getPatterns()).isEmpty();
	}

	@Test // GH-2386
	void shouldUnsubscribeChannelIfNotUsedAnymore() {

		when(reactiveCommands.subscribe(any())).thenReturn(Mono.empty());
		when(reactiveCommands.unsubscribe(any())).thenReturn(Mono.empty());

		sut.subscribe(wrap("channel")) //
				.as(StepVerifier::create) //
				.verifyComplete();

		sut.subscribe(wrap("channel")) //
				.as(StepVerifier::create) //
				.verifyComplete();

		sut.unsubscribe(wrap("channel")) //
				.as(StepVerifier::create) //
				.verifyComplete();

		sut.unsubscribe(wrap("channel")) //
				.as(StepVerifier::create) //
				.verifyComplete();

		verify(reactiveCommands, times(2)).subscribe(wrap("channel"));
		verify(reactiveCommands, times(1)).unsubscribe(wrap("channel"));
		assertThat(sut.getChannels()).isEmpty();
		assertThat(sut.getPatterns()).isEmpty();
	}

	@Test // GH-2386
	void shouldSubscribePatternMultipleTimes() {

		when(reactiveCommands.psubscribe(any())).thenReturn(Mono.empty());

		sut.pSubscribe(wrap("pattern")) //
				.as(StepVerifier::create) //
				.verifyComplete();

		sut.pSubscribe(wrap("pattern")) //
				.as(StepVerifier::create) //
				.verifyComplete();

		verify(reactiveCommands, times(2)).psubscribe(wrap("pattern"));
		assertThat(sut.getChannels()).isEmpty();
		assertThat(sut.getPatterns()).hasSize(1);
	}

	@Test // GH-2386
	void shouldNotUnsubscribePatternIfUsedMultipleTimes() {

		when(reactiveCommands.psubscribe(any())).thenReturn(Mono.empty());
		when(reactiveCommands.punsubscribe(any())).thenReturn(Mono.empty());

		sut.pSubscribe(wrap("pattern")) //
				.as(StepVerifier::create) //
				.verifyComplete();

		sut.pSubscribe(wrap("pattern")) //
				.as(StepVerifier::create) //
				.verifyComplete();

		sut.pUnsubscribe(wrap("pattern")) //
				.as(StepVerifier::create) //
				.verifyComplete();

		verify(reactiveCommands, times(2)).psubscribe(wrap("pattern"));
		verifyNoMoreInteractions(reactiveCommands);
		assertThat(sut.getChannels()).isEmpty();
		assertThat(sut.getPatterns()).hasSize(1);
	}

	@Test // GH-2386
	void shouldUnsubscribePatternIfNotUsedAnymore() {

		when(reactiveCommands.psubscribe(any())).thenReturn(Mono.empty());
		when(reactiveCommands.punsubscribe(any())).thenReturn(Mono.empty());

		sut.pSubscribe(wrap("pattern")) //
				.as(StepVerifier::create) //
				.verifyComplete();

		sut.pSubscribe(wrap("pattern")) //
				.as(StepVerifier::create) //
				.verifyComplete();

		sut.pUnsubscribe(wrap("pattern")) //
				.as(StepVerifier::create) //
				.verifyComplete();

		sut.pUnsubscribe(wrap("pattern")) //
				.as(StepVerifier::create) //
				.verifyComplete();

		verify(reactiveCommands, times(2)).psubscribe(wrap("pattern"));
		verify(reactiveCommands, times(1)).punsubscribe(wrap("pattern"));
		assertThat(sut.getChannels()).isEmpty();
		assertThat(sut.getPatterns()).isEmpty();
	}

	@Test // GH-3508
	void unsubscribeShouldValidateChannelsNotNull() {
		assertThatIllegalArgumentException().isThrownBy(() -> sut.unsubscribe((ByteBuffer[]) null))
				.withMessage("Channels must not be null");
		assertThatIllegalArgumentException().isThrownBy(() -> sut.unsubscribe(wrap("ch1"), null))
				.withMessage("Channels must not contain null elements");
		assertThatIllegalArgumentException().isThrownBy(() -> sut.pUnsubscribe(wrap("pat*"), null))
				.withMessage("Patterns must not contain null elements");
		assertThat(sut.getChannels()).isEmpty();
		assertThat(sut.getPatterns()).isEmpty();
	}

	@Test // GH-3508
	void unsubscribeFailureShouldRestoreTargetCountsForRetryAndOtherSubscribers() {

		when(reactiveCommands.subscribe(any(ByteBuffer[].class))).thenReturn(Mono.empty());
		when(reactiveCommands.unsubscribe(any(ByteBuffer[].class)))
				.thenReturn(Mono.error(new RedisCommandTimeoutException("timeout"))).thenReturn(Mono.empty());

		sut.subscribe(wrap("ch1")).as(StepVerifier::create).verifyComplete();
		sut.subscribe(wrap("ch1"), wrap("ch2")).as(StepVerifier::create).verifyComplete();

		// Unsubscribing ch1 (count 2 -> 1) and ch2 (count 1 -> 0) fails when sending UNSUBSCRIBE ch2
		sut.unsubscribe(wrap("ch1"), wrap("ch2")).as(StepVerifier::create).expectError(QueryTimeoutException.class).verify();

		// Both ch1 (restored to 2) and ch2 (restored to 1) must remain tracked
		assertThat(sut.getChannels()).containsKeys(new ByteArrayWrapper("ch1".getBytes()),
				new ByteArrayWrapper("ch2".getBytes()));

		// Retrying unsubscribes ch2 (1 -> 0) and leaves ch1 at count 1 (does NOT unsubscribe ch1!)
		sut.unsubscribe(wrap("ch1"), wrap("ch2")).as(StepVerifier::create).verifyComplete();
		verify(reactiveCommands, times(2)).unsubscribe(wrap("ch2"));
		verify(reactiveCommands, never()).unsubscribe(wrap("ch1"));
		assertThat(sut.getChannels()).containsOnlyKeys(new ByteArrayWrapper("ch1".getBytes()));

		// First subscriber's subsequent unsubscribe now decrements ch1 (1 -> 0) and sends UNSUBSCRIBE ch1
		sut.unsubscribe(wrap("ch1")).as(StepVerifier::create).verifyComplete();
		verify(reactiveCommands).unsubscribe(wrap("ch1"));
		assertThat(sut.getChannels()).isEmpty();
	}

	private static ByteBuffer wrap(String content) {
		return ByteBuffer.wrap(content.getBytes());
	}
}
