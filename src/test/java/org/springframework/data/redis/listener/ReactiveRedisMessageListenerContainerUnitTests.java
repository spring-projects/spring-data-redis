/*
 * Copyright 2018-present the original author or authors.
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
package org.springframework.data.redis.listener;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.springframework.data.redis.util.ByteUtils.*;

import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

import java.nio.ByteBuffer;
import java.util.concurrent.CancellationException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import org.springframework.data.redis.connection.ReactivePubSubCommands;
import org.springframework.data.redis.connection.ReactiveRedisConnection;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.connection.ReactiveSubscription;
import org.springframework.data.redis.connection.ReactiveSubscription.ChannelMessage;
import org.springframework.data.redis.connection.ReactiveSubscription.Message;
import org.springframework.data.redis.connection.ReactiveSubscription.PatternMessage;

/**
 * Unit tests for {@link ReactiveRedisMessageListenerContainer}.
 *
 * @author Mark Paluch
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class ReactiveRedisMessageListenerContainerUnitTests {

	private ReactiveRedisMessageListenerContainer container;

	@Mock ReactiveRedisConnectionFactory connectionFactoryMock;
	@Mock ReactiveRedisConnection connectionMock;
	@Mock ReactivePubSubCommands commandsMock;
	@Mock ReactiveSubscription subscriptionMock;

	@BeforeEach
	void before() {

		when(connectionFactoryMock.getReactiveConnection()).thenReturn(connectionMock);
		when(connectionMock.pubSubCommands()).thenReturn(commandsMock);
		when(connectionMock.closeLater()).thenReturn(Mono.empty());
		when(commandsMock.createSubscription(any())).thenReturn(Mono.just(subscriptionMock));
		when(subscriptionMock.subscribe(any())).thenReturn(Mono.empty());
		when(subscriptionMock.pSubscribe(any())).thenReturn(Mono.empty());
		when(subscriptionMock.unsubscribe()).thenReturn(Mono.empty());
	}

	@Test // DATAREDIS-612
	void shouldSubscribeToPattern() {

		when(subscriptionMock.receive()).thenReturn(Flux.never());

		container = createContainer();

		container.receive(Topic.pattern("foo*")).as(StepVerifier::create).thenAwait().thenCancel().verify();

		verify(subscriptionMock).pSubscribe(getByteBuffer("foo*"));
	}

	@Test // DATAREDIS-612
	void shouldSubscribeToMultiplePatterns() {

		when(subscriptionMock.receive()).thenReturn(Flux.never());
		container = createContainer();

		container.receive(Topic.pattern("foo*"), Topic.pattern("bar*")).as(StepVerifier::create).thenRequest(1).thenAwait()
				.thenCancel().verify();

		verify(subscriptionMock).pSubscribe(getByteBuffer("foo*"), getByteBuffer("bar*"));
	}

	@Test // DATAREDIS-612
	void shouldSubscribeToChannel() {

		when(subscriptionMock.receive()).thenReturn(Flux.never());
		container = createContainer();

		container.receive(Topic.channel("foo")).as(StepVerifier::create).thenAwait().thenCancel().verify();

		verify(subscriptionMock).subscribe(getByteBuffer("foo"));
	}

	@Test // DATAREDIS-612
	void shouldSubscribeToMultipleChannels() {

		when(subscriptionMock.receive()).thenReturn(Flux.never());
		container = createContainer();

		container.receive(Topic.channel("foo"), Topic.channel("bar")).as(StepVerifier::create).thenAwait().thenCancel()
				.verify();

		verify(subscriptionMock).subscribe(getByteBuffer("foo"), getByteBuffer("bar"));
	}

	@Test // DATAREDIS-612
	void shouldEmitChannelMessage() {

		Sinks.Many<Message<ByteBuffer, ByteBuffer>> sink = Sinks.many().unicast().onBackpressureBuffer();

		when(subscriptionMock.receive()).thenReturn(sink.asFlux());
		container = createContainer();

		Flux<Message<String, String>> messageStream = container.receive(Topic.channel("foo"));

		messageStream.as(StepVerifier::create).then(() -> {
			sink.tryEmitNext(createChannelMessage("foo", "message"));
		}).assertNext(msg -> {

			assertThat(msg.getChannel()).isEqualTo("foo");
			assertThat(msg.getMessage()).isEqualTo("message");
		}).thenCancel().verify();
	}

	@Test // DATAREDIS-612
	void shouldEmitPatternMessage() {

		Sinks.Many<Message<ByteBuffer, ByteBuffer>> sink = Sinks.many().unicast().onBackpressureBuffer();

		when(subscriptionMock.receive()).thenReturn(sink.asFlux());
		container = createContainer();

		Flux<PatternMessage<String, String, String>> messageStream = container.receive(Topic.pattern("foo*"));

		messageStream.as(StepVerifier::create).then(() -> {
			sink.tryEmitNext(createPatternMessage("foo*", "foo", "message"));
		}).assertNext(msg -> {

			assertThat(msg.getPattern()).isEqualTo("foo*");
			assertThat(msg.getChannel()).isEqualTo("foo");
			assertThat(msg.getMessage()).isEqualTo("message");
		}).thenCancel().verify();
	}

	@Test // DATAREDIS-612
	void shouldRegisterSubscription() {

		Sinks.Many<Message<ByteBuffer, ByteBuffer>> sink = Sinks.many().multicast().onBackpressureBuffer();

		Sinks.One<Void> subscribeMono = Sinks.one();

		reset(subscriptionMock);
		when(subscriptionMock.subscribe(any())).thenReturn(subscribeMono.asMono());
		when(subscriptionMock.unsubscribe()).thenReturn(Mono.empty());
		when(subscriptionMock.receive()).thenReturn(sink.asFlux());
		container = createContainer();

		Flux<Message<String, String>> messageStream = container.receive(Topic.channel("foo*"));

		Disposable subscription = messageStream.subscribe();

		assertThat(container.getActiveSubscriptions()).isEmpty();
		subscribeMono.tryEmitEmpty();
		assertThat(container.getActiveSubscriptions()).isNotEmpty();
		subscription.dispose();
		assertThat(container.getActiveSubscriptions()).isEmpty();
	}

	@Test // DATAREDIS-612, GH-1622
	void shouldRegisterSubscriptionMultipleSubscribers() {

		Sinks.Many<Message<ByteBuffer, ByteBuffer>> sink = Sinks.many().multicast().onBackpressureBuffer();

		reset(subscriptionMock);
		when(subscriptionMock.subscribe(any())).thenReturn(Mono.empty());
		when(subscriptionMock.unsubscribe()).thenReturn(Mono.empty());
		when(subscriptionMock.receive()).thenReturn(sink.asFlux());
		container = createContainer();

		Flux<Message<String, String>> messageStream = container.receive(Topic.channel("foo*"));

		Disposable first = messageStream.subscribe();
		Disposable second = messageStream.subscribe();

		first.dispose();

		verify(subscriptionMock, never()).unsubscribe();
		assertThat(container.getActiveSubscriptions()).isNotEmpty();

		second.dispose();

		verify(subscriptionMock).cancel();
		assertThat(container.getActiveSubscriptions()).isEmpty();
	}

	@Test // DATAREDIS-612, GH-1622
	void shouldUnsubscribeOnCancel() {

		Sinks.Many<Message<ByteBuffer, ByteBuffer>> sink = Sinks.many().unicast().onBackpressureBuffer();
		when(subscriptionMock.receive()).thenReturn(sink.asFlux());
		container = createContainer();

		Flux<PatternMessage<String, String, String>> messageStream = container.receive(Topic.pattern("foo*"));

		messageStream.as(StepVerifier::create).then(() -> {

			// Then required to trigger cancel.

		}).thenCancel().verify();

		verify(subscriptionMock).cancel();
	}

	@Test // DATAREDIS-612
	void shouldTerminateSubscriptionsOnShutdown() {

		Sinks.Many<Message<ByteBuffer, ByteBuffer>> sink = Sinks.many().unicast().onBackpressureBuffer();

		when(subscriptionMock.receive()).thenReturn(sink.asFlux());
		when(subscriptionMock.cancel()).thenReturn(Mono.defer(() -> {

			sink.tryEmitError(new CancellationException());
			return Mono.empty();
		}));
		container = createContainer();

		Flux<PatternMessage<String, String, String>> messageStream = container.receive(Topic.pattern("foo*"));

		messageStream.as(StepVerifier::create).then(() -> {
			container.destroy();
		}).verifyError(CancellationException.class);
	}

	@Test // DATAREDIS-612
	void shouldCleanupDownstream() {

		Sinks.Many<Message<ByteBuffer, ByteBuffer>> sink = Sinks.many().unicast().onBackpressureBuffer();

		when(subscriptionMock.receive()).thenReturn(sink.asFlux());
		container = createContainer();

		Flux<PatternMessage<String, String, String>> messageStream = container.receive(Topic.pattern("foo*"));

		messageStream.as(StepVerifier::create).then(() -> {
			assertThat(sink.currentSubscriberCount()).isGreaterThan(0);
			sink.tryEmitNext(createPatternMessage("foo*", "foo", "message"));
		}).expectNextCount(1).thenCancel().verify();

		assertThat(sink.currentSubscriberCount()).isEqualTo(0);
	}

	private ReactiveRedisMessageListenerContainer createContainer() {
		return new ReactiveRedisMessageListenerContainer(connectionFactoryMock);
	}

	private static ChannelMessage<ByteBuffer, ByteBuffer> createChannelMessage(String channel, String body) {
		return new ChannelMessage<>(getByteBuffer(channel), getByteBuffer(body));
	}

	private static PatternMessage<ByteBuffer, ByteBuffer, ByteBuffer> createPatternMessage(String pattern, String channel,
			String body) {

		return new PatternMessage<>(getByteBuffer(pattern), getByteBuffer(channel), getByteBuffer(body));
	}
}
