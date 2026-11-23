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
import static org.awaitility.Awaitility.*;
import static org.mockito.Mockito.*;

import io.lettuce.core.RedisCommandTimeoutException;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.pubsub.RedisPubSubListener;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.pubsub.api.reactive.RedisPubSubReactiveCommands;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import org.springframework.dao.QueryTimeoutException;
import org.springframework.data.redis.connection.ReactiveSubscription;
import org.springframework.data.redis.connection.SubscriptionListener;
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
		assertThatIllegalArgumentException().isThrownBy(() -> sut.subscribe(wrap("ch1"), null))
				.withMessage("Channels must not contain null elements");
		assertThatIllegalArgumentException().isThrownBy(() -> sut.pSubscribe(wrap("pat*"), null))
				.withMessage("Patterns must not contain null elements");
		assertThatIllegalArgumentException().isThrownBy(() -> sut.unsubscribe(wrap("ch1"), null))
				.withMessage("Channels must not contain null elements");
		assertThatIllegalArgumentException().isThrownBy(() -> sut.pUnsubscribe(wrap("pat*"), null))
				.withMessage("Patterns must not contain null elements");
		assertThat(sut.getChannels()).isEmpty();
		assertThat(sut.getPatterns()).isEmpty();
	}

	@Test // GH-3508
	void subscribeFailureAndCancellationShouldRollbackTargetReferenceCount() {

		when(reactiveCommands.subscribe(any())).thenReturn(Mono.empty())
				.thenReturn(Mono.error(new RedisCommandTimeoutException("timeout"))).thenReturn(Mono.never());
		when(reactiveCommands.unsubscribe(any())).thenReturn(Mono.empty());

		sut.subscribe(wrap("channel")).as(StepVerifier::create).verifyComplete();

		sut.subscribe(wrap("channel")).as(StepVerifier::create).expectError(QueryTimeoutException.class).verify();

		sut.subscribe(wrap("channel")).as(StepVerifier::create).thenCancel().verify();

		// Only the first subscriber must remain tracked so its unsubscribe sends UNSUBSCRIBE
		assertThat(sut.getChannels()).hasSize(1);
		sut.unsubscribe(wrap("channel")).as(StepVerifier::create).verifyComplete();

		verify(reactiveCommands).unsubscribe(wrap("channel"));
		assertThat(sut.getChannels()).isEmpty();
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

	@Test // GH-3508
	void pSubscribeAndPUnsubscribeFailuresShouldRollbackTargetReferenceCounts() {

		when(reactiveCommands.psubscribe(any())).thenReturn(Mono.empty())
				.thenReturn(Mono.error(new RedisCommandTimeoutException("timeout")));
		when(reactiveCommands.punsubscribe(any())).thenReturn(Mono.error(new RedisCommandTimeoutException("timeout")))
				.thenReturn(Mono.empty());

		sut.pSubscribe(wrap("pat*")).as(StepVerifier::create).verifyComplete();
		sut.pSubscribe(wrap("pat*")).as(StepVerifier::create).expectError(QueryTimeoutException.class).verify();

		sut.pUnsubscribe(wrap("pat*")).as(StepVerifier::create).expectError(QueryTimeoutException.class).verify();
		assertThat(sut.getPatterns()).hasSize(1);

		sut.pUnsubscribe(wrap("pat*")).as(StepVerifier::create).verifyComplete();
		verify(reactiveCommands, times(2)).punsubscribe(wrap("pat*"));
		assertThat(sut.getPatterns()).isEmpty();
	}

	@Test // GH-3508
	@SuppressWarnings("unchecked")
	void lateSubscribedAfterTimeoutRollbackShouldUnsubscribeOrphanedChannelsOnly() {

		when(reactiveCommands.subscribe(wrap("ch-active"))).thenReturn(Mono.empty());
		when(reactiveCommands.subscribe(wrap("ch-inflight"))).thenReturn(Mono.never());
		when(reactiveCommands.subscribe(wrap("ch-orphan")))
				.thenReturn(Mono.error(new RedisCommandTimeoutException("timeout")));
		when(reactiveCommands.subscribe(wrap("ch-debounce")))
				.thenReturn(Mono.error(new RedisCommandTimeoutException("timeout")));
		when(reactiveCommands.unsubscribe(wrap("ch-orphan"))).thenReturn(Mono.empty());

		Sinks.One<Void> pendingUnsub = Sinks.one();
		when(reactiveCommands.unsubscribe(wrap("ch-debounce"))).thenReturn(pendingUnsub.asMono())
				.thenReturn(Mono.empty());

		sut.subscribe(wrap("ch-active")).as(StepVerifier::create).verifyComplete();
		sut.subscribe(wrap("ch-inflight")).subscribe();
		sut.subscribe(wrap("ch-orphan")).as(StepVerifier::create).expectError(QueryTimeoutException.class).verify();
		sut.subscribe(wrap("ch-debounce")).as(StepVerifier::create).expectError(QueryTimeoutException.class).verify();

		ArgumentCaptor<RedisPubSubListener<ByteBuffer, ByteBuffer>> listenerCaptor = ArgumentCaptor
				.forClass(RedisPubSubListener.class);
		verify(lettuceConnection, times(1)).addListener(listenerCaptor.capture());
		RedisPubSubListener<ByteBuffer, ByteBuffer> listener = listenerCaptor.getValue();

		// Active and in-flight channels must never be unsubscribed on subscribed notifications
		listener.subscribed(wrap("ch-active"), 1);
		listener.subscribed(wrap("ch-inflight"), 2);
		verify(reactiveCommands, never()).unsubscribe(wrap("ch-active"));
		verify(reactiveCommands, never()).unsubscribe(wrap("ch-inflight"));

		// Orphaned channel must be unsubscribed immediately and leave no active cleanup state
		listener.subscribed(wrap("ch-orphan"), 3);
		verify(reactiveCommands, times(1)).unsubscribe(wrap("ch-orphan"));
		assertThat(sut.getActiveChannelCleanups()).isEmpty();

		// Duplicate notifications while unsubscribe is in-flight are debounced and followed up once settled
		listener.subscribed(wrap("ch-debounce"), 4);
		listener.subscribed(wrap("ch-debounce"), 4);
		verify(reactiveCommands, times(1)).unsubscribe(wrap("ch-debounce"));
		assertThat(sut.getActiveChannelCleanups()).hasSize(1);

		pendingUnsub.tryEmitEmpty();
		verify(reactiveCommands, times(2)).unsubscribe(wrap("ch-debounce"));
		assertThat(sut.getActiveChannelCleanups()).isEmpty();
	}

	@Test // GH-3508
	@SuppressWarnings("unchecked")
	void latePSubscribedAfterTimeoutRollbackShouldPUnsubscribeOrphanedPatternsOnly() {

		when(reactiveCommands.psubscribe(wrap("pat-active*"))).thenReturn(Mono.empty());
		when(reactiveCommands.psubscribe(wrap("pat-inflight*"))).thenReturn(Mono.never());
		when(reactiveCommands.psubscribe(wrap("pat-orphan*")))
				.thenReturn(Mono.error(new RedisCommandTimeoutException("timeout")));
		when(reactiveCommands.punsubscribe(wrap("pat-orphan*"))).thenReturn(Mono.empty());

		sut.pSubscribe(wrap("pat-active*")).as(StepVerifier::create).verifyComplete();
		sut.pSubscribe(wrap("pat-inflight*")).subscribe();
		sut.pSubscribe(wrap("pat-orphan*")).as(StepVerifier::create).expectError(QueryTimeoutException.class).verify();

		ArgumentCaptor<RedisPubSubListener<ByteBuffer, ByteBuffer>> listenerCaptor = ArgumentCaptor
				.forClass(RedisPubSubListener.class);
		verify(lettuceConnection, times(1)).addListener(listenerCaptor.capture());
		RedisPubSubListener<ByteBuffer, ByteBuffer> listener = listenerCaptor.getValue();

		listener.psubscribed(wrap("pat-active*"), 1);
		listener.psubscribed(wrap("pat-inflight*"), 2);
		verify(reactiveCommands, never()).punsubscribe(wrap("pat-active*"));
		verify(reactiveCommands, never()).punsubscribe(wrap("pat-inflight*"));

		listener.psubscribed(wrap("pat-orphan*"), 3);
		verify(reactiveCommands, times(1)).punsubscribe(wrap("pat-orphan*"));
		assertThat(sut.getActivePatternCleanups()).isEmpty();
	}

	@Test // GH-3508
	@SuppressWarnings("unchecked")
	void closeShouldRemoveOrphanCleanupListenerAndIgnoreSubsequentNotifications() {

		when(reactiveCommands.subscribe(wrap("ch-active"))).thenReturn(Mono.empty());
		sut.subscribe(wrap("ch-active")).as(StepVerifier::create).verifyComplete();

		ArgumentCaptor<RedisPubSubListener<ByteBuffer, ByteBuffer>> pubSubListenerCaptor = ArgumentCaptor
				.forClass(RedisPubSubListener.class);
		verify(lettuceConnection).addListener(pubSubListenerCaptor.capture());
		RedisPubSubListener<ByteBuffer, ByteBuffer> listener = pubSubListenerCaptor.getValue();

		sut.close();
		verify(lettuceConnection).removeListener(listener);

		// Late callback after close() must be ignored
		listener.subscribed(wrap("ch-after-close"), 1);
		verify(reactiveCommands, never()).unsubscribe(wrap("ch-after-close"));
	}

	@Test // GH-3508
	@SuppressWarnings("unchecked")
	void concurrentCloseLaterDuringListenerRegistrationShouldRemoveListenersBeforePoolRelease() throws Exception {

		LettuceConnectionProvider provider = mock(LettuceConnectionProvider.class);
		StatefulConnection<ByteBuffer, ByteBuffer> dedicated = mock(StatefulConnection.class);
		when(provider.getConnectionAsync(StatefulConnection.class))
				.thenReturn(CompletableFuture.completedFuture(dedicated));
		when(provider.getConnectionAsync(StatefulRedisPubSubConnection.class))
				.thenReturn(CompletableFuture.completedFuture(lettuceConnection));
		when(provider.releaseAsync(any())).thenReturn(CompletableFuture.completedFuture(null));

		LettuceReactiveRedisConnection realConnection = new LettuceReactiveRedisConnection(provider);
		LettuceReactivePubSubCommands realPubSub = (LettuceReactivePubSubCommands) realConnection.pubSubCommands();

		CountDownLatch enteredAddListener = new CountDownLatch(1);
		CountDownLatch allowRegistrationToProceed = new CountDownLatch(1);
		AtomicReference<RedisPubSubListener<ByteBuffer, ByteBuffer>> registeredPubSubListener = new AtomicReference<>();

		when(reactiveCommands.subscribe(any())).thenReturn(Mono.empty());
		doAnswer(inv -> {
			registeredPubSubListener.set(inv.getArgument(0));
			enteredAddListener.countDown();
			assertThat(allowRegistrationToProceed.await(5, TimeUnit.SECONDS)).isTrue();
			return null;
		}).when(lettuceConnection).addListener(any(RedisPubSubListener.class));

		CompletableFuture<Void> subscribeFuture = CompletableFuture
				.runAsync(() -> realPubSub.subscribe(wrap("ch-race")).block());
		assertThat(enteredAddListener.await(5, TimeUnit.SECONDS)).isTrue();

		CompletableFuture<Void> closeLaterFuture = CompletableFuture.runAsync(() -> realConnection.closeLater().block());

		// closeLater() must wait for in-progress listener installation rather than releasing a dirty connection
		verify(provider, after(50).never()).releaseAsync(lettuceConnection);
		assertThat(closeLaterFuture.isDone()).isFalse();

		allowRegistrationToProceed.countDown();
		subscribeFuture.exceptionally(ex -> {
			assertThat(ex).hasRootCauseInstanceOf(IllegalStateException.class);
			return null;
		}).get(5, TimeUnit.SECONDS);
		closeLaterFuture.get(5, TimeUnit.SECONDS);

		InOrder inOrder = inOrder(lettuceConnection, provider);
		inOrder.verify(lettuceConnection).addListener(registeredPubSubListener.get());
		inOrder.verify(lettuceConnection).removeListener(registeredPubSubListener.get());
		inOrder.verify(provider).releaseAsync(lettuceConnection);

		// Subsequent operations after closeLater() must not re-install listeners
		assertThatIllegalStateException().isThrownBy(() -> realPubSub.subscribe(wrap("ch-after-close")).block());
		verify(lettuceConnection, times(1)).addListener(any(RedisPubSubListener.class));
		assertThat(realPubSub.getActiveChannelCleanups()).isEmpty();
	}

	@Test // GH-3508
	@SuppressWarnings("unchecked")
	void repeatedDistinctFailedTopicsShouldRetainZeroStateAcrossAckedAndUnackedFailures() {

		when(reactiveCommands.unsubscribe(any(ByteBuffer.class))).thenReturn(Mono.empty());
		when(reactiveCommands.punsubscribe(any(ByteBuffer.class))).thenReturn(Mono.empty());

		for (int i = 0; i < 30; i++) {
			String ch = "ch-ack-" + i;
			String pat = "pat-ack-" + i + "*";
			String deadCh = "ch-dead-" + i;
			when(reactiveCommands.subscribe(wrap(ch)))
					.thenReturn(Mono.error(new RedisCommandTimeoutException("timeout")));
			when(reactiveCommands.psubscribe(wrap(pat)))
					.thenReturn(Mono.error(new RedisCommandTimeoutException("timeout")));
			when(reactiveCommands.subscribe(wrap(deadCh)))
					.thenReturn(Mono.error(new RedisCommandTimeoutException("timeout")));

			sut.subscribe(wrap(ch)).as(StepVerifier::create).expectError(QueryTimeoutException.class).verify();
			sut.pSubscribe(wrap(pat)).as(StepVerifier::create).expectError(QueryTimeoutException.class).verify();
			sut.subscribe(wrap(deadCh)).as(StepVerifier::create).expectError(QueryTimeoutException.class).verify();
		}

		assertThat(sut.getChannels()).isEmpty();
		assertThat(sut.getPatterns()).isEmpty();
		assertThat(sut.getActiveChannelCleanups()).isEmpty();
		assertThat(sut.getActivePatternCleanups()).isEmpty();

		ArgumentCaptor<RedisPubSubListener<ByteBuffer, ByteBuffer>> pubSubCaptor = ArgumentCaptor
				.forClass(RedisPubSubListener.class);
		verify(lettuceConnection).addListener(pubSubCaptor.capture());
		RedisPubSubListener<ByteBuffer, ByteBuffer> listener = pubSubCaptor.getValue();

		for (int i = 0; i < 30; i++) {
			listener.subscribed(wrap("ch-ack-" + i), 1);
			listener.psubscribed(wrap("pat-ack-" + i + "*"), 1);
		}

		assertThat(sut.getActiveChannelCleanups()).isEmpty();
		assertThat(sut.getActivePatternCleanups()).isEmpty();

		// Failed first unsubscribe followed by reconnect replay still cleans up without leaking state
		when(reactiveCommands.subscribe(wrap("ch-retry")))
				.thenReturn(Mono.error(new RedisCommandTimeoutException("timeout")));
		when(reactiveCommands.unsubscribe(wrap("ch-retry")))
				.thenReturn(Mono.error(new RedisCommandTimeoutException("unsub-timeout"))).thenReturn(Mono.empty());

		sut.subscribe(wrap("ch-retry")).as(StepVerifier::create).expectError(QueryTimeoutException.class).verify();

		listener.subscribed(wrap("ch-retry"), 1);
		assertThat(sut.getActiveChannelCleanups()).isEmpty();

		listener.subscribed(wrap("ch-retry"), 1);
		verify(reactiveCommands, times(2)).unsubscribe(wrap("ch-retry"));
		assertThat(sut.getActiveChannelCleanups()).isEmpty();
	}

	@Test // GH-3508
	@SuppressWarnings("unchecked")
	void orphanCleanupPausedBeforeSendShouldCompleteBeforeConcurrentSubscribe() throws Exception {

		List<String> commandOrder = new CopyOnWriteArrayList<>();
		CountDownLatch cleanupPrepared = new CountDownLatch(1);
		CountDownLatch allowCleanupSend = new CountDownLatch(1);

		when(reactiveCommands.subscribe(wrap("ch-order")))
				.thenReturn(Mono.error(new RedisCommandTimeoutException("timeout")))
				.thenReturn(Mono.fromRunnable(() -> commandOrder.add("SUBSCRIBE")));
		when(reactiveCommands.unsubscribe(wrap("ch-order"))).thenReturn(Mono.<Void> defer(() -> {
			cleanupPrepared.countDown();
			try {
				assertThat(allowCleanupSend.await(5, TimeUnit.SECONDS)).isTrue();
			} catch (InterruptedException ex) {
				Thread.currentThread().interrupt();
				return Mono.error(ex);
			}
			commandOrder.add("UNSUBSCRIBE");
			return Mono.empty();
		}).subscribeOn(Schedulers.boundedElastic()));

		sut.subscribe(wrap("ch-order")).as(StepVerifier::create).expectError(QueryTimeoutException.class).verify();

		ArgumentCaptor<RedisPubSubListener<ByteBuffer, ByteBuffer>> pubSubCaptor = ArgumentCaptor
				.forClass(RedisPubSubListener.class);
		verify(lettuceConnection).addListener(pubSubCaptor.capture());
		RedisPubSubListener<ByteBuffer, ByteBuffer> listener = pubSubCaptor.getValue();

		// Late ACK prepares orphan UNSUBSCRIBE, which pauses before actual transmission
		listener.subscribed(wrap("ch-order"), 1);
		assertThat(cleanupPrepared.await(5, TimeUnit.SECONDS)).isTrue();

		// Concurrent new subscription on the same channel starts while cleanup UNSUBSCRIBE has not been sent yet
		ByteArrayWrapper wrapper = new ByteArrayWrapper("ch-order".getBytes());
		CompletableFuture<Void> newSubscribeFuture = CompletableFuture
				.runAsync(() -> sut.subscribe(wrap("ch-order")).block());
		await().atMost(Duration.ofSeconds(5)).until(() -> sut.getChannels().containsKey(wrapper));
		assertThat(commandOrder).isEmpty();

		allowCleanupSend.countDown();
		newSubscribeFuture.get(5, TimeUnit.SECONDS);

		assertThat(commandOrder).containsExactly("UNSUBSCRIBE", "SUBSCRIBE");
		assertThat(sut.getChannels()).containsKey(wrapper);
		assertThat(sut.getChannels().get(wrapper).getSubscriberCount()).isEqualTo(1L);
		assertThat(sut.getActiveChannelCleanups()).isEmpty();
	}

	@Test // GH-3508
	@SuppressWarnings("unchecked")
	void regularUnsubscribeAndPreRollbackAckShouldSerializeAndCleanupOrphans() throws Exception {

		List<String> order = new CopyOnWriteArrayList<>();
		CountDownLatch unsubscribeEntered = new CountDownLatch(1);
		CountDownLatch allowUnsubscribe = new CountDownLatch(1);

		when(reactiveCommands.subscribe(wrap("ch-reg"))) //
				.thenReturn(Mono.empty()) //
				.thenReturn(Mono.fromRunnable(() -> order.add("SUBSCRIBE")));
		when(reactiveCommands.unsubscribe(wrap("ch-reg"))).thenReturn(Mono.<Void> defer(() -> {
			unsubscribeEntered.countDown();
			try {
				assertThat(allowUnsubscribe.await(5, TimeUnit.SECONDS)).isTrue();
			} catch (InterruptedException ex) {
				Thread.currentThread().interrupt();
				return Mono.error(ex);
			}
			order.add("UNSUBSCRIBE");
			return Mono.empty();
		}).subscribeOn(Schedulers.boundedElastic()));

		sut.subscribe(wrap("ch-reg")).as(StepVerifier::create).verifyComplete();

		CompletableFuture<Void> unsubFuture = CompletableFuture.runAsync(() -> sut.unsubscribe(wrap("ch-reg")).block());
		assertThat(unsubscribeEntered.await(5, TimeUnit.SECONDS)).isTrue();

		ByteArrayWrapper wrapper = new ByteArrayWrapper("ch-reg".getBytes());
		CompletableFuture<Void> resubFuture = CompletableFuture.runAsync(() -> sut.subscribe(wrap("ch-reg")).block());
		await().atMost(Duration.ofSeconds(5)).until(() -> sut.getChannels().containsKey(wrapper));
		assertThat(order).isEmpty();

		allowUnsubscribe.countDown();
		unsubFuture.get(5, TimeUnit.SECONDS);
		resubFuture.get(5, TimeUnit.SECONDS);

		assertThat(order).containsExactly("UNSUBSCRIBE", "SUBSCRIBE");
		assertThat(sut.getChannels().get(wrapper).getSubscriberCount()).isEqualTo(1L);

		// Pre-rollback ACK: subscribed() arrives while in-flight subscribe is still pending, then subscribe is cancelled
		Sinks.Empty<Void> neverCompletes = Sinks.empty();
		when(reactiveCommands.subscribe(wrap("ch-early-ack"))).thenReturn(neverCompletes.asMono());
		when(reactiveCommands.unsubscribe(wrap("ch-early-ack"))).thenReturn(Mono.empty());

		ArgumentCaptor<RedisPubSubListener<ByteBuffer, ByteBuffer>> pubSubCaptor = ArgumentCaptor
				.forClass(RedisPubSubListener.class);
		verify(lettuceConnection).addListener(pubSubCaptor.capture());
		RedisPubSubListener<ByteBuffer, ByteBuffer> listener = pubSubCaptor.getValue();

		reactor.core.Disposable inFlight = sut.subscribe(wrap("ch-early-ack")).subscribe();
		listener.subscribed(wrap("ch-early-ack"), 1);
		verify(reactiveCommands, never()).unsubscribe(wrap("ch-early-ack"));

		inFlight.dispose();
		verify(reactiveCommands, times(1)).unsubscribe(wrap("ch-early-ack"));

		// Recheck during failed first cleanup must still trigger follow-up cleanup
		Sinks.One<Void> failingCleanup = Sinks.one();
		when(reactiveCommands.subscribe(wrap("ch-recheck-err")))
				.thenReturn(Mono.error(new RedisCommandTimeoutException("timeout")));
		when(reactiveCommands.unsubscribe(wrap("ch-recheck-err"))).thenReturn(failingCleanup.asMono())
				.thenReturn(Mono.empty());

		sut.subscribe(wrap("ch-recheck-err")).as(StepVerifier::create).expectError(QueryTimeoutException.class).verify();
		listener.subscribed(wrap("ch-recheck-err"), 1);
		listener.subscribed(wrap("ch-recheck-err"), 1);
		failingCleanup.tryEmitError(new RedisCommandTimeoutException("cleanup-timeout"));
		verify(reactiveCommands, times(2)).unsubscribe(wrap("ch-recheck-err"));
	}

	@Test // GH-3508
	@SuppressWarnings("unchecked")
	void closeLaterShouldDisposePendingCleanupAndCancelTrackedSubscriptionsBeforeRelease() {

		LettuceConnectionProvider provider = mock(LettuceConnectionProvider.class);
		StatefulConnection<ByteBuffer, ByteBuffer> dedicated = mock(StatefulConnection.class);
		when(provider.getConnectionAsync(StatefulConnection.class))
				.thenReturn(CompletableFuture.completedFuture(dedicated));
		when(provider.getConnectionAsync(StatefulRedisPubSubConnection.class))
				.thenReturn(CompletableFuture.completedFuture(lettuceConnection));
		when(provider.releaseAsync(any())).thenReturn(CompletableFuture.completedFuture(null));

		LettuceReactiveRedisConnection realConnection = new LettuceReactiveRedisConnection(provider);
		LettuceReactivePubSubCommands realPubSub = (LettuceReactivePubSubCommands) realConnection.pubSubCommands();

		AtomicBoolean cleanupDisposed = new AtomicBoolean();
		when(reactiveCommands.subscribe(wrap("ch-sub"))).thenReturn(Mono.empty());
		when(reactiveCommands.unsubscribe(wrap("ch-sub"))).thenReturn(Mono.empty());
		when(reactiveCommands.ping()).thenReturn(Mono.just("PONG"));
		when(reactiveCommands.subscribe(wrap("ch-orphan")))
				.thenReturn(Mono.error(new RedisCommandTimeoutException("timeout")));
		when(reactiveCommands.unsubscribe(wrap("ch-orphan")))
				.thenReturn(Mono.<Void> never().doOnCancel(() -> cleanupDisposed.set(true)));

		ReactiveSubscription activeSub = realPubSub.createSubscription(SubscriptionListener.NO_OP_SUBSCRIPTION_LISTENER)
				.block();
		activeSub.subscribe(wrap("ch-sub")).as(StepVerifier::create).verifyComplete();

		realPubSub.subscribe(wrap("ch-orphan")).as(StepVerifier::create).expectError(QueryTimeoutException.class)
				.verify();

		ArgumentCaptor<RedisPubSubListener<ByteBuffer, ByteBuffer>> pubSubCaptor = ArgumentCaptor
				.forClass(RedisPubSubListener.class);
		verify(lettuceConnection, times(2)).addListener(pubSubCaptor.capture());
		pubSubCaptor.getAllValues().get(0).subscribed(wrap("ch-orphan"), 1);

		realConnection.closeLater().as(StepVerifier::create).verifyComplete();

		assertThat(cleanupDisposed).isTrue();
		verify(reactiveCommands).unsubscribe(wrap("ch-sub"));
		verify(lettuceConnection, times(2)).removeListener(any(RedisPubSubListener.class));
		verify(provider).releaseAsync(lettuceConnection);
		realPubSub.createSubscription(SubscriptionListener.NO_OP_SUBSCRIPTION_LISTENER).as(StepVerifier::create)
				.expectError(IllegalStateException.class).verify();
	}

	@Test // GH-3508
	@SuppressWarnings("unchecked")
	void closeLaterShouldFailSubscriptionAwaitingOrphanCleanupWithoutDispatchingSecondSubscribe() throws Exception {

		LettuceConnectionProvider provider = mock(LettuceConnectionProvider.class);
		StatefulConnection<ByteBuffer, ByteBuffer> dedicated = mock(StatefulConnection.class);
		when(provider.getConnectionAsync(StatefulConnection.class))
				.thenReturn(CompletableFuture.completedFuture(dedicated));
		when(provider.getConnectionAsync(StatefulRedisPubSubConnection.class))
				.thenReturn(CompletableFuture.completedFuture(lettuceConnection));
		when(provider.releaseAsync(any())).thenReturn(CompletableFuture.completedFuture(null));

		LettuceReactiveRedisConnection realConnection = new LettuceReactiveRedisConnection(provider);
		LettuceReactivePubSubCommands realPubSub = (LettuceReactivePubSubCommands) realConnection.pubSubCommands();

		Sinks.One<Void> pendingCleanupUnsub = Sinks.one();
		when(reactiveCommands.subscribe(wrap("ch-close-wait")))
				.thenReturn(Mono.error(new RedisCommandTimeoutException("timeout"))).thenReturn(Mono.empty());
		when(reactiveCommands.unsubscribe(wrap("ch-close-wait"))).thenReturn(pendingCleanupUnsub.asMono());

		// 1. First subscription fails with timeout
		realPubSub.subscribe(wrap("ch-close-wait")).as(StepVerifier::create).expectError(QueryTimeoutException.class)
				.verify();

		ArgumentCaptor<RedisPubSubListener<ByteBuffer, ByteBuffer>> pubSubCaptor = ArgumentCaptor
				.forClass(RedisPubSubListener.class);
		verify(lettuceConnection).addListener(pubSubCaptor.capture());
		RedisPubSubListener<ByteBuffer, ByteBuffer> listener = pubSubCaptor.getValue();

		// 2. Late ACK starts orphan cleanup and holds completion pending
		listener.subscribed(wrap("ch-close-wait"), 1);
		verify(reactiveCommands, times(1)).unsubscribe(wrap("ch-close-wait"));

		// 3. Second subscription on the same channel acquires the connection and enters cleanup wait
		AtomicReference<Throwable> secondSubError = new AtomicReference<>();
		CountDownLatch secondSubTerminated = new CountDownLatch(1);
		ByteArrayWrapper wrapper = new ByteArrayWrapper("ch-close-wait".getBytes());

		realPubSub.subscribe(wrap("ch-close-wait")).subscribe(v -> {}, ex -> {
			secondSubError.set(ex);
			secondSubTerminated.countDown();
		});

		assertThat(realPubSub.getChannels()).containsKey(wrapper);
		assertThat(realPubSub.getChannels().get(wrapper).getSubscriberCount()).isEqualTo(1L);
		verify(reactiveCommands, times(1)).subscribe(wrap("ch-close-wait"));

		// 4. Closing the parent connection aborts the waiting subscription with closed error instead of resuming SUBSCRIBE
		realConnection.closeLater().as(StepVerifier::create).verifyComplete();

		// 5. Waiting subscription fails with IllegalStateException, 2nd native SUBSCRIBE is never called, and state is clean
		assertThat(secondSubTerminated.await(5, TimeUnit.SECONDS)).isTrue();
		assertThat(secondSubError.get()).isInstanceOf(IllegalStateException.class)
				.hasMessage("Unable to connect; Connection is closed");

		pendingCleanupUnsub.tryEmitEmpty();
		verify(reactiveCommands, times(1)).subscribe(wrap("ch-close-wait"));
		assertThat(realPubSub.getChannels()).isEmpty();
		assertThat(realPubSub.getActiveChannelCleanups()).isEmpty();
		verify(provider).releaseAsync(lettuceConnection);
	}

	private static ByteBuffer wrap(String content) {
		return ByteBuffer.wrap(content.getBytes());
	}
}
