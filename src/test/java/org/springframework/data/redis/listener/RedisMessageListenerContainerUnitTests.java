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

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisInvalidSubscriptionException;
import org.springframework.data.redis.connection.Subscription;
import org.springframework.data.redis.connection.SubscriptionListener;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.listener.adapter.MessageListenerAdapter;
import org.springframework.data.redis.listener.adapter.RedisListenerExecutionFailedException;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Unit tests for {@link RedisMessageListenerContainer}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @author Seongjun Lee
 */
class RedisMessageListenerContainerUnitTests {

	private final Object handler = new Object() {

		@SuppressWarnings("unused")
		public void handleMessage(Object message) {}
	};

	private final MessageListenerAdapter adapter = new MessageListenerAdapter(handler);

	private RedisMessageListenerContainer container;

	private RedisConnectionFactory connectionFactoryMock;
	private RedisConnection connectionMock;
	private Subscription subscriptionMock;
	private Executor executorMock;

	@BeforeEach
	void setUp() {

		executorMock = mock(Executor.class);
		connectionFactoryMock = mock(JedisConnectionFactory.class);
		connectionMock = mock(RedisConnection.class);
		subscriptionMock = mock(Subscription.class);

		container = new RedisMessageListenerContainer();
		container.setConnectionFactory(connectionFactoryMock);
		container.setBeanName("container");
		container.setTaskExecutor(new SyncTaskExecutor());
		container.setSubscriptionExecutor(executorMock);
		container.setMaxSubscriptionRegistrationWaitingTime(1);
		container.afterPropertiesSet();
	}

	@Test // DATAREDIS-840
	void containerShouldStopGracefullyOnUnsubscribeErrors() {

		when(connectionFactoryMock.getConnection()).thenReturn(connectionMock);
		doThrow(new IllegalStateException()).when(subscriptionMock).pUnsubscribe();

		doAnswer(it -> {

			Runnable r = it.getArgument(0);
			r.run();
			return null;
		}).when(executorMock).execute(any());

		doAnswer(it -> {

			SubscriptionListener listener = it.getArgument(0);
			when(connectionMock.isSubscribed()).thenReturn(true);

			listener.onChannelSubscribed("a".getBytes(StandardCharsets.UTF_8), 0);

			return null;
		}).when(connectionMock).subscribe(any(), any());

		container.addMessageListener(adapter, new ChannelTopic("a"));
		container.start();

		when(connectionMock.getSubscription()).thenReturn(subscriptionMock);

		container.stop();

		assertThat(container.isRunning()).isFalse();
		verify(connectionMock).close();
	}

	@Test // GH-2335
	void containerStartShouldReportFailureOnRedisUnavailability() {

		when(connectionFactoryMock.getConnection()).thenThrow(new RedisConnectionFailureException("Booh"));

		doAnswer(it -> {

			Runnable r = it.getArgument(0);
			r.run();
			return null;
		}).when(executorMock).execute(any());

		container.addMessageListener(adapter, new ChannelTopic("a"));
		assertThatExceptionOfType(RedisListenerExecutionFailedException.class).isThrownBy(() -> container.start());

		assertThat(container.isRunning()).isTrue();
		assertThat(container.isListening()).isFalse();
	}

	@Test // GH-2335
	void containerListenShouldReportFailureOnRedisUnavailability() {

		when(connectionFactoryMock.getConnection()).thenThrow(new RedisConnectionFailureException("Booh"));

		doAnswer(it -> {

			Runnable r = it.getArgument(0);
			r.run();
			return null;
		}).when(executorMock).execute(any());

		container.start();

		assertThatExceptionOfType(RedisListenerExecutionFailedException.class)
				.isThrownBy(() -> container.addMessageListener(adapter, new ChannelTopic("a")));

		assertThat(container.isRunning()).isTrue();
		assertThat(container.isListening()).isFalse();
	}

	@Test // GH-2335
	void shouldRecoverFromConnectionFailure() throws Exception {

		AtomicInteger requestCount = new AtomicInteger();
		AtomicBoolean shouldThrowSubscriptionException = new AtomicBoolean();

		container = new RedisMessageListenerContainer();
		container.setConnectionFactory(connectionFactoryMock);
		container.setBeanName("container");
		container.setTaskExecutor(new SyncTaskExecutor());
		container.setSubscriptionExecutor(new SimpleAsyncTaskExecutor());
		container.setMaxSubscriptionRegistrationWaitingTime(1000);
		container.setRecoveryBackoff(new FixedBackOff(1, 5));
		container.afterPropertiesSet();

		doAnswer(it -> {

			int req = requestCount.incrementAndGet();
			if (req == 1 || req == 3) {
				return connectionMock;
			}

			throw new RedisConnectionFailureException("Booh");
		}).when(connectionFactoryMock).getConnection();

		CountDownLatch exceptionWait = new CountDownLatch(1);
		CountDownLatch armed = new CountDownLatch(1);
		CountDownLatch recoveryArmed = new CountDownLatch(1);

		doAnswer(it -> {

			SubscriptionListener listener = it.getArgument(0);
			when(connectionMock.isSubscribed()).thenReturn(true);

			listener.onChannelSubscribed("a".getBytes(StandardCharsets.UTF_8), 1);

			armed.countDown();
			exceptionWait.await();

			if (shouldThrowSubscriptionException.compareAndSet(true, false)) {
				when(connectionMock.isSubscribed()).thenReturn(false);
				throw new RedisConnectionFailureException("Disconnected");
			}

			recoveryArmed.countDown();

			return null;
		}).when(connectionMock).subscribe(any(), any());

		container.start();
		container.addMessageListener(new MessageListenerAdapter(handler), new ChannelTopic("a"));
		armed.await();

		// let an exception happen
		shouldThrowSubscriptionException.set(true);
		exceptionWait.countDown();

		// wait for subscription recovery
		recoveryArmed.await();

		assertThat(recoveryArmed.getCount()).isZero();

	}

	@Test // GH-964
	void failsOnDuplicateInit() {
		assertThatIllegalStateException().isThrownBy(() -> container.afterPropertiesSet());
	}

	@Test // GH-3237
	void removeListenerBySingleTopicShouldFailWhenTopicIsNull() {
		assertThatIllegalArgumentException().isThrownBy(() -> container.removeMessageListener(adapter, (Topic) null));
	}

	@Test // GH-3237
	void removeListenerBySingleTopicShouldFailWhenListenerIsNull() {
		assertThatIllegalArgumentException().isThrownBy(() -> container.removeMessageListener(null, new ChannelTopic("a")));
	}

	@Test // GH-3237
	void removeListenerBySetShouldFailWhenListenerIsNull() {
		assertThatIllegalArgumentException().isThrownBy(() -> container.removeMessageListener(null, Collections.emptySet()));
	}

	@Test // GH-3237
	void removeListenerBySetShouldFailWhenSetIsNull() {
		assertThatIllegalArgumentException().isThrownBy(() -> container.removeMessageListener(adapter, (Set)null));
	}

	@Test // GH-3237
	void removeListenerFromAllTopicsShouldFailWhenListenerIsNull() {
		assertThatIllegalArgumentException().isThrownBy(() -> container.removeMessageListener(null));
	}

	@Test // GH-3208
	void defaultPhaseShouldBeMaxValue() {
		assertThat(container.getPhase()).isEqualTo(Integer.MAX_VALUE);
	}

	@Test // GH-3208
	void shouldApplyConfiguredPhase() {
		container.setPhase(3208);
		assertThat(container.getPhase()).isEqualTo(3208);
	}

	@Test // GH-3208
	void defaultAutoStartupShouldBeTrue() {
		assertThat(container.isAutoStartup()).isEqualTo(true);
	}

	@Test // GH-3208
	void shouldApplyConfiguredAutoStartup() {
		container.setAutoStartup(false);
		assertThat(container.isAutoStartup()).isEqualTo(false);
	}

	@Test // GH-3080
	void shouldRecoverWhenSubscriptionDiesOnConcurrentRemove() {

		// The race only manifests with async drivers (Lettuce) because BlockingSubscriber
		// sets connection=null after doSubscribe, making incremental subscribeChannel a no-op.
		// Use a LettuceConnectionFactory mock so the async Subscriber code path is exercised.
		LettuceConnectionFactory asyncFactory = mock(LettuceConnectionFactory.class);

		RedisMessageListenerContainer asyncContainer = new RedisMessageListenerContainer();
		asyncContainer.setConnectionFactory(asyncFactory);
		asyncContainer.setBeanName("async-container");
		asyncContainer.setTaskExecutor(new SyncTaskExecutor());
		asyncContainer.setMaxSubscriptionRegistrationWaitingTime(1000);
		asyncContainer.afterPropertiesSet();

		when(asyncFactory.getConnection()).thenReturn(connectionMock);

		// Initial subscription: listener1 on "a". Fires onChannelSubscribed synchronously.
		// Use any(byte[][].class) for the vararg parameter so the stub matches regardless of
		// how many channels are passed (1 for initial, 2 for recovery full re-subscribe).
		doAnswer(it -> {
			SubscriptionListener listener = it.getArgument(0);
			when(connectionMock.isSubscribed()).thenReturn(true);
			listener.onChannelSubscribed("a".getBytes(StandardCharsets.UTF_8), 1);
			return null;
		}).when(connectionMock).subscribe(any(), any(byte[][].class));

		asyncContainer.addMessageListener(adapter, new ChannelTopic("a"));
		asyncContainer.start();
		assertThat(asyncContainer.isListening()).isTrue();

		// connection.getSubscription() now returns subscriptionMock for incremental subscribes.
		when(connectionMock.getSubscription()).thenReturn(subscriptionMock);

		// Simulate the race: removeMessageListener concurrently unsubscribed the last channel,
		// which triggered AbstractSubscription.closeIfUnsubscribed() → alive=false.
		// Any incremental subscribe("b") now throws RedisInvalidSubscriptionException.
		doThrow(new RedisInvalidSubscriptionException("Subscription has been unsubscribed"))
				.when(subscriptionMock).subscribe(any());

		// Recovery subscription answer: fires "a" and "b" (full re-subscribe from channelMapping).
		// Override the stub so the recovery connection.subscribe uses this answer.
		doAnswer(it -> {
			SubscriptionListener listener = it.getArgument(0);
			when(connectionMock.isSubscribed()).thenReturn(true);
			listener.onChannelSubscribed("a".getBytes(StandardCharsets.UTF_8), 1);
			listener.onChannelSubscribed("b".getBytes(StandardCharsets.UTF_8), 1);
			return null;
		}).when(connectionMock).subscribe(any(), any(byte[][].class));

		// Reset isSubscribed so Subscriber.initialize() does not bail out for the recovery call.
		when(connectionMock.isSubscribed()).thenReturn(false);

		MessageListener listener2 = (message, pattern) -> {};

		// Should NOT throw: fix catches RedisInvalidSubscriptionException, calls stopListening()
		// to dispose the dead connection, then lazyListen() to re-subscribe all channelMapping
		// entries (which already contain "b" added above).
		assertThatNoException()
				.isThrownBy(() -> asyncContainer.addMessageListener(listener2, new ChannelTopic("b")));

		// subscriptionMock.subscribe was called exactly once (the failed incremental attempt for "b").
		verify(subscriptionMock, times(1)).subscribe(any(byte[][].class));

		// connection.subscribe was called twice: initial ("a") + recovery ("a","b").
		verify(connectionMock, times(2)).subscribe(any(), any(byte[][].class));

		// Container recovered and is listening again.
		assertThat(asyncContainer.isListening()).isTrue();

		// listener2 is registered in listenerTopics – no memory leak.
		@SuppressWarnings("unchecked")
		Map<MessageListener, Set<Topic>> listenerTopics =
				(Map<MessageListener, Set<Topic>>) ReflectionTestUtils.getField(asyncContainer, "listenerTopics");
		assertThat(listenerTopics).containsKey(listener2);

		try {
			asyncContainer.destroy();
		} catch (Exception ignored) {}
	}

}
