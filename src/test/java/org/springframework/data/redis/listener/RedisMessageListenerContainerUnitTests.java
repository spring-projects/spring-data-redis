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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.Subscription;
import org.springframework.data.redis.connection.SubscriptionListener;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.listener.adapter.MessageListenerAdapter;
import org.springframework.data.redis.listener.adapter.RedisListenerExecutionFailedException;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Unit tests for {@link RedisMessageListenerContainer}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @author Seongjun Lee
 * @author rlaehddus302
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

	@Test // GH-3447
	void concurrentInitialAddMessageListenerShouldInitiateSingleSubscription() throws Exception {

		AtomicBoolean holdCallers = new AtomicBoolean();
		CountDownLatch stateInspected = new CountDownLatch(2);
		Semaphore gate = new Semaphore(0);
		CountDownLatch keepSubscribed = new CountDownLatch(1);
		AtomicReference<SubscriptionListener> subscriptionListener = new AtomicReference<>();
		List<String> additionallySubscribed = new CopyOnWriteArrayList<>();

		container = new RedisMessageListenerContainer();
		container.setConnectionFactory(connectionFactoryMock);
		container.setBeanName("container");
		container.setTaskExecutor(new SyncTaskExecutor());
		container.setSubscriptionExecutor(new SimpleAsyncTaskExecutor());
		container.setMaxSubscriptionRegistrationWaitingTime(1000);

		// BackOff.start() is called after the listening state was inspected and before the subscription is initiated.
		// Holding both callers here makes each of them observe "not listening".
		container.setRecoveryBackoff(() -> {

			if (holdCallers.get()) {
				stateInspected.countDown();
				gate.acquireUninterruptibly();
			}

			return new FixedBackOff(1, 5).start();
		});
		container.afterPropertiesSet();

		when(connectionFactoryMock.getConnection()).thenReturn(connectionMock);
		when(connectionMock.getSubscription()).thenReturn(subscriptionMock);

		doAnswer(it -> {

			SubscriptionListener listener = it.getArgument(0);
			subscriptionListener.set(listener);
			when(connectionMock.isSubscribed()).thenReturn(true);

			for (byte[] channel : channelsOf(it.getArguments(), 1)) {
				listener.onChannelSubscribed(channel, 1);
			}

			keepSubscribed.await();
			return null;
		}).when(connectionMock).subscribe(any(), any(byte[][].class));

		doAnswer(it -> {

			for (byte[] channel : channelsOf(it.getArguments(), 0)) {
				additionallySubscribed.add(new String(channel, StandardCharsets.UTF_8));
				subscriptionListener.get().onChannelSubscribed(channel, 1);
			}

			return null;
		}).when(subscriptionMock).subscribe(any(byte[][].class));

		container.start();
		holdCallers.set(true);

		ExecutorService callers = Executors.newFixedThreadPool(2);

		try {

			Future<?> first = callers.submit(() -> container.addMessageListener(adapter, new ChannelTopic("a")));
			Future<?> second = callers.submit(() -> container.addMessageListener(adapter, new ChannelTopic("b")));

			assertThat(stateInspected.await(5, TimeUnit.SECONDS)).isTrue();

			// one caller initiates the subscription and completes it ...
			gate.release();

			long deadline = System.currentTimeMillis() + 5000;
			while (!container.isListening() && System.currentTimeMillis() < deadline) {
				Thread.sleep(10);
			}

			assertThat(container.isListening()).isTrue();

			// ... then the other one proceeds although it observed "not listening" earlier
			gate.release();

			first.get(5, TimeUnit.SECONDS);
			second.get(5, TimeUnit.SECONDS);

			// a single subscription connection
			verify(connectionFactoryMock, times(1)).getConnection();

			// the caller that did not initiate the subscription subscribes its channel explicitly
			assertThat(additionallySubscribed).hasSize(1).containsAnyOf("a", "b");
		} finally {
			keepSubscribed.countDown();
			callers.shutdownNow();
		}
	}

	private static List<byte[]> channelsOf(Object[] arguments, int fromIndex) {

		List<byte[]> channels = new ArrayList<>();

		for (int i = fromIndex; i < arguments.length; i++) {

			if (arguments[i] instanceof byte[][] array) {
				channels.addAll(List.of(array));
			} else if (arguments[i] instanceof byte[] channel) {
				channels.add(channel);
			}
		}

		return channels;
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

}
