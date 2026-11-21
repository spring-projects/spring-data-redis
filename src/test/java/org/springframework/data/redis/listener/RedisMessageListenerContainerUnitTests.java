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
import static org.awaitility.Awaitility.*;
import static org.mockito.Mockito.*;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.Subscription;
import org.springframework.data.redis.connection.SubscriptionListener;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.util.ByteArrayWrapper;
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
 * @author Daeryeon Choi
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

	@ParameterizedTest // GH-3508
	@ValueSource(booleans = { true, false })
	@SuppressWarnings("unchecked")
	void partialRemoveListenerFailureShouldRollbackStateAndAllowRetry(boolean specifyTopic) {

		LettuceConnectionFactory asyncFactory = mock(LettuceConnectionFactory.class);
		RedisConnection asyncConnection = mock(RedisConnection.class);
		Subscription asyncSubscription = mock(Subscription.class);
		AtomicReference<SubscriptionListener> listenerRef = new AtomicReference<>();

		RedisMessageListenerContainer asyncContainer = createStartedAsyncContainer(asyncFactory, asyncConnection,
				asyncSubscription, listenerRef);

		MessageListener listener1 = mock(MessageListener.class);
		MessageListener listener2 = mock(MessageListener.class);
		byte[] ch1 = "ch1".getBytes(StandardCharsets.UTF_8);
		byte[] ch2 = "ch2".getBytes(StandardCharsets.UTF_8);

		doAnswer(inv -> {
			listenerRef.get().onChannelSubscribed(ch1, 1);
			return null;
		}).when(asyncSubscription).subscribe(ch1);

		doAnswer(inv -> {
			listenerRef.get().onChannelSubscribed(ch2, 2);
			return null;
		}).when(asyncSubscription).subscribe(ch2);

		asyncContainer.addMessageListener(listener1, new ChannelTopic("ch1"));
		asyncContainer.addMessageListener(listener2, new ChannelTopic("ch2"));

		doThrow(new QueryTimeoutException("timeout")).doNothing().when(asyncSubscription).unsubscribe(ch1);
		Runnable removeAction = specifyTopic
				? () -> asyncContainer.removeMessageListener(listener1, new ChannelTopic("ch1"))
				: () -> asyncContainer.removeMessageListener(listener1);

		assertThatExceptionOfType(QueryTimeoutException.class).isThrownBy(removeAction::run);

		Map<ByteArrayWrapper, Collection<MessageListener>> channelMapping = (Map<ByteArrayWrapper, Collection<MessageListener>>) ReflectionTestUtils
				.getField(asyncContainer, "channelMapping");
		Map<MessageListener, Set<Topic>> listenerTopics = (Map<MessageListener, Set<Topic>>) ReflectionTestUtils
				.getField(asyncContainer, "listenerTopics");

		assertThat(channelMapping).containsKey(new ByteArrayWrapper(ch1));
		assertThat(listenerTopics).containsKey(listener1);
		assertThat(listenerTopics.get(listener1)).containsExactly(new ChannelTopic("ch1"));

		removeAction.run();
		verify(asyncSubscription, times(2)).unsubscribe(ch1);
		assertThat(channelMapping).doesNotContainKey(new ByteArrayWrapper(ch1));
		assertThat(listenerTopics).doesNotContainKey(listener1);
		assertThat(channelMapping).containsKey(new ByteArrayWrapper(ch2));
	}

	@Test // GH-3508
	void delayedUnsubscribeAckAfterPartialRemoveRollbackShouldResubscribeRetainedTopic() {

		LettuceConnectionFactory asyncFactory = mock(LettuceConnectionFactory.class);
		RedisConnection asyncConnection = mock(RedisConnection.class);
		Subscription asyncSubscription = mock(Subscription.class);
		AtomicReference<SubscriptionListener> listenerRef = new AtomicReference<>();

		RedisMessageListenerContainer asyncContainer = createStartedAsyncContainer(asyncFactory, asyncConnection,
				asyncSubscription, listenerRef);

		MessageListener listener1 = mock(MessageListener.class);
		MessageListener listener2 = mock(MessageListener.class);
		byte[] ch1 = "ch1".getBytes(StandardCharsets.UTF_8);
		byte[] ch2 = "ch2".getBytes(StandardCharsets.UTF_8);

		doAnswer(inv -> {
			listenerRef.get().onChannelSubscribed(ch1, 1);
			return null;
		}).when(asyncSubscription).subscribe(ch1);
		doAnswer(inv -> {
			listenerRef.get().onChannelSubscribed(ch2, 2);
			return null;
		}).when(asyncSubscription).subscribe(ch2);

		asyncContainer.addMessageListener(listener1, new ChannelTopic("ch1"));
		asyncContainer.addMessageListener(listener2, new ChannelTopic("ch2"));

		doThrow(new QueryTimeoutException("timeout")).when(asyncSubscription).unsubscribe(ch1);
		assertThatExceptionOfType(QueryTimeoutException.class)
				.isThrownBy(() -> asyncContainer.removeMessageListener(listener1, new ChannelTopic("ch1")));

		// First restore attempt fails (e.g., socket disconnecting), then reconnect onChannelSubscribed triggers retry
		doThrow(new QueryTimeoutException("disconnected")).doNothing().when(asyncSubscription).subscribe(ch1);
		listenerRef.get().onChannelUnsubscribed(ch1, 1);
		await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> verify(asyncSubscription, times(2)).subscribe(ch1));

		listenerRef.get().onChannelSubscribed(ch2, 1);
		await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> verify(asyncSubscription, times(3)).subscribe(ch1));
	}

	@Test // GH-3508
	void orphanSubscriptionNotificationShouldTriggerAsyncUnsubscribe() {

		LettuceConnectionFactory asyncFactory = mock(LettuceConnectionFactory.class);
		RedisConnection asyncConnection = mock(RedisConnection.class);
		Subscription asyncSubscription = mock(Subscription.class);
		AtomicReference<SubscriptionListener> listenerRef = new AtomicReference<>();

		RedisMessageListenerContainer asyncContainer = createStartedAsyncContainer(asyncFactory, asyncConnection,
				asyncSubscription, listenerRef);

		MessageListener listener = mock(MessageListener.class);
		byte[] activeCh = "active-ch".getBytes(StandardCharsets.UTF_8);
		byte[] orphanCh = "orphan-ch".getBytes(StandardCharsets.UTF_8);
		byte[] orphanPat = "orphan-pat*".getBytes(StandardCharsets.UTF_8);

		doAnswer(inv -> {
			listenerRef.get().onChannelSubscribed(activeCh, 1);
			return null;
		}).when(asyncSubscription).subscribe(activeCh);

		asyncContainer.addMessageListener(listener, new ChannelTopic("active-ch"));

		listenerRef.get().onChannelSubscribed(orphanCh, 2);
		listenerRef.get().onPatternSubscribed(orphanPat, 3);

		await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
			verify(asyncSubscription).unsubscribe(orphanCh);
			verify(asyncSubscription).pUnsubscribe(orphanPat);
		});
		verify(asyncSubscription, never()).unsubscribe(activeCh);
	}

	private RedisMessageListenerContainer createStartedAsyncContainer(LettuceConnectionFactory asyncFactory,
			RedisConnection asyncConnection, Subscription asyncSubscription,
			AtomicReference<SubscriptionListener> listenerRef) {

		byte[] initCh = "init-ch".getBytes(StandardCharsets.UTF_8);
		when(asyncFactory.getConnection()).thenReturn(asyncConnection);
		when(asyncConnection.getSubscription()).thenReturn(asyncSubscription);
		when(asyncConnection.isSubscribed()).thenReturn(false, true);

		doAnswer(inv -> {
			SubscriptionListener listener = inv.getArgument(0);
			listenerRef.set(listener);
			listener.onChannelSubscribed(initCh, 1);
			return null;
		}).when(asyncConnection).subscribe(any(), any());

		RedisMessageListenerContainer asyncContainer = new RedisMessageListenerContainer();
		asyncContainer.setConnectionFactory(asyncFactory);
		asyncContainer.setBeanName("asyncContainer");
		asyncContainer.setTaskExecutor(new SimpleAsyncTaskExecutor());
		asyncContainer.setMaxSubscriptionRegistrationWaitingTime(2000);
		asyncContainer.afterPropertiesSet();

		MessageListener bootstrapListener = mock(MessageListener.class);
		asyncContainer.addMessageListener(bootstrapListener, new ChannelTopic("init-ch"));
		asyncContainer.start();

		return asyncContainer;
	}

}
