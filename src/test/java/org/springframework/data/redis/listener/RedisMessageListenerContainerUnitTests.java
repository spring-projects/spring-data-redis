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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
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
	@SuppressWarnings("unchecked")
	void dynamicAddListenerFailureShouldRollbackMappingsAndRemoveSynchronization() {

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

		asyncContainer.addMessageListener(listener1, new ChannelTopic("ch1"));

		doThrow(new QueryTimeoutException("timeout")).when(asyncSubscription).subscribe(ch2);
		assertThatExceptionOfType(RedisListenerExecutionFailedException.class)
				.isThrownBy(() -> asyncContainer.addMessageListener(listener1, new ChannelTopic("ch2")))
				.withCauseInstanceOf(QueryTimeoutException.class);

		// Existing listener1 on ch1 must be preserved while ch2 is rolled back
		Map<ByteArrayWrapper, Collection<MessageListener>> channelMapping = (Map<ByteArrayWrapper, Collection<MessageListener>>) ReflectionTestUtils
				.getField(asyncContainer, "channelMapping");
		Map<MessageListener, Set<Topic>> listenerTopics = (Map<MessageListener, Set<Topic>>) ReflectionTestUtils
				.getField(asyncContainer, "listenerTopics");

		assertThat(channelMapping.get(new ByteArrayWrapper(ch1))).containsExactly(listener1);
		assertThat(channelMapping).doesNotContainKey(new ByteArrayWrapper(ch2));
		assertThat(listenerTopics.get(listener1)).containsExactly(new ChannelTopic("ch1"));

		// Also verify adding another listener to existing ch1 rolls back only listener2 on failure
		doThrow(new QueryTimeoutException("timeout")).when(asyncSubscription).subscribe(ch1);
		assertThatExceptionOfType(RedisListenerExecutionFailedException.class)
				.isThrownBy(() -> asyncContainer.addMessageListener(listener2, new ChannelTopic("ch1")))
				.withCauseInstanceOf(QueryTimeoutException.class);

		assertThat(channelMapping.get(new ByteArrayWrapper(ch1))).containsExactly(listener1);
		assertThat(listenerTopics).doesNotContainKey(listener2);

		// Verify no leaked SubscriptionSynchronization instances remain
		Object subscriber = ReflectionTestUtils.getField(asyncContainer, "subscriber");
		SynchronizingMessageListener syncListener = (SynchronizingMessageListener) ReflectionTestUtils.getField(subscriber,
				"synchronizingMessageListener");
		List<?> synchronizations = (List<?>) ReflectionTestUtils.getField(syncListener, "synchronizations");
		assertThat(synchronizations).isEmpty();
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

	@Test // GH-3508
	void shouldContinueRecoveryOnTransientQueryTimeoutException() throws Exception {

		AtomicInteger requestCount = new AtomicInteger();
		AtomicBoolean shouldThrowSubscriptionException = new AtomicBoolean();

		RedisMessageListenerContainer recoveryContainer = new RedisMessageListenerContainer();
		recoveryContainer.setConnectionFactory(connectionFactoryMock);
		recoveryContainer.setBeanName("container");
		recoveryContainer.setTaskExecutor(new SyncTaskExecutor());
		recoveryContainer.setSubscriptionExecutor(new SimpleAsyncTaskExecutor());
		recoveryContainer.setMaxSubscriptionRegistrationWaitingTime(1000);
		recoveryContainer.setRecoveryBackoff(new FixedBackOff(1, 5));
		recoveryContainer.afterPropertiesSet();

		doAnswer(it -> {
			int req = requestCount.incrementAndGet();
			if (req == 1 || req == 3 || req == 4) {
				return connectionMock;
			}
			throw new QueryTimeoutException("transient connection acquisition timeout");
		}).when(connectionFactoryMock).getConnection();

		CountDownLatch exceptionWait = new CountDownLatch(1);
		CountDownLatch armed = new CountDownLatch(1);
		CountDownLatch recoveryArmed = new CountDownLatch(1);
		AtomicInteger subscribeCalls = new AtomicInteger();

		doAnswer(it -> {

			int call = subscribeCalls.incrementAndGet();
			SubscriptionListener listener = it.getArgument(0);

			if (call == 2) {
				when(connectionMock.isSubscribed()).thenReturn(false);
				throw new QueryTimeoutException("transient subscribe timeout");
			}

			when(connectionMock.isSubscribed()).thenReturn(true);
			listener.onChannelSubscribed("a".getBytes(StandardCharsets.UTF_8), 1);

			if (call == 1) {
				armed.countDown();
				exceptionWait.await();
				if (shouldThrowSubscriptionException.compareAndSet(true, false)) {
					when(connectionMock.isSubscribed()).thenReturn(false);
					throw new RedisConnectionFailureException("Disconnected");
				}
			} else {
				recoveryArmed.countDown();
			}

			return null;
		}).when(connectionMock).subscribe(any(), any());

		recoveryContainer.start();
		recoveryContainer.addMessageListener(new MessageListenerAdapter(handler), new ChannelTopic("a"));
		armed.await();

		shouldThrowSubscriptionException.set(true);
		exceptionWait.countDown();

		assertThat(recoveryArmed.await(5, TimeUnit.SECONDS)).isTrue();
	}

	@Test // GH-3508
	@SuppressWarnings("unchecked")
	void emptyStartedContainerAddListenerFailureShouldRollbackAndNotLeakOnNextAdd() {

		LettuceConnectionFactory asyncFactory = mock(LettuceConnectionFactory.class);
		RedisConnection asyncConnection = mock(RedisConnection.class);
		Subscription asyncSubscription = mock(Subscription.class);
		List<List<byte[]>> subscribedChannels = new ArrayList<>();

		byte[] ch2 = "ch2".getBytes(StandardCharsets.UTF_8);

		when(asyncFactory.getConnection()).thenThrow(new RedisConnectionFailureException("unavailable"))
				.thenReturn(asyncConnection);
		when(asyncConnection.getSubscription()).thenReturn(asyncSubscription);
		when(asyncConnection.isSubscribed()).thenReturn(false);

		doAnswer(inv -> {
			SubscriptionListener listener = inv.getArgument(0);
			byte[][] channels = (byte[][]) inv.getRawArguments()[1];
			subscribedChannels.add(Arrays.asList(channels));
			for (byte[] channel : channels) {
				listener.onChannelSubscribed(channel, 1);
			}
			return null;
		}).when(asyncConnection).subscribe(any(), any());

		RedisMessageListenerContainer emptyContainer = new RedisMessageListenerContainer();
		emptyContainer.setConnectionFactory(asyncFactory);
		emptyContainer.setBeanName("emptyContainer");
		emptyContainer.setTaskExecutor(new SyncTaskExecutor());
		emptyContainer.setMaxSubscriptionRegistrationWaitingTime(1000);
		emptyContainer.afterPropertiesSet();
		emptyContainer.start();

		MessageListener listener1 = mock(MessageListener.class);
		MessageListener listener2 = mock(MessageListener.class);

		assertThatExceptionOfType(RedisListenerExecutionFailedException.class)
				.isThrownBy(() -> emptyContainer.addMessageListener(listener1, new ChannelTopic("ch1")))
				.withCauseInstanceOf(RedisConnectionFailureException.class);

		Map<ByteArrayWrapper, Collection<MessageListener>> channelMapping = (Map<ByteArrayWrapper, Collection<MessageListener>>) ReflectionTestUtils
				.getField(emptyContainer, "channelMapping");
		Map<ByteArrayWrapper, Collection<MessageListener>> patternMapping = (Map<ByteArrayWrapper, Collection<MessageListener>>) ReflectionTestUtils
				.getField(emptyContainer, "patternMapping");
		Map<MessageListener, Set<Topic>> listenerTopics = (Map<MessageListener, Set<Topic>>) ReflectionTestUtils
				.getField(emptyContainer, "listenerTopics");

		assertThat(emptyContainer.isRunning()).isTrue();
		assertThat(emptyContainer.isListening()).isFalse();
		assertThat(channelMapping).isEmpty();
		assertThat(patternMapping).isEmpty();
		assertThat(listenerTopics).isEmpty();

		emptyContainer.addMessageListener(listener2, new ChannelTopic("ch2"));

		assertThat(subscribedChannels).hasSize(1);
		assertThat(subscribedChannels.get(0)).containsExactly(ch2);
		assertThat(channelMapping).containsOnlyKeys(new ByteArrayWrapper(ch2));
		assertThat(listenerTopics).containsOnlyKeys(listener2);
		assertThat(emptyContainer.isListening()).isTrue();
	}

	@Test // GH-3508
	@SuppressWarnings("unchecked")
	void staticListenersRegisteredBeforeStartShouldBePreservedAcrossStartAndRuntimeFailures() {

		LettuceConnectionFactory asyncFactory = mock(LettuceConnectionFactory.class);
		RedisConnection asyncConnection = mock(RedisConnection.class);
		Subscription asyncSubscription = mock(Subscription.class);
		List<List<byte[]>> subscribedChannels = new ArrayList<>();

		byte[] ch0 = "ch0".getBytes(StandardCharsets.UTF_8);

		when(asyncFactory.getConnection()) //
				.thenThrow(new RedisConnectionFailureException("start-fail")) //
				.thenThrow(new RedisConnectionFailureException("runtime-fail")) //
				.thenReturn(asyncConnection);
		when(asyncConnection.getSubscription()).thenReturn(asyncSubscription);
		when(asyncConnection.isSubscribed()).thenReturn(false);

		doAnswer(inv -> {
			SubscriptionListener listener = inv.getArgument(0);
			byte[][] channels = (byte[][]) inv.getRawArguments()[1];
			subscribedChannels.add(Arrays.asList(channels));
			for (byte[] channel : channels) {
				listener.onChannelSubscribed(channel, 1);
			}
			return null;
		}).when(asyncConnection).subscribe(any(), any());

		RedisMessageListenerContainer staticContainer = new RedisMessageListenerContainer();
		staticContainer.setConnectionFactory(asyncFactory);
		staticContainer.setBeanName("staticContainer");
		staticContainer.setTaskExecutor(new SyncTaskExecutor());
		staticContainer.setMaxSubscriptionRegistrationWaitingTime(1000);
		staticContainer.afterPropertiesSet();

		MessageListener staticListener = mock(MessageListener.class);
		MessageListener failedRuntimeListener = mock(MessageListener.class);

		staticContainer.addMessageListener(staticListener, new ChannelTopic("ch0"));

		assertThatExceptionOfType(RedisListenerExecutionFailedException.class).isThrownBy(staticContainer::start);
		assertThat(staticContainer.isRunning()).isTrue();
		assertThat(staticContainer.isListening()).isFalse();

		// Runtime addMessageListener failure while still not listening must roll back only the runtime listener
		assertThatExceptionOfType(RedisListenerExecutionFailedException.class)
				.isThrownBy(() -> staticContainer.addMessageListener(failedRuntimeListener, new ChannelTopic("ch1")));

		Map<ByteArrayWrapper, Collection<MessageListener>> channelMapping = (Map<ByteArrayWrapper, Collection<MessageListener>>) ReflectionTestUtils
				.getField(staticContainer, "channelMapping");
		Map<MessageListener, Set<Topic>> listenerTopics = (Map<MessageListener, Set<Topic>>) ReflectionTestUtils
				.getField(staticContainer, "listenerTopics");

		assertThat(channelMapping).containsOnlyKeys(new ByteArrayWrapper(ch0));
		assertThat(listenerTopics).containsOnlyKeys(staticListener);

		// After recovery, stop() + start() re-initializes and subscribes staticListener on ch0
		staticContainer.stop();
		staticContainer.start();

		assertThat(subscribedChannels).hasSize(1);
		assertThat(subscribedChannels.get(0)).containsExactly(ch0);
		assertThat(staticContainer.isRunning()).isTrue();
		assertThat(staticContainer.isListening()).isTrue();
	}

	@Test // GH-3508
	@SuppressWarnings("unchecked")
	void emptyContainerRuntimeAddListenerTimeoutFollowedByLateAckShouldCleanupOrphanAndEmptyListeningState() {

		LettuceConnectionFactory asyncFactory = mock(LettuceConnectionFactory.class);
		RedisConnection asyncConnection = mock(RedisConnection.class);
		Subscription asyncSubscription = mock(Subscription.class);
		AtomicReference<SubscriptionListener> listenerRef = new AtomicReference<>();
		List<List<byte[]>> subscribedChannels = new ArrayList<>();

		byte[] ch1 = "ch1".getBytes(StandardCharsets.UTF_8);
		byte[] ch2 = "ch2".getBytes(StandardCharsets.UTF_8);

		when(asyncFactory.getConnection()).thenReturn(asyncConnection);
		when(asyncConnection.getSubscription()).thenReturn(asyncSubscription);
		when(asyncConnection.isSubscribed()).thenReturn(false);

		doAnswer(inv -> {
			SubscriptionListener listener = inv.getArgument(0);
			listenerRef.set(listener);
			byte[][] channels = (byte[][]) inv.getRawArguments()[1];
			subscribedChannels.add(Arrays.asList(channels));
			// First call (ch1) delays ACK so lazyListen times out; second call (ch2) succeeds immediately
			if (channels.length == 1 && Arrays.equals(channels[0], ch2)) {
				listener.onChannelSubscribed(ch2, 1);
			}
			return null;
		}).when(asyncConnection).subscribe(any(), any());

		RedisMessageListenerContainer timeoutContainer = new RedisMessageListenerContainer();
		timeoutContainer.setConnectionFactory(asyncFactory);
		timeoutContainer.setBeanName("timeoutContainer");
		timeoutContainer.setTaskExecutor(new SyncTaskExecutor());
		timeoutContainer.setMaxSubscriptionRegistrationWaitingTime(20);
		timeoutContainer.afterPropertiesSet();
		timeoutContainer.start();

		MessageListener listener1 = mock(MessageListener.class);
		MessageListener listener2 = mock(MessageListener.class);

		assertThatIllegalStateException()
				.isThrownBy(() -> timeoutContainer.addMessageListener(listener1, new ChannelTopic("ch1")));

		Map<ByteArrayWrapper, Collection<MessageListener>> channelMapping = (Map<ByteArrayWrapper, Collection<MessageListener>>) ReflectionTestUtils
				.getField(timeoutContainer, "channelMapping");
		assertThat(channelMapping).isEmpty();
		assertThat(timeoutContainer.isListening()).isFalse();
		verify(asyncSubscription).close();
		verify(asyncConnection).close();

		// Even when zero ACK ever arrived for ch1, subsequent addMessageListener(ch2) must immediately dispatch SUBSCRIBE ch2
		SubscriptionListener staleListener = listenerRef.get();
		timeoutContainer.addMessageListener(listener2, new ChannelTopic("ch2"));

		assertThat(subscribedChannels).hasSize(2);
		assertThat(subscribedChannels.get(1)).containsExactly(ch2);
		assertThat(timeoutContainer.isListening()).isTrue();

		// Late ACK from closed session must clean up orphan if needed without disturbing active ch2
		staleListener.onChannelSubscribed(ch1, 1);
		await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> verify(asyncSubscription).unsubscribe(ch1));
		assertThat(timeoutContainer.isListening()).isTrue();
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
