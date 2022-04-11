/*
 * Copyright 2018-2022 the original author or authors.
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.Subscription;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.listener.adapter.MessageListenerAdapter;

/**
 * Unit tests for {@link RedisMessageListenerContainer}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
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
		connectionFactoryMock = mock(LettuceConnectionFactory.class);
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
			new Thread(r).start();
			return null;
		}).when(executorMock).execute(any());

		doAnswer(it -> {

			when(connectionMock.isSubscribed()).thenReturn(true);
			return null;
		}).when(connectionMock).subscribe(any(), any());

		container.addMessageListener(adapter, new ChannelTopic("a"));
		container.start();

		when(connectionMock.getSubscription()).thenReturn(subscriptionMock);

		container.stop();

		assertThat(container.isRunning()).isFalse();
		verify(subscriptionMock).close();
	}

	@Test // GH-964
	void testConnectionErrorOnPatternSubscribedIsRetried() throws InterruptedException {
		container.setRecoveryInterval(1);
		when(connectionFactoryMock.getConnection()).thenReturn(connectionMock);
		when(connectionMock.getSubscription()).thenReturn(subscriptionMock);
		doThrow(new RedisConnectionFailureException("")).doNothing().when(subscriptionMock).pSubscribe(any());

		List<Thread> threads = new ArrayList<>();

		doAnswer(it -> {

			Runnable r = it.getArgument(0);
			Thread thread = new Thread(r);
			threads.add(thread);
			thread.start();
			return null;
		}).when(executorMock).execute(any());

		doAnswer(it -> {

			when(connectionMock.isSubscribed()).thenReturn(true);
			return null;
		}).when(connectionMock).subscribe(any(), any());

		container.addMessageListener(adapter, new ChannelTopic("a"));
		container.addMessageListener(adapter, new PatternTopic("foo.pattern.*"));
		container.start();

		threads.forEach(thread -> {
			try {
				thread.join(5000);
			} catch (InterruptedException e) {}
		});

		container.stop();

		assertThat(container.isRunning()).isFalse();
		verify(subscriptionMock).close();
		verify(subscriptionMock, times(2)).pSubscribe(any());
		verify(connectionMock).subscribe(any(), any());
	}

	@Test // GH-964
	void testUnexpectedErrorOnPatternSubscribedIsNotRetried() throws InterruptedException {
		when(connectionFactoryMock.getConnection()).thenReturn(connectionMock);
		when(connectionMock.getSubscription()).thenReturn(subscriptionMock);
		doThrow(new RuntimeException()).when(subscriptionMock).pSubscribe(any());

		List<Thread> threads = new ArrayList<>();

		doAnswer(it -> {

			Runnable r = it.getArgument(0);
			Thread thread = new Thread(r);
			threads.add(thread);
			thread.start();
			return null;
		}).when(executorMock).execute(any());

		doAnswer(it -> {

			when(connectionMock.isSubscribed()).thenReturn(true);
			return null;
		}).when(connectionMock).subscribe(any(), any());

		container.addMessageListener(adapter, new ChannelTopic("a"));
		container.addMessageListener(adapter, new PatternTopic("foo.pattern.*"));
		container.start();

		threads.forEach(thread -> {
			try {
				thread.join(5000);
			} catch (InterruptedException e) {}
		});

		container.stop();

		assertThat(container.isRunning()).isFalse();
		verify(subscriptionMock).close();
		verify(subscriptionMock).pSubscribe(any());
		verify(connectionMock).subscribe(any(), any());
	}

	@Test // GH-964
	void testConnectionErrorOnPatternSubscribedIsNotRetriedWhenThreadIsCancelled() throws InterruptedException {
		container.setRecoveryInterval(10);
		when(connectionFactoryMock.getConnection()).thenReturn(connectionMock);
		when(connectionMock.getSubscription()).thenReturn(subscriptionMock);
		doThrow(new RedisConnectionFailureException("")).when(subscriptionMock).pSubscribe(any());

		List<Thread> threads = new ArrayList<>();

		doAnswer(it -> {

			Runnable r = it.getArgument(0);
			Thread thread = new Thread(r);
			threads.add(thread);
			thread.start();
			return null;
		}).when(executorMock).execute(any());

		doAnswer(it -> {

			when(connectionMock.isSubscribed()).thenReturn(true);
			return null;
		}).when(connectionMock).subscribe(any(), any());

		container.addMessageListener(adapter, new ChannelTopic("a"));
		container.addMessageListener(adapter, new PatternTopic("foo.pattern.*"));
		container.start();

		threads.forEach(thread -> {
			try {
				thread.join(1000);
			} catch (InterruptedException e) {}
		});

		container.stop();

		threads.forEach(thread -> {
			try {
				thread.join(5000);
			} catch (InterruptedException e) {}
		});

		assertThat(container.isRunning()).isFalse();
		verify(subscriptionMock).close();
		verify(subscriptionMock, atLeastOnce()).pSubscribe(any());
		verify(connectionMock).subscribe(any(), any());
		assertThat(threads).allMatch(thread -> !thread.isAlive());
	}

	@Test // GH-964
	void testConnectionErrorOnMainSubcriptionThreadCancelsPatternSubscriptionThread() throws InterruptedException {
		Map<Thread, Boolean> threads = new HashMap<>();
		container.setRecoveryInterval(10);
		when(connectionFactoryMock.getConnection()).thenReturn(connectionMock);
		when(connectionMock.getSubscription()).thenReturn(subscriptionMock);

		doAnswer(it -> {

			if (threads.get(Thread.currentThread())) {
				throw new RedisConnectionFailureException("");
			}
			when(connectionMock.isSubscribed()).thenReturn(true);
			return null;
		}).when(connectionMock).subscribe(any(), any());

		doAnswer(it -> {

			Runnable r = it.getArgument(0);
			Thread thread = new Thread(r);
			threads.put(thread, threads.size() >= 2 ? Boolean.FALSE : Boolean.TRUE);
			thread.start();
			return null;
		}).when(executorMock).execute(any());

		container.addMessageListener(adapter, new ChannelTopic("a"));
		container.addMessageListener(adapter, new PatternTopic("foo.pattern.*"));
		container.start();

		threads.keySet().forEach(thread -> {
			try {
				thread.join(1000);
			} catch (InterruptedException e) {}
		});

		container.stop();

		threads.keySet().forEach(thread -> {
			try {
				thread.join(5000);
			} catch (InterruptedException e) {}
		});

		assertThat(container.isRunning()).isFalse();
		verify(subscriptionMock).close();
		verify(subscriptionMock, atLeastOnce()).pSubscribe(any());
		verify(connectionMock, times(2)).subscribe(any(), any());
		assertThat(threads.keySet()).allMatch(thread -> !thread.isAlive());
	}
}
