/*
 * Copyright 2018-2023 the original author or authors.
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
import java.util.concurrent.Executor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.Subscription;
import org.springframework.data.redis.connection.SubscriptionListener;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.listener.adapter.MessageListenerAdapter;
import org.springframework.data.redis.listener.adapter.RedisListenerExecutionFailedException;

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

	@Test // GH-964
	void failsOnDuplicateInit() {
		assertThatExceptionOfType(IllegalStateException.class).isThrownBy(() -> container.afterPropertiesSet());
	}
}
