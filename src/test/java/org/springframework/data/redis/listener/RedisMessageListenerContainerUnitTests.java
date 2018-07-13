/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.redis.listener;

import static org.hamcrest.core.Is.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.util.concurrent.Executor;

import org.junit.Before;
import org.junit.Test;
import org.springframework.core.task.SyncTaskExecutor;
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
public class RedisMessageListenerContainerUnitTests {

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

	@Before
	public void setUp() {

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
	public void containerShouldStopGracefullyOnUnsubscribeErrors() {

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

		assertThat(container.isRunning(), is(false));
		verify(subscriptionMock).close();
	}
}
