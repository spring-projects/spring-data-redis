/*
 * Copyright 2016-2023 the original author or authors.
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

import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.listener.adapter.MessageListenerAdapter;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Integration tests for {@link RedisMessageListenerContainer}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 */
class RedisMessageListenerContainerFailureIntegrationTests {

	private final Object handler = new Object() {

		@SuppressWarnings("unused")
		public void handleMessage(Object message) {}
	};

	private final MessageListenerAdapter adapter = new MessageListenerAdapter(handler);

	private JedisConnectionFactory connectionFactory;
	private RedisMessageListenerContainer container;

	private Executor executorMock;

	@BeforeEach
	void setUp() {

		executorMock = mock(Executor.class);

		RedisStandaloneConfiguration configuration = new RedisStandaloneConfiguration();
		configuration.setPort(SettingsUtils.getPort());
		configuration.setHostName(SettingsUtils.getHost());
		configuration.setDatabase(2);

		connectionFactory = new JedisConnectionFactory(configuration);
		connectionFactory.afterPropertiesSet();

		container = new RedisMessageListenerContainer();
		container.setConnectionFactory(connectionFactory);
		container.setBeanName("container");
		container.setTaskExecutor(new SyncTaskExecutor());
		container.setSubscriptionExecutor(executorMock);
		container.afterPropertiesSet();
	}

	@AfterEach
	void tearDown() throws Exception {

		container.destroy();
		connectionFactory.destroy();
	}

	@Test // DATAREDIS-415, GH-964
	void interruptAtStart() {

		Thread main = Thread.currentThread();

		// interrupt thread once Executor.execute is called
		doAnswer(invocationOnMock -> {

			main.interrupt();
			throw new InterruptedException();
		}).when(executorMock).execute(any(Runnable.class));

		container.addMessageListener(adapter, new ChannelTopic("a"));
		assertThatThrownBy(() -> container.start()).isInstanceOf(CompletionException.class)
				.hasRootCauseInstanceOf(InterruptedException.class);

		// reset the interrupted flag to not destroy the teardown
		Thread.interrupted();

		assertThat(container.isRunning()).isTrue();
		assertThat(container.isListening()).isFalse();
	}

	@Test // GH-964
	void connectionFailureAndRetry() {

		// interrupt thread once Executor.execute is called
		doAnswer(invocationOnMock -> {

			throw new RedisConnectionFailureException("I want to break free");
		}).when(executorMock).execute(any(Runnable.class));

		container.setRecoveryBackoff(new FixedBackOff(1, 5));
		container.addMessageListener(adapter, new ChannelTopic("a"));
		assertThatThrownBy(() -> container.start()).isInstanceOf(CompletionException.class)
				.hasRootCauseInstanceOf(RedisConnectionFailureException.class);

		assertThat(container.isRunning()).isTrue();
		assertThat(container.isListening()).isFalse();
	}
}
