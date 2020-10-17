/*
 * Copyright 2015-2020 the original author or authors.
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

import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@RunWith(MockitoJUnitRunner.class)
public class KeyExpirationEventMessageListenerTests {

	RedisMessageListenerContainer container;
	RedisConnectionFactory connectionFactory;
	KeyExpirationEventMessageListener listener;

	@Mock ApplicationEventPublisher publisherMock;

	@Before
	public void setUp() {

		JedisConnectionFactory connectionFactory = new JedisConnectionFactory();
		connectionFactory.afterPropertiesSet();
		this.connectionFactory = connectionFactory;

		container = new RedisMessageListenerContainer();
		container.setConnectionFactory(connectionFactory);
		container.afterPropertiesSet();
		container.start();

		listener = new KeyExpirationEventMessageListener(container);
		listener.setApplicationEventPublisher(publisherMock);
		listener.init();

		try (RedisConnection connection = connectionFactory.getConnection()) {
			connection.flushAll();
		}
	}

	@After
	public void tearDown() throws Exception {

		listener.destroy();
		container.destroy();
		if (connectionFactory instanceof DisposableBean) {
			((DisposableBean) connectionFactory).destroy();
		}
	}

	@Test // DATAREDIS-425
	public void listenerShouldPublishEventCorrectly() {

		byte[] key = ("to-expire:" + UUID.randomUUID().toString()).getBytes();
		AtomicBoolean called = new AtomicBoolean();
		doAnswer(invocation -> {
			called.set(true);
			return null;
		}).when(publisherMock).publishEvent(any(ApplicationEvent.class));

		try (RedisConnection connection = connectionFactory.getConnection()) {

			connection.pSetEx(key, 1, key);
			Awaitility.await().until(() -> !connection.exists(key));
		}

		Awaitility.await().untilTrue(called);

		ArgumentCaptor<ApplicationEvent> captor = ArgumentCaptor.forClass(ApplicationEvent.class);

		verify(publisherMock, times(1)).publishEvent(captor.capture());
		assertThat((byte[]) captor.getValue().getSource()).isEqualTo(key);
	}

	@Test // DATAREDIS-425
	public void listenerShouldNotReactToDeleteEvents() throws InterruptedException {

		byte[] key = ("to-delete:" + UUID.randomUUID().toString()).getBytes();

		try (RedisConnection connection = connectionFactory.getConnection()) {

			connection.setEx(key, 10, "foo".getBytes());
			connection.del(key);
		}

		Thread.sleep(500);
		verifyNoInteractions(publisherMock);
	}
}
