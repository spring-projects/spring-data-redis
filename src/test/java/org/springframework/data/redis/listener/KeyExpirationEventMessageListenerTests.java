/*
 * Copyright 2015-2018 the original author or authors.
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

import java.util.UUID;

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
	}

	@After
	public void tearDown() throws Exception {

		RedisConnection connection = connectionFactory.getConnection();
		try {
			connection.flushAll();
		} finally {
			connection.close();
		}

		listener.destroy();
		container.destroy();
		if (connectionFactory instanceof DisposableBean) {
			((DisposableBean) connectionFactory).destroy();
		}
	}

	@Test // DATAREDIS-425
	public void listenerShouldPublishEventCorrectly() throws InterruptedException {

		byte[] key = ("to-expire:" + UUID.randomUUID().toString()).getBytes();

		RedisConnection connection = connectionFactory.getConnection();
		try {
			connection.setEx(key, 2, "foo".getBytes());

			int iteration = 0;
			while (connection.get(key) != null || iteration >= 3) {

				Thread.sleep(2000);
				iteration++;
			}
		} finally {
			connection.close();
		}

		Thread.sleep(2000);
		ArgumentCaptor<ApplicationEvent> captor = ArgumentCaptor.forClass(ApplicationEvent.class);

		verify(publisherMock, times(1)).publishEvent(captor.capture());
		assertThat((byte[]) captor.getValue().getSource(), is(key));
	}

	@Test // DATAREDIS-425
	public void listenerShouldNotReactToDeleteEvents() throws InterruptedException {

		byte[] key = ("to-delete:" + UUID.randomUUID().toString()).getBytes();

		RedisConnection connection = connectionFactory.getConnection();
		try {

			connection.setEx(key, 10, "foo".getBytes());
			Thread.sleep(2000);
			connection.del(key);
			Thread.sleep(2000);
		} finally {
			connection.close();
		}

		Thread.sleep(2000);
		verifyZeroInteractions(publisherMock);
	}
}
