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
import static org.hamcrest.core.IsInstanceOf.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.data.redis.connection.DefaultMessage;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.core.RedisKeyExpiredEvent;

/**
 * @author Christoph Strobl
 */
@RunWith(MockitoJUnitRunner.class)
public class KeyExpirationEventMessageListenerUnitTests {

	private static final String MESSAGE_CHANNEL = "channel";
	private static final String MESSAGE_BODY = "body";
	private static final Message MESSAGE = new DefaultMessage(MESSAGE_CHANNEL.getBytes(), MESSAGE_BODY.getBytes());

	@Mock RedisMessageListenerContainer containerMock;
	@Mock ApplicationEventPublisher publisherMock;
	KeyExpirationEventMessageListener listener;

	@Before
	public void setUp() {

		listener = new KeyExpirationEventMessageListener(containerMock);
		listener.setApplicationEventPublisher(publisherMock);
	}

	@Test // DATAREDIS-425
	public void handleMessageShouldPublishKeyExpiredEvent() {

		listener.onMessage(MESSAGE, "*".getBytes());

		ArgumentCaptor<ApplicationEvent> captor = ArgumentCaptor.forClass(ApplicationEvent.class);

		verify(publisherMock, times(1)).publishEvent(captor.capture());
		assertThat(captor.getValue(), instanceOf(RedisKeyExpiredEvent.class));
		assertThat((byte[]) captor.getValue().getSource(), is(MESSAGE_BODY.getBytes()));
	}

	@Test // DATAREDIS-425
	public void handleMessageShouldNotRespondToNullMessage() {

		listener.onMessage(null, "*".getBytes());

		verifyZeroInteractions(publisherMock);
	}

	@Test // DATAREDIS-425, DATAREDIS-692
	public void handleMessageShouldNotRespondToEmptyMessage() {

		listener.onMessage(new DefaultMessage(new byte[] {}, new byte[] {}), "*".getBytes());

		verifyZeroInteractions(publisherMock);
	}
}
