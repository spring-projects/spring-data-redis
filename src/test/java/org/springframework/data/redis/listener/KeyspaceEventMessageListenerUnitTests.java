/*
 * Copyright 2017-2023 the original author or authors.
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
 *  limitations under the License.
 */
package org.springframework.data.redis.listener;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.withSettings;

import java.util.Properties;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.quality.Strictness;

import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisServerCommands;
import org.springframework.lang.Nullable;

/**
 * Unit tests for {@link KeyspaceEventMessageListener}.
 *
 * @author John Blum
 */
@ExtendWith(MockitoExtension.class)
class KeyspaceEventMessageListenerUnitTests {

	@Mock
	private RedisMessageListenerContainer mockMessageListenerContainer;

	private Message mockMessage(@Nullable String channel, @Nullable String body) {

		Message mockMessage = mock(Message.class, withSettings().strictness(Strictness.LENIENT));

		doReturn(toBytes(body)).when(mockMessage).getBody();
		doReturn(toBytes(channel)).when(mockMessage).getChannel();

		return mockMessage;
	}

	@Nullable
	private byte[] toBytes(@Nullable String value) {
		return value != null ? value.getBytes() : null;
	}

	private KeyspaceEventMessageListener newKeyspaceEventMessageListener() {
		return newKeyspaceEventMessageListener(listener -> { });
	}

	private KeyspaceEventMessageListener newKeyspaceEventMessageListener(
			Consumer<KeyspaceEventMessageListener> preConditions) {

		TestKeyspaceEventMessageListener listener =
				new TestKeyspaceEventMessageListener(this.mockMessageListenerContainer);

		preConditions.accept(listener);

		return spy(listener);
	}

	@SuppressWarnings("all")
	private Properties singletonProperties(String propertyName, String propertyValue) {
		Properties properties = new Properties();
		properties.setProperty(propertyName, propertyValue);
		return properties;
	}

	@Test // GH-2670
	void handlesMessageWithChannelAndBody() {

		Message mockMessage = mockMessage("TestChannel", "TestBody");

		KeyspaceEventMessageListener listener = newKeyspaceEventMessageListener();

		listener.onMessage(mockMessage, null);

		verify(listener, times(1)).onMessage(eq(mockMessage), isNull());
		verify(mockMessage, times(1)).getChannel();
		verify(mockMessage, times(1)).getBody();
		verify(listener, times(1)).doHandleMessage(eq(mockMessage));
		verifyNoMoreInteractions(mockMessage, listener);
	}

	@Test // GH-2670
	public void ignoreMessageWithNoBody() {

		Message mockMessage = mockMessage("TestChannel", null);

		KeyspaceEventMessageListener listener = newKeyspaceEventMessageListener();

		listener.onMessage(mockMessage, null);

		verify(listener, times(1)).onMessage(eq(mockMessage), isNull());
		verify(mockMessage, times(1)).getChannel();
		verify(mockMessage, times(1)).getBody();
		verify(listener, never()).doHandleMessage(any());
		verifyNoMoreInteractions(mockMessage, listener);
	}

	@Test // GH-2670
	public void ignoreMessageWithNoChannel() {

		Message mockMessage = mockMessage(null, "TestBody");

		KeyspaceEventMessageListener listener = newKeyspaceEventMessageListener();

		listener.onMessage(mockMessage, null);

		verify(listener, times(1)).onMessage(eq(mockMessage), isNull());
		verify(mockMessage, times(1)).getChannel();
		verify(listener, never()).doHandleMessage(any());
		verifyNoMoreInteractions(mockMessage, listener);
	}

	@Test // GH-2670
	public void doNotConfigureKeyspaceEventNotificationsWhenConfigParameterNotSpecified() {

		KeyspaceEventMessageListener listener = newKeyspaceEventMessageListener(it ->
			assertThat(it.getKeyspaceNotificationsConfigParameter()).isEmpty());

		listener.init();

		verify(listener, never()).configureKeyspaceEventNotifications(any());
	}

	@Test // GH-2670
	public void doNotConfigureKeyspaceEventNotificationsWhenContainerHasNoConnectionFactory() {

		KeyspaceEventMessageListener listener = newKeyspaceEventMessageListener();

		listener.setKeyspaceNotificationsConfigParameter("EA");
		listener.init();

		assertThat(listener.getKeyspaceNotificationsConfigParameter()).isEqualTo("EA");

		verify(listener, times(1)).configureKeyspaceEventNotifications(any());
		verify(listener, never()).setKeyspaceEventNotifications(any(), any());
	}

	@Test // GH-2670
	public void doNotConfigureKeyspaceEventNotificationsWhenRedisServerSettingIsAlreadySet() {

		RedisConnectionFactory mockConnectionFactory = mock(RedisConnectionFactory.class);

		RedisConnection mockConnection = mock(RedisConnection.class);

		RedisServerCommands mockServerCommands = mock(RedisServerCommands.class);

		Properties config = singletonProperties(KeyspaceEventMessageListener.NOTIFY_KEYSPACE_EVENTS, "Em");

		doReturn(mockConnectionFactory).when(this.mockMessageListenerContainer).getConnectionFactory();
		doReturn(mockConnection).when(mockConnectionFactory).getConnection();
		doReturn(mockServerCommands).when(mockConnection).serverCommands();
		doReturn(config).when(mockServerCommands).getConfig(any());

		KeyspaceEventMessageListener listener = newKeyspaceEventMessageListener();

		listener.setKeyspaceNotificationsConfigParameter("EA");
		listener.init();

		assertThat(listener.getKeyspaceNotificationsConfigParameter()).isEqualTo("EA");

		verify(listener, times(1)).configureKeyspaceEventNotifications(any());
		verify(mockServerCommands, times(1)).getConfig(eq(KeyspaceEventMessageListener.NOTIFY_KEYSPACE_EVENTS));
		verify(listener, never()).setKeyspaceEventNotifications(any(), any());
		verify(mockConnection, times(1)).close();
	}

	@Test // GH-2670
	public void configuresKeyspaceEventNotificationsWhenRedisServerHasNoSettings() {

		RedisConnectionFactory mockConnectionFactory = mock(RedisConnectionFactory.class);

		RedisConnection mockConnection = mock(RedisConnection.class);

		RedisServerCommands mockServerCommands = mock(RedisServerCommands.class);

		doReturn(mockConnectionFactory).when(this.mockMessageListenerContainer).getConnectionFactory();
		doReturn(mockConnection).when(mockConnectionFactory).getConnection();
		doReturn(mockServerCommands).when(mockConnection).serverCommands();
		doReturn(null).when(mockServerCommands).getConfig(any());

		KeyspaceEventMessageListener listener = newKeyspaceEventMessageListener();

		listener.setKeyspaceNotificationsConfigParameter("EA");
		listener.init();

		assertThat(listener.getKeyspaceNotificationsConfigParameter()).isEqualTo("EA");

		verify(listener, times(1)).configureKeyspaceEventNotifications(any());
		verify(mockServerCommands, times(1))
				.getConfig(eq(KeyspaceEventMessageListener.NOTIFY_KEYSPACE_EVENTS));
		verify(listener, times(1))
				.setKeyspaceEventNotifications(eq(mockConnection), eq("EA"));
		verify(mockServerCommands, times(1))
				.setConfig(eq(KeyspaceEventMessageListener.NOTIFY_KEYSPACE_EVENTS), eq("EA"));
		verify(mockConnection, times(1)).close();
	}

	@Test // GH-2670
	public void configuresKeyspaceEventNotificationsCorrectly() {

		RedisConnectionFactory mockConnectionFactory = mock(RedisConnectionFactory.class);

		RedisConnection mockConnection = mock(RedisConnection.class);

		RedisServerCommands mockServerCommands = mock(RedisServerCommands.class);

		Properties config = singletonProperties(KeyspaceEventMessageListener.NOTIFY_KEYSPACE_EVENTS, "  ");

		doReturn(mockConnectionFactory).when(this.mockMessageListenerContainer).getConnectionFactory();
		doReturn(mockConnection).when(mockConnectionFactory).getConnection();
		doReturn(mockServerCommands).when(mockConnection).serverCommands();
		doReturn(config).when(mockServerCommands).getConfig(any());

		KeyspaceEventMessageListener listener = newKeyspaceEventMessageListener();

		listener.setKeyspaceNotificationsConfigParameter("EA");
		listener.init();

		assertThat(listener.getKeyspaceNotificationsConfigParameter()).isEqualTo("EA");

		verify(listener, times(1)).configureKeyspaceEventNotifications(any());
		verify(mockServerCommands, times(1))
				.getConfig(eq(KeyspaceEventMessageListener.NOTIFY_KEYSPACE_EVENTS));
		verify(listener, times(1))
				.setKeyspaceEventNotifications(eq(mockConnection), eq("EA"));
		verify(mockServerCommands, times(1))
				.setConfig(eq(KeyspaceEventMessageListener.NOTIFY_KEYSPACE_EVENTS), eq("EA"));
		verify(mockConnection, times(1)).close();
	}

	static class TestKeyspaceEventMessageListener extends KeyspaceEventMessageListener {

		TestKeyspaceEventMessageListener(RedisMessageListenerContainer messageListenerContainer) {
			super(messageListenerContainer);
		}

		@Override
		protected void doHandleMessage(Message message) {

		}
	}
}
