/*
 * Copyright 2011-2025 the original author or authors.
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
package org.springframework.data.redis.listener.adapter;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.connection.DefaultMessage;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.SerializationException;
import org.springframework.data.redis.serializer.StringRedisSerializer;

/**
 * Unit test for MessageListenerAdapter.
 *
 * @author Costin Leau
 * @author Greg Turnquist
 * @author Thomas Darimont
 * @author Mark Paluch
 */
@ExtendWith(MockitoExtension.class)
class MessageListenerUnitTests {

	private static final StringRedisSerializer serializer = StringRedisSerializer.UTF_8;
	private static final String CHANNEL = "some::test:";
	private static final byte[] RAW_CHANNEL = serializer.serialize(CHANNEL);
	private static final String PAYLOAD = "do re mi";
	private static final byte[] RAW_PAYLOAD = serializer.serialize(PAYLOAD);
	private static final Message STRING_MSG = new DefaultMessage(RAW_CHANNEL, RAW_PAYLOAD);

	private MessageListenerAdapter adapter;

	public static interface Delegate {
		void handleMessage(String argument);

		void customMethod(String arg);

		void customMethodWithChannel(String arg, String channel);
	}

	@Mock private Delegate target;

	@BeforeEach
	void setUp() {
		this.adapter = new MessageListenerAdapter();
	}

	@Test
	void testThatWhenNoDelegateIsSuppliedTheDelegateIsAssumedToBeTheMessageListenerAdapterItself() throws Exception {
		assertThat(adapter.getDelegate()).isSameAs(adapter);
	}

	@Test
	void testThatTheDefaultMessageHandlingMethodNameIsTheConstantDefault() throws Exception {
		assertThat(adapter.getDefaultListenerMethod()).isEqualTo(MessageListenerAdapter.ORIGINAL_DEFAULT_LISTENER_METHOD);
	}

	@Test
	void testAdapterWithListenerAndDefaultMessage() throws Exception {
		MessageListener mock = mock(MessageListener.class);

		MessageListenerAdapter adapter = new MessageListenerAdapter(mock) {
			protected void handleListenerException(Throwable ex) {
				throw new IllegalStateException(ex);
			}
		};

		adapter.onMessage(STRING_MSG, null);
		verify(mock).onMessage(STRING_MSG, null);
	}

	@Test
	void testRawMessage() throws Exception {
		MessageListenerAdapter adapter = new MessageListenerAdapter(target);
		adapter.afterPropertiesSet();
		adapter.onMessage(STRING_MSG, null);

		verify(target).handleMessage(PAYLOAD);
	}

	@Test
	void testCustomMethod() throws Exception {
		MessageListenerAdapter adapter = new MessageListenerAdapter(target);
		adapter.setDefaultListenerMethod("customMethod");
		adapter.afterPropertiesSet();

		adapter.onMessage(STRING_MSG, null);

		verify(target).customMethod(PAYLOAD);
	}

	@Test
	void testCustomMethodWithAlternateConstructor() throws Exception {
		MessageListenerAdapter adapter = new MessageListenerAdapter(target, "customMethod");
		adapter.afterPropertiesSet();

		adapter.onMessage(STRING_MSG, null);

		verify(target).customMethod(PAYLOAD);
	}

	@Test
	void testCustomMethodWithChannel() {
		MessageListenerAdapter adapter = new MessageListenerAdapter(target);
		adapter.setDefaultListenerMethod("customMethodWithChannel");
		adapter.afterPropertiesSet();

		adapter.onMessage(STRING_MSG, RAW_CHANNEL);

		verify(target).customMethodWithChannel(PAYLOAD, CHANNEL);
	}

	@Test
	void testCustomMethodWithChannelAndAlternateConstructor() {
		MessageListenerAdapter adapter = new MessageListenerAdapter(target, "customMethodWithChannel");
		adapter.afterPropertiesSet();

		adapter.onMessage(STRING_MSG, RAW_CHANNEL);

		verify(target).customMethodWithChannel(PAYLOAD, CHANNEL);
	}

	@Test // DATAREDIS-92
	void triggersListenerImplementingInterfaceCorrectly() {

		SampleListener listener = new SampleListener();

		MessageListener listenerAdapter = new MessageListenerAdapter(listener) {
			@Override
			public void setDefaultListenerMethod(String defaultListenerMethod) {
				throw new RuntimeException("Boom");
			}
		};

		listenerAdapter.onMessage(STRING_MSG, RAW_CHANNEL);
		assertThat(listener.count).isEqualTo(1);
	}

	@Test // DATAREDIS-337
	void defaultConcreteHandlerMethodShouldOnlyBeInvokedOnce() {

		ConcreteMessageHandler listener = spy(new ConcreteMessageHandler());

		MessageListenerAdapter adapter = new MessageListenerAdapter(listener);
		adapter.afterPropertiesSet();

		adapter.onMessage(new DefaultMessage("channel1".getBytes(), "body".getBytes()), "".getBytes());

		verify(listener, times(1)).handleMessage(anyString(), anyString());
	}

	@Test // DATAREDIS-337
	void defaultConcreteHandlerMethodWithoutSerializerShouldOnlyBeInvokedOnce() {

		ConcreteMessageHandler listener = spy(new ConcreteMessageHandler());

		MessageListenerAdapter adapter = new MessageListenerAdapter(listener);
		adapter.setSerializer(null);
		adapter.afterPropertiesSet();

		adapter.onMessage(new DefaultMessage("channel1".getBytes(), "body".getBytes()), "".getBytes());

		verify(listener, times(1)).handleMessage(any(byte[].class), anyString());
	}

	@Test // DATAREDIS-337
	void defaultConcreteHandlerMethodWithCustomSerializerShouldOnlyBeInvokedOnce() {

		ConcreteMessageHandler listener = spy(new ConcreteMessageHandler());

		MessageListenerAdapter adapter = new MessageListenerAdapter(listener);
		adapter.setSerializer(new PojoRedisSerializer());
		adapter.afterPropertiesSet();

		adapter.onMessage(new DefaultMessage(new byte[0], "body".getBytes()), "".getBytes());

		verify(listener, times(1)).handleMessage(any(Pojo.class), anyString());
	}

	@Test // DATAREDIS-337
	void customConcreteHandlerMethodShouldOnlyBeInvokedOnce() {

		ConcreteMessageHandler listener = spy(new ConcreteMessageHandler());

		MessageListenerAdapter adapter = new MessageListenerAdapter(listener);
		adapter.setDefaultListenerMethod("handle");
		adapter.afterPropertiesSet();

		adapter.onMessage(new DefaultMessage("channel1".getBytes(), "body".getBytes()), "".getBytes());

		verify(listener, times(1)).handle(anyString(), anyString());
	}

	@Test // DATAREDIS-337
	void customConcreteMessageOnlyHandlerMethodShouldOnlyBeInvokedOnce() {

		ConcreteMessageHandler listener = spy(new ConcreteMessageHandler());

		MessageListenerAdapter adapter = new MessageListenerAdapter(listener);
		adapter.setDefaultListenerMethod("handleMessageOnly");
		adapter.afterPropertiesSet();

		adapter.onMessage(new DefaultMessage("channel1".getBytes(), "body".getBytes()), "".getBytes());

		verify(listener, times(1)).handleMessageOnly(anyString());
	}

	@Test // DATAREDIS-337
	void customConcreteHandlerMethodWithoutSerializerShouldOnlyBeInvokedOnce() {

		ConcreteMessageHandler listener = spy(new ConcreteMessageHandler());

		MessageListenerAdapter adapter = new MessageListenerAdapter(listener);
		adapter.setDefaultListenerMethod("handle");
		adapter.setSerializer(null);
		adapter.afterPropertiesSet();

		adapter.onMessage(new DefaultMessage("channel1".getBytes(), "body".getBytes()), "".getBytes());

		verify(listener, times(1)).handle(any(byte[].class), anyString());
	}

	@Test // DATAREDIS-337
	void customConcreteHandlerMethodWithCustomSerializerShouldOnlyBeInvokedOnce() {

		ConcreteMessageHandler listener = spy(new ConcreteMessageHandler());

		MessageListenerAdapter adapter = new MessageListenerAdapter(listener);
		adapter.setDefaultListenerMethod("handle");
		adapter.setSerializer(new PojoRedisSerializer());
		adapter.afterPropertiesSet();

		adapter.onMessage(new DefaultMessage(new byte[0], "body".getBytes()), "".getBytes());

		verify(listener, times(1)).handle(any(Pojo.class), anyString());
	}

	class SampleListener implements MessageListener {

		int count;

		public void onMessage(Message message, byte[] pattern) {
			count++;
		}
	}

	/**
	 * @author Thomas Darimont
	 */
	static class AbstractMessageHandler {

		public void handleMessage(Pojo message, String channel) {}

		public void handleMessage(byte[] message, String channel) {}

		public void handleMessage(String message, String channel) {}

		public void handle(Pojo message, String channel) {}

		public void handle(String message, String channel) {}

		public void handle(byte[] message, String channel) {}

		public void handleMessageOnly(String message) {}
	}

	/**
	 * @author Thomas Darimont
	 */
	static class ConcreteMessageHandler extends AbstractMessageHandler {

		public void handleMessage(Pojo message, String channel) {}

		public void handleMessage(byte[] message, String channel) {}

		public void handleMessage(String message, String channel) {}

		public void handle(Pojo message, String channel) {}

		public void handle(String message, String channel) {}

		public void handle(byte[] message, String channel) {}

		public void handleMessageOnly(String message) {}
	}

	static class Pojo {}

	static class PojoRedisSerializer implements RedisSerializer<Pojo> {

		@Override
		public byte[] serialize(Pojo value) throws SerializationException {
			return new byte[0];
		}

		@Override
		public Pojo deserialize(byte[] bytes) throws SerializationException {
			return new Pojo();
		}
	}
}
