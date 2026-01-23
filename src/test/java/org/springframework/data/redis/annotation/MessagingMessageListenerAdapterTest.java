/*
 * Copyright 2026-present the original author or authors.
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
package org.springframework.data.redis.annotation;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.listener.adapter.MessagingMessageListenerAdapter;
import org.springframework.messaging.converter.StringMessageConverter;
import org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory;
import org.springframework.messaging.handler.invocation.InvocableHandlerMethod;

/**
 * Unit tests for {@link MessagingMessageListenerAdapter}
 *
 * @author Ilyass Bougati
 */
@ExtendWith(MockitoExtension.class)
public class MessagingMessageListenerAdapterTest {
	@Mock private Message message;

	@Test
	void shouldConvertBytesToStringAndInvokeMethod() throws NoSuchMethodException {
		DefaultMessageHandlerMethodFactory factory = new DefaultMessageHandlerMethodFactory();
		StringMessageConverter stringConverter = new StringMessageConverter();
		factory.setMessageConverter(stringConverter);

		factory.afterPropertiesSet();

		TestDelegate delegate = new TestDelegate();
		Method method = TestDelegate.class.getMethod("handleString", String.class);

		InvocableHandlerMethod invocableMethod = factory.createInvocableHandlerMethod(delegate, method);
		MessagingMessageListenerAdapter adapter = new MessagingMessageListenerAdapter(invocableMethod);

		byte[] payload = "Hello World".getBytes(StandardCharsets.UTF_8);
		when(message.getBody()).thenReturn(payload);

		adapter.onMessage(message, null);

		assertThat(delegate.capturedPayload).isEqualTo("Hello World");
	}

	@Test // GH-1004
	void shouldPassRawBytes_WhenArgumentIsByteArray() throws NoSuchMethodException {
		DefaultMessageHandlerMethodFactory factory = new DefaultMessageHandlerMethodFactory();
		factory.afterPropertiesSet();

		TestDelegate delegate = new TestDelegate();
		Method method = TestDelegate.class.getMethod("handleBytes", byte[].class);

		InvocableHandlerMethod invocableMethod = factory.createInvocableHandlerMethod(delegate, method);
		MessagingMessageListenerAdapter adapter = new MessagingMessageListenerAdapter(invocableMethod);

		byte[] payload = { 1, 2, 3 };
		when(message.getBody()).thenReturn(payload);

		adapter.onMessage(message, null);

		assertThat(delegate.capturedBytes).isEqualTo(payload);
	}

	static class TestDelegate {
		String capturedPayload;
		byte[] capturedBytes;

		public void handleString(String payload) {
			this.capturedPayload = payload;
		}

		public void handleBytes(byte[] payload) {
			this.capturedBytes = payload;
		}
	}
}
