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
package org.springframework.data.redis.config;

import static org.mockito.Mockito.*;

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.data.redis.annotation.RedisListener;
import org.springframework.data.redis.annotation.RedisListenerAnnotationBeanPostProcessor;
import org.springframework.data.redis.connection.DefaultMessage;
import org.springframework.data.redis.listener.adapter.HandlerMethodMessageListenerAdapter;
import org.springframework.messaging.handler.annotation.Payload;

/**
 * Unit tests for {@link RedisListenerEndpointRegistrar} applying various conversions.
 *
 * @author Ilyass Bougati
 * @author Mark Paluch
 */
@ExtendWith(MockitoExtension.class)
class RedisListenerEndpointConversionUnitTests {

	@Mock RedisListenerEndpointRegistry endpointRegistry;
	@Mock BeanFactory beanFactory;

	private RedisListenerAnnotationBeanPostProcessor processor;

	@BeforeEach
	void setUp() {

		processor = new RedisListenerAnnotationBeanPostProcessor();
		processor.setEndpointRegistry(endpointRegistry);
		processor.afterSingletonsInstantiated();
		processor.setBeanFactory(beanFactory);
	}

	@Test // GH-1004
	void shouldInjectConvertedPayload() throws NoSuchMethodException {

		AnnotatedService bean = mock(AnnotatedService.class);
		Method method = AnnotatedService.class.getMethod("handle", Person.class, String.class, byte[].class);

		MethodRedisListenerEndpoint endpoint = processor.createEndpoint(method.getAnnotation(RedisListener.class), method,
				bean);

		HandlerMethodMessageListenerAdapter listener = endpoint.createListener();

		String json = "{\"firstname\":\"Walter\",\"lastname\":\"White\"}";
		listener.onMessage(new StringMessage("test-channel", json), null);

		verify(bean).handle(new Person("Walter", "White"), json, json.getBytes(StandardCharsets.UTF_8));
	}

	static class AnnotatedService {

		@RedisListener(topic = "test-channel")
		public void handle(@Payload Person person, @Payload String body, @Payload byte[] content) {}

	}

	record Person(String firstname, String lastname) {

	}

	static class StringMessage extends DefaultMessage {

		public StringMessage(String channel, String body) {
			super(channel.getBytes(StandardCharsets.UTF_8), body.getBytes(StandardCharsets.UTF_8));
		}
	}
}
