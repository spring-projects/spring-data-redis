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

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.data.redis.config.MethodRedisListenerEndpoint;
import org.springframework.data.redis.config.RedisListenerEndpointRegistry;
import org.springframework.data.redis.connection.DefaultMessage;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.listener.Topic;
import org.springframework.data.redis.listener.adapter.HandlerMethodMessageListenerAdapter;
import org.springframework.data.redis.listener.support.PubSubHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Headers;

/**
 * Unit tests for {@link RedisListenerAnnotationBeanPostProcessor}.
 *
 * @author Ilyass Bougati
 * @author Mark Paluch
 */
@MockitoSettings(strictness = Strictness.LENIENT)
class RedisListenerAnnotationBeanPostProcessorUnitTests {

	@Mock RedisListenerEndpointRegistry endpointRegistry;
	@Mock BeanFactory beanFactory;
	@Mock RedisMessageListenerContainer container;

	private RedisListenerAnnotationBeanPostProcessor processor;

	@BeforeEach
	void setUp() {

		processor = new RedisListenerAnnotationBeanPostProcessor();
		processor.setEndpointRegistry(endpointRegistry);
		processor.afterSingletonsInstantiated();
		processor.setBeanFactory(beanFactory);

		when(beanFactory.getBean(RedisMessageListenerContainer.class)).thenReturn(container);
	}

	@Test // GH-1004
	void shouldRegisterEndpoint() throws NoSuchMethodException {

		AnnotatedService bean = new AnnotatedService();

		Object result = processor.postProcessAfterInitialization(bean, "annotatedService");

		processor.afterSingletonsInstantiated();

		ArgumentCaptor<MethodRedisListenerEndpoint> endpointCaptor = ArgumentCaptor
				.forClass(MethodRedisListenerEndpoint.class);

		assertThat(result).isSameAs(bean);

		verify(endpointRegistry).registerListener(endpointCaptor.capture(), eq(container));

		MethodRedisListenerEndpoint registeredEndpoint = endpointCaptor.getValue();
		assertThat(registeredEndpoint.getBean()).isEqualTo(bean);
		assertThat(registeredEndpoint.getMethod()).isEqualTo(AnnotatedService.class.getMethod("handle", String.class));
	}

	@Test // GH-1004
	void shouldNotRegisterWithoutAnnotation() {

		PlainService bean = new PlainService();

		Object result = processor.postProcessAfterInitialization(bean, "plainService");

		assertThat(result).isSameAs(bean);
		verifyNoInteractions(endpointRegistry);
		verifyNoInteractions(beanFactory);
	}

	@Test // GH-1004
	void shouldInjectPayload() throws NoSuchMethodException {

		WithArgumentResolution bean = mock(WithArgumentResolution.class);
		Method method = WithArgumentResolution.class.getMethod("handle", String.class, Topic.class);

		MethodRedisListenerEndpoint endpoint = processor.createEndpoint(method.getAnnotation(RedisListener.class),
				method, bean);

		HandlerMethodMessageListenerAdapter listener = endpoint.createListener();

		listener.onMessage(new StringMessage("test-channel", "hello"), null);

		verify(bean).handle("hello", ChannelTopic.of("test-channel"));
	}


	@Test // GH-1004
	void shouldInjectConvertedPayload() throws NoSuchMethodException {

		WithArgumentResolution bean = mock(WithArgumentResolution.class);
		Method method = WithArgumentResolution.class.getMethod("handle", String.class, String.class);

		MethodRedisListenerEndpoint endpoint = processor.createEndpoint(method.getAnnotation(RedisListener.class),
				method, bean);

		HandlerMethodMessageListenerAdapter listener = endpoint.createListener();

		listener.onMessage(new StringMessage("test-channel", "hello"), null);

		verify(bean).handle("hello", "test-channel");
	}

	@Test // GH-1004
	void shouldInjectHeaders() throws NoSuchMethodException {

		WithArgumentResolution bean = mock(WithArgumentResolution.class);
		Method method = WithArgumentResolution.class.getMethod("handleHeaders", String.class, Map.class);

		MethodRedisListenerEndpoint endpoint = processor.createEndpoint(method.getAnnotation(RedisListener.class),
				method, bean);

		HandlerMethodMessageListenerAdapter listener = endpoint.createListener();

		listener.onMessage(new StringMessage("test-channel", "hello"), null);

		ArgumentCaptor<Map<String, Object>> headersCaptor = ArgumentCaptor.forClass(Map.class);

		verify(bean).handleHeaders(eq("hello"), headersCaptor.capture());
		Map<String, Object> headers = headersCaptor.getValue();

		assertThat(headers).containsEntry(PubSubHeaders.TOPIC, ChannelTopic.of("test-channel"))
				.containsEntry(PubSubHeaders.CHANNEL, ChannelTopic.of("test-channel")) //
				.doesNotContainKey(PubSubHeaders.PATTERN);
	}

	static class AnnotatedService {

		@RedisListener(topic = "test-channel")
		public void handle(String message) {}

	}

	static class WithArgumentResolution {

		@RedisListener(topic = "test-channel")
		public void handle(String message, @Header Topic topic) {}

		@RedisListener(topic = "test-channel")
		public void handle(String message, @Header String topic) {}

		@RedisListener(topic = "test-channel")
		public void handleHeaders(String message, @Headers Map<String, Object> headers) {}

	}

	static class PlainService {

		public void doSomething() {}

	}

	static class StringMessage extends DefaultMessage {

		public StringMessage(String channel, String body) {
			super(channel.getBytes(StandardCharsets.UTF_8), body.getBytes(StandardCharsets.UTF_8));
		}
	}
}
