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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.data.redis.config.MethodRedisListenerEndpoint;
import org.springframework.data.redis.config.RedisListenerEndpointRegistry;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;

/**
 * Unit tests for {@link RedisListenerAnnotationBeanPostProcessor}.
 *
 * @author Ilyass Bougati
 */
@ExtendWith(MockitoExtension.class)
public class RedisListenerAnnotationBeanPostProcessorTests {
	@Mock private RedisListenerEndpointRegistry endpointRegistry;
	@Mock private BeanFactory beanFactory;
	@Mock private RedisMessageListenerContainer container;

	private RedisListenerAnnotationBeanPostProcessor postProcessor;

	@BeforeEach
	void setUp() {
		this.postProcessor = new RedisListenerAnnotationBeanPostProcessor();
		this.postProcessor.setEndpointRegistry(this.endpointRegistry);

		this.postProcessor.setBeanFactory(this.beanFactory);
	}

	@Test
	void postProcess_withRedisListener_registersEndpoint() throws NoSuchMethodException {
		AnnotatedService bean = new AnnotatedService();
		when(this.beanFactory.getBean("redisMessageListenerContainer", RedisMessageListenerContainer.class))
				.thenReturn(this.container);

		Object result = this.postProcessor.postProcessAfterInitialization(bean, "annotatedService");

		assertThat(result).isSameAs(bean);

		ArgumentCaptor<MethodRedisListenerEndpoint> endpointCaptor = ArgumentCaptor
				.forClass(MethodRedisListenerEndpoint.class);

		verify(this.endpointRegistry).registerListenerContainer(endpointCaptor.capture(), eq(this.container));

		MethodRedisListenerEndpoint registeredEndpoint = endpointCaptor.getValue();
		assertThat(registeredEndpoint.getBean()).isEqualTo(bean);
		assertThat(registeredEndpoint.getMethod()).isEqualTo(AnnotatedService.class.getMethod("handle", String.class));
	}

	@Test
	void postProcess_withNoAnnotation_doesNothing() {
		PlainService bean = new PlainService();

		Object result = this.postProcessor.postProcessAfterInitialization(bean, "plainService");

		assertThat(result).isSameAs(bean);
		verifyNoInteractions(this.endpointRegistry);
		verifyNoInteractions(this.beanFactory);
	}

	static class AnnotatedService {
		@RedisListener(channels = "test-channel")
		public void handle(String message) {}
	}

	static class PlainService {
		public void doSomething() {}
	}
}
