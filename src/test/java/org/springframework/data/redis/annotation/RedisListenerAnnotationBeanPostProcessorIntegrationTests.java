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

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.config.RedisListenerConfigUtils;
import org.springframework.data.redis.config.RedisListenerEndpointRegistry;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.listener.Topic;

/**
 * Integration test for {@link EnableRedisListeners} and {@link RedisListener}
 *
 * @author Ilyass Bougati
 * @author Mark Paluch
 */
class RedisListenerAnnotationBeanPostProcessorIntegrationTests {

	@Test // GH-1004
	void registersListenerWithDefaultContainer() {

		AtomicReference<RedisListenerEndpointRegistry> registryRef = new AtomicReference<>();

		doWithContext(context -> {
			RedisMessageListenerContainer container = context.getBean("redisMessageListenerContainer",
					RedisMessageListenerContainer.class);

			verify(container).addMessageListener(any(), any(Topic.class));

			RedisListenerEndpointRegistry registry = context.getBean(RedisListenerEndpointRegistry.class);
			assertThat(registry.isRunning()).isTrue();
			registryRef.set(registry);
		}, DefaultConfig.class, SimpleService.class);

		assertThat(registryRef.get().isRunning()).isFalse();
	}

	@Test // GH-3340
	void registersListenerWithNamedContainer() {

		doWithContext(context -> {
			RedisMessageListenerContainer customContainer = context.getBean("customContainer1",
					RedisMessageListenerContainer.class);
			RedisMessageListenerContainer defaultContainer = context
					.getBean(RedisListenerConfigUtils.REDIS_MESSAGE_LISTENER_BEAN_NAME, RedisMessageListenerContainer.class);

			verify(customContainer).addMessageListener(any(), any(Topic.class));
			verify(defaultContainer, never()).addMessageListener(any(), any(Topic.class));
		}, DefaultConfig.class, MultiContainerService.class, CustomContainerConfig.class);
	}

	@Test // GH-3340
	void registersListenersAcrossMultipleContainers() {

		doWithContext(context -> {
			RedisMessageListenerContainer containerOne = context.getBean("customContainer1",
					RedisMessageListenerContainer.class);
			RedisMessageListenerContainer containerTwo = context.getBean("customContainer2",
					RedisMessageListenerContainer.class);

			verify(containerOne).addMessageListener(any(), any(Topic.class));
			verify(containerTwo).addMessageListener(any(), any(Topic.class));
		}, CustomContainerConfig.class, MultiContainerService.class);
	}

	@Test // GH-3340
	void failsWithMissingNamedContainer() {

		assertThatThrownBy(() -> new AnnotationConfigApplicationContext(DefaultConfig.class, NamedContainerService.class))
				.hasRootCauseInstanceOf(NoSuchBeanDefinitionException.class).hasMessageContaining("customContainer");
	}

	@Test // GH-3340
	void registersListenersMultipleContainers() {

		doWithContext(context -> {
			RedisMessageListenerContainer container = context
					.getBean(RedisListenerConfigUtils.REDIS_MESSAGE_LISTENER_BEAN_NAME, RedisMessageListenerContainer.class);

			verify(container).addMessageListener(any(), any(Topic.class));
		}, DefaultConfig.class, CustomContainerConfig.class, UnnamedContainerService.class);
	}

	@Test // GH-3340
	void registrationFailsOnUnresolvableContainer() {

		assertThatExceptionOfType(BeanCreationException.class)
				.isThrownBy(() -> doWithContext(context -> {}, CustomContainerConfig.class, UnnamedContainerService.class));
	}

	private static void doWithContext(Consumer<ApplicationContext> action, Class<?>... annotatedClasses) {
		try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
			context.register(annotatedClasses);
			context.refresh();
			action.accept(context);
		}
	}

	@Configuration
	@EnableRedisListeners
	static class DefaultConfig {

		@Bean
		public RedisMessageListenerContainer redisMessageListenerContainer() {
			return mock(RedisMessageListenerContainer.class);
		}
	}

	static class SimpleService {

		@RedisListener(topic = "test-topic")
		public void handle(String msg) {}

	}

	static class UnnamedContainerService {

		@RedisListener(topic = "test-topic", container = "")
		public void handle(String msg) {}

	}

	static class NamedContainerService {

		@RedisListener(container = "customContainer", topic = "test-topic")
		public void handle(String msg) {}

	}

	@Configuration
	@EnableRedisListeners
	static class CustomContainerConfig {

		@Bean
		public RedisMessageListenerContainer customContainer1() {
			return mock(RedisMessageListenerContainer.class);
		}

		@Bean
		public RedisMessageListenerContainer customContainer2() {
			return mock(RedisMessageListenerContainer.class);
		}

	}

	static class MultiContainerService {

		@RedisListener(container = "customContainer1", topic = "topic-one")
		@RedisListener(container = "customContainer2", topic = "topic-two")
		public void handle(String msg) {}

	}

}
