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

import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.Test;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.config.RedisListenerContainerFactory;
import org.springframework.data.redis.config.RedisListenerEndpoint;
import org.springframework.data.redis.config.RedisListenerEndpointRegistry;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mock;

/**
 * Integration test for {@link EnableRedisListeners} and {@link RedisListener}
 *
 * @author Ilyass Bougati
 */
class RedisListenerAnnotationBeanPostProcessorTests {

	@Test // GH-1004
	void registersListenerWithDefaultFactory() {
		ConfigurableApplicationContext context = new AnnotationConfigApplicationContext(Config.class, SimpleService.class);

		RedisListenerContainerFactory<?> factory = context.getBean("redisListenerContainerFactory",
				RedisListenerContainerFactory.class);
		then(factory).should().createListenerContainer(any(RedisListenerEndpoint.class));

		RedisListenerEndpointRegistry registry = context.getBean(RedisListenerEndpointRegistry.class);
		assertThat(registry.getListenerContainers()).isNotEmpty();

		context.close();
	}

	@Configuration
	@EnableRedisListeners
	static class Config {
		@Bean("redisListenerContainerFactory")
		RedisListenerContainerFactory<@NonNull RedisMessageListenerContainer> redisListenerContainerFactory() {
			RedisListenerContainerFactory<@NonNull RedisMessageListenerContainer> factory = mock(
					RedisListenerContainerFactory.class);
			given(factory.createListenerContainer(any())).willReturn(new RedisMessageListenerContainer());
			return factory;
		}
	}

	static class SimpleService {
		@RedisListener(topics = "test-topic")
		public void handle(String message) {}
	}
}
