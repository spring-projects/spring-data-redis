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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.junit.jupiter.api.Test;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.config.RedisListenerEndpointRegistry;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;

/**
 * Integration test for {@link EnableRedisListeners} and {@link RedisListener}
 *
 * @author Ilyass Bougati
 */
class RedisListenerAnnotationBeanPostProcessorIntegrationTests {
	@Test // GH-1004
	void registersListenerWithDefaultContainer() {
		ConfigurableApplicationContext context = new AnnotationConfigApplicationContext(Config.class, SimpleService.class);
		RedisMessageListenerContainer container = context.getBean("redisMessageListenerContainer",
				RedisMessageListenerContainer.class);

		verify(container).addMessageListener(any(), anyCollection());

		RedisListenerEndpointRegistry registry = context.getBean(RedisListenerEndpointRegistry.class);
		assertThat(registry.isRunning()).isTrue();

		context.close();
		assertThat(registry.isRunning()).isFalse();
	}

	@Configuration
	@EnableRedisListeners
	static class Config {
		@Bean
		public RedisMessageListenerContainer redisMessageListenerContainer() {
			return mock(RedisMessageListenerContainer.class);
		}
	}

	static class SimpleService {
		@RedisListener(channels = "test-topic")
		public void handle(String msg) {}
	}
}
