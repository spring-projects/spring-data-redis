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

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.junit.jupiter.api.Test;

import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.annotation.EnableRedisListeners;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;

/**
 * Unit tests for {@link RedisListenerConfigurer}.
 *
 * @author Ilyass Bougati
 * @author Mark Paluch
 */
class RedisListenerConfigurerIntegrationTests {

	@Test
	void shouldApplyConfiguration() {

		try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {

			context.register(TestConfig.class);
			context.refresh();

			MockCustomConfigurer configurer = context.getBean(MockCustomConfigurer.class);

			assertThat(configurer.isRegistrarConfigured).isTrue();

			RedisListenerEndpointRegistry registry = context.getBean(RedisListenerEndpointRegistry.class);
			assertThat(registry.getEndpoints()).hasSize(1);
		}
	}

	@Configuration
	@EnableRedisListeners
	static class TestConfig {

		@Bean
		public MockCustomConfigurer customConfigurer() {
			return new MockCustomConfigurer();
		}

		@Bean
		public RedisListenerEndpointRegistrar redisListenerEndpointRegistrar(RedisListenerEndpointRegistry registry) {
			RedisListenerEndpointRegistrar registrar = new RedisListenerEndpointRegistrar();
			registrar.setEndpointRegistry(registry);
			return registrar;
		}
	}

	static class MockCustomConfigurer implements RedisListenerConfigurer {

		boolean isRegistrarConfigured = false;

		@Override
		public void configureRedisListeners(RedisListenerEndpointRegistrar registrar) {
			SimpleRedisListenerEndpoint endpoint = new SimpleRedisListenerEndpoint(mock(MessageListener.class));
			endpoint.setId("test");
			endpoint.setTopic("my-channel");
			registrar.registerEndpoint(endpoint, mock(RedisMessageListenerContainer.class));
			this.isRegistrarConfigured = true;
		}
	}
}
