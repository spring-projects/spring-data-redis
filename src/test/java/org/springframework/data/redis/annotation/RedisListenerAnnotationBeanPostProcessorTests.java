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
