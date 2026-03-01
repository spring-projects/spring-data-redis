package org.springframework.data.redis.annotation;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.config.RedisListenerConfigurer;
import org.springframework.data.redis.config.RedisListenerEndpointRegistrar;
import org.springframework.data.redis.config.RedisListenerEndpointRegistry;

class RedisListenerConfigurerIntegrationTests {

	@Test
	void infrastructureInvokesConfigurerCallbacks() {
		try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {

			context.register(TestConfig.class);
			context.refresh();

			MockCustomConfigurer configurer = context.getBean(MockCustomConfigurer.class);

			assertThat(configurer.isRegistrarConfigured).isTrue();
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
		public void configureRegistrar(RedisListenerEndpointRegistrar registrar) {
			this.isRegistrarConfigured = true;
		}
	}
}
