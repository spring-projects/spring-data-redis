package org.springframework.data.redis.annotation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;

import org.junit.jupiter.api.Test;
import org.springframework.core.convert.converter.ConverterRegistry;
import org.springframework.data.redis.config.RedisListenerConfigurer;
import org.springframework.data.redis.config.RedisListenerEndpointRegistrar;
import org.springframework.data.redis.config.RedisMessageConverters;

class RedisListenerConfigurerTests {

	@Test
	void defaultMethodsShouldExecuteWithoutExceptionAndReturnExpectedDefaults() {
		RedisListenerConfigurer configurer = new RedisListenerConfigurer() {};

		assertThat(configurer.getValidator()).isNull();

		configurer.addConverters(mock(ConverterRegistry.class));
		configurer.addArgumentResolvers(new ArrayList<>());

		configurer.configureMessageConverters(mock(RedisMessageConverters.Builder.class));
		configurer.configureRegistrar(mock(RedisListenerEndpointRegistrar.class));
	}
}
