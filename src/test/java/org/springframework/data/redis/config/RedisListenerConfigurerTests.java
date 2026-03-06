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

import java.nio.charset.StandardCharsets;
import java.util.List;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;

import org.springframework.core.convert.converter.ConverterRegistry;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.data.redis.serializer.JdkSerializationRedisSerializer;
import org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory;
import org.springframework.messaging.handler.annotation.support.DestinationVariableMethodArgumentResolver;
import org.springframework.messaging.handler.invocation.HandlerMethodArgumentResolver;
import org.springframework.validation.Validator;

/**
 * Unit tests for {@link RedisListenerConfigurer}.
 *
 * @author Mark Paluch
 */
class RedisListenerConfigurerTests {

	RedisListenerEndpointRegistrar registrar = new RedisListenerEndpointRegistrar();

	@Test
	void shouldApplyConfiguration() {

		Validator validator = mock(Validator.class);
		DestinationVariableMethodArgumentResolver resolver = new DestinationVariableMethodArgumentResolver(
				DefaultConversionService.getSharedInstance());

		RedisListenerConfigurer configurer = new RedisListenerConfigurer() {

			@Override
			public void addConverters(ConverterRegistry registry) {
				RedisListenerConfigurer.super.addConverters(registry);
			}

			@Override
			public void addArgumentResolvers(List<HandlerMethodArgumentResolver> resolvers) {
				resolvers.add(resolver);
			}

			@Override
			public @Nullable Validator getValidator() {
				return validator;
			}

			@Override
			public void configureMessageConverters(RedisMessageConverters.Builder builder) {
				builder.withStringConverter(StandardCharsets.UTF_8).addCustomConverter(new JdkSerializationRedisSerializer());
			}

		};

		registrar.apply(List.of(configurer));

		DefaultMessageHandlerMethodFactory factory = (DefaultMessageHandlerMethodFactory) registrar
				.getMessageHandlerMethodFactory();

		assertThat(factory).hasFieldOrPropertyWithValue("validator", validator);
		assertThat(factory).hasFieldOrPropertyWithValue("customArgumentResolvers", List.of(resolver));
	}
}
