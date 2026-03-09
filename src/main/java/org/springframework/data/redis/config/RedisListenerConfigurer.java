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

import java.util.List;

import org.jspecify.annotations.Nullable;

import org.springframework.core.convert.converter.ConverterRegistry;
import org.springframework.data.redis.serializer.RedisMessageConverters;
import org.springframework.messaging.handler.invocation.HandlerMethodArgumentResolver;
import org.springframework.validation.Validator;

/**
 * Optional interface to be implemented by a Spring-managed bean willing to customize how Redis listener endpoints are
 * configured. Typically used to define the default {@link RedisListenerEndpointRegistrar} or to customize the payload
 * type conversion and validation.
 * <p>
 * See {@link org.springframework.data.redis.annotation.EnableRedisListeners @EnableRedisListeners} for usage examples.
 *
 * @author Ilyass Bougati
 * @author Mark Paluch
 * @since 4.1
 */
public interface RedisListenerConfigurer {

	/**
	 * Add custom {@link org.springframework.core.convert.converter.Converter Converters} and
	 * {@link org.springframework.core.convert.converter.GenericConverter GenericConverters} to perform type conversion
	 * for message payloads.
	 */
	default void addConverters(ConverterRegistry registry) {}

	/**
	 * Add custom {@link HandlerMethodArgumentResolver resolvers} to support custom controller method arguments.
	 */
	default void addArgumentResolvers(List<HandlerMethodArgumentResolver> resolvers) {}

	/**
	 * Provide a custom {@link Validator}.
	 */
	@Nullable
	default Validator getValidator() {
		return null;
	}

	/**
	 * Configure the {@link org.springframework.messaging.converter.MessageConverter MessageConverters} to use for payload
	 * conversion.
	 */
	default void configureMessageConverters(RedisMessageConverters.Builder builder) {}

	/**
	 * Callback allowing a {@link org.springframework.data.redis.config.RedisListenerEndpointRegistry} and specific
	 * {@link org.springframework.data.redis.config.RedisListenerEndpoint} instances to be registered against the given
	 * {@link org.springframework.data.redis.config.RedisListenerEndpointRegistrar}.
	 * <p>
	 * This serves as a generic escape-hatch for aspects not exposed through the interface.
	 *
	 * @param registrar the registrar to be configured
	 */
	default void configureRedisListeners(RedisListenerEndpointRegistrar registrar) {}

}
