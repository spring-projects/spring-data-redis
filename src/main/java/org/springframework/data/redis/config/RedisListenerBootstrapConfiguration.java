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

import org.springframework.beans.factory.BeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Role;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.data.redis.annotation.EnableRedisListeners;
import org.springframework.data.redis.annotation.RedisListener;
import org.springframework.data.redis.annotation.RedisListenerAnnotationBeanPostProcessor;
import org.springframework.data.redis.annotation.RedisListenerConfigurationSelector;

/**
 * {@code @Configuration} class that registers the container infrastructure beans necessary
 * to enable {@link RedisListener} annotation processing.
 *
 * <p>Registered by the {@link RedisListenerConfigurationSelector} when {@link EnableRedisListeners}
 * is used.
 *
 * @author Ilyass Bougati
 * @see EnableRedisListeners
 * @see RedisListenerConfigurationSelector
 */
@Configuration(proxyBeanMethods = false)
@Role(BeanDefinition.ROLE_INFRASTRUCTURE)
public class RedisListenerBootstrapConfiguration {

	@Bean
	@Role(BeanDefinition.ROLE_INFRASTRUCTURE)
	public RedisListenerEndpointRegistry redisListenerEndpointRegistry() {
		return new RedisListenerEndpointRegistry();
	}

	@Bean
	@Role(BeanDefinition.ROLE_INFRASTRUCTURE)
	public RedisListenerAnnotationBeanPostProcessor redisListenerAnnotationBeanPostProcessor(
			RedisListenerEndpointRegistry registry, BeanFactory beanFactory) {

		RedisListenerAnnotationBeanPostProcessor bpp = new RedisListenerAnnotationBeanPostProcessor();
		bpp.setEndpointRegistry(registry);
		bpp.setBeanFactory(beanFactory);
		return bpp;
	}
}
