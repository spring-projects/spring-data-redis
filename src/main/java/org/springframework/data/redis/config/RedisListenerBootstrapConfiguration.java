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

import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Role;
import org.springframework.data.redis.annotation.EnableRedisListeners;
import org.springframework.data.redis.annotation.RedisListener;
import org.springframework.data.redis.annotation.RedisListenerAnnotationBeanPostProcessor;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

/**
 * {@code @Configuration} class that registers a {@link RedisListenerAnnotationBeanPostProcessor} bean capable of
 * processing Spring's {@link RedisListener @RedisListener} annotation. Also registers a default
 * {@link RedisListenerEndpointRegistry}.
 * <p>
 * This configuration class is automatically imported when using the {@code @EnableRedisListeners} annotation. See the
 * {@link EnableRedisListeners @EnableRedisListeners} for complete usage details.
 *
 * @author Ilyass Bougati
 * @author Mark Paluch
 * @since 4.1
 */
@Configuration(proxyBeanMethods = false)
@Role(BeanDefinition.ROLE_INFRASTRUCTURE)
public class RedisListenerBootstrapConfiguration {

	public RedisListenerBootstrapConfiguration() {
		Assert.state(ClassUtils.isPresent("org.springframework.messaging.handler.invocation.InvocableHandlerMethod",
				MethodRedisListenerEndpoint.class.getClassLoader()), "spring-messaging must be on the class path");
	}

	@Bean(name = RedisListenerConfigUtils.REDIS_LISTENER_ANNOTATION_PROCESSOR_BEAN_NAME)
	@Role(BeanDefinition.ROLE_INFRASTRUCTURE)
	public RedisListenerAnnotationBeanPostProcessor redisListenerAnnotationBeanPostProcessor() {
		return new RedisListenerAnnotationBeanPostProcessor();
	}

	@Bean(name = RedisListenerConfigUtils.REDIS_LISTENER_ENDPOINT_REGISTRY_BEAN_NAME)
	@Role(BeanDefinition.ROLE_INFRASTRUCTURE)
	public RedisListenerEndpointRegistry redisListenerEndpointRegistry() {
		return new RedisListenerEndpointRegistry();
	}

}
