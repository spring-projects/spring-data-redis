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

import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.core.Ordered;
import org.springframework.data.redis.config.RedisListenerContainerFactory;
import org.springframework.data.redis.config.RedisListenerEndpointRegistry;
import org.springframework.aop.support.AopUtils;
import org.springframework.core.MethodIntrospector;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.util.StringUtils;
import org.jspecify.annotations.NonNull;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Bean post-processor that registers methods annotated with {@link RedisListener} to be invoked by a Redis message
 * listener container created under the cover by a
 * {@link org.springframework.data.redis.config.RedisListenerContainerFactory}.
 *
 * @author Ilyass Bougati
 * @see RedisListener
 * @see RedisListenerEndpointRegistry
 */
public class RedisListenerAnnotationBeanPostProcessor implements BeanPostProcessor, Ordered {

	private int order = Ordered.LOWEST_PRECEDENCE;
	private RedisListenerEndpointRegistry endpointRegistry;
	private String containerFactoryBeanName = "redisListenerContainerFactory";
	private final Set<Class<?>> nonAnnotatedClasses = ConcurrentHashMap.newKeySet();
	private BeanFactory beanFactory;

	@Override
	public Object postProcessAfterInitialization(Object bean, @NonNull String beanName) {
		if (!this.nonAnnotatedClasses.contains(bean.getClass())) {
			Class<?> targetClass = AopUtils.getTargetClass(bean);
			Map<Method, RedisListener> annotatedMethods = MethodIntrospector.selectMethods(targetClass,
					(MethodIntrospector.MetadataLookup<RedisListener>) method -> AnnotatedElementUtils
							.findMergedAnnotation(method, RedisListener.class));

			if (annotatedMethods.isEmpty()) {
				this.nonAnnotatedClasses.add(bean.getClass());
			} else {
				annotatedMethods
						.forEach((method, redisListener) -> processRedisListener(redisListener, method, bean, beanName));
			}
		}
		return bean;
	}

	protected void processRedisListener(RedisListener redisListener, Method method, Object bean, String beanName) {
		MethodRedisListenerEndpoint endpoint = new MethodRedisListenerEndpoint();
		endpoint.setBean(bean);
		endpoint.setMethod(method);
		endpoint.setId(getEndpointId(redisListener));
		endpoint.setTopics(redisListener.topics());
		endpoint.setTopicPatterns(redisListener.topicPatterns());
		endpoint.setBeanFactory(this.beanFactory);

		// Use the factory from the annotation, or fall back to default
		String containerFactoryName = redisListener.containerFactory();
		if (!StringUtils.hasText(containerFactoryName)) {
			containerFactoryName = this.containerFactoryBeanName;
		}

		RedisListenerContainerFactory<?> factory = this.beanFactory.getBean(containerFactoryName,
				RedisListenerContainerFactory.class);
		this.endpointRegistry.registerListenerContainer(endpoint, factory);
	}

	private String getEndpointId(RedisListener redisListener) {
		if (StringUtils.hasText(redisListener.id())) {
			return redisListener.id();
		}
		return java.util.UUID.randomUUID().toString();
	}

	@Override
	public int getOrder() {
		return this.order;
	}

	public void setOrder(int order) {
		this.order = order;
	}

	public RedisListenerEndpointRegistry getEndpointRegistry() {
		return endpointRegistry;
	}

	public void setEndpointRegistry(RedisListenerEndpointRegistry endpointRegistry) {
		this.endpointRegistry = endpointRegistry;
	}

	public String getContainerFactoryBeanName() {
		return containerFactoryBeanName;
	}

	public void setContainerFactoryBeanName(String containerFactoryBeanName) {
		this.containerFactoryBeanName = containerFactoryBeanName;
	}

	public void setBeanFactory(BeanFactory beanFactory) {
		this.beanFactory = beanFactory;
	}
}
