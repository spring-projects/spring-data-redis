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

import java.lang.reflect.Method;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.jspecify.annotations.NonNull;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.core.MethodIntrospector;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.data.redis.config.MethodRedisListenerEndpoint;
import org.springframework.data.redis.config.RedisListenerEndpointRegistry;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory;
import org.springframework.messaging.handler.annotation.support.MessageHandlerMethodFactory;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

/**
 * Bean post-processor that registers methods annotated with {@link RedisListener} to be invoked by a
 * {@link org.springframework.data.redis.listener.RedisMessageListenerContainer}.
 * <p>
 * The container is retrieved from the {@link org.springframework.beans.factory.BeanFactory} name.
 *
 * @author Ilyass Bougati
 * @see RedisListener
 * @see RedisListenerEndpointRegistry
 */
public class RedisListenerAnnotationBeanPostProcessor implements BeanPostProcessor, Ordered {

	private static final boolean messagingPresent = ClassUtils.isPresent(
			"org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory",
			RedisListenerAnnotationBeanPostProcessor.class.getClassLoader());

	private MessageHandlerMethodFactory messageHandlerMethodFactory;

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
		MethodRedisListenerEndpoint endpoint = new MethodRedisListenerEndpoint(bean, method);
		endpoint.setBeanFactory(this.beanFactory);
		endpoint.setId(getEndpointId(redisListener));
		endpoint.setChannels(redisListener.channels());
		endpoint.setPatterns(redisListener.patterns());

		endpoint.setMessageHandlerMethodFactory(getMessageHandlerMethodFactory());

		RedisMessageListenerContainer container = this.beanFactory.getBean(redisListener.container(),
				RedisMessageListenerContainer.class);

		this.endpointRegistry.registerListenerContainer(endpoint, container);
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

	public void setMessageHandlerMethodFactory(MessageHandlerMethodFactory messageHandlerMethodFactory) {
		this.messageHandlerMethodFactory = messageHandlerMethodFactory;
	}

	private MessageHandlerMethodFactory getMessageHandlerMethodFactory() {
		if (this.messageHandlerMethodFactory == null) {
			if (!messagingPresent) {
				throw new IllegalStateException("spring-messaging is required to use @RedisListener. "
						+ "Please add org.springframework:spring-messaging to your classpath.");
			}
			this.messageHandlerMethodFactory = createDefaultMessageHandlerMethodFactory();
		}
		return this.messageHandlerMethodFactory;
	}

	private MessageHandlerMethodFactory createDefaultMessageHandlerMethodFactory() {
		DefaultMessageHandlerMethodFactory defaultFactory = new DefaultMessageHandlerMethodFactory();
		defaultFactory.afterPropertiesSet();
		return defaultFactory;
	}
}
