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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jspecify.annotations.Nullable;

import org.springframework.aop.framework.AopInfrastructureBean;
import org.springframework.aop.framework.AopProxyUtils;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.BeanInitializationException;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.beans.factory.config.EmbeddedValueResolver;
import org.springframework.core.MethodIntrospector;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.core.annotation.AnnotationAwareOrderComparator;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.data.redis.config.MethodRedisListenerEndpoint;
import org.springframework.data.redis.config.RedisListenerConfigUtils;
import org.springframework.data.redis.config.RedisListenerConfigurer;
import org.springframework.data.redis.config.RedisListenerEndpointRegistrar;
import org.springframework.data.redis.config.RedisListenerEndpointRegistry;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory;
import org.springframework.messaging.handler.annotation.support.MessageHandlerMethodFactory;
import org.springframework.messaging.handler.invocation.InvocableHandlerMethod;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;
import org.springframework.util.StringValueResolver;

/**
 * Bean post-processor that registers methods annotated with {@link RedisListener} to be subscribed to a Redis message
 * listener container according to the attributes of the annotation.
 * <p>
 * Annotated methods can use flexible arguments as defined by {@link RedisListener}.
 * <p>
 * This post-processor is automatically registered by Spring's by the {@link EnableRedisListeners} annotation.
 * <p>
 * See the {@link EnableRedisListeners} javadocs for complete usage details.
 *
 * @author Ilyass Bougati
 * @author Mark Paluch
 * @since 4.1
 * @see RedisListener
 */
public class RedisListenerAnnotationBeanPostProcessor
		implements BeanPostProcessor, BeanFactoryAware, Ordered, SmartInitializingSingleton {

	protected final Log logger = LogFactory.getLog(getClass());

	private final RedisListenerEndpointRegistrar registrar = new RedisListenerEndpointRegistrar();

	private @Nullable RedisListenerEndpointRegistry endpointRegistry;

	private final MessageHandlerMethodFactoryAdapter messageHandlerMethodFactory = new MessageHandlerMethodFactoryAdapter();

	private int order = Ordered.LOWEST_PRECEDENCE;

	private @Nullable BeanFactory beanFactory;

	private @Nullable StringValueResolver embeddedValueResolver;

	private final AtomicInteger counter = new AtomicInteger();

	private final Set<Class<?>> nonAnnotatedClasses = Collections.newSetFromMap(new ConcurrentHashMap<>(64));

	@Override
	public int getOrder() {
		return this.order;
	}

	public void setOrder(int order) {
		this.order = order;
	}

	/**
	 * Set the {@link RedisListenerEndpointRegistry} that will hold the created endpoint.
	 */
	public void setEndpointRegistry(@Nullable RedisListenerEndpointRegistry endpointRegistry) {
		this.endpointRegistry = endpointRegistry;
	}

	/**
	 * Set the {@link MessageHandlerMethodFactory} to use to configure the message listener responsible to serve an
	 * endpoint detected by this processor.
	 * <p>
	 * By default, {@link DefaultMessageHandlerMethodFactory} is used and it can be configured further to support
	 * additional method arguments or to customize conversion and validation support. See
	 * {@link DefaultMessageHandlerMethodFactory} Javadoc for more details.
	 */
	public void setMessageHandlerMethodFactory(MessageHandlerMethodFactory messageHandlerMethodFactory) {
		this.messageHandlerMethodFactory.setMessageHandlerMethodFactory(messageHandlerMethodFactory);
	}

	/**
	 * Making a {@link BeanFactory} available is optional; if not set, {@link #setEndpointRegistry endpoint registry} has
	 * to be explicitly configured.
	 */
	@Override
	public void setBeanFactory(BeanFactory beanFactory) {
		this.beanFactory = beanFactory;
		if (beanFactory instanceof ConfigurableBeanFactory cbf) {
			this.embeddedValueResolver = new EmbeddedValueResolver(cbf);
			this.registrar.setBeanFactory(cbf);
		}
	}

	@Override
	public void afterSingletonsInstantiated() {

		this.nonAnnotatedClasses.clear();

		if (this.beanFactory instanceof ListableBeanFactory lbf) {
			// Apply JmsListenerConfigurer beans from the BeanFactory, if any
			Map<String, RedisListenerConfigurer> beans = lbf.getBeansOfType(RedisListenerConfigurer.class);
			List<RedisListenerConfigurer> configurers = new ArrayList<>(beans.values());
			AnnotationAwareOrderComparator.sort(configurers);
			registrar.apply(configurers);
		}

		if (this.registrar.getEndpointRegistry() == null) {

			// Determine RedisListenerEndpointRegistry bean from the BeanFactory
			if (this.endpointRegistry == null) {
				Assert.state(this.beanFactory != null, "BeanFactory must be set to find endpoint registry by bean name");
				this.endpointRegistry = this.beanFactory.getBean(
						RedisListenerConfigUtils.REDIS_LISTENER_ENDPOINT_REGISTRY_BEAN_NAME, RedisListenerEndpointRegistry.class);
			}
			this.registrar.setEndpointRegistry(this.endpointRegistry);
		}

		if (!this.messageHandlerMethodFactory.hasMessageHandlerMethodFactory()) {

			// Set the custom handler method factory once resolved by the configurer
			MessageHandlerMethodFactory handlerMethodFactory = this.registrar.getMessageHandlerMethodFactory();
			this.messageHandlerMethodFactory.setMessageHandlerMethodFactory(handlerMethodFactory);
		}

		this.registrar.afterPropertiesSet();
	}

	@Override
	public Object postProcessAfterInitialization(Object bean, String beanName) {

		if (bean instanceof AopInfrastructureBean || bean instanceof RedisMessageListenerContainer
				|| bean instanceof RedisListenerEndpointRegistry) {
			return bean;
		}

		Class<?> targetClass = AopProxyUtils.ultimateTargetClass(bean);
		if (!this.nonAnnotatedClasses.contains(targetClass)
				&& AnnotationUtils.isCandidateClass(targetClass, RedisListener.class)) {
			Map<Method, Set<RedisListener>> annotatedMethods = MethodIntrospector.selectMethods(targetClass,
					(MethodIntrospector.MetadataLookup<Set<RedisListener>>) method -> {
						Set<RedisListener> listenerMethods = AnnotatedElementUtils.getMergedRepeatableAnnotations(method,
								RedisListener.class, RedisListeners.class);
						return (!listenerMethods.isEmpty() ? listenerMethods : null);
					});
			if (annotatedMethods.isEmpty()) {
				this.nonAnnotatedClasses.add(targetClass);
			} else {
				annotatedMethods.forEach(
						(method, listeners) -> listeners.forEach(listener -> processRedisListener(listener, method, bean)));
			}
		}
		return bean;
	}

	/**
	 * Process the given {@link RedisListener} annotation on the given method, registering a corresponding endpoint for
	 * the given bean instance.
	 *
	 * @param redisListener the annotation to process
	 * @param method the annotated method
	 * @param bean the instance to invoke the method on
	 */
	protected void processRedisListener(RedisListener redisListener, Method method, Object bean) {

		RedisMessageListenerContainer container;
		String containerName = resolve(redisListener.container());
		Assert.state(this.beanFactory != null, "BeanFactory must be set to obtain container container by bean name");

		if (StringUtils.hasText(containerName)) {
			try {
				container = this.beanFactory.getBean(containerName, RedisMessageListenerContainer.class);
			} catch (NoSuchBeanDefinitionException ex) {
				throw new BeanInitializationException("Could not register Redis listener endpoint on [" + method + "], no "
						+ RedisMessageListenerContainer.class.getSimpleName() + " with name '" + containerName
						+ "' was found in the application context", ex);
			}
		} else {
			try {
				container = this.beanFactory.getBean(RedisMessageListenerContainer.class);
			} catch (NoSuchBeanDefinitionException ex) {
				throw new BeanInitializationException("Could not register Redis listener endpoint on [" + method + "], no "
						+ RedisMessageListenerContainer.class.getSimpleName() + " was found in the application context", ex);
			}
		}

		MethodRedisListenerEndpoint endpoint = createEndpoint(redisListener, method, bean);
		this.registrar.registerEndpoint(endpoint, container);
	}

	public MethodRedisListenerEndpoint createEndpoint(RedisListener redisListener, Method method, Object bean) {

		MethodRedisListenerEndpoint endpoint = new MethodRedisListenerEndpoint(bean, method);
		endpoint.setMessageHandlerMethodFactory(this.messageHandlerMethodFactory);
		endpoint.setId(getEndpointId(redisListener));
		endpoint.setTopic(redisListener.topic());
		endpoint.setConsumes(redisListener.consumes());

		return endpoint;
	}

	RedisListenerEndpointRegistrar getRegistrar() {
		return registrar;
	}

	private String getEndpointId(RedisListener redisListener) {
		if (StringUtils.hasText(redisListener.id())) {
			String id = resolve(redisListener.id());
			return (id != null ? id : "");
		} else {
			return "org.springframework.data.redis.config.RedisListenerEndpoint#" + this.counter.getAndIncrement();
		}
	}

	private @Nullable String resolve(String value) {
		return (this.embeddedValueResolver != null ? this.embeddedValueResolver.resolveStringValue(value) : value);
	}

	/**
	 * A {@link MessageHandlerMethodFactory} adapter that offers a configurable underlying instance to use. Useful if the
	 * factory to use is determined once the endpoints have been registered but not created yet.
	 *
	 * @see RedisListenerEndpointRegistrar#setMessageHandlerMethodFactory
	 */
	private class MessageHandlerMethodFactoryAdapter implements MessageHandlerMethodFactory {

		private @Nullable MessageHandlerMethodFactory messageHandlerMethodFactory;

		public void setMessageHandlerMethodFactory(MessageHandlerMethodFactory messageHandlerMethodFactory) {
			this.messageHandlerMethodFactory = messageHandlerMethodFactory;
		}

		public boolean hasMessageHandlerMethodFactory() {
			return this.messageHandlerMethodFactory != null;
		}

		@Override
		public InvocableHandlerMethod createInvocableHandlerMethod(Object bean, Method method) {
			return getMessageHandlerMethodFactory().createInvocableHandlerMethod(bean, method);
		}

		private MessageHandlerMethodFactory getMessageHandlerMethodFactory() {
			if (this.messageHandlerMethodFactory == null) {
				this.messageHandlerMethodFactory = createDefaultJmsHandlerMethodFactory();
			}
			return this.messageHandlerMethodFactory;
		}

		private MessageHandlerMethodFactory createDefaultJmsHandlerMethodFactory() {
			DefaultMessageHandlerMethodFactory defaultFactory = new DefaultMessageHandlerMethodFactory();
			if (beanFactory != null) {
				defaultFactory.setBeanFactory(beanFactory);
			}
			defaultFactory.afterPropertiesSet();
			return defaultFactory;
		}

	}

}
