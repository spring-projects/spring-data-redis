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
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.springframework.aop.framework.AopInfrastructureBean;
import org.springframework.aop.framework.AopProxyUtils;
import org.springframework.beans.factory.*;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.beans.factory.config.EmbeddedValueResolver;
import org.springframework.core.MethodIntrospector;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.data.redis.config.*;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.format.support.DefaultFormattingConversionService;
import org.springframework.messaging.handler.invocation.HandlerMethodArgumentResolver;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;
import org.springframework.util.StringValueResolver;
import org.springframework.validation.Validator;

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
	 * Making a {@link BeanFactory} available is optional; if not set, {@link #setEndpointRegistry endpoint registry} has
	 * to be explicitly configured.
	 */
	@Override
	public void setBeanFactory(@NonNull BeanFactory beanFactory) {
		this.beanFactory = beanFactory;
		if (beanFactory instanceof ConfigurableBeanFactory cbf) {
			this.embeddedValueResolver = new EmbeddedValueResolver(cbf);
			this.registrar.setBeanFactory(cbf);
		}
	}

	@Override
	public Object postProcessAfterInitialization(@NonNull Object bean, @NonNull String beanName) {
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
		String containerName = resolve(redisListener.container());
		RedisMessageListenerContainer listenerContainer = null;

		if (StringUtils.hasText(containerName)) {
			Assert.state(this.beanFactory != null, "BeanFactory must be set to obtain container by name");

			try {
				listenerContainer = this.beanFactory.getBean(containerName, RedisMessageListenerContainer.class);
			} catch (NoSuchBeanDefinitionException ex) {
				throw new BeanInitializationException(
						"Could not register Redis listener endpoint, no RedisMessageListenerContainer with id '" + containerName
								+ "' was found",
						ex);
			}
		}

		MethodRedisListenerEndpoint endpoint = createEndpoint(redisListener, method, bean);

		if (listenerContainer != null) {
			endpoint.setListenerContainer(listenerContainer);
		}

		this.registrar.registerEndpoint(endpoint);
	}

	MethodRedisListenerEndpoint createEndpoint(RedisListener redisListener, Method method, Object bean) {
		MethodRedisListenerEndpoint endpoint = new MethodRedisListenerEndpoint(bean, method);
		endpoint.setId(getEndpointId(redisListener));
		endpoint.setTopic(redisListener.topic());
		endpoint.setConsumes(redisListener.consumes().isEmpty() ? null : redisListener.consumes());
		return endpoint;
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

	@Override
	public void afterSingletonsInstantiated() {
		this.nonAnnotatedClasses.clear();
		Assert.state(this.beanFactory != null, "BeanFactory must be set");

		if (this.endpointRegistry == null) {
			this.endpointRegistry = this.beanFactory.getBean(RedisListenerEndpointRegistry.class);
		}
		this.registrar.setEndpointRegistry(this.endpointRegistry);

		try {
			RedisMessageListenerContainer container = this.beanFactory.getBean(RedisMessageListenerContainer.class);
			this.registrar.setListenerContainer(container);
		} catch (NoSuchBeanDefinitionException ex) {
			// Gracefully ignore if a default container isn't provided
		}

		if (this.beanFactory instanceof ListableBeanFactory lbf) {
			Map<String, RedisListenerConfigurer> configurers = lbf.getBeansOfType(RedisListenerConfigurer.class);

			if (!configurers.isEmpty()) {
				DefaultFormattingConversionService conversionService = new DefaultFormattingConversionService();
				List<HandlerMethodArgumentResolver> argumentResolvers = new ArrayList<>();
				RedisMessageConverters.Builder converterBuilder = RedisMessageConverters.builder();
				Validator[] validator = new Validator[1];

				configurers.values().forEach(c -> {
					c.addConverters(conversionService);
					c.addArgumentResolvers(argumentResolvers);
					c.configureMessageConverters(converterBuilder);
					c.configureRegistrar(this.registrar);

					Validator customValidator = c.getValidator();
					if (customValidator != null) {
						validator[0] = customValidator;
					}
				});

				this.registrar.setConversionService(conversionService);
				this.registrar.setMessageConverter(converterBuilder.build());

				if (!argumentResolvers.isEmpty()) {
					this.registrar.setCustomArgumentResolvers(argumentResolvers);
				}
				if (validator[0] != null) {
					this.registrar.setValidator(validator[0]);
				}
			}
		}

		this.registrar.afterPropertiesSet();
	}
}
