/*
 * Copyright 2026-present the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.redis.config;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.jspecify.annotations.Nullable;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory;
import org.springframework.messaging.handler.annotation.support.MessageHandlerMethodFactory;
import org.springframework.messaging.handler.invocation.HandlerMethodArgumentResolver;
import org.springframework.util.Assert;
import org.springframework.util.MimeType;
import org.springframework.validation.Validator;

/**
 * Helper bean for registering {@link RedisListenerEndpoint} instances with a {@link RedisListenerEndpointRegistry}.
 * <p>
 * Provides convenient methods to register endpoints and configure the underlying infrastructure, such as the
 * {@link MessageHandlerMethodFactory} and {@link MessageConverter}.
 *
 * @author Ilyass Bougati
 */
public class RedisListenerEndpointRegistrar implements BeanFactoryAware, InitializingBean {
	private @Nullable RedisListenerEndpointRegistry endpointRegistry;
	private @Nullable RedisMessageListenerContainer listenerContainer;
	private @Nullable MessageHandlerMethodFactory methodFactory;
	private final List<RedisListenerEndpoint> redisListenerEndpointDescriptors = new ArrayList<>();
	private @Nullable ConfigurableBeanFactory beanFactory;
	private boolean startImmediately;

	private @Nullable MessageConverter messageConverter;
	private @Nullable Validator validator;
	private @Nullable ConversionService conversionService;
	private @Nullable MimeType defaultMimeType;

	private @Nullable List<HandlerMethodArgumentResolver> customArgumentResolvers;

	@Override
	public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
		if (beanFactory instanceof ConfigurableBeanFactory configurableBeanFactory) {
			this.beanFactory = configurableBeanFactory;
		}
	}

	@Override
	public void afterPropertiesSet() {
		Assert.state(this.endpointRegistry != null, "RedisListenerEndpointRegistry must be set");
		registerAllEndpoints();
	}

	private MessageHandlerMethodFactory createDefaultMessageHandlerMethodFactory() {
		DefaultMessageHandlerMethodFactory defaultFactory = new DefaultMessageHandlerMethodFactory();

		if (this.beanFactory != null) {
			defaultFactory.setBeanFactory(this.beanFactory);
		}

		defaultFactory.setConversionService(
				Objects.requireNonNullElseGet(this.conversionService, DefaultConversionService::getSharedInstance));

		defaultFactory.setMessageConverter(
				Objects.requireNonNullElseGet(this.messageConverter, () -> RedisMessageConverters.builder().build()));

		if (this.validator != null) {
			defaultFactory.setValidator(this.validator);
		}

		if (this.customArgumentResolvers != null && !this.customArgumentResolvers.isEmpty()) {
			defaultFactory.setCustomArgumentResolvers(this.customArgumentResolvers);
		}

		defaultFactory.afterPropertiesSet();
		return defaultFactory;
	}

	/**
	 * Register all queued endpoints directly with the underlying registry.
	 */
	protected void registerAllEndpoints() {
		Assert.state(this.endpointRegistry != null, "No RedisListenerEndpointRegistry set");
		for (RedisListenerEndpoint endpoint : this.redisListenerEndpointDescriptors) {
			if (endpoint instanceof MethodRedisListenerEndpoint methodEndpoint) {
				if (this.methodFactory != null) {
					methodEndpoint.setMessageHandlerMethodFactory(this.methodFactory);
				}
			}
			this.endpointRegistry.registerListener(endpoint, listenerContainer);
		}
		this.startImmediately = true;
	}

	/**
	 * Register a new {@link RedisListenerEndpoint} alongside the {@link RedisMessageListenerContainer}.
	 * <p>
	 * If the underlying {@link RedisListenerEndpointRegistry} is already active, the endpoint is registered and started
	 * immediately. Otherwise, it is queued and registered once the registry becomes active.
	 * 
	 * @param endpoint the {@link RedisListenerEndpoint} to register
	 */
	public void registerEndpoint(RedisListenerEndpoint endpoint) {
		Assert.notNull(endpoint, "Endpoint must not be null");
		Assert.hasText(endpoint.getId(), "Endpoint id must be set");

		if (endpoint instanceof MethodRedisListenerEndpoint methodEndpoint) {
			if (methodEndpoint.getMessageHandlerMethodFactory() == null) {
				methodEndpoint.setMessageHandlerMethodFactory(getMessageHandlerMethodFactory());
			}
		}

		if (this.startImmediately) {
			Assert.state(this.endpointRegistry != null, "No RedisListenerEndpointRegistry set");
			if (endpoint instanceof MethodRedisListenerEndpoint methodEndpoint) {
				methodEndpoint.setMessageHandlerMethodFactory(getMessageHandlerMethodFactory());
			}
			this.endpointRegistry.registerListener(endpoint, listenerContainer);
		} else {
			this.redisListenerEndpointDescriptors.add(endpoint);
		}
	}

	/**
	 * Return the custom {@link MessageHandlerMethodFactory} to use, if any.
	 * <p>
	 * If no custom factory is set, a default one will be created based on the configured {@link ConversionService},
	 * {@link MessageConverter}, and {@link Validator}.
	 * 
	 * @return the {@link MessageHandlerMethodFactory}
	 */
	public MessageHandlerMethodFactory getMessageHandlerMethodFactory() {
		if (this.methodFactory == null) {
			this.methodFactory = createDefaultMessageHandlerMethodFactory();
		}
		return this.methodFactory;
	}

	/**
	 * Set the {@link RedisListenerEndpointRegistry} instance to use.
	 * 
	 * @param endpointRegistry the {@link RedisListenerEndpointRegistry}
	 */
	public void setEndpointRegistry(@Nullable RedisListenerEndpointRegistry endpointRegistry) {
		this.endpointRegistry = endpointRegistry;
	}

	/**
	 * Set the {@link RedisMessageListenerContainer} to use.
	 * 
	 * @param listenerContainer the {@link RedisMessageListenerContainer}
	 */
	public void setListenerContainer(@Nullable RedisMessageListenerContainer listenerContainer) {
		this.listenerContainer = listenerContainer;
	}

	/**
	 * Set custom {@link HandlerMethodArgumentResolver resolvers} to support custom controller method arguments.
	 * 
	 * @param customArgumentResolvers the list of resolvers to configure
	 */
	public void setCustomArgumentResolvers(List<HandlerMethodArgumentResolver> customArgumentResolvers) {
		this.customArgumentResolvers = customArgumentResolvers;
	}

	/**
	 * Set the {@link Validator} to use for payload validation.
	 * 
	 * @param validator the {@link Validator} to configure
	 */
	public void setValidator(@Nullable Validator validator) {
		this.validator = validator;
	}

	/**
	 * Set the {@link ConversionService} to use for payload conversion.
	 * 
	 * @param conversionService the {@link ConversionService} to configure
	 */
	public void setConversionService(@Nullable ConversionService conversionService) {
		this.conversionService = conversionService;
	}

	/**
	 * Set the {@link MessageConverter} to use for payload conversion.
	 * 
	 * @param messageConverter the {@link MessageConverter} to configure
	 */
	public void setMessageConverter(@Nullable MessageConverter messageConverter) {
		this.messageConverter = messageConverter;
	}

	/**
	 * Return the {@link RedisListenerEndpointRegistry} instance for this registrar, may be {@code null}.
	 * 
	 * @return the {@link RedisListenerEndpointRegistry}
	 */
	public @Nullable RedisListenerEndpointRegistry getEndpointRegistry() {
		return endpointRegistry;
	}
}
