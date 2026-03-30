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

import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.converter.ConverterRegistry;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.serializer.RedisMessageConverters;
import org.springframework.format.support.DefaultFormattingConversionService;
import org.springframework.messaging.converter.CompositeMessageConverter;
import org.springframework.messaging.converter.GenericMessageConverter;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory;
import org.springframework.messaging.handler.annotation.support.MessageHandlerMethodFactory;
import org.springframework.messaging.handler.invocation.HandlerMethodArgumentResolver;
import org.springframework.util.Assert;
import org.springframework.validation.Validator;

/**
 * Helper bean for registering {@link RedisListenerEndpoint} instances with a {@link RedisListenerEndpointRegistry}.
 * <p>
 * Provides convenient methods to register endpoints and configure the underlying infrastructure, such as the
 * {@link MessageHandlerMethodFactory} and {@link MessageConverter}.
 *
 * @author Ilyass Bougati
 * @author Mark Paluch
 * @since 4.1
 */
public class RedisListenerEndpointRegistrar implements BeanFactoryAware, InitializingBean {

	private final EndpointRegistrationConfiguration configuration = new EndpointRegistrationConfiguration();

	private @Nullable RedisListenerEndpointRegistry endpointRegistry;

	private @Nullable MessageHandlerMethodFactory messageHandlerMethodFactory;

	private @Nullable BeanFactory beanFactory;

	private final List<RedisListenerEndpointDescriptor> redisListenerEndpointDescriptors = new ArrayList<>();

	private boolean startImmediately;

	/**
	 * Set the {@link RedisListenerEndpointRegistry} instance to use.
	 *
	 * @param endpointRegistry the {@link RedisListenerEndpointRegistry}
	 */
	public void setEndpointRegistry(@Nullable RedisListenerEndpointRegistry endpointRegistry) {
		this.endpointRegistry = endpointRegistry;
	}

	/**
	 * Return the {@link RedisListenerEndpointRegistry} instance for this registrar, may be {@code null}.
	 */
	public @Nullable RedisListenerEndpointRegistry getEndpointRegistry() {
		return this.endpointRegistry;
	}

	/**
	 * Set the {@link MessageHandlerMethodFactory} to use to configure the message listener responsible to serve an
	 * endpoint detected by this processor.
	 * <p>
	 * By default, {@link DefaultMessageHandlerMethodFactory} is used and it can be configured further to support
	 * additional method arguments or to customize conversion and validation support. See
	 * {@link DefaultMessageHandlerMethodFactory} javadoc for more details.
	 */
	public void setMessageHandlerMethodFactory(@Nullable MessageHandlerMethodFactory messageHandlerMethodFactory) {
		this.messageHandlerMethodFactory = messageHandlerMethodFactory;
	}

	/**
	 * Return the {@link MessageHandlerMethodFactory} to use.
	 * <p>
	 * If no custom factory is set, a default one will be created based on the configured {@link ConversionService},
	 * {@link MessageConverter}, and {@link Validator}.
	 *
	 * @return the {@link MessageHandlerMethodFactory}
	 */
	public MessageHandlerMethodFactory getMessageHandlerMethodFactory() {
		if (this.messageHandlerMethodFactory == null) {
			this.messageHandlerMethodFactory = configuration.createMessageHandlerMethodFactory();
		}
		return this.messageHandlerMethodFactory;
	}

	@Override
	public void setBeanFactory(BeanFactory beanFactory) {
		this.beanFactory = beanFactory;
	}

	/**
	 * Set custom {@link HandlerMethodArgumentResolver resolvers} to support custom controller method arguments.
	 *
	 * @param customArgumentResolvers the list of resolvers to configure
	 */
	public void setCustomArgumentResolvers(List<HandlerMethodArgumentResolver> customArgumentResolvers) {
		configuration.setCustomArgumentResolvers(customArgumentResolvers);
	}

	/**
	 * Set the {@link Validator} to use for payload validation.
	 *
	 * @param validator the {@link Validator} to configure
	 */
	public void setValidator(@Nullable Validator validator) {
		configuration.setValidator(validator);
	}

	/**
	 * Set the {@link ConversionService} to use for payload conversion.
	 *
	 * @param conversionService the {@link ConversionService} to configure
	 */
	public void setConversionService(@Nullable ConversionService conversionService) {
		configuration.setConversionService(conversionService);
	}

	/**
	 * Set the {@link MessageConverter} to use for payload conversion.
	 *
	 * @param messageConverter the {@link MessageConverter} to configure
	 */
	public void setMessageConverter(@Nullable MessageConverter messageConverter) {
		configuration.setMessageConverter(messageConverter);
	}

	List<RedisListenerEndpointDescriptor> getRedisListenerEndpointDescriptors() {
		return redisListenerEndpointDescriptors;
	}

	@Override
	public void afterPropertiesSet() {
		registerAllEndpoints();
	}

	/**
	 * Register all queued endpoints directly with the underlying registry.
	 */
	protected void registerAllEndpoints() {

		Assert.state(this.endpointRegistry != null, "No RedisListenerEndpointRegistry set");

		for (RedisListenerEndpointDescriptor descriptor : this.redisListenerEndpointDescriptors) {
			if (descriptor.endpoint instanceof MethodRedisListenerEndpoint methodEndpoint) {
				methodEndpoint.setMessageHandlerMethodFactory(getMessageHandlerMethodFactory());
			}
			this.endpointRegistry.registerListener(descriptor.endpoint, descriptor.container);
		}

		this.startImmediately = true; // trigger immediate startup
	}

	/**
	 * Register a new {@link RedisListenerEndpoint} alongside the {@link RedisMessageListenerContainer}.
	 * <p>
	 * If the underlying {@link RedisListenerEndpointRegistry} is already active, the endpoint is registered and started
	 * immediately. Otherwise, it is queued and registered once the registry becomes active.
	 *
	 * @param endpoint the {@link RedisListenerEndpoint} to register
	 */
	public void registerEndpoint(RedisListenerEndpoint endpoint, RedisMessageListenerContainer container) {

		Assert.notNull(endpoint, "Endpoint must not be null");
		Assert.hasText(endpoint.getId(), "Endpoint id must be set");

		RedisListenerEndpointDescriptor descriptor = new RedisListenerEndpointDescriptor(endpoint, container);

		if (this.startImmediately) {
			Assert.state(this.endpointRegistry != null, "No RedisListenerEndpointRegistry set");

			if (endpoint instanceof MethodRedisListenerEndpoint methodEndpoint) {
				methodEndpoint.setMessageHandlerMethodFactory(getMessageHandlerMethodFactory());
			}
			this.endpointRegistry.registerListener(endpoint, descriptor.container);
		} else {
			this.redisListenerEndpointDescriptors.add(descriptor);
		}
	}

	public void apply(List<RedisListenerConfigurer> configurers) {

		ConverterRegistry registry;
		if (configuration.conversionService instanceof ConverterRegistry cr) {
			registry = cr;
		} else {
			DefaultFormattingConversionService conversionService = new DefaultFormattingConversionService();
			registry = conversionService;
			this.configuration.setConversionService(conversionService);
		}

		RedisMessageConverters.Builder converterBuilder = RedisMessageConverters.builder();

		for (RedisListenerConfigurer configurer : configurers) {

			configurer.addConverters(registry);
			configurer.addArgumentResolvers(this.configuration.customArgumentResolvers);
			configurer.configureMessageConverters(converterBuilder);
			configurer.configureRedisListeners(this);

			Validator validator = configurer.getValidator();
			if (validator != null) {
				configuration.setValidator(validator);
			}
		}

		MessageConverter converter = converterBuilder.build().getConverter();

		if (converter instanceof CompositeMessageConverter cmc && configuration.conversionService != null) {
			cmc.getConverters().add(new GenericMessageConverter(configuration.conversionService));
		}

		this.configuration.setMessageConverter(converter);
	}

	/**
	 * Holder for an endpoint and its container.
	 */
	record RedisListenerEndpointDescriptor(RedisListenerEndpoint endpoint, RedisMessageListenerContainer container) {

	}

	/**
	 * Configuration holder.
	 */
	class EndpointRegistrationConfiguration {

		private @Nullable MessageConverter messageConverter;

		private @Nullable Validator validator;

		private @Nullable ConversionService conversionService;

		private List<HandlerMethodArgumentResolver> customArgumentResolvers = new ArrayList<>();

		public void setMessageConverter(@Nullable MessageConverter messageConverter) {
			this.messageConverter = messageConverter;
		}

		public void setValidator(@Nullable Validator validator) {
			this.validator = validator;
		}

		public @Nullable ConversionService getConversionService() {
			return conversionService;
		}

		public void setConversionService(ConversionService conversionService) {
			this.conversionService = conversionService;
		}

		public void setCustomArgumentResolvers(List<HandlerMethodArgumentResolver> customArgumentResolvers) {
			this.customArgumentResolvers = customArgumentResolvers;
		}

		private MessageHandlerMethodFactory createMessageHandlerMethodFactory() {

			DefaultMessageHandlerMethodFactory factory = new DefaultMessageHandlerMethodFactory();

			if (beanFactory != null) {
				factory.setBeanFactory(beanFactory);
			}

			ConversionService conversionService = Objects.requireNonNullElseGet(this.conversionService,
					DefaultConversionService::getSharedInstance);
			factory.setConversionService(conversionService);

			factory.setMessageConverter(Objects.requireNonNullElseGet(this.messageConverter,
					() -> {

						MessageConverter converter = RedisMessageConverters.createMessageConverter();

						if (converter instanceof CompositeMessageConverter cmc) {
							cmc.getConverters().add(new GenericMessageConverter(conversionService));
						}

						return converter;
					}));

			if (this.validator != null) {
				factory.setValidator(this.validator);
			}

			if (!this.customArgumentResolvers.isEmpty()) {
				factory.setCustomArgumentResolvers(this.customArgumentResolvers);
			}

			factory.afterPropertiesSet();
			return factory;
		}

	}

}
