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

import jakarta.annotation.Nullable;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.messaging.converter.*;
import org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory;
import org.springframework.messaging.handler.annotation.support.MessageHandlerMethodFactory;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.MimeType;
import org.springframework.validation.Validator;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
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

    @Override
    public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
        if (beanFactory instanceof ConfigurableBeanFactory configurableBeanFactory) {
            this.beanFactory = configurableBeanFactory;
        }
    }

    @Override
    public void afterPropertiesSet() {
        resolveRegistry();

        if (this.methodFactory == null) {
            this.methodFactory = createDefaultMessageHandlerMethodFactory();
        }

        registerAllEndpoints();
    }

    private void resolveRegistry() {
        if (this.endpointRegistry == null) {
            Assert.state(this.beanFactory != null,
                    "BeanFactory must be set to resolve RedisListenerEndpointRegistry");
            try {
                this.endpointRegistry = this.beanFactory.getBean("redisListenerEndpointRegistry", RedisListenerEndpointRegistry.class);
            } catch (NoSuchBeanDefinitionException ex) {
                this.endpointRegistry = new RedisListenerEndpointRegistry();
            }
        }
    }

    private MessageHandlerMethodFactory createDefaultMessageHandlerMethodFactory() {
        DefaultMessageHandlerMethodFactory defaultFactory = new DefaultMessageHandlerMethodFactory();

        if (this.beanFactory != null) {
            defaultFactory.setBeanFactory(this.beanFactory);
        }

        defaultFactory.setConversionService(Objects.requireNonNullElseGet(this.conversionService, DefaultConversionService::getSharedInstance));
        defaultFactory.setMessageConverter(Objects.requireNonNullElseGet(this.messageConverter, this::buildSmartDefaultMessageConverter));

        if (this.validator != null) {
            defaultFactory.setValidator(this.validator);
        }

        defaultFactory.afterPropertiesSet();

        return defaultFactory;
    }

    private MessageConverter buildSmartDefaultMessageConverter() {
        List<MessageConverter> converters = new ArrayList<>();

        DefaultContentTypeResolver resolver = new DefaultContentTypeResolver();
        if (this.defaultMimeType != null) {
            resolver.setDefaultMimeType(this.defaultMimeType);
        }

        ClassLoader classLoader = (this.beanFactory != null) ? this.beanFactory.getBeanClassLoader() : ClassUtils.getDefaultClassLoader();

        if (ClassUtils.isPresent("com.fasterxml.jackson.databind.ObjectMapper", classLoader)) {
            JacksonJsonMessageConverter jacksonConverter = new JacksonJsonMessageConverter();
            jacksonConverter.setContentTypeResolver(resolver);
            converters.add(jacksonConverter);
        }

        StringMessageConverter stringConverter = new StringMessageConverter(StandardCharsets.UTF_8);
        stringConverter.setContentTypeResolver(resolver);
        converters.add(stringConverter);

        ByteArrayMessageConverter byteArrayConverter = new ByteArrayMessageConverter();
        byteArrayConverter.setContentTypeResolver(resolver);
        converters.add(byteArrayConverter);

        return new CompositeMessageConverter(converters);
    }

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

    public void registerEndpoint(RedisListenerEndpoint endpoint) {
        Assert.notNull(endpoint, "Endpoint must not be null");
        Assert.hasText(endpoint.getId(), "Endpoint id must be set");

        if (this.startImmediately) {
            Assert.state(this.endpointRegistry != null, "No RedisListenerEndpointRegistry set");
            if (endpoint instanceof MethodRedisListenerEndpoint methodEndpoint) {
                if (this.methodFactory != null) {
                    methodEndpoint.setMessageHandlerMethodFactory(this.methodFactory);
                }
            }
            this.endpointRegistry.registerListener(endpoint, listenerContainer);
        } else {
            this.redisListenerEndpointDescriptors.add(endpoint);
        }
    }

    public void setEndpointRegistry(@Nullable RedisListenerEndpointRegistry endpointRegistry) {
        this.endpointRegistry = endpointRegistry;
    }

    public void setListenerContainer(@Nullable RedisMessageListenerContainer listenerContainer) {
        this.listenerContainer = listenerContainer;
    }

    public void setDefaultMimeType(@Nullable MimeType defaultMimeType) {
        this.defaultMimeType = defaultMimeType;
    }
}