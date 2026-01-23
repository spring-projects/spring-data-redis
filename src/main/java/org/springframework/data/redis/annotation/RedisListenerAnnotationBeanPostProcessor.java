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

import org.springframework.aop.framework.AopInfrastructureBean;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.core.Ordered;
import org.springframework.data.redis.config.RedisListenerEndpointRegistry;

/**
 * Bean post-processor that registers methods annotated with {@link RedisListener}
 * to be invoked by a Redis message listener container created under the cover
 * by a {@link org.springframework.data.redis.config.RedisListenerContainerFactory}.
 *
 * @author Ilyass Bougati
 * @see RedisListener
 * @see RedisListenerEndpointRegistry
 */
public class RedisListenerAnnotationBeanPostProcessor implements BeanPostProcessor, Ordered, AopInfrastructureBean {

    private RedisListenerEndpointRegistry endpointRegistry;

    private String containerFactoryBeanName;

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        // TODO: Scan for @RedisListener annotations and register endpoints
        return bean;
    }

    @Override
    public int getOrder() {
        return LOWEST_PRECEDENCE;
    }

    public void setEndpointRegistry(RedisListenerEndpointRegistry endpointRegistry) {
        this.endpointRegistry = endpointRegistry;
    }

    public void setContainerFactoryBeanName(String containerFactoryBeanName) {
        this.containerFactoryBeanName = containerFactoryBeanName;
    }
}
