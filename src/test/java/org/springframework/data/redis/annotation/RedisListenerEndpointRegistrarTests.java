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

import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import java.lang.reflect.Method;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.context.support.StaticApplicationContext;
import org.springframework.data.redis.config.MethodRedisListenerEndpoint;
import org.springframework.data.redis.config.RedisListenerEndpointRegistrar;
import org.springframework.data.redis.config.RedisListenerEndpointRegistry;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.util.ReflectionUtils;

/**
 * @author Ilyass Bougati
 */
@ExtendWith(MockitoExtension.class)
class RedisListenerEndpointRegistrarTests {

	private RedisListenerEndpointRegistrar registrar;
	@Mock private RedisListenerEndpointRegistry registry;
	@Mock private RedisMessageListenerContainer container;

	private Method dummyMethod;
	private Object dummyBean;

	@BeforeEach
	void setup() {
		this.registrar = new RedisListenerEndpointRegistrar();
		StaticApplicationContext context = new StaticApplicationContext();

		this.registrar.setBeanFactory(context.getBeanFactory());
		this.registrar.setEndpointRegistry(this.registry);
		this.registrar.setListenerContainer(this.container);

		this.dummyBean = new Object();
		this.dummyMethod = ReflectionUtils.findMethod(Object.class, "toString");
	}

	@Test
	void registerEndpoint_BeforeInitialization_AddsToWaitingRoom() {
		MethodRedisListenerEndpoint endpoint = new MethodRedisListenerEndpoint(this.dummyBean, this.dummyMethod);
		endpoint.setId("test-endpoint-1");

		this.registrar.registerEndpoint(endpoint);

		verifyNoInteractions(this.registry);
	}

	@Test
	void afterPropertiesSet_BuildsDefaultFactory_AndFlushesWaitingRoom() {
		MethodRedisListenerEndpoint endpoint = new MethodRedisListenerEndpoint(this.dummyBean, this.dummyMethod);
		endpoint.setId("test-endpoint-2");
		this.registrar.registerEndpoint(endpoint);

		this.registrar.afterPropertiesSet();
		verify(this.registry).registerListener(eq(endpoint), eq(this.container));
	}

	@Test
	void registerEndpoint_AfterInitialization_RegistersImmediately() {
		this.registrar.afterPropertiesSet();

		MethodRedisListenerEndpoint endpoint = new MethodRedisListenerEndpoint(this.dummyBean, this.dummyMethod);
		endpoint.setId("test-endpoint-3");

		this.registrar.registerEndpoint(endpoint);
		verify(this.registry).registerListener(eq(endpoint), eq(this.container));
	}

	@Test
	void resolveRegistry_FailsIfNoRegistryAndNoBeanFactory() {
		RedisListenerEndpointRegistrar badRegistrar = new RedisListenerEndpointRegistrar();

		assertThatIllegalStateException().isThrownBy(badRegistrar::afterPropertiesSet)
				.withMessageContaining("BeanFactory must be set");
	}
}
