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

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.lang.reflect.Method;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.springframework.context.support.StaticApplicationContext;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.util.ReflectionUtils;

/**
 * Unit tests for {@link RedisListenerEndpointRegistrar}.
 *
 * @author Ilyass Bougati
 * @author Mark Paluch
 */
@ExtendWith(MockitoExtension.class)
class RedisListenerEndpointRegistrarUnitTests {

	RedisListenerEndpointRegistrar registrar;

	@Mock RedisListenerEndpointRegistry registry;

	@Mock RedisMessageListenerContainer container;

	Method dummyMethod;
	Object dummyBean;

	@BeforeEach
	void setup() {
		this.registrar = new RedisListenerEndpointRegistrar();
		StaticApplicationContext context = new StaticApplicationContext();

		this.registrar.setBeanFactory(context.getBeanFactory());
		this.registrar.setEndpointRegistry(this.registry);

		this.dummyBean = new Object();
		this.dummyMethod = ReflectionUtils.findMethod(Object.class, "toString");
	}

	@Test
	void uninitializedRegistrarDefersRegistration() {

		MethodRedisListenerEndpoint endpoint = new MethodRedisListenerEndpoint(this.dummyBean, this.dummyMethod);
		endpoint.setId("test-endpoint-1");

		this.registrar.registerEndpoint(endpoint, container);

		verifyNoInteractions(this.registry);
		assertThat(this.registrar.getRedisListenerEndpointDescriptors()).hasSize(1);
	}

	@Test
	void registersQueuedRegistrations() {

		MethodRedisListenerEndpoint endpoint = new MethodRedisListenerEndpoint(this.dummyBean, this.dummyMethod);
		endpoint.setId("test-endpoint-2");
		this.registrar.registerEndpoint(endpoint, container);

		this.registrar.afterPropertiesSet();
		verify(this.registry).registerListener(eq(endpoint), eq(this.container));
	}

	@Test
	void registersEndpointsImmediately() {

		this.registrar.afterPropertiesSet();

		MethodRedisListenerEndpoint endpoint = new MethodRedisListenerEndpoint(this.dummyBean, this.dummyMethod);
		endpoint.setId("test-endpoint-3");

		this.registrar.registerEndpoint(endpoint, container);
		verify(this.registry).registerListener(eq(endpoint), eq(this.container));
	}

	@Test
	void requiresRedisListenerEndpointRegistry() {

		RedisListenerEndpointRegistrar badRegistrar = new RedisListenerEndpointRegistrar();

		assertThatIllegalStateException().isThrownBy(badRegistrar::afterPropertiesSet)
				.withMessageContaining("RedisListenerEndpointRegistry");
	}
}
