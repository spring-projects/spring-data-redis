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

import java.lang.reflect.Method;

import org.jspecify.annotations.Nullable;

import org.springframework.aop.framework.AopProxyUtils;
import org.springframework.aop.support.AopUtils;
import org.springframework.data.redis.listener.adapter.HandlerMethodMessageListenerAdapter;
import org.springframework.messaging.handler.annotation.support.MessageHandlerMethodFactory;
import org.springframework.messaging.handler.invocation.InvocableHandlerMethod;
import org.springframework.util.Assert;

/**
 * A {@link RedisListenerEndpoint} providing the method invocation mechanism for a specific bean method.
 *
 * @author Ilyass Bougati
 * @author Mark Paluch
 * @since 4.1
 * @see HandlerMethodMessageListenerAdapter
 */
public class MethodRedisListenerEndpoint extends AbstractRedisListenerEndpoint {

	private final Object bean;

	private final Method method;

	private @Nullable Method mostSpecificMethod;

	private @Nullable String consumes;

	private @Nullable MessageHandlerMethodFactory messageHandlerMethodFactory;

	public MethodRedisListenerEndpoint(Object bean, Method method) {
		this.bean = bean;
		this.method = method;
	}

	public Object getBean() {
		return this.bean;
	}

	public Method getMethod() {
		return this.method;
	}

	/**
	 * Set the mime type the listener consumes.
	 */
	public void setConsumes(@Nullable String consumes) {
		this.consumes = consumes;
	}

	/**
	 * Set the most specific method known for this endpoint's declaration.
	 * <p>
	 * In case of a proxy, this will be the method on the target class (if annotated itself, that is, if not just
	 * annotated in an interface).
	 */
	public void setMostSpecificMethod(@Nullable Method mostSpecificMethod) {
		this.mostSpecificMethod = mostSpecificMethod;
	}

	public @Nullable Method getMostSpecificMethod() {
		if (this.mostSpecificMethod != null) {
			return this.mostSpecificMethod;
		}

		Method method = getMethod();
		Object bean = getBean();
		if (AopUtils.isAopProxy(bean)) {
			Class<?> targetClass = AopProxyUtils.ultimateTargetClass(bean);
			method = AopUtils.getMostSpecificMethod(method, targetClass);
		}

		return method;
	}

	/**
	 * Set the {@link MessageHandlerMethodFactory} to use to build the {@link InvocableHandlerMethod} responsible to
	 * manage the invocation of this endpoint.
	 */
	public void setMessageHandlerMethodFactory(MessageHandlerMethodFactory messageHandlerMethodFactory) {
		this.messageHandlerMethodFactory = messageHandlerMethodFactory;
	}


	@Override
	public HandlerMethodMessageListenerAdapter createListener() {

		Assert.state(this.messageHandlerMethodFactory != null, "MessageHandlerMethodFactory not set");
		InvocableHandlerMethod invocableHandlerMethod = this.messageHandlerMethodFactory
				.createInvocableHandlerMethod(this.bean, this.method);

		return new HandlerMethodMessageListenerAdapter(invocableHandlerMethod,
				this.consumes);
	}

	/**
	 * Return a description for this endpoint.
	 * <p>
	 * Available to subclasses, for inclusion in their {@code toString()} result.
	 */
	protected StringBuilder getEndpointDescription() {
		StringBuilder result = new StringBuilder();
		return result.append("' | bean='").append(this.bean).append(" | method='").append(this.method).append('\'');
	}

	@Override
	public String toString() {
		return getEndpointDescription().toString();
	}

}
