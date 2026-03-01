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
import org.springframework.context.SmartLifecycle;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.listener.Topic;
import org.springframework.data.redis.listener.adapter.HandlerMethodMessageListenerAdapter;
import org.springframework.data.redis.listener.support.SimpleTopicResolver;
import org.springframework.data.redis.listener.support.TopicResolver;
import org.springframework.messaging.handler.annotation.support.MessageHandlerMethodFactory;
import org.springframework.messaging.handler.invocation.InvocableHandlerMethod;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

/**
 * A {@link RedisListenerEndpoint} providing the method invocation mechanism for a specific bean method.
 *
 * @author Ilyass Bougati
 * @since 4.1
 * @see HandlerMethodMessageListenerAdapter
 */
public class MethodRedisListenerEndpoint implements RedisListenerEndpoint, SmartLifecycle {

	private static final boolean messagingPresent = ClassUtils.isPresent(
			"org.springframework.messaging.handler.invocation.InvocableHandlerMethod",
			MethodRedisListenerEndpoint.class.getClassLoader());

	private static final TopicResolver TOPIC_RESOLVER = new SimpleTopicResolver();

	private final Object lifecycleMonitor = new Object();

	private final Object bean;

	private final Method method;

	private @Nullable Method mostSpecificMethod;

	private String id = "";

	private @Nullable String topic;

	private @Nullable String consumes;

	private @Nullable MessageHandlerMethodFactory messageHandlerMethodFactory;

	private @Nullable MessageListener messageListener;

	private @Nullable RedisMessageListenerContainer listenerContainer;

	private boolean running = false;

	public MethodRedisListenerEndpoint(Object bean, Method method) {

		Assert.state(messagingPresent,
				"spring-messaging is required to use @RedisListener. Please add it to your classpath.");

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
	 * Set a custom id for this endpoint.
	 */
	public void setId(String id) {
		this.id = id;
	}

	/**
	 * Return the id of this endpoint (possibly generated).
	 */
	public String getId() {
		return this.id;
	}

	/**
	 * Set the name of the topic for this endpoint.
	 */
	public void setTopic(@Nullable String topic) {
		this.topic = topic;
	}

	public void setConsumes(@Nullable String consumes) {
		this.consumes = consumes;
	}

	/**
	 * Return the name of the topic for this endpoint.
	 */
	public @Nullable String getTopic() {
		return this.topic;
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
	public void register(RedisMessageListenerContainer listenerContainer) {

		Assert.state(this.messageHandlerMethodFactory != null, "MessageHandlerMethodFactory not set");

		this.listenerContainer = listenerContainer;
		this.messageListener = createListener();
	}

	public HandlerMethodMessageListenerAdapter createListener() {

		Assert.state(this.messageHandlerMethodFactory != null, "MessageHandlerMethodFactory not set");
		InvocableHandlerMethod invocableHandlerMethod = this.messageHandlerMethodFactory
				.createInvocableHandlerMethod(this.bean, this.method);

		HandlerMethodMessageListenerAdapter listener = new HandlerMethodMessageListenerAdapter(invocableHandlerMethod,
				this.consumes);

		// Endpoint is now aware of its listener
		return listener;
	}

	@Override
	public void start() {

		Assert.state(this.listenerContainer != null, "ListenerContainer not initialized");
		Assert.state(this.messageListener != null, "MessageListener not initialized");

		synchronized (this.lifecycleMonitor) {
			if (!this.isRunning()) {

				Topic topic = TOPIC_RESOLVER.resolveTopic(getTopic());
				this.listenerContainer.addMessageListener(this.messageListener, topic);
				this.running = true;
			}
		}
	}

	@Override
	public void stop() {

		Assert.state(this.listenerContainer != null, "ListenerContainer not initialized");
		Assert.state(this.messageListener != null, "MessageListener not initialized");

		synchronized (this.lifecycleMonitor) {
			if (this.isRunning()) {
				if (this.listenerContainer != null && this.messageListener != null) {
					this.listenerContainer.removeMessageListener(this.messageListener);
				}
				this.running = false;
			}
		}
	}

	@Override
	public boolean isRunning() {
		return this.running;
	}

	/**
	 * Return a description for this endpoint.
	 * <p>
	 * Available to subclasses, for inclusion in their {@code toString()} result.
	 */
	protected StringBuilder getEndpointDescription() {
		StringBuilder result = new StringBuilder();
		return result.append(getClass().getSimpleName()).append('[').append(this.id).append("] topic=").append(this.topic)
				.append("' | bean='").append(this.bean).append(" | method='").append(this.method).append('\'');
	}

	@Override
	public String toString() {
		return getEndpointDescription().toString();
	}

	public void setListenerContainer(@Nullable RedisMessageListenerContainer listenerContainer) {
		this.listenerContainer = listenerContainer;
	}

	public @Nullable MessageHandlerMethodFactory getMessageHandlerMethodFactory() {
		return messageHandlerMethodFactory;
	}
}
