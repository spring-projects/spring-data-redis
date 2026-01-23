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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.jspecify.annotations.Nullable;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.config.EmbeddedValueResolver;
import org.springframework.context.SmartLifecycle;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.PatternTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.listener.Topic;
import org.springframework.data.redis.listener.adapter.MessagingMessageListenerAdapter;
import org.springframework.messaging.handler.annotation.support.MessageHandlerMethodFactory;
import org.springframework.messaging.handler.invocation.InvocableHandlerMethod;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringValueResolver;

/**
 * A {@link RedisListenerEndpoint} providing the method invocation mechanism for a specific bean method.
 *
 * @author Ilyass Bougati
 * @see MessagingMessageListenerAdapter
 */
public class MethodRedisListenerEndpoint implements RedisListenerEndpoint, BeanFactoryAware, SmartLifecycle {

	private static final boolean messagingPresent = ClassUtils.isPresent(
			"org.springframework.messaging.handler.invocation.InvocableHandlerMethod",
			MethodRedisListenerEndpoint.class.getClassLoader());

	private final Object bean;
	private final Method method;
	private String id = "";
	private String[] channels = new String[0];
	private String[] patterns = new String[0];

	@Nullable private StringValueResolver embeddedValueResolver;
	@Nullable private MessageListener messageListener;
	@Nullable private RedisMessageListenerContainer listenerContainer;
	private boolean running = false;
	private final Object lifecycleMonitor = new Object();

	@Nullable private MessageHandlerMethodFactory messageHandlerMethodFactory;

	public MethodRedisListenerEndpoint(Object bean, Method method) {
		this.bean = bean;
		this.method = method;
	}

	@Override
	public void setupListenerContainer(RedisMessageListenerContainer listenerContainer) {
		this.listenerContainer = listenerContainer;

		Assert.state(messagingPresent,
				"spring-messaging is required to use @RedisListener. Please add it to your classpath.");

		Assert.state(this.messageHandlerMethodFactory != null,
				"Could not create message listener - MessageHandlerMethodFactory not set");

		InvocableHandlerMethod invocableHandlerMethod = this.messageHandlerMethodFactory
				.createInvocableHandlerMethod(this.bean, this.method);

		// Endpoint is now aware of its listener
		this.messageListener = new MessagingMessageListenerAdapter(invocableHandlerMethod);
	}

	private Collection<Topic> getTopics() {
		List<Topic> topics = new ArrayList<>();
		for (String channel : this.channels) {
			topics.add(new ChannelTopic(channel));
		}
		for (String pattern : this.patterns) {
			topics.add(new PatternTopic(pattern));
		}
		return topics;
	}

	protected MessagingMessageListenerAdapter createMessageListenerInstance() {
		Assert.state(this.messageHandlerMethodFactory != null,
				"Could not create message listener - MessageHandlerMethodFactory not set");

		InvocableHandlerMethod invocableHandlerMethod = this.messageHandlerMethodFactory
				.createInvocableHandlerMethod(this.bean, this.method);

		return new MessagingMessageListenerAdapter(invocableHandlerMethod);
	}

	@Override
	public void start() {
		synchronized (this.lifecycleMonitor) {
			if (!this.isRunning()) {
				Assert.state(this.listenerContainer != null, "ListenerContainer not initialized");
				Assert.state(this.messageListener != null, "MessageListener not initialized");

				this.listenerContainer.addMessageListener(this.messageListener, getTopics());
				this.running = true;
			}
		}
	}

	@Override
	public void stop() {
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

	private String resolve(String value) {
		if (this.embeddedValueResolver != null) {
			String resolved = this.embeddedValueResolver.resolveStringValue(value);
			org.springframework.util.Assert.notNull(resolved, "Resolved topic expression must not be null");
			return resolved;
		}
		return value;
	}

	@Override
	public void setBeanFactory(BeanFactory beanFactory) {
		if (beanFactory instanceof org.springframework.beans.factory.config.ConfigurableBeanFactory) {
			this.embeddedValueResolver = new EmbeddedValueResolver(
					(org.springframework.beans.factory.config.ConfigurableBeanFactory) beanFactory);
		}
	}

	@Override
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public void setChannels(String... channels) {
		this.channels = channels;
	}

	public void setPatterns(String... patterns) {
		this.patterns = patterns;
	}

	public void setMessageHandlerMethodFactory(MessageHandlerMethodFactory messageHandlerMethodFactory) {
		this.messageHandlerMethodFactory = messageHandlerMethodFactory;
	}

	public Object getBean() {
		return bean;
	}

	public Method getMethod() {
		return method;
	}
}
