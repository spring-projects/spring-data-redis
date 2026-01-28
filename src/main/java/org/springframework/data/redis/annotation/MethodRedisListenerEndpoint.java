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

import org.jspecify.annotations.NonNull;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.config.EmbeddedValueResolver;
import org.springframework.data.redis.config.RedisListenerEndpoint;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.PatternTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.jspecify.annotations.Nullable;
import org.springframework.util.StringValueResolver;

/**
 * A {@link RedisListenerEndpoint} providing the method invocation mechanism for a specific
 * bean method.
 *
 * @author Ilyass Bougati
 * @see MessagingMessageListenerAdapter
 */
public class MethodRedisListenerEndpoint implements RedisListenerEndpoint, BeanFactoryAware {

	private Object bean;
	private Method method;
	private BeanFactory beanFactory;
	private String id;
	private String[] topics;
	private String[] topicPatterns;
	@Nullable private StringValueResolver embeddedValueResolver;

	@Override
	public void setupListenerContainer(@NonNull RedisMessageListenerContainer listenerContainer) {
		MessagingMessageListenerAdapter adapter = createMessageListenerInstance();
		adapter.setHandlerMethod(bean, method);

		// Register Topics
		if (this.topics != null) {
			for (String topic : this.topics) {
				String resolvedTopic = resolve(topic);
				listenerContainer.addMessageListener(adapter, new ChannelTopic(resolvedTopic));
			}
		}

		// Register Patterns
		if (this.topicPatterns != null) {
			for (String pattern : this.topicPatterns) {
				String resolvedPattern = resolve(pattern);
				listenerContainer.addMessageListener(adapter, new PatternTopic(resolvedPattern));
			}
		}
	}

	protected MessagingMessageListenerAdapter createMessageListenerInstance() {
		return new MessagingMessageListenerAdapter();
	}

	private String resolve(String value) {
		if (this.embeddedValueResolver != null) {
			return this.embeddedValueResolver.resolveStringValue(value);
		}
		return value;
	}

	@Override
	public void setBeanFactory(@NonNull BeanFactory beanFactory) {
		this.beanFactory = beanFactory;
		if (beanFactory instanceof org.springframework.beans.factory.config.ConfigurableBeanFactory) {
			this.embeddedValueResolver = new EmbeddedValueResolver(
					(org.springframework.beans.factory.config.ConfigurableBeanFactory) beanFactory);
		}
	}

	public void setBean(Object bean) {
		this.bean = bean;
	}

	public void setMethod(Method method) {
		this.method = method;
	}

	public void setId(String id) {
		this.id = id;
	}

	public void setTopics(String... topics) {
		this.topics = topics;
	}

	public void setTopicPatterns(String... topicPatterns) {
		this.topicPatterns = topicPatterns;
	}

	@Override
	public @NonNull String getId() {
		return id;
	}
}
