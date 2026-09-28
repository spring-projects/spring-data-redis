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

import org.jspecify.annotations.Nullable;

import org.springframework.context.SmartLifecycle;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.listener.Topic;
import org.springframework.data.redis.listener.support.SimpleTopicResolver;
import org.springframework.data.redis.listener.support.TopicResolver;
import org.springframework.util.Assert;

/**
 * Base model for a Redis listener endpoint.
 *
 * @author Ilyass Bougati
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 4.1
 */
public abstract class AbstractRedisListenerEndpoint extends RedisListenerEndpointSupport
		implements RedisListenerEndpoint, SmartLifecycle {

	static final TopicResolver TOPIC_RESOLVER = new SimpleTopicResolver();

	private @Nullable String topic;

	/**
	 * Set the name of the topic for this endpoint.
	 */
	public void setTopic(@Nullable String topic) {
		this.topic = topic;
	}

	/**
	 * Return the name of the topic for this endpoint.
	 */
	public @Nullable String getTopic() {
		return this.topic;
	}

	/**
	 * Subscribe the listener to the {@link #getTopic() topic} of this endpoint.
	 *
	 * @param listenerContainer the container to subscribe with
	 * @param messageListener the listener created by {@link #createListener()}
	 * @since 4.2
	 */
	protected void subscribe(RedisMessageListenerContainer listenerContainer, MessageListener messageListener) {

		Topic topic = resolveTopic();
		listenerContainer.addMessageListener(messageListener, topic);
	}

	Topic resolveTopic() {
		String topicName = getTopic();
		Assert.hasText(topicName, "Topic must not be null or empty");

		return TOPIC_RESOLVER.resolveTopic(topicName);
	}

	/**
	 * Return a description for this endpoint.
	 * <p>
	 * Available to subclasses, for inclusion in their {@code toString()} result.
	 */
	protected StringBuilder getEndpointDescription() {
		StringBuilder result = new StringBuilder();
		return result.append(getClass().getSimpleName()).append('[').append(this.getId()).append("] topic=")
				.append(this.topic);
	}

}
