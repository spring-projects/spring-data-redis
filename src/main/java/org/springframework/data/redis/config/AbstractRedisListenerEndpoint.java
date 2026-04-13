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
import org.springframework.util.StringUtils;

/**
 * Base model for a Redis listener endpoint.
 *
 * @author Ilyass Bougati
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 4.1
 */
public abstract class AbstractRedisListenerEndpoint implements RedisListenerEndpoint, SmartLifecycle {

	private static final TopicResolver TOPIC_RESOLVER = new SimpleTopicResolver();

	private final Object lifecycleMonitor = new Object();

	private String id = "";

	private @Nullable String topic;

	private @Nullable MessageListener messageListener;

	private @Nullable RedisMessageListenerContainer listenerContainer;

	private boolean running = false;

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

	/**
	 * Return the name of the topic for this endpoint.
	 */
	public @Nullable String getTopic() {
		return this.topic;
	}

	@Override
	public void register(RedisMessageListenerContainer listenerContainer) {

		this.listenerContainer = listenerContainer;
		this.messageListener = createListener();
	}

	/**
	 * Create the {@link MessageListener} for this endpoint.
	 */
	protected abstract @Nullable MessageListener createListener();

	@Override
	public void start() {

		Assert.state(this.listenerContainer != null, "ListenerContainer not initialized");
		Assert.state(this.messageListener != null, "MessageListener not initialized");

		synchronized (this.lifecycleMonitor) {
			if (!this.isRunning()) {

				String topicName = getTopic();
				Assert.hasText(topicName, "Topic must not be null or empty");

				Topic topic = TOPIC_RESOLVER.resolveTopic(topicName);
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
		return result.append(getClass().getSimpleName()).append('[').append(this.id).append("] topic=").append(this.topic);
	}

	@Override
	public String toString() {
		return getEndpointDescription().toString();
	}

}
