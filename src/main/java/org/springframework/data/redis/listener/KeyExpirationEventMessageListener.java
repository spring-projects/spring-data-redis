/*
 * Copyright 2015-2020 the original author or authors.
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
package org.springframework.data.redis.listener;

import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.core.RedisKeyExpiredEvent;
import org.springframework.lang.Nullable;

/**
 * {@link MessageListener} publishing {@link RedisKeyExpiredEvent}s via {@link ApplicationEventPublisher} by listening
 * to Redis keyspace notifications for key expirations.
 *
 * @author Christoph Strobl
 * @since 1.7
 */
public class KeyExpirationEventMessageListener extends KeyspaceEventMessageListener
		implements ApplicationEventPublisherAware {

	private static final String KEYEVENT_EXPIRED_TOPIC_PATTERN = "__keyevent@%s__:expired";

	private @Nullable ApplicationEventPublisher publisher;
	private @Nullable Integer database;

	/**
	 * Creates new {@link MessageListener} for {@code __keyevent@*__:expired} messages.
	 *
	 * @param listenerContainer must not be {@literal null}.
	 */
	public KeyExpirationEventMessageListener(RedisMessageListenerContainer listenerContainer) {
		this(listenerContainer, null);
	}

	/**
	 * Creates new {@link MessageListener} for {@code __keyevent@database__:expired} messages.
	 *
	 * @param listenerContainer must not be {@literal null}.
	 * @param database the database index to listen to for keyspace notifications. Use {@literal null} or negative value
	 *          for all.
	 * @since 2.3
	 */
	public KeyExpirationEventMessageListener(RedisMessageListenerContainer listenerContainer,
			@Nullable Integer database) {

		super(listenerContainer);
		this.database = database;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.listener.KeyspaceEventMessageListener#doRegister(org.springframework.data.redis.listener.RedisMessageListenerContainer)
	 */
	@Override
	protected void doRegister(RedisMessageListenerContainer listenerContainer) {
		listenerContainer.addMessageListener(this, computeTopic(database()));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.listener.KeyspaceEventMessageListener#doHandleMessage(org.springframework.data.redis.connection.Message)
	 */
	@Override
	protected void doHandleMessage(Message message) {
		publishEvent(new RedisKeyExpiredEvent(message.getBody()));
	}

	/**
	 * Publish the event in case an {@link ApplicationEventPublisher} is set.
	 *
	 * @param event can be {@literal null}.
	 */
	protected void publishEvent(RedisKeyExpiredEvent event) {

		if (publisher != null) {
			this.publisher.publishEvent(event);
		}
	}

	/**
	 * Get the database index to listen to.
	 *
	 * @return can be {@literal null}.
	 * @since 2.3
	 */
	@Nullable
	public Integer database() {
		return this.database;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.context.ApplicationEventPublisherAware#setApplicationEventPublisher(org.springframework.context.ApplicationEventPublisher)
	 */
	@Override
	public void setApplicationEventPublisher(ApplicationEventPublisher applicationEventPublisher) {
		this.publisher = applicationEventPublisher;
	}

	/**
	 * Compute the {@link Topic} for the actual subscription.
	 *
	 * @param database can be {@literal null}.
	 * @return never {@literal null}.
	 */
	protected Topic computeTopic(@Nullable Integer database) {

		return new PatternTopic(
				String.format(KEYEVENT_EXPIRED_TOPIC_PATTERN, database != null && database >= 0 ? database : "*"));
	}
}
