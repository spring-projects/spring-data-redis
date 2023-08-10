/*
 * Copyright 2015-2023 the original author or authors.
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
 * <p>
 * For development-time convenience the {@link #setKeyspaceNotificationsConfigParameter(String)} is set to
 * {@literal "Ex"}, by default. However, it is strongly recommended that users specifically set
 * {@literal notify-keyspace-events} to the appropriate value on the Redis server, in {@literal redis.conf}.
 * <p>
 * Any Redis server configuration coming from your Spring (Data Redis) application only occurs during Spring container
 * initialization, and is not persisted across Redis server restarts.
 *
 * @author Christoph Strobl
 * @author John Blum
 * @since 1.7
 */
public class KeyExpirationEventMessageListener extends KeyspaceEventMessageListener implements
		ApplicationEventPublisherAware {

	private static final String EXPIRED_KEY_EVENTS = "Ex";

	private static final Topic KEYEVENT_EXPIRED_TOPIC = new PatternTopic("__keyevent@*__:expired");

	private @Nullable ApplicationEventPublisher publisher;

	/**
	 * Creates new {@link MessageListener} for {@code __keyevent@*__:expired} messages and configures notification on
	 * expired keys ({@literal Ex}).
	 *
	 * @param listenerContainer must not be {@literal null}.
	 */
	public KeyExpirationEventMessageListener(RedisMessageListenerContainer listenerContainer) {
		super(listenerContainer, EXPIRED_KEY_EVENTS);
	}

	@Override
	protected void doRegister(RedisMessageListenerContainer listenerContainer) {
		listenerContainer.addMessageListener(this, KEYEVENT_EXPIRED_TOPIC);
	}

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

	@Override
	public void setApplicationEventPublisher(ApplicationEventPublisher applicationEventPublisher) {
		this.publisher = applicationEventPublisher;
	}
}
