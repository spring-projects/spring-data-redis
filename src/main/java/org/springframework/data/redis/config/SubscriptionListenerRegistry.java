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

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.jspecify.annotations.Nullable;

import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.SubscriptionListener;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.listener.Topic;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

/**
 * {@link RedisListenerEndpointRegistry} that notifies listener beans implementing {@link SubscriptionListener} once per
 * topic instead of once per registered endpoint.
 *
 * @author Mark Paluch
 * @since 4.2
 * @see SubscriptionListenerEndpoint
 */
public class SubscriptionListenerRegistry extends RedisListenerEndpointRegistry {

	private final MultiValueMap<Key, Topic> mapping = new LinkedMultiValueMap<>();

	@Override
	public void registerListener(RedisListenerEndpoint endpoint, RedisMessageListenerContainer container) {

		if (endpoint instanceof MethodRedisListenerEndpoint methodRedisListenerEndpoint) {
			super.registerListener(new MessageListenerWrapperEndpoint(methodRedisListenerEndpoint), container);
		} else {
			super.registerListener(endpoint, container);
		}

		if (!(endpoint instanceof SubscriptionListenerEndpoint)) {
			potentiallyAddSubscriptionListenerMapping(endpoint, container);
		}
	}

	private void potentiallyAddSubscriptionListenerMapping(RedisListenerEndpoint endpoint,
			RedisMessageListenerContainer container) {

		if (endpoint instanceof MethodRedisListenerEndpoint methodEndpoint
				&& methodEndpoint.getBean() instanceof SubscriptionListener listener) {

			Key key = new Key(container, listener);
			Topic topic = methodEndpoint.resolveTopic();

			this.mapping.add(key, topic);
		}
	}

	@Override
	public void start() {

		registerSubscriptionListeners();
		super.start();
	}

	private void registerSubscriptionListeners() {

		for (Map.Entry<Key, List<Topic>> entry : this.mapping.entrySet()) {

			Key key = entry.getKey();
			Collection<Topic> topics = entry.getValue();

			if (topics.isEmpty()) {
				continue;
			}

			SubscriptionListenerEndpoint endpoint = new SubscriptionListenerEndpoint(new HashSet<>(topics), key.listener());
			endpoint.setId(key.listener.getClass().getName());
			registerListener(endpoint, key.container());
		}
	}

	private record Key(RedisMessageListenerContainer container, SubscriptionListener listener) {

		@Override
		public boolean equals(@Nullable Object other) {
			return other instanceof Key key && this.container == key.container && this.listener == key.listener;
		}

		@Override
		public int hashCode() {
			return 31 * System.identityHashCode(this.container) + System.identityHashCode(this.listener);
		}

	}

	static class SubscriptionListenerEndpoint extends RedisListenerEndpointSupport
			implements MessageListener, SubscriptionListener {

		private final Collection<? extends Topic> topics;

		private final SubscriptionListener subscriptionListener;

		public SubscriptionListenerEndpoint(Collection<? extends Topic> topics, SubscriptionListener subscriptionListener) {
			this.topics = topics;
			this.subscriptionListener = subscriptionListener;
			setId(subscriptionListener.getClass().getName());
		}

		@Override
		protected @Nullable MessageListener createListener() {
			return this;
		}

		@Override
		protected void subscribe(RedisMessageListenerContainer listenerContainer, MessageListener messageListener) {
			listenerContainer.addMessageListener(messageListener, topics);
		}

		@Override
		public void onMessage(Message message, byte @Nullable [] pattern) {}

		@Override
		public void onChannelSubscribed(byte[] channel, long count) {
			subscriptionListener.onChannelSubscribed(channel, count);
		}

		@Override
		public void onChannelUnsubscribed(byte[] channel, long count) {
			subscriptionListener.onChannelUnsubscribed(channel, count);
		}

		@Override
		public void onPatternSubscribed(byte[] pattern, long count) {
			subscriptionListener.onPatternSubscribed(pattern, count);
		}

		@Override
		public void onPatternUnsubscribed(byte[] pattern, long count) {
			subscriptionListener.onPatternUnsubscribed(pattern, count);
		}

		@Override
		protected StringBuilder getEndpointDescription() {
			StringBuilder result = new StringBuilder();
			return result.append("' | subscriptionListener='").append(this.subscriptionListener);
		}

	}

}
