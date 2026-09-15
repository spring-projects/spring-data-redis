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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jspecify.annotations.Nullable;

import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.SubscriptionListener;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.listener.Topic;

/**
 * {@link RedisListenerEndpointRegistry} that notifies listener beans implementing {@link SubscriptionListener} once per
 * topic instead of once per registered endpoint.
 *
 * @author Mark Paluch
 * @author Moritz Halbritter
 * @since 4.2
 */
public class SubscriptionForwardingEndpointRegistry extends RedisListenerEndpointRegistry {

	private final Map<Key, SubscriptionForwarder> forwarders = new LinkedHashMap<>();

	@Override
	public void registerListener(RedisListenerEndpoint endpoint, RedisMessageListenerContainer container) {

		super.registerListener(endpoint, container);

		if (!(endpoint instanceof MethodRedisListenerEndpoint methodEndpoint)) {
			return;
		}

		if (!(methodEndpoint.getBean() instanceof SubscriptionListener listener)) {
			return;
		}

		Key key = new Key(container, listener);
		synchronized (this.forwarders) {
			this.forwarders.computeIfAbsent(key, k -> new SubscriptionForwarder(listener)).endpoints.add(methodEndpoint);
		}
	}

	@Override
	public void start() {

		super.start();

		synchronized (this.forwarders) {
			this.forwarders.forEach((key, forwarder) -> key.container().addMessageListener(forwarder, forwarder.topics()));
		}
	}

	@Override
	public void stop() {

		super.stop();

		synchronized (this.forwarders) {
			this.forwarders.forEach((key, forwarder) -> key.container().removeMessageListener(forwarder));
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

	private static class SubscriptionForwarder implements MessageListener, SubscriptionListener {

		private final SubscriptionListener delegate;

		private final List<AbstractRedisListenerEndpoint> endpoints = new ArrayList<>();

		SubscriptionForwarder(SubscriptionListener delegate) {
			this.delegate = delegate;
		}

		Set<Topic> topics() {

			Set<Topic> topics = new LinkedHashSet<>();
			for (AbstractRedisListenerEndpoint endpoint : this.endpoints) {
				topics.add(endpoint.resolveTopic());
			}
			return topics;
		}

		@Override
		public void onMessage(Message message, byte @Nullable [] pattern) {}

		@Override
		public void onChannelSubscribed(byte[] channel, long count) {
			this.delegate.onChannelSubscribed(channel, count);
		}

		@Override
		public void onChannelUnsubscribed(byte[] channel, long count) {
			this.delegate.onChannelUnsubscribed(channel, count);
		}

		@Override
		public void onPatternSubscribed(byte[] pattern, long count) {
			this.delegate.onPatternSubscribed(pattern, count);
		}

		@Override
		public void onPatternUnsubscribed(byte[] pattern, long count) {
			this.delegate.onPatternUnsubscribed(pattern, count);
		}

	}

}
