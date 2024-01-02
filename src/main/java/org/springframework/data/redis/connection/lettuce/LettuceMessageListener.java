/*
 * Copyright 2011-2024 the original author or authors.
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
package org.springframework.data.redis.connection.lettuce;

import io.lettuce.core.pubsub.RedisPubSubListener;

import org.springframework.data.redis.connection.DefaultMessage;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.SubscriptionListener;
import org.springframework.util.Assert;

/**
 * MessageListener wrapper around Lettuce {@link RedisPubSubListener}.
 *
 * @author Costin Leau
 * @author Mark Paluch
 */
class LettuceMessageListener implements RedisPubSubListener<byte[], byte[]> {

	private final MessageListener listener;
	private final SubscriptionListener subscriptionListener;

	LettuceMessageListener(MessageListener listener, SubscriptionListener subscriptionListener) {

		Assert.notNull(listener, "MessageListener must not be null");
		Assert.notNull(subscriptionListener, "SubscriptionListener must not be null");

		this.listener = listener;
		this.subscriptionListener = subscriptionListener;
	}

	public void message(byte[] channel, byte[] message) {
		listener.onMessage(new DefaultMessage(channel, message), null);
	}

	public void message(byte[] pattern, byte[] channel, byte[] message) {
		listener.onMessage(new DefaultMessage(channel, message), pattern);
	}

	public void subscribed(byte[] channel, long count) {
		subscriptionListener.onChannelSubscribed(channel, count);
	}

	public void psubscribed(byte[] pattern, long count) {
		subscriptionListener.onPatternSubscribed(pattern, count);
	}

	public void unsubscribed(byte[] channel, long count) {
		subscriptionListener.onChannelUnsubscribed(channel, count);
	}

	public void punsubscribed(byte[] pattern, long count) {
		subscriptionListener.onPatternUnsubscribed(pattern, count);
	}
}
