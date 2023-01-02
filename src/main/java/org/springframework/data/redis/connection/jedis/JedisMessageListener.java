/*
 * Copyright 2011-2023 the original author or authors.
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
package org.springframework.data.redis.connection.jedis;

import redis.clients.jedis.BinaryJedisPubSub;

import org.springframework.data.redis.connection.DefaultMessage;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.SubscriptionListener;
import org.springframework.util.Assert;

/**
 * MessageListener adapter on top of Jedis.
 *
 * @author Costin Leau
 * @author Mark Paluch
 */
class JedisMessageListener extends BinaryJedisPubSub {

	private final MessageListener listener;
	private final SubscriptionListener subscriptionListener;

	JedisMessageListener(MessageListener listener) {

		Assert.notNull(listener, "MessageListener is required");

		this.listener = listener;
		this.subscriptionListener = listener instanceof SubscriptionListener ? (SubscriptionListener) listener
				: SubscriptionListener.NO_OP_SUBSCRIPTION_LISTENER;
	}

	public void onMessage(byte[] channel, byte[] message) {
		listener.onMessage(new DefaultMessage(channel, message), null);
	}

	public void onPMessage(byte[] pattern, byte[] channel, byte[] message) {
		listener.onMessage(new DefaultMessage(channel, message), pattern);
	}

	public void onPSubscribe(byte[] pattern, int subscribedChannels) {
		subscriptionListener.onPatternSubscribed(pattern, subscribedChannels);
	}

	public void onPUnsubscribe(byte[] pattern, int subscribedChannels) {
		subscriptionListener.onPatternUnsubscribed(pattern, subscribedChannels);
	}

	public void onSubscribe(byte[] channel, int subscribedChannels) {
		subscriptionListener.onChannelSubscribed(channel, subscribedChannels);
	}

	public void onUnsubscribe(byte[] channel, int subscribedChannels) {
		subscriptionListener.onChannelUnsubscribed(channel, subscribedChannels);
	}
}
