/*
 * Copyright 2011 the original author or authors.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.keyvalue.redis.connection.jedis;

import org.springframework.data.keyvalue.redis.connection.DefaultMessage;
import org.springframework.data.keyvalue.redis.connection.MessageListener;
import org.springframework.util.Assert;

import redis.clients.jedis.JedisPubSub;

/**
 * MessageListener adapter on top of Jedis.
 * 
 * @author Costin Leau
 */
class JedisMessageListener extends JedisPubSub {

	private final MessageListener listener;

	JedisMessageListener(MessageListener listener) {
		Assert.notNull(listener, "message listener is required");
		this.listener = listener;
	}

	@Override
	public void onMessage(String channel, String message) {
		listener.onMessage(new DefaultMessage(channel.getBytes(), message.getBytes()), null);
	}

	@Override
	public void onPMessage(String pattern, String channel, String message) {
		listener.onMessage(new DefaultMessage(channel.getBytes(), message.getBytes()), pattern.getBytes());
	}

	@Override
	public void onPSubscribe(String pattern, int subscribedChannels) {
		// no-op
	}

	@Override
	public void onPUnsubscribe(String pattern, int subscribedChannels) {
		// no-op	
	}

	@Override
	public void onSubscribe(String channel, int subscribedChannels) {
		// no-op
	}

	@Override
	public void onUnsubscribe(String channel, int subscribedChannels) {
		// no-op	
	}
}