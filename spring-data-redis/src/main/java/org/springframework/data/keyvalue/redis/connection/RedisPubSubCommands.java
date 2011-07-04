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
package org.springframework.data.keyvalue.redis.connection;

/**
 * PubSub-specific Redis commands.
 * 
 * @author Costin Leau
 */
public interface RedisPubSubCommands {

	/**
	 * Indicates whether the current connection is subscribed (to at least one channel)
	 * or not.
	 * 
	 * @return true if the connection is subscribed, false otherwise
	 */
	boolean isSubscribed();

	/**
	 * Returns the current subscription for this connection or null if the connection is
	 * not subscribed.
	 * 
	 * @return the current subscription, null if none is available
	 */
	Subscription getSubscription();

	/**
	 * Publishes the given message to the given channel.
	 * 
	 * @param channel the channel to publish to
	 * @param message message to publish
	 * @return the number of clients that received the message
	 */
	Long publish(byte[] channel, byte[] message);

	/**
	 * Subscribes the connection to the given channels.
	 * Once subscribed, a connection
	 * enters listening mode and can only subscribe to other channels or unsubscribe.
	 * No other commands are accepted until the connection is unsubscribed.
	 * <p/>
	 * Note that this operation is blocking and the current thread starts waiting
	 * for new messages immediately.
	 * 
	 * @param listener message listener
	 * @param channels channel names
	 */
	void subscribe(MessageListener listener, byte[]... channels);

	/**
	 * Subscribes the connection to all channels matching the given patterns.
	 * Once subscribed, a connection
	 * enters listening mode and can only subscribe to other channels or unsubscribe.
	 * No other commands are accepted until the connection is unsubscribed.
	 * <p/>
	 * Note that this operation is blocking and the current thread starts waiting
	 * for new messages immediately.
	 * 
	 * @param listener message listener
	 * @param patterns channel name patterns
	 */
	void pSubscribe(MessageListener listener, byte[]... patterns);
}