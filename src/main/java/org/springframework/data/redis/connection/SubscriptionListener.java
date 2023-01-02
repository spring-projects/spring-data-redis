/*
 * Copyright 2021-2023 the original author or authors.
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
package org.springframework.data.redis.connection;

/**
 * Listener for subscription notifications.
 * <p>
 * Subscription notifications are reported by Redis as confirmation for subscribe and unsubscribe operations for
 * channels and patterns.
 *
 * @author Mark Paluch
 * @since 2.6
 */
public interface SubscriptionListener {

	/**
	 * Empty {@link SubscriptionListener}.
	 */
	SubscriptionListener NO_OP_SUBSCRIPTION_LISTENER = new SubscriptionListener() {};

	/**
	 * Notification when Redis has confirmed a channel subscription.
	 *
	 * @param channel name of the channel.
	 * @param count subscriber count.
	 */
	default void onChannelSubscribed(byte[] channel, long count) {}

	/**
	 * Notification when Redis has confirmed a channel un-subscription.
	 *
	 * @param channel name of the channel.
	 * @param count subscriber count.
	 */
	default void onChannelUnsubscribed(byte[] channel, long count) {}

	/**
	 * Notification when Redis has confirmed a pattern subscription.
	 *
	 * @param pattern the pattern.
	 * @param count subscriber count.
	 */
	default void onPatternSubscribed(byte[] pattern, long count) {}

	/**
	 * Notification when Redis has confirmed a pattern un-subscription.
	 *
	 * @param pattern the pattern.
	 * @param count subscriber count.
	 */
	default void onPatternUnsubscribed(byte[] pattern, long count) {}

}
