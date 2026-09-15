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

import org.springframework.data.redis.connection.SubscriptionListener;

/**
 * {@link SubscriptionListener} forwarding all callbacks to a {@link #delegate() delegate}.
 *
 * @author Moritz Halbritter
 * @since 4.2
 */
interface DelegatingSubscriptionListener extends SubscriptionListener {

	/**
	 * Return the listener to forward callbacks to.
	 */
	SubscriptionListener delegate();

	@Override
	default void onChannelSubscribed(byte[] channel, long count) {
		delegate().onChannelSubscribed(channel, count);
	}

	@Override
	default void onChannelUnsubscribed(byte[] channel, long count) {
		delegate().onChannelUnsubscribed(channel, count);
	}

	@Override
	default void onPatternSubscribed(byte[] pattern, long count) {
		delegate().onPatternSubscribed(pattern, count);
	}

	@Override
	default void onPatternUnsubscribed(byte[] pattern, long count) {
		delegate().onPatternUnsubscribed(pattern, count);
	}

}
