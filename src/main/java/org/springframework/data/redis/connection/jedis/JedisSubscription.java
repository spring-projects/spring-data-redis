/*
 * Copyright 2011-2018 the original author or authors.
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
package org.springframework.data.redis.connection.jedis;

import redis.clients.jedis.BinaryJedisPubSub;

import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.util.AbstractSubscription;
import org.springframework.lang.Nullable;

/**
 * Jedis specific subscription.
 *
 * @author Costin Leau
 */
class JedisSubscription extends AbstractSubscription {

	private final BinaryJedisPubSub jedisPubSub;

	JedisSubscription(MessageListener listener, BinaryJedisPubSub jedisPubSub, @Nullable byte[][] channels,
			@Nullable byte[][] patterns) {
		super(listener, channels, patterns);
		this.jedisPubSub = jedisPubSub;
	}

	protected void doClose() {
		if (!getChannels().isEmpty()) {
			jedisPubSub.unsubscribe();
		}
		if (!getPatterns().isEmpty()) {
			jedisPubSub.punsubscribe();
		}
	}

	protected void doPsubscribe(byte[]... patterns) {
		jedisPubSub.psubscribe(patterns);
	}

	protected void doPUnsubscribe(boolean all, byte[]... patterns) {
		if (all) {
			jedisPubSub.punsubscribe();
		} else {
			jedisPubSub.punsubscribe(patterns);
		}
	}

	protected void doSubscribe(byte[]... channels) {
		jedisPubSub.subscribe(channels);
	}

	protected void doUnsubscribe(boolean all, byte[]... channels) {
		if (all) {
			jedisPubSub.unsubscribe();
		} else {
			jedisPubSub.unsubscribe(channels);
		}
	}
}
