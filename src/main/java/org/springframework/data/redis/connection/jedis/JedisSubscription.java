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
package org.springframework.data.redis.connection.jedis;

import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.util.AbstractSubscription;

import redis.clients.jedis.BinaryJedisPubSub;

/**
 * Jedis specific subscription.
 * 
 * @author Costin Leau
 */
class JedisSubscription extends AbstractSubscription {

	private final BinaryJedisPubSub jedisPubSub;

	JedisSubscription(MessageListener listener, BinaryJedisPubSub jedisPubSub, byte[][] channels, byte[][] patterns) {
		super(listener, channels, patterns);
		this.jedisPubSub = jedisPubSub;
	}

	@Override
	protected void doClose() {
		jedisPubSub.unsubscribe();
		jedisPubSub.punsubscribe();
	}

	@Override
	protected void doPsubscribe(byte[]... patterns) {
		jedisPubSub.psubscribe(patterns);
	}

	@Override
	protected void doPUnsubscribe(boolean all, byte[]... patterns) {
		if (all) {
			jedisPubSub.punsubscribe();
		}
		else {
			jedisPubSub.punsubscribe(patterns);
		}
	}

	@Override
	protected void doSubscribe(byte[]... channels) {
		jedisPubSub.subscribe(channels);
	}

	@Override
	protected void doUnsubscribe(boolean all, byte[]... channels) {
		if (all) {
			jedisPubSub.unsubscribe();
		}
		else {
			jedisPubSub.unsubscribe(channels);
		}
	}
}