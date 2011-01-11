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

import java.util.ArrayList;
import java.util.Collection;

import org.springframework.data.keyvalue.redis.connection.MessageListener;
import org.springframework.data.keyvalue.redis.connection.Subscription;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

import redis.clients.jedis.JedisPubSub;

/**
 * Jedis specific subscription.
 * 
 * @author Costin Leau
 */
class JedisSubscription implements Subscription {

	private final MessageListener listener;
	private final JedisPubSub jedisPubSub;

	private final Collection<byte[]> channels = new ArrayList<byte[]>(2);
	private final Collection<byte[]> patterns = new ArrayList<byte[]>(2);

	JedisSubscription(MessageListener listener, JedisPubSub jedisPubSub, byte[][] channels, byte[][] patterns) {
		Assert.notNull(listener);
		this.listener = listener;
		this.jedisPubSub = jedisPubSub;

		if (!ObjectUtils.isEmpty(channels)) {
			for (byte[] bs : channels) {
				this.channels.add(bs);
			}
		}

		if (!ObjectUtils.isEmpty(patterns)) {
			for (byte[] bs : patterns) {
				this.patterns.add(bs);
			}
		}
	}

	@Override
	public Collection<byte[]> getChannels() {
		return channels;
	}

	@Override
	public MessageListener getListener() {
		return listener;
	}

	@Override
	public Collection<byte[]> getPatterns() {
		return patterns;
	}

	@Override
	public void pSubscribe(byte[]... patterns) {
		Assert.notEmpty(patterns, "at least one pattern required");

		for (byte[] bs : patterns) {
			this.patterns.add(bs);
		}

		jedisPubSub.psubscribe(JedisUtils.convert(patterns));
	}

	@Override
	public void pUnsubscribe() {
		jedisPubSub.punsubscribe();
	}

	@Override
	public void pUnsubscribe(byte[]... patterns) {
		if (ObjectUtils.isEmpty(patterns)) {
			unsubscribe();
		}

		else {
			for (byte[] bs : patterns) {
				this.patterns.remove(bs);
			}

			jedisPubSub.punsubscribe(JedisUtils.convert(patterns));
		}
	}

	@Override
	public void subscribe(byte[]... channels) {
		Assert.notEmpty(patterns, "at least one pattern required");

		for (byte[] bs : patterns) {
			this.patterns.add(bs);
		}

		jedisPubSub.subscribe(JedisUtils.convert(channels));
	}

	@Override
	public void unsubscribe() {
		jedisPubSub.unsubscribe();
	}

	@Override
	public void unsubscribe(byte[]... channels) {
		if (ObjectUtils.isEmpty(channels)) {
			unsubscribe();
		}
		else {
			for (byte[] bs : patterns) {
				this.patterns.remove(bs);
			}

			jedisPubSub.unsubscribe(JedisUtils.convert(channels));
		}
	}

	@Override
	public boolean isAlive() {
		return jedisPubSub.isSubscribed();
	}
}