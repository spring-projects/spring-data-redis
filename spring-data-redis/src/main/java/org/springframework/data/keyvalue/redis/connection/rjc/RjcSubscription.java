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
package org.springframework.data.keyvalue.redis.connection.rjc;

import java.util.ArrayList;
import java.util.Collection;

import org.idevlab.rjc.message.RedisSubscriber;
import org.springframework.data.keyvalue.redis.connection.MessageListener;
import org.springframework.data.keyvalue.redis.connection.Subscription;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

/**
 * Message subscription on top of RJC.
 * 
 * @author Costin Leau
 */
class RjcSubscription implements Subscription {

	private final MessageListener listener;
	private final RedisSubscriber subscriber;
	private final RjcMessageListener listenerAdapter;

	private final Collection<byte[]> channels = new ArrayList<byte[]>(2);
	private final Collection<byte[]> patterns = new ArrayList<byte[]>(2);

	RjcSubscription(MessageListener listener, RedisSubscriber subscriber) {
		Assert.notNull(listener);
		this.listener = listener;
		this.subscriber = subscriber;
		this.listenerAdapter = new RjcMessageListener(listener);
	}

	@Override
	public Collection<byte[]> getChannels() {
		synchronized (channels) {
			return new ArrayList<byte[]>(channels);
		}
	}

	@Override
	public MessageListener getListener() {
		return listener;
	}

	@Override
	public Collection<byte[]> getPatterns() {
		synchronized (patterns) {
			return new ArrayList<byte[]>(patterns);
		}
	}

	@Override
	public void pSubscribe(byte[]... patterns) {
		Assert.notEmpty(patterns, "at least one pattern required");

		synchronized (this.patterns) {
			for (byte[] bs : patterns) {
				this.patterns.add(bs);
			}
		}

		for (String pattern : RjcUtils.decodeMultiple(patterns)) {
			subscriber.psubscribe(pattern, listenerAdapter);
		}
	}

	@Override
	public void pUnsubscribe() {
		pUnsubscribe(null);

		synchronized (patterns) {
			patterns.clear();
		}
	}

	@Override
	public void pUnsubscribe(byte[]... patterns) {
		if (ObjectUtils.isEmpty(patterns)) {
			patterns = this.patterns.toArray(new byte[this.patterns.size()][]);
		}

		synchronized (this.patterns) {
			for (byte[] bs : patterns) {
				this.patterns.remove(bs);
			}
		}

		subscriber.punsubscribe(RjcUtils.decodeMultiple(patterns));
	}

	@Override
	public void subscribe(byte[]... channels) {
		Assert.notEmpty(channels, "at least one channel required");

		synchronized (this.channels) {
			for (byte[] bs : channels) {
				this.channels.add(bs);
			}
		}

		for (String channel : RjcUtils.decodeMultiple(channels)) {
			subscriber.subscribe(channel, listenerAdapter);
		}
	}

	@Override
	public void unsubscribe() {
		unsubscribe(null);

		synchronized (patterns) {
			patterns.clear();
		}
	}

	@Override
	public void unsubscribe(byte[]... channels) {
		if (ObjectUtils.isEmpty(channels)) {
			channels = this.channels.toArray(new byte[this.channels.size()][]);
		}

		synchronized (this.channels) {
			for (byte[] bs : channels) {
				this.channels.remove(bs);
			}
		}

		subscriber.punsubscribe(RjcUtils.decodeMultiple(channels));
	}

	@Override
	public boolean isAlive() {
		return (!channels.isEmpty() || !patterns.isEmpty());
	}
}