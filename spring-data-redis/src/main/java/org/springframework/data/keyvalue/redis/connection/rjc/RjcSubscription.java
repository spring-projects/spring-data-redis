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

import org.idevlab.rjc.Client;
import org.idevlab.rjc.message.RedisNodeSubscriber;
import org.springframework.data.keyvalue.redis.connection.MessageListener;
import org.springframework.data.keyvalue.redis.connection.util.AbstractSubscription;

/**
 * Message subscription on top of RJC.
 * 
 * @author Costin Leau
 */
class RjcSubscription extends AbstractSubscription {

	private final RedisNodeSubscriber subscriber;
	private final Client client;
	// rjc does not support subscription while listening
	// so we have to handle this ourselves through the client
	private volatile boolean subscribed = false;

	RjcSubscription(MessageListener listener, RedisNodeSubscriber subscriber, Client client) {
		super(listener);
		this.subscriber = subscriber;
		subscriber.setMessageListener(new RjcMessageListener(listener));
		subscriber.setPMessageListener(new RjcMessageListener(listener));
		this.client = client;
	}

	@Override
	protected void doClose() {
		subscribed = false;
		client.unsubscribe();
		client.punsubscribe();
		client.rollbackTimeout();
	}

	@Override
	protected void doPsubscribe(byte[]... patterns) {
		String[] pats = RjcUtils.decodeMultiple(patterns);
		
		if (subscribed) {
			client.psubscribe(pats);
		}
		else {
			subscriber.setPatterns(RjcUtils.addArray(subscriber.getPatterns(), pats));
			subscribed = true;
			subscriber.subscribe();
		}
	}

	@Override
	protected void doPUnsubscribe(boolean all, byte[]... patterns) {
		client.punsubscribe(RjcUtils.decodeMultiple(patterns));
	}

	@Override
	protected void doSubscribe(byte[]... channels) {
		String[] chs = RjcUtils.decodeMultiple(channels);

		if (subscribed) {
			client.subscribe(chs);
		}
		else {
			subscriber.setPatterns(RjcUtils.addArray(subscriber.getChannels(), chs));
			subscribed = true;
			subscriber.subscribe();
		}
	}

	@Override
	protected void doUnsubscribe(boolean all, byte[]... channels) {
		client.punsubscribe(RjcUtils.decodeMultiple(channels));
	}
}