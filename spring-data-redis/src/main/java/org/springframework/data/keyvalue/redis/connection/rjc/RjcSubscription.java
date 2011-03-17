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
	private final RjcMessageListener listenerAdapter;
	private final Object pubSubMonitor;

	RjcSubscription(MessageListener listener, RedisNodeSubscriber subscriber, Object pubSubMonitor) {
		super(listener);
		this.subscriber = subscriber;
		this.listenerAdapter = new RjcMessageListener(listener);
		this.pubSubMonitor = pubSubMonitor;
	}

	@Override
	protected void doClose() {
		try {
			subscriber.close();
		} finally {
			synchronized (pubSubMonitor) {
				pubSubMonitor.notifyAll();
			}
		}
	}

	@Override
	protected void doPsubscribe(byte[]... patterns) {
		for (String str : RjcUtils.decodeMultiple(patterns)) {
			subscriber.psubscribe(str, listenerAdapter);
		}
	}

	@Override
	protected void doPUnsubscribe(boolean all, byte[]... patterns) {
		for (String str : RjcUtils.decodeMultiple(patterns)) {
			subscriber.punsubscribe(str);
		}
	}

	@Override
	protected void doSubscribe(byte[]... channels) {
		for (String str : RjcUtils.decodeMultiple(channels)) {
			subscriber.subscribe(str, listenerAdapter);
		}
	}

	@Override
	protected void doUnsubscribe(boolean all, byte[]... channels) {
		for (String str : RjcUtils.decodeMultiple(channels)) {
			subscriber.unsubscribe(str);
		}
	}
}