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
package org.springframework.data.redis.connection.rjc;

import org.idevlab.rjc.message.RedisNodeSubscriber;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.util.AbstractSubscription;

/**
 * Message subscription on top of RJC.
 * 
 * @author Costin Leau
 */
class RjcSubscription extends AbstractSubscription {

	private final RedisNodeSubscriber subscriber;

	RjcSubscription(MessageListener listener, RedisNodeSubscriber subscriber) {
		super(listener);
		this.subscriber = subscriber;
		subscriber.setMessageListener(new RjcMessageListener(listener));
		subscriber.setPMessageListener(new RjcMessageListener(listener));
	}

	@Override
	protected void doClose() {
		subscriber.close();
	}

	@Override
	protected void doPsubscribe(byte[]... patterns) {
		subscriber.psubscribe(RjcUtils.decodeMultiple(patterns));
	}

	@Override
	protected void doPUnsubscribe(boolean all, byte[]... patterns) {
		subscriber.punsubscribe(RjcUtils.decodeMultiple(patterns));
	}

	@Override
	protected void doSubscribe(byte[]... channels) {
		subscriber.subscribe(RjcUtils.decodeMultiple(channels));
	}

	@Override
	protected void doUnsubscribe(boolean all, byte[]... channels) {
		subscriber.punsubscribe(RjcUtils.decodeMultiple(channels));
	}
}