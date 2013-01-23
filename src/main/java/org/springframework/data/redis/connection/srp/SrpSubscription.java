/*
 * Copyright 2011-2013 the original author or authors.
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

package org.springframework.data.redis.connection.srp;

import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.util.AbstractSubscription;

import redis.client.RedisClient;
import redis.client.ReplyListener;

/**
 * Message subscription on top of SRP.
 * 
 * @author Costin Leau
 */
class SrpSubscription extends AbstractSubscription {

	private final RedisClient client;
	private final ReplyListener listener;

	SrpSubscription(MessageListener listener, RedisClient client) {
		super(listener);
		this.client = client;
		this.listener = new SrpMessageListener(listener);
		client.addListener(this.listener);
	}

	protected void doClose() {
		client.unsubscribe((Object[]) null);
		client.punsubscribe((Object[]) null);
		client.removeListener(this.listener);
	}


	protected void doPsubscribe(byte[]... patterns) {
		client.psubscribe((Object[]) patterns);
	}

	protected void doPUnsubscribe(boolean all, byte[]... patterns) {
		if (all) {
			client.punsubscribe((Object[]) null);
		}
		else {
			client.punsubscribe((Object[]) patterns);
		}
	}

	protected void doSubscribe(byte[]... channels) {
		client.subscribe((Object[]) channels);
	}

	protected void doUnsubscribe(boolean all, byte[]... channels) {
		if (all) {
			client.unsubscribe((Object[]) null);
		}
		else {
			client.unsubscribe((Object[]) channels);
		}
	}
}