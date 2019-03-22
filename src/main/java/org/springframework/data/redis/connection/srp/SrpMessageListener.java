/*
 * Copyright 2011-2016 the original author or authors.
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

package org.springframework.data.redis.connection.srp;

import org.springframework.data.redis.connection.DefaultMessage;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.util.Assert;

import redis.client.ReplyListener;

/**
 * MessageListener wrapper around SRP {@link ReplyListener}.
 * 
 * @author Costin Leau
 * @deprecated since 1.7. Will be removed in subsequent version.
 */
@Deprecated
class SrpMessageListener implements ReplyListener {

	private final MessageListener listener;

	SrpMessageListener(MessageListener listener) {
		Assert.notNull(listener, "message listener is required");
		this.listener = listener;
	}

	public void message(byte[] channel, byte[] message) {
		listener.onMessage(new DefaultMessage(channel, message), null);
	}

	public void pmessage(byte[] pattern, byte[] channel, byte[] message) {
		listener.onMessage(new DefaultMessage(channel, message), pattern);
	}

	public void psubscribed(byte[] arg0, int arg1) {}

	public void punsubscribed(byte[] arg0, int arg1) {}

	public void subscribed(byte[] arg0, int arg1) {}

	public void unsubscribed(byte[] arg0, int arg1) {}
}
