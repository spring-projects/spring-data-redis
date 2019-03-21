/*
 * Copyright 2011-2013 the original author or authors.
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

package org.springframework.data.redis.connection.lettuce;

import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.util.AbstractSubscription;

import com.lambdaworks.redis.pubsub.RedisPubSubConnection;

/**
 * Message subscription on top of Lettuce.
 * 
 * @author Costin Leau
 */
class LettuceSubscription extends AbstractSubscription {

	final RedisPubSubConnection<byte[], byte[]> pubsub;
	private LettuceMessageListener listener;

	LettuceSubscription(MessageListener listener, RedisPubSubConnection<byte[], byte[]> pubsubConnection) {
		super(listener);
		this.pubsub = pubsubConnection;
		this.listener = new LettuceMessageListener(listener);

		pubsub.addListener(this.listener);
	}

	protected void doClose() {
		if (!getChannels().isEmpty()) {
			pubsub.unsubscribe(new byte[0]);
		}
		if (!getPatterns().isEmpty()) {
			pubsub.punsubscribe(new byte[0]);
		}
		pubsub.removeListener(this.listener);
		pubsub.close();
	}

	protected void doPsubscribe(byte[]... patterns) {
		pubsub.psubscribe(patterns);
	}

	protected void doPUnsubscribe(boolean all, byte[]... patterns) {
		// lettuce doesn't automatically subscribe from all channels
		pubsub.punsubscribe(patterns);
	}

	protected void doSubscribe(byte[]... channels) {
		pubsub.subscribe(channels);
	}

	protected void doUnsubscribe(boolean all, byte[]... channels) {
		// lettuce doesn't automatically subscribe from all patterns
		pubsub.unsubscribe(channels);
	}
}
