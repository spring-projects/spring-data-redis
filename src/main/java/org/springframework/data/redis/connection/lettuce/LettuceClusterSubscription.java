/*
 * Copyright 2011-2019 the original author or authors.
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
package org.springframework.data.redis.connection.lettuce;

import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import io.lettuce.core.cluster.pubsub.api.sync.RedisClusterPubSubCommands;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.data.redis.connection.ClusterMessageListener;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.util.AbstractSubscription;

/**
 * Message subscription on top of Lettuce cluster support.
 *
 * @author Bruce Cloud
 * @since 2.2
 */
class LettuceClusterSubscription extends AbstractSubscription {

	private final StatefulRedisClusterPubSubConnection<byte[], byte[]> connection;
	private final LettuceMessageListener listener;
	private final LettuceConnectionProvider connectionProvider;
	private final RedisClusterPubSubCommands<byte[], byte[]> pubsub;
	private final boolean listensForKeyspaceNotifications;
	
	private final Log log = LogFactory.getLog(getClass());
	
	LettuceClusterSubscription(MessageListener listener, StatefulRedisClusterPubSubConnection<byte[], byte[]> pubsubConnection,
			LettuceConnectionProvider connectionProvider) {

		super(listener);

		this.connection = pubsubConnection;
		
		this.listener = new LettuceMessageListener(listener);
		this.listensForKeyspaceNotifications = (listener instanceof ClusterMessageListener
			&& ((ClusterMessageListener)listener).listensForKeyspaceNotifications());
		if (listensForKeyspaceNotifications) pubsubConnection.setNodeMessagePropagation(true);
		
		this.connectionProvider = connectionProvider;
		this.pubsub = connection.sync();

		this.connection.addListener(this.listener);
	}

	protected StatefulRedisPubSubConnection<byte[], byte[]> getNativeConnection() {
		return connection;
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.util.AbstractSubscription#doClose()
	 */
	protected void doClose() {

		if (!getChannels().isEmpty()) {
			pubsub.unsubscribe(new byte[0]);
		}

		if (!getPatterns().isEmpty()) {
			pubsub.punsubscribe(new byte[0]);
		}

		connection.removeListener(this.listener);
		connectionProvider.release(connection);
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.util.AbstractSubscription#doPsubscribe(byte[][])
	 */
	protected void doPsubscribe(byte[]... patterns) {
		
		if (listensForKeyspaceNotifications) {
			pubsub.masters().commands().psubscribe(patterns);
		}
		else {
			pubsub.psubscribe(patterns);
		}
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.util.AbstractSubscription#doPUnsubscribe(boolean, byte[][])
	 */
	protected void doPUnsubscribe(boolean all, byte[]... patterns) {

		// ignore `all` flag as Lettuce unsubscribes from all patterns if none provided.
		pubsub.punsubscribe(patterns);
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.util.AbstractSubscription#doSubscribe(byte[][])
	 */
	protected void doSubscribe(byte[]... channels) {
		
		if (listensForKeyspaceNotifications) {
			pubsub.masters().commands().subscribe(channels);
		}
		else {
			pubsub.subscribe(channels);
		}
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.util.AbstractSubscription#doUnsubscribe(boolean, byte[][])
	 */
	protected void doUnsubscribe(boolean all, byte[]... channels) {

		// ignore `all` flag as Lettuce unsubscribes from all channels if none provided.
		pubsub.unsubscribe(channels);
	}

}
