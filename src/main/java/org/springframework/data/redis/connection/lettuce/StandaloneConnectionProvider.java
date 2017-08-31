/*
 * Copyright 2017 the original author or authors.
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

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.sentinel.api.StatefulRedisSentinelConnection;

import org.springframework.data.redis.connection.lettuce.LettuceConnectionProvider.TargetAware;

/**
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.0
 */
class StandaloneConnectionProvider implements LettuceConnectionProvider, TargetAware {

	private final RedisClient client;
	private final RedisCodec<?, ?> codec;

	StandaloneConnectionProvider(RedisClient client, RedisCodec<?, ?> codec) {

		this.client = client;
		this.codec = codec;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnectionProvider#getConnection(java.lang.Class)
	 */
	@Override
	public <T extends StatefulConnection<?, ?>> T getConnection(Class<T> connectionType) {
		if (connectionType.equals(StatefulRedisSentinelConnection.class)) {
			return connectionType.cast(client.connectSentinel());
		}

		if (connectionType.equals(StatefulRedisPubSubConnection.class)) {
			return connectionType.cast(client.connectPubSub(codec));
		}

		if (StatefulConnection.class.isAssignableFrom(connectionType)) {
			return connectionType.cast(client.connect(codec));
		}

		throw new UnsupportedOperationException("Connection type " + connectionType + " not supported!");
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnectionProvider.TargetAware#getConnection(java.lang.Class, io.lettuce.core.RedisURI)
	 */
	@Override
	public <T extends StatefulConnection<?, ?>> T getConnection(Class<T> connectionType, RedisURI redisURI) {

		if (connectionType.equals(StatefulRedisSentinelConnection.class)) {
			return connectionType.cast(client.connectSentinel(redisURI));
		}

		if (connectionType.equals(StatefulRedisPubSubConnection.class)) {
			return connectionType.cast(client.connectPubSub(codec, redisURI));
		}

		if (StatefulConnection.class.isAssignableFrom(connectionType)) {
			return connectionType.cast(client.connect(codec, redisURI));
		}

		throw new UnsupportedOperationException("Connection type " + connectionType + " not supported!");
	}
}
