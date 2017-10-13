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

import io.lettuce.core.ReadFrom;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import lombok.RequiredArgsConstructor;

import java.util.Optional;

/**
 * Connection provider for Cluster connections.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.0
 */
@RequiredArgsConstructor
class ClusterConnectionProvider implements LettuceConnectionProvider {

	private final RedisClusterClient client;
	private final RedisCodec<?, ?> codec;
	private final Optional<ReadFrom> readFrom;

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnectionProvider#getConnection(java.lang.Class)
	 */
	@Override
	public <T extends StatefulConnection<?, ?>> T getConnection(Class<T> connectionType) {

		if (connectionType.equals(StatefulRedisPubSubConnection.class)) {
			return connectionType.cast(client.connectPubSub(codec));
		}

		if (StatefulRedisClusterConnection.class.isAssignableFrom(connectionType)
				|| connectionType.equals(StatefulConnection.class)) {

			StatefulRedisClusterConnection<?, ?> connection = client.connect(codec);
			readFrom.ifPresent(connection::setReadFrom);

			return connectionType.cast(connection);
		}

		throw new UnsupportedOperationException("Connection type " + connectionType + " not supported!");
	}

	public RedisClusterClient getClient() {
		return client;
	}
}
