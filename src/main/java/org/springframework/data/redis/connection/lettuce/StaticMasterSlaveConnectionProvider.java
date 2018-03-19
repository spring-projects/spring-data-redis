/*
 * Copyright 2018 the original author or authors.
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
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.masterslave.MasterSlave;
import io.lettuce.core.masterslave.StatefulRedisMasterSlaveConnection;

import java.util.Collection;
import java.util.Optional;

import org.springframework.lang.Nullable;

/**
 * {@link LettuceConnectionProvider} implementation for a static Master/Slave connection suitable for eg. AWS
 * ElastiCache with replicas setup.<br/>
 * Lettuce auto-discovers node roles from the static {@link RedisURI} collection.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.1
 */
class StaticMasterSlaveConnectionProvider implements LettuceConnectionProvider {

	private final RedisClient client;
	private final RedisCodec<?, ?> codec;
	private final Optional<ReadFrom> readFrom;
	private final Collection<RedisURI> nodes;

	/**
	 * Create new {@link StaticMasterSlaveConnectionProvider}.
	 *
	 * @param client must not be {@literal null}.
	 * @param codec must not be {@literal null}.
	 * @param nodes must not be {@literal null}.
	 * @param readFrom can be {@literal null}.
	 */
	StaticMasterSlaveConnectionProvider(RedisClient client, RedisCodec<?, ?> codec, Collection<RedisURI> nodes,
			@Nullable ReadFrom readFrom) {

		this.client = client;
		this.codec = codec;
		this.readFrom = Optional.ofNullable(readFrom);
		this.nodes = nodes;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnectionProvider#getConnection(java.lang.Class)
	 */
	@Override
	public <T extends StatefulConnection<?, ?>> T getConnection(Class<T> connectionType) {

		if (StatefulConnection.class.isAssignableFrom(connectionType)) {

			StatefulRedisMasterSlaveConnection<?, ?> connection = MasterSlave.connect(client, codec, nodes);
			readFrom.ifPresent(connection::setReadFrom);

			return connectionType.cast(connection);
		}

		throw new UnsupportedOperationException(String.format("Connection type %s not supported!", connectionType));
	}
}
