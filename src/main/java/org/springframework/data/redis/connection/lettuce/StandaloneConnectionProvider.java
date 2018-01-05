/*
 * Copyright 2017-2018 the original author or authors.
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
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.masterslave.MasterSlave;
import io.lettuce.core.masterslave.StatefulRedisMasterSlaveConnection;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.sentinel.api.StatefulRedisSentinelConnection;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.springframework.beans.DirectFieldAccessor;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionProvider.TargetAware;
import org.springframework.lang.Nullable;

/**
 * {@link LettuceConnectionProvider} implementation for a standalone Redis setup.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.0
 */
class StandaloneConnectionProvider implements LettuceConnectionProvider, TargetAware {

	private final RedisClient client;
	private final RedisCodec<?, ?> codec;
	private final Optional<ReadFrom> readFrom;
	private final Supplier<RedisURI> redisURISupplier;

	/**
	 * Create new {@link StandaloneConnectionProvider}.
	 *
	 * @param client must not be {@literal null}.
	 * @param codec must not be {@literal null}.
	 */
	StandaloneConnectionProvider(RedisClient client, RedisCodec<?, ?> codec) {
		this(client, codec, null);
	}

	/**
	 * Create new {@link StandaloneConnectionProvider}.
	 *
	 * @param client must not be {@literal null}.
	 * @param codec must not be {@literal null}.
	 * @param readFrom can be {@literal null}.
	 * @since 2.1
	 */
	StandaloneConnectionProvider(RedisClient client, RedisCodec<?, ?> codec, @Nullable ReadFrom readFrom) {

		this.client = client;
		this.codec = codec;
		this.readFrom = Optional.ofNullable(readFrom);

		redisURISupplier = new Supplier<RedisURI>() {

			AtomicReference<RedisURI> uriFieldReference = new AtomicReference();

			@Override
			public RedisURI get() {

				RedisURI uri = uriFieldReference.get();
				if (uri != null) {
					return uri;
				}

				uri = RedisURI.class.cast(new DirectFieldAccessor(client).getPropertyValue("redisURI"));

				return uriFieldReference.compareAndSet(null, uri) ? uri : uriFieldReference.get();
			}
		};
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnectionProvider#getConnection(java.lang.Class)
	 */
	@SuppressWarnings("null")
	@Override
	public <T extends StatefulConnection<?, ?>> T getConnection(Class<T> connectionType) {

		if (connectionType.equals(StatefulRedisSentinelConnection.class)) {
			return connectionType.cast(client.connectSentinel());
		}

		if (connectionType.equals(StatefulRedisPubSubConnection.class)) {
			return connectionType.cast(client.connectPubSub(codec));
		}

		if (StatefulConnection.class.isAssignableFrom(connectionType)) {

			return connectionType.cast(readFrom.map(it -> this.masterSlaveConnection(redisURISupplier.get(), it))
					.orElseGet(() -> client.connect(codec)));
		}

		throw new UnsupportedOperationException("Connection type " + connectionType + " not supported!");
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnectionProvider.TargetAware#getConnection(java.lang.Class, io.lettuce.core.RedisURI)
	 */
	@SuppressWarnings("null")
	@Override
	public <T extends StatefulConnection<?, ?>> T getConnection(Class<T> connectionType, RedisURI redisURI) {

		if (connectionType.equals(StatefulRedisSentinelConnection.class)) {
			return connectionType.cast(client.connectSentinel(redisURI));
		}

		if (connectionType.equals(StatefulRedisPubSubConnection.class)) {
			return connectionType.cast(client.connectPubSub(codec, redisURI));
		}

		if (StatefulConnection.class.isAssignableFrom(connectionType)) {

			return connectionType
					.cast(readFrom.map(it -> this.masterSlaveConnection(redisURI, it)).orElseGet(() -> client.connect(codec)));
		}

		throw new UnsupportedOperationException("Connection type " + connectionType + " not supported!");
	}

	private StatefulRedisConnection masterSlaveConnection(RedisURI redisUri, ReadFrom readFrom) {

		StatefulRedisMasterSlaveConnection<?, ?> connection = MasterSlave.connect(client, codec, redisUri);
		connection.setReadFrom(readFrom);

		return connection;
	}

}
