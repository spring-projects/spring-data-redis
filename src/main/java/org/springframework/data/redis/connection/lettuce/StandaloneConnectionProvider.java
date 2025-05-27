/*
 * Copyright 2017-2025 the original author or authors.
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

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.ReadFrom;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.masterreplica.MasterReplica;
import io.lettuce.core.masterreplica.StatefulRedisMasterReplicaConnection;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.sentinel.api.StatefulRedisSentinelConnection;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.jspecify.annotations.Nullable;
import org.springframework.beans.DirectFieldAccessor;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionProvider.TargetAware;

/**
 * {@link LettuceConnectionProvider} implementation for a standalone Redis setup.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.0
 */
class StandaloneConnectionProvider implements LettuceConnectionProvider, TargetAware, RedisClientProvider {

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

			AtomicReference<RedisURI> uriFieldReference = new AtomicReference<>();

			@Override
			@SuppressWarnings("NullAway")
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

	@Override
	public <T extends StatefulConnection<?, ?>> T getConnection(Class<T> connectionType) {

		if (connectionType.equals(StatefulRedisSentinelConnection.class)) {
			return connectionType.cast(client.connectSentinel());
		}

		if (connectionType.equals(StatefulRedisPubSubConnection.class)) {
			return connectionType.cast(client.connectPubSub(codec));
		}

		if (StatefulConnection.class.isAssignableFrom(connectionType)) {

			return connectionType.cast(readFrom.map(it -> this.masterReplicaConnection(redisURISupplier.get(), it))
					.orElseGet(() -> client.connect(codec)));
		}

		throw new UnsupportedOperationException("Connection type " + connectionType + " not supported");
	}

	@Override
	public <T extends StatefulConnection<?, ?>> CompletionStage<T> getConnectionAsync(Class<T> connectionType) {
		return getConnectionAsync(connectionType, redisURISupplier.get());
	}

	@SuppressWarnings({ "null", "unchecked", "rawtypes" })
	@Override
	public <T extends StatefulConnection<?, ?>> CompletionStage<T> getConnectionAsync(Class<T> connectionType,
			RedisURI redisURI) {

		if (connectionType.equals(StatefulRedisSentinelConnection.class)) {
			return client.connectSentinelAsync(StringCodec.UTF8, redisURI).thenApply(connectionType::cast);
		}

		if (connectionType.equals(StatefulRedisPubSubConnection.class)) {
			return client.connectPubSubAsync(codec, redisURI).thenApply(connectionType::cast);
		}

		if (StatefulConnection.class.isAssignableFrom(connectionType)) {
			return readFrom.map(it -> this.masterReplicaConnectionAsync(redisURI, it)) //
					.orElseGet(() -> (CompletionStage) client.connectAsync(codec, redisURI)) //
					.thenApply(connectionType::cast);
		}

		return LettuceFutureUtils
				.failed(new UnsupportedOperationException("Connection type " + connectionType + " not supported"));
	}

	@Override
	public AbstractRedisClient getRedisClient() {
		return client;
	}

	private StatefulRedisConnection masterReplicaConnection(RedisURI redisUri, ReadFrom readFrom) {

		StatefulRedisMasterReplicaConnection<?, ?> connection = MasterReplica.connect(client, codec, redisUri);
		connection.setReadFrom(readFrom);

		return connection;
	}

	private CompletionStage<StatefulRedisConnection<?, ?>> masterReplicaConnectionAsync(RedisURI redisUri,
			ReadFrom readFrom) {

		CompletableFuture<? extends StatefulRedisMasterReplicaConnection<?, ?>> connection = MasterReplica
				.connectAsync(client,
				codec, redisUri);

		return connection.thenApply(conn -> {

			conn.setReadFrom(readFrom);

			return conn;
		});
	}
}
