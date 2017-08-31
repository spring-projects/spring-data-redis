/*
 * Copyright 2016-2017 the original author or authors.
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

import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.reactive.BaseRedisReactiveCommands;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.reactive.RedisClusterReactiveCommands;
import io.lettuce.core.codec.RedisCodec;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.util.function.Function;

import org.reactivestreams.Publisher;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.InvalidDataAccessResourceUsageException;
import org.springframework.data.redis.connection.*;
import org.springframework.util.Assert;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.0
 */
class LettuceReactiveRedisConnection implements ReactiveRedisConnection {

	static final RedisCodec<ByteBuffer, ByteBuffer> CODEC = ByteBufferCodec.INSTANCE;

	private final LettuceConnectionProvider connectionProvider;

	private StatefulConnection<ByteBuffer, ByteBuffer> connection;

	/**
	 * Creates new {@link LettuceReactiveRedisConnection}.
	 *
	 * @param connectionProvider must not be {@literal null}.
	 * @throws IllegalArgumentException when {@code client} is {@literal null}.
	 * @throws InvalidDataAccessResourceUsageException when {@code client} is not suitable for connection.
	 */
	LettuceReactiveRedisConnection(LettuceConnectionProvider connectionProvider) {

		Assert.notNull(connectionProvider, "LettuceConnectionProvider must not be null!");

		this.connectionProvider = connectionProvider;
		this.connection = connectionProvider.getConnection(StatefulConnection.class);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisConnection#keyCommands()
	 */
	@Override
	public ReactiveKeyCommands keyCommands() {
		return new LettuceReactiveKeyCommands(this);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisConnection#stringCommands()
	 */
	@Override
	public ReactiveStringCommands stringCommands() {
		return new LettuceReactiveStringCommands(this);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisConnection#numberCommands()
	 */
	@Override
	public ReactiveNumberCommands numberCommands() {
		return new LettuceReactiveNumberCommands(this);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisConnection#listCommands()
	 */
	@Override
	public ReactiveListCommands listCommands() {
		return new LettuceReactiveListCommands(this);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisConnection#setCommands()
	 */
	@Override
	public ReactiveSetCommands setCommands() {
		return new LettuceReactiveSetCommands(this);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisConnection#zSetCommands()
	 */
	@Override
	public ReactiveZSetCommands zSetCommands() {
		return new LettuceReactiveZSetCommands(this);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisConnection#hashCommands()
	 */
	@Override
	public ReactiveHashCommands hashCommands() {
		return new LettuceReactiveHashCommands(this);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisConnection#geoCommands()
	 */
	@Override
	public ReactiveGeoCommands geoCommands() {
		return new LettuceReactiveGeoCommands(this);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisConnection#hyperLogLogCommands()
	 */
	@Override
	public ReactiveHyperLogLogCommands hyperLogLogCommands() {
		return new LettuceReactiveHyperLogLogCommands(this);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisConnection#scriptingCommands()
	 */
	@Override
	public ReactiveScriptingCommands scriptingCommands() {
		return new LettuceReactiveScriptingCommands(this);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisConnection#hyperLogLogCommands()
	 */
	@Override
	public ReactiveServerCommands serverCommands() {
		return new LettuceReactiveServerCommands(this);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisConnection#ping()
	 */
	@Override
	public Mono<String> ping() {
		return execute(BaseRedisReactiveCommands::ping).next();
	}

	/**
	 * @param callback
	 * @return
	 */
	public <T> Flux<T> execute(LettuceReactiveCallback<T> callback) {
		return Flux.defer(() -> callback.doWithCommands(getCommands())).onErrorMap(translateException());
	}

	/* (non-Javadoc)
	 * @see java.io.Closeable#close()
	 */
	@Override
	public void close() {

		synchronized (connectionProvider) {
			connectionProvider.release(connection);
			connection = null;
		}
	}

	protected StatefulConnection<ByteBuffer, ByteBuffer> getConnection() {
		return connection;
	}

	protected RedisClusterReactiveCommands<ByteBuffer, ByteBuffer> getCommands() {

		if (connection instanceof StatefulRedisConnection) {
			return ((StatefulRedisConnection<ByteBuffer, ByteBuffer>) connection).reactive();
		} else if (connection instanceof StatefulRedisClusterConnection) {
			return ((StatefulRedisClusterConnection<ByteBuffer, ByteBuffer>) connection).reactive();
		}

		throw new RuntimeException("o.O unknown connection type " + connection);
	}

	<T> Function<Throwable, Throwable> translateException() {

		return throwable -> {

			if (throwable instanceof RuntimeException) {

				DataAccessException convertedException = LettuceConverters.exceptionConverter()
						.convert((RuntimeException) throwable);
				return convertedException != null ? convertedException : throwable;
			}

			return throwable;
		};
	}

	interface LettuceReactiveCallback<T> {
		Publisher<T> doWithCommands(RedisClusterReactiveCommands<ByteBuffer, ByteBuffer> cmd);
	}

	enum ByteBufferCodec implements RedisCodec<ByteBuffer, ByteBuffer> {

		INSTANCE;

		@Override
		public ByteBuffer decodeKey(ByteBuffer bytes) {

			ByteBuffer buffer = ByteBuffer.allocate(bytes.remaining());
			buffer.put(bytes);
			buffer.flip();
			return buffer;
		}

		@Override
		public ByteBuffer decodeValue(ByteBuffer bytes) {
			return decodeKey(bytes);
		}

		@Override
		public ByteBuffer encodeKey(ByteBuffer key) {
			return key.duplicate();
		}

		@Override
		public ByteBuffer encodeValue(ByteBuffer value) {
			return value.duplicate();
		}
	}
}
