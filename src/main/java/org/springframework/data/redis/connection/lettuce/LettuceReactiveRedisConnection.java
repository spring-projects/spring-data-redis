/*
 * Copyright 2016-2018 the original author or authors.
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
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import org.reactivestreams.Publisher;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.InvalidDataAccessResourceUsageException;
import org.springframework.data.redis.connection.*;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.0
 */
class LettuceReactiveRedisConnection implements ReactiveRedisConnection {

	static final RedisCodec<ByteBuffer, ByteBuffer> CODEC = ByteBufferCodec.INSTANCE;

	private final AsyncConnect<StatefulConnection<ByteBuffer, ByteBuffer>> dedicatedConnection;
	private final AsyncConnect<StatefulRedisPubSubConnection<ByteBuffer, ByteBuffer>> pubSubConnection;

	private @Nullable Mono<StatefulConnection<ByteBuffer, ByteBuffer>> sharedConnection;

	/**
	 * Creates new {@link LettuceReactiveRedisConnection}.
	 *
	 * @param connectionProvider must not be {@literal null}.
	 * @throws IllegalArgumentException when {@code client} is {@literal null}.
	 * @throws InvalidDataAccessResourceUsageException when {@code client} is not suitable for connection.
	 */
	@SuppressWarnings("unchecked")
	LettuceReactiveRedisConnection(LettuceConnectionProvider connectionProvider) {

		Assert.notNull(connectionProvider, "LettuceConnectionProvider must not be null!");

		this.dedicatedConnection = new AsyncConnect(connectionProvider, StatefulConnection.class);
		this.pubSubConnection = new AsyncConnect(connectionProvider, StatefulRedisPubSubConnection.class);
	}

	/**
	 * Creates new {@link LettuceReactiveRedisConnection} given a shared {@link StatefulConnection connection}.
	 *
	 * @param sharedConnection must not be {@literal null}.
	 * @param connectionProvider must not be {@literal null}.
	 * @throws IllegalArgumentException when {@code client} is {@literal null}.
	 * @throws InvalidDataAccessResourceUsageException when {@code client} is not suitable for connection.
	 * @since 2.0.1
	 */
	@SuppressWarnings("unchecked")
	LettuceReactiveRedisConnection(StatefulConnection<ByteBuffer, ByteBuffer> sharedConnection,
			LettuceConnectionProvider connectionProvider) {

		Assert.notNull(sharedConnection, "Shared StatefulConnection must not be null!");
		Assert.notNull(connectionProvider, "LettuceConnectionProvider must not be null!");

		this.dedicatedConnection = new AsyncConnect(connectionProvider, StatefulConnection.class);
		this.pubSubConnection = new AsyncConnect(connectionProvider, StatefulRedisPubSubConnection.class);
		this.sharedConnection = Mono.just(sharedConnection);
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
	 * @see org.springframework.data.redis.connection.ReactiveRedisConnection#pubSubCommands()
	 */
	@Override
	public ReactivePubSubCommands pubSubCommands() {
		return new LettuceReactivePubSubCommands(this);
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
	 * @see org.springframework.data.redis.connection.ReactiveRedisConnection#serverCommands()
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
		return getCommands().flatMapMany(callback::doWithCommands).onErrorMap(translateException());
	}

	/**
	 * @param callback
	 * @return
	 * @since 2.0.1
	 */
	public <T> Flux<T> executeDedicated(LettuceReactiveCallback<T> callback) {
		return getDedicatedCommands().flatMapMany(callback::doWithCommands).onErrorMap(translateException());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisConnection#closeLater()
	 */
	public Mono<Void> closeLater() {
		return Mono.fromRunnable(() -> dedicatedConnection.close());
	}

	protected Mono<? extends StatefulConnection<ByteBuffer, ByteBuffer>> getConnection() {

		if (sharedConnection != null) {
			return sharedConnection;
		}

		return getDedicatedConnection();
	}

	protected Mono<StatefulConnection<ByteBuffer, ByteBuffer>> getDedicatedConnection() {
		return dedicatedConnection.getConnection().onErrorMap(translateException());
	}

	protected Mono<StatefulRedisPubSubConnection<ByteBuffer, ByteBuffer>> getPubSubConnection() {
		return pubSubConnection.getConnection().onErrorMap(translateException());
	}

	protected Mono<? extends RedisClusterReactiveCommands<ByteBuffer, ByteBuffer>> getCommands() {

		if (sharedConnection != null) {
			return sharedConnection.map(LettuceReactiveRedisConnection::getRedisClusterReactiveCommands);
		}

		return getDedicatedCommands();
	}

	protected Mono<? extends RedisClusterReactiveCommands<ByteBuffer, ByteBuffer>> getDedicatedCommands() {
		return dedicatedConnection.getConnection().map(LettuceReactiveRedisConnection::getRedisClusterReactiveCommands);
	}

	private static RedisClusterReactiveCommands<ByteBuffer, ByteBuffer> getRedisClusterReactiveCommands(
			StatefulConnection<ByteBuffer, ByteBuffer> connection) {

		if (connection instanceof StatefulRedisConnection) {
			return ((StatefulRedisConnection<ByteBuffer, ByteBuffer>) connection).reactive();
		} else if (connection instanceof StatefulRedisClusterConnection) {
			return ((StatefulRedisClusterConnection<ByteBuffer, ByteBuffer>) connection).reactive();
		}

		throw new IllegalStateException("o.O unknown connection type " + connection);
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

	/**
	 * Asynchronous connection utility. This utility has a lifecycle controlled by {@link #getConnection()} and
	 * {@link #close()} methods:
	 * <ol>
	 * <li>Initial state: Not connected. Calling {@link #getConnection()} will transition to the next state.</li>
	 * <li>Connection requested: First caller to {@link #getConnection()} initiates an asynchronous connect. Connection is
	 * accessible through the resulting {@link Mono}.</li>
	 * <li>Closing: Call to {@link #close()} initiates connection closing.</li>
	 * <li>Closed: Connection is closed.</li>
	 * </ol>
	 *
	 * @author Mark Paluch
	 * @author Christoph Strobl
	 * @since 2.0.1
	 */
	static class AsyncConnect<T extends StatefulConnection<?, ?>> {

		private final Mono<T> connectionPublisher;
		private final LettuceConnectionProvider connectionProvider;

		private AtomicReference<State> state = new AtomicReference<>(State.INITIAL);
		private volatile @Nullable CompletableFuture<T> connection;

		@SuppressWarnings("unchecked")
		AsyncConnect(LettuceConnectionProvider connectionProvider, Class<? extends T> connectionType) {

			Assert.notNull(connectionProvider, "LettuceConnectionProvider must not be null!");

			this.connectionProvider = connectionProvider;

			Mono<T> defer = Mono.defer(() -> Mono.<T> just(connectionProvider.getConnection(connectionType)));

			this.connectionPublisher = defer.subscribeOn(Schedulers.elastic());
		}

		/**
		 * Obtain a connection publisher. This method connects asynchronously by requesting a connection from
		 * {@link LettuceConnectionProvider} with non-blocking synchronization if called concurrently.
		 *
		 * @return never {@literal null}.
		 */
		Mono<T> getConnection() {

			if (state.get() == State.CLOSED) {
				throw new IllegalStateException("Unable to connect. Connection is closed!");
			}

			CompletableFuture<T> connection = this.connection;

			if (connection != null) {
				return Mono.fromCompletionStage(connection);
			}

			if (state.compareAndSet(State.INITIAL, State.CONNECTION_REQUESTED)) {
				this.connection = connectionPublisher.toFuture();
			}

			while (true) {

				connection = this.connection;
				if (connection != null) {
					return Mono.fromCompletionStage(connection);
				}

				Thread thread = Thread.currentThread();
				if (thread.isInterrupted()) {
					thread.interrupt();
					return Mono.error(new InterruptedException());
				}
			}
		}

		/**
		 * Close connection (blocking call).
		 */
		void close() {

			if (state.compareAndSet(State.CONNECTION_REQUESTED, State.CLOSING)) {
				getConnection().doOnSuccess(connectionProvider::release).doOnSuccess(it -> state.set(State.CLOSED)).block();
			}

			state.compareAndSet(State.INITIAL, State.CLOSED);
		}

		enum State {
			INITIAL, CONNECTION_REQUESTED, CLOSING, CLOSED;
		}
	}
}
