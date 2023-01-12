/*
 * Copyright 2016-2023 the original author or authors.
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

import static org.springframework.data.redis.connection.lettuce.LettuceReactiveRedisConnection.AsyncConnect.State.*;

import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.reactive.BaseRedisReactiveCommands;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.reactive.RedisClusterReactiveCommands;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
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

	private final Object mutex = new Object();

	private final AsyncConnect<StatefulConnection<ByteBuffer, ByteBuffer>> dedicatedConnection;
	private final AsyncConnect<StatefulRedisPubSubConnection<ByteBuffer, ByteBuffer>> pubSubConnection;

	private volatile LettuceReactivePubSubCommands pubSub;

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

		Assert.notNull(connectionProvider, "LettuceConnectionProvider must not be null");

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

		Assert.notNull(sharedConnection, "Shared StatefulConnection must not be null");
		Assert.notNull(connectionProvider, "LettuceConnectionProvider must not be null");

		this.dedicatedConnection = new AsyncConnect(connectionProvider, StatefulConnection.class);
		this.pubSubConnection = new AsyncConnect(connectionProvider, StatefulRedisPubSubConnection.class);
		this.sharedConnection = Mono.just(sharedConnection);
	}

	@Override
	public ReactiveKeyCommands keyCommands() {
		return new LettuceReactiveKeyCommands(this);
	}

	@Override
	public ReactiveStringCommands stringCommands() {
		return new LettuceReactiveStringCommands(this);
	}

	@Override
	public ReactiveNumberCommands numberCommands() {
		return new LettuceReactiveNumberCommands(this);
	}

	@Override
	public ReactiveListCommands listCommands() {
		return new LettuceReactiveListCommands(this);
	}

	@Override
	public ReactiveSetCommands setCommands() {
		return new LettuceReactiveSetCommands(this);
	}

	@Override
	public ReactiveZSetCommands zSetCommands() {
		return new LettuceReactiveZSetCommands(this);
	}

	@Override
	public ReactiveHashCommands hashCommands() {
		return new LettuceReactiveHashCommands(this);
	}

	@Override
	public ReactiveGeoCommands geoCommands() {
		return new LettuceReactiveGeoCommands(this);
	}

	@Override
	public ReactiveHyperLogLogCommands hyperLogLogCommands() {
		return new LettuceReactiveHyperLogLogCommands(this);
	}

	@Override
	public ReactivePubSubCommands pubSubCommands() {

		synchronized (mutex) {
			if (this.pubSub == null) {
				this.pubSub = new LettuceReactivePubSubCommands(this);
			}
			return pubSub;
		}
	}

	@Override
	public ReactiveScriptingCommands scriptingCommands() {
		return new LettuceReactiveScriptingCommands(this);
	}

	@Override
	public ReactiveServerCommands serverCommands() {
		return new LettuceReactiveServerCommands(this);
	}

	@Override
	public ReactiveStreamCommands streamCommands() {
		return new LettuceReactiveStreamCommands(this);
	}

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

	public Mono<Void> closeLater() {
		return Flux.mergeDelayError(2, dedicatedConnection.close(), pubSubConnection.close()).then();
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

				DataAccessException convertedException = LettuceExceptionConverter.INSTANCE
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
	 * <li>Connection requested: First subscriber to {@link #getConnection()} initiates an asynchronous connect.
	 * Connection is accessible through the resulting {@link Mono} and will be cached. Connection will be closed if
	 * {@link AsyncConnect} gets closed before the connection publisher emits the actual connection.</li>
	 * <li>Closing: Call to {@link #close()} initiates connection closing.</li>
	 * <li>Closed: Connection is closed.</li>
	 * </ol>
	 *
	 * @author Mark Paluch
	 * @author Christoph Strobl
	 * @since 2.0.1
	 */
	static class AsyncConnect<T extends io.lettuce.core.api.StatefulConnection<?, ?>> {

		static AtomicReferenceFieldUpdater<AsyncConnect, State> STATE = AtomicReferenceFieldUpdater
				.newUpdater(AsyncConnect.class, State.class, "state");

		private final Mono<T> connectionPublisher;
		private final LettuceConnectionProvider connectionProvider;

		private volatile State state = State.INITIAL;
		private volatile @Nullable StatefulConnection<ByteBuffer, ByteBuffer> connection;

		@SuppressWarnings("unchecked")
		AsyncConnect(LettuceConnectionProvider connectionProvider, Class<T> connectionType) {

			Assert.notNull(connectionProvider, "LettuceConnectionProvider must not be null");
			Assert.notNull(connectionType, "Connection type must not be null");

			this.connectionProvider = connectionProvider;

			Mono<StatefulConnection> defer = Mono
					.fromCompletionStage(() -> connectionProvider.getConnectionAsync(connectionType));

			this.connectionPublisher = defer.doOnNext(it -> {

				if (isClosing(STATE.get(this))) {
					it.closeAsync();
				} else {
					connection = it;
				}
			}) //
					.cache() //
					.handle((connection, sink) -> {

						if (isClosing(STATE.get(this))) {
							sink.error(new IllegalStateException("Unable to connect; Connection is closed"));
						} else {
							sink.next((T) connection);
						}
					});
		}

		/**
		 * Obtain a connection publisher. This method connects asynchronously by requesting a connection from
		 * {@link LettuceConnectionProvider} with non-blocking synchronization if called concurrently.
		 *
		 * @return never {@literal null}.
		 */
		Mono<T> getConnection() {

			State state = STATE.get(this);
			if (isClosing(state)) {
				return Mono.error(new IllegalStateException("Unable to connect; Connection is closed"));
			}

			STATE.compareAndSet(this, State.INITIAL, State.CONNECTION_REQUESTED);

			return connectionPublisher;
		}

		/**
		 * Close connection.
		 */
		Mono<Void> close() {

			return Mono.defer(() -> {

				if (STATE.compareAndSet(this, State.INITIAL, CLOSING)
						|| STATE.compareAndSet(this, State.CONNECTION_REQUESTED, CLOSING)) {

					StatefulConnection<ByteBuffer, ByteBuffer> connection = this.connection;
					this.connection = null;

					STATE.set(this, State.CLOSED);
					if (connection != null) {
						return Mono.fromCompletionStage(connectionProvider.releaseAsync(connection));
					}

				}
				return Mono.empty();
			});
		}

		private static boolean isClosing(State state) {
			return state == State.CLOSING || state == State.CLOSED;
		}

		enum State {
			INITIAL, CONNECTION_REQUESTED, CLOSING, CLOSED;
		}
	}
}
