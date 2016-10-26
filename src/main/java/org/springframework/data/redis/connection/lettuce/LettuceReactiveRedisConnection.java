/*
 * Copyright 2016 the original author or authors.
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

import java.nio.ByteBuffer;
import java.util.function.Function;

import org.reactivestreams.Publisher;
import org.springframework.core.convert.converter.Converter;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.InvalidDataAccessResourceUsageException;
import org.springframework.data.redis.connection.*;
import org.springframework.util.Assert;

import com.lambdaworks.redis.AbstractRedisClient;
import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.api.StatefulConnection;
import com.lambdaworks.redis.api.StatefulRedisConnection;
import com.lambdaworks.redis.cluster.RedisClusterClient;
import com.lambdaworks.redis.cluster.api.StatefulRedisClusterConnection;
import com.lambdaworks.redis.cluster.api.reactive.RedisClusterReactiveCommands;
import com.lambdaworks.redis.codec.RedisCodec;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import rx.Observable;

/**
 * @author Christoph Strobl
 * @since 2.0
 */
public class LettuceReactiveRedisConnection implements ReactiveRedisConnection {

	private StatefulConnection<ByteBuffer, ByteBuffer> connection;

	private static final RedisCodec<ByteBuffer, ByteBuffer> CODEC = ByteBufferCodec.INSTANCE;

	public LettuceReactiveRedisConnection(AbstractRedisClient client) {

		Assert.notNull(client, "RedisClient must not be null!");

		if (client instanceof RedisClient) {
			connection = ((RedisClient) client).connect(CODEC);
		} else if (client instanceof RedisClusterClient) {
			connection = ((RedisClusterClient) client).connect(CODEC);
		} else {
			throw new InvalidDataAccessResourceUsageException(
					String.format("Cannot use client of type %s", client.getClass()));
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisConnection#keyCommands()
	 */
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

	/**
	 * @param callback
	 * @return
	 */
	public <T> Flux<T> execute(LettuceReactiveCallback<T> callback) {
		return Flux.defer(() -> callback.doWithCommands(getCommands())).onErrorResumeWith(translateExeception());
	}

	@Override
	public void close() {
		connection.close();
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

	<T> Function<Throwable, Publisher<? extends T>> translateExeception() {

		return throwable -> {

			if (throwable instanceof RuntimeException) {

				DataAccessException convertedException = null;
				if (throwable instanceof RuntimeException) {
					convertedException = LettuceConverters.exceptionConverter().convert((RuntimeException) throwable);
				}
				return Flux.error(convertedException != null ? convertedException : throwable);
			}

			return Flux.error(throwable);
		};
	}

	interface LettuceReactiveCallback<T> {
		Publisher<T> doWithCommands(RedisClusterReactiveCommands<ByteBuffer, ByteBuffer> cmd);
	}

	static enum ByteBufferCodec implements RedisCodec<ByteBuffer, ByteBuffer> {

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
