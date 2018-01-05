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
import io.lettuce.core.api.reactive.BaseRedisReactiveCommands;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.reactive.RedisClusterReactiveCommands;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;

import org.springframework.data.redis.connection.ClusterTopologyProvider;
import org.springframework.data.redis.connection.ReactiveRedisClusterConnection;
import org.springframework.data.redis.connection.RedisClusterNode;
import org.springframework.data.redis.connection.RedisNode;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * {@link ReactiveRedisClusterConnection} implementation for {@literal Lettuce}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.0
 */
class LettuceReactiveRedisClusterConnection extends LettuceReactiveRedisConnection
		implements ReactiveRedisClusterConnection {

	private final ClusterTopologyProvider topologyProvider;

	/**
	 * Creates new {@link LettuceReactiveRedisClusterConnection} given {@link LettuceConnectionProvider} and
	 * {@link RedisClusterClient}.
	 *
	 * @param connectionProvider must not be {@literal null}.
	 * @param client must not be {@literal null}.
	 * @throws IllegalArgumentException when {@code client} is {@literal null}.
	 */
	LettuceReactiveRedisClusterConnection(LettuceConnectionProvider connectionProvider, RedisClusterClient client) {

		super(connectionProvider);

		this.topologyProvider = new LettuceClusterTopologyProvider(client);
	}

	/**
	 * Creates new {@link LettuceReactiveRedisClusterConnection} given a shared {@link StatefulConnection connection},
	 * {@link LettuceConnectionProvider} and {@link RedisClusterClient}.
	 *
	 * @param sharedConnection must not be {@literal null}.
	 * @param connectionProvider must not be {@literal null}.
	 * @param client must not be {@literal null}.
	 * @throws IllegalArgumentException when {@code client} is {@literal null}.
	 * @since 2.0.1
	 */
	@SuppressWarnings("unchecked")
	LettuceReactiveRedisClusterConnection(StatefulConnection<ByteBuffer, ByteBuffer> sharedConnection,
			LettuceConnectionProvider connectionProvider, RedisClusterClient client) {

		super(sharedConnection, connectionProvider);

		this.topologyProvider = new LettuceClusterTopologyProvider(client);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceReactiveRedisConnection#keyCommands()
	 */
	@Override
	public LettuceReactiveClusterKeyCommands keyCommands() {
		return new LettuceReactiveClusterKeyCommands(this);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceReactiveRedisConnection#listCommands()
	 */
	@Override
	public LettuceReactiveClusterListCommands listCommands() {
		return new LettuceReactiveClusterListCommands(this);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceReactiveRedisConnection#setCommands()
	 */
	@Override
	public LettuceReactiveClusterSetCommands setCommands() {
		return new LettuceReactiveClusterSetCommands(this);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceReactiveRedisConnection#zSetCommands()
	 */
	@Override
	public LettuceReactiveClusterZSetCommands zSetCommands() {
		return new LettuceReactiveClusterZSetCommands(this);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceReactiveRedisConnection#hyperLogLogCommands()
	 */
	@Override
	public LettuceReactiveClusterHyperLogLogCommands hyperLogLogCommands() {
		return new LettuceReactiveClusterHyperLogLogCommands(this);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceReactiveRedisConnection#stringCommands()
	 */
	@Override
	public LettuceReactiveClusterStringCommands stringCommands() {
		return new LettuceReactiveClusterStringCommands(this);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceReactiveRedisConnection#geoCommands()
	 */
	@Override
	public LettuceReactiveClusterGeoCommands geoCommands() {
		return new LettuceReactiveClusterGeoCommands(this);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceReactiveRedisConnection#hashCommands()
	 */
	@Override
	public LettuceReactiveClusterHashCommands hashCommands() {
		return new LettuceReactiveClusterHashCommands(this);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceReactiveRedisConnection#numberCommands()
	 */
	@Override
	public LettuceReactiveClusterNumberCommands numberCommands() {
		return new LettuceReactiveClusterNumberCommands(this);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceReactiveRedisConnection#scriptingCommands()
	 */
	@Override
	public LettuceReactiveClusterScriptingCommands scriptingCommands() {
		return new LettuceReactiveClusterScriptingCommands(this);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceReactiveRedisConnection#serverCommands()
	 */
	@Override
	public LettuceReactiveClusterServerCommands serverCommands() {
		return new LettuceReactiveClusterServerCommands(this, topologyProvider);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisClusterConnection#ping(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public Mono<String> ping(RedisClusterNode node) {
		return execute(node, BaseRedisReactiveCommands::ping).next();
	}

	/**
	 * @param node must not be {@literal null}.
	 * @param callback must not be {@literal null}.
	 * @throws IllegalArgumentException when {@code node} or {@code callback} is {@literal null}.
	 * @return {@link Flux} emitting execution results.
	 */
	public <T> Flux<T> execute(RedisNode node, LettuceReactiveCallback<T> callback) {

		try {
			Assert.notNull(node, "RedisClusterNode must not be null!");
			Assert.notNull(callback, "ReactiveCallback must not be null!");
		} catch (IllegalArgumentException e) {
			return Flux.error(e);
		}

		return getCommands(node).flatMapMany(callback::doWithCommands).onErrorMap(translateException());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceReactiveRedisConnection#getConnection()
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	protected Mono<StatefulRedisClusterConnection<ByteBuffer, ByteBuffer>> getConnection() {
		return (Mono) super.getConnection();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceReactiveRedisConnection#getCommands()
	 */
	protected Mono<RedisClusterReactiveCommands<ByteBuffer, ByteBuffer>> getCommands() {
		return getConnection().map(StatefulRedisClusterConnection::reactive);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	protected Mono<RedisReactiveCommands<ByteBuffer, ByteBuffer>> getCommands(RedisNode node) {

		if (StringUtils.hasText(node.getId())) {
			return getConnection().cast(StatefulRedisClusterConnection.class)
					.map(it -> it.getConnection(node.getId()).reactive());
		}

		return getConnection().cast(StatefulRedisClusterConnection.class)
				.map(it -> it.getConnection(node.getHost(), node.getPort()).reactive());
	}
}
