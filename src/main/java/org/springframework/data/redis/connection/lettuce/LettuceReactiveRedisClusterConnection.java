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
	 * Creates new {@link LettuceReactiveRedisClusterConnection}.
	 *
	 * @param client must not be {@literal null}.
	 * @throws IllegalArgumentException when {@code client} is {@literal null}.
	 * @throws org.springframework.dao.InvalidDataAccessResourceUsageException when {@code client} is not suitable for
	 *           cluster environment.
	 */
	LettuceReactiveRedisClusterConnection(RedisClusterClient client) {

		super(client);

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

		return Flux.defer(() -> callback.doWithCommands(getCommands(node))).onErrorMap(translateException());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceReactiveRedisConnection#getConnection()
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	protected StatefulRedisClusterConnection<ByteBuffer, ByteBuffer> getConnection() {

		Assert.isInstanceOf(StatefulRedisClusterConnection.class, super.getConnection(),
				"Connection needs to be instance of StatefulRedisClusterConnection");

		return (StatefulRedisClusterConnection) super.getConnection();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceReactiveRedisConnection#getCommands()
	 */
	protected RedisClusterReactiveCommands<ByteBuffer, ByteBuffer> getCommands() {
		return getConnection().reactive();
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	protected RedisReactiveCommands<ByteBuffer, ByteBuffer> getCommands(RedisNode node) {

		if (!(getConnection() instanceof StatefulRedisClusterConnection)) {
			throw new IllegalArgumentException("o.O connection needs to be cluster compatible " + getConnection());
		}

		if (StringUtils.hasText(node.getId())) {
			return ((StatefulRedisClusterConnection) getConnection()).getConnection(node.getId()).reactive();
		}

		return ((StatefulRedisClusterConnection) getConnection()).getConnection(node.getHost(), node.getPort()).reactive();
	}
}
