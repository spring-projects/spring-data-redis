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

import io.lettuce.core.api.reactive.RedisServerReactiveCommands;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

import org.reactivestreams.Publisher;
import org.springframework.data.redis.connection.ClusterTopologyProvider;
import org.springframework.data.redis.connection.ReactiveClusterServerCommands;
import org.springframework.data.redis.connection.RedisClusterNode;
import org.springframework.data.redis.connection.RedisServerCommands.FlushOption;
import org.springframework.data.redis.core.types.RedisClientInfo;
import org.springframework.data.redis.util.ByteUtils;
import org.springframework.util.Assert;

/**
 * {@link ReactiveClusterServerCommands} implementation for {@literal Lettuce}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @author Dennis Neufeld
 * @since 2.0
 */
class LettuceReactiveClusterServerCommands extends LettuceReactiveServerCommands
		implements ReactiveClusterServerCommands {

	private final LettuceReactiveRedisClusterConnection connection;
	private final ClusterTopologyProvider topologyProvider;

	/**
	 * Create new {@link LettuceReactiveClusterServerCommands}.
	 *
	 * @param connection must not be {@literal null}.
	 * @param topologyProvider must not be {@literal null}.
	 * @throws IllegalArgumentException when {@code connection} is {@literal null}.
	 * @throws IllegalArgumentException when {@code topologyProvider} is {@literal null}.
	 */
	LettuceReactiveClusterServerCommands(LettuceReactiveRedisClusterConnection connection,
			ClusterTopologyProvider topologyProvider) {

		super(connection);

		this.connection = connection;
		this.topologyProvider = topologyProvider;
	}

	@Override
	public Mono<String> bgReWriteAof(RedisClusterNode node) {
		return connection.execute(node, RedisServerReactiveCommands::bgrewriteaof).next();
	}

	@Override
	public Mono<String> bgSave(RedisClusterNode node) {
		return connection.execute(node, RedisServerReactiveCommands::bgsave).next();
	}

	@Override
	public Mono<Long> lastSave(RedisClusterNode node) {
		return connection.execute(node, RedisServerReactiveCommands::lastsave).map(Date::getTime).next();
	}

	@Override
	public Mono<String> save(RedisClusterNode node) {
		return connection.execute(node, RedisServerReactiveCommands::save).next();
	}

	@Override
	public Mono<Long> dbSize(RedisClusterNode node) {
		return connection.execute(node, RedisServerReactiveCommands::dbsize).next();
	}

	@Override
	public Mono<String> flushDb(RedisClusterNode node) {
		return connection.execute(node, RedisServerReactiveCommands::flushdb).next();
	}

	@Override
	public Mono<String> flushDb(RedisClusterNode node, FlushOption option) {
		return connection.execute(node, it -> it.flushdb(LettuceConverters.toFlushMode(option))).next();
	}

	@Override
	public Mono<String> flushAll(RedisClusterNode node) {
		return connection.execute(node, RedisServerReactiveCommands::flushall).next();
	}

	@Override
	public Mono<String> flushAll(RedisClusterNode node, FlushOption option) {
		return connection.execute(node, it -> it.flushall(LettuceConverters.toFlushMode(option))).next();
	}

	@Override
	public Mono<Properties> info() {
		return Flux.merge(executeOnAllNodes(this::info)).collect(PropertiesCollector.INSTANCE);
	}

	@Override
	public Mono<Properties> info(RedisClusterNode node) {

		return connection.execute(node, RedisServerReactiveCommands::info) //
				.map(LettuceConverters::toProperties) //
				.next();
	}

	@Override
	public Mono<Properties> info(String section) {

		Assert.hasText(section, "Section must not be null or empty");

		return Flux.merge(executeOnAllNodes(redisClusterNode -> info(redisClusterNode, section)))
				.collect(PropertiesCollector.INSTANCE);
	}

	@Override
	public Mono<Properties> info(RedisClusterNode node, String section) {

		Assert.hasText(section, "Section must not be null or empty");

		return connection.execute(node, c -> c.info(section)) //
				.map(LettuceConverters::toProperties).next();
	}

	@Override
	public Mono<Properties> getConfig(String pattern) {

		Assert.hasText(pattern, "Pattern must not be null or empty");

		return Flux.merge(executeOnAllNodes(node -> getConfig(node, pattern))) //
				.collect(PropertiesCollector.INSTANCE);
	}

	@Override
	public Mono<Properties> getConfig(RedisClusterNode node, String pattern) {

		Assert.hasText(pattern, "Pattern must not be null or empty");

		return connection.execute(node, c -> c.configGet(pattern)) //
				.map(LettuceConverters::toProperties) //
				.next();
	}

	@Override
	public Mono<String> setConfig(String param, String value) {
		return Flux.merge(executeOnAllNodes(node -> setConfig(node, param, value))).map(Tuple2::getT2).last();
	}

	@Override
	public Mono<String> setConfig(RedisClusterNode node, String param, String value) {

		Assert.hasText(param, "Parameter must not be null or empty");
		Assert.hasText(value, "Value must not be null or empty");

		return connection.execute(node, c -> c.configSet(param, value)).next();
	}

	@Override
	public Mono<String> resetConfigStats() {
		return Flux.merge(executeOnAllNodes(this::resetConfigStats)).map(Tuple2::getT2).last();
	}

	@Override
	public Mono<String> resetConfigStats(RedisClusterNode node) {
		return connection.execute(node, RedisServerReactiveCommands::configResetstat).next();
	}

	@Override
	public Mono<Long> time(RedisClusterNode node) {

		return connection.execute(node, RedisServerReactiveCommands::time) //
				.map(ByteUtils::getBytes) //
				.collectList() //
				.map(LettuceConverters.toTimeConverter(TimeUnit.MILLISECONDS)::convert);
	}

	@Override
	public Flux<RedisClientInfo> getClientList() {
		return Flux.merge(executeOnAllNodesMany(this::getClientList)).map(Tuple2::getT2);
	}

	@Override
	public Flux<RedisClientInfo> getClientList(RedisClusterNode node) {

		return connection.execute(node, RedisServerReactiveCommands::clientList)
				.concatMapIterable(LettuceConverters.stringToRedisClientListConverter()::convert);
	}

	private <T> Collection<Publisher<Tuple2<RedisClusterNode, T>>> executeOnAllNodes(
			Function<RedisClusterNode, Mono<T>> callback) {

		Set<RedisClusterNode> nodes = topologyProvider.getTopology().getNodes();
		List<Publisher<Tuple2<RedisClusterNode, T>>> pipeline = new ArrayList<>(nodes.size());

		for (RedisClusterNode redisClusterNode : nodes) {
			pipeline.add(callback.apply(redisClusterNode).map(p -> Tuples.of(redisClusterNode, p)));
		}

		return pipeline;
	}

	private <T> Collection<Publisher<Tuple2<RedisClusterNode, T>>> executeOnAllNodesMany(
			Function<RedisClusterNode, Flux<T>> callback) {

		Set<RedisClusterNode> nodes = topologyProvider.getTopology().getNodes();
		List<Publisher<Tuple2<RedisClusterNode, T>>> pipeline = new ArrayList<>(nodes.size());

		for (RedisClusterNode redisClusterNode : nodes) {
			pipeline.add(callback.apply(redisClusterNode).map(p -> Tuples.of(redisClusterNode, p)));
		}

		return pipeline;
	}

	/**
	 * Collector to merge {@link Tuple2} of {@link RedisClusterNode} and {@link Properties} into a single
	 * {@link Properties} object by prefixing original the keys with {@link RedisClusterNode#asString()}.
	 */
	private enum PropertiesCollector implements Collector<Tuple2<RedisClusterNode, Properties>, Properties, Properties> {

		INSTANCE;

		@Override
		public Supplier<Properties> supplier() {
			return Properties::new;
		}

		@Override
		public BiConsumer<Properties, Tuple2<RedisClusterNode, Properties>> accumulator() {

			return (properties, tuple) -> {

				for (Entry<Object, Object> entry : tuple.getT2().entrySet()) {
					properties.put(tuple.getT1().asString() + "." + entry.getKey(), entry.getValue());
				}
			};
		}

		@Override
		public BinaryOperator<Properties> combiner() {

			return (left, right) -> {

				Properties merge = new Properties();

				merge.putAll(left);
				merge.putAll(right);

				return merge;
			};
		}

		@Override
		public Function<Properties, Properties> finisher() {
			return properties -> properties;
		}

		@Override
		public Set<Characteristics> characteristics() {
			return new HashSet<>(Arrays.asList(Characteristics.UNORDERED, Characteristics.IDENTITY_FINISH));
		}
	}
}
