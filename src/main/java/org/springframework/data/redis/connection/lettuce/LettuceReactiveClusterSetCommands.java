/*
 * Copyright 2016-2025 the original author or authors.
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

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.reactivestreams.Publisher;
import org.springframework.data.redis.connection.ClusterSlotHashUtil;
import org.springframework.data.redis.connection.ReactiveClusterSetCommands;
import org.springframework.data.redis.connection.ReactiveRedisConnection.BooleanResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.CommandResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.NumericResponse;
import org.springframework.util.Assert;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.0
 */
class LettuceReactiveClusterSetCommands extends LettuceReactiveSetCommands implements ReactiveClusterSetCommands {

	/**
	 * Create new {@link LettuceReactiveClusterSetCommands}.
	 *
	 * @param connection must not be {@literal null}.
	 */
	LettuceReactiveClusterSetCommands(LettuceReactiveRedisConnection connection) {
		super(connection);
	}

	@Override
	public Flux<CommandResponse<SUnionCommand, Flux<ByteBuffer>>> sUnion(Publisher<SUnionCommand> commands) {

		return getConnection().execute(cmd -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKeys(), "Keys must not be null");

			if (ClusterSlotHashUtil.isSameSlotForAllKeys(command.getKeys())) {
				return super.sUnion(Mono.just(command));
			}

			Flux<ByteBuffer> result = Flux.merge(command.getKeys().stream().map(cmd::smembers).collect(Collectors.toList()))
					.distinct();

			return Mono.just(new CommandResponse<>(command, result));
		}));
	}

	@Override
	public Flux<NumericResponse<SUnionStoreCommand, Long>> sUnionStore(Publisher<SUnionStoreCommand> commands) {

		return getConnection().execute(cmd -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKeys(), "Source keys must not be null");
			Assert.notNull(command.getKey(), "Destination key must not be null");

			List<ByteBuffer> keys = new ArrayList<>(command.getKeys());
			keys.add(command.getKey());

			if (ClusterSlotHashUtil.isSameSlotForAllKeys(keys)) {
				return super.sUnionStore(Mono.just(command));
			}

			return sUnion(Mono.just(SUnionCommand.keys(command.getKeys()))).next().flatMap(values -> {

				Flux<ByteBuffer> output = values.getOutput();
				Mono<Long> result =  output != null ? output.collectList().flatMap(it -> {

					ByteBuffer[] members = it.toArray(new ByteBuffer[0]);
					return cmd.sadd(command.getKey(), members);
				}) : Mono.empty();

				return result.map(value -> new NumericResponse<>(command, value));
			});
		}));
	}

	@Override
	public Flux<CommandResponse<SInterCommand, Flux<ByteBuffer>>> sInter(Publisher<SInterCommand> commands) {

		return getConnection().execute(cmd -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKeys(), "Keys must not be null");

			if (ClusterSlotHashUtil.isSameSlotForAllKeys(command.getKeys())) {
				return super.sInter(Mono.just(command));
			}

			Mono<List<ByteBuffer>> sourceSet = cmd.smembers(command.getKeys().get(0)).distinct().collectList();

			List<Mono<List<ByteBuffer>>> intersectingSets = new ArrayList<>();

			for (int i = 1; i < command.getKeys().size(); i++) {
				intersectingSets.add(cmd.smembers(command.getKeys().get(i)).distinct().collectList());
			}

			Flux<List<ByteBuffer>> result = Flux.zip(sourceSet, Flux.merge(intersectingSets).collectList(),
					(source, intersectings) -> {

						for (List<ByteBuffer> intersecting : intersectings) {
							source.retainAll(intersecting);
						}
				return source;
			});

			return Mono.just(new CommandResponse<>(command, result.concatMap(v -> Flux.fromStream(v.stream()))));
		}));
	}

	@Override
	public Flux<NumericResponse<SInterStoreCommand, Long>> sInterStore(Publisher<SInterStoreCommand> commands) {

		return getConnection().execute(cmd -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKeys(), "Source keys must not be null");
			Assert.notNull(command.getKey(), "Destination key must not be null");

			List<ByteBuffer> keys = new ArrayList<>(command.getKeys());
			keys.add(command.getKey());

			if (ClusterSlotHashUtil.isSameSlotForAllKeys(keys)) {
				return super.sInterStore(Mono.just(command));
			}

			return sInter(Mono.just(SInterCommand.keys(command.getKeys()))).next().flatMap(values -> {

				Flux<ByteBuffer> output = values.getOutput();
				Mono<Long> result = output != null ? output.collectList().flatMap(it -> {

					ByteBuffer[] members = it.toArray(new ByteBuffer[0]);
					return cmd.sadd(command.getKey(), members);
				}) : Mono.empty();

				return result.map(value -> new NumericResponse<>(command, value));
			});
		}));
	}

	@Override
	public Flux<CommandResponse<SDiffCommand, Flux<ByteBuffer>>> sDiff(Publisher<SDiffCommand> commands) {

		return getConnection().execute(cmd -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKeys(), "Keys must not be null");

			if (ClusterSlotHashUtil.isSameSlotForAllKeys(command.getKeys())) {
				return super.sDiff(Mono.just(command));
			}

			Mono<List<ByteBuffer>> sourceSet = cmd.smembers(command.getKeys().get(0)).distinct().collectList();

			List<Mono<List<ByteBuffer>>> intersectingSets = new ArrayList<>();

			for (int i = 1; i < command.getKeys().size(); i++) {
				intersectingSets.add(cmd.smembers(command.getKeys().get(i)).distinct().collectList());
			}

			Flux<List<ByteBuffer>> result = Flux.zip(sourceSet, Flux.merge(intersectingSets).collectList(),
					(source, intersectings) -> {

						for (List<ByteBuffer> intersecting : intersectings) {
							source.removeAll(intersecting);
						}

				return source;
			});

			return Mono.just(new CommandResponse<>(command, result.concatMap(v -> Flux.fromStream(v.stream()))));

		}));
	}

	@Override
	public Flux<NumericResponse<SDiffStoreCommand, Long>> sDiffStore(Publisher<SDiffStoreCommand> commands) {

		return getConnection().execute(cmd -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKeys(), "Source keys must not be null");
			Assert.notNull(command.getKey(), "Destination key must not be null");

			List<ByteBuffer> keys = new ArrayList<>(command.getKeys());
			keys.add(command.getKey());

			if (ClusterSlotHashUtil.isSameSlotForAllKeys(keys)) {
				return super.sDiffStore(Mono.just(command));
			}

			return sDiff(Mono.just(SDiffCommand.keys(command.getKeys()))).next().flatMap(values -> {

				Flux<ByteBuffer> output = values.getOutput();
				Mono<Long> result = output != null ? output.collectList().flatMap(it -> {

					ByteBuffer[] members = it.toArray(new ByteBuffer[0]);
					return cmd.sadd(command.getKey(), members);
				}) : Mono.empty();

				return result.map(value -> new NumericResponse<>(command, value));
			});
		}));
	}

	@Override
	public Flux<BooleanResponse<SMoveCommand>> sMove(Publisher<SMoveCommand> commands) {

		return getConnection().execute(cmd -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKey(), "Source key must not be null");
			Assert.notNull(command.getDestination(), "Destination key must not be null");

			if (ClusterSlotHashUtil.isSameSlotForAllKeys(command.getKey(), command.getDestination())) {
				return super.sMove(Mono.just(command));
			}

			Mono<Boolean> result = cmd.exists(command.getKey())
					.flatMap(nrKeys -> nrKeys == 0 ? Mono.empty() : cmd.sismember(command.getKey(), command.getValue()))
					.flatMap(exists -> {

						if (!exists) {
							return Mono.just(Boolean.FALSE);
						}
						return cmd.sismember(command.getDestination(), command.getValue()).flatMap(existsInTarget -> {

							Mono<Boolean> tmp = cmd.srem(command.getKey(), command.getValue()).map(nrRemoved -> nrRemoved > 0);
							if (!existsInTarget) {
								return tmp.flatMap(removed -> cmd.sadd(command.getDestination(), command.getValue())
										.map(LettuceConverters::toBoolean));
							}
							return tmp;
						});

					});

			return result.defaultIfEmpty(Boolean.FALSE).map(value -> new BooleanResponse<>(command, value));
		}));
	}
}
