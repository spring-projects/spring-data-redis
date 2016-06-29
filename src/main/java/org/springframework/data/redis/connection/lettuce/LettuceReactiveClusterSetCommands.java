/*
 * Copyright 2016. the original author or authors.
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
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.reactivestreams.Publisher;
import org.springframework.data.redis.connection.ClusterSlotHashUtil;
import org.springframework.data.redis.connection.ReactiveClusterSetCommands;
import org.springframework.data.redis.connection.ReactiveRedisConnection;
import org.springframework.util.Assert;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import rx.Observable;

/**
 * @author Christoph Strobl
 * @since 2.0
 */
public class LettuceReactiveClusterSetCommands extends LettuceReactiveSetCommands
		implements ReactiveClusterSetCommands {

	/**
	 * Create new {@link LettuceReactiveSetCommands}.
	 *
	 * @param connection must not be {@literal null}.
	 */
	public LettuceReactiveClusterSetCommands(LettuceReactiveRedisConnection connection) {
		super(connection);
	}

	@Override
	public Flux<ReactiveRedisConnection.MultiValueResponse<SUnionCommand, ByteBuffer>> sUnion(
			Publisher<SUnionCommand> commands) {

		return getConnection().execute(cmd -> Flux.from(commands).flatMap(command -> {

			Assert.notNull(command.getKeys(), "Keys must not be null!");

			if (ClusterSlotHashUtil.isSameSlotForAllKeys(command.getKeys())) {
				return super.sUnion(Mono.just(command));
			}

			Observable<List<ByteBuffer>> result = Observable
					.merge(command.getKeys().stream().map(key -> cmd.smembers(key.array())).collect(Collectors.toList()))
					.map(ByteBuffer::wrap).distinct().toList();

			return LettuceReactiveRedisConnection.<List<ByteBuffer>> monoConverter().convert(result)
					.map(value -> new ReactiveRedisConnection.MultiValueResponse<>(command, value));
		}));
	}

	@Override
	public Flux<ReactiveRedisConnection.NumericResponse<SUnionStoreCommand, Long>> sUnionStore(
			Publisher<SUnionStoreCommand> commands) {

		return getConnection().execute(cmd -> Flux.from(commands).flatMap(command -> {

			Assert.notNull(command.getKeys(), "Source keys must not be null!");
			Assert.notNull(command.getKey(), "Destination key must not be null!");

			List<ByteBuffer> keys = new ArrayList<>(command.getKeys());
			keys.add(command.getKey());

			if (ClusterSlotHashUtil.isSameSlotForAllKeys(keys)) {
				return super.sUnionStore(Mono.just(command));
			}

			return sUnion(Mono.just(SUnionCommand.keys(command.getKeys()))).next().flatMap(values -> {
				Observable<Long> result = cmd.sadd(command.getKey().array(),
						values.getOutput().stream().map(ByteBuffer::array).toArray(size -> new byte[size][]));
				return LettuceReactiveRedisConnection.<Long> monoConverter().convert(result)
						.map(value -> new ReactiveRedisConnection.NumericResponse<>(command, value));
			});
		}));
	}

	@Override
	public Flux<ReactiveRedisConnection.MultiValueResponse<SInterCommand, ByteBuffer>> sInter(
			Publisher<SInterCommand> commands) {

		return getConnection().execute(cmd -> Flux.from(commands).flatMap(command -> {

			Assert.notNull(command.getKeys(), "Keys must not be null!");

			if (ClusterSlotHashUtil.isSameSlotForAllKeys(command.getKeys())) {
				return super.sInter(Mono.just(command));
			}

			Observable<List<ByteBuffer>> sourceSet = cmd.smembers(command.getKeys().get(0).array()).map(ByteBuffer::wrap)
					.distinct().toList();

			List<Observable<List<ByteBuffer>>> intersectingSets = new ArrayList<>();

			for (int i = 1; i < command.getKeys().size(); i++) {
				intersectingSets.add(cmd.smembers(command.getKeys().get(i).array()).map(ByteBuffer::wrap).distinct().toList());
			}

			Observable<List<ByteBuffer>> result = Observable.zip(sourceSet, Observable.merge(intersectingSets),
					(source, intersecting) -> {

						source.retainAll(intersecting);
						return source;
					});

			return LettuceReactiveRedisConnection.<List<ByteBuffer>> monoConverter().convert(result)
					.map(value -> new ReactiveRedisConnection.MultiValueResponse<>(command, value));
		}));
	}

	@Override
	public Flux<ReactiveRedisConnection.NumericResponse<SInterStoreCommand, Long>> sInterStore(
			Publisher<SInterStoreCommand> commands) {

		return getConnection().execute(cmd -> Flux.from(commands).flatMap(command -> {

			Assert.notNull(command.getKeys(), "Source keys must not be null!");
			Assert.notNull(command.getKey(), "Destination key must not be null!");

			List<ByteBuffer> keys = new ArrayList<>(command.getKeys());
			keys.add(command.getKey());

			if (ClusterSlotHashUtil.isSameSlotForAllKeys(keys)) {
				return super.sInterStore(Mono.just(command));
			}

			return sInter(Mono.just(SInterCommand.keys(command.getKeys()))).next().flatMap(values -> {
				Observable<Long> result = cmd.sadd(command.getKey().array(),
						values.getOutput().stream().map(ByteBuffer::array).toArray(size -> new byte[size][]));
				return LettuceReactiveRedisConnection.<Long> monoConverter().convert(result)
						.map(value -> new ReactiveRedisConnection.NumericResponse<>(command, value));
			});
		}));
	}

	@Override
	public Flux<ReactiveRedisConnection.MultiValueResponse<SDiffCommand, ByteBuffer>> sDiff(
			Publisher<SDiffCommand> commands) {

		return getConnection().execute(cmd -> Flux.from(commands).flatMap(command -> {

			Assert.notNull(command.getKeys(), "Keys must not be null!");

			if (ClusterSlotHashUtil.isSameSlotForAllKeys(command.getKeys())) {
				return super.sDiff(Mono.just(command));
			}

			Observable<List<ByteBuffer>> sourceSet = cmd.smembers(command.getKeys().get(0).array()).map(ByteBuffer::wrap)
					.distinct().toList();

			List<Observable<List<ByteBuffer>>> intersectingSets = new ArrayList<>();

			for (int i = 1; i < command.getKeys().size(); i++) {
				intersectingSets.add(cmd.smembers(command.getKeys().get(i).array()).map(ByteBuffer::wrap).distinct().toList());
			}

			Observable<List<ByteBuffer>> result = Observable.zip(sourceSet, Observable.merge(intersectingSets),
					(source, intersecting) -> {

						source.removeAll(intersecting);
						return source;
					});

			return LettuceReactiveRedisConnection.<List<ByteBuffer>> monoConverter().convert(result)
					.map(value -> new ReactiveRedisConnection.MultiValueResponse<>(command, value));

		}));
	}

	@Override
	public Flux<ReactiveRedisConnection.NumericResponse<SDiffStoreCommand, Long>> sDiffStore(
			Publisher<SDiffStoreCommand> commands) {

		return getConnection().execute(cmd -> Flux.from(commands).flatMap(command -> {

			Assert.notNull(command.getKeys(), "Source keys must not be null!");
			Assert.notNull(command.getKey(), "Destination key must not be null!");

			List<ByteBuffer> keys = new ArrayList<>(command.getKeys());
			keys.add(command.getKey());

			if (ClusterSlotHashUtil.isSameSlotForAllKeys(keys)) {
				return super.sDiffStore(Mono.just(command));
			}

			return sDiff(Mono.just(SDiffCommand.keys(command.getKeys()))).next().flatMap(values -> {
				Observable<Long> result = cmd.sadd(command.getKey().array(),
						values.getOutput().stream().map(ByteBuffer::array).toArray(size -> new byte[size][]));
				return LettuceReactiveRedisConnection.<Long> monoConverter().convert(result)
						.map(value -> new ReactiveRedisConnection.NumericResponse<>(command, value));
			});
		}));
	}

	@Override
	public Flux<ReactiveRedisConnection.BooleanResponse<SMoveCommand>> sMove(Publisher<SMoveCommand> commands) {

		return getConnection().execute(cmd -> Flux.from(commands).flatMap(command -> {

			Assert.notNull(command.getKey(), "Source key must not be null!");
			Assert.notNull(command.getDestination(), "Destination key must not be null!");

			if (ClusterSlotHashUtil.isSameSlotForAllKeys(command.getKey(), command.getDestination())) {
				return super.sMove(Mono.just(command));
			}

			Observable<Boolean> result = cmd.exists(command.getKey().array()).flatMap(nrKeys -> nrKeys == 0
					? Observable.empty() : cmd.sismember(command.getKey().array(), command.getValue().array()))
					.flatMap(exists -> {

						if (!exists) {
							return Observable.just(Boolean.FALSE);
						}
						return cmd.sismember(command.getDestination().array(), command.getValue().array())
								.flatMap(existsInTarget -> {

									Observable<Boolean> tmp = cmd.srem(command.getKey().array(), command.getValue().array())
											.map(nrRemoved -> nrRemoved > 0);
									if (!existsInTarget) {
										return tmp.flatMap(removed -> cmd.sadd(command.getDestination().array(), command.getValue().array())
												.map(LettuceConverters::toBoolean));
									}
									return tmp;
								});

					});

			return LettuceReactiveRedisConnection.<Boolean> monoConverter().convert(result.defaultIfEmpty(Boolean.FALSE))
					.map(value -> new ReactiveRedisConnection.BooleanResponse<>(command, value));
		}));
	}
}
