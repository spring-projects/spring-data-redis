/*
 * Copyright 2016-2021 the original author or authors.
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

import io.lettuce.core.KeyValue;
import io.lettuce.core.ScanStream;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.reactivestreams.Publisher;

import org.springframework.data.redis.connection.ReactiveHashCommands;
import org.springframework.data.redis.connection.ReactiveRedisConnection.BooleanResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.CommandResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.KeyCommand;
import org.springframework.data.redis.connection.ReactiveRedisConnection.KeyScanCommand;
import org.springframework.data.redis.connection.ReactiveRedisConnection.MultiValueResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.NumericResponse;
import org.springframework.data.redis.connection.convert.Converters;
import org.springframework.util.Assert;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.0
 */
class LettuceReactiveHashCommands implements ReactiveHashCommands {

	private final LettuceReactiveRedisConnection connection;

	/**
	 * Create new {@link LettuceReactiveHashCommands}.
	 *
	 * @param connection must not be {@literal null}.
	 */
	LettuceReactiveHashCommands(LettuceReactiveRedisConnection connection) {

		Assert.notNull(connection, "Connection must not be null!");
		this.connection = connection;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveHashCommands#hSet(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<BooleanResponse<HSetCommand>> hSet(Publisher<HSetCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null!");
			Assert.notNull(command.getFieldValueMap(), "FieldValueMap must not be null!");

			Mono<Boolean> result;

			if (command.getFieldValueMap().size() == 1) {

				Entry<ByteBuffer, ByteBuffer> entry = command.getFieldValueMap().entrySet().iterator().next();

				result = command.isUpsert() ? cmd.hset(command.getKey(), entry.getKey(), entry.getValue())
						: cmd.hsetnx(command.getKey(), entry.getKey(), entry.getValue());
			} else {

				Map<ByteBuffer, ByteBuffer> entries = command.getFieldValueMap();

				result = cmd.hmset(command.getKey(), entries).map(LettuceConverters::stringToBoolean);
			}

			return result.map(value -> new BooleanResponse<>(command, value));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveHashCommands#hMGet(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<MultiValueResponse<HGetCommand, ByteBuffer>> hMGet(Publisher<HGetCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null!");
			Assert.notNull(command.getFields(), "Fields must not be null!");

			Mono<List<KeyValue<ByteBuffer, ByteBuffer>>> result;

			if (command.getFields().size() == 1) {
				ByteBuffer key = command.getFields().iterator().next();
				result = cmd.hget(command.getKey(), key.duplicate()).map(value -> KeyValue.fromNullable(key, value))
						.map(Collections::singletonList).onErrorReturn(Collections.emptyList());
			} else {
				result = cmd.hmget(command.getKey(), command.getFields().stream().toArray(ByteBuffer[]::new)).collectList();
			}

			return result.map(value -> new MultiValueResponse<>(command,
					value.stream().map(keyValue -> keyValue.getValueOrElse(null)).collect(Collectors.toList())));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveHashCommands#hExists(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<BooleanResponse<HExistsCommand>> hExists(Publisher<HExistsCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null!");
			Assert.notNull(command.getName(), "Name must not be null!");

			return cmd.hexists(command.getKey(), command.getField()).map(value -> new BooleanResponse<>(command, value));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveHashCommands#hDel(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<NumericResponse<HDelCommand, Long>> hDel(Publisher<HDelCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null!");
			Assert.notNull(command.getFields(), "Fields must not be null!");

			return cmd.hdel(command.getKey(), command.getFields().stream().toArray(ByteBuffer[]::new))
					.map(value -> new NumericResponse<>(command, value));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveHashCommands#hLen(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<NumericResponse<KeyCommand, Long>> hLen(Publisher<KeyCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKey(), "Command.getKey() must not be null!");

			return cmd.hlen(command.getKey()).map(value -> new NumericResponse<>(command, value));
		}));
	}

	@Override
	public Flux<CommandResponse<HRandFieldCommand, Flux<ByteBuffer>>> hRandField(Publisher<HRandFieldCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).map(command -> {

			Assert.notNull(command.getKey(), "Command.getKey() must not be null!");

			return new CommandResponse<>(command, cmd.hrandfield(command.getKey(), command.getCount()));
		}));
	}

	@Override
	public Flux<CommandResponse<HRandFieldCommand, Flux<Entry<ByteBuffer, ByteBuffer>>>> hRandFieldWithValues(
			Publisher<HRandFieldCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).map(command -> {

			Assert.notNull(command.getKey(), "Command.getKey() must not be null!");

			Flux<Entry<ByteBuffer, ByteBuffer>> flux = cmd.hrandfieldWithvalues(command.getKey(), command.getCount())
					.handle((it, sink) -> {

						if (it.isEmpty()) {
							return;
						}

						sink.next(Converters.entryOf(it.getKey(), it.getValue()));
					});

			return new CommandResponse<>(command, flux);
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveHashCommands#hKeys(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<CommandResponse<KeyCommand, Flux<ByteBuffer>>> hKeys(Publisher<KeyCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null!");

			Flux<ByteBuffer> result = cmd.hkeys(command.getKey());

			return Mono.just(new CommandResponse<>(command, result));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveHashCommands#hKeys(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<CommandResponse<KeyCommand, Flux<ByteBuffer>>> hVals(Publisher<KeyCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null!");

			Flux<ByteBuffer> result = cmd.hvals(command.getKey());

			return Mono.just(new CommandResponse<>(command, result));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveHashCommands#hGetAll(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<CommandResponse<KeyCommand, Flux<Map.Entry<ByteBuffer, ByteBuffer>>>> hGetAll(
			Publisher<KeyCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null!");

			Flux<KeyValue<ByteBuffer, ByteBuffer>> result = cmd.hgetall(command.getKey());

			return Mono.just(new CommandResponse<>(command, result.map(LettuceReactiveHashCommands::toEntry)));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveHashCommands#hScan(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<CommandResponse<KeyCommand, Flux<Map.Entry<ByteBuffer, ByteBuffer>>>> hScan(
			Publisher<KeyScanCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null!");
			Assert.notNull(command.getOptions(), "ScanOptions must not be null!");

			Flux<KeyValue<ByteBuffer, ByteBuffer>> result = ScanStream.hscan(cmd, command.getKey(),
					LettuceConverters.toScanArgs(command.getOptions()));

			Flux<Entry<ByteBuffer, ByteBuffer>> entryFlux = result.map(it -> new Entry<ByteBuffer, ByteBuffer>() {

				@Override
				public ByteBuffer getKey() {
					return it.getKey();
				}

				@Override
				public ByteBuffer getValue() {
					return it.getValue();
				}

				@Override
				public ByteBuffer setValue(ByteBuffer value) {
					throw new UnsupportedOperationException("Cannot set value for entry in cursor.");
				}
			});

			return Mono.just(new CommandResponse<>(command, entryFlux));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveHashCommands#hstrlen(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<NumericResponse<HStrLenCommand, Long>> hStrLen(Publisher<HStrLenCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null!");
			Assert.notNull(command.getField(), "Field must not be null!");

			return cmd.hstrlen(command.getKey(), command.getField()).map(value -> new NumericResponse<>(command, value));
		}));
	}

	private static Map.Entry<ByteBuffer, ByteBuffer> toEntry(KeyValue<ByteBuffer, ByteBuffer> kv) {

		return new Entry<ByteBuffer, ByteBuffer>() {

			@Override
			public ByteBuffer getKey() {
				return kv.getKey();
			}

			@Override
			public ByteBuffer getValue() {
				return kv.getValue();
			}

			@Override
			public ByteBuffer setValue(ByteBuffer value) {
				throw new UnsupportedOperationException("Cannot set value for entry");
			}
		};
	}
}
