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
import org.springframework.data.redis.connection.ReactiveRedisConnection.MultiValueResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.NumericResponse;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

import com.lambdaworks.redis.KeyValue;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.0
 */
public class LettuceReactiveHashCommands implements ReactiveHashCommands {

	private final LettuceReactiveRedisConnection connection;

	/**
	 * Create new {@link LettuceReactiveHashCommands}.
	 *
	 * @param connection must not be {@literal null}.
	 */
	public LettuceReactiveHashCommands(LettuceReactiveRedisConnection connection) {

		Assert.notNull(connection, "Connection must not be null!");
		this.connection = connection;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveHashCommands#hSet(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<BooleanResponse<HSetCommand>> hSet(Publisher<HSetCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).flatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null!");
			Assert.notNull(command.getFieldValueMap(), "FieldValueMap must not be null!");

			Mono<Boolean> result;

			if (command.getFieldValueMap().size() == 1) {

				Entry<ByteBuffer, ByteBuffer> entry = command.getFieldValueMap().entrySet().iterator().next();

				result = ObjectUtils.nullSafeEquals(command.isUpsert(), Boolean.TRUE)
						? cmd.hset(command.getKey(), entry.getKey(), entry.getValue())
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

		return connection.execute(cmd -> Flux.from(commands).flatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null!");
			Assert.notNull(command.getFields(), "Fields must not be null!");

			Mono<List<KeyValue<ByteBuffer, ByteBuffer>>> result;

			if (command.getFields().size() == 1) {
				ByteBuffer key = command.getFields().iterator().next();
				result = cmd.hget(command.getKey(), key.duplicate()).map(value -> KeyValue.fromNullable(key, value))
						.map(Collections::singletonList).otherwiseReturn(Collections.emptyList());
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

		return connection.execute(cmd -> Flux.from(commands).flatMap(command -> {

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

		return connection.execute(cmd -> Flux.from(commands).flatMap(command -> {

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

		return connection.execute(cmd -> Flux.from(commands).flatMap(command -> {

			Assert.notNull(command.getKey(), "Command.getKey() must not be null!");

			return cmd.hlen(command.getKey()).map(value -> new NumericResponse<>(command, value));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveHashCommands#hKeys(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<MultiValueResponse<KeyCommand, ByteBuffer>> hKeys(Publisher<KeyCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).flatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null!");

			Mono<List<ByteBuffer>> result = cmd.hkeys(command.getKey()).collectList();

			return result.map(value -> new MultiValueResponse<>(command, value));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveHashCommands#hKeys(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<MultiValueResponse<KeyCommand, ByteBuffer>> hVals(Publisher<KeyCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).flatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null!");

			Mono<List<ByteBuffer>> result = cmd.hvals(command.getKey()).collectList();

			return result.map(value -> new MultiValueResponse<>(command, value));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveHashCommands#hGetAll(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<CommandResponse<KeyCommand, Map<ByteBuffer, ByteBuffer>>> hGetAll(Publisher<KeyCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).flatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null!");

			Mono<Map<ByteBuffer, ByteBuffer>> result = cmd.hgetall(command.getKey());

			return result.map(value -> new CommandResponse<>(command, value));
		}));
	}
}
