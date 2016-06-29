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
import java.util.List;
import java.util.stream.Collectors;

import org.reactivestreams.Publisher;
import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.connection.ReactiveKeyCommands;
import org.springframework.data.redis.connection.ReactiveRedisConnection.BooleanResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.CommandResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.KeyCommand;
import org.springframework.data.redis.connection.ReactiveRedisConnection.MultiValueResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.NumericResponse;
import org.springframework.util.Assert;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * @author Christoph Strobl
 * @since 2.0
 */
public class LettuceReactiveKeyCommands implements ReactiveKeyCommands {

	private final LettuceReactiveRedisConnection connection;

	/**
	 * Create new {@link LettuceReactiveKeyCommands}.
	 * 
	 * @param connection must not be {@literal null}.
	 */
	public LettuceReactiveKeyCommands(LettuceReactiveRedisConnection connection) {

		Assert.notNull(connection, "Connection must not be null!");
		this.connection = connection;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisConnection.ReactiveKeyCommands#exists(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<BooleanResponse<KeyCommand>> exists(Publisher<KeyCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).flatMap((command) -> {

			Assert.notNull(command.getKey(), "Key must not be null!");

			return LettuceReactiveRedisConnection.<BooleanResponse<KeyCommand>> monoConverter()
					.convert(cmd.exists(command.getKey().array()).map(LettuceConverters.longToBooleanConverter()::convert)
							.map((value) -> new BooleanResponse<>(command, value)));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisConnection.ReactiveKeyCommands#type(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<CommandResponse<KeyCommand, DataType>> type(Publisher<KeyCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).flatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null!");

			return LettuceReactiveRedisConnection.<DataType> monoConverter()
					.convert(cmd.type(command.getKey().array()).map(LettuceConverters::toDataType))
					.map(respValue -> new CommandResponse<>(command, respValue));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisConnection.ReactiveKeyCommands#del(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<NumericResponse<KeyCommand, Long>> del(Publisher<KeyCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).flatMap((command) -> {

			Assert.notNull(command.getKey(), "Key must not be null!");

			return LettuceReactiveRedisConnection.<NumericResponse<KeyCommand, Long>> monoConverter()
					.convert(cmd.del(command.getKey().array()).map((value) -> new NumericResponse<>(command, value)));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisConnection.ReactiveKeyCommands#mDel(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<NumericResponse<List<ByteBuffer>, Long>> mDel(Publisher<List<ByteBuffer>> keysCollection) {

		return connection.execute(cmd -> Flux.from(keysCollection).flatMap((keys) -> {

			Assert.notEmpty(keys, "Keys must not be null!");

			return LettuceReactiveRedisConnection.<NumericResponse<List<ByteBuffer>, Long>> monoConverter()
					.convert(cmd
							.del(keys.stream().map(ByteBuffer::array).collect(Collectors.toList()).toArray(new byte[keys.size()][]))
							.map((value) -> new NumericResponse<>(keys, value)));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisConnection.ReactiveKeyCommands#keys(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<MultiValueResponse<ByteBuffer, ByteBuffer>> keys(Publisher<ByteBuffer> patterns) {
		return connection.execute(cmd -> Flux.from(patterns).flatMap(pattern -> {

			Assert.notNull(pattern, "Pattern must not be null!");

			return LettuceReactiveRedisConnection.<List<ByteBuffer>> monoConverter()
					.convert(cmd.keys(pattern.array()).map(ByteBuffer::wrap).toList())
					.map(value -> new MultiValueResponse<>(pattern, value));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisConnection.ReactiveKeyCommands#randomKey()
	 */
	@Override
	public Mono<ByteBuffer> randomKey() {
		return connection.execute(cmd -> LettuceReactiveRedisConnection.<ByteBuffer> monoConverter()
				.convert(cmd.randomkey().map(ByteBuffer::wrap))).next();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisConnection.ReactiveKeyCommands#rename(org.reactivestreams.Publisher, java.util.function.Supplier)
	 */
	@Override
	public Flux<BooleanResponse<RenameCommand>> rename(Publisher<RenameCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).flatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null!");
			Assert.notNull(command.getNewName(), "New name must not be null!");

			return LettuceReactiveRedisConnection.<Boolean> monoConverter().convert(
					cmd.rename(command.getKey().array(), command.getNewName().array()).map(LettuceConverters::stringToBoolean))
					.map(value -> new BooleanResponse<>(command, value));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisConnection.ReactiveKeyCommands#rename(org.reactivestreams.Publisher, java.util.function.Supplier)
	 */
	@Override
	public Flux<BooleanResponse<RenameCommand>> renameNX(Publisher<RenameCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).flatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null!");
			Assert.notNull(command.getNewName(), "New name must not be null!");

			return LettuceReactiveRedisConnection.<Boolean> monoConverter()
					.convert(cmd.renamenx(command.getKey().array(), command.getNewName().array()))
					.map(value -> new BooleanResponse<>(command, value));
		}));
	}
}
