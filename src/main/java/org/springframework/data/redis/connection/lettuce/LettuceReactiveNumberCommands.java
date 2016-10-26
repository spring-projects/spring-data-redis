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

import org.reactivestreams.Publisher;
import org.springframework.data.redis.connection.ReactiveNumberCommands;
import org.springframework.data.redis.connection.ReactiveRedisConnection.KeyCommand;
import org.springframework.data.redis.connection.ReactiveRedisConnection.NumericResponse;
import org.springframework.util.Assert;
import org.springframework.util.NumberUtils;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.0
 */
public class LettuceReactiveNumberCommands implements ReactiveNumberCommands {

	private final LettuceReactiveRedisConnection connection;

	/**
	 * Create new {@link LettuceReactiveStringCommands}.
	 * 
	 * @param connection must not be {@literal null}.
	 */
	public LettuceReactiveNumberCommands(LettuceReactiveRedisConnection connection) {

		Assert.notNull(connection, "Connection must not be null!");
		this.connection = connection;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisConnection.ReactiveNumberCommands#incr(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<NumericResponse<KeyCommand, Long>> incr(Publisher<KeyCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).flatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null!");

			return cmd.incr(command.getKey()).map(value -> new NumericResponse<>(command, value));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisConnection.ReactiveNumberCommands#incrBy(org.reactivestreams.Publisher, java.util.function.Supplier)
	 */
	@Override
	public <T extends Number> Flux<NumericResponse<IncrByCommand<T>, T>> incrBy(Publisher<IncrByCommand<T>> commands) {

		return connection.execute(cmd -> Flux.from(commands).flatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null!");
			Assert.notNull(command.getValue(), "Value for INCRBY must not be null.");

			T incrBy = command.getValue();

			Mono<? extends Number> result = null;
			if (incrBy instanceof Double || incrBy instanceof Float) {
				result = cmd.incrbyfloat(command.getKey(), incrBy.doubleValue());
			} else {
				result = cmd.incrby(command.getKey(), incrBy.longValue());
			}

			return result.map(val -> NumberUtils.convertNumberToTargetClass(val, incrBy.getClass()))
					.map(res -> new NumericResponse<>(command, (T) res));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisConnection.ReactiveNumberCommands#decr(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<NumericResponse<KeyCommand, Long>> decr(Publisher<KeyCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).flatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null!");

			return cmd.decr(command.getKey()).map(value -> new NumericResponse<>(command, value));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisConnection.ReactiveNumberCommands#decrBy(org.reactivestreams.Publisher, java.util.function.Supplier)
	 */
	@Override
	public <T extends Number> Flux<NumericResponse<DecrByCommand<T>, T>> decrBy(Publisher<DecrByCommand<T>> commands) {

		return connection.execute(cmd -> Flux.from(commands).flatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null!");
			Assert.notNull(command.getValue(), "Value for DECRBY must not be null.");

			T decrBy = command.getValue();

			Mono<? extends Number> result = null;
			if (decrBy instanceof Double || decrBy instanceof Float) {
				result = cmd.incrbyfloat(command.getKey(), decrBy.doubleValue() * (-1.0D));
			} else {
				result = cmd.decrby(command.getKey(), decrBy.longValue());
			}

			return result.map(val -> NumberUtils.convertNumberToTargetClass(val, decrBy.getClass()))
					.map(res -> new NumericResponse<>(command, (T) res));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveNumberCommands#hIncrBy(org.reactivestreams.Publisher)
	 */
	@Override
	public <T extends Number> Flux<NumericResponse<HIncrByCommand<T>, T>> hIncrBy(Publisher<HIncrByCommand<T>> commands) {

		return connection.execute(cmd -> Flux.from(commands).flatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null!");
			Assert.notNull(command.getValue(), "Value must not be null!");

			T incrBy = command.getValue();

			Mono<? extends Number> result = null;

			if (incrBy instanceof Double || incrBy instanceof Float) {
				result = cmd.hincrbyfloat(command.getKey(), command.getField(), incrBy.doubleValue());
			} else {
				result = cmd.hincrby(command.getKey(), command.getField(), incrBy.longValue());
			}

			return result.map(val -> NumberUtils.convertNumberToTargetClass(val, incrBy.getClass()))
					.map(value -> new NumericResponse<>(command, (T) value));
		}));
	}

}
