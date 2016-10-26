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
import java.time.temporal.ChronoUnit;
import java.util.Arrays;

import org.reactivestreams.Publisher;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.connection.ReactiveListCommands;
import org.springframework.data.redis.connection.ReactiveRedisConnection.BooleanResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.ByteBufferResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.KeyCommand;
import org.springframework.data.redis.connection.ReactiveRedisConnection.MultiValueResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.NumericResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.RangeCommand;
import org.springframework.data.redis.connection.RedisListCommands.Position;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.0
 */
public class LettuceReactiveListCommands implements ReactiveListCommands {

	private final LettuceReactiveRedisConnection connection;

	/**
	 * Create new {@link LettuceReactiveListCommands}.
	 *
	 * @param connection must not be {@literal null}.
	 */
	public LettuceReactiveListCommands(LettuceReactiveRedisConnection connection) {

		Assert.notNull(connection, "Connection must not be null!");
		this.connection = connection;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveListCommands#lPush(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<NumericResponse<PushCommand, Long>> push(Publisher<PushCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).flatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null!");
			Assert.notEmpty(command.getValues(), "Values must not be null or empty!");

			if (!command.getUpsert() && command.getValues().size() > 1) {
				throw new InvalidDataAccessApiUsageException(
						String.format("%s PUSHX only allows one value!", command.getDirection()));
			}

			Mono<Long> pushResult = null;

			if (ObjectUtils.nullSafeEquals(Direction.RIGHT, command.getDirection())) {
				pushResult = command.getUpsert()
						? cmd.rpush(command.getKey(), command.getValues().stream().toArray(ByteBuffer[]::new))
						: cmd.rpushx(command.getKey(), command.getValues().get(0));
			} else {
				pushResult = command.getUpsert()
						? cmd.lpush(command.getKey(), command.getValues().stream().toArray(ByteBuffer[]::new))
						: cmd.lpushx(command.getKey(), command.getValues().get(0));
			}

			return pushResult.map(value -> new NumericResponse<>(command, value));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveListCommands#lLen(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<NumericResponse<KeyCommand, Long>> lLen(Publisher<KeyCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).flatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null!");

			return cmd.llen(command.getKey()).map(value -> new NumericResponse<>(command, value));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveListCommands#lRange(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<MultiValueResponse<RangeCommand, ByteBuffer>> lRange(Publisher<RangeCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).flatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null!");
			Assert.notNull(command.getRange(), "Range must not be null!");

			return cmd.lrange(command.getKey(), command.getRange().getLowerBound(), command.getRange().getUpperBound())
					.collectList().map(value -> new MultiValueResponse<>(command, value));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveListCommands#lTrim(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<BooleanResponse<RangeCommand>> lTrim(Publisher<RangeCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).flatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null!");
			Assert.notNull(command.getRange(), "Range must not be null!");

			return cmd.ltrim(command.getKey(), command.getRange().getLowerBound(), command.getRange().getUpperBound())
					.map(LettuceConverters::stringToBoolean).map(value -> new BooleanResponse<>(command, value));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveListCommands#lIndex(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<ByteBufferResponse<LIndexCommand>> lIndex(Publisher<LIndexCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).flatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null!");
			Assert.notNull(command.getIndex(), "Index value must not be null!");

			return cmd.lindex(command.getKey(), command.getIndex()).map(value -> new ByteBufferResponse<>(command, value));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveListCommands#lInsert(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<NumericResponse<LInsertCommand, Long>> lInsert(Publisher<LInsertCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).flatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null!");
			Assert.notNull(command.getValue(), "Value must not be null!");
			Assert.notNull(command.getPivot(), "Pivot must not be null!");
			Assert.notNull(command.getPosition(), "Position must not be null!");

			return cmd.linsert(command.getKey(), Position.BEFORE.equals(command.getPosition()), command.getPivot(),
					command.getValue()).map(value -> new NumericResponse<>(command, value));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveListCommands#lSet(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<BooleanResponse<LSetCommand>> lSet(Publisher<LSetCommand> commands) {

		return connection.execute(cmd -> {

			return Flux.from(commands).flatMap(command -> {

				Assert.notNull(command.getKey(), "Key must not be null!");
				Assert.notNull(command.getValue(), "value must not be null!");
				Assert.notNull(command.getIndex(), "Index must not be null!");

				return cmd.lset(command.getKey(), command.getIndex(), command.getValue())
						.map(LettuceConverters::stringToBoolean).map(value -> new BooleanResponse<>(command, value));
			});
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveListCommands#lRem(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<NumericResponse<LRemCommand, Long>> lRem(Publisher<LRemCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).flatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null!");
			Assert.notNull(command.getValue(), "Value must not be null!");
			Assert.notNull(command.getCount(), "Count must not be null!");

			return cmd.lrem(command.getKey(), command.getCount(), command.getValue())
					.map(value -> new NumericResponse<>(command, value));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveListCommands#rPop(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<ByteBufferResponse<PopCommand>> pop(Publisher<PopCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).flatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null!");
			Assert.notNull(command.getDirection(), "Direction must not be null!");

			Mono<ByteBuffer> popResult = ObjectUtils.nullSafeEquals(Direction.RIGHT, command.getDirection())
					? cmd.rpop(command.getKey()) : cmd.lpop(command.getKey());

			return popResult.map(value -> new ByteBufferResponse<>(command, value));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveListCommands#bPop(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<PopResponse> bPop(Publisher<BPopCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).flatMap(command -> {

			Assert.notNull(command.getKeys(), "Keys must not be null!");
			Assert.notNull(command.getDirection(), "Direction must not be null!");

			long timeout = command.getTimeout().get(ChronoUnit.SECONDS);

			Mono<PopResult> mappedMono = (ObjectUtils.nullSafeEquals(Direction.RIGHT, command.getDirection())
					? cmd.brpop(timeout, command.getKeys().stream().toArray(ByteBuffer[]::new))
					: cmd.blpop(timeout, command.getKeys().stream().toArray(ByteBuffer[]::new)))
							.map(kv -> Arrays.asList(kv.getKey(), kv.getValue())).map(PopResult::new);

			return mappedMono.map(value -> new PopResponse(command, value));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveListCommands#rPopLPush(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<ByteBufferResponse<RPopLPushCommand>> rPopLPush(Publisher<RPopLPushCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).flatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null!");
			Assert.notNull(command.getDestination(), "Destination key must not be null!");

			return cmd.rpoplpush(command.getKey(), command.getDestination())
					.map(value -> new ByteBufferResponse<>(command, value));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveListCommands#bRPopLPush(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<ByteBufferResponse<BRPopLPushCommand>> bRPopLPush(Publisher<BRPopLPushCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).flatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null!");
			Assert.notNull(command.getDestination(), "Destination key must not be null!");
			Assert.notNull(command.getTimeout(), "Timeout must not be null!");

			return cmd.brpoplpush(command.getTimeout().get(ChronoUnit.SECONDS), command.getKey(), command.getDestination())

					.map(value -> new ByteBufferResponse<>(command, value));
		}));
	}

	protected LettuceReactiveRedisConnection getConnection() {
		return connection;
	}
}
