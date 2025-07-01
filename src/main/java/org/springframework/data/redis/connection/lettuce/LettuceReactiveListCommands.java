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

import io.lettuce.core.LMoveArgs;
import io.lettuce.core.LPosArgs;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.reactivestreams.Publisher;

import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.ReactiveListCommands;
import org.springframework.data.redis.connection.ReactiveRedisConnection.BooleanResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.ByteBufferResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.CommandResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.KeyCommand;
import org.springframework.data.redis.connection.ReactiveRedisConnection.NumericResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.RangeCommand;
import org.springframework.data.redis.connection.RedisListCommands.Position;
import org.springframework.data.redis.core.TimeoutUtils;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Michele Mancioppi
 * @author dengliming
 * @since 2.0
 */
class LettuceReactiveListCommands implements ReactiveListCommands {

	private final LettuceReactiveRedisConnection connection;

	/**
	 * Create new {@link LettuceReactiveListCommands}.
	 *
	 * @param connection must not be {@literal null}.
	 */
	LettuceReactiveListCommands(LettuceReactiveRedisConnection connection) {

		Assert.notNull(connection, "Connection must not be null");

		this.connection = connection;
	}

	@Override
	public Flux<NumericResponse<PushCommand, Long>> push(Publisher<PushCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null");
			Assert.notEmpty(command.getValues(), "Values must not be null or empty");

			if (!command.getUpsert() && command.getValues().size() > 1) {
				throw new InvalidDataAccessApiUsageException(
						"%s PUSHX only allows one value".formatted(command.getDirection()));
			}

			Mono<Long> pushResult;

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

	@Override
	public Flux<NumericResponse<KeyCommand, Long>> lLen(Publisher<KeyCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null");

			return cmd.llen(command.getKey()).map(value -> new NumericResponse<>(command, value));
		}));
	}

	@Override
	public Flux<CommandResponse<RangeCommand, Flux<ByteBuffer>>> lRange(Publisher<RangeCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null");
			Assert.notNull(command.getRange(), "Range must not be null");

			Range<Long> range = command.getRange();

			Flux<ByteBuffer> result = cmd.lrange(command.getKey(), //
					LettuceConverters.getLowerBoundIndex(range), //
					LettuceConverters.getUpperBoundIndex(range));

			return Mono.just(new CommandResponse<>(command, result));
		}));
	}

	@Override
	public Flux<BooleanResponse<RangeCommand>> lTrim(Publisher<RangeCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null");
			Assert.notNull(command.getRange(), "Range must not be null");

			Range<Long> range = command.getRange();

			Mono<String> result = cmd.ltrim(command.getKey(), //
					LettuceConverters.getLowerBoundIndex(range), //
					LettuceConverters.getUpperBoundIndex(range));

			return result.map(LettuceConverters::stringToBoolean).map(value -> new BooleanResponse<>(command, value));
		}));
	}

	@Override
	public Flux<NumericResponse<LPosCommand, Long>> lPos(Publisher<LPosCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).concatMap(command -> {

			LPosArgs args = new LPosArgs();
			if (command.getRank() != null) {
				args.rank(command.getRank());
			}

			Flux<Long> values;
			if (command.getCount() != null) {
				values = cmd.lpos(command.getKey(), command.getElement(), command.getCount(), args);
			} else {
				values = cmd.lpos(command.getKey(), command.getElement(), args).flux();
			}

			return values.map(value -> new NumericResponse<>(command, value));
		}));
	}

	@Override
	public Flux<ByteBufferResponse<LIndexCommand>> lIndex(Publisher<LIndexCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null");
			Assert.notNull(command.getIndex(), "Index value must not be null");

			return cmd.lindex(command.getKey(), command.getIndex()).map(value -> new ByteBufferResponse<>(command, value));
		}));
	}

	@Override
	public Flux<NumericResponse<LInsertCommand, Long>> lInsert(Publisher<LInsertCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null");
			Assert.notNull(command.getValue(), "Value must not be null");
			Assert.notNull(command.getPivot(), "Pivot must not be null");
			Assert.notNull(command.getPosition(), "Position must not be null");

			return cmd.linsert(command.getKey(), Position.BEFORE.equals(command.getPosition()), command.getPivot(),
					command.getValue()).map(value -> new NumericResponse<>(command, value));
		}));
	}

	@Override
	public Flux<ByteBufferResponse<LMoveCommand>> lMove(Publisher<? extends LMoveCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKey(), "Source key must not be null");
			Assert.notNull(command.getFrom(), "Source direction must not be null");
			Assert.notNull(command.getDestinationKey(), "Destination key must not be null");
			Assert.notNull(command.getTo(), "Destination direction must not be null");

			LMoveArgs lMoveArgs = LettuceConverters.toLmoveArgs(command.getFrom(), command.getTo());

			return cmd.lmove(command.getKey(), command.getDestinationKey(), lMoveArgs)
					.map(value -> new ByteBufferResponse<>(command, value));
		}));
	}

	@Override
	public Flux<ByteBufferResponse<BLMoveCommand>> bLMove(Publisher<BLMoveCommand> commands) {

		return connection.executeDedicated(cmd -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKey(), "Source key must not be null");
			Assert.notNull(command.getFrom(), "Source direction must not be null");
			Assert.notNull(command.getDestinationKey(), "Destination key must not be null");
			Assert.notNull(command.getTo(), "Destination direction must not be null");
			Assert.notNull(command.getTimeout(), "Timeout must not be null");

			LMoveArgs lMoveArgs = LettuceConverters.toLmoveArgs(command.getFrom(), command.getTo());
			double timeout = TimeoutUtils.toDoubleSeconds(command.getTimeout().toMillis(), TimeUnit.MILLISECONDS);

			return cmd.blmove(command.getKey(), command.getDestinationKey(), lMoveArgs, timeout)
					.map(value -> new ByteBufferResponse<>(command, value));
		}));
	}

	@Override
	public Flux<BooleanResponse<LSetCommand>> lSet(Publisher<LSetCommand> commands) {

		return connection.execute(cmd -> {

			return Flux.from(commands).concatMap(command -> {

				Assert.notNull(command.getKey(), "Key must not be null");
				Assert.notNull(command.getValue(), "value must not be null");
				Assert.notNull(command.getIndex(), "Index must not be null");

				return cmd.lset(command.getKey(), command.getIndex(), command.getValue())
						.map(LettuceConverters::stringToBoolean).map(value -> new BooleanResponse<>(command, value));
			});
		});
	}

	@Override
	public Flux<NumericResponse<LRemCommand, Long>> lRem(Publisher<LRemCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null");
			Assert.notNull(command.getValue(), "Value must not be null");
			Assert.notNull(command.getCount(), "Count must not be null");

			return cmd.lrem(command.getKey(), command.getCount(), command.getValue())
					.map(value -> new NumericResponse<>(command, value));
		}));
	}

	@Override
	public Flux<ByteBufferResponse<PopCommand>> pop(Publisher<PopCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null");
			Assert.notNull(command.getDirection(), "Direction must not be null");

			Mono<ByteBuffer> popResult = ObjectUtils.nullSafeEquals(Direction.RIGHT, command.getDirection())
					? cmd.rpop(command.getKey())
					: cmd.lpop(command.getKey());

			return popResult.map(value -> new ByteBufferResponse<>(command, value));
		}));
	}

	@Override
	public Flux<CommandResponse<PopCommand, Flux<ByteBuffer>>> popList(Publisher<PopCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null");
			Assert.notNull(command.getDirection(), "Direction must not be null");

			Flux<ByteBuffer> popResult = ObjectUtils.nullSafeEquals(Direction.RIGHT, command.getDirection())
					? cmd.rpop(command.getKey(), command.getCount())
					: cmd.lpop(command.getKey(), command.getCount());

			return Mono.just(new CommandResponse<>(command, popResult));
		}));
	}

	@Override
	public Flux<PopResponse> bPop(Publisher<BPopCommand> commands) {

		return connection.executeDedicated(cmd -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKeys(), "Keys must not be null");
			Assert.notNull(command.getDirection(), "Direction must not be null");

			long timeout = command.getTimeout().get(ChronoUnit.SECONDS);

			Mono<PopResult> mappedMono = (ObjectUtils.nullSafeEquals(Direction.RIGHT, command.getDirection())
					? cmd.brpop(timeout, command.getKeys().toArray(ByteBuffer[]::new))
					: cmd.blpop(timeout, command.getKeys().toArray(ByteBuffer[]::new)))
							.map(kv -> Arrays.asList(kv.getKey(), kv.getValue())).map(PopResult::new);

			return mappedMono.map(value -> new PopResponse(command, value));
		}));
	}

	@Override
	public Flux<ByteBufferResponse<RPopLPushCommand>> rPopLPush(Publisher<RPopLPushCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null");
			Assert.notNull(command.getDestination(), "Destination key must not be null");

			return cmd.rpoplpush(command.getKey(), command.getDestination())
					.map(value -> new ByteBufferResponse<>(command, value));
		}));
	}

	@Override
	public Flux<ByteBufferResponse<BRPopLPushCommand>> bRPopLPush(Publisher<BRPopLPushCommand> commands) {

		return connection.executeDedicated(cmd -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null");
			Assert.notNull(command.getDestination(), "Destination key must not be null");
			Assert.notNull(command.getTimeout(), "Timeout must not be null");

			return cmd.brpoplpush(command.getTimeout().get(ChronoUnit.SECONDS), command.getKey(), command.getDestination())

					.map(value -> new ByteBufferResponse<>(command, value));
		}));
	}

	protected LettuceReactiveRedisConnection getConnection() {
		return connection;
	}
}
