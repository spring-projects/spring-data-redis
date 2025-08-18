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

import io.lettuce.core.KeyValue;
import io.lettuce.core.Limit;
import io.lettuce.core.Range;
import io.lettuce.core.ScanStream;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.Value;
import io.lettuce.core.ZAddArgs;
import io.lettuce.core.ZStoreArgs;
import org.springframework.data.redis.connection.zset.RankAndScore;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.jspecify.annotations.Nullable;
import org.reactivestreams.Publisher;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.redis.connection.ReactiveRedisConnection.CommandResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.KeyCommand;
import org.springframework.data.redis.connection.ReactiveRedisConnection.KeyScanCommand;
import org.springframework.data.redis.connection.ReactiveRedisConnection.MultiValueResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.NumericResponse;
import org.springframework.data.redis.connection.ReactiveZSetCommands;
import org.springframework.data.redis.connection.zset.Aggregate;
import org.springframework.data.redis.connection.zset.DefaultTuple;
import org.springframework.data.redis.connection.zset.Tuple;
import org.springframework.data.redis.core.TimeoutUtils;
import org.springframework.data.redis.util.ByteUtils;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

/**
 * {@link ReactiveZSetCommands} implementation for {@literal Lettuce}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Michele Mancioppi
 * @author Andrey Shlykov
 * @author John Blum
 * @author Seongil Kim
 * @since 2.0
 */
class LettuceReactiveZSetCommands implements ReactiveZSetCommands {

	private final LettuceReactiveRedisConnection connection;

	/**
	 * Create new {@link LettuceReactiveZSetCommands}.
	 *
	 * @param connection must not be {@literal null}.
	 */
	LettuceReactiveZSetCommands(LettuceReactiveRedisConnection connection) {

		Assert.notNull(connection, "Connection must not be null");

		this.connection = connection;
	}

	@Override
	@SuppressWarnings("unchecked")
	public Flux<NumericResponse<ZAddCommand, Number>> zAdd(Publisher<ZAddCommand> commands) {

		return this.connection.execute(reactiveCommands -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null");
			Assert.notEmpty(command.getTuples(), "Tuples must not be empty or null");

			ZAddArgs args = null;

			if (command.isIncr()) {

				if (command.getTuples().size() > 1) {
					throw new IllegalArgumentException("ZADD INCR must not contain more than one tuple");
				}

				Tuple tuple = command.getTuples().iterator().next();

				return reactiveCommands.zaddincr(command.getKey(), tuple.getScore(), ByteBuffer.wrap(tuple.getValue()))
						.map(value -> new NumericResponse<>(command, value));
			}

			if (command.isReturnTotalChanged()) {
				args = ZAddArgs.Builder.ch();
			}

			if (command.isIfNotExists()) {
				args = args == null ? ZAddArgs.Builder.nx() : args.nx();
			} else if (command.isIfExists()) {
				args = args == null ? ZAddArgs.Builder.xx() : args.xx();
			}

			if (command.isGt()) {
				args = args == null ? ZAddArgs.Builder.gt() : args.gt();
			} else if (command.isLt()) {
				args = args == null ? ZAddArgs.Builder.lt() : args.lt();
			}

			ScoredValue<ByteBuffer>[] values = (ScoredValue<ByteBuffer>[]) command.getTuples().stream()
					.map(tuple -> ScoredValue.fromNullable(tuple.getScore(), ByteBuffer.wrap(tuple.getValue())))
					.toArray(ScoredValue[]::new);

			Mono<Long> result = args == null ? reactiveCommands.zadd(command.getKey(), values)
					: reactiveCommands.zadd(command.getKey(), args, values);

			return result.map(value -> new NumericResponse<>(command, value));
		}));
	}

	@Override
	public Flux<NumericResponse<ZRemCommand, Long>> zRem(Publisher<ZRemCommand> commands) {

		return this.connection.execute(reactiveCommands -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null");
			Assert.notEmpty(command.getValues(), "Values must not be null or empty");

			ByteBuffer[] values = command.getValues().toArray(ByteBuffer[]::new);

			return reactiveCommands.zrem(command.getKey(), values).map(value -> new NumericResponse<>(command, value));
		}));
	}

	@Override
	public Flux<NumericResponse<ZIncrByCommand, Double>> zIncrBy(Publisher<ZIncrByCommand> commands) {

		return this.connection.execute(reactiveCommands -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null");
			Assert.notNull(command.getValue(), "Member must not be null");
			Assert.notNull(command.getIncrement(), "Increment value must not be null");

			return reactiveCommands.zincrby(command.getKey(), command.getIncrement().doubleValue(), command.getValue())
					.map(value -> new NumericResponse<>(command, value));
		}));
	}

	@Override
	public Flux<CommandResponse<ZRandMemberCommand, Flux<ByteBuffer>>> zRandMember(
			Publisher<ZRandMemberCommand> commands) {

		return this.connection.execute(reactiveCommands -> Flux.from(commands).map(command -> {

			Assert.notNull(command.getKey(), "Key must not be null");

			return new CommandResponse<>(command, reactiveCommands.zrandmember(command.getKey(), command.getCount()));
		}));
	}

	@Override
	public Flux<CommandResponse<ZRandMemberCommand, Flux<Tuple>>> zRandMemberWithScore(
			Publisher<ZRandMemberCommand> commands) {

		return this.connection.execute(reactiveCommands -> Flux.from(commands).map(command -> {

			Assert.notNull(command.getKey(), "Key must not be null");

			Flux<ScoredValue<ByteBuffer>> result = reactiveCommands.zrandmemberWithScores(command.getKey(),
					command.getCount());

			return new CommandResponse<>(command, result.map(this::toTuple));
		}));
	}

	@Override
	public Flux<NumericResponse<ZRankCommand, Long>> zRank(Publisher<ZRankCommand> commands) {

		return this.connection.execute(reactiveCommands -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null");
			Assert.notNull(command.getValue(), "Value must not be null");

			Mono<Long> result = ObjectUtils.nullSafeEquals(command.getDirection(), Direction.ASC)
					? reactiveCommands.zrank(command.getKey(), command.getValue())
					: reactiveCommands.zrevrank(command.getKey(), command.getValue());

			return result.map(value -> new NumericResponse<>(command, value));
		}));
	}

	@Override
	public Flux<CommandResponse<ZRankCommand, RankAndScore>> zRankWithScore(Publisher<ZRankCommand> commands) {

		return this.connection.execute(reactiveCommands -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null");
			Assert.notNull(command.getValue(), "Value must not be null");

			Mono<RankAndScore> result = ObjectUtils.nullSafeEquals(command.getDirection(), Direction.ASC)
					? reactiveCommands.zrankWithScore(command.getKey(), command.getValue()).map(this::toRankAndScore)
					: reactiveCommands.zrevrankWithScore(command.getKey(), command.getValue()).map(this::toRankAndScore);

			return result.map(value -> new CommandResponse<>(command, value));
		}));
	}

	@Override
	public Flux<CommandResponse<ZRangeCommand, Flux<Tuple>>> zRange(Publisher<ZRangeCommand> commands) {

		return this.connection.execute(reactiveCommands -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null");
			Assert.notNull(command.getRange(), "Range must not be null");

			Flux<Tuple> result;

			long start = LettuceConverters.getLowerBoundIndex(command.getRange());
			long stop = LettuceConverters.getUpperBoundIndex(command.getRange());

			if (ObjectUtils.nullSafeEquals(command.getDirection(), Direction.ASC)) {
				if (command.isWithScores()) {
					result = reactiveCommands.zrangeWithScores(command.getKey(), start, stop).map(this::toTuple);
				} else {
					result = reactiveCommands.zrange(command.getKey(), start, stop).map(value -> toTuple(value, Double.NaN));
				}
			} else {
				if (command.isWithScores()) {
					result = reactiveCommands.zrevrangeWithScores(command.getKey(), start, stop).map(this::toTuple);
				} else {
					result = reactiveCommands.zrevrange(command.getKey(), start, stop).map(value -> toTuple(value, Double.NaN));
				}
			}

			return Mono.just(new CommandResponse<>(command, result));
		}));
	}

	@Override
	@SuppressWarnings("unchecked")
	public Flux<CommandResponse<ZRangeStoreCommand, Mono<Long>>> zRangeStore(Publisher<ZRangeStoreCommand> commands) {

		return this.connection.execute(reactiveCommands -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKey(), "Source key must not be null");
			Assert.notNull(command.getDestKey(), "Destination key must not be null");
			Assert.notNull(command.getRange(), "Range must not be null");
			Assert.notNull(command.getLimit(), "Limit must not be null");

			Limit limit = LettuceConverters.toLimit(command.getLimit());
			Mono<Long> result;

			if (command.getDirection() == Direction.ASC) {

				switch (command.getRangeMode()) {
					case ByScore -> result = reactiveCommands.zrangestorebyscore(command.getDestKey(), command.getKey(),
							(Range<? extends Number>) LettuceConverters.toRange(command.getRange()), limit);
					case ByLex -> result = reactiveCommands.zrangestorebylex(command.getDestKey(), command.getKey(),
							RangeConverter.toRange(command.getRange()), limit);
					default -> throw new IllegalStateException("Unsupported value: " + command.getRangeMode());
				}
			} else {
				switch (command.getRangeMode()) {
					case ByScore -> result = reactiveCommands.zrevrangestorebyscore(command.getDestKey(), command.getKey(),
							(Range<? extends Number>) LettuceConverters.toRange(command.getRange()), limit);
					case ByLex -> result = reactiveCommands.zrevrangestorebylex(command.getDestKey(), command.getKey(),
							RangeConverter.toRange(command.getRange()), limit);
					default -> throw new IllegalStateException("Unsupported value: " + command.getRangeMode());
				}
			}

			return Mono.just(new CommandResponse<>(command, result));
		}));
	}

	@Override
	public Flux<CommandResponse<ZRangeByScoreCommand, Flux<Tuple>>> zRangeByScore(
			Publisher<ZRangeByScoreCommand> commands) {

		return this.connection.execute(reactiveCommands -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null");
			Assert.notNull(command.getRange(), "Range must not be null");

			boolean isLimited = command.getLimit().isPresent() && !command.getLimit().get().isUnlimited();

			Publisher<Tuple> result;

			if (ObjectUtils.nullSafeEquals(command.getDirection(), Direction.ASC)) {

				Range<? extends Number> range = RangeConverter.toRange(command.getRange());

				if (command.isWithScores()) {
					if (!isLimited) {
						result = reactiveCommands.zrangebyscoreWithScores(command.getKey(), range).map(this::toTuple);
					} else {
						result = reactiveCommands
								.zrangebyscoreWithScores(command.getKey(), range, LettuceConverters.toLimit(command.getLimit().get()))
								.map(this::toTuple);
					}
				} else {
					if (!isLimited) {
						result = reactiveCommands.zrangebyscore(command.getKey(), range).map(value -> toTuple(value, Double.NaN));
					} else {

						result = reactiveCommands
								.zrangebyscore(command.getKey(), range, LettuceConverters.toLimit(command.getLimit().get()))
								.map(value -> toTuple(value, Double.NaN));
					}
				}
			} else {

				Range<? extends Number> range = RangeConverter.toRange(command.getRange());

				if (command.isWithScores()) {
					if (!isLimited) {
						result = reactiveCommands.zrevrangebyscoreWithScores(command.getKey(), range).map(this::toTuple);
					} else {
						result = reactiveCommands.zrevrangebyscoreWithScores(command.getKey(), range,
								LettuceConverters.toLimit(command.getLimit().get())).map(this::toTuple);
					}
				} else {
					if (!isLimited) {
						result = reactiveCommands.zrevrangebyscore(command.getKey(), range)
								.map(value -> toTuple(value, Double.NaN));
					} else {
						result = reactiveCommands
								.zrevrangebyscore(command.getKey(), range, LettuceConverters.toLimit(command.getLimit().get()))
								.map(value -> toTuple(value, Double.NaN));
					}
				}
			}

			return Mono.just(new CommandResponse<>(command, Flux.from(result)));
		}));
	}

	@Override
	public Flux<CommandResponse<KeyCommand, Flux<Tuple>>> zScan(Publisher<KeyScanCommand> commands) {

		return this.connection.execute(reactiveCommands -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null");
			Assert.notNull(command.getOptions(), "ScanOptions must not be null");

			Flux<Tuple> result = ScanStream
					.zscan(reactiveCommands, command.getKey(), LettuceConverters.toScanArgs(command.getOptions()))
					.map(this::toTuple);

			return Mono.just(new CommandResponse<>(command, result));
		}));
	}

	@Override
	public Flux<NumericResponse<ZCountCommand, Long>> zCount(Publisher<ZCountCommand> commands) {

		return this.connection.execute(reactiveCommands -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null");
			Assert.notNull(command.getRange(), "Range must not be null");

			Range<? extends Number> range = RangeConverter.toRange(command.getRange());
			Mono<Long> result = reactiveCommands.zcount(command.getKey(), range);

			return result.map(value -> new NumericResponse<>(command, value));
		}));
	}

	@Override
	public Flux<NumericResponse<ZLexCountCommand, Long>> zLexCount(Publisher<ZLexCountCommand> commands) {

		return this.connection.execute(reactiveCommands -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null");
			Assert.notNull(command.getRange(), "Range must not be null");

			Mono<Long> result = reactiveCommands.zlexcount(command.getKey(), RangeConverter.toRange(command.getRange()));

			return result.map(value -> new NumericResponse<>(command, value));
		}));
	}

	@Override
	public Flux<CommandResponse<ZPopCommand, Flux<Tuple>>> zPop(Publisher<ZPopCommand> commands) {

		return this.connection.execute(reactiveCommands -> Flux.from(commands).map(command -> {

			Assert.notNull(command.getKey(), "Key must not be null");

			Flux<ScoredValue<ByteBuffer>> result;

			if (command.getCount() > 1) {
				result = command.getDirection() == PopDirection.MIN
						? reactiveCommands.zpopmin(command.getKey(), command.getCount())
						: reactiveCommands.zpopmax(command.getKey(), command.getCount());
			} else {
				result = (command.getDirection() == PopDirection.MIN ? reactiveCommands.zpopmin(command.getKey())
						: reactiveCommands.zpopmax(command.getKey())).flux();
			}

			return new CommandResponse<>(command, result.filter(Value::hasValue).map(this::toTuple));
		}));
	}

	@Override
	@SuppressWarnings("NullAway")
	public Flux<CommandResponse<BZPopCommand, Flux<Tuple>>> bZPop(Publisher<BZPopCommand> commands) {

		return this.connection.execute(reactiveCommands -> Flux.from(commands).map(command -> {

			Assert.notNull(command.getKey(), "Key must not be null");
			Assert.notNull(command.getTimeout(), "Timeout must not be null");

			if (command.getTimeUnit() == TimeUnit.MILLISECONDS) {

				double timeout = TimeoutUtils.toDoubleSeconds(command.getTimeout(), command.getTimeUnit());

				Mono<KeyValue<ByteBuffer, ScoredValue<ByteBuffer>>> commandResult = command.getDirection() == PopDirection.MIN
						? reactiveCommands.bzpopmin(timeout, command.getKey())
						: reactiveCommands.bzpopmax(timeout, command.getKey());

				Mono<ScoredValue<ByteBuffer>> result = commandResult.filter(Value::hasValue).map(Value::getValue);

				return new CommandResponse<>(command, result.filter(Value::hasValue).map(this::toTuple).flux());
			} else {

				long timeout = command.getTimeUnit().toSeconds(command.getTimeout());

				Mono<KeyValue<ByteBuffer, ScoredValue<ByteBuffer>>> commandResult = command.getDirection() == PopDirection.MIN
						? reactiveCommands.bzpopmin(timeout, command.getKey())
						: reactiveCommands.bzpopmax(timeout, command.getKey());

				Mono<ScoredValue<ByteBuffer>> result = commandResult.filter(Value::hasValue).map(Value::getValue);

				return new CommandResponse<>(command, result.filter(Value::hasValue).map(this::toTuple).flux());
			}
		}));
	}

	@Override
	public Flux<NumericResponse<KeyCommand, Long>> zCard(Publisher<KeyCommand> commands) {

		return this.connection.execute(reactiveCommands -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null");

			return reactiveCommands.zcard(command.getKey()).map(value -> new NumericResponse<>(command, value));
		}));
	}

	@Override
	public Flux<NumericResponse<ZScoreCommand, Double>> zScore(Publisher<ZScoreCommand> commands) {

		return this.connection.execute(reactiveCommands -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null");
			Assert.notNull(command.getValue(), "Value must not be null");

			return reactiveCommands.zscore(command.getKey(), command.getValue())
					.map(value -> new NumericResponse<>(command, value));
		}));
	}

	@Override
	public Flux<MultiValueResponse<ZMScoreCommand, Double>> zMScore(Publisher<ZMScoreCommand> commands) {

		return this.connection.execute(reactiveCommands -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null");
			Assert.notNull(command.getValues(), "Values must not be null");

			return reactiveCommands.zmscore(command.getKey(), command.getValues().toArray(new ByteBuffer[0]))
					.map(value -> new MultiValueResponse<>(command, value));
		}));
	}

	@Override
	public Flux<NumericResponse<ZRemRangeByRankCommand, Long>> zRemRangeByRank(
			Publisher<ZRemRangeByRankCommand> commands) {

		return this.connection.execute(reactiveCommands -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null");
			Assert.notNull(command.getRange(), "Range must not be null");

			Mono<Long> result = reactiveCommands.zremrangebyrank(command.getKey(), //
					LettuceConverters.getLowerBoundIndex(command.getRange()), //
					LettuceConverters.getUpperBoundIndex(command.getRange()));

			return result.map(value -> new NumericResponse<>(command, value));
		}));
	}

	@Override
	public Flux<NumericResponse<ZRemRangeByScoreCommand, Long>> zRemRangeByScore(
			Publisher<ZRemRangeByScoreCommand> commands) {

		return this.connection.execute(reactiveCommands -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null");
			Assert.notNull(command.getRange(), "Range must not be null");

			Range<? extends Number> range = RangeConverter.toRange(command.getRange());
			Mono<Long> result = reactiveCommands.zremrangebyscore(command.getKey(), range);

			return result.map(value -> new NumericResponse<>(command, value));
		}));
	}

	@Override
	public Flux<NumericResponse<ZRemRangeByLexCommand, Long>> zRemRangeByLex(Publisher<ZRemRangeByLexCommand> commands) {

		return this.connection.execute(reactiveCommands -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null");
			Assert.notNull(command.getRange(), "Range must not be null");

			Mono<Long> result = reactiveCommands.zremrangebylex(command.getKey(), RangeConverter.toRange(command.getRange()));

			return result.map(value -> new NumericResponse<>(command, value));
		}));
	}

	@Override
	public Flux<CommandResponse<ZDiffCommand, Flux<ByteBuffer>>> zDiff(Publisher<? extends ZDiffCommand> commands) {

		return this.connection.execute(reactiveCommands -> Flux.from(commands).map(command -> {

			Assert.notEmpty(command.getKeys(), "Keys must not be null or empty");

			ByteBuffer[] sourceKeys = command.getKeys().toArray(new ByteBuffer[0]);

			return new CommandResponse<>(command, reactiveCommands.zdiff(sourceKeys));
		}));
	}

	@Override
	public Flux<CommandResponse<ZDiffCommand, Flux<Tuple>>> zDiffWithScores(Publisher<? extends ZDiffCommand> commands) {

		return this.connection.execute(reactiveCommands -> Flux.from(commands).map(command -> {

			Assert.notEmpty(command.getKeys(), "Keys must not be null or empty");

			ByteBuffer[] sourceKeys = command.getKeys().toArray(new ByteBuffer[0]);

			return new CommandResponse<>(command, reactiveCommands.zdiffWithScores(sourceKeys).map(this::toTuple));
		}));
	}

	@Override
	public Flux<NumericResponse<ZDiffStoreCommand, Long>> zDiffStore(Publisher<ZDiffStoreCommand> commands) {

		return this.connection.execute(reactiveCommands -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKey(), "Destination key must not be null");
			Assert.notEmpty(command.getSourceKeys(), "Source keys must not be null or empty");

			ByteBuffer[] sourceKeys = command.getSourceKeys().toArray(new ByteBuffer[0]);

			return reactiveCommands.zdiffstore(command.getKey(), sourceKeys)
					.map(value -> new NumericResponse<>(command, value));
		}));
	}

	@Override
	public Flux<CommandResponse<ZAggregateCommand, Flux<ByteBuffer>>> zInter(
			Publisher<? extends ZAggregateCommand> commands) {

		return this.connection.execute(reactiveCommands -> Flux.from(commands).map(command -> {

			Assert.notEmpty(command.getSourceKeys(), "Source keys must not be null or empty");

			ZStoreArgs args = null;

			if (command.getAggregateFunction().isPresent() || !command.getWeights().isEmpty()) {
				args = zStoreArgs(command.getAggregateFunction().isPresent() ? command.getAggregateFunction().get() : null,
						command.getWeights());
			}

			ByteBuffer[] sourceKeys = command.getSourceKeys().toArray(new ByteBuffer[0]);

			Flux<ByteBuffer> result = args != null ? reactiveCommands.zinter(args, sourceKeys)
					: reactiveCommands.zinter(sourceKeys);

			return new CommandResponse<>(command, result);
		}));
	}

	@Override
	public Flux<CommandResponse<ZAggregateCommand, Flux<Tuple>>> zInterWithScores(
			Publisher<? extends ZAggregateCommand> commands) {

		return this.connection.execute(reactiveCommands -> Flux.from(commands).map(command -> {

			Assert.notEmpty(command.getSourceKeys(), "Source keys must not be null or empty");

			ZStoreArgs args = null;

			if (command.getAggregateFunction().isPresent() || !command.getWeights().isEmpty()) {
				args = zStoreArgs(command.getAggregateFunction().isPresent() ? command.getAggregateFunction().get() : null,
						command.getWeights());
			}

			ByteBuffer[] sourceKeys = command.getSourceKeys().toArray(new ByteBuffer[0]);

			Flux<ScoredValue<ByteBuffer>> result = args != null ? reactiveCommands.zinterWithScores(args, sourceKeys)
					: reactiveCommands.zinterWithScores(sourceKeys);

			return new CommandResponse<>(command, result.map(this::toTuple));
		}));
	}

	@Override
	public Flux<NumericResponse<ZAggregateStoreCommand, Long>> zInterStore(
			Publisher<? extends ZAggregateStoreCommand> commands) {

		return this.connection.execute(reactiveCommands -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKey(), "Destination key must not be null");
			Assert.notEmpty(command.getSourceKeys(), "Source keys must not be null or empty");

			ZStoreArgs args = null;

			if (command.getAggregateFunction().isPresent() || !command.getWeights().isEmpty()) {
				args = zStoreArgs(command.getAggregateFunction().isPresent() ? command.getAggregateFunction().get() : null,
						command.getWeights());
			}

			ByteBuffer[] sourceKeys = command.getSourceKeys().toArray(new ByteBuffer[0]);

			Mono<Long> result = args != null ? reactiveCommands.zinterstore(command.getKey(), args, sourceKeys)
					: reactiveCommands.zinterstore(command.getKey(), sourceKeys);

			return result.map(value -> new NumericResponse<>(command, value));
		}));
	}

	@Override
	public Flux<CommandResponse<ZAggregateCommand, Flux<ByteBuffer>>> zUnion(
			Publisher<? extends ZAggregateCommand> commands) {

		return this.connection.execute(reactiveCommands -> Flux.from(commands).map(command -> {

			Assert.notEmpty(command.getSourceKeys(), "Source keys must not be null or empty");

			ZStoreArgs args = null;

			if (command.getAggregateFunction().isPresent() || !command.getWeights().isEmpty()) {
				args = zStoreArgs(command.getAggregateFunction().isPresent() ? command.getAggregateFunction().get() : null,
						command.getWeights());
			}

			ByteBuffer[] sourceKeys = command.getSourceKeys().toArray(ByteBuffer[]::new);

			Flux<ByteBuffer> result = args != null ? reactiveCommands.zunion(args, sourceKeys)
					: reactiveCommands.zunion(sourceKeys);

			return new CommandResponse<>(command, result);
		}));
	}

	@Override
	public Flux<CommandResponse<ZAggregateCommand, Flux<Tuple>>> zUnionWithScores(
			Publisher<? extends ZAggregateCommand> commands) {

		return this.connection.execute(reactiveCommands -> Flux.from(commands).map(command -> {

			Assert.notEmpty(command.getSourceKeys(), "Source keys must not be null or empty");

			ZStoreArgs args = null;

			if (command.getAggregateFunction().isPresent() || !command.getWeights().isEmpty()) {
				args = zStoreArgs(command.getAggregateFunction().isPresent() ? command.getAggregateFunction().get() : null,
						command.getWeights());
			}

			ByteBuffer[] sourceKeys = command.getSourceKeys().toArray(ByteBuffer[]::new);

			Flux<ScoredValue<ByteBuffer>> result = args != null ? reactiveCommands.zunionWithScores(args, sourceKeys)
					: reactiveCommands.zunionWithScores(sourceKeys);

			return new CommandResponse<>(command, result.map(this::toTuple));
		}));
	}

	@Override
	public Flux<NumericResponse<ZAggregateStoreCommand, Long>> zUnionStore(
			Publisher<? extends ZAggregateStoreCommand> commands) {

		return this.connection.execute(reactiveCommands -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKey(), "Destination key must not be null");
			Assert.notEmpty(command.getSourceKeys(), "Source keys must not be null or empty");

			ZStoreArgs args = null;

			if (command.getAggregateFunction().isPresent() || !command.getWeights().isEmpty()) {
				args = zStoreArgs(command.getAggregateFunction().isPresent() ? command.getAggregateFunction().get() : null,
						command.getWeights());
			}

			ByteBuffer[] sourceKeys = command.getSourceKeys().toArray(ByteBuffer[]::new);

			Mono<Long> result = args != null ? reactiveCommands.zunionstore(command.getKey(), args, sourceKeys)
					: reactiveCommands.zunionstore(command.getKey(), sourceKeys);

			return result.map(value -> new NumericResponse<>(command, value));
		}));
	}

	@Override
	public Flux<CommandResponse<ZRangeByLexCommand, Flux<ByteBuffer>>> zRangeByLex(
			Publisher<ZRangeByLexCommand> commands) {

		return this.connection.execute(reactiveCommands -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKey(), "Destination key must not be null");

			Flux<ByteBuffer> result;

			if (!command.getLimit().isUnlimited()) {
				if (ObjectUtils.nullSafeEquals(command.getDirection(), Direction.ASC)) {
					result = reactiveCommands.zrangebylex(command.getKey(), RangeConverter.toRange(command.getRange()),
							LettuceConverters.toLimit(command.getLimit()));
				} else {
					result = reactiveCommands.zrevrangebylex(command.getKey(), RangeConverter.toRange(command.getRange()),
							LettuceConverters.toLimit(command.getLimit()));
				}
			} else {
				if (ObjectUtils.nullSafeEquals(command.getDirection(), Direction.ASC)) {
					result = reactiveCommands.zrangebylex(command.getKey(), RangeConverter.toRange(command.getRange()));
				} else {
					result = reactiveCommands.zrevrangebylex(command.getKey(), RangeConverter.toRange(command.getRange()));
				}
			}

			return Mono.just(new CommandResponse<>(command, result));
		}));
	}

	private static ZStoreArgs zStoreArgs(@Nullable Aggregate aggregate, @Nullable List<Double> weights) {

		ZStoreArgs args = new ZStoreArgs();

		if (aggregate != null) {
			switch (aggregate) {
				case MIN -> args.min();
				case MAX -> args.max();
				default -> args.sum();
			}
		}

		if (weights != null) {
			args.weights(weights.stream().mapToDouble(it -> it).toArray());
		}

		return args;
	}

	private Tuple toTuple(ScoredValue<ByteBuffer> scoredValue) {
		return scoredValue.map(it -> new DefaultTuple(ByteUtils.getBytes(it), scoredValue.getScore())).getValue();
	}

	private Tuple toTuple(ByteBuffer value, double score) {
		return new DefaultTuple(ByteUtils.getBytes(value), score);
	}

	private RankAndScore toRankAndScore(ScoredValue<Long> scoredValue) {
		return new RankAndScore(scoredValue.getValue(), scoredValue.getScore());
	}

	protected LettuceReactiveRedisConnection getConnection() {
		return this.connection;
	}
}
