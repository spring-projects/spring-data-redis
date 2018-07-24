/*
 * Copyright 2016-2018 the original author or authors.
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

import io.lettuce.core.Range;
import io.lettuce.core.Range.Boundary;
import io.lettuce.core.ScanStream;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.ZAddArgs;
import io.lettuce.core.ZStoreArgs;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.protocol.LettuceCharsets;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.util.List;

import org.reactivestreams.Publisher;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.redis.connection.DefaultTuple;
import org.springframework.data.redis.connection.ReactiveRedisConnection.CommandResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.KeyCommand;
import org.springframework.data.redis.connection.ReactiveRedisConnection.KeyScanCommand;
import org.springframework.data.redis.connection.ReactiveRedisConnection.NumericResponse;
import org.springframework.data.redis.connection.ReactiveZSetCommands;
import org.springframework.data.redis.connection.RedisZSetCommands.Aggregate;
import org.springframework.data.redis.connection.RedisZSetCommands.Tuple;
import org.springframework.data.redis.util.ByteUtils;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Michele Mancioppi
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

		Assert.notNull(connection, "Connection must not be null!");

		this.connection = connection;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveZSetCommands#zAdd(org.reactivestreams.Publisher)
	 */
	@Override
	@SuppressWarnings("unchecked")
	public Flux<NumericResponse<ZAddCommand, Number>> zAdd(Publisher<ZAddCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null!");
			Assert.notEmpty(command.getTuples(), "Tuples must not be empty or null!");

			ZAddArgs args = null;

			if (command.isIncr() || command.isUpsert() || command.isReturnTotalChanged()) {

				if (command.isIncr()) {

					if (command.getTuples().size() > 1) {
						throw new IllegalArgumentException("ZADD INCR must not contain more than one tuple!");
					}

					Tuple tuple = command.getTuples().iterator().next();

					return cmd.zaddincr(command.getKey(), tuple.getScore(), ByteBuffer.wrap(tuple.getValue()))
							.map(value -> new NumericResponse<>(command, value));
				}

				if (command.isReturnTotalChanged()) {
					args = ZAddArgs.Builder.ch();
				}

				if (command.isUpsert()) {
					args = ZAddArgs.Builder.nx();
				} else {
					args = ZAddArgs.Builder.xx();
				}
			}

			ScoredValue<ByteBuffer>[] values = (ScoredValue<ByteBuffer>[]) command.getTuples().stream()
					.map(tuple -> ScoredValue.fromNullable(tuple.getScore(), ByteBuffer.wrap(tuple.getValue())))
					.toArray(ScoredValue[]::new);

			Mono<Long> result = args == null ? cmd.zadd(command.getKey(), values) : cmd.zadd(command.getKey(), args, values);

			return result.map(value -> new NumericResponse<>(command, value));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveZSetCommands#zRem(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<NumericResponse<ZRemCommand, Long>> zRem(Publisher<ZRemCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null!");
			Assert.notEmpty(command.getValues(), "Values must not be null or empty!");

			return cmd.zrem(command.getKey(), command.getValues().stream().toArray(ByteBuffer[]::new))
					.map(value -> new NumericResponse<>(command, value));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveZSetCommands#zIncrBy(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<NumericResponse<ZIncrByCommand, Double>> zIncrBy(Publisher<ZIncrByCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null!");
			Assert.notNull(command.getValue(), "Member must not be null!");
			Assert.notNull(command.getIncrement(), "Increment value must not be null!");

			return cmd.zincrby(command.getKey(), command.getIncrement().doubleValue(), command.getValue())
					.map(value -> new NumericResponse<>(command, value));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveZSetCommands#zRank(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<NumericResponse<ZRankCommand, Long>> zRank(Publisher<ZRankCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null!");
			Assert.notNull(command.getValue(), "Value must not be null!");

			Mono<Long> result = ObjectUtils.nullSafeEquals(command.getDirection(), Direction.ASC)
					? cmd.zrank(command.getKey(), command.getValue()) : cmd.zrevrank(command.getKey(), command.getValue());

			return result.map(value -> new NumericResponse<>(command, value));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveZSetCommands#zRange(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<CommandResponse<ZRangeCommand, Flux<Tuple>>> zRange(Publisher<ZRangeCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null!");
			Assert.notNull(command.getRange(), "Range must not be null!");

			Flux<Tuple> result;

			long start = LettuceConverters.getLowerBoundIndex(command.getRange());
			long stop = LettuceConverters.getUpperBoundIndex(command.getRange());

			if (ObjectUtils.nullSafeEquals(command.getDirection(), Direction.ASC)) {
				if (command.isWithScores()) {

					result = cmd
							.zrangeWithScores(command.getKey(), start, stop).map(sc -> new DefaultTuple(getBytes(sc), sc.getScore()));
				} else {

					result = cmd
							.zrange(command.getKey(), start, stop)
							.map(value -> new DefaultTuple(ByteUtils.getBytes(value), Double.NaN));
				}
			} else {
				if (command.isWithScores()) {

					result = cmd
							.zrevrangeWithScores(command.getKey(), start, stop)
							.map(sc -> new DefaultTuple(getBytes(sc), sc.getScore()));
				} else {

					result = cmd
							.zrevrange(command.getKey(), start, stop)
							.map(value -> new DefaultTuple(ByteUtils.getBytes(value), Double.NaN));
				}
			}

			return Mono.just(new CommandResponse<>(command, result));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveZSetCommands#zRange(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<CommandResponse<ZRangeByScoreCommand, Flux<Tuple>>> zRangeByScore(
			Publisher<ZRangeByScoreCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null!");
			Assert.notNull(command.getRange(), "Range must not be null!");

			boolean isLimited = command.getLimit().isPresent() && !command.getLimit().get().isUnlimited();

			Publisher<Tuple> result;

			if (ObjectUtils.nullSafeEquals(command.getDirection(), Direction.ASC)) {

				Range<Number> range = ArgumentConverters.toRange(command.getRange());

				if (command.isWithScores()) {

					if (!isLimited) {
						result = cmd.zrangebyscoreWithScores(command.getKey(), range)
								.map(sc -> new DefaultTuple(ByteUtils.getBytes(sc.getValue()), sc.getScore()));
					} else {
						result = cmd
								.zrangebyscoreWithScores(command.getKey(), range, LettuceConverters.toLimit(command.getLimit().get()))
								.map(sc -> new DefaultTuple(ByteUtils.getBytes(sc.getValue()), sc.getScore()));
					}
				} else {

					if (!isLimited) {
						result = cmd.zrangebyscore(command.getKey(), range)
								.map(value -> new DefaultTuple(ByteUtils.getBytes(value), Double.NaN));
					} else {

						result = cmd.zrangebyscore(command.getKey(), range, LettuceConverters.toLimit(command.getLimit().get()))
								.map(value -> new DefaultTuple(ByteUtils.getBytes(value), Double.NaN));
					}
				}
			} else {

				Range<Number> range = ArgumentConverters.toRange(command.getRange());

				if (command.isWithScores()) {

					if (!isLimited) {
						result = cmd.zrevrangebyscoreWithScores(command.getKey(), range)
								.map(sc -> new DefaultTuple(ByteUtils.getBytes(sc.getValue()), sc.getScore()));
					} else {

						result = cmd
								.zrevrangebyscoreWithScores(command.getKey(), range,
										LettuceConverters.toLimit(command.getLimit().get()))
								.map(sc -> new DefaultTuple(ByteUtils.getBytes(sc.getValue()), sc.getScore()));
					}
				} else {

					if (!isLimited) {
						result = cmd.zrevrangebyscore(command.getKey(), range)
								.map(value -> new DefaultTuple(ByteUtils.getBytes(value), Double.NaN));
					} else {

						result = cmd.zrevrangebyscore(command.getKey(), range, LettuceConverters.toLimit(command.getLimit().get()))
								.map(value -> new DefaultTuple(ByteUtils.getBytes(value), Double.NaN));
					}
				}
			}

			return Mono.just(new CommandResponse<>(command, Flux.from(result)));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveZSetCommands#zScan(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<CommandResponse<KeyCommand, Flux<Tuple>>> zScan(Publisher<KeyScanCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null!");
			Assert.notNull(command.getOptions(), "ScanOptions must not be null!");

			Flux<Tuple> result = ScanStream.zscan(cmd, command.getKey(), LettuceConverters.toScanArgs(command.getOptions()))
					.map(it -> new DefaultTuple(ByteUtils.getBytes(it.getValue()), it.getScore()));

			return Mono.just(new CommandResponse<>(command, result));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveZSetCommands#zCount(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<NumericResponse<ZCountCommand, Long>> zCount(Publisher<ZCountCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null!");
			Assert.notNull(command.getRange(), "Range must not be null!");

			Range<Number> range = ArgumentConverters.toRange(command.getRange());
			Mono<Long> result = cmd.zcount(command.getKey(), range);

			return result.map(value -> new NumericResponse<>(command, value));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveZSetCommands#zCard(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<NumericResponse<KeyCommand, Long>> zCard(Publisher<KeyCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null!");

			return cmd.zcard(command.getKey()).map(value -> new NumericResponse<>(command, value));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveZSetCommands#zScore(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<NumericResponse<ZScoreCommand, Double>> zScore(Publisher<ZScoreCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null!");
			Assert.notNull(command.getValue(), "Value must not be null!");

			return cmd.zscore(command.getKey(), command.getValue()).map(value -> new NumericResponse<>(command, value));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveZSetCommands#zRemRangeByRank(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<NumericResponse<ZRemRangeByRankCommand, Long>> zRemRangeByRank(
			Publisher<ZRemRangeByRankCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null!");
			Assert.notNull(command.getRange(), "Range must not be null!");

			Mono<Long> result = cmd.zremrangebyrank(command.getKey(), //
					LettuceConverters.getLowerBoundIndex(command.getRange()), //
					LettuceConverters.getUpperBoundIndex(command.getRange()));

			return result
					.map(value -> new NumericResponse<>(command, value));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveZSetCommands#zRemRangeByRank(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<NumericResponse<ZRemRangeByScoreCommand, Long>> zRemRangeByScore(
			Publisher<ZRemRangeByScoreCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null!");
			Assert.notNull(command.getRange(), "Range must not be null!");

			Range<Number> range = ArgumentConverters.toRange(command.getRange());
			Mono<Long> result = cmd.zremrangebyscore(command.getKey(), range);

			return result.map(value -> new NumericResponse<>(command, value));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveZSetCommands#zUnionStore(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<NumericResponse<ZUnionStoreCommand, Long>> zUnionStore(Publisher<ZUnionStoreCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKey(), "Destination key must not be null!");
			Assert.notEmpty(command.getSourceKeys(), "Source keys must not be null or empty!");

			ZStoreArgs args = null;
			if (command.getAggregateFunction().isPresent() || !command.getWeights().isEmpty()) {
				args = zStoreArgs(command.getAggregateFunction().isPresent() ? command.getAggregateFunction().get() : null,
						command.getWeights());
			}

			ByteBuffer[] sourceKeys = command.getSourceKeys().stream().toArray(ByteBuffer[]::new);
			Mono<Long> result = args != null ? cmd.zunionstore(command.getKey(), args, sourceKeys)
					: cmd.zunionstore(command.getKey(), sourceKeys);
			return result.map(value -> new NumericResponse<>(command, value));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveZSetCommands#zInterStore(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<NumericResponse<ZInterStoreCommand, Long>> zInterStore(Publisher<ZInterStoreCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKey(), "Destination key must not be null!");
			Assert.notEmpty(command.getSourceKeys(), "Source keys must not be null or empty!");

			ZStoreArgs args = null;
			if (command.getAggregateFunction().isPresent() || !command.getWeights().isEmpty()) {
				args = zStoreArgs(command.getAggregateFunction().isPresent() ? command.getAggregateFunction().get() : null,
						command.getWeights());
			}

			ByteBuffer[] sourceKeys = command.getSourceKeys().stream().toArray(ByteBuffer[]::new);
			Mono<Long> result = args != null ? cmd.zinterstore(command.getKey(), args, sourceKeys)
					: cmd.zinterstore(command.getKey(), sourceKeys);
			return result.map(value -> new NumericResponse<>(command, value));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveZSetCommands#zRangeByLex(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<CommandResponse<ZRangeByLexCommand, Flux<ByteBuffer>>> zRangeByLex(
			Publisher<ZRangeByLexCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKey(), "Destination key must not be null!");

			Flux<ByteBuffer> result;

			if (!command.getLimit().isUnlimited()) {

				if (ObjectUtils.nullSafeEquals(command.getDirection(), Direction.ASC)) {
					result = cmd.zrangebylex(command.getKey(), ArgumentConverters.toRange(command.getRange()),
							LettuceConverters.toLimit(command.getLimit()));
				} else {
					result = cmd.zrevrangebylex(command.getKey(), ArgumentConverters.toRange(command.getRange()),
							LettuceConverters.toLimit(command.getLimit()));
				}
			} else {
				if (ObjectUtils.nullSafeEquals(command.getDirection(), Direction.ASC)) {
					result = cmd.zrangebylex(command.getKey(), ArgumentConverters.toRange(command.getRange()));
				} else {
					result = cmd.zrevrangebylex(command.getKey(), ArgumentConverters.toRange(command.getRange()));
				}
			}

			return Mono.just(new CommandResponse<>(command, result));
		}));
	}

	private static ZStoreArgs zStoreArgs(@Nullable Aggregate aggregate, @Nullable List<Double> weights) {

		ZStoreArgs args = new ZStoreArgs();
		if (aggregate != null) {
			switch (aggregate) {
				case MIN:
					args.min();
					break;
				case MAX:
					args.max();
					break;
				default:
					args.sum();
					break;
			}
		}

		if (weights != null) {
			args.weights(weights.stream().mapToDouble(it -> it).toArray());
		}

		return args;
	}

	private static byte[] getBytes(ScoredValue<ByteBuffer> scoredValue) {
		return scoredValue.optional().map(ByteUtils::getBytes).orElse(new byte[0]);
	}

	protected LettuceReactiveRedisConnection getConnection() {
		return connection;
	}

	/**
	 * @author Christoph Strobl
	 * @author Mark Paluch
	 */
	private static class ArgumentConverters {

		static <T> Range<T> toRange(org.springframework.data.domain.Range<?> range) {
			return Range.from(lowerBoundArgOf(range), upperBoundArgOf(range));
		}

		@SuppressWarnings("unchecked")
		static <T> Boundary<T> lowerBoundArgOf(org.springframework.data.domain.Range<?> range) {
			return (Boundary<T>) rangeToBoundArgumentConverter(false).convert(range);
		}

		@SuppressWarnings("unchecked")
		static <T> Boundary<T> upperBoundArgOf(org.springframework.data.domain.Range<?> range) {
			return (Boundary<T>) rangeToBoundArgumentConverter(true).convert(range);
		}

		private static Converter<org.springframework.data.domain.Range<?>, Boundary<?>> rangeToBoundArgumentConverter(
				Boolean upper) {

			return (source) -> {
				Boolean inclusive = upper ? source.getUpperBound().isInclusive() : source.getLowerBound().isInclusive();
				Object value = upper ? LettuceConverters.getUpperBound(source).orElse(null)
						: LettuceConverters.getLowerBound(source).orElse(null);

				if (value == null) {
					return Boundary.unbounded();
				}

				if (value instanceof Number) {
					return inclusive ? Boundary.including((Number) value) : Boundary.excluding((Number) value);
				}

				if (value instanceof String) {

					StringCodec stringCodec = new StringCodec(LettuceCharsets.UTF8);
					if (!StringUtils.hasText((String) value) || ObjectUtils.nullSafeEquals(value, "+")
							|| ObjectUtils.nullSafeEquals(value, "-")) {
						return Boundary.unbounded();
					}
					return inclusive ? Boundary.including(stringCodec.encodeValue((String) value))
							: Boundary.excluding(stringCodec.encodeValue((String) value));
				}

				return inclusive ? Boundary.including((ByteBuffer) value) : Boundary.excluding((ByteBuffer) value);
			};
		}
	}
}
