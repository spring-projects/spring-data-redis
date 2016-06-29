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

import org.reactivestreams.Publisher;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.redis.connection.DefaultTuple;
import org.springframework.data.redis.connection.ReactiveRedisConnection.KeyCommand;
import org.springframework.data.redis.connection.ReactiveRedisConnection.MultiValueResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.NumericResponse;
import org.springframework.data.redis.connection.ReactiveZSetCommands;
import org.springframework.data.redis.connection.RedisZSetCommands.Aggregate;
import org.springframework.data.redis.connection.RedisZSetCommands.Tuple;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

import com.lambdaworks.redis.ScoredValue;
import com.lambdaworks.redis.ZAddArgs;
import com.lambdaworks.redis.ZStoreArgs;

import reactor.core.publisher.Flux;
import rx.Observable;

/**
 * @author Christoph Strobl
 * @since 2.0
 */
public class LettuceReactiveZSetCommands implements ReactiveZSetCommands {

	private final LettuceReactiveRedisConnection connection;

	/**
	 * Create new {@link LettuceReactiveSetCommands}.
	 *
	 * @param connection must not be {@literal null}.
	 */
	public LettuceReactiveZSetCommands(LettuceReactiveRedisConnection connection) {

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

		return connection.execute(cmd -> Flux.from(commands).flatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null!");
			Assert.notEmpty(command.getTuples(), "Tuples must not be empty or null.");

			ZAddArgs args = null;

			if (command.getIncr().isPresent() || command.getUpsert().isPresent() || command.getReturnTotalChanged().isPresent()) {

				if (command.getIncr().isPresent() && ObjectUtils.nullSafeEquals(command.getIncr().get(), Boolean.TRUE)) {

					if (command.getTuples().size() > 1) {
						throw new IllegalArgumentException("ZADD INCR must not contain more than one tuple.");
					}

					Tuple tuple = command.getTuples().iterator().next();

					return LettuceReactiveRedisConnection.<Double> monoConverter()
							.convert(cmd.zaddincr(command.getKey().array(), tuple.getScore(), tuple.getValue()))
							.map(value -> new NumericResponse<>(command, value));
				}

				if (command.getReturnTotalChanged().isPresent() && ObjectUtils.nullSafeEquals(command.getReturnTotalChanged().get(), Boolean.TRUE)) {
					args = ZAddArgs.Builder.ch();
				}

				if (command.getUpsert().isPresent()) {

					if (command.getUpsert().get().equals(Boolean.TRUE)) {
						args = ZAddArgs.Builder.nx();
					} else {
						args = ZAddArgs.Builder.xx();
					}
				}
			}

			ScoredValue<byte[]>[] values = (ScoredValue<byte[]>[]) command.getTuples().stream()
					.map(tuple -> new ScoredValue<byte[]>(tuple.getScore(), tuple.getValue()))
					.toArray(size -> new ScoredValue[size]);

			Observable<Long> result = args == null ? cmd.zadd(command.getKey().array(), values)
					: cmd.zadd(command.getKey().array(), args, values);

			return LettuceReactiveRedisConnection.<Long> monoConverter().convert(result)
					.map(value -> new NumericResponse<>(command, value));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveZSetCommands#zRem(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<NumericResponse<ZRemCommand, Long>> zRem(Publisher<ZRemCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).flatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null!");
			Assert.notEmpty(command.getValues(), "Values must not be null or empty!");

			return LettuceReactiveRedisConnection.<Long> monoConverter()
					.convert(cmd.zrem(command.getKey().array(),
							command.getValues().stream().map(ByteBuffer::array).toArray(size -> new byte[size][])))
					.map(value -> new NumericResponse<>(command, value));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveZSetCommands#zIncrBy(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<NumericResponse<ZIncrByCommand, Double>> zIncrBy(Publisher<ZIncrByCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).flatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null!");
			Assert.notNull(command.getValue(), "Member must not be null!");
			Assert.notNull(command.getIncrement(), "Increment value must not be null!");

			return LettuceReactiveRedisConnection.<Double> monoConverter()
					.convert(
							cmd.zincrby(command.getKey().array(), command.getIncrement().doubleValue(), command.getValue().array()))
					.map(value -> new NumericResponse<>(command, value));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveZSetCommands#zRank(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<NumericResponse<ZRankCommand, Long>> zRank(Publisher<ZRankCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).flatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null!");
			Assert.notNull(command.getValue(), "Value must not be null!");

			Observable<Long> result = ObjectUtils.nullSafeEquals(command.getDirection(), Direction.ASC)
					? cmd.zrank(command.getKey().array(), command.getValue().array())
					: cmd.zrevrank(command.getKey().array(), command.getValue().array());

			return LettuceReactiveRedisConnection.<Long> monoConverter().convert(result)
					.map(value -> new NumericResponse<>(command, value));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveZSetCommands#zRange(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<MultiValueResponse<ZRangeCommand, Tuple>> zRange(Publisher<ZRangeCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).flatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null!");
			Assert.notNull(command.getRange(), "Range must not be null!");

			Observable<List<Tuple>> result = Observable.empty();

			if (ObjectUtils.nullSafeEquals(command.getDirection(), Direction.ASC)) {
				if (command.getWithScores().isPresent() && ObjectUtils.nullSafeEquals(command.getWithScores().get(), Boolean.TRUE)) {

					result = cmd.zrangeWithScores(command.getKey().array(), command.getRange().getLowerBound(),
							command.getRange().getUpperBound()).map(sc -> (Tuple) new DefaultTuple(sc.value, sc.score)).toList();
				} else {

					result = cmd
							.zrange(command.getKey().array(), command.getRange().getLowerBound(), command.getRange().getUpperBound())
							.map(value -> (Tuple) new DefaultTuple(value, Double.NaN)).toList();
				}
			} else {
				if (command.getWithScores().isPresent() && ObjectUtils.nullSafeEquals(command.getWithScores().get(), Boolean.TRUE)) {
					result = cmd.zrevrangeWithScores(command.getKey().array(), command.getRange().getLowerBound(),
							command.getRange().getUpperBound()).map(sc -> (Tuple) new DefaultTuple(sc.value, sc.score)).toList();
				} else {

					result = cmd
							.zrevrange(command.getKey().array(), command.getRange().getLowerBound(),
									command.getRange().getUpperBound())
							.map(value -> (Tuple) new DefaultTuple(value, Double.NaN)).toList();
				}
			}

			return LettuceReactiveRedisConnection.<List<Tuple>> monoConverter().convert(result)
					.map(value -> new MultiValueResponse<>(command, value));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveZSetCommands#zRange(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<MultiValueResponse<ZRangeByScoreCommand, Tuple>> zRangeByScore(Publisher<ZRangeByScoreCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).flatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null!");
			Assert.notNull(command.getRange(), "Range must not be null!");

			Object lowerBound = AgrumentConverters.lowerBoundArgOf(command.getRange());
			Object upperBound = AgrumentConverters.upperBoundArgOf(command.getRange());

			boolean requiresStringConversion = lowerBound instanceof String || upperBound instanceof String;

			boolean isLimited = command.getLimit().isPresent();

			Observable<List<Tuple>> result = Observable.empty();

			if (ObjectUtils.nullSafeEquals(command.getDirection(), Direction.ASC)) {
				if (command.getWithScores().isPresent() && ObjectUtils.nullSafeEquals(command.getWithScores().get(), Boolean.TRUE)) {

					if (!isLimited) {
						result = (requiresStringConversion
								? cmd.zrangebyscoreWithScores(command.getKey().array(), lowerBound.toString(), upperBound.toString())
								: cmd.zrangebyscoreWithScores(command.getKey().array(), (Double) lowerBound, (Double) upperBound))
										.map(sc -> (Tuple) new DefaultTuple(sc.value, sc.score)).toList();
					} else {
						result = (requiresStringConversion
								? cmd.zrangebyscoreWithScores(command.getKey().array(), lowerBound.toString(), upperBound.toString(),
										command.getLimit().get().getOffset(), command.getLimit().get().getCount())
								: cmd.zrangebyscoreWithScores(command.getKey().array(), (Double) lowerBound, (Double) upperBound,
										command.getLimit().get().getOffset(), command.getLimit().get().getCount()))
												.map(sc -> (Tuple) new DefaultTuple(sc.value, sc.score)).toList();
					}
				} else {

					if (!isLimited) {
						result = (requiresStringConversion
								? cmd.zrangebyscore(command.getKey().array(), lowerBound.toString(), upperBound.toString())
								: cmd.zrangebyscore(command.getKey().array(), (Double) lowerBound, (Double) upperBound))
										.map(value -> (Tuple) new DefaultTuple(value, Double.NaN)).toList();
					} else {

						result = (requiresStringConversion
								? cmd.zrangebyscore(command.getKey().array(), lowerBound.toString(), upperBound.toString(),
										command.getLimit().get().getOffset(), command.getLimit().get().getCount())
								: cmd.zrangebyscore(command.getKey().array(), (Double) lowerBound, (Double) upperBound,
										command.getLimit().get().getOffset(), command.getLimit().get().getCount()))
												.map(value -> (Tuple) new DefaultTuple(value, Double.NaN)).toList();
					}
				}
			} else {
				if (command.getWithScores().isPresent() && ObjectUtils.nullSafeEquals(command.getWithScores().get(), Boolean.TRUE)) {

					if (!isLimited) {
						result = (requiresStringConversion
								? cmd.zrevrangebyscoreWithScores(command.getKey().array(), lowerBound.toString(), upperBound.toString())
								: cmd.zrevrangebyscoreWithScores(command.getKey().array(), (Double) lowerBound, (Double) upperBound))
										.map(sc -> (Tuple) new DefaultTuple(sc.value, sc.score)).toList();
					} else {

						result = (requiresStringConversion
								? cmd.zrevrangebyscoreWithScores(command.getKey().array(), lowerBound.toString(), upperBound.toString(),
										command.getLimit().get().getOffset(), command.getLimit().get().getCount())
								: cmd.zrevrangebyscoreWithScores(command.getKey().array(), (Double) lowerBound, (Double) upperBound,
										command.getLimit().get().getOffset(), command.getLimit().get().getCount()))
												.map(sc -> (Tuple) new DefaultTuple(sc.value, sc.score)).toList();
					}
				} else {

					if (!isLimited) {
						result = (requiresStringConversion
								? cmd.zrevrangebyscore(command.getKey().array(), lowerBound.toString(), upperBound.toString())
								: cmd.zrevrangebyscore(command.getKey().array(), (Double) lowerBound, (Double) upperBound))
										.map(value -> (Tuple) new DefaultTuple(value, Double.NaN)).toList();
					} else {

						result = (requiresStringConversion
								? cmd.zrevrangebyscore(command.getKey().array(), lowerBound.toString(), upperBound.toString(),
										command.getLimit().get().getOffset(), command.getLimit().get().getCount())
								: cmd.zrevrangebyscore(command.getKey().array(), (Double) lowerBound, (Double) upperBound,
										command.getLimit().get().getOffset(), command.getLimit().get().getCount()))
												.map(value -> (Tuple) new DefaultTuple(value, Double.NaN)).toList();
					}
				}
			}

			return LettuceReactiveRedisConnection.<List<Tuple>> monoConverter().convert(result)
					.map(value -> new MultiValueResponse<>(command, value));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveZSetCommands#zCount(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<NumericResponse<ZCountCommand, Long>> zCount(Publisher<ZCountCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).flatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null!");
			Assert.notNull(command.getRange(), "Range must not be null!");

			Object lowerBound = AgrumentConverters.lowerBoundArgOf(command.getRange());
			Object upperBound = AgrumentConverters.upperBoundArgOf(command.getRange());

			Observable<Long> result = Observable.empty();

			if (lowerBound instanceof String || upperBound instanceof String) {
				result = cmd.zcount(command.getKey().array(), lowerBound.toString(), upperBound.toString());
			} else {
				result = cmd.zcount(command.getKey().array(), (Double) lowerBound, (Double) upperBound);
			}

			return LettuceReactiveRedisConnection.<Long> monoConverter().convert(result)
					.map(value -> new NumericResponse<>(command, value));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveZSetCommands#zCard(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<NumericResponse<KeyCommand, Long>> zCard(Publisher<KeyCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).flatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null!");

			return LettuceReactiveRedisConnection.<Long> monoConverter().convert(cmd.zcard(command.getKey().array()))
					.map(value -> new NumericResponse<>(command, value));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveZSetCommands#zScore(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<NumericResponse<ZScoreCommand, Double>> zScore(Publisher<ZScoreCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).flatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null!");
			Assert.notNull(command.getValue(), "Value must not be null!");

			return LettuceReactiveRedisConnection.<Double> monoConverter()
					.convert(cmd.zscore(command.getKey().array(), command.getValue().array()))
					.map(value -> new NumericResponse<>(command, value));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveZSetCommands#zRemRangeByRank(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<NumericResponse<ZRemRangeByRankCommand, Long>> zRemRangeByRank(
			Publisher<ZRemRangeByRankCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).flatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null!");
			Assert.notNull(command.getRange(), "Range must not be null!");

			return LettuceReactiveRedisConnection
					.<Long> monoConverter().convert(cmd.zremrangebyrank(command.getKey().array(),
							command.getRange().getLowerBound(), command.getRange().getUpperBound()))
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

		return connection.execute(cmd -> Flux.from(commands).flatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null!");
			Assert.notNull(command.getRange(), "Range must not be null!");

			Object lowerBound = AgrumentConverters.lowerBoundArgOf(command.getRange());
			Object upperBound = AgrumentConverters.upperBoundArgOf(command.getRange());

			Observable<Long> result = (lowerBound instanceof String || upperBound instanceof String)
					? cmd.zremrangebyscore(command.getKey().array(), lowerBound.toString(), upperBound.toString())
					: cmd.zremrangebyscore(command.getKey().array(), (Double) lowerBound, (Double) upperBound);

			return LettuceReactiveRedisConnection.<Long> monoConverter().convert(result)
					.map(value -> new NumericResponse<>(command, value));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveZSetCommands#zUnionStore(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<NumericResponse<ZUnionStoreCommand, Long>> zUnionStore(Publisher<ZUnionStoreCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).flatMap(command -> {

			Assert.notNull(command.getKey(), "Destination key must not be null!");
			Assert.notEmpty(command.getSourceKeys(), "Source keys must not be null or empty!");

			ZStoreArgs args = null;
			if (command.getAggregateFunction().isPresent() || !command.getWeights().isEmpty()) {
				args = zStoreArgs(command.getAggregateFunction().isPresent() ? command.getAggregateFunction().get() : null, command.getWeights());
			}

			byte[][] sourceKeys = command.getSourceKeys().stream().map(ByteBuffer::array).toArray(size -> new byte[size][]);
			Observable<Long> result = args != null ? cmd.zunionstore(command.getKey().array(), args, sourceKeys)
					: cmd.zunionstore(command.getKey().array(), sourceKeys);
			return LettuceReactiveRedisConnection.<Long> monoConverter().convert(result)
					.map(value -> new NumericResponse<>(command, value));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveZSetCommands#zInterStore(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<NumericResponse<ZInterStoreCommand, Long>> zInterStore(Publisher<ZInterStoreCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).flatMap(command -> {

			Assert.notNull(command.getKey(), "Destination key must not be null!");
			Assert.notEmpty(command.getSourceKeys(), "Source keys must not be null or empty!");

			ZStoreArgs args = null;
			if (command.getAggregateFunction().isPresent() || !command.getWeights().isEmpty()) {
				args = zStoreArgs(command.getAggregateFunction().isPresent() ? command.getAggregateFunction().get() : null, command.getWeights());
			}

			byte[][] sourceKeys = command.getSourceKeys().stream().map(ByteBuffer::array).toArray(size -> new byte[size][]);
			Observable<Long> result = args != null ? cmd.zinterstore(command.getKey().array(), args, sourceKeys)
					: cmd.zinterstore(command.getKey().array(), sourceKeys);
			return LettuceReactiveRedisConnection.<Long> monoConverter().convert(result)
					.map(value -> new NumericResponse<>(command, value));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveZSetCommands#zRangeByLex(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<MultiValueResponse<ZRangeByLexCommand, ByteBuffer>> zRangeByLex(Publisher<ZRangeByLexCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).flatMap(command -> {

			Assert.notNull(command.getKey(), "Destination key must not be null!");

			Observable<byte[]> result = Observable.empty();

			String lowerBound = AgrumentConverters.lowerBoundArgOf(command.getRange()).toString();
			String upperBound = AgrumentConverters.upperBoundArgOf(command.getRange()).toString();

			if (command.getLimit() != null) {

				if (ObjectUtils.nullSafeEquals(command.getDirection(), Direction.ASC)) {
					result = cmd.zrangebylex(command.getKey().array(), lowerBound, upperBound, command.getLimit().getOffset(),
							command.getLimit().getCount());
				} else {

					// TODO: fix when https://github.com/mp911de/lettuce/issues/369 resolved
					throw new UnsupportedOperationException("Lettuce does not support ZREVRANGEBYLEX.");
				}
			} else {
				if (ObjectUtils.nullSafeEquals(command.getDirection(), Direction.ASC)) {
					result = cmd.zrangebylex(command.getKey().array(), lowerBound, upperBound);
				} else {

					// TODO: fix when https://github.com/mp911de/lettuce/issues/369 resolved
					throw new UnsupportedOperationException("Lettuce does not support ZREVRANGEBYLEX.");
				}
			}

			return LettuceReactiveRedisConnection.<List<ByteBuffer>> monoConverter()
					.convert(result.map(ByteBuffer::wrap).toList()).map(value -> new MultiValueResponse<>(command, value));
		}));
	}

	private ZStoreArgs zStoreArgs(Aggregate aggregate, List<Double> weights) {

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

		// TODO: fix when https://github.com/mp911de/lettuce/issues/368 resolved
		if (weights != null) {
			long[] lg = new long[weights.size()];
			for (int i = 0; i < lg.length; i++) {
				lg[i] = weights.get(i).longValue();
			}
			args.weights(lg);
		}
		return args;
	}

	protected LettuceReactiveRedisConnection getConnection() {
		return connection;
	}
}
