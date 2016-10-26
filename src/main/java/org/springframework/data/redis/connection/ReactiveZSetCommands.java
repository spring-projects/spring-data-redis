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
package org.springframework.data.redis.connection;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import com.lambdaworks.redis.Range.Boundary;
import org.reactivestreams.Publisher;
import org.springframework.data.domain.Range;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.redis.connection.ReactiveRedisConnection.KeyCommand;
import org.springframework.data.redis.connection.ReactiveRedisConnection.MultiValueResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.NumericResponse;
import org.springframework.data.redis.connection.RedisZSetCommands.Aggregate;
import org.springframework.data.redis.connection.RedisZSetCommands.Limit;
import org.springframework.data.redis.connection.RedisZSetCommands.Tuple;
import org.springframework.util.Assert;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.0
 */
public interface ReactiveZSetCommands {

	/**
	 * @author Christoph Strobl
	 */
	class ZAddCommand extends KeyCommand {

		private final List<Tuple> tuples;
		private final Boolean upsert;
		private final Boolean returnTotalChanged;
		private final Boolean incr;

		private ZAddCommand(ByteBuffer key, List<Tuple> tuples, Boolean upsert, Boolean returnTotalChanged, Boolean incr) {

			super(key);
			this.tuples = tuples;
			this.upsert = upsert;
			this.returnTotalChanged = returnTotalChanged;
			this.incr = incr;
		}

		public static ZAddCommand tuple(Tuple tuple) {
			return tuples(Collections.singletonList(tuple));
		}

		public static ZAddCommand tuples(List<Tuple> tuples) {
			return new ZAddCommand(null, tuples, null, null, null);
		}

		public ZAddCommand to(ByteBuffer key) {
			return new ZAddCommand(key, tuples, upsert, returnTotalChanged, incr);
		}

		public ZAddCommand xx() {
			return new ZAddCommand(getKey(), tuples, false, returnTotalChanged, incr);
		}

		public ZAddCommand nx() {
			return new ZAddCommand(getKey(), tuples, true, returnTotalChanged, incr);
		}

		public ZAddCommand ch() {
			return new ZAddCommand(getKey(), tuples, upsert, true, incr);
		}

		public ZAddCommand incr() {
			return new ZAddCommand(getKey(), tuples, upsert, upsert, true);
		}

		public List<Tuple> getTuples() {
			return tuples;
		}

		public Optional<Boolean> getUpsert() {
			return Optional.ofNullable(upsert);
		}

		public Optional<Boolean> getIncr() {
			return Optional.ofNullable(incr);
		}

		public Optional<Boolean> getReturnTotalChanged() {
			return Optional.ofNullable(returnTotalChanged);
		}
	}

	/**
	 * Add {@code value} to a sorted set at {@code key}, or update its {@code score} if it already exists.
	 *
	 * @param key must not be {@literal null}.
	 * @param score must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return
	 */
	default Mono<Long> zAdd(ByteBuffer key, Double score, ByteBuffer value) {

		Assert.notNull(key, "key must not be null");
		Assert.notNull(score, "score must not be null");
		Assert.notNull(value, "value must not be null");

		return zAdd(Mono.just(ZAddCommand.tuple(new DefaultTuple(value.array(), score)).to(key))).next()
				.map(resp -> resp.getOutput().longValue());
	}

	/**
	 * Add {@link ZAddCommand#getTuple()} to a sorted set at {@link ZAddCommand#getKey()}, or update its {@code score} if
	 * it already exists.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 */
	Flux<NumericResponse<ZAddCommand, Number>> zAdd(Publisher<ZAddCommand> commands);

	/**
	 * @author Christoph Strobl
	 */
	class ZRemCommand extends KeyCommand {

		private final List<ByteBuffer> values;

		private ZRemCommand(ByteBuffer key, List<ByteBuffer> values) {

			super(key);
			this.values = values;
		}

		public static ZRemCommand values(List<ByteBuffer> values) {
			return new ZRemCommand(null, values);
		}

		public ZRemCommand from(ByteBuffer key) {
			return new ZRemCommand(key, values);
		}

		public List<ByteBuffer> getValues() {
			return values;
		}
	}

	/**
	 * Remove {@code value} from sorted set. Return number of removed elements.
	 *
	 * @param key must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return
	 */
	default Mono<Long> zRem(ByteBuffer key, ByteBuffer value) {
		return zRem(key, Collections.singletonList(value));
	}

	/**
	 * Remove {@code values} from sorted set. Return number of removed elements.
	 *
	 * @param key must not be {@literal null}.
	 * @param values must not be {@literal null}.
	 * @return
	 */
	default Mono<Long> zRem(ByteBuffer key, List<ByteBuffer> values) {

		Assert.notNull(values, "values must not be null");

		return zRem(Mono.just(ZRemCommand.values(values).from(key))).next().map(NumericResponse::getOutput);
	}

	/**
	 * Remove {@link ZRemCommand#getValues()} from sorted set. Return number of removed elements.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 */
	Flux<NumericResponse<ZRemCommand, Long>> zRem(Publisher<ZRemCommand> commands);

	/**
	 * @author Christoph Strobl
	 */
	class ZIncrByCommand extends KeyCommand {

		private final ByteBuffer value;
		private final Number increment;

		public ZIncrByCommand(ByteBuffer key, ByteBuffer value, Number increment) {

			super(key);
			this.value = value;
			this.increment = increment;
		}

		public static ZIncrByCommand scoreOf(ByteBuffer member) {
			return new ZIncrByCommand(null, member, null);
		}

		public ZIncrByCommand by(Number increment) {
			return new ZIncrByCommand(getKey(), value, increment);
		}

		public ZIncrByCommand storedWithin(ByteBuffer key) {
			return new ZIncrByCommand(key, value, increment);
		}

		public ByteBuffer getValue() {
			return value;
		}

		public Number getIncrement() {
			return increment;
		}
	}

	/**
	 * Increment the score of element with {@code value} in sorted set by {@code increment}.
	 *
	 * @param key must not be {@literal null}.
	 * @param increment must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return
	 */
	default Mono<Double> zIncrBy(ByteBuffer key, Number increment, ByteBuffer value) {

		Assert.notNull(key, "key must not be null");
		Assert.notNull(increment, "increment must not be null");
		Assert.notNull(value, "value must not be null");

		return zIncrBy(Mono.just(ZIncrByCommand.scoreOf(value).by(increment).storedWithin(key))).next()
				.map(NumericResponse::getOutput);
	}

	/**
	 * Increment the score of element with {@link ZIncrByCommand#getValue()} in sorted set by
	 * {@link ZIncrByCommand#getIncrement()}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 */
	Flux<NumericResponse<ZIncrByCommand, Double>> zIncrBy(Publisher<ZIncrByCommand> commands);

	/**
	 * @author Christoph Strobl
	 */
	class ZRankCommand extends KeyCommand {

		private final ByteBuffer value;
		private final Direction direction;

		private ZRankCommand(ByteBuffer key, ByteBuffer value, Direction direction) {

			super(key);
			this.value = value;
			this.direction = direction;
		}

		public static ZRankCommand indexOf(ByteBuffer member) {
			return new ZRankCommand(null, member, Direction.ASC);
		}

		public static ZRankCommand reverseIndexOf(ByteBuffer member) {
			return new ZRankCommand(null, member, Direction.DESC);
		}

		public ZRankCommand storedWithin(ByteBuffer key) {
			return new ZRankCommand(key, value, direction);
		}

		public ByteBuffer getValue() {
			return value;
		}

		public Direction getDirection() {
			return direction;
		}
	}

	/**
	 * Determine the index of element with {@code value} in a sorted set.
	 *
	 * @param key must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return
	 */
	default Mono<Long> zRank(ByteBuffer key, ByteBuffer value) {

		Assert.notNull(key, "key must not be null");
		Assert.notNull(value, "value must not be null");

		return zRank(Mono.just(ZRankCommand.indexOf(value).storedWithin(key))).next().map(NumericResponse::getOutput);
	}

	/**
	 * Determine the index of element with {@code value} in a sorted set when scored high to low.
	 *
	 * @param key must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return
	 */
	default Mono<Long> zRevRank(ByteBuffer key, ByteBuffer value) {

		Assert.notNull(key, "key must not be null");
		Assert.notNull(value, "value must not be null");

		return zRank(Mono.just(ZRankCommand.reverseIndexOf(value).storedWithin(key))).next()
				.map(NumericResponse::getOutput);
	}

	/**
	 * Determine the index of element with {@code value} in a sorted set when scored by
	 * {@link ZRankCommand#getDirection()}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 */
	Flux<NumericResponse<ZRankCommand, Long>> zRank(Publisher<ZRankCommand> commands);

	/**
	 * @author Christoph Strobl
	 */
	class ZRangeCommand extends KeyCommand {

		private final Range<Long> range;
		private final Boolean withScores;
		private final Direction direction;

		public ZRangeCommand(ByteBuffer key, Range<Long> range, Direction direction, Boolean withScores) {
			super(key);
			this.range = range;
			this.withScores = withScores;
			this.direction = direction;
		}

		public static ZRangeCommand reverseValuesWithin(Range<Long> range) {
			return new ZRangeCommand(null, range, Direction.DESC, null);
		}

		public static ZRangeCommand valuesWithin(Range<Long> range) {
			return new ZRangeCommand(null, range, Direction.ASC, null);
		}

		public ZRangeCommand withScores() {
			return new ZRangeCommand(getKey(), range, direction, Boolean.TRUE);
		}

		public ZRangeCommand from(ByteBuffer key) {
			return new ZRangeCommand(key, range, direction, withScores);
		}

		public Range<Long> getRange() {
			return range;
		}

		public Optional<Boolean> getWithScores() {
			return Optional.ofNullable(withScores);
		}

		public Direction getDirection() {
			return direction;
		}
	}

	/**
	 * Get elements in {@code range} from sorted set.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return
	 */
	default Mono<List<ByteBuffer>> zRange(ByteBuffer key, Range<Long> range) {

		Assert.notNull(key, "key must not be null");

		return zRange(Mono.just(ZRangeCommand.valuesWithin(range).from(key))).next().map(
				resp -> resp.getOutput().stream().map(tuple -> ByteBuffer.wrap(tuple.getValue())).collect(Collectors.toList()));
	}

	/**
	 * Get set of {@link Tuple}s in {@code range} from sorted set.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return
	 */
	default Mono<List<Tuple>> zRangeWithScores(ByteBuffer key, Range<Long> range) {

		Assert.notNull(key, "key must not be null");

		return zRange(Mono.just(ZRangeCommand.valuesWithin(range).withScores().from(key))).next()
				.map(MultiValueResponse::getOutput);
	}

	/**
	 * Get elements in {@code range} from sorted set in reverse {@code score} ordering.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return
	 */
	default Mono<List<ByteBuffer>> zRevRange(ByteBuffer key, Range<Long> range) {

		Assert.notNull(key, "key must not be null");

		return zRange(Mono.just(ZRangeCommand.reverseValuesWithin(range).from(key))).next().map(
				resp -> resp.getOutput().stream().map(tuple -> ByteBuffer.wrap(tuple.getValue())).collect(Collectors.toList()));
	}

	/**
	 * Get set of {@link Tuple}s in {@code range} from sorted set in reverse {@code score} ordering.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return
	 */
	default Mono<List<Tuple>> zRevRangeWithScores(ByteBuffer key, Range<Long> range) {

		Assert.notNull(key, "key must not be null");

		return zRange(Mono.just(ZRangeCommand.reverseValuesWithin(range).withScores().from(key))).next()
				.map(MultiValueResponse::getOutput);
	}

	/**
	 * Get set of {@link Tuple}s in {@code range} from sorted set.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 */
	Flux<MultiValueResponse<ZRangeCommand, Tuple>> zRange(Publisher<ZRangeCommand> commands);

	/**
	 * @author Christoph Strobl
	 */
	class ZRangeByScoreCommand extends KeyCommand {

		private final Range<Double> range;
		private final Boolean withScores;
		private final Direction direction;
		private final Limit limit;

		private ZRangeByScoreCommand(ByteBuffer key, Range<Double> range, Direction direction, Boolean withScores,
				Limit limit) {

			super(key);
			this.range = range;
			this.withScores = withScores;
			this.direction = direction;
			this.limit = limit;
		}

		public static ZRangeByScoreCommand reverseScoresWithin(Range<Double> range) {
			return new ZRangeByScoreCommand(null, range, Direction.DESC, null, null);
		}

		public static ZRangeByScoreCommand scoresWithin(Range<Double> range) {
			return new ZRangeByScoreCommand(null, range, Direction.ASC, null, null);
		}

		public ZRangeByScoreCommand withScores() {
			return new ZRangeByScoreCommand(getKey(), range, direction, Boolean.TRUE, limit);
		}

		public ZRangeByScoreCommand from(ByteBuffer key) {
			return new ZRangeByScoreCommand(key, range, direction, withScores, limit);
		}

		public ZRangeByScoreCommand limitTo(Limit limit) {
			return new ZRangeByScoreCommand(getKey(), range, direction, withScores, limit);
		}

		public Range<Double> getRange() {
			return range;
		}

		public Optional<Boolean> getWithScores() {
			return Optional.ofNullable(withScores);
		}

		public Direction getDirection() {
			return direction;
		}

		public Optional<Limit> getLimit() {
			return Optional.ofNullable(limit);
		}
	}

	/**
	 * Get elements in {@code range} from sorted set.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return
	 */
	default Mono<List<ByteBuffer>> zRangeByScore(ByteBuffer key, Range<Double> range) {

		Assert.notNull(key, "key must not be null");

		return zRangeByScore(Mono.just(ZRangeByScoreCommand.scoresWithin(range).from(key))).next().map(
				resp -> resp.getOutput().stream().map(tuple -> ByteBuffer.wrap(tuple.getValue())).collect(Collectors.toList()));
	}

	/**
	 * Get elements in {@code range} from sorted set.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @param limit can be {@literal null}.
	 * @return
	 */
	default Mono<List<ByteBuffer>> zRangeByScore(ByteBuffer key, Range<Double> range, Limit limit) {

		Assert.notNull(key, "key must not be null");
		Assert.notNull(range, "range must not be null");

		return zRangeByScore(Mono.just(ZRangeByScoreCommand.scoresWithin(range).from(key).limitTo(limit))).next().map(
				resp -> resp.getOutput().stream().map(tuple -> ByteBuffer.wrap(tuple.getValue())).collect(Collectors.toList()));
	}

	/**
	 * Get set of {@link Tuple}s in {@code range} from sorted set.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return
	 */
	default Mono<List<Tuple>> zRangeByScoreWithScores(ByteBuffer key, Range<Double> range) {

		Assert.notNull(key, "key must not be null");
		Assert.notNull(range, "range must not be null");

		return zRangeByScore(Mono.just(ZRangeByScoreCommand.scoresWithin(range).withScores().from(key))).next()
				.map(MultiValueResponse::getOutput);
	}

	/**
	 * Get set of {@link Tuple}s in {@code range} from sorted set.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @param limit can be {@literal null}.
	 * @return
	 */
	default Mono<List<Tuple>> zRangeByScoreWithScores(ByteBuffer key, Range<Double> range, Limit limit) {

		Assert.notNull(key, "key must not be null");
		Assert.notNull(range, "range must not be null");

		return zRangeByScore(Mono.just(ZRangeByScoreCommand.scoresWithin(range).withScores().from(key).limitTo(limit)))
				.next().map(MultiValueResponse::getOutput);
	}

	/**
	 * Get elements in {@code range} from sorted set in reverse {@code score} ordering.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return
	 */
	default Mono<List<ByteBuffer>> zRevRangeByScore(ByteBuffer key, Range<Double> range) {

		Assert.notNull(key, "key must not be null");

		return zRangeByScore(Mono.just(ZRangeByScoreCommand.reverseScoresWithin(range).from(key))).next().map(
				resp -> resp.getOutput().stream().map(tuple -> ByteBuffer.wrap(tuple.getValue())).collect(Collectors.toList()));
	}

	/**
	 * Get elements in {@code range} from sorted set in reverse {@code score} ordering.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @param limit can be {@literal null}.
	 * @return
	 */
	default Mono<List<ByteBuffer>> zRevRangeByScore(ByteBuffer key, Range<Double> range, Limit limit) {

		Assert.notNull(key, "key must not be null");
		Assert.notNull(range, "range must not be null");

		return zRangeByScore(Mono.just(ZRangeByScoreCommand.reverseScoresWithin(range).from(key).limitTo(limit))).next()
				.map(resp -> resp.getOutput().stream().map(tuple -> ByteBuffer.wrap(tuple.getValue()))
						.collect(Collectors.toList()));
	}

	/**
	 * Get set of {@link Tuple}s in {@code range} from sorted set in reverse {@code score} ordering.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return
	 */
	default Mono<List<Tuple>> zRevRangeByScoreWithScores(ByteBuffer key, Range<Double> range) {

		Assert.notNull(key, "key must not be null");
		Assert.notNull(range, "range must not be null");

		return zRangeByScore(Mono.just(ZRangeByScoreCommand.reverseScoresWithin(range).withScores().from(key))).next()
				.map(MultiValueResponse::getOutput);
	}

	/**
	 * Get set of {@link Tuple}s in {@code range} from sorted set in reverse {@code score} ordering.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @param limit can be {@literal null}.
	 * @return
	 */
	default Mono<List<Tuple>> zRevRangeByScoreWithScores(ByteBuffer key, Range<Double> range, Limit limit) {

		Assert.notNull(key, "key must not be null");
		Assert.notNull(range, "range must not be null");

		return zRangeByScore(
				Mono.just(ZRangeByScoreCommand.reverseScoresWithin(range).withScores().from(key).limitTo(limit))).next()
						.map(MultiValueResponse::getOutput);
	}

	/**
	 * Get set of {@link Tuple}s in {@code range} from sorted set.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 */
	Flux<MultiValueResponse<ZRangeByScoreCommand, Tuple>> zRangeByScore(Publisher<ZRangeByScoreCommand> commands);

	/**
	 * @author Christoph Strobl
	 */
	class ZCountCommand extends KeyCommand {

		private final Range<Double> range;

		private ZCountCommand(ByteBuffer key, Range<Double> range) {

			super(key);
			this.range = range;
		}

		public static ZCountCommand scoresWithin(Range<Double> range) {
			return new ZCountCommand(null, range);
		}

		public ZCountCommand forKey(ByteBuffer key) {
			return new ZCountCommand(key, range);
		}

		public Range<Double> getRange() {
			return range;
		}

	}

	/**
	 * Count number of elements within sorted set with scores within {@link Range}. <br />
	 * <b>NOTE</b> please use {@link Double#NEGATIVE_INFINITY} for {@code -inf} and {@link Double#POSITIVE_INFINITY} for
	 * {@code +inf}.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return
	 */
	default Mono<Long> zCount(ByteBuffer key, Range<Double> range) {

		Assert.notNull(key, "key must not be null");
		Assert.notNull(range, "range must not be null");

		return zCount(Mono.just(ZCountCommand.scoresWithin(range).forKey(key))).next().map(NumericResponse::getOutput);
	}

	/**
	 * Count number of elements within sorted set with scores within {@link Range}. <br />
	 * <b>NOTE</b> please use {@link Double#NEGATIVE_INFINITY} for {@code -inf} and {@link Double#POSITIVE_INFINITY} for
	 * {@code +inf}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 */
	Flux<NumericResponse<ZCountCommand, Long>> zCount(Publisher<ZCountCommand> commands);

	/**
	 * Get the size of sorted set with {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 */
	default Mono<Long> zCard(ByteBuffer key) {

		Assert.notNull(key, "key must not be null");

		return zCard(Mono.just(new KeyCommand(key))).next().map(NumericResponse::getOutput);
	}

	/**
	 * Get the size of sorted set with {@link KeyCommand#getKey()}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 */
	Flux<NumericResponse<KeyCommand, Long>> zCard(Publisher<KeyCommand> commands);

	/**
	 * @author Christoph Strobl
	 */
	class ZScoreCommand extends KeyCommand {

		private final ByteBuffer value;

		private ZScoreCommand(ByteBuffer key, ByteBuffer value) {

			super(key);
			this.value = value;
		}

		public static ZScoreCommand scoreOf(ByteBuffer member) {
			return new ZScoreCommand(null, member);
		}

		public ZScoreCommand forKey(ByteBuffer key) {
			return new ZScoreCommand(key, value);
		}

		public ByteBuffer getValue() {
			return value;
		}

	}

	/**
	 * Get the score of element with {@code value} from sorted set with key {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return
	 */
	default Mono<Double> zScore(ByteBuffer key, ByteBuffer value) {

		Assert.notNull(key, "key must not be null");
		Assert.notNull(value, "value must not be null");

		return zScore(Mono.just(ZScoreCommand.scoreOf(value).forKey(key))).next().map(NumericResponse::getOutput);
	}

	/**
	 * Get the score of element with {@link ZScoreCommand#getValue()} from sorted set with key
	 * {@link ZScoreCommand#getKey()}
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 */
	Flux<NumericResponse<ZScoreCommand, Double>> zScore(Publisher<ZScoreCommand> commands);

	/**
	 * @author Christoph Strobl
	 */
	class ZRemRangeByRankCommand extends KeyCommand {

		private final Range<Long> range;

		private ZRemRangeByRankCommand(ByteBuffer key, Range<Long> range) {
			super(key);
			this.range = range;
		}

		public static ZRemRangeByRankCommand valuesWithin(Range<Long> range) {
			return new ZRemRangeByRankCommand(null, range);
		}

		public ZRemRangeByRankCommand from(ByteBuffer key) {
			return new ZRemRangeByRankCommand(key, range);
		}

		public Range<Long> getRange() {
			return range;
		}
	}

	/**
	 * Remove elements in {@link Range} from sorted set with {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return
	 */
	default Mono<Long> zRemRangeByRank(ByteBuffer key, Range<Long> range) {

		Assert.notNull(key, "key must not be null");
		Assert.notNull(range, "range must not be null");

		return zRemRangeByRank(Mono.just(ZRemRangeByRankCommand.valuesWithin(range).from(key))).next()
				.map(NumericResponse::getOutput);
	}

	/**
	 * Remove elements in {@link Range} from sorted set with {@link ZRemRangeByRankCommand#getKey()}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 */
	Flux<NumericResponse<ZRemRangeByRankCommand, Long>> zRemRangeByRank(Publisher<ZRemRangeByRankCommand> commands);

	/**
	 * @author Christoph Strobl
	 */
	class ZRemRangeByScoreCommand extends KeyCommand {

		private final Range<Double> range;

		private ZRemRangeByScoreCommand(ByteBuffer key, Range<Double> range) {

			super(key);
			this.range = range;
		}

		public static ZRemRangeByScoreCommand scoresWithin(Range<Double> range) {
			return new ZRemRangeByScoreCommand(null, range);
		}

		public ZRemRangeByScoreCommand from(ByteBuffer key) {
			return new ZRemRangeByScoreCommand(key, range);
		}

		public Range<Double> getRange() {
			return range;
		}

	}

	/**
	 * Remove elements in {@link Range} from sorted set with {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return
	 */
	default Mono<Long> zRemRangeByScore(ByteBuffer key, Range<Double> range) {

		Assert.notNull(key, "key must not be null");
		Assert.notNull(range, "range must not be null");

		return zRemRangeByScore(Mono.just(ZRemRangeByScoreCommand.scoresWithin(range).from(key))).next()
				.map(NumericResponse::getOutput);
	}

	/**
	 * Remove elements in {@link Range} from sorted set with {@link ZRemRangeByRankCommand#getKey()}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 */
	Flux<NumericResponse<ZRemRangeByScoreCommand, Long>> zRemRangeByScore(Publisher<ZRemRangeByScoreCommand> commands);

	/**
	 * @author Christoph Strobl
	 */
	class ZUnionStoreCommand extends KeyCommand {

		private final List<ByteBuffer> sourceKeys;
		private final List<Double> weights;
		private final Aggregate aggregateFunction;

		private ZUnionStoreCommand(ByteBuffer key, List<ByteBuffer> sourceKeys, List<Double> weights, Aggregate aggregate) {

			super(key);
			this.sourceKeys = sourceKeys;
			this.weights = weights;
			this.aggregateFunction = aggregate;
		}

		public static ZUnionStoreCommand sets(List<ByteBuffer> keys) {
			return new ZUnionStoreCommand(null, keys, null, null);
		}

		public ZUnionStoreCommand applyWeights(List<Double> weights) {
			return new ZUnionStoreCommand(getKey(), sourceKeys, weights, aggregateFunction);
		}

		public ZUnionStoreCommand aggregateUsing(Aggregate aggregateFunction) {
			return new ZUnionStoreCommand(getKey(), sourceKeys, weights, aggregateFunction);
		}

		public ZUnionStoreCommand storeAs(ByteBuffer key) {
			return new ZUnionStoreCommand(key, sourceKeys, weights, aggregateFunction);
		}

		public List<ByteBuffer> getSourceKeys() {
			return sourceKeys;
		}

		public List<Double> getWeights() {
			return weights == null ? Collections.emptyList() : weights;
		}

		public Optional<Aggregate> getAggregateFunction() {
			return Optional.ofNullable(aggregateFunction);
		}

		public Integer getNumKeys() {
			return sourceKeys != null ? sourceKeys.size() : null;
		}
	}

	/**
	 * Union sorted {@code sets} and store result in destination {@code destinationKey}.
	 *
	 * @param destinationKey must not be {@literal null}.
	 * @param sets must not be {@literal null}.
	 * @return
	 */
	default Mono<Long> zUnionStore(ByteBuffer destinationKey, List<ByteBuffer> sets) {
		return zUnionStore(destinationKey, sets, null);
	}

	/**
	 * Union sorted {@code sets} and store result in destination {@code destinationKey} and apply weights to individual
	 * sets.
	 *
	 * @param destinationKey must not be {@literal null}.
	 * @param sets must not be {@literal null}.
	 * @param weights can be {@literal null}.
	 * @return
	 */
	default Mono<Long> zUnionStore(ByteBuffer destinationKey, List<ByteBuffer> sets, List<Double> weights) {
		return zUnionStore(destinationKey, sets, weights, null);
	}

	/**
	 * Union sorted {@code sets} by applying {@code aggregateFunction} and store result in destination
	 * {@code destinationKey} and apply weights to individual sets.
	 *
	 * @param destinationKey must not be {@literal null}.
	 * @param sets must not be {@literal null}.
	 * @param weights can be {@literal null}.
	 * @param aggregateFunction can be {@literal null}.
	 * @return
	 */
	default Mono<Long> zUnionStore(ByteBuffer destinationKey, List<ByteBuffer> sets, List<Double> weights,
			Aggregate aggregateFunction) {

		Assert.notNull(destinationKey, "destinationKey must not be null");
		Assert.notNull(sets, "sets must not be null");

		return zUnionStore(Mono.just(
				ZUnionStoreCommand.sets(sets).aggregateUsing(aggregateFunction).applyWeights(weights).storeAs(destinationKey)))
						.next().map(NumericResponse::getOutput);
	}

	/**
	 * Union sorted {@code sets} by applying {@code aggregateFunction} and store result in destination
	 * {@code destinationKey} and apply weights to individual sets.
	 *
	 * @param commands
	 * @return
	 */
	Flux<NumericResponse<ZUnionStoreCommand, Long>> zUnionStore(Publisher<ZUnionStoreCommand> commands);

	/**
	 * @author Christoph Strobl
	 */
	class ZInterStoreCommand extends KeyCommand {

		private final List<ByteBuffer> sourceKeys;
		private final List<Double> weights;
		private final Aggregate aggregateFunction;

		private ZInterStoreCommand(ByteBuffer key, List<ByteBuffer> sourceKeys, List<Double> weights, Aggregate aggregate) {

			super(key);
			this.sourceKeys = sourceKeys;
			this.weights = weights;
			this.aggregateFunction = aggregate;
		}

		public static ZInterStoreCommand sets(List<ByteBuffer> keys) {
			return new ZInterStoreCommand(null, keys, null, null);
		}

		public ZInterStoreCommand applyWeights(List<Double> weights) {
			return new ZInterStoreCommand(getKey(), sourceKeys, weights, aggregateFunction);
		}

		public ZInterStoreCommand aggregateUsing(Aggregate aggregateFunction) {
			return new ZInterStoreCommand(getKey(), sourceKeys, weights, aggregateFunction);
		}

		public ZInterStoreCommand storeAs(ByteBuffer key) {
			return new ZInterStoreCommand(key, sourceKeys, weights, aggregateFunction);
		}

		public List<ByteBuffer> getSourceKeys() {
			return sourceKeys;
		}

		public List<Double> getWeights() {
			return weights == null ? Collections.emptyList() : weights;
		}

		public Optional<Aggregate> getAggregateFunction() {
			return Optional.ofNullable(aggregateFunction);
		}

		public Integer getNumKeys() {
			return sourceKeys != null ? sourceKeys.size() : null;
		}
	}

	/**
	 * Intersect sorted {@code sets} and store result in destination {@code destinationKey}.
	 *
	 * @param destinationKey must not be {@literal null}.
	 * @param sets must not be {@literal null}.
	 * @return
	 */
	default Mono<Long> zInterStore(ByteBuffer destinationKey, List<ByteBuffer> sets) {
		return zInterStore(destinationKey, sets, null);
	}

	/**
	 * Intersect sorted {@code sets} and store result in destination {@code destinationKey} and apply weights to
	 * individual sets.
	 *
	 * @param destinationKey must not be {@literal null}.
	 * @param sets must not be {@literal null}.
	 * @param weights can be {@literal null}.
	 * @return
	 */
	default Mono<Long> zInterStore(ByteBuffer destinationKey, List<ByteBuffer> sets, List<Double> weights) {
		return zInterStore(destinationKey, sets, weights, null);
	}

	/**
	 * Intersect sorted {@code sets} by applying {@code aggregateFunction} and store result in destination
	 * {@code destinationKey} and apply weights to individual sets.
	 *
	 * @param destinationKey must not be {@literal null}.
	 * @param sets must not be {@literal null}.
	 * @param weights can be {@literal null}.
	 * @param aggregateFunction can be {@literal null}.
	 * @return
	 */
	default Mono<Long> zInterStore(ByteBuffer destinationKey, List<ByteBuffer> sets, List<Double> weights,
			Aggregate aggregateFunction) {

		Assert.notNull(destinationKey, "destinationKey must not be null");
		Assert.notNull(sets, "sets must not be null");

		return zInterStore(Mono.just(
				ZInterStoreCommand.sets(sets).aggregateUsing(aggregateFunction).applyWeights(weights).storeAs(destinationKey)))
						.next().map(NumericResponse::getOutput);
	}

	/**
	 * Intersect sorted {@code sets} by applying {@code aggregateFunction} and store result in destination
	 * {@code destinationKey} and apply weights to individual sets.
	 *
	 * @param commands
	 * @return
	 */
	Flux<NumericResponse<ZInterStoreCommand, Long>> zInterStore(Publisher<ZInterStoreCommand> commands);

	/**
	 * @author Christoph Strobl
	 */
	class ZRangeByLexCommand extends KeyCommand {

		private final Range<String> range;
		private final Direction direction;
		private final Limit limit;

		private ZRangeByLexCommand(ByteBuffer key, Range<String> range, Direction direction, Limit limit) {

			super(key);
			this.range = range;
			this.direction = direction;
			this.limit = limit;
		}

		public static ZRangeByLexCommand reverseStringsWithin(Range<String> range) {
			return new ZRangeByLexCommand(null, range, Direction.DESC, null);
		}

		public static ZRangeByLexCommand stringsWithin(Range<String> range) {
			return new ZRangeByLexCommand(null, range, Direction.ASC, null);
		}

		public ZRangeByLexCommand from(ByteBuffer key) {
			return new ZRangeByLexCommand(key, range, direction, limit);
		}

		public ZRangeByLexCommand limitTo(Limit limit) {
			return new ZRangeByLexCommand(getKey(), range, direction, limit);
		}

		public Range<String> getRange() {
			return range;
		}

		public Limit getLimit() {
			return limit;
		}

		public Direction getDirection() {
			return direction;
		}
	}

	/**
	 * Get all the elements in {@link Range} from the sorted set at {@literal key} in lexicographical ordering.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return
	 */
	default Mono<List<ByteBuffer>> zRangeByLex(ByteBuffer key, Range<String> range) {
		return zRangeByLex(key, range, null);
	}

	/**
	 * Get all the elements in {@link Range} from the sorted set at {@literal key} in lexicographical ordering. Result is
	 * limited via {@link Limit}.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @param limit can be {@literal null}.
	 * @return
	 */
	default Mono<List<ByteBuffer>> zRangeByLex(ByteBuffer key, Range<String> range, Limit limit) {

		Assert.notNull(key, "key must not be null");
		Assert.notNull(range, "range must not be null");

		return zRangeByLex(Mono.just(ZRangeByLexCommand.stringsWithin(range).from(key).limitTo(limit))).next()
				.map(MultiValueResponse::getOutput);
	}

	/**
	 * Get all the elements in {@link Range} from the sorted set at {@literal key} in lexicographical ordering.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return
	 */
	default Mono<List<ByteBuffer>> zRevRangeByLex(ByteBuffer key, Range<String> range) {
		return zRevRangeByLex(key, range, null);
	}

	/**
	 * Get all the elements in {@link Range} from the sorted set at {@literal key} in lexicographical ordering. Result is
	 * limited via {@link Limit}.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @param limit can be {@literal null}.
	 * @return
	 */
	default Mono<List<ByteBuffer>> zRevRangeByLex(ByteBuffer key, Range<String> range, Limit limit) {

		Assert.notNull(key, "key must not be null");
		Assert.notNull(range, "range must not be null");

		return zRangeByLex(Mono.just(ZRangeByLexCommand.reverseStringsWithin(range).from(key).limitTo(limit))).next()
				.map(MultiValueResponse::getOutput);
	}

	/**
	 * Get all the elements in {@link Range} from the sorted set at {@literal key} in lexicographical ordering. Result is
	 * limited via {@link Limit} and sorted by {@link ZRangeByLexCommand#getDirection()}.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @param limit can be {@literal null}.
	 * @return
	 */
	Flux<MultiValueResponse<ZRangeByLexCommand, ByteBuffer>> zRangeByLex(Publisher<ZRangeByLexCommand> commands);

}
