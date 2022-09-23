/*
 * Copyright 2016-2022 the original author or authors.
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
package org.springframework.data.redis.connection;

import static org.springframework.data.redis.connection.ReactiveRedisConnection.*;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.reactivestreams.Publisher;
import org.springframework.data.domain.Range;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.redis.connection.zset.Aggregate;
import org.springframework.data.redis.connection.zset.DefaultTuple;
import org.springframework.data.redis.connection.zset.Tuple;
import org.springframework.data.redis.connection.zset.Weights;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.util.ByteUtils;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Redis Sorted Set commands executed using reactive infrastructure.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Andrey Shlykov
 * @since 2.0
 */
public interface ReactiveZSetCommands {

	/**
	 * {@code ZADD} command parameters.
	 *
	 * @author Christoph Strobl
	 * @see <a href="https://redis.io/commands/zadd">Redis Documentation: ZADD</a>
	 */
	class ZAddCommand extends KeyCommand {

		private final List<Tuple> tuples;
		private final boolean upsert;
		private final boolean returnTotalChanged;
		private final boolean incr;
		private final boolean gt;
		private final boolean lt;

		private ZAddCommand(@Nullable ByteBuffer key, List<Tuple> tuples, boolean upsert, boolean returnTotalChanged,
				boolean incr, boolean gt, boolean lt) {

			super(key);

			this.tuples = tuples;
			this.upsert = upsert;
			this.returnTotalChanged = returnTotalChanged;
			this.incr = incr;
			this.gt = gt;
			this.lt = lt;
		}

		/**
		 * Creates a new {@link ZAddCommand} given a {@link Tuple}.
		 *
		 * @param tuple must not be {@literal null}.
		 * @return a new {@link ZAddCommand} for {@link Tuple}.
		 */
		public static ZAddCommand tuple(Tuple tuple) {

			Assert.notNull(tuple, "Tuple must not be null");

			return tuples(Collections.singletonList(tuple));
		}

		/**
		 * Creates a new {@link ZAddCommand} given a {@link Collection} of {@link Tuple}.
		 *
		 * @param tuples must not be {@literal null}.
		 * @return a new {@link ZAddCommand} for {@link Tuple}.
		 */
		public static ZAddCommand tuples(Collection<? extends Tuple> tuples) {

			Assert.notNull(tuples, "Tuples must not be null");

			return new ZAddCommand(null, new ArrayList<>(tuples), false, false, false, false, false);
		}

		/**
		 * Applies the {@literal key}. Constructs a new command instance with all previously configured properties.
		 *
		 * @param key must not be {@literal null}.
		 * @return a new {@link ZAddCommand} with {@literal key} applied.
		 */
		public ZAddCommand to(ByteBuffer key) {

			Assert.notNull(key, "Key must not be null");

			return new ZAddCommand(key, tuples, upsert, returnTotalChanged, incr, gt, lt);
		}

		/**
		 * Applies {@literal xx} mode (Only update elements that already exist. Never add elements). Constructs a new
		 * command instance with all previously configured properties.
		 *
		 * @return a new {@link ZAddCommand} with {@literal xx} applied.
		 */
		public ZAddCommand xx() {
			return new ZAddCommand(getKey(), tuples, false, returnTotalChanged, incr, gt, lt);
		}

		/**
		 * Applies {@literal nx} mode (Don't update already existing elements. Always add new elements). Constructs a new
		 * command instance with all previously configured properties.
		 *
		 * @return a new {@link ZAddCommand} with {@literal nx} applied.
		 */
		public ZAddCommand nx() {
			return new ZAddCommand(getKey(), tuples, true, returnTotalChanged, incr, gt, lt);
		}

		/**
		 * Applies {@literal ch} mode (Modify the return value from the number of new elements added, to the total number of
		 * elements changed). Constructs a new command instance with all previously configured properties.
		 *
		 * @return a new {@link ZAddCommand} with {@literal ch} applied.
		 */
		public ZAddCommand ch() {
			return new ZAddCommand(getKey(), tuples, upsert, true, incr, gt, lt);
		}

		/**
		 * Applies {@literal incr} mode (When this option is specified ZADD acts like ZINCRBY). Constructs a new command
		 * instance with all previously configured properties.
		 *
		 * @return a new {@link ZAddCommand} with {@literal incr} applied.
		 */
		public ZAddCommand incr() {
			return new ZAddCommand(getKey(), tuples, upsert, upsert, true, gt, lt);
		}

		/**
		 * Applies {@literal GT} mode. Constructs a new command instance with all previously configured properties.
		 *
		 * @return a new {@link ZAddCommand} with {@literal incr} applied.
		 * @since 2.5
		 */
		public ZAddCommand gt() {
			return new ZAddCommand(getKey(), tuples, upsert, upsert, incr, true, lt);
		}

		/**
		 * Applies {@literal LT} mode. Constructs a new command instance with all previously configured properties.
		 *
		 * @return a new {@link ZAddCommand} with {@literal incr} applied.
		 * @since 2.5
		 */
		public ZAddCommand lt() {
			return new ZAddCommand(getKey(), tuples, upsert, upsert, incr, gt, true);
		}

		/**
		 * @return
		 */
		public List<Tuple> getTuples() {
			return tuples;
		}

		/**
		 * @return
		 */
		public boolean isUpsert() {
			return upsert;
		}

		/**
		 * @return
		 */
		public boolean isIncr() {
			return incr;
		}

		/**
		 * @return {@literal true} if {@literal GT} is set.
		 * @since 2.5
		 */
		public boolean isGt() {
			return gt;
		}

		/**
		 * @return {@literal true} if {@literal LT} is set.
		 * @since 2.5
		 */
		public boolean isLt() {
			return lt;
		}

		/**
		 * @return
		 */
		public boolean isReturnTotalChanged() {
			return returnTotalChanged;
		}
	}

	/**
	 * Add {@literal value} to a sorted set at {@literal key}, or update its {@literal score} if it already exists.
	 *
	 * @param key must not be {@literal null}.
	 * @param score must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/zadd">Redis Documentation: ZADD</a>
	 */
	default Mono<Long> zAdd(ByteBuffer key, Double score, ByteBuffer value) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(score, "Score must not be null");
		Assert.notNull(value, "Value must not be null");

		return zAdd(Mono.just(ZAddCommand.tuple(new DefaultTuple(ByteUtils.getBytes(value), score)).to(key))).next()
				.map(resp -> resp.getOutput().longValue());
	}

	/**
	 * Add a {@literal tuples} to a sorted set at {@literal key}, or update their score if it already exists.
	 *
	 * @param key must not be {@literal null}.
	 * @param tuples must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/zadd">Redis Documentation: ZADD</a>
	 */
	default Mono<Long> zAdd(ByteBuffer key, Collection<? extends Tuple> tuples) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(tuples, "Tuples must not be null");

		return zAdd(Mono.just(ZAddCommand.tuples(tuples).to(key))).next().map(resp -> resp.getOutput().longValue());
	}

	/**
	 * Add {@link ZAddCommand#getTuples()} to a sorted set at {@link ZAddCommand#getKey()}, or update its {@literal score}
	 * if it already exists.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/zadd">Redis Documentation: ZADD</a>
	 */
	Flux<NumericResponse<ZAddCommand, Number>> zAdd(Publisher<ZAddCommand> commands);

	/**
	 * {@code ZREM} command parameters.
	 *
	 * @author Christoph Strobl
	 * @see <a href="https://redis.io/commands/zrem">Redis Documentation: ZREM</a>
	 */
	class ZRemCommand extends KeyCommand {

		private final List<ByteBuffer> values;

		private ZRemCommand(@Nullable ByteBuffer key, List<ByteBuffer> values) {

			super(key);

			this.values = values;
		}

		/**
		 * Creates a new {@link ZRemCommand} given a {@link Tuple}.
		 *
		 * @param value must not be {@literal null}.
		 * @return a new {@link ZRemCommand} for {@link Tuple}.
		 */
		public static ZRemCommand values(ByteBuffer value) {

			Assert.notNull(value, "Value must not be null");

			return new ZRemCommand(null, Collections.singletonList(value));
		}

		/**
		 * Creates a new {@link ZRemCommand} given a {@link Collection} of {@link Tuple}.
		 *
		 * @param values must not be {@literal null}.
		 * @return a new {@link ZRemCommand} for {@link Tuple}.
		 */
		public static ZRemCommand values(Collection<ByteBuffer> values) {

			Assert.notNull(values, "Values must not be null");

			return new ZRemCommand(null, new ArrayList<>(values));
		}

		/**
		 * Applies the {@literal key}. Constructs a new command instance with all previously configured properties.
		 *
		 * @param key must not be {@literal null}.
		 * @return a new {@link ZRemCommand} with {@literal key} applied.
		 */
		public ZRemCommand from(ByteBuffer key) {

			Assert.notNull(key, "Key must not be null");

			return new ZRemCommand(key, values);
		}

		/**
		 * @return
		 */
		public List<ByteBuffer> getValues() {
			return values;
		}
	}

	/**
	 * Remove {@literal value} from sorted set. Return number of removed elements.
	 *
	 * @param key must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/zrem">Redis Documentation: ZREM</a>
	 */
	default Mono<Long> zRem(ByteBuffer key, ByteBuffer value) {

		Assert.notNull(value, "Value must not be null");

		return zRem(key, Collections.singletonList(value));
	}

	/**
	 * Remove {@literal values} from sorted set. Return number of removed elements.
	 *
	 * @param key must not be {@literal null}.
	 * @param values must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/zrem">Redis Documentation: ZREM</a>
	 */
	default Mono<Long> zRem(ByteBuffer key, Collection<ByteBuffer> values) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(values, "Values must not be null");

		return zRem(Mono.just(ZRemCommand.values(values).from(key))).next().map(NumericResponse::getOutput);
	}

	/**
	 * Remove {@link ZRemCommand#getValues()} from sorted set. Return number of removed elements.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/zrem">Redis Documentation: ZREM</a>
	 */
	Flux<NumericResponse<ZRemCommand, Long>> zRem(Publisher<ZRemCommand> commands);

	/**
	 * {@code ZINCRBY} command parameters.
	 *
	 * @author Christoph Strobl
	 * @see <a href="https://redis.io/commands/zincrby">Redis Documentation: ZINCRBY</a>
	 */
	class ZIncrByCommand extends KeyCommand {

		private final ByteBuffer value;
		private final @Nullable Number increment;

		private ZIncrByCommand(@Nullable ByteBuffer key, ByteBuffer value, @Nullable Number increment) {

			super(key);

			this.value = value;
			this.increment = increment;
		}

		/**
		 * Creates a new {@link ZIncrByCommand} given a {@link ByteBuffer member}.
		 *
		 * @param member must not be {@literal null}.
		 * @return a new {@link ZAddCommand} for {@link Tuple}.
		 */
		public static ZIncrByCommand scoreOf(ByteBuffer member) {

			Assert.notNull(member, "Member must not be null");

			return new ZIncrByCommand(null, member, null);
		}

		/**
		 * Applies the numeric {@literal increment}. Constructs a new command instance with all previously configured
		 * properties.
		 *
		 * @param increment must not be {@literal null}.
		 * @return a new {@link ZIncrByCommand} with {@literal increment} applied.
		 */
		public ZIncrByCommand by(Number increment) {

			Assert.notNull(increment, "Increment must not be null");

			return new ZIncrByCommand(getKey(), value, increment);
		}

		/**
		 * Applies the {@literal key}. Constructs a new command instance with all previously configured properties.
		 *
		 * @param key must not be {@literal null}.
		 * @return a new {@link ZIncrByCommand} with {@literal key} applied.
		 */
		public ZIncrByCommand storedWithin(ByteBuffer key) {

			Assert.notNull(key, "Key must not be null");

			return new ZIncrByCommand(key, value, increment);
		}

		/**
		 * @return never {@literal null}.
		 */
		public ByteBuffer getValue() {
			return value;
		}

		/**
		 * @return can be {@literal null}.
		 */
		@Nullable
		public Number getIncrement() {
			return increment;
		}
	}

	/**
	 * Increment the score of element with {@literal value} in sorted set by {@literal increment}.
	 *
	 * @param key must not be {@literal null}.
	 * @param increment must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/zincrby">Redis Documentation: ZINCRBY</a>
	 */
	default Mono<Double> zIncrBy(ByteBuffer key, Number increment, ByteBuffer value) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(increment, "Increment must not be null");
		Assert.notNull(value, "Value must not be null");

		return zIncrBy(Mono.just(ZIncrByCommand.scoreOf(value).by(increment).storedWithin(key))).next()
				.map(NumericResponse::getOutput);
	}

	/**
	 * Increment the score of element with {@link ZIncrByCommand#getValue()} in sorted set by
	 * {@link ZIncrByCommand#getIncrement()}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/zincrby">Redis Documentation: ZINCRBY</a>
	 */
	Flux<NumericResponse<ZIncrByCommand, Double>> zIncrBy(Publisher<ZIncrByCommand> commands);

	/**
	 * {@code ZRANDMEMBER} command parameters.
	 *
	 * @author Mark Paluch
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/srandmember">Redis Documentation: ZRANDMEMBER</a>
	 */
	class ZRandMemberCommand extends KeyCommand {

		private final long count;

		private ZRandMemberCommand(@Nullable ByteBuffer key, long count) {

			super(key);
			this.count = count;
		}

		/**
		 * Creates a new {@link ZRandMemberCommand} given the number of values to retrieve.
		 *
		 * @param nrValuesToRetrieve
		 * @return a new {@link ZRandMemberCommand} for a number of values to retrieve.
		 */
		public static ZRandMemberCommand valueCount(long nrValuesToRetrieve) {
			return new ZRandMemberCommand(null, nrValuesToRetrieve);
		}

		/**
		 * Creates a new {@link ZRandMemberCommand} to retrieve one random member.
		 *
		 * @return a new {@link ZRandMemberCommand} to retrieve one random member.
		 */
		public static ZRandMemberCommand singleValue() {
			return new ZRandMemberCommand(null, 1);
		}

		/**
		 * Applies the {@literal key}. Constructs a new command instance with all previously configured properties.
		 *
		 * @param key must not be {@literal null}.
		 * @return a new {@link ZRandMemberCommand} with {@literal key} applied.
		 */
		public ZRandMemberCommand from(ByteBuffer key) {

			Assert.notNull(key, "Key must not be null");

			return new ZRandMemberCommand(key, count);
		}

		/**
		 * @return
		 */
		public long getCount() {
			return count;
		}
	}

	/**
	 * Get random element from sorted set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zrandmember">Redis Documentation: ZRANDMEMBER</a>
	 */
	default Mono<ByteBuffer> zRandMember(ByteBuffer key) {
		return zRandMember(Mono.just(ZRandMemberCommand.singleValue().from(key))).flatMap(CommandResponse::getOutput)
				.next();
	}

	/**
	 * Get {@code count} random elements from sorted set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param count if the provided {@code count} argument is positive, return a list of distinct fields, capped either at
	 *          {@code count} or the set size. If {@code count} is negative, the behavior changes and the command is
	 *          allowed to return the same value multiple times. In this case, the number of returned values is the
	 *          absolute value of the specified count.
	 * @return
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zrandmember">Redis Documentation: ZRANDMEMBER</a>
	 */
	default Flux<ByteBuffer> zRandMember(ByteBuffer key, long count) {
		return zRandMember(Mono.just(ZRandMemberCommand.valueCount(count).from(key))).flatMap(CommandResponse::getOutput);
	}

	/**
	 * Get random elements from sorted set at {@code key}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zrandmember">Redis Documentation: ZRANDMEMBER</a>
	 */
	Flux<CommandResponse<ZRandMemberCommand, Flux<ByteBuffer>>> zRandMember(Publisher<ZRandMemberCommand> commands);

	/**
	 * Get random element from sorted set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zrandmember">Redis Documentation: ZRANDMEMBER</a>
	 */
	default Mono<Tuple> zRandMemberWithScore(ByteBuffer key) {
		return zRandMemberWithScore(Mono.just(ZRandMemberCommand.singleValue().from(key)))
				.flatMap(CommandResponse::getOutput).next();
	}

	/**
	 * Get {@code count} random elements from sorted set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param count if the provided {@code count} argument is positive, return a list of distinct fields, capped either at
	 *          {@code count} or the set size. If {@code count} is negative, the behavior changes and the command is
	 *          allowed to return the same value multiple times. In this case, the number of returned values is the
	 *          absolute value of the specified count.
	 * @return
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zrandmember">Redis Documentation: ZRANDMEMBER</a>
	 */
	default Flux<Tuple> zRandMemberWithScore(ByteBuffer key, long count) {
		return zRandMemberWithScore(Mono.just(ZRandMemberCommand.valueCount(count).from(key)))
				.flatMap(CommandResponse::getOutput);
	}

	/**
	 * Get random elements from sorted set at {@code key}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zrandmember">Redis Documentation: ZRANDMEMBER</a>
	 */
	Flux<CommandResponse<ZRandMemberCommand, Flux<Tuple>>> zRandMemberWithScore(Publisher<ZRandMemberCommand> commands);

	/**
	 * {@code ZRANK}/{@literal ZREVRANK} command parameters.
	 *
	 * @author Christoph Strobl
	 * @see <a href="https://redis.io/commands/zrank">Redis Documentation: ZRANK</a>
	 * @see <a href="https://redis.io/commands/zrevrank">Redis Documentation: ZREVRANK</a>
	 */
	class ZRankCommand extends KeyCommand {

		private final ByteBuffer value;
		private final Direction direction;

		private ZRankCommand(@Nullable ByteBuffer key, ByteBuffer value, Direction direction) {

			super(key);

			this.value = value;
			this.direction = direction;
		}

		/**
		 * Creates a new {@link ZRankCommand} given a {@link ByteBuffer member} to obtain its rank (ordering low to high).
		 *
		 * @param member must not be {@literal null}.
		 * @return a new {@link ZRankCommand} for {@link Tuple}.
		 */
		public static ZRankCommand indexOf(ByteBuffer member) {

			Assert.notNull(member, "Member must not be null");

			return new ZRankCommand(null, member, Direction.ASC);
		}

		/**
		 * Creates a new {@link ZIncrByCommand} given a {@link ByteBuffer member} to obtain its reversed rank (ordering high
		 * to low).
		 *
		 * @param member must not be {@literal null}.
		 * @return a new {@link ZRankCommand} for {@link Tuple}.
		 */
		public static ZRankCommand reverseIndexOf(ByteBuffer member) {

			Assert.notNull(member, "Member must not be null");

			return new ZRankCommand(null, member, Direction.DESC);
		}

		/**
		 * Applies the {@literal key}. Constructs a new command instance with all previously configured properties.
		 *
		 * @param key must not be {@literal null}.
		 * @return a new {@link ZRankCommand} with {@literal key} applied.
		 */
		public ZRankCommand storedWithin(ByteBuffer key) {

			Assert.notNull(key, "Key must not be null");

			return new ZRankCommand(key, value, direction);
		}

		/**
		 * @return
		 */
		public ByteBuffer getValue() {
			return value;
		}

		/**
		 * @return
		 */
		public Direction getDirection() {
			return direction;
		}
	}

	/**
	 * Determine the index of element with {@literal value} in a sorted set.
	 *
	 * @param key must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/zrank">Redis Documentation: ZRANK</a>
	 */
	default Mono<Long> zRank(ByteBuffer key, ByteBuffer value) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(value, "Value must not be null");

		return zRank(Mono.just(ZRankCommand.indexOf(value).storedWithin(key))).next().map(NumericResponse::getOutput);
	}

	/**
	 * Determine the index of element with {@literal value} in a sorted set when scored high to low.
	 *
	 * @param key must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/zrevrank">Redis Documentation: ZREVRANK</a>
	 */
	default Mono<Long> zRevRank(ByteBuffer key, ByteBuffer value) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(value, "Value must not be null");

		return zRank(Mono.just(ZRankCommand.reverseIndexOf(value).storedWithin(key))).next()
				.map(NumericResponse::getOutput);
	}

	/**
	 * Determine the index of element with {@literal value} in a sorted set when scored by
	 * {@link ZRankCommand#getDirection()}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/zrank">Redis Documentation: ZRANK</a>
	 * @see <a href="https://redis.io/commands/zrevrank">Redis Documentation: ZREVRANK</a>
	 */
	Flux<NumericResponse<ZRankCommand, Long>> zRank(Publisher<ZRankCommand> commands);

	/**
	 * {@code ZRANGE}/{@literal ZREVRANGE} command parameters.
	 *
	 * @author Christoph Strobl
	 * @see <a href="https://redis.io/commands/zrange">Redis Documentation: ZRANGE</a>
	 * @see <a href="https://redis.io/commands/zrevrange">Redis Documentation: ZREVRANGE</a>
	 */
	class ZRangeCommand extends KeyCommand {

		private final Range<Long> range;
		private final boolean withScores;
		private final Direction direction;

		private ZRangeCommand(@Nullable ByteBuffer key, Range<Long> range, Direction direction, boolean withScores) {

			super(key);

			this.range = range;
			this.withScores = withScores;
			this.direction = direction;
		}

		/**
		 * Creates a new {@link ZRangeCommand} given a {@link Range} to obtain elements ordered from the lowest to the
		 * highest score.
		 *
		 * @param range must not be {@literal null}.
		 * @return a new {@link ZRangeCommand} for {@link Tuple}.
		 */
		public static ZRangeCommand valuesWithin(Range<Long> range) {

			Assert.notNull(range, "Range must not be null");

			return new ZRangeCommand(null, range, Direction.ASC, false);
		}

		/**
		 * Creates a new {@link ZRangeCommand} given a {@link Range} to obtain elements ordered from the highest to the
		 * lowest score.
		 *
		 * @param range must not be {@literal null}.
		 * @return a new {@link ZRangeCommand} for {@link Tuple}.
		 */
		public static ZRangeCommand reverseValuesWithin(Range<Long> range) {

			Assert.notNull(range, "Range must not be null");

			return new ZRangeCommand(null, range, Direction.DESC, false);
		}

		/**
		 * Return the score along with each returned element. Constructs a new command instance with all previously
		 * configured properties.
		 *
		 * @return a new {@link ZRangeCommand} with score retrieval applied.
		 */
		public ZRangeCommand withScores() {
			return new ZRangeCommand(getKey(), range, direction, true);
		}

		/**
		 * Applies the {@literal key}. Constructs a new command instance with all previously configured properties.
		 *
		 * @param key must not be {@literal null}.
		 * @return a new {@link ZRangeCommand} with {@literal key} applied.
		 */
		public ZRangeCommand from(ByteBuffer key) {

			Assert.notNull(key, "Key must not be null");

			return new ZRangeCommand(key, range, direction, withScores);
		}

		/**
		 * @return
		 */
		public Range<Long> getRange() {
			return range;
		}

		/**
		 * @return
		 */
		public boolean isWithScores() {
			return withScores;
		}

		/**
		 * @return
		 */
		public Direction getDirection() {
			return direction;
		}
	}

	/**
	 * Get elements in {@literal range} from sorted set.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/zrange">Redis Documentation: ZRANGE</a>
	 */
	default Flux<ByteBuffer> zRange(ByteBuffer key, Range<Long> range) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(range, "Range must not be null");

		return zRange(Mono.just(ZRangeCommand.valuesWithin(range).from(key))) //
				.flatMap(CommandResponse::getOutput).map(tuple -> ByteBuffer.wrap(tuple.getValue()));
	}

	/**
	 * Get set of {@link Tuple}s in {@literal range} from sorted set.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/zrange">Redis Documentation: ZRANGE</a>
	 */
	default Flux<Tuple> zRangeWithScores(ByteBuffer key, Range<Long> range) {

		Assert.notNull(key, "Key must not be null");

		return zRange(Mono.just(ZRangeCommand.valuesWithin(range).withScores().from(key)))
				.flatMap(CommandResponse::getOutput);
	}

	/**
	 * Get elements in {@literal range} from sorted set in reverse {@literal score} ordering.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/zrevrange">Redis Documentation: ZREVRANGE</a>
	 */
	default Flux<ByteBuffer> zRevRange(ByteBuffer key, Range<Long> range) {

		Assert.notNull(key, "Key must not be null");

		return zRange(Mono.just(ZRangeCommand.reverseValuesWithin(range).from(key))).flatMap(CommandResponse::getOutput)
				.map(tuple -> ByteBuffer.wrap(tuple.getValue()));
	}

	/**
	 * Get set of {@link Tuple}s in {@literal range} from sorted set in reverse {@literal score} ordering.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/zrevrange">Redis Documentation: ZREVRANGE</a>
	 */
	default Flux<Tuple> zRevRangeWithScores(ByteBuffer key, Range<Long> range) {

		Assert.notNull(key, "Key must not be null");

		return zRange(Mono.just(ZRangeCommand.reverseValuesWithin(range).withScores().from(key)))
				.flatMap(CommandResponse::getOutput);
	}

	/**
	 * Get set of {@link Tuple}s in {@literal range} from sorted set.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/zrange">Redis Documentation: ZRANGE</a>
	 * @see <a href="https://redis.io/commands/zrevrange">Redis Documentation: ZREVRANGE</a>
	 */
	Flux<CommandResponse<ZRangeCommand, Flux<Tuple>>> zRange(Publisher<ZRangeCommand> commands);

	/**
	 * {@code ZRANGESTORE} command parameters.
	 *
	 * @author Mark Paluch
	 * @since 3.0
	 * @see <a href="https://redis.io/commands/zrangestore">Redis Documentation: ZRANGESTORE</a>
	 */
	class ZRangeStoreCommand extends KeyCommand {

		private final ByteBuffer destKey;
		private final RangeMode rangeMode;
		private final Range<?> range;
		private final Direction direction;
		private final Limit limit;

		private ZRangeStoreCommand(@Nullable ByteBuffer srcKey, @Nullable ByteBuffer destKey, RangeMode rangeMode,
				Range<?> range, Direction direction, Limit limit) {

			super(srcKey);
			this.destKey = destKey;
			this.rangeMode = rangeMode;
			this.range = range;
			this.direction = direction;
			this.limit = limit;
		}

		/**
		 * Creates a new {@link ZRangeStoreCommand} given a {@link Range} to obtain elements ordered from the lowest to the
		 * highest score.
		 *
		 * @param range must not be {@literal null}.
		 * @return a new {@link ZRangeCommand} for {@link Tuple}.
		 */
		public static ZRangeStoreCommand scoreWithin(Range<Double> range) {

			Assert.notNull(range, "Range must not be null");

			return new ZRangeStoreCommand(null, null, RangeMode.ByScore, range, Direction.ASC, Limit.unlimited());
		}

		/**
		 * Creates a new {@link ZRangeStoreCommand} given a {@link Range} to obtain elements ordered from the highest to the
		 * lowest score.
		 *
		 * @param range must not be {@literal null}.
		 * @return a new {@link ZRangeStoreCommand} for {@link Tuple}.
		 */
		public static ZRangeStoreCommand reverseScoreWithin(Range<Double> range) {

			Assert.notNull(range, "Range must not be null");

			return new ZRangeStoreCommand(null, null, RangeMode.ByScore, range, Direction.DESC, Limit.unlimited());
		}

		/**
		 * Creates a new {@link ZRangeStoreCommand} given a {@link Range} to obtain elements ordered from the lowest to the
		 * highest lexicographical value.
		 *
		 * @param range must not be {@literal null}.
		 * @return a new {@link ZRangeCommand} for {@link Tuple}.
		 */
		public static ZRangeStoreCommand valueWithin(Range<String> range) {

			Assert.notNull(range, "Range must not be null");

			return new ZRangeStoreCommand(null, null, RangeMode.ByLex, range, Direction.ASC, Limit.unlimited());
		}

		/**
		 * Creates a new {@link ZRangeStoreCommand} given a {@link Range} to obtain elements ordered from the highest to the
		 * lowest lexicographical value.
		 *
		 * @param range must not be {@literal null}.
		 * @return a new {@link ZRangeStoreCommand} for {@link Tuple}.
		 */
		public static ZRangeStoreCommand reverseValueWithin(Range<String> range) {

			Assert.notNull(range, "Range must not be null");

			return new ZRangeStoreCommand(null, null, RangeMode.ByLex, range, Direction.DESC, Limit.unlimited());
		}

		/**
		 * Applies the {@literal key}. Constructs a new command instance with all previously configured properties.
		 *
		 * @param key must not be {@literal null}.
		 * @return a new {@link ZRangeStoreCommand} with {@literal key} applied.
		 */
		public ZRangeStoreCommand from(ByteBuffer key) {

			Assert.notNull(key, "Key must not be null");

			return new ZRangeStoreCommand(key, destKey, rangeMode, range, direction, limit);
		}

		/**
		 * Applies the {@literal key}. Constructs a new command instance with all previously configured properties.
		 *
		 * @param key must not be {@literal null}.
		 * @return a new {@link ZRangeStoreCommand} with {@literal key} applied.
		 */
		public ZRangeStoreCommand to(ByteBuffer key) {

			Assert.notNull(key, "Key must not be null");

			return new ZRangeStoreCommand(getKey(), key, rangeMode, range, direction, limit);
		}

		/**
		 * Applies the {@literal limit}. Constructs a new command instance with all previously configured properties.
		 *
		 * @param limit must not be {@literal null}.
		 * @return a new {@link ZRangeStoreCommand} with {@literal key} applied.
		 */
		public ZRangeStoreCommand limit(Limit limit) {

			Assert.notNull(limit, "Limit must not be null");

			return new ZRangeStoreCommand(getKey(), getDestKey(), rangeMode, range, direction, limit);
		}

		public ByteBuffer getDestKey() {
			return destKey;
		}

		public RangeMode getRangeMode() {
			return rangeMode;
		}

		public Range<?> getRange() {
			return range;
		}

		public Direction getDirection() {
			return direction;
		}

		public Limit getLimit() {
			return limit;
		}

		public enum RangeMode {
			ByScore, ByLex,
		}
	}

	/**
	 * Add elements from {@code srcKey} by score {@literal range} to {@code destKey}.
	 *
	 * @param srcKey must not be {@literal null}.
	 * @param destKey must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @param limit must not be {@literal null}.
	 * @return
	 * @since 3.0
	 * @see <a href="https://redis.io/commands/zrangestore">Redis Documentation: ZRANGESTORE</a>
	 */
	default Mono<Long> zRangeStoreByScore(ByteBuffer srcKey, ByteBuffer destKey, Range<Double> range, Limit limit) {

		Assert.notNull(srcKey, "Source key must not be null");
		Assert.notNull(destKey, "Destination key must not be null");
		Assert.notNull(range, "Range must not be null");
		Assert.notNull(limit, "Limit must not be null");

		return zRangeStore(Mono.just(ZRangeStoreCommand.scoreWithin(range).from(srcKey).to(destKey).limit(limit))) //
				.flatMap(CommandResponse::getOutput).next();
	}

	/**
	 * Add elements from {@code srcKey} by lexicographical {@literal range} to {@code destKey}.
	 *
	 * @param srcKey must not be {@literal null}.
	 * @param destKey must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @param limit must not be {@literal null}.
	 * @return
	 * @since 3.0
	 * @see <a href="https://redis.io/commands/zrangestore">Redis Documentation: ZRANGESTORE</a>
	 */
	default Mono<Long> zRangeStoreByLex(ByteBuffer srcKey, ByteBuffer destKey, Range<String> range, Limit limit) {

		Assert.notNull(srcKey, "Source key must not be null");
		Assert.notNull(destKey, "Destination key must not be null");
		Assert.notNull(range, "Range must not be null");
		Assert.notNull(limit, "Limit must not be null");

		return zRangeStore(Mono.just(ZRangeStoreCommand.valueWithin(range).from(srcKey).to(destKey).limit(limit))) //
				.flatMap(CommandResponse::getOutput).next();
	}

	/**
	 * Add elements from {@code srcKey} by reverse score {@literal range} to {@code destKey}.
	 *
	 * @param srcKey must not be {@literal null}.
	 * @param destKey must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @param limit must not be {@literal null}.
	 * @return
	 * @since 3.0
	 * @see <a href="https://redis.io/commands/zrangestore">Redis Documentation: ZRANGESTORE</a>
	 */
	default Mono<Long> zRangeStoreRevByScore(ByteBuffer srcKey, ByteBuffer destKey, Range<Double> range, Limit limit) {

		Assert.notNull(srcKey, "Source key must not be null");
		Assert.notNull(destKey, "Destination key must not be null");
		Assert.notNull(range, "Range must not be null");
		Assert.notNull(limit, "Limit must not be null");

		return zRangeStore(Mono.just(ZRangeStoreCommand.reverseScoreWithin(range).from(srcKey).to(destKey).limit(limit))) //
				.flatMap(CommandResponse::getOutput).next();
	}

	/**
	 * Add elements from {@code srcKey} by reverse lexicographical {@literal range} to {@code destKey}.
	 *
	 * @param srcKey must not be {@literal null}.
	 * @param destKey must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @param limit must not be {@literal null}.
	 * @return
	 * @since 3.0
	 * @see <a href="https://redis.io/commands/zrangestore">Redis Documentation: ZRANGESTORE</a>
	 */
	default Mono<Long> zRangeStoreRevByLex(ByteBuffer srcKey, ByteBuffer destKey, Range<String> range, Limit limit) {

		Assert.notNull(srcKey, "Source key must not be null");
		Assert.notNull(destKey, "Destination key must not be null");
		Assert.notNull(range, "Range must not be null");
		Assert.notNull(limit, "Limit must not be null");

		return zRangeStore(Mono.just(ZRangeStoreCommand.reverseValueWithin(range).from(srcKey).to(destKey).limit(limit))) //
				.flatMap(CommandResponse::getOutput).next();
	}

	/**
	 * Get set of {@link Tuple}s in {@literal range} from sorted set.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 * @since 3.0
	 * @see <a href="https://redis.io/commands/zrangestore">Redis Documentation: ZRANGESTORE</a>
	 */
	Flux<CommandResponse<ZRangeStoreCommand, Mono<Long>>> zRangeStore(Publisher<ZRangeStoreCommand> commands);

	/**
	 * {@literal ZRANGEBYSCORE}/{@literal ZREVRANGEBYSCORE}.
	 *
	 * @author Christoph Strobl
	 * @see <a href="https://redis.io/commands/zrangebyscore">Redis Documentation: ZRANGEBYSCORE</a>
	 * @see <a href="https://redis.io/commands/zrevrangebyscore">Redis Documentation: ZREVRANGEBYSCORE</a>
	 */
	class ZRangeByScoreCommand extends KeyCommand {

		private final Range<Double> range;
		private final boolean withScores;
		private final Direction direction;
		private final @Nullable Limit limit;

		private ZRangeByScoreCommand(@Nullable ByteBuffer key, Range<Double> range, Direction direction, boolean withScores,
				@Nullable Limit limit) {

			super(key);

			this.range = range;
			this.withScores = withScores;
			this.direction = direction;
			this.limit = limit;
		}

		/**
		 * Creates a new {@link ZRangeByScoreCommand} given a {@link Range} to obtain elements ordered from the lowest to
		 * the highest score.
		 *
		 * @param range must not be {@literal null}.
		 * @return a new {@link ZRangeByScoreCommand} for {@link Tuple}.
		 */
		public static ZRangeByScoreCommand scoresWithin(Range<Double> range) {

			Assert.notNull(range, "Range must not be null");

			return new ZRangeByScoreCommand(null, range, Direction.ASC, false, null);
		}

		/**
		 * Creates a new {@link ZRangeByScoreCommand} given a {@link Range} to obtain elements ordered from the highest to
		 * the lowest score.
		 *
		 * @param range must not be {@literal null}.
		 * @return a new {@link ZRangeByScoreCommand} for {@link Tuple}.
		 */
		public static ZRangeByScoreCommand reverseScoresWithin(Range<Double> range) {

			Assert.notNull(range, "Range must not be null");

			return new ZRangeByScoreCommand(null, range, Direction.DESC, false, null);
		}

		/**
		 * Return the score along with each returned element. Constructs a new command instance with all previously
		 * configured properties.
		 *
		 * @return a new {@link ZRangeByScoreCommand} with score retrieval applied.
		 */
		public ZRangeByScoreCommand withScores() {
			return new ZRangeByScoreCommand(getKey(), range, direction, true, limit);
		}

		/**
		 * Applies the {@literal key}. Constructs a new command instance with all previously configured properties.
		 *
		 * @param key must not be {@literal null}.
		 * @return a new {@link ZRangeByScoreCommand} with {@literal key} applied.
		 */
		public ZRangeByScoreCommand from(ByteBuffer key) {

			Assert.notNull(key, "Key must not be null");

			return new ZRangeByScoreCommand(key, range, direction, withScores, limit);
		}

		/**
		 * Applies the {@link Limit}. Constructs a new command instance with all previously configured properties.
		 *
		 * @param limit must not be {@literal null}.
		 * @return a new {@link ZRangeByScoreCommand} with {@link Limit} applied.
		 */
		public ZRangeByScoreCommand limitTo(Limit limit) {

			Assert.notNull(limit, "Limit must not be null");
			return new ZRangeByScoreCommand(getKey(), range, direction, withScores, limit);
		}

		/**
		 * @return
		 */
		public Range<Double> getRange() {
			return range;
		}

		/**
		 * @return
		 */
		public boolean isWithScores() {
			return withScores;
		}

		/**
		 * @return
		 */
		public Direction getDirection() {
			return direction;
		}

		/**
		 * @return
		 */
		public Optional<Limit> getLimit() {
			return Optional.ofNullable(limit);
		}
	}

	/**
	 * Get elements in {@literal range} from sorted set.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/zrangebyscore">Redis Documentation: ZRANGEBYSCORE</a>
	 */
	default Flux<ByteBuffer> zRangeByScore(ByteBuffer key, Range<Double> range) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(range, "Range must not be null");

		return zRangeByScore(Mono.just(ZRangeByScoreCommand.scoresWithin(range).from(key))) //
				.flatMap(CommandResponse::getOutput) //
				.map(tuple -> ByteBuffer.wrap(tuple.getValue()));
	}

	/**
	 * Get elements in {@literal range} from sorted set.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @param limit must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/zrangebyscore">Redis Documentation: ZRANGEBYSCORE</a>
	 */
	default Flux<ByteBuffer> zRangeByScore(ByteBuffer key, Range<Double> range, Limit limit) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(range, "Range must not be null");
		Assert.notNull(limit, "Limit must not be null");

		return zRangeByScore(Mono.just(ZRangeByScoreCommand.scoresWithin(range).from(key).limitTo(limit))) //
				.flatMap(CommandResponse::getOutput) //
				.map(tuple -> ByteBuffer.wrap(tuple.getValue()));
	}

	/**
	 * Get {@link Tuple}s in {@literal range} from sorted set.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/zrangebyscore">Redis Documentation: ZRANGEBYSCORE</a>
	 */
	default Flux<Tuple> zRangeByScoreWithScores(ByteBuffer key, Range<Double> range) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(range, "Range must not be null");

		return zRangeByScore(Mono.just(ZRangeByScoreCommand.scoresWithin(range).withScores().from(key)))
				.flatMap(CommandResponse::getOutput);
	}

	/**
	 * Get {@link Tuple}s in {@literal range} from sorted set.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @param limit must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/zrangebyscore">Redis Documentation: ZRANGEBYSCORE</a>
	 */
	default Flux<Tuple> zRangeByScoreWithScores(ByteBuffer key, Range<Double> range, Limit limit) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(range, "Range must not be null");
		Assert.notNull(limit, "Limit must not be null");

		return zRangeByScore(Mono.just(ZRangeByScoreCommand.scoresWithin(range).withScores().from(key).limitTo(limit)))
				.flatMap(CommandResponse::getOutput);
	}

	/**
	 * Get elements in {@literal range} from sorted set in reverse {@literal score} ordering.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/zrevrangebyscore">Redis Documentation: ZREVRANGEBYSCORE</a>
	 */
	default Flux<ByteBuffer> zRevRangeByScore(ByteBuffer key, Range<Double> range) {

		Assert.notNull(key, "Key must not be null");

		return zRangeByScore(Mono.just(ZRangeByScoreCommand.reverseScoresWithin(range).from(key))) //
				.flatMap(CommandResponse::getOutput) //
				.map(tuple -> ByteBuffer.wrap(tuple.getValue()));
	}

	/**
	 * Get elements in {@literal range} from sorted set in reverse {@literal score} ordering.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @param limit must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/zrevrangebyscore">Redis Documentation: ZREVRANGEBYSCORE</a>
	 */
	default Flux<ByteBuffer> zRevRangeByScore(ByteBuffer key, Range<Double> range, Limit limit) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(range, "Range must not be null");
		Assert.notNull(limit, "Limit must not be null");

		return zRangeByScore(Mono.just(ZRangeByScoreCommand.reverseScoresWithin(range).from(key).limitTo(limit))) //
				.flatMap(CommandResponse::getOutput) //
				.map(tuple -> ByteBuffer.wrap(tuple.getValue()));
	}

	/**
	 * Get set of {@link Tuple}s in {@literal range} from sorted set in reverse {@literal score} ordering.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/zrevrangebyscore">Redis Documentation: ZREVRANGEBYSCORE</a>
	 */
	default Flux<Tuple> zRevRangeByScoreWithScores(ByteBuffer key, Range<Double> range) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(range, "Range must not be null");

		return zRangeByScore(Mono.just(ZRangeByScoreCommand.reverseScoresWithin(range).withScores().from(key)))
				.flatMap(CommandResponse::getOutput);
	}

	/**
	 * Get {@link Tuple}s in {@literal range} from sorted set in reverse {@literal score} ordering.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @param limit must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/zrevrangebyscore">Redis Documentation: ZREVRANGEBYSCORE</a>
	 */
	default Flux<Tuple> zRevRangeByScoreWithScores(ByteBuffer key, Range<Double> range, Limit limit) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(range, "Range must not be null");
		Assert.notNull(limit, "Limit must not be null");

		return zRangeByScore(
				Mono.just(ZRangeByScoreCommand.reverseScoresWithin(range).withScores().from(key).limitTo(limit)))
						.flatMap(CommandResponse::getOutput);
	}

	/**
	 * Get {@link Tuple}s in {@literal range} from sorted set.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/zrangebyscore">Redis Documentation: ZRANGEBYSCORE</a>
	 * @see <a href="https://redis.io/commands/zrevrangebyscore">Redis Documentation: ZREVRANGEBYSCORE</a>
	 */
	Flux<CommandResponse<ZRangeByScoreCommand, Flux<Tuple>>> zRangeByScore(Publisher<ZRangeByScoreCommand> commands);

	/**
	 * Use a {@link Flux} to iterate over members in the sorted set at {@code key}. The resulting {@link Flux} acts as a
	 * cursor and issues {@code ZSCAN} commands itself as long as the subscriber signals demand.
	 *
	 * @param key must not be {@literal null}.
	 * @return the {@link Flux} emitting the raw {@link Tuple tuples} one by one.
	 * @throws IllegalArgumentException when key is {@literal null}.
	 * @see <a href="https://redis.io/commands/zscan">Redis Documentation: ZSCAN</a>
	 * @since 2.1
	 */
	default Flux<Tuple> zScan(ByteBuffer key) {
		return zScan(key, ScanOptions.NONE);
	}

	/**
	 * Use a {@link Flux} to iterate over members in the sorted set at {@code key} given {@link ScanOptions}. The
	 * resulting {@link Flux} acts as a cursor and issues {@code ZSCAN} commands itself as long as the subscriber signals.
	 *
	 * @param key must not be {@literal null}.
	 * @param options must not be {@literal null}. Use {@link ScanOptions#NONE} instead.
	 * @return the {@link Flux} emitting the raw {@link Tuple tuples} one by one.
	 * @throws IllegalArgumentException when one of the required arguments is {@literal null}.
	 * @see <a href="https://redis.io/commands/zscan">Redis Documentation: ZSCAN</a>
	 * @since 2.1
	 */
	default Flux<Tuple> zScan(ByteBuffer key, ScanOptions options) {

		return zScan(Mono.just(KeyScanCommand.key(key).withOptions(options))).map(CommandResponse::getOutput)
				.flatMap(it -> it);
	}

	/**
	 * Use a {@link Flux} to iterate over members in the sorted set at {@code key}. The resulting {@link Flux} acts as a
	 * cursor and issues {@code ZSCAN} commands itself as long as the subscriber signals demand.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/zscan">Redis Documentation: ZSCAN</a>
	 * @since 2.1
	 */
	Flux<CommandResponse<KeyCommand, Flux<Tuple>>> zScan(Publisher<KeyScanCommand> commands);

	/**
	 * {@code ZCOUNT} command parameters.
	 *
	 * @author Christoph Strobl
	 * @see <a href="https://redis.io/commands/zcount">Redis Documentation: ZCOUNT</a>
	 */
	class ZCountCommand extends KeyCommand {

		private final Range<Double> range;

		private ZCountCommand(@Nullable ByteBuffer key, Range<Double> range) {

			super(key);
			this.range = range;
		}

		/**
		 * Creates a new {@link ZCountCommand} given a {@link Range}.
		 *
		 * @param range must not be {@literal null}.
		 * @return a new {@link ZCountCommand} for {@link Range}.
		 */
		public static ZCountCommand scoresWithin(Range<Double> range) {

			Assert.notNull(range, "Range must not be null");

			return new ZCountCommand(null, range);
		}

		/**
		 * Applies the {@literal key}. Constructs a new command instance with all previously configured properties.
		 *
		 * @param key must not be {@literal null}.
		 * @return a new {@link ZCountCommand} with {@literal key} applied.
		 */
		public ZCountCommand forKey(ByteBuffer key) {

			Assert.notNull(key, "Key must not be null");

			return new ZCountCommand(key, range);
		}

		/**
		 * @return
		 */
		public Range<Double> getRange() {
			return range;
		}
	}

	/**
	 * Count number of elements within sorted set with scores within {@link Range}. <br />
	 * <b>NOTE</b> please use {@link Double#NEGATIVE_INFINITY} for {@literal -inf} and {@link Double#POSITIVE_INFINITY}
	 * for {@literal +inf}.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/zcount">Redis Documentation: ZCOUNT</a>
	 */
	default Mono<Long> zCount(ByteBuffer key, Range<Double> range) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(range, "Range must not be null");

		return zCount(Mono.just(ZCountCommand.scoresWithin(range).forKey(key))).next().map(NumericResponse::getOutput);
	}

	/**
	 * Count number of elements within sorted set with scores within {@link Range}. <br />
	 * <b>NOTE</b> please use {@link Double#NEGATIVE_INFINITY} for {@literal -inf} and {@link Double#POSITIVE_INFINITY}
	 * for {@literal +inf}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/zcount">Redis Documentation: ZCOUNT</a>
	 */
	Flux<NumericResponse<ZCountCommand, Long>> zCount(Publisher<ZCountCommand> commands);

	/**
	 * {@code ZLEXCOUNT} command parameters.
	 *
	 * @author Andrey Shlykov
	 * @since 2.4
	 * @see <a href="https://redis.io/commands/zlexcount">Redis Documentation: ZLEXCOUNT</a>
	 */
	class ZLexCountCommand extends KeyCommand {

		private final Range<String> range;

		private ZLexCountCommand(@Nullable ByteBuffer key, Range<String> range) {
			super(key);
			this.range = range;
		}

		/**
		 * Creates a new {@link ZLexCountCommand} given a {@link Range} of {@link String} to retrieve elements count.
		 *
		 * @param range must not be {@literal null}.
		 * @return a new {@link ZLexCountCommand} for {@link Range}.
		 */
		public static ZLexCountCommand stringsWithin(Range<String> range) {

			Assert.notNull(range, "Range must not be null");

			return new ZLexCountCommand(null, range);
		}

		/**
		 * Applies the {@literal key}. Constructs a new command instance with all previously configured properties.
		 *
		 * @param key must not be {@literal null}.
		 * @return a new {@link ZLexCountCommand} with {@literal key} applied.
		 */
		public ZLexCountCommand forKey(ByteBuffer key) {

			Assert.notNull(key, "Key must not be null");

			return new ZLexCountCommand(key, range);
		}

		/**
		 * @return
		 */
		public Range<String> getRange() {
			return range;
		}
	}

	/**
	 * Count number of elements within sorted set with value between {@code Range#min} and {@code Range#max} applying
	 * lexicographical ordering.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return
	 * @since 2.4
	 * @see <a href="https://redis.io/commands/zlexcount">Redis Documentation: ZLEXCOUNT</a>
	 */
	default Mono<Long> zLexCount(ByteBuffer key, Range<String> range) {
		return zLexCount(Mono.just(ZLexCountCommand.stringsWithin(range).forKey(key))).next()
				.map(NumericResponse::getOutput);
	}

	/**
	 * Count number of elements within sorted set with value between {@code Range#min} and {@code Range#max} applying
	 * lexicographical ordering.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 * @since 2.4
	 * @see <a href="https://redis.io/commands/zlexcount">Redis Documentation: ZLEXCOUNT</a>
	 */
	Flux<NumericResponse<ZLexCountCommand, Long>> zLexCount(Publisher<ZLexCountCommand> commands);

	/**
	 * @author Mark Paluch
	 */
	enum PopDirection {
		MIN, MAX
	}

	/**
	 * {@code ZPOPMIN}/{@literal ZPOPMAX} command parameters.
	 *
	 * @author Mark Paluch
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zpopmin">Redis Documentation: ZPOPMIN</a>
	 * @see <a href="https://redis.io/commands/zpopmax">Redis Documentation: ZPOPMAX</a>
	 */
	class ZPopCommand extends KeyCommand {

		private final PopDirection direction;

		private final long count;

		private ZPopCommand(PopDirection direction, @Nullable ByteBuffer key, long count) {

			super(key);
			this.count = count;
			this.direction = direction;
		}

		/**
		 * Creates a new {@link ZPopCommand} for min pop ({@literal ZPOPMIN}).
		 *
		 * @return a new {@link ZPopCommand} for min pop ({@literal ZPOPMIN}).
		 */
		public static ZPopCommand min() {
			return new ZPopCommand(PopDirection.MIN, null, 1);
		}

		/**
		 * Creates a new {@link ZPopCommand} for max pop ({@literal ZPOPMAX}).
		 *
		 * @return a new {@link ZPopCommand} for max pop ({@literal ZPOPMAX}).
		 */
		public static ZPopCommand max() {
			return new ZPopCommand(PopDirection.MAX, null, 1);
		}

		/**
		 * Applies the {@literal key}. Constructs a new command instance with all previously configured properties.
		 *
		 * @param key must not be {@literal null}.
		 * @return a new {@link ZPopCommand} with {@literal value} applied.
		 */
		public ZPopCommand from(ByteBuffer key) {

			Assert.notNull(key, "Key must not be null");

			return new ZPopCommand(direction, key, count);
		}

		/**
		 * Applies the {@literal key}. Constructs a new command instance with all previously configured properties.
		 *
		 * @param count
		 * @return a new {@link ZPopCommand} with {@literal value} applied.
		 */
		public ZPopCommand count(long count) {
			return new ZPopCommand(direction, getKey(), count);
		}

		/**
		 * @return never {@literal null}.
		 */
		public PopDirection getDirection() {
			return direction;
		}

		public long getCount() {
			return count;
		}
	}

	/**
	 * {@code BZPOPMIN}/{@literal BZPOPMAX} command parameters.
	 *
	 * @author Mark Paluch
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/bzpopmin">Redis Documentation: BZPOPMIN</a>
	 * @see <a href="https://redis.io/commands/bzpopmax">Redis Documentation: BZPOPMAX</a>
	 */
	class BZPopCommand extends KeyCommand {

		private final PopDirection direction;

		private final @Nullable TimeUnit timeUnit;
		private final @Nullable Long timeout;

		private final long count;

		private BZPopCommand(@Nullable ByteBuffer key, @Nullable Long timeout, @Nullable TimeUnit timeUnit, long count,
				PopDirection direction) {

			super(key);
			this.count = count;
			this.timeout = timeout;
			this.timeUnit = timeUnit;
			this.direction = direction;
		}

		/**
		 * Creates a new {@link BZPopCommand} for min pop ({@literal ZPOPMIN}).
		 *
		 * @return a new {@link BZPopCommand} for min pop ({@literal ZPOPMIN}).
		 */
		public static BZPopCommand min() {
			return new BZPopCommand(null, null, null, 0, PopDirection.MIN);
		}

		/**
		 * Creates a new {@link BZPopCommand} for max pop ({@literal ZPOPMAX}).
		 *
		 * @return a new {@link BZPopCommand} for max pop ({@literal ZPOPMAX}).
		 */
		public static BZPopCommand max() {
			return new BZPopCommand(null, null, null, 0, PopDirection.MAX);
		}

		/**
		 * Applies the {@literal key}. Constructs a new command instance with all previously configured properties.
		 *
		 * @param key must not be {@literal null}.
		 * @return a new {@link BZPopCommand} with {@literal value} applied.
		 */
		public BZPopCommand from(ByteBuffer key) {

			Assert.notNull(key, "Key must not be null");

			return new BZPopCommand(key, timeout, timeUnit, count, direction);
		}

		/**
		 * Applies the {@literal key}. Constructs a new command instance with all previously configured properties.
		 *
		 * @param count
		 * @return a new {@link BZPopCommand} with {@literal value} applied.
		 */
		public BZPopCommand count(long count) {
			return new BZPopCommand(getKey(), timeout, timeUnit, count, direction);
		}

		/**
		 * Applies a {@link Duration timeout}. Constructs a new command instance with all previously configured properties.
		 *
		 * @param timeout must not be {@literal null}.
		 * @return a new {@link BZPopCommand} with {@link Duration timeout} applied.
		 */
		public BZPopCommand blockingFor(Duration timeout) {

			Assert.notNull(timeout, "Timeout must not be null");

			return blockingFor(timeout.toMillis(), TimeUnit.MILLISECONDS);
		}

		/**
		 * Applies a {@link Duration timeout}. Constructs a new command instance with all previously configured properties.
		 *
		 * @param timeout value.
		 * @param timeUnit must not be {@literal null}.
		 * @return a new {@link BZPopCommand} with {@link Duration timeout} applied.
		 */
		public BZPopCommand blockingFor(long timeout, TimeUnit timeUnit) {

			Assert.notNull(timeUnit, "TimeUnit must not be null");

			return new BZPopCommand(getKey(), timeout, timeUnit, count, direction);
		}

		/**
		 * @return never {@literal null}.
		 */
		public PopDirection getDirection() {
			return direction;
		}

		@Nullable
		public Long getTimeout() {
			return timeout;
		}

		@Nullable
		public TimeUnit getTimeUnit() {
			return timeUnit;
		}

		public long getCount() {
			return count;
		}
	}

	/**
	 * Remove and return the value with its score having the lowest score from sorted set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zpopmin">Redis Documentation: ZPOPMIN</a>
	 */
	default Mono<Tuple> zPopMin(ByteBuffer key) {
		return zPop(Mono.just(ZPopCommand.min().from(key))).map(CommandResponse::getOutput).flatMap(Flux::next).next();
	}

	/**
	 * Remove and return {@code count} values with their score having the lowest score from sorted set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param count number of elements to pop.
	 * @return
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zpopmin">Redis Documentation: ZPOPMIN</a>
	 */
	default Flux<Tuple> zPopMin(ByteBuffer key, long count) {
		return zPop(Mono.just(ZPopCommand.min().from(key).count(count))).map(CommandResponse::getOutput)
				.flatMap(Function.identity());
	}

	/**
	 * Remove and return the value with its score having the lowest score from sorted set at {@code key}. <b>Blocks
	 * connection</b> until element available or {@code timeout} reached.
	 *
	 * @param key must not be {@literal null}.
	 * @param timeout must not be {@literal null}.
	 * @return
	 * @throws IllegalArgumentException if the timeout is {@literal null} or negative.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/bzpopmin">Redis Documentation: BZPOPMIN</a>
	 */
	default Mono<Tuple> bZPopMin(ByteBuffer key, Duration timeout) {

		Assert.notNull(timeout, "Timeout must not be null");
		Assert.isTrue(!timeout.isNegative(), "Timeout must not be negative");

		return bZPop(Mono.just(BZPopCommand.min().from(key).blockingFor(timeout))).map(CommandResponse::getOutput)
				.flatMap(Flux::next).next();
	}

	/**
	 * Remove and return the value with its score having the highest score from sorted set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zpopmax">Redis Documentation: ZPOPMAX</a>
	 */
	default Mono<Tuple> zPopMax(ByteBuffer key) {
		return zPop(Mono.just(ZPopCommand.max().from(key))).map(CommandResponse::getOutput).flatMap(Flux::next).next();
	}

	/**
	 * Remove and return {@code count} values with their score having the highest score from sorted set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param count number of elements to pop.
	 * @return
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zpopmax">Redis Documentation: ZPOPMAX</a>
	 */
	default Flux<Tuple> zPopMax(ByteBuffer key, long count) {
		return zPop(Mono.just(ZPopCommand.max().from(key).count(count))).map(CommandResponse::getOutput)
				.flatMap(Function.identity());
	}

	/**
	 * Remove and return the value with its score having the highest score from sorted set at {@code key}. <b>Blocks
	 * connection</b> until element available or {@code timeout} reached.
	 *
	 * @param key must not be {@literal null}.
	 * @param timeout must not be {@literal null}.
	 * @return
	 * @throws IllegalArgumentException if the timeout is {@literal null} or negative.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/bzpopmax">Redis Documentation: BZPOPMAX</a>
	 */
	default Mono<Tuple> bZPopMax(ByteBuffer key, Duration timeout) {

		Assert.notNull(timeout, "Timeout must not be null");
		Assert.isTrue(!timeout.isNegative(), "Timeout must not be negative");

		return bZPop(Mono.just(BZPopCommand.max().from(key).blockingFor(timeout))).map(CommandResponse::getOutput)
				.flatMap(Flux::next).next();
	}

	/**
	 * Remove and return elements from sorted set at {@link ByteBuffer keyCommand#getKey()}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/zpopmin">Redis Documentation: ZPOPMIN</a>
	 * @see <a href="https://redis.io/commands/zpopmax">Redis Documentation: ZPOPMAX</a>
	 */
	Flux<CommandResponse<ZPopCommand, Flux<Tuple>>> zPop(Publisher<ZPopCommand> commands);

	/**
	 * Remove and return elements from sorted set at {@link ByteBuffer keyCommand#getKey()}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/zpopmin">Redis Documentation: ZPOPMIN</a>
	 * @see <a href="https://redis.io/commands/zpopmax">Redis Documentation: ZPOPMAX</a>
	 */
	Flux<CommandResponse<BZPopCommand, Flux<Tuple>>> bZPop(Publisher<BZPopCommand> commands);

	/**
	 * Get the size of sorted set with {@literal key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/zcard">Redis Documentation: ZCARD</a>
	 */
	default Mono<Long> zCard(ByteBuffer key) {

		Assert.notNull(key, "Key must not be null");

		return zCard(Mono.just(new KeyCommand(key))).next().map(NumericResponse::getOutput);
	}

	/**
	 * Get the size of sorted set with {@linByteBuffer keyCommand#getKey()}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/zcard">Redis Documentation: ZCARD</a>
	 */
	Flux<NumericResponse<KeyCommand, Long>> zCard(Publisher<KeyCommand> commands);

	/**
	 * {@code ZSCORE} command parameters.
	 *
	 * @author Christoph Strobl
	 * @see <a href="https://redis.io/commands/zscore">Redis Documentation: ZSCORE</a>
	 */
	class ZScoreCommand extends KeyCommand {

		private final ByteBuffer value;

		private ZScoreCommand(@Nullable ByteBuffer key, ByteBuffer value) {

			super(key);
			this.value = value;
		}

		/**
		 * Creates a new {@link ZScoreCommand} given a {@link ByteBuffer member}.
		 *
		 * @param member must not be {@literal null}.
		 * @return a new {@link ZScoreCommand} for {@link ByteBuffer member}.
		 */
		public static ZScoreCommand scoreOf(ByteBuffer member) {

			Assert.notNull(member, "Member must not be null");

			return new ZScoreCommand(null, member);
		}

		/**
		 * Applies the {@literal key}. Constructs a new command instance with all previously configured properties.
		 *
		 * @param key must not be {@literal null}.
		 * @return a new {@link ZScoreCommand} with {@literal key} applied.
		 */
		public ZScoreCommand forKey(ByteBuffer key) {

			Assert.notNull(key, "Key must not be null");

			return new ZScoreCommand(key, value);
		}

		/**
		 * @return
		 */
		public ByteBuffer getValue() {
			return value;
		}
	}

	/**
	 * Get the score of element with {@literal value} from sorted set with key {@literal key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/zscore">Redis Documentation: ZSCORE</a>
	 */
	default Mono<Double> zScore(ByteBuffer key, ByteBuffer value) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(value, "Value must not be null");

		return zScore(Mono.just(ZScoreCommand.scoreOf(value).forKey(key))).next().map(NumericResponse::getOutput);
	}

	/**
	 * Get the score of element with {@link ZScoreCommand#getValue()} from sorted set with key
	 * {@link ZScoreCommand#getKey()}
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/zscore">Redis Documentation: ZSCORE</a>
	 */
	Flux<NumericResponse<ZScoreCommand, Double>> zScore(Publisher<ZScoreCommand> commands);

	/**
	 * {@code ZMSCORE} command parameters.
	 *
	 * @author Mark Paluch
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zmscore">Redis Documentation: ZMSCORE</a>
	 */
	class ZMScoreCommand extends KeyCommand {

		private final Collection<ByteBuffer> values;

		private ZMScoreCommand(@Nullable ByteBuffer key, Collection<ByteBuffer> values) {

			super(key);

			this.values = values;
		}

		/**
		 * Creates a new {@link ZMScoreCommand} given a {@link ByteBuffer member}.
		 *
		 * @param member must not be {@literal null}.
		 * @return a new {@link ZMScoreCommand} for {@link ByteBuffer}.
		 */
		public static ZMScoreCommand scoreOf(ByteBuffer member) {

			Assert.notNull(member, "Member must not be null");

			return new ZMScoreCommand(null, Collections.singletonList(member));
		}

		/**
		 * Creates a new {@link ZMScoreCommand} given a {@link List members}.
		 *
		 * @param members must not be {@literal null}.
		 * @return a new {@link ZMScoreCommand} for {@link List} of members.
		 */
		public static ZMScoreCommand scoreOf(Collection<ByteBuffer> members) {

			Assert.notNull(members, "Members must not be null");

			return new ZMScoreCommand(null, members);
		}

		/**
		 * Applies the {@literal key}. Constructs a new command instance with all previously configured properties.
		 *
		 * @param key must not be {@literal null}.
		 * @return a new {@link ZMScoreCommand} with {@literal key} applied.
		 */
		public ZMScoreCommand forKey(ByteBuffer key) {

			Assert.notNull(key, "Key must not be null");

			return new ZMScoreCommand(key, values);
		}

		/**
		 * @return
		 */
		public Collection<ByteBuffer> getValues() {
			return values;
		}
	}

	/**
	 * Get the scores of elements with {@literal values} from sorted set with key {@literal key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param values must not be {@literal null}.
	 * @return
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zmscore">Redis Documentation: ZMSCORE</a>
	 */
	default Mono<List<Double>> zMScore(ByteBuffer key, Collection<ByteBuffer> values) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(values, "Values must not be null");

		return zMScore(Mono.just(ZMScoreCommand.scoreOf(values).forKey(key))).next().map(MultiValueResponse::getOutput);
	}

	/**
	 * Get the scores of elements with {@link ZMScoreCommand#getValues()} from sorted set with key
	 * {@link ZMScoreCommand#getKey()}
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zmscore">Redis Documentation: ZMSCORE</a>
	 */
	Flux<MultiValueResponse<ZMScoreCommand, Double>> zMScore(Publisher<ZMScoreCommand> commands);

	/**
	 * {@code ZREMRANGEBYRANK} command parameters.
	 *
	 * @author Christoph Strobl
	 * @see <a href="https://redis.io/commands/zremrangebyrank">Redis Documentation: ZREMRANGEBYRANK</a>
	 */
	class ZRemRangeByRankCommand extends KeyCommand {

		private final Range<Long> range;

		private ZRemRangeByRankCommand(ByteBuffer key, Range<Long> range) {
			super(key);
			this.range = range;
		}

		/**
		 * Creates a new {@link ZRemRangeByRankCommand} given a {@link Range}.
		 *
		 * @param range must not be {@literal null}.
		 * @return a new {@link ZRemRangeByRankCommand} for {@link Range}.
		 */
		public static ZRemRangeByRankCommand valuesWithin(Range<Long> range) {

			Assert.notNull(range, "Range must not be null");

			return new ZRemRangeByRankCommand(null, range);
		}

		/**
		 * Applies the {@literal key}. Constructs a new command instance with all previously configured properties.
		 *
		 * @param key must not be {@literal null}.
		 * @return a new {@link ZRemRangeByRankCommand} with {@literal key} applied.
		 */
		public ZRemRangeByRankCommand from(ByteBuffer key) {

			Assert.notNull(key, "Key must not be null");

			return new ZRemRangeByRankCommand(key, range);
		}

		/**
		 * @return
		 */
		public Range<Long> getRange() {
			return range;
		}
	}

	/**
	 * Remove elements in {@link Range} from sorted set with {@literal key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/zremrangebyrank">Redis Documentation: ZREMRANGEBYRANK</a>
	 */
	default Mono<Long> zRemRangeByRank(ByteBuffer key, Range<Long> range) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(range, "Range must not be null");

		return zRemRangeByRank(Mono.just(ZRemRangeByRankCommand.valuesWithin(range).from(key))).next()
				.map(NumericResponse::getOutput);
	}

	/**
	 * Remove elements in {@link Range} from sorted set with {@link ZRemRangeByRankCommand#getKey()}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/zremrangebyrank">Redis Documentation: ZREMRANGEBYRANK</a>
	 */
	Flux<NumericResponse<ZRemRangeByRankCommand, Long>> zRemRangeByRank(Publisher<ZRemRangeByRankCommand> commands);

	/**
	 * {@code ZREMRANGEBYSCORE} command parameters.
	 *
	 * @author Christoph Strobl
	 * @see <a href="https://redis.io/commands/zremrangebyscore">Redis Documentation: ZREMRANGEBYSCORE</a>
	 */
	class ZRemRangeByScoreCommand extends KeyCommand {

		private final Range<Double> range;

		private ZRemRangeByScoreCommand(@Nullable ByteBuffer key, Range<Double> range) {

			super(key);
			this.range = range;
		}

		/**
		 * Creates a new {@link ZRemRangeByScoreCommand} given a {@link Range}.
		 *
		 * @param range must not be {@literal null}.
		 * @return a new {@link ZRemRangeByScoreCommand} for {@link Range}.
		 */
		public static ZRemRangeByScoreCommand scoresWithin(Range<Double> range) {
			return new ZRemRangeByScoreCommand(null, range);
		}

		/**
		 * Applies the {@literal key}. Constructs a new command instance with all previously configured properties.
		 *
		 * @param key must not be {@literal null}.
		 * @return a new {@link ZRemRangeByRankCommand} with {@literal key} applied.
		 */
		public ZRemRangeByScoreCommand from(ByteBuffer key) {

			Assert.notNull(key, "Key must not be null");

			return new ZRemRangeByScoreCommand(key, range);
		}

		/**
		 * @return
		 */
		public Range<Double> getRange() {
			return range;
		}
	}

	/**
	 * Remove elements in {@link Range} from sorted set with {@literal key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/zremrangebyscore">Redis Documentation: ZREMRANGEBYSCORE</a>
	 */
	default Mono<Long> zRemRangeByScore(ByteBuffer key, Range<Double> range) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(range, "Range must not be null");

		return zRemRangeByScore(Mono.just(ZRemRangeByScoreCommand.scoresWithin(range).from(key))).next()
				.map(NumericResponse::getOutput);
	}

	/**
	 * Remove elements in {@link Range} from sorted set with {@link ZRemRangeByRankCommand#getKey()}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/zremrangebyscore">Redis Documentation: ZREMRANGEBYSCORE</a>
	 */
	Flux<NumericResponse<ZRemRangeByScoreCommand, Long>> zRemRangeByScore(Publisher<ZRemRangeByScoreCommand> commands);

	/**
	 * {@code ZREMRANGEBYLEX} command parameters.
	 *
	 * @author Christoph Strobl
	 * @since 2.5
	 * @see <a href="https://redis.io/commands/zremrangebylex">Redis Documentation: ZREMRANGEBYLEX</a>
	 */
	class ZRemRangeByLexCommand extends KeyCommand {

		private final Range<String> range;

		private ZRemRangeByLexCommand(@Nullable ByteBuffer key, Range<String> range) {

			super(key);
			this.range = range;
		}

		/**
		 * Creates a new {@link ZRemRangeByLexCommand} given a {@link Range}.
		 *
		 * @param range must not be {@literal null}.
		 * @return a new {@link ZRemRangeByScoreCommand} for {@link Range}.
		 */
		public static ZRemRangeByLexCommand lexWithin(Range<String> range) {
			return new ZRemRangeByLexCommand(null, range);
		}

		/**
		 * Applies the {@literal key}. Constructs a new command instance with all previously configured properties.
		 *
		 * @param key must not be {@literal null}.
		 * @return a new {@link ZRemRangeByLexCommand} with {@literal key} applied.
		 */
		public ZRemRangeByLexCommand from(ByteBuffer key) {

			Assert.notNull(key, "Key must not be null");

			return new ZRemRangeByLexCommand(key, range);
		}

		/**
		 * @return
		 */
		public Range<String> getRange() {
			return range;
		}
	}

	/**
	 * Remove elements in {@link Range} from sorted set with {@literal key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return a {@link Mono} emitting the number of removed elements.
	 * @since 2.5
	 * @see <a href="https://redis.io/commands/zremrangebylex">Redis Documentation: ZREMRANGEBYLEX</a>
	 */
	default Mono<Long> zRemRangeByLex(ByteBuffer key, Range<String> range) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(range, "Range must not be null");

		return zRemRangeByLex(Mono.just(ZRemRangeByLexCommand.lexWithin(range).from(key))).next()
				.map(NumericResponse::getOutput);
	}

	/**
	 * Remove elements in {@link Range} from sorted set with {@link ZRemRangeByLexCommand#getKey()}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 * @since 2.5
	 * @see <a href="https://redis.io/commands/zremrangebylex">Redis Documentation: ZREMRANGEBYLEX</a>
	 */
	Flux<NumericResponse<ZRemRangeByLexCommand, Long>> zRemRangeByLex(Publisher<ZRemRangeByLexCommand> commands);

	/**
	 * {@code ZDIFF} command parameters.
	 *
	 * @author Mark Paluch
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zdiff">Redis Documentation: ZDIFF</a>
	 */
	class ZDiffCommand implements Command {

		private final List<ByteBuffer> keys;

		private ZDiffCommand(List<ByteBuffer> keys) {
			this.keys = keys;
		}

		/**
		 * Creates a new {@link ZDiffCommand} given a {@link Collection} of keys.
		 *
		 * @param keys must not be {@literal null}.
		 * @return a new {@link ZDiffCommand} for a {@link Collection} of values.
		 */
		public static ZDiffCommand sets(Collection<ByteBuffer> keys) {

			Assert.notNull(keys, "Keys must not be null");

			return new ZDiffCommand(new ArrayList<>(keys));
		}

		@Override
		@Nullable
		public ByteBuffer getKey() {
			return null;
		}

		/**
		 * @return
		 */
		public List<ByteBuffer> getKeys() {
			return keys;
		}
	}

	/**
	 * Diff sorted {@literal sets}.
	 *
	 * @param sets must not be {@literal null}.
	 * @return
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zdiff">Redis Documentation: ZDIFF</a>
	 */
	default Flux<ByteBuffer> zDiff(List<ByteBuffer> sets) {
		return zDiff(Mono.just(ZDiffCommand.sets(sets))).flatMap(CommandResponse::getOutput);
	}

	/**
	 * Diff sorted {@literal sets}.
	 *
	 * @param commands
	 * @return
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zdiff">Redis Documentation: ZDIFF</a>
	 */
	Flux<CommandResponse<ZDiffCommand, Flux<ByteBuffer>>> zDiff(Publisher<? extends ZDiffCommand> commands);

	/**
	 * Diff sorted {@literal sets}.
	 *
	 * @param sets must not be {@literal null}.
	 * @return
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zdiff">Redis Documentation: ZDIFF</a>
	 */
	default Flux<Tuple> zDiffWithScores(List<ByteBuffer> sets) {
		return zDiffWithScores(Mono.just(ZDiffCommand.sets(sets))).flatMap(CommandResponse::getOutput);
	}

	/**
	 * Diff sorted {@literal sets}.
	 *
	 * @param commands
	 * @return
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zdiff">Redis Documentation: ZDIFF</a>
	 */
	Flux<CommandResponse<ZDiffCommand, Flux<Tuple>>> zDiffWithScores(Publisher<? extends ZDiffCommand> commands);

	/**
	 * {@code ZDIFFSTORE} command parameters.
	 *
	 * @author Mark Paluch
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zdiffstore">Redis Documentation: ZDIFFSTORE</a>
	 */
	class ZDiffStoreCommand extends KeyCommand {

		private final List<ByteBuffer> sourceKeys;

		private ZDiffStoreCommand(@Nullable ByteBuffer key, List<ByteBuffer> sourceKeys) {

			super(key);

			this.sourceKeys = sourceKeys;
		}

		/**
		 * Creates a new {@link ZDiffStoreCommand} given a {@link Collection} of keys.
		 *
		 * @param keys must not be {@literal null}.
		 * @return a new {@link ZDiffStoreCommand} for a {@link Collection} of values.
		 */
		public static ZDiffStoreCommand sourceKeys(Collection<ByteBuffer> keys) {

			Assert.notNull(keys, "Keys must not be null");

			return new ZDiffStoreCommand(null, new ArrayList<>(keys));
		}

		/**
		 * Applies the {@literal key} at which the result is stored. Constructs a new command instance with all previously
		 * configured properties.
		 *
		 * @param key must not be {@literal null}.
		 * @return a new {@link ZDiffStoreCommand} with {@literal key} applied.
		 */
		public ZDiffStoreCommand storeAs(ByteBuffer key) {

			Assert.notNull(key, "Key must not be null");

			return new ZDiffStoreCommand(key, sourceKeys);
		}

		/**
		 * @return
		 */
		public List<ByteBuffer> getSourceKeys() {
			return sourceKeys;
		}
	}

	/**
	 * Diff sorted {@literal sets} and store result in destination {@literal destinationKey}.
	 *
	 * @param destinationKey must not be {@literal null}.
	 * @param sets must not be {@literal null}.
	 * @return
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zdiffstore">Redis Documentation: ZDIFFSTORE</a>
	 */
	default Mono<Long> zDiffStore(ByteBuffer destinationKey, List<ByteBuffer> sets) {

		Assert.notNull(destinationKey, "DestinationKey must not be null");
		Assert.notNull(sets, "Sets must not be null");

		return zDiffStore(Mono.just(ZDiffStoreCommand.sourceKeys(sets).storeAs(destinationKey))).next()
				.map(NumericResponse::getOutput);
	}

	/**
	 * Diff sorted {@literal sets} and store result in destination {@literal destinationKey}.
	 *
	 * @param commands
	 * @return
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zdiffstore">Redis Documentation: ZDIFFSTORE</a>
	 */
	Flux<NumericResponse<ZDiffStoreCommand, Long>> zDiffStore(Publisher<ZDiffStoreCommand> commands);

	/**
	 * {@code ZINTER}/{@code ZUNION} command parameters.
	 *
	 * @author Christoph Strobl
	 * @author Mark Paluch
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zinter">Redis Documentation: ZINTER</a>
	 * @see <a href="https://redis.io/commands/zunion">Redis Documentation: ZUNION</a>
	 */
	class ZAggregateCommand implements Command {

		private final List<ByteBuffer> sourceKeys;
		private final List<Double> weights;
		private final @Nullable Aggregate aggregateFunction;

		private ZAggregateCommand(List<ByteBuffer> sourceKeys, List<Double> weights, @Nullable Aggregate aggregate) {

			this.sourceKeys = sourceKeys;
			this.weights = weights;
			this.aggregateFunction = aggregate;
		}

		/**
		 * Creates a new {@link ZAggregateCommand} given a {@link List} of keys.
		 *
		 * @param keys must not be {@literal null}.
		 * @return a new {@link ZAggregateCommand} for {@link Range}.
		 */
		public static ZAggregateCommand sets(List<ByteBuffer> keys) {

			Assert.notNull(keys, "Keys must not be null");

			return new ZAggregateCommand(new ArrayList<>(keys), Collections.emptyList(), null);
		}

		/**
		 * Applies the {@link List} of weights. Constructs a new command instance with all previously configured properties.
		 *
		 * @param weights must not be {@literal null}.
		 * @return a new {@link ZAggregateCommand} with {@literal weights} applied.
		 */
		public ZAggregateCommand applyWeights(List<Double> weights) {
			return new ZAggregateCommand(sourceKeys, weights, aggregateFunction);
		}

		/**
		 * Applies the {@link Weights}. Constructs a new command instance with all previously configured properties.
		 *
		 * @param weights must not be {@literal null}.
		 * @return a new {@link ZAggregateCommand} with {@literal weights} applied.
		 * @since 2.1
		 */
		public ZAggregateCommand applyWeights(Weights weights) {
			return applyWeights(weights.toList());
		}

		/**
		 * Applies a specific {@link Aggregate} function. Constructs a new command instance with all previously configured
		 * properties.
		 *
		 * @param aggregateFunction can be {@literal null}.
		 * @return a new {@link ZAggregateStoreCommand} with {@link Aggregate} applied.
		 */
		public ZAggregateCommand aggregateUsing(@Nullable Aggregate aggregateFunction) {

			return new ZAggregateCommand(sourceKeys, weights, aggregateFunction);
		}

		@Nullable
		@Override
		public ByteBuffer getKey() {
			return null;
		}

		/**
		 * @return never {@literal null}.
		 */
		public List<ByteBuffer> getSourceKeys() {
			return sourceKeys;
		}

		/**
		 * @return never {@literal null}.
		 */
		public List<Double> getWeights() {
			return weights;
		}

		/**
		 * @return never {@literal null}.
		 */
		public Optional<Aggregate> getAggregateFunction() {
			return Optional.ofNullable(aggregateFunction);
		}
	}

	/**
	 * {@code ZINTERSTORE}/{@code ZUNIONSTORE} command parameters.
	 *
	 * @author Christoph Strobl
	 * @author Mark Paluch
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zinterstore">Redis Documentation: ZINTERSTORE</a>
	 * @see <a href="https://redis.io/commands/zunionstore">Redis Documentation: ZUNIONSTORE</a>
	 */
	class ZAggregateStoreCommand extends KeyCommand {

		private final List<ByteBuffer> sourceKeys;
		private final List<Double> weights;
		private final @Nullable Aggregate aggregateFunction;

		private ZAggregateStoreCommand(@Nullable ByteBuffer key, List<ByteBuffer> sourceKeys, List<Double> weights,
				@Nullable Aggregate aggregate) {

			super(key);
			this.sourceKeys = sourceKeys;
			this.weights = weights;
			this.aggregateFunction = aggregate;
		}

		/**
		 * Creates a new {@link ZAggregateStoreCommand} given a {@link List} of keys.
		 *
		 * @param keys must not be {@literal null}.
		 * @return a new {@link ZAggregateStoreCommand} for {@link Range}.
		 */
		public static ZAggregateStoreCommand sets(List<ByteBuffer> keys) {

			Assert.notNull(keys, "Keys must not be null");

			return new ZAggregateStoreCommand(null, new ArrayList<>(keys), Collections.emptyList(), null);
		}

		/**
		 * Applies the {@link List} of weights. Constructs a new command instance with all previously configured properties.
		 *
		 * @param weights must not be {@literal null}.
		 * @return a new {@link ZAggregateStoreCommand} with {@literal weights} applied.
		 */
		public ZAggregateStoreCommand applyWeights(List<Double> weights) {
			return new ZAggregateStoreCommand(getKey(), sourceKeys, weights, aggregateFunction);
		}

		/**
		 * Applies the {@link Weights}. Constructs a new command instance with all previously configured properties.
		 *
		 * @param weights must not be {@literal null}.
		 * @return a new {@link ZAggregateStoreCommand} with {@literal weights} applied.
		 * @since 2.1
		 */
		public ZAggregateStoreCommand applyWeights(Weights weights) {
			return applyWeights(weights.toList());
		}

		/**
		 * Applies a specific {@link Aggregate} function. Constructs a new command instance with all previously configured
		 * properties.
		 *
		 * @param aggregateFunction can be {@literal null}.
		 * @return a new {@link ZAggregateStoreCommand} with {@link Aggregate} applied.
		 */
		public ZAggregateStoreCommand aggregateUsing(@Nullable Aggregate aggregateFunction) {

			return new ZAggregateStoreCommand(getKey(), sourceKeys, weights, aggregateFunction);
		}

		/**
		 * Applies the {@literal key} at which the result is stored. Constructs a new command instance with all previously
		 * configured properties.
		 *
		 * @param key must not be {@literal null}.
		 * @return a new {@link ZAggregateStoreCommand} with {@literal key} applied.
		 */
		public ZAggregateStoreCommand storeAs(ByteBuffer key) {

			Assert.notNull(key, "Key must not be null");

			return new ZAggregateStoreCommand(key, sourceKeys, weights, aggregateFunction);
		}

		/**
		 * @return never {@literal null}.
		 */
		public List<ByteBuffer> getSourceKeys() {
			return sourceKeys;
		}

		/**
		 * @return never {@literal null}.
		 */
		public List<Double> getWeights() {
			return weights;
		}

		/**
		 * @return never {@literal null}.
		 */
		public Optional<Aggregate> getAggregateFunction() {
			return Optional.ofNullable(aggregateFunction);
		}
	}

	/**
	 * Intersect sorted {@literal sets}.
	 *
	 * @param sets must not be {@literal null}.
	 * @return
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zinter">Redis Documentation: ZINTER</a>
	 */
	default Flux<ByteBuffer> zInter(List<ByteBuffer> sets) {

		Assert.notNull(sets, "Sets must not be null");

		return zInter(Mono.just(ZAggregateCommand.sets(sets))).flatMap(CommandResponse::getOutput);
	}

	/**
	 * Intersect sorted {@literal sets} by applying {@literal aggregateFunction} and apply weights to individual sets.
	 *
	 * @param commands
	 * @return
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zinter">Redis Documentation: ZINTER</a>
	 */
	Flux<CommandResponse<ZAggregateCommand, Flux<ByteBuffer>>> zInter(Publisher<? extends ZAggregateCommand> commands);

	/**
	 * Intersect sorted {@literal sets}.
	 *
	 * @param sets must not be {@literal null}.
	 * @return
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zinter">Redis Documentation: ZINTER</a>
	 */
	default Flux<Tuple> zInterWithScores(List<ByteBuffer> sets) {
		return zInterWithScores(sets, Collections.emptyList());
	}

	/**
	 * Intersect sorted {@literal sets} and apply weights to individual sets.
	 *
	 * @param sets must not be {@literal null}.
	 * @param weights must not be {@literal null}.
	 * @return
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zinter">Redis Documentation: ZINTER</a>
	 */
	default Flux<Tuple> zInterWithScores(List<ByteBuffer> sets, List<Double> weights) {
		return zInterWithScores(sets, weights, null);
	}

	/**
	 * Intersect sorted {@literal sets} and apply weights to individual sets.
	 *
	 * @param sets must not be {@literal null}.
	 * @param weights must not be {@literal null}.
	 * @return
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zinter">Redis Documentation: ZINTER</a>
	 */
	default Flux<Tuple> zInterWithScores(List<ByteBuffer> sets, Weights weights) {
		return zInterWithScores(sets, weights, null);
	}

	/**
	 * Intersect sorted {@literal sets} by applying {@literal aggregateFunction} and apply weights to individual sets.
	 *
	 * @param sets must not be {@literal null}.
	 * @param weights must not be {@literal null}.
	 * @param aggregateFunction can be {@literal null}.
	 * @return
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zinter">Redis Documentation: ZINTER</a>
	 */
	default Flux<Tuple> zInterWithScores(List<ByteBuffer> sets, List<Double> weights,
			@Nullable Aggregate aggregateFunction) {

		Assert.notNull(sets, "Sets must not be null");

		return zInterWithScores(
				Mono.just(ZAggregateCommand.sets(sets).aggregateUsing(aggregateFunction).applyWeights(weights)))
						.flatMap(CommandResponse::getOutput);
	}

	/**
	 * Intersect sorted {@literal sets} by applying {@literal aggregateFunction} and apply weights to individual sets.
	 *
	 * @param sets must not be {@literal null}.
	 * @param weights must not be {@literal null}.
	 * @param aggregateFunction can be {@literal null}.
	 * @return
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zinter">Redis Documentation: ZINTER</a>
	 */
	default Flux<Tuple> zInterWithScores(List<ByteBuffer> sets, Weights weights, @Nullable Aggregate aggregateFunction) {

		Assert.notNull(sets, "Sets must not be null");

		return zInterWithScores(
				Mono.just(ZAggregateCommand.sets(sets).aggregateUsing(aggregateFunction).applyWeights(weights)))
						.flatMap(CommandResponse::getOutput);
	}

	/**
	 * Intersect sorted {@literal sets} by applying {@literal aggregateFunction} and apply weights to individual sets.
	 *
	 * @param commands
	 * @return
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zinter">Redis Documentation: ZINTER</a>
	 */
	Flux<CommandResponse<ZAggregateCommand, Flux<Tuple>>> zInterWithScores(
			Publisher<? extends ZAggregateCommand> commands);

	/**
	 * {@code ZINTERSTORE} command parameters.
	 *
	 * @author Christoph Strobl
	 * @see <a href="https://redis.io/commands/zinterstore">Redis Documentation: ZINTERSTORE</a>
	 */
	class ZInterStoreCommand extends ZAggregateStoreCommand {

		private ZInterStoreCommand(ByteBuffer key, List<ByteBuffer> sourceKeys, List<Double> weights,
				@Nullable Aggregate aggregate) {
			super(key, sourceKeys, weights, aggregate);
		}

		/**
		 * Creates a new {@link ZInterStoreCommand} given a {@link List} of keys.
		 *
		 * @param keys must not be {@literal null}.
		 * @return a new {@link ZInterStoreCommand} for {@link List} of keys.
		 */
		public static ZInterStoreCommand sets(List<ByteBuffer> keys) {

			Assert.notNull(keys, "Keys must not be null");

			return new ZInterStoreCommand(null, new ArrayList<>(keys), Collections.emptyList(), null);
		}

		/**
		 * Applies the {@link Collection} of weights. Constructs a new command instance with all previously configured
		 * properties.
		 *
		 * @param weights must not be {@literal null}.
		 * @return a new {@link ZInterStoreCommand} with {@literal weights} applied.
		 */
		public ZInterStoreCommand applyWeights(List<Double> weights) {
			return new ZInterStoreCommand(getKey(), getSourceKeys(), weights, getAggregateFunction().orElse(null));
		}

		/**
		 * Applies the {@link Weights}. Constructs a new command instance with all previously configured properties.
		 *
		 * @param weights must not be {@literal null}.
		 * @return a new {@link ZInterStoreCommand} with {@literal weights} applied.
		 * @since 2.1
		 */
		public ZInterStoreCommand applyWeights(Weights weights) {
			return applyWeights(weights.toList());
		}

		/**
		 * Applies a specific {@link Aggregate} function. Constructs a new command instance with all previously configured
		 * properties.
		 *
		 * @param aggregateFunction can be {@literal null}.
		 * @return a new {@link ZInterStoreCommand} with {@link Aggregate} applied.
		 */
		public ZInterStoreCommand aggregateUsing(@Nullable Aggregate aggregateFunction) {

			return new ZInterStoreCommand(getKey(), getSourceKeys(), getWeights(), aggregateFunction);
		}

		/**
		 * Applies the {@literal key} at which the result is stored. Constructs a new command instance with all previously
		 * configured properties.
		 *
		 * @param key must not be {@literal null}.
		 * @return a new {@link ZInterStoreCommand} with {@literal key} applied.
		 */
		public ZInterStoreCommand storeAs(ByteBuffer key) {

			Assert.notNull(key, "Key must not be null");

			return new ZInterStoreCommand(key, getSourceKeys(), getWeights(), getAggregateFunction().orElse(null));
		}

	}

	/**
	 * Intersect sorted {@literal sets} and store result in destination {@literal destinationKey}.
	 *
	 * @param destinationKey must not be {@literal null}.
	 * @param sets must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/zinterstore">Redis Documentation: ZINTERSTORE</a>
	 */
	default Mono<Long> zInterStore(ByteBuffer destinationKey, List<ByteBuffer> sets) {
		return zInterStore(destinationKey, sets, Collections.emptyList());
	}

	/**
	 * Intersect sorted {@literal sets} and store result in destination {@literal destinationKey} and apply weights to
	 * individual sets.
	 *
	 * @param destinationKey must not be {@literal null}.
	 * @param sets must not be {@literal null}.
	 * @param weights must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/zinterstore">Redis Documentation: ZINTERSTORE</a>
	 */
	default Mono<Long> zInterStore(ByteBuffer destinationKey, List<ByteBuffer> sets, List<Double> weights) {
		return zInterStore(destinationKey, sets, weights, null);
	}

	/**
	 * Intersect sorted {@literal sets} and store result in destination {@literal destinationKey} and apply weights to
	 * individual sets.
	 *
	 * @param destinationKey must not be {@literal null}.
	 * @param sets must not be {@literal null}.
	 * @param weights must not be {@literal null}.
	 * @return
	 * @since 2.1
	 * @see <a href="https://redis.io/commands/zinterstore">Redis Documentation: ZINTERSTORE</a>
	 */
	default Mono<Long> zInterStore(ByteBuffer destinationKey, List<ByteBuffer> sets, Weights weights) {
		return zInterStore(destinationKey, sets, weights, null);
	}

	/**
	 * Intersect sorted {@literal sets} by applying {@literal aggregateFunction} and store result in destination
	 * {@literal destinationKey} and apply weights to individual sets.
	 *
	 * @param destinationKey must not be {@literal null}.
	 * @param sets must not be {@literal null}.
	 * @param weights must not be {@literal null}.
	 * @param aggregateFunction can be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/zinterstore">Redis Documentation: ZINTERSTORE</a>
	 */
	default Mono<Long> zInterStore(ByteBuffer destinationKey, List<ByteBuffer> sets, List<Double> weights,
			@Nullable Aggregate aggregateFunction) {

		Assert.notNull(destinationKey, "DestinationKey must not be null");
		Assert.notNull(sets, "Sets must not be null");

		return zInterStore(Mono.just(
				ZInterStoreCommand.sets(sets).aggregateUsing(aggregateFunction).applyWeights(weights).storeAs(destinationKey)))
						.next().map(NumericResponse::getOutput);
	}

	/**
	 * Intersect sorted {@literal sets} by applying {@literal aggregateFunction} and store result in destination
	 * {@literal destinationKey} and apply weights to individual sets.
	 *
	 * @param destinationKey must not be {@literal null}.
	 * @param sets must not be {@literal null}.
	 * @param weights must not be {@literal null}.
	 * @param aggregateFunction can be {@literal null}.
	 * @return
	 * @since 2.1
	 * @see <a href="https://redis.io/commands/zinterstore">Redis Documentation: ZINTERSTORE</a>
	 */
	default Mono<Long> zInterStore(ByteBuffer destinationKey, List<ByteBuffer> sets, Weights weights,
			@Nullable Aggregate aggregateFunction) {

		Assert.notNull(destinationKey, "DestinationKey must not be null");
		Assert.notNull(sets, "Sets must not be null");

		return zInterStore(Mono.just(
				ZInterStoreCommand.sets(sets).aggregateUsing(aggregateFunction).applyWeights(weights).storeAs(destinationKey)))
						.next().map(NumericResponse::getOutput);
	}

	/**
	 * Intersect sorted {@literal sets} by applying {@literal aggregateFunction} and store result in destination
	 * {@literal destinationKey} and apply weights to individual sets.
	 *
	 * @param commands
	 * @return
	 * @see <a href="https://redis.io/commands/zinterstore">Redis Documentation: ZINTERSTORE</a>
	 */
	Flux<NumericResponse<ZAggregateStoreCommand, Long>> zInterStore(Publisher<? extends ZAggregateStoreCommand> commands);

	/**
	 * Union sorted {@literal sets}.
	 *
	 * @param sets must not be {@literal null}.
	 * @return
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zunion">Redis Documentation: ZUNION</a>
	 */
	default Flux<ByteBuffer> zUnion(List<ByteBuffer> sets) {

		Assert.notNull(sets, "Sets must not be null");

		return zUnion(Mono.just(ZAggregateCommand.sets(sets))).flatMap(CommandResponse::getOutput);
	}

	/**
	 * Union sorted {@literal sets}.
	 *
	 * @param sets must not be {@literal null}.
	 * @return
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zunion">Redis Documentation: ZUNION</a>
	 */
	default Flux<Tuple> zUnionWithScores(List<ByteBuffer> sets) {
		return zUnionWithScores(sets, Collections.emptyList());
	}

	/**
	 * Union sorted {@literal sets} and apply weights to individual sets.
	 *
	 * @param sets must not be {@literal null}.
	 * @param weights must not be {@literal null}.
	 * @return
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zunion">Redis Documentation: ZUNION</a>
	 */
	default Flux<Tuple> zUnionWithScores(List<ByteBuffer> sets, List<Double> weights) {
		return zUnionWithScores(sets, weights, null);
	}

	/**
	 * Union sorted {@literal sets} and apply weights to individual sets.
	 *
	 * @param sets must not be {@literal null}.
	 * @param weights must not be {@literal null}.
	 * @return
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zunion">Redis Documentation: ZUNION</a>
	 */
	default Flux<Tuple> zUnionWithScores(List<ByteBuffer> sets, Weights weights) {
		return zUnionWithScores(sets, weights, null);
	}

	/**
	 * Union sorted {@literal sets} by applying {@literal aggregateFunction} and apply weights to individual sets.
	 *
	 * @param sets must not be {@literal null}.
	 * @param weights can be {@literal null}.
	 * @param aggregateFunction can be {@literal null}.
	 * @return
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zunion">Redis Documentation: ZUNION</a>
	 */
	default Flux<Tuple> zUnionWithScores(List<ByteBuffer> sets, List<Double> weights,
			@Nullable Aggregate aggregateFunction) {

		Assert.notNull(sets, "Sets must not be null");

		return zUnionWithScores(
				Mono.just(ZAggregateCommand.sets(sets).aggregateUsing(aggregateFunction).applyWeights(weights)))
						.flatMap(CommandResponse::getOutput);
	}

	/**
	 * Union sorted {@literal sets} by applying {@literal aggregateFunction} and apply weights to individual sets.
	 *
	 * @param sets must not be {@literal null}.
	 * @param weights can be {@literal null}.
	 * @param aggregateFunction can be {@literal null}.
	 * @return
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zunion">Redis Documentation: ZUNION</a>
	 */
	default Flux<Tuple> zUnionWithScores(List<ByteBuffer> sets, Weights weights, @Nullable Aggregate aggregateFunction) {

		Assert.notNull(sets, "Sets must not be null");

		return zUnionWithScores(
				Mono.just(ZAggregateCommand.sets(sets).aggregateUsing(aggregateFunction).applyWeights(weights)))
						.flatMap(CommandResponse::getOutput);
	}

	/**
	 * Union sorted {@literal sets} by applying {@literal aggregateFunction} and apply weights to individual sets.
	 *
	 * @param commands
	 * @return
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zunion">Redis Documentation: ZUNION</a>
	 */
	Flux<CommandResponse<ZAggregateCommand, Flux<Tuple>>> zUnionWithScores(
			Publisher<? extends ZAggregateCommand> commands);

	/**
	 * Union sorted {@literal sets} by applying {@literal aggregateFunction} and apply weights to individual sets.
	 *
	 * @param commands
	 * @return
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zunion">Redis Documentation: ZUNION</a>
	 */
	Flux<CommandResponse<ZAggregateCommand, Flux<ByteBuffer>>> zUnion(Publisher<? extends ZAggregateCommand> commands);

	/**
	 * {@code ZUNIONSTORE} command parameters.
	 *
	 * @author Christoph Strobl
	 * @see <a href="https://redis.io/commands/zunionstore">Redis Documentation: ZUNIONSTORE</a>
	 */
	class ZUnionStoreCommand extends ZAggregateStoreCommand {

		private ZUnionStoreCommand(@Nullable ByteBuffer key, List<ByteBuffer> sourceKeys, List<Double> weights,
				@Nullable Aggregate aggregate) {

			super(key, sourceKeys, weights, aggregate);
		}

		/**
		 * Creates a new {@link ZUnionStoreCommand} given a {@link List} of keys.
		 *
		 * @param keys must not be {@literal null}.
		 * @return a new {@link ZUnionStoreCommand} for {@link Range}.
		 */
		public static ZUnionStoreCommand sets(List<ByteBuffer> keys) {

			Assert.notNull(keys, "Keys must not be null");

			return new ZUnionStoreCommand(null, new ArrayList<>(keys), Collections.emptyList(), null);
		}

		/**
		 * Applies the {@link List} of weights. Constructs a new command instance with all previously configured properties.
		 *
		 * @param weights must not be {@literal null}.
		 * @return a new {@link ZUnionStoreCommand} with {@literal weights} applied.
		 */
		public ZUnionStoreCommand applyWeights(List<Double> weights) {
			return new ZUnionStoreCommand(getKey(), getSourceKeys(), weights, getAggregateFunction().orElse(null));
		}

		/**
		 * Applies the {@link Weights}. Constructs a new command instance with all previously configured properties.
		 *
		 * @param weights must not be {@literal null}.
		 * @return a new {@link ZUnionStoreCommand} with {@literal weights} applied.
		 * @since 2.1
		 */
		public ZUnionStoreCommand applyWeights(Weights weights) {
			return applyWeights(weights.toList());
		}

		/**
		 * Applies a specific {@link Aggregate} function. Constructs a new command instance with all previously configured
		 * properties.
		 *
		 * @param aggregateFunction can be {@literal null}.
		 * @return a new {@link ZUnionStoreCommand} with {@link Aggregate} applied.
		 */
		public ZUnionStoreCommand aggregateUsing(@Nullable Aggregate aggregateFunction) {

			return new ZUnionStoreCommand(getKey(), getSourceKeys(), getWeights(), aggregateFunction);
		}

		/**
		 * Applies the {@literal key} at which the result is stored. Constructs a new command instance with all previously
		 * configured properties.
		 *
		 * @param key must not be {@literal null}.
		 * @return a new {@link ZUnionStoreCommand} with {@literal key} applied.
		 */
		public ZUnionStoreCommand storeAs(ByteBuffer key) {

			Assert.notNull(key, "Key must not be null");

			return new ZUnionStoreCommand(key, getSourceKeys(), getWeights(), getAggregateFunction().orElse(null));
		}

	}

	/**
	 * Union sorted {@literal sets} and store result in destination {@literal destinationKey}.
	 *
	 * @param destinationKey must not be {@literal null}.
	 * @param sets must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/zunionstore">Redis Documentation: ZUNIONSTORE</a>
	 */
	default Mono<Long> zUnionStore(ByteBuffer destinationKey, List<ByteBuffer> sets) {
		return zUnionStore(destinationKey, sets, Collections.emptyList());
	}

	/**
	 * Union sorted {@literal sets} and store result in destination {@literal destinationKey} and apply weights to
	 * individual sets.
	 *
	 * @param destinationKey must not be {@literal null}.
	 * @param sets must not be {@literal null}.
	 * @param weights must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/zunionstore">Redis Documentation: ZUNIONSTORE</a>
	 */
	default Mono<Long> zUnionStore(ByteBuffer destinationKey, List<ByteBuffer> sets, List<Double> weights) {
		return zUnionStore(destinationKey, sets, weights, null);
	}

	/**
	 * Union sorted {@literal sets} and store result in destination {@literal destinationKey} and apply weights to
	 * individual sets.
	 *
	 * @param destinationKey must not be {@literal null}.
	 * @param sets must not be {@literal null}.
	 * @param weights must not be {@literal null}.
	 * @return
	 * @since 2.1
	 * @see <a href="https://redis.io/commands/zunionstore">Redis Documentation: ZUNIONSTORE</a>
	 */
	default Mono<Long> zUnionStore(ByteBuffer destinationKey, List<ByteBuffer> sets, Weights weights) {
		return zUnionStore(destinationKey, sets, weights, null);
	}

	/**
	 * Union sorted {@literal sets} by applying {@literal aggregateFunction} and store result in destination
	 * {@literal destinationKey} and apply weights to individual sets.
	 *
	 * @param destinationKey must not be {@literal null}.
	 * @param sets must not be {@literal null}.
	 * @param weights can be {@literal null}.
	 * @param aggregateFunction can be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/zunionstore">Redis Documentation: ZUNIONSTORE</a>
	 */
	default Mono<Long> zUnionStore(ByteBuffer destinationKey, List<ByteBuffer> sets, List<Double> weights,
			@Nullable Aggregate aggregateFunction) {

		Assert.notNull(destinationKey, "DestinationKey must not be null");
		Assert.notNull(sets, "Sets must not be null");

		return zUnionStore(Mono.just(ZAggregateStoreCommand.sets(sets).aggregateUsing(aggregateFunction)
				.applyWeights(weights).storeAs(destinationKey))).next().map(NumericResponse::getOutput);
	}

	/**
	 * Union sorted {@literal sets} by applying {@literal aggregateFunction} and store result in destination
	 * {@literal destinationKey} and apply weights to individual sets.
	 *
	 * @param destinationKey must not be {@literal null}.
	 * @param sets must not be {@literal null}.
	 * @param weights can be {@literal null}.
	 * @param aggregateFunction can be {@literal null}.
	 * @return
	 * @since 2.1
	 * @see <a href="https://redis.io/commands/zunionstore">Redis Documentation: ZUNIONSTORE</a>
	 */
	default Mono<Long> zUnionStore(ByteBuffer destinationKey, List<ByteBuffer> sets, Weights weights,
			@Nullable Aggregate aggregateFunction) {

		Assert.notNull(destinationKey, "DestinationKey must not be null");
		Assert.notNull(sets, "Sets must not be null");

		return zUnionStore(Mono.just(
				ZUnionStoreCommand.sets(sets).aggregateUsing(aggregateFunction).applyWeights(weights).storeAs(destinationKey)))
						.next().map(NumericResponse::getOutput);
	}

	/**
	 * Union sorted {@literal sets} by applying {@literal aggregateFunction} and store result in destination
	 * {@literal destinationKey} and apply weights to individual sets.
	 *
	 * @param commands
	 * @return
	 * @see <a href="https://redis.io/commands/zunionstore">Redis Documentation: ZUNIONSTORE</a>
	 */
	Flux<NumericResponse<ZAggregateStoreCommand, Long>> zUnionStore(Publisher<? extends ZAggregateStoreCommand> commands);

	/**
	 * {@code ZRANGEBYLEX}/{@literal ZREVRANGEBYLEX} command parameters.
	 *
	 * @author Christoph Strobl
	 * @see <a href="https://redis.io/commands/zrangebylex">Redis Documentation: ZRANGEBYLEX</a>
	 * @see <a href="https://redis.io/commands/zrevrangebylex">Redis Documentation: ZREVRANGEBYLEX</a>
	 */
	class ZRangeByLexCommand extends KeyCommand {

		private final Range<String> range;
		private final Direction direction;
		private final Limit limit;

		private ZRangeByLexCommand(@Nullable ByteBuffer key, Range<String> range, Direction direction, Limit limit) {

			super(key);
			this.range = range;
			this.direction = direction;
			this.limit = limit;
		}

		/**
		 * Creates a new {@link ZRangeByLexCommand} given a {@link Range} of {@link String} to retrieve elements
		 * lexicographical ordering.
		 *
		 * @param range must not be {@literal null}.
		 * @return a new {@link ZRangeByLexCommand} for {@link Tuple}.
		 */
		public static ZRangeByLexCommand stringsWithin(Range<String> range) {

			Assert.notNull(range, "Range must not be null");

			return new ZRangeByLexCommand(null, range, Direction.ASC, Limit.unlimited());
		}

		/**
		 * Creates a new {@link ZRangeByLexCommand} given a {@link Range} of {@link String} to obtain elements in reverse
		 * lexicographical ordering.
		 *
		 * @param range must not be {@literal null}.
		 * @return a new {@link ZRangeByLexCommand} for {@link Tuple}.
		 */
		public static ZRangeByLexCommand reverseStringsWithin(Range<String> range) {

			Assert.notNull(range, "Range must not be null");

			return new ZRangeByLexCommand(null, range, Direction.DESC, Limit.unlimited());
		}

		/**
		 * Applies the {@literal key}. Constructs a new command instance with all previously configured properties.
		 *
		 * @param key must not be {@literal null}.
		 * @return a new {@link ZRangeByLexCommand} with {@literal key} applied.
		 */
		public ZRangeByLexCommand from(ByteBuffer key) {

			Assert.notNull(key, "Key must not be null");

			return new ZRangeByLexCommand(key, range, direction, limit);
		}

		/**
		 * Applies the {@link Limit}. Constructs a new command instance with all previously configured properties.
		 *
		 * @param limit must not be {@literal null}.
		 * @return a new {@link ZRangeByLexCommand} with {@link Limit} applied.
		 */
		public ZRangeByLexCommand limitTo(Limit limit) {

			Assert.notNull(limit, "Limit must not be null");
			return new ZRangeByLexCommand(getKey(), range, direction, limit);
		}

		/**
		 * @return
		 */
		public Range<String> getRange() {
			return range;
		}

		/**
		 * @return
		 */
		public Limit getLimit() {
			return limit;
		}

		/**
		 * @return
		 */
		public Direction getDirection() {
			return direction;
		}
	}

	/**
	 * Get all elements in {@link Range} from the sorted set at {@literal key} in lexicographical ordering.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/zrangebylex">Redis Documentation: ZRANGEBYLEX</a>
	 */
	default Flux<ByteBuffer> zRangeByLex(ByteBuffer key, Range<String> range) {
		return zRangeByLex(key, range, Limit.unlimited());
	}

	/**
	 * Get all elements in {@link Range} from the sorted set at {@literal key} in lexicographical ordering. Result is
	 * limited via {@link Limit}.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @param limit can be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/zrangebylex">Redis Documentation: ZRANGEBYLEX</a>
	 */
	default Flux<ByteBuffer> zRangeByLex(ByteBuffer key, Range<String> range, Limit limit) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(range, "Range must not be null");

		return zRangeByLex(Mono.just(ZRangeByLexCommand.stringsWithin(range).from(key).limitTo(limit)))
				.flatMap(CommandResponse::getOutput);
	}

	/**
	 * Get all elements in {@link Range} from the sorted set at {@literal key} in lexicographical ordering.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/zrevrangebylex">Redis Documentation: ZREVRANGEBYLEX</a>
	 */
	default Flux<ByteBuffer> zRevRangeByLex(ByteBuffer key, Range<String> range) {
		return zRevRangeByLex(key, range, Limit.unlimited());
	}

	/**
	 * Get all elements in {@link Range} from the sorted set at {@literal key} in lexicographical ordering. Result is
	 * limited via {@link Limit}.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @param limit must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/zrevrangebylex">Redis Documentation: ZREVRANGEBYLEX</a>
	 */
	default Flux<ByteBuffer> zRevRangeByLex(ByteBuffer key, Range<String> range, Limit limit) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(range, "Range must not be null");
		Assert.notNull(limit, "Limit must not be null");

		return zRangeByLex(Mono.just(ZRangeByLexCommand.reverseStringsWithin(range).from(key).limitTo(limit)))
				.flatMap(CommandResponse::getOutput);
	}

	/**
	 * Get all elements in {@link Range} from the sorted set at {@literal key} in lexicographical ordering. Result is
	 * limited via {@link Limit} and sorted by {@link ZRangeByLexCommand#getDirection()}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/zrangebylex">Redis Documentation: ZRANGEBYLEX</a>
	 * @see <a href="https://redis.io/commands/zrevrangebylex">Redis Documentation: ZREVRANGEBYLEX</a>
	 */
	Flux<CommandResponse<ZRangeByLexCommand, Flux<ByteBuffer>>> zRangeByLex(Publisher<ZRangeByLexCommand> commands);

}
