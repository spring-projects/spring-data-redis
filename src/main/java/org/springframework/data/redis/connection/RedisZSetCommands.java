/*
 * Copyright 2011-2018 the original author or authors.
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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.DoubleUnaryOperator;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;

import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * ZSet(SortedSet)-specific commands supported by Redis.
 *
 * @author Costin Leau
 * @author Christoph Strobl
 * @author Thomas Darimont
 * @author David Liu
 * @author Mark Paluch
 */
public interface RedisZSetCommands {

	/**
	 * Sort aggregation operations.
	 */
	enum Aggregate {
		SUM, MIN, MAX;
	}

	/**
	 * Value object encapsulating a multiplication factor for each input sorted set. This means that the score of every
	 * element in every input sorted set is multiplied by this factor before being passed to the aggregation function.
	 *
	 * @author Mark Paluch
	 * @author Christoph Strobl
	 * @since 2.1
	 */
	class Weights {

		private final List<Double> weights;

		private Weights(List<Double> weights) {
			this.weights = weights;
		}

		/**
		 * Create new {@link Weights} given {@code weights} as {@code int}.
		 *
		 * @param weights must not be {@literal null}.
		 * @return the {@link Weights} for {@code weights}.
		 */
		public static Weights of(int... weights) {

			Assert.notNull(weights, "Weights must not be null!");
			return new Weights(Arrays.stream(weights).mapToDouble(value -> value).boxed().collect(Collectors.toList()));
		}

		/**
		 * Create new {@link Weights} given {@code weights} as {@code double}.
		 *
		 * @param weights must not be {@literal null}.
		 * @return the {@link Weights} for {@code weights}.
		 */
		public static Weights of(double... weights) {

			Assert.notNull(weights, "Weights must not be null!");

			return new Weights(DoubleStream.of(weights).boxed().collect(Collectors.toList()));
		}

		/**
		 * Creates equal {@link Weights} for a number of input sets {@code count} with a weight of one.
		 *
		 * @param count number of input sets. Must be greater or equal to zero.
		 * @return equal {@link Weights} for a number of input sets with a weight of one.
		 */
		public static Weights fromSetCount(int count) {

			Assert.isTrue(count >= 0, "Count of input sorted sets must be greater or equal to zero!");

			return new Weights(IntStream.range(0, count).mapToDouble(value -> 1).boxed().collect(Collectors.toList()));
		}

		/**
		 * Creates a new {@link Weights} object that contains all weights multiplied by {@code multiplier}
		 *
		 * @param multiplier multiplier used to multiply each weight with.
		 * @return equal {@link Weights} for a number of input sets with a weight of one.
		 */
		public Weights multiply(int multiplier) {
			return apply(it -> it * multiplier);
		}

		/**
		 * Creates a new {@link Weights} object that contains all weights multiplied by {@code multiplier}
		 *
		 * @param multiplier multiplier used to multiply each weight with.
		 * @return equal {@link Weights} for a number of input sets with a weight of one.
		 */
		public Weights multiply(double multiplier) {
			return apply(it -> it * multiplier);
		}

		/**
		 * Creates a new {@link Weights} object that contains all weights with {@link Function} applied.
		 *
		 * @param operator operator function.
		 * @return the new {@link Weights} with {@link DoubleUnaryOperator} applied.
		 */
		public Weights apply(Function<Double, Double> operator) {
			return new Weights(weights.stream().map(operator).collect(Collectors.toList()));
		}

		/**
		 * Retrieve the weight at {@code index}.
		 *
		 * @param index the weight index.
		 * @return the weight at {@code index}.
		 * @throws IndexOutOfBoundsException if the index is out of range
		 */
		public double getWeight(int index) {
			return weights.get(index);
		}

		/**
		 * @return number of weights.
		 */
		public int size() {
			return weights.size();
		}

		/**
		 * @return an array containing all of the weights in this list in proper sequence (from first to last element).
		 */
		public double[] toArray() {
			return weights.stream().mapToDouble(Double::doubleValue).toArray();
		}

		/**
		 * @return a {@link List} containing all of the weights in this list in proper sequence (from first to last
		 *         element).
		 */
		public List<Double> toList() {
			return Collections.unmodifiableList(weights);
		}
	}

	/**
	 * ZSet tuple.
	 */
	interface Tuple extends Comparable<Double> {

		/**
		 * @return the raw value of the member.
		 */
		byte[] getValue();

		/**
		 * @return the member score value used for sorting.
		 */
		Double getScore();
	}

	/**
	 * {@link Range} defines {@literal min} and {@literal max} values to retrieve from a {@literal ZSET}.
	 *
	 * @author Christoph Strobl
	 * @since 1.6
	 */
	class Range {

		@Nullable Boundary min;
		@Nullable Boundary max;

		/**
		 * @return new {@link Range}
		 */
		public static Range range() {
			return new Range();
		}

		/**
		 * @return new {@link Range} with {@literal min} and {@literal max} set to {@link Boundary#infinite()}.
		 */
		public static Range unbounded() {

			Range range = new Range();
			range.min = Boundary.infinite();
			range.max = Boundary.infinite();
			return range;
		}

		/**
		 * Greater Than Equals
		 *
		 * @param min must not be {@literal null}.
		 * @return this.
		 */
		public Range gte(Object min) {

			Assert.notNull(min, "Min already set for range.");
			this.min = new Boundary(min, true);
			return this;
		}

		/**
		 * Greater Than
		 *
		 * @param min must not be {@literal null}.
		 * @return this.
		 */
		public Range gt(Object min) {

			Assert.notNull(min, "Min already set for range.");
			this.min = new Boundary(min, false);
			return this;
		}

		/**
		 * Less Then Equals
		 *
		 * @param max must not be {@literal null}.
		 * @return this.
		 */
		public Range lte(Object max) {

			Assert.notNull(max, "Max already set for range.");
			this.max = new Boundary(max, true);
			return this;
		}

		/**
		 * Less Than
		 *
		 * @param max must not be {@literal null}.
		 * @return this.
		 */
		public Range lt(Object max) {

			Assert.notNull(max, "Max already set for range.");
			this.max = new Boundary(max, false);
			return this;
		}

		/**
		 * @return {@literal null} if not set.
		 */
		@Nullable
		public Boundary getMin() {
			return min;
		}

		/**
		 * @return {@literal null} if not set.
		 */
		@Nullable
		public Boundary getMax() {
			return max;
		}

		/**
		 * @author Christoph Strobl
		 * @since 1.6
		 */
		public static class Boundary {

			@Nullable Object value;
			boolean including;

			static Boundary infinite() {
				return new Boundary(null, true);
			}

			Boundary(@Nullable Object value, boolean including) {
				this.value = value;
				this.including = including;
			}

			@Nullable
			public Object getValue() {
				return value;
			}

			public boolean isIncluding() {
				return including;
			}
		}

	}

	/**
	 * @author Christoph Strobl
	 * @since 1.6
	 */
	class Limit {

		private static final Limit UNLIMITED = new Limit() {

			@Override
			public int getCount() {
				return -1;
			}

			@Override
			public int getOffset() {
				return super.getOffset();
			}
		};

		int offset;
		int count;

		public static Limit limit() {
			return new Limit();
		}

		public Limit offset(int offset) {
			this.offset = offset;
			return this;
		}

		public Limit count(int count) {
			this.count = count;
			return this;
		}

		public int getCount() {
			return count;
		}

		public int getOffset() {
			return offset;
		}

		public boolean isUnlimited() {
			return this.equals(UNLIMITED);
		}

		/**
		 * @return new {@link Limit} indicating no limit;
		 * @since 1.3
		 */
		public static Limit unlimited() {
			return UNLIMITED;
		}
	}

	/**
	 * Add {@code value} to a sorted set at {@code key}, or update its {@code score} if it already exists.
	 *
	 * @param key must not be {@literal null}.
	 * @param score the score.
	 * @param value the value.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/zadd">Redis Documentation: ZADD</a>
	 */
	@Nullable
	Boolean zAdd(byte[] key, double score, byte[] value);

	/**
	 * Add {@code tuples} to a sorted set at {@code key}, or update its {@code score} if it already exists.
	 *
	 * @param key must not be {@literal null}.
	 * @param tuples must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/zadd">Redis Documentation: ZADD</a>
	 */
	@Nullable
	Long zAdd(byte[] key, Set<Tuple> tuples);

	/**
	 * Remove {@code values} from sorted set. Return number of removed elements.
	 *
	 * @param key must not be {@literal null}.
	 * @param values must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/zrem">Redis Documentation: ZREM</a>
	 */
	@Nullable
	Long zRem(byte[] key, byte[]... values);

	/**
	 * Increment the score of element with {@code value} in sorted set by {@code increment}.
	 *
	 * @param key must not be {@literal null}.
	 * @param increment
	 * @param value the value.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/zincrby">Redis Documentation: ZINCRBY</a>
	 */
	@Nullable
	Double zIncrBy(byte[] key, double increment, byte[] value);

	/**
	 * Determine the index of element with {@code value} in a sorted set.
	 *
	 * @param key must not be {@literal null}.
	 * @param value the value. Must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/zrank">Redis Documentation: ZRANK</a>
	 */
	@Nullable
	Long zRank(byte[] key, byte[] value);

	/**
	 * Determine the index of element with {@code value} in a sorted set when scored high to low.
	 *
	 * @param key must not be {@literal null}.
	 * @param value the value.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/zrevrank">Redis Documentation: ZREVRANK</a>
	 */
	@Nullable
	Long zRevRank(byte[] key, byte[] value);

	/**
	 * Get elements between {@code start} and {@code end} from sorted set.
	 *
	 * @param key must not be {@literal null}.
	 * @param start
	 * @param end
	 * @return empty {@link Set} when key does not exists or no members in range. {@literal null} when used in pipeline /
	 *         transaction.
	 * @see <a href="http://redis.io/commands/zrange">Redis Documentation: ZRANGE</a>
	 */
	@Nullable
	Set<byte[]> zRange(byte[] key, long start, long end);

	/**
	 * Get set of {@link Tuple}s between {@code start} and {@code end} from sorted set.
	 *
	 * @param key must not be {@literal null}.
	 * @param start
	 * @param end
	 * @return empty {@link Set} when key does not exists or no members in range. {@literal null} when used in pipeline /
	 *         transaction.
	 * @see <a href="http://redis.io/commands/zrange">Redis Documentation: ZRANGE</a>
	 */
	@Nullable
	Set<Tuple> zRangeWithScores(byte[] key, long start, long end);

	/**
	 * Get elements where score is between {@code min} and {@code max} from sorted set.
	 *
	 * @param key must not be {@literal null}.
	 * @param min
	 * @param max
	 * @return empty {@link Set} when key does not exists or no members in range. {@literal null} when used in pipeline /
	 *         transaction.
	 * @see <a href="http://redis.io/commands/zrangebyscore">Redis Documentation: ZRANGEBYSCORE</a>
	 */
	@Nullable
	default Set<byte[]> zRangeByScore(byte[] key, double min, double max) {
		return zRangeByScore(key, new Range().gte(min).lte(max));
	}

	/**
	 * Get set of {@link Tuple}s where score is between {@code Range#min} and {@code Range#max} from sorted set.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return empty {@link Set} when key does not exists or no members in range. {@literal null} when used in pipeline /
	 *         transaction.
	 * @since 1.6
	 * @see <a href="http://redis.io/commands/zrangebyscore">Redis Documentation: ZRANGEBYSCORE</a>
	 */
	@Nullable
	default Set<Tuple> zRangeByScoreWithScores(byte[] key, Range range) {
		return zRangeByScoreWithScores(key, range, Limit.unlimited());
	}

	/**
	 * Get set of {@link Tuple}s where score is between {@code min} and {@code max} from sorted set.
	 *
	 * @param key must not be {@literal null}.
	 * @param min
	 * @param max
	 * @return empty {@link Set} when key does not exists or no members in range. {@literal null} when used in pipeline /
	 *         transaction.
	 * @see <a href="http://redis.io/commands/zrangebyscore">Redis Documentation: ZRANGEBYSCORE</a>
	 */
	@Nullable
	default Set<Tuple> zRangeByScoreWithScores(byte[] key, double min, double max) {
		return zRangeByScoreWithScores(key, new Range().gte(min).lte(max));
	}

	/**
	 * Get elements in range from {@code start} to {@code end} where score is between {@code min} and {@code max} from
	 * sorted set.
	 *
	 * @param key must not be {@literal null}.
	 * @param min
	 * @param max
	 * @param offset
	 * @param count
	 * @return empty {@link Set} when key does not exists or no members in range. {@literal null} when used in pipeline /
	 *         transaction.
	 * @see <a href="http://redis.io/commands/zrangebyscore">Redis Documentation: ZRANGEBYSCORE</a>
	 */
	@Nullable
	default Set<byte[]> zRangeByScore(byte[] key, double min, double max, long offset, long count) {
		return zRangeByScore(key, new Range().gte(min).lte(max),
				new Limit().offset(Long.valueOf(offset).intValue()).count(Long.valueOf(count).intValue()));
	}

	/**
	 * Get set of {@link Tuple}s in range from {@code start} to {@code end} where score is between {@code min} and
	 * {@code max} from sorted set.
	 *
	 * @param key must not be {@literal null}.
	 * @param min
	 * @param max
	 * @param offset
	 * @param count
	 * @return empty {@link Set} when key does not exists or no members in range. {@literal null} when used in pipeline /
	 *         transaction.
	 * @see <a href="http://redis.io/commands/zrangebyscore">Redis Documentation: ZRANGEBYSCORE</a>
	 */
	@Nullable
	default Set<Tuple> zRangeByScoreWithScores(byte[] key, double min, double max, long offset, long count) {
		return zRangeByScoreWithScores(key, new Range().gte(min).lte(max),
				new Limit().offset(Long.valueOf(offset).intValue()).count(Long.valueOf(count).intValue()));
	}

	/**
	 * Get set of {@link Tuple}s in range from {@code Limit#offset} to {@code Limit#offset + Limit#count} where score is
	 * between {@code Range#min} and {@code Range#max} from sorted set.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @param limit must not be {@literal null}.
	 * @return empty {@link Set} when key does not exists or no members in range. {@literal null} when used in pipeline /
	 *         transaction.
	 * @since 1.6
	 * @see <a href="http://redis.io/commands/zrangebyscore">Redis Documentation: ZRANGEBYSCORE</a>
	 */
	@Nullable
	Set<Tuple> zRangeByScoreWithScores(byte[] key, Range range, Limit limit);

	/**
	 * Get elements in range from {@code start} to {@code end} from sorted set ordered from high to low.
	 *
	 * @param key must not be {@literal null}.
	 * @param start
	 * @param end
	 * @return empty {@link Set} when key does not exists or no members in range. {@literal null} when used in pipeline /
	 *         transaction.
	 * @see <a href="http://redis.io/commands/zrevrange">Redis Documentation: ZREVRANGE</a>
	 */
	@Nullable
	Set<byte[]> zRevRange(byte[] key, long start, long end);

	/**
	 * Get set of {@link Tuple}s in range from {@code start} to {@code end} from sorted set ordered from high to low.
	 *
	 * @param key must not be {@literal null}.
	 * @param start
	 * @param end
	 * @return empty {@link Set} when key does not exists or no members in range. {@literal null} when used in pipeline /
	 *         transaction.
	 * @see <a href="http://redis.io/commands/zrevrange">Redis Documentation: ZREVRANGE</a>
	 */
	@Nullable
	Set<Tuple> zRevRangeWithScores(byte[] key, long start, long end);

	/**
	 * Get elements where score is between {@code min} and {@code max} from sorted set ordered from high to low.
	 *
	 * @param key must not be {@literal null}.
	 * @param min
	 * @param max
	 * @return empty {@link Set} when key does not exists or no members in range. {@literal null} when used in pipeline /
	 *         transaction.
	 * @see <a href="http://redis.io/commands/zrevrange">Redis Documentation: ZREVRANGE</a>
	 */
	@Nullable
	default Set<byte[]> zRevRangeByScore(byte[] key, double min, double max) {
		return zRevRangeByScore(key, new Range().gte(min).lte(max));
	}

	/**
	 * Get elements where score is between {@code Range#min} and {@code Range#max} from sorted set ordered from high to
	 * low.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return empty {@link Set} when key does not exists or no members in range. {@literal null} when used in pipeline /
	 *         transaction.
	 * @since 1.6
	 * @see <a href="http://redis.io/commands/zrevrangebyscore">Redis Documentation: ZREVRANGEBYSCORE</a>
	 */
	@Nullable
	default Set<byte[]> zRevRangeByScore(byte[] key, Range range) {
		return zRevRangeByScore(key, range, Limit.unlimited());
	}

	/**
	 * Get set of {@link Tuple} where score is between {@code min} and {@code max} from sorted set ordered from high to
	 * low.
	 *
	 * @param key must not be {@literal null}.
	 * @param min
	 * @param max
	 * @return empty {@link Set} when key does not exists or no members in range. {@literal null} when used in pipeline /
	 *         transaction.
	 * @see <a href="http://redis.io/commands/zrevrangebyscore">Redis Documentation: ZREVRANGEBYSCORE</a>
	 */
	@Nullable
	default Set<Tuple> zRevRangeByScoreWithScores(byte[] key, double min, double max) {
		return zRevRangeByScoreWithScores(key, new Range().gte(min).lte(max), Limit.unlimited());
	}

	/**
	 * Get elements in range from {@code start} to {@code end} where score is between {@code min} and {@code max} from
	 * sorted set ordered high -> low.
	 *
	 * @param key must not be {@literal null}.
	 * @param min
	 * @param max
	 * @param offset
	 * @param count
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/zrevrangebyscore">Redis Documentation: ZREVRANGEBYSCORE</a>
	 */
	@Nullable
	default Set<byte[]> zRevRangeByScore(byte[] key, double min, double max, long offset, long count) {

		return zRevRangeByScore(key, new Range().gte(min).lte(max),
				new Limit().offset(Long.valueOf(offset).intValue()).count(Long.valueOf(count).intValue()));
	}

	/**
	 * Get elements in range from {@code Limit#offset} to {@code Limit#offset + Limit#count} where score is between
	 * {@code Range#min} and {@code Range#max} from sorted set ordered high -> low.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @param limit must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 1.6
	 * @see <a href="http://redis.io/commands/zrevrangebyscore">Redis Documentation: ZREVRANGEBYSCORE</a>
	 */
	@Nullable
	Set<byte[]> zRevRangeByScore(byte[] key, Range range, Limit limit);

	/**
	 * Get set of {@link Tuple} in range from {@code start} to {@code end} where score is between {@code min} and
	 * {@code max} from sorted set ordered high -> low.
	 *
	 * @param key must not be {@literal null}.
	 * @param min
	 * @param max
	 * @param offset
	 * @param count
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/zrevrangebyscore">Redis Documentation: ZREVRANGEBYSCORE</a>
	 */
	@Nullable
	default Set<Tuple> zRevRangeByScoreWithScores(byte[] key, double min, double max, long offset, long count) {

		return zRevRangeByScoreWithScores(key, new Range().gte(min).lte(max),
				new Limit().offset(Long.valueOf(offset).intValue()).count(Long.valueOf(count).intValue()));
	}

	/**
	 * Get set of {@link Tuple} where score is between {@code Range#min} and {@code Range#max} from sorted set ordered
	 * from high to low.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 1.6
	 * @see <a href="http://redis.io/commands/zrevrangebyscore">Redis Documentation: ZREVRANGEBYSCORE</a>
	 */
	@Nullable
	default Set<Tuple> zRevRangeByScoreWithScores(byte[] key, Range range) {
		return zRevRangeByScoreWithScores(key, range, Limit.unlimited());
	}

	/**
	 * Get set of {@link Tuple} in range from {@code Limit#offset} to {@code Limit#count} where score is between
	 * {@code Range#min} and {@code Range#max} from sorted set ordered high -> low.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @param limit must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 1.6
	 * @see <a href="http://redis.io/commands/zrevrangebyscore">Redis Documentation: ZREVRANGEBYSCORE</a>
	 */
	@Nullable
	Set<Tuple> zRevRangeByScoreWithScores(byte[] key, Range range, Limit limit);

	/**
	 * Count number of elements within sorted set with scores between {@code min} and {@code max}.
	 *
	 * @param key must not be {@literal null}.
	 * @param min
	 * @param max
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/zcount">Redis Documentation: ZCOUNT</a>
	 */
	@Nullable
	default Long zCount(byte[] key, double min, double max) {
		return zCount(key, new Range().gte(min).lte(max));
	}

	/**
	 * Count number of elements within sorted set with scores between {@code Range#min} and {@code Range#max}.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 1.6
	 * @see <a href="http://redis.io/commands/zcount">Redis Documentation: ZCOUNT</a>
	 */
	@Nullable
	Long zCount(byte[] key, Range range);

	/**
	 * Get the size of sorted set with {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/zcard">Redis Documentation: ZCARD</a>
	 */
	@Nullable
	Long zCard(byte[] key);

	/**
	 * Get the score of element with {@code value} from sorted set with key {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value the value.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/zscore">Redis Documentation: ZSCORE</a>
	 */
	@Nullable
	Double zScore(byte[] key, byte[] value);

	/**
	 * Remove elements in range between {@code start} and {@code end} from sorted set with {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param start
	 * @param end
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/zremrangebyrank">Redis Documentation: ZREMRANGEBYRANK</a>
	 */
	@Nullable
	Long zRemRange(byte[] key, long start, long end);

	/**
	 * Remove elements with scores between {@code min} and {@code max} from sorted set with {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param min
	 * @param max
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/zremrangebyscore">Redis Documentation: ZREMRANGEBYSCORE</a>
	 */
	@Nullable
	default Long zRemRangeByScore(byte[] key, double min, double max) {
		return zRemRangeByScore(key, new Range().gte(min).lte(max));
	}

	/**
	 * Remove elements with scores between {@code Range#min} and {@code Range#max} from sorted set with {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 1.6
	 * @see <a href="http://redis.io/commands/zremrangebyscore">Redis Documentation: ZREMRANGEBYSCORE</a>
	 */
	@Nullable
	Long zRemRangeByScore(byte[] key, Range range);

	/**
	 * Union sorted {@code sets} and store result in destination {@code key}.
	 *
	 * @param destKey must not be {@literal null}.
	 * @param sets must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/zunionstore">Redis Documentation: ZUNIONSTORE</a>
	 */
	@Nullable
	Long zUnionStore(byte[] destKey, byte[]... sets);

	/**
	 * Union sorted {@code sets} and store result in destination {@code key}.
	 *
	 * @param destKey must not be {@literal null}.
	 * @param aggregate must not be {@literal null}.
	 * @param weights must not be {@literal null}.
	 * @param sets must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/zunionstore">Redis Documentation: ZUNIONSTORE</a>
	 */
	@Nullable
	default Long zUnionStore(byte[] destKey, Aggregate aggregate, int[] weights, byte[]... sets) {
		return zUnionStore(destKey, aggregate, Weights.of(weights), sets);
	}

	/**
	 * Union sorted {@code sets} and store result in destination {@code key}.
	 *
	 * @param destKey must not be {@literal null}.
	 * @param aggregate must not be {@literal null}.
	 * @param weights must not be {@literal null}.
	 * @param sets must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.1
	 * @see <a href="http://redis.io/commands/zunionstore">Redis Documentation: ZUNIONSTORE</a>
	 */
	@Nullable
	Long zUnionStore(byte[] destKey, Aggregate aggregate, Weights weights, byte[]... sets);

	/**
	 * Intersect sorted {@code sets} and store result in destination {@code key}.
	 *
	 * @param destKey must not be {@literal null}.
	 * @param sets must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/zinterstore">Redis Documentation: ZINTERSTORE</a>
	 */
	@Nullable
	Long zInterStore(byte[] destKey, byte[]... sets);

	/**
	 * Intersect sorted {@code sets} and store result in destination {@code key}.
	 *
	 * @param destKey must not be {@literal null}.
	 * @param aggregate must not be {@literal null}.
	 * @param weights
	 * @param sets must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/zinterstore">Redis Documentation: ZINTERSTORE</a>
	 */
	@Nullable
	default Long zInterStore(byte[] destKey, Aggregate aggregate, int[] weights, byte[]... sets) {
		return zInterStore(destKey, aggregate, Weights.of(weights), sets);
	}

	/**
	 * Intersect sorted {@code sets} and store result in destination {@code key}.
	 *
	 * @param destKey must not be {@literal null}.
	 * @param aggregate must not be {@literal null}.
	 * @param weights must not be {@literal null}.
	 * @param sets must not be {@literal null}.
	 * @return
	 * @since 2.1
	 * @see <a href="http://redis.io/commands/zinterstore">Redis Documentation: ZINTERSTORE</a>
	 */
	@Nullable
	Long zInterStore(byte[] destKey, Aggregate aggregate, Weights weights, byte[]... sets);

	/**
	 * Use a {@link Cursor} to iterate over elements in sorted set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param options must not be {@literal null}.
	 * @return
	 * @since 1.4
	 * @see <a href="http://redis.io/commands/zscan">Redis Documentation: ZSCAN</a>
	 */
	Cursor<Tuple> zScan(byte[] key, ScanOptions options);

	/**
	 * Get elements where score is between {@code min} and {@code max} from sorted set.
	 *
	 * @param key must not be {@literal null}.
	 * @param min
	 * @param max
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 1.5
	 * @see <a href="http://redis.io/commands/zrangebyscore">Redis Documentation: ZRANGEBYSCORE</a>
	 */
	@Nullable
	default Set<byte[]> zRangeByScore(byte[] key, String min, String max) {
		return zRangeByScore(key, new Range().gte(min).lte(max));
	}

	/**
	 * Get elements where score is between {@code Range#min} and {@code Range#max} from sorted set.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 1.6
	 * @see <a href="http://redis.io/commands/zrangebyscore">Redis Documentation: ZRANGEBYSCORE</a>
	 */
	@Nullable
	default Set<byte[]> zRangeByScore(byte[] key, Range range) {
		return zRangeByScore(key, range, Limit.unlimited());
	}

	/**
	 * Get elements in range from {@code start} to {@code end} where score is between {@code min} and {@code max} from
	 * sorted set.
	 *
	 * @param key must not be {@literal null}.
	 * @param min must not be {@literal null}.
	 * @param max must not be {@literal null}.
	 * @param offset
	 * @param count
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 1.5
	 * @see <a href="http://redis.io/commands/zrangebyscore">Redis Documentation: ZRANGEBYSCORE</a>
	 */
	@Nullable
	Set<byte[]> zRangeByScore(byte[] key, String min, String max, long offset, long count);

	/**
	 * Get elements in range from {@code Limit#count} to {@code Limit#offset} where score is between {@code Range#min} and
	 * {@code Range#max} from sorted set.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @param limit must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 1.6
	 * @see <a href="http://redis.io/commands/zrangebyscore">Redis Documentation: ZRANGEBYSCORE</a>
	 */
	@Nullable
	Set<byte[]> zRangeByScore(byte[] key, Range range, Limit limit);

	/**
	 * Get all the elements in the sorted set at {@literal key} in lexicographical ordering.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 1.6
	 * @see <a href="http://redis.io/commands/zrangebylex">Redis Documentation: ZRANGEBYLEX</a>
	 */
	@Nullable
	default Set<byte[]> zRangeByLex(byte[] key) {
		return zRangeByLex(key, Range.unbounded());
	}

	/**
	 * Get all the elements in {@link Range} from the sorted set at {@literal key} in lexicographical ordering.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 1.6
	 * @see <a href="http://redis.io/commands/zrangebylex">Redis Documentation: ZRANGEBYLEX</a>
	 */
	@Nullable
	default Set<byte[]> zRangeByLex(byte[] key, Range range) {
		return zRangeByLex(key, range, Limit.unlimited());
	}

	/**
	 * Get all the elements in {@link Range} from the sorted set at {@literal key} in lexicographical ordering. Result is
	 * limited via {@link Limit}.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @param limit must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 1.6
	 * @see <a href="http://redis.io/commands/zrangebylex">Redis Documentation: ZRANGEBYLEX</a>
	 */
	@Nullable
	Set<byte[]> zRangeByLex(byte[] key, Range range, Limit limit);

}
