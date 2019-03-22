/*
 * Copyright 2011-2019 the original author or authors.
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

import java.util.Set;

import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;
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
	public enum Aggregate {
		SUM, MIN, MAX;
	}

	/**
	 * ZSet tuple.
	 */
	public interface Tuple extends Comparable<Double> {

		byte[] getValue();

		Double getScore();
	}

	/**
	 * {@link Range} defines {@literal min} and {@literal max} values to retrieve from a {@literal ZSET}.
	 *
	 * @author Christoph Strobl
	 * @since 1.6
	 */
	public class Range {

		Boundary min;
		Boundary max;

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
		 * @param min
		 * @return
		 */
		public Range gte(Object min) {

			Assert.notNull(min, "Min already set for range.");
			this.min = new Boundary(min, true);
			return this;
		}

		/**
		 * Greater Than
		 *
		 * @param min
		 * @return
		 */
		public Range gt(Object min) {

			Assert.notNull(min, "Min already set for range.");
			this.min = new Boundary(min, false);
			return this;
		}

		/**
		 * Less Then Equals
		 *
		 * @param max
		 * @return
		 */
		public Range lte(Object max) {

			Assert.notNull(max, "Max already set for range.");
			this.max = new Boundary(max, true);
			return this;
		}

		/**
		 * Less Than
		 *
		 * @param max
		 * @return
		 */
		public Range lt(Object max) {

			Assert.notNull(max, "Max already set for range.");
			this.max = new Boundary(max, false);
			return this;
		}

		/**
		 * @return {@literal null} if not set.
		 */
		public Boundary getMin() {
			return min;
		}

		/**
		 * @return {@literal null} if not set.
		 */
		public Boundary getMax() {
			return max;
		}

		/**
		 * @author Christoph Strobl
		 * @since 1.6
		 */
		public static class Boundary {

			Object value;
			boolean including;

			static Boundary infinite() {
				return new Boundary(null, true);
			}

			Boundary(Object value, boolean including) {
				this.value = value;
				this.including = including;
			}

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
	public class Limit {

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
	}

	/**
	 * Add {@code value} to a sorted set at {@code key}, or update its {@code score} if it already exists.
	 *
	 * @param key must not be {@literal null}.
	 * @param score the score.
	 * @param value the value.
	 * @return
	 * @see <a href="http://redis.io/commands/zadd">Redis Documentation: ZADD</a>
	 */
	Boolean zAdd(byte[] key, double score, byte[] value);

	/**
	 * Add {@code tuples} to a sorted set at {@code key}, or update its {@code score} if it already exists.
	 *
	 * @param key must not be {@literal null}.
	 * @param tuples must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/zadd">Redis Documentation: ZADD</a>
	 */
	Long zAdd(byte[] key, Set<Tuple> tuples);

	/**
	 * Remove {@code values} from sorted set. Return number of removed elements.
	 *
	 * @param key must not be {@literal null}.
	 * @param values must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/zrem">Redis Documentation: ZREM</a>
	 */
	Long zRem(byte[] key, byte[]... values);

	/**
	 * Increment the score of element with {@code value} in sorted set by {@code increment}.
	 *
	 * @param key must not be {@literal null}.
	 * @param increment
	 * @param value the value.
	 * @return
	 * @see <a href="http://redis.io/commands/zincrby">Redis Documentation: ZINCRBY</a>
	 */
	Double zIncrBy(byte[] key, double increment, byte[] value);

	/**
	 * Determine the index of element with {@code value} in a sorted set.
	 *
	 * @param key must not be {@literal null}.
	 * @param value the value.
	 * @return
	 * @see <a href="http://redis.io/commands/zrank">Redis Documentation: ZRANK</a>
	 */
	Long zRank(byte[] key, byte[] value);

	/**
	 * Determine the index of element with {@code value} in a sorted set when scored high to low.
	 *
	 * @param key must not be {@literal null}.
	 * @param value the value.
	 * @return
	 * @see <a href="http://redis.io/commands/zrevrank">Redis Documentation: ZREVRANK</a>
	 */
	Long zRevRank(byte[] key, byte[] value);

	/**
	 * Get elements between {@code start} and {@code end} from sorted set.
	 *
	 * @param key must not be {@literal null}.
	 * @param start
	 * @param end
	 * @return
	 * @see <a href="http://redis.io/commands/zrange">Redis Documentation: ZRANGE</a>
	 */
	Set<byte[]> zRange(byte[] key, long start, long end);

	/**
	 * Get set of {@link Tuple}s between {@code start} and {@code end} from sorted set.
	 *
	 * @param key must not be {@literal null}.
	 * @param start
	 * @param end
	 * @return
	 * @see <a href="http://redis.io/commands/zrange">Redis Documentation: ZRANGE</a>
	 */
	Set<Tuple> zRangeWithScores(byte[] key, long start, long end);

	/**
	 * Get elements where score is between {@code min} and {@code max} from sorted set.
	 *
	 * @param key must not be {@literal null}.
	 * @param min
	 * @param max
	 * @return
	 * @see <a href="http://redis.io/commands/zrangebyscore">Redis Documentation: ZRANGEBYSCORE</a>
	 */
	Set<byte[]> zRangeByScore(byte[] key, double min, double max);

	/**
	 * Get set of {@link Tuple}s where score is between {@code Range#min} and {@code Range#max} from sorted set.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return
	 * @since 1.6
	 * @see <a href="http://redis.io/commands/zrangebyscore">Redis Documentation: ZRANGEBYSCORE</a>
	 */
	Set<Tuple> zRangeByScoreWithScores(byte[] key, Range range);

	/**
	 * Get set of {@link Tuple}s where score is between {@code min} and {@code max} from sorted set.
	 *
	 * @param key must not be {@literal null}.
	 * @param min
	 * @param max
	 * @return
	 * @see <a href="http://redis.io/commands/zrangebyscore">Redis Documentation: ZRANGEBYSCORE</a>
	 */
	Set<Tuple> zRangeByScoreWithScores(byte[] key, double min, double max);

	/**
	 * Get elements in range from {@code start} to {@code end} where score is between {@code min} and {@code max} from
	 * sorted set.
	 *
	 * @param key must not be {@literal null}.
	 * @param min
	 * @param max
	 * @param offset
	 * @param count
	 * @return
	 * @see <a href="http://redis.io/commands/zrangebyscore">Redis Documentation: ZRANGEBYSCORE</a>
	 */
	Set<byte[]> zRangeByScore(byte[] key, double min, double max, long offset, long count);

	/**
	 * Get set of {@link Tuple}s in range from {@code start} to {@code end} where score is between {@code min} and
	 * {@code max} from sorted set.
	 *
	 * @param key
	 * @param min
	 * @param max
	 * @param offset
	 * @param count
	 * @return
	 * @see <a href="http://redis.io/commands/zrangebyscore">Redis Documentation: ZRANGEBYSCORE</a>
	 */
	Set<Tuple> zRangeByScoreWithScores(byte[] key, double min, double max, long offset, long count);

	/**
	 * Get set of {@link Tuple}s in range from {@code Limit#offset} to {@code Limit#offset + Limit#count} where score is
	 * between {@code Range#min} and {@code Range#max} from sorted set.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @param limit
	 * @return
	 * @since 1.6
	 * @see <a href="http://redis.io/commands/zrangebyscore">Redis Documentation: ZRANGEBYSCORE</a>
	 */
	Set<Tuple> zRangeByScoreWithScores(byte[] key, Range range, Limit limit);

	/**
	 * Get elements in range from {@code start} to {@code end} from sorted set ordered from high to low.
	 *
	 * @param key must not be {@literal null}.
	 * @param start
	 * @param end
	 * @return
	 * @see <a href="http://redis.io/commands/zrevrange">Redis Documentation: ZREVRANGE</a>
	 */
	Set<byte[]> zRevRange(byte[] key, long start, long end);

	/**
	 * Get set of {@link Tuple}s in range from {@code start} to {@code end} from sorted set ordered from high to low.
	 *
	 * @param key must not be {@literal null}.
	 * @param start
	 * @param end
	 * @return
	 * @see <a href="http://redis.io/commands/zrevrange">Redis Documentation: ZREVRANGE</a>
	 */
	Set<Tuple> zRevRangeWithScores(byte[] key, long start, long end);

	/**
	 * Get elements where score is between {@code min} and {@code max} from sorted set ordered from high to low.
	 *
	 * @param key must not be {@literal null}.
	 * @param min
	 * @param max
	 * @return
	 * @see <a href="http://redis.io/commands/zrevrange">Redis Documentation: ZREVRANGE</a>
	 */
	Set<byte[]> zRevRangeByScore(byte[] key, double min, double max);

	/**
	 * Get elements where score is between {@code Range#min} and {@code Range#max} from sorted set ordered from high to
	 * low.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return
	 * @since 1.6
	 * @see <a href="http://redis.io/commands/zrevrangebyscore">Redis Documentation: ZREVRANGEBYSCORE</a>
	 */
	Set<byte[]> zRevRangeByScore(byte[] key, Range range);

	/**
	 * Get set of {@link Tuple} where score is between {@code min} and {@code max} from sorted set ordered from high to
	 * low.
	 *
	 * @param key must not be {@literal null}.
	 * @param min
	 * @param max
	 * @return
	 * @see <a href="http://redis.io/commands/zrevrangebyscore">Redis Documentation: ZREVRANGEBYSCORE</a>
	 */
	Set<Tuple> zRevRangeByScoreWithScores(byte[] key, double min, double max);

	/**
	 * Get elements in range from {@code start} to {@code end} where score is between {@code min} and {@code max} from
	 * sorted set ordered high -> low.
	 *
	 * @param key must not be {@literal null}.
	 * @param min
	 * @param max
	 * @param offset
	 * @param count
	 * @return
	 * @see <a href="http://redis.io/commands/zrevrangebyscore">Redis Documentation: ZREVRANGEBYSCORE</a>
	 */
	Set<byte[]> zRevRangeByScore(byte[] key, double min, double max, long offset, long count);

	/**
	 * Get elements in range from {@code Limit#offset} to {@code Limit#offset + Limit#count} where score is between
	 * {@code Range#min} and {@code Range#max} from sorted set ordered high -> low.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @param limit
	 * @return
	 * @since 1.6
	 * @see <a href="http://redis.io/commands/zrevrangebyscore">Redis Documentation: ZREVRANGEBYSCORE</a>
	 */
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
	 * @return
	 * @see <a href="http://redis.io/commands/zrevrangebyscore">Redis Documentation: ZREVRANGEBYSCORE</a>
	 */
	Set<Tuple> zRevRangeByScoreWithScores(byte[] key, double min, double max, long offset, long count);

	/**
	 * Get set of {@link Tuple} where score is between {@code Range#min} and {@code Range#max} from sorted set ordered
	 * from high to low.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return
	 * @since 1.6
	 * @see <a href="http://redis.io/commands/zrevrangebyscore">Redis Documentation: ZREVRANGEBYSCORE</a>
	 */
	Set<Tuple> zRevRangeByScoreWithScores(byte[] key, Range range);

	/**
	 * Get set of {@link Tuple} in range from {@code Limit#offset} to {@code Limit#count} where score is between
	 * {@code Range#min} and {@code Range#max} from sorted set ordered high -> low.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @param limit
	 * @return
	 * @since 1.6
	 * @see <a href="http://redis.io/commands/zrevrangebyscore">Redis Documentation: ZREVRANGEBYSCORE</a>
	 */
	Set<Tuple> zRevRangeByScoreWithScores(byte[] key, Range range, Limit limit);

	/**
	 * Count number of elements within sorted set with scores between {@code min} and {@code max}.
	 *
	 * @param key must not be {@literal null}.
	 * @param min
	 * @param max
	 * @return
	 * @see <a href="http://redis.io/commands/zcount">Redis Documentation: ZCOUNT</a>
	 */
	Long zCount(byte[] key, double min, double max);

	/**
	 * Count number of elements within sorted set with scores between {@code Range#min} and {@code Range#max}.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return
	 * @since 1.6
	 * @see <a href="http://redis.io/commands/zcount">Redis Documentation: ZCOUNT</a>
	 */
	Long zCount(byte[] key, Range range);

	/**
	 * Get the size of sorted set with {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/zcard">Redis Documentation: ZCARD</a>
	 */
	Long zCard(byte[] key);

	/**
	 * Get the score of element with {@code value} from sorted set with key {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value the value.
	 * @return
	 * @see <a href="http://redis.io/commands/zscore">Redis Documentation: ZSCORE</a>
	 */
	Double zScore(byte[] key, byte[] value);

	/**
	 * Remove elements in range between {@code start} and {@code end} from sorted set with {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param start
	 * @param end
	 * @return
	 * @see <a href="http://redis.io/commands/zremrangebyrank">Redis Documentation: ZREMRANGEBYRANK</a>
	 */
	Long zRemRange(byte[] key, long start, long end);

	/**
	 * Remove elements with scores between {@code min} and {@code max} from sorted set with {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param min
	 * @param max
	 * @return
	 * @see <a href="http://redis.io/commands/zremrangebyscore">Redis Documentation: ZREMRANGEBYSCORE</a>
	 */
	Long zRemRangeByScore(byte[] key, double min, double max);

	/**
	 * Remove elements with scores between {@code Range#min} and {@code Range#max} from sorted set with {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return
	 * @since 1.6
	 * @see <a href="http://redis.io/commands/zremrangebyscore">Redis Documentation: ZREMRANGEBYSCORE</a>
	 */
	Long zRemRangeByScore(byte[] key, Range range);

	/**
	 * Union sorted {@code sets} and store result in destination {@code key}.
	 *
	 * @param destKey must not be {@literal null}.
	 * @param sets must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/zunionstore">Redis Documentation: ZUNIONSTORE</a>
	 */
	Long zUnionStore(byte[] destKey, byte[]... sets);

	/**
	 * Union sorted {@code sets} and store result in destination {@code key}.
	 *
	 * @param destKey must not be {@literal null}.
	 * @param aggregate must not be {@literal null}.
	 * @param weights
	 * @param sets must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/zunionstore">Redis Documentation: ZUNIONSTORE</a>
	 */
	Long zUnionStore(byte[] destKey, Aggregate aggregate, int[] weights, byte[]... sets);

	/**
	 * Intersect sorted {@code sets} and store result in destination {@code key}.
	 *
	 * @param destKey must not be {@literal null}.
	 * @param sets must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/zinterstore">Redis Documentation: ZINTERSTORE</a>
	 */
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
	Long zInterStore(byte[] destKey, Aggregate aggregate, int[] weights, byte[]... sets);

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
	 * @return
	 * @since 1.5
	 * @see <a href="http://redis.io/commands/zrangebyscore">Redis Documentation: ZRANGEBYSCORE</a>
	 */
	Set<byte[]> zRangeByScore(byte[] key, String min, String max);

	/**
	 * Get elements where score is between {@code Range#min} and {@code Range#max} from sorted set.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return
	 * @since 1.6
	 * @see <a href="http://redis.io/commands/zrangebyscore">Redis Documentation: ZRANGEBYSCORE</a>
	 */
	Set<byte[]> zRangeByScore(byte[] key, Range range);

	/**
	 * Get elements in range from {@code start} to {@code end} where score is between {@code min} and {@code max} from
	 * sorted set.
	 *
	 * @param key must not be {@literal null}.
	 * @param min must not be {@literal null}.
	 * @param max must not be {@literal null}.
	 * @param offset
	 * @param count
	 * @return
	 * @since 1.5
	 * @see <a href="http://redis.io/commands/zrangebyscore">Redis Documentation: ZRANGEBYSCORE</a>
	 */
	Set<byte[]> zRangeByScore(byte[] key, String min, String max, long offset, long count);

	/**
	 * Get elements in range from {@code Limit#count} to {@code Limit#offset} where score is between {@code Range#min} and
	 * {@code Range#max} from sorted set.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @param limit
	 * @return
	 * @since 1.6
	 * @see <a href="http://redis.io/commands/zrangebyscore">Redis Documentation: ZRANGEBYSCORE</a>
	 */
	Set<byte[]> zRangeByScore(byte[] key, Range range, Limit limit);

	/**
	 * Get all the elements in the sorted set at {@literal key} in lexicographical ordering.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 * @since 1.6
	 * @see <a href="http://redis.io/commands/zrangebylex">Redis Documentation: ZRANGEBYLEX</a>
	 */
	Set<byte[]> zRangeByLex(byte[] key);

	/**
	 * Get all the elements in {@link Range} from the sorted set at {@literal key} in lexicographical ordering.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return
	 * @since 1.6
	 * @see <a href="http://redis.io/commands/zrangebylex">Redis Documentation: ZRANGEBYLEX</a>
	 */
	Set<byte[]> zRangeByLex(byte[] key, Range range);

	/**
	 * Get all the elements in {@link Range} from the sorted set at {@literal key} in lexicographical ordering. Result is
	 * limited via {@link Limit}.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @param range can be {@literal null}.
	 * @return
	 * @since 1.6
	 * @see <a href="http://redis.io/commands/zrangebylex">Redis Documentation: ZRANGEBYLEX</a>
	 */
	Set<byte[]> zRangeByLex(byte[] key, Range range, Limit limit);

}
