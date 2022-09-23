/*
 * Copyright 2011-2022 the original author or authors.
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

import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.springframework.data.redis.connection.zset.Aggregate;
import org.springframework.data.redis.connection.zset.Tuple;
import org.springframework.data.redis.connection.zset.Weights;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

/**
 * ZSet(SortedSet)-specific commands supported by Redis.
 *
 * @author Costin Leau
 * @author Christoph Strobl
 * @author Thomas Darimont
 * @author David Liu
 * @author Mark Paluch
 * @author Andrey Shlykov
 * @author Shyngys Sapraliyev
 */
public interface RedisZSetCommands {

	/**
	 * {@link org.springframework.data.domain.Range} defines {@literal min} and {@literal max} values to retrieve from a
	 * {@literal ZSET}.
	 *
	 * @author Christoph Strobl
	 * @since 1.6
	 * @deprecated since 3.0, use {@link org.springframework.data.domain.Range} or {@link #toRange()} instead.
	 */
	@Deprecated
	class Range {

		@Nullable Boundary min;
		@Nullable Boundary max;

		/**
		 * @return new {@link org.springframework.data.domain.Range}
		 */
		public static Range range() {
			return new Range();
		}

		/**
		 * @return new {@link org.springframework.data.domain.Range} with {@literal min} and {@literal max} set to
		 *         {@link Boundary#infinite()}.
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

		/**
		 * Create a {@link org.springframework.data.domain.Range} object from this range.
		 *
		 * @return a {@link org.springframework.data.domain.Range} object using bounds from this range.
		 * @since 3.0
		 */
		public <T> org.springframework.data.domain.Range<T> toRange() {

			org.springframework.data.domain.Range.Bound<Object> lower = toBound(min);
			org.springframework.data.domain.Range.Bound<Object> upper = toBound(max);

			return (org.springframework.data.domain.Range<T>) org.springframework.data.domain.Range.from(lower).to(upper);
		}

		private org.springframework.data.domain.Range.Bound<Object> toBound(@Nullable Boundary boundary) {

			if (boundary == null || boundary.value == null) {
				return org.springframework.data.domain.Range.Bound.unbounded();
			}

			return boundary.isIncluding() ? org.springframework.data.domain.Range.Bound.inclusive(boundary.getValue())
					: org.springframework.data.domain.Range.Bound.exclusive(boundary.getValue());
		}

	}

	/**
	 * @author Christoph Strobl
	 * @since 1.6
	 * @deprecated since 3.0, use {@link org.springframework.data.redis.connection.Limit} instead.
	 */
	@Deprecated
	class Limit extends org.springframework.data.redis.connection.Limit {

	}

	/**
	 * {@code ZADD} specific arguments. <br />
	 * Looking of the {@code INCR} flag? Use the {@code ZINCRBY} operation instead.
	 *
	 * @since 2.5
	 * @see <a href="https://redis.io/commands/zadd">Redis Documentation: ZADD</a>
	 */
	class ZAddArgs {

		private static final ZAddArgs NONE = new ZAddArgs(EnumSet.noneOf(Flag.class));

		private final Set<Flag> flags;

		private ZAddArgs(Set<Flag> flags) {
			this.flags = flags;
		}

		/**
		 * @return new instance of {@link ZAddArgs} without any flags set.
		 */
		public static ZAddArgs empty() {
			return new ZAddArgs(EnumSet.noneOf(Flag.class));
		}

		/**
		 * @return new instance of {@link ZAddArgs} without {@link Flag#NX} set.
		 */
		public static ZAddArgs ifNotExists() {
			return empty().nx();
		}

		/**
		 * @return new instance of {@link ZAddArgs} without {@link Flag#NX} set.
		 */
		public static ZAddArgs ifExists() {
			return empty().xx();
		}

		/**
		 * Only update elements that already exist.
		 *
		 * @return this.
		 */
		public ZAddArgs nx() {

			flags.add(Flag.NX);
			return this;
		}

		/**
		 * Don't update already existing elements.
		 *
		 * @return this.
		 */
		public ZAddArgs xx() {

			flags.add(Flag.XX);
			return this;
		}

		/**
		 * Only update existing elements if the new score is less than the current score.
		 *
		 * @return this.
		 */
		public ZAddArgs lt() {

			flags.add(Flag.LT);
			return this;
		}

		/**
		 * Only update existing elements if the new score is greater than the current score.
		 *
		 * @return this.
		 */
		public ZAddArgs gt() {

			flags.add(Flag.GT);
			return this;
		}

		/**
		 * Only update elements that already exist.
		 *
		 * @return this.
		 */
		public ZAddArgs ch() {

			flags.add(Flag.CH);
			return this;
		}

		/**
		 * Only update elements that already exist.
		 *
		 * @return this.
		 */
		public boolean contains(Flag flag) {
			return flags.contains(flag);
		}

		/**
		 * @return {@literal true} if no flags set.
		 */
		public boolean isEmpty() {
			return !flags.isEmpty();
		}

		@Override
		public boolean equals(Object o) {

			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}

			ZAddArgs zAddArgs = (ZAddArgs) o;

			return ObjectUtils.nullSafeEquals(flags, zAddArgs.flags);
		}

		@Override
		public int hashCode() {
			return ObjectUtils.nullSafeHashCode(flags);
		}

		public enum Flag {

			/**
			 * Only update elements that already exist.
			 */
			XX,

			/**
			 * Don't update already existing elements.
			 */
			NX,

			/**
			 * Only update existing elements if the new score is greater than the current score.
			 */
			GT,

			/**
			 * Only update existing elements if the new score is less than the current score.
			 */
			LT,

			/**
			 * Modify the return value from the number of new elements added, to the total number of elements changed.
			 */
			CH
		}
	}

	/**
	 * Add {@code value} to a sorted set at {@code key}, or update its {@code score} if it already exists.
	 *
	 * @param key must not be {@literal null}.
	 * @param score the score.
	 * @param value the value.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zadd">Redis Documentation: ZADD</a>
	 */
	@Nullable
	default Boolean zAdd(byte[] key, double score, byte[] value) {
		return zAdd(key, score, value, ZAddArgs.NONE);
	}

	/**
	 * Add {@code value} to a sorted set at {@code key}, or update its {@code score} depending on the given
	 * {@link ZAddArgs args}.
	 *
	 * @param key must not be {@literal null}.
	 * @param score the score.
	 * @param value the value.
	 * @param args must not be {@literal null} use {@link ZAddArgs#empty()} instead.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.5
	 * @see <a href="https://redis.io/commands/zadd">Redis Documentation: ZADD</a>
	 */
	@Nullable
	Boolean zAdd(byte[] key, double score, byte[] value, ZAddArgs args);

	/**
	 * Add {@code tuples} to a sorted set at {@code key}, or update its {@code score} if it already exists.
	 *
	 * @param key must not be {@literal null}.
	 * @param tuples must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zadd">Redis Documentation: ZADD</a>
	 */
	@Nullable
	default Long zAdd(byte[] key, Set<Tuple> tuples) {
		return zAdd(key, tuples, ZAddArgs.NONE);
	}

	/**
	 * Add {@code tuples} to a sorted set at {@code key}, or update its {@code score} depending on the given
	 * {@link ZAddArgs args}.
	 *
	 * @param key must not be {@literal null}.
	 * @param tuples must not be {@literal null}.
	 * @param args must not be {@literal null} use {@link ZAddArgs#empty()} instead.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.5
	 * @see <a href="https://redis.io/commands/zadd">Redis Documentation: ZADD</a>
	 */
	Long zAdd(byte[] key, Set<Tuple> tuples, ZAddArgs args);

	/**
	 * Remove {@code values} from sorted set. Return number of removed elements.
	 *
	 * @param key must not be {@literal null}.
	 * @param values must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zrem">Redis Documentation: ZREM</a>
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
	 * @see <a href="https://redis.io/commands/zincrby">Redis Documentation: ZINCRBY</a>
	 */
	@Nullable
	Double zIncrBy(byte[] key, double increment, byte[] value);

	/**
	 * Get random element from sorted set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return can be {@literal null}.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zrandmember">Redis Documentation: ZRANDMEMBER</a>
	 */
	@Nullable
	byte[] zRandMember(byte[] key);

	/**
	 * Get {@code count} random elements from sorted set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param count if the provided {@code count} argument is positive, return a list of distinct fields, capped either at
	 *          {@code count} or the set size. If {@code count} is negative, the behavior changes and the command is
	 *          allowed to return the same value multiple times. In this case, the number of returned values is the
	 *          absolute value of the specified count.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zrandmember">Redis Documentation: ZRANDMEMBER</a>
	 */
	@Nullable
	List<byte[]> zRandMember(byte[] key, long count);

	/**
	 * Get random element from sorted set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return can be {@literal null}.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zrandmember">Redis Documentation: ZRANDMEMBER</a>
	 */
	@Nullable
	Tuple zRandMemberWithScore(byte[] key);

	/**
	 * Get {@code count} random elements from sorted set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param count if the provided {@code count} argument is positive, return a list of distinct fields, capped either at
	 *          {@code count} or the set size. If {@code count} is negative, the behavior changes and the command is
	 *          allowed to return the same value multiple times. In this case, the number of returned values is the
	 *          absolute value of the specified count.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zrandmember">Redis Documentation: ZRANDMEMBER</a>
	 */
	@Nullable
	List<Tuple> zRandMemberWithScore(byte[] key, long count);

	/**
	 * Determine the index of element with {@code value} in a sorted set.
	 *
	 * @param key must not be {@literal null}.
	 * @param value the value. Must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zrank">Redis Documentation: ZRANK</a>
	 */
	@Nullable
	Long zRank(byte[] key, byte[] value);

	/**
	 * Determine the index of element with {@code value} in a sorted set when scored high to low.
	 *
	 * @param key must not be {@literal null}.
	 * @param value the value.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zrevrank">Redis Documentation: ZREVRANK</a>
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
	 * @see <a href="https://redis.io/commands/zrange">Redis Documentation: ZRANGE</a>
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
	 * @see <a href="https://redis.io/commands/zrange">Redis Documentation: ZRANGE</a>
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
	 * @see <a href="https://redis.io/commands/zrangebyscore">Redis Documentation: ZRANGEBYSCORE</a>
	 */
	@Nullable
	default Set<byte[]> zRangeByScore(byte[] key, double min, double max) {
		return zRangeByScore(key, org.springframework.data.domain.Range.closed(min, max));
	}

	/**
	 * Get set of {@link Tuple}s where score is between {@code Range#min} and {@code Range#max} from sorted set.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return empty {@link Set} when key does not exists or no members in range. {@literal null} when used in pipeline /
	 *         transaction.
	 * @since 1.6
	 * @see <a href="https://redis.io/commands/zrangebyscore">Redis Documentation: ZRANGEBYSCORE</a>
	 */
	@Nullable
	default Set<Tuple> zRangeByScoreWithScores(byte[] key,
			org.springframework.data.domain.Range<? extends Number> range) {
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
	 * @see <a href="https://redis.io/commands/zrangebyscore">Redis Documentation: ZRANGEBYSCORE</a>
	 */
	@Nullable
	default Set<Tuple> zRangeByScoreWithScores(byte[] key, double min, double max) {
		return zRangeByScoreWithScores(key, org.springframework.data.domain.Range.closed(min, max));
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
	 * @see <a href="https://redis.io/commands/zrangebyscore">Redis Documentation: ZRANGEBYSCORE</a>
	 */
	@Nullable
	default Set<byte[]> zRangeByScore(byte[] key, double min, double max, long offset, long count) {
		return zRangeByScore(key, org.springframework.data.domain.Range.closed(min, max),
				new org.springframework.data.redis.connection.Limit().offset(Long.valueOf(offset).intValue())
						.count(Long.valueOf(count).intValue()));
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
	 * @see <a href="https://redis.io/commands/zrangebyscore">Redis Documentation: ZRANGEBYSCORE</a>
	 */
	@Nullable
	default Set<Tuple> zRangeByScoreWithScores(byte[] key, double min, double max, long offset, long count) {
		return zRangeByScoreWithScores(key, org.springframework.data.domain.Range.closed(min, max),
				new org.springframework.data.redis.connection.Limit().offset(Long.valueOf(offset).intValue())
						.count(Long.valueOf(count).intValue()));
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
	 * @see <a href="https://redis.io/commands/zrangebyscore">Redis Documentation: ZRANGEBYSCORE</a>
	 */
	@Nullable
	Set<Tuple> zRangeByScoreWithScores(byte[] key, org.springframework.data.domain.Range<? extends Number> range,
			org.springframework.data.redis.connection.Limit limit);

	/**
	 * Get elements in range from {@code start} to {@code end} from sorted set ordered from high to low.
	 *
	 * @param key must not be {@literal null}.
	 * @param start
	 * @param end
	 * @return empty {@link Set} when key does not exists or no members in range. {@literal null} when used in pipeline /
	 *         transaction.
	 * @see <a href="https://redis.io/commands/zrevrange">Redis Documentation: ZREVRANGE</a>
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
	 * @see <a href="https://redis.io/commands/zrevrange">Redis Documentation: ZREVRANGE</a>
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
	 * @see <a href="https://redis.io/commands/zrevrange">Redis Documentation: ZREVRANGE</a>
	 */
	@Nullable
	default Set<byte[]> zRevRangeByScore(byte[] key, double min, double max) {
		return zRevRangeByScore(key, org.springframework.data.domain.Range.closed(min, max));
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
	 * @see <a href="https://redis.io/commands/zrevrangebyscore">Redis Documentation: ZREVRANGEBYSCORE</a>
	 */
	@Nullable
	default Set<byte[]> zRevRangeByScore(byte[] key, org.springframework.data.domain.Range<? extends Number> range) {
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
	 * @see <a href="https://redis.io/commands/zrevrangebyscore">Redis Documentation: ZREVRANGEBYSCORE</a>
	 */
	@Nullable
	default Set<Tuple> zRevRangeByScoreWithScores(byte[] key, double min, double max) {
		return zRevRangeByScoreWithScores(key, org.springframework.data.domain.Range.closed(min, max), Limit.unlimited());
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
	 * @see <a href="https://redis.io/commands/zrevrangebyscore">Redis Documentation: ZREVRANGEBYSCORE</a>
	 */
	@Nullable
	default Set<byte[]> zRevRangeByScore(byte[] key, double min, double max, long offset, long count) {

		return zRevRangeByScore(key, org.springframework.data.domain.Range.closed(min, max),
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
	 * @see <a href="https://redis.io/commands/zrevrangebyscore">Redis Documentation: ZREVRANGEBYSCORE</a>
	 */
	@Nullable
	Set<byte[]> zRevRangeByScore(byte[] key, org.springframework.data.domain.Range<? extends Number> range,
			org.springframework.data.redis.connection.Limit limit);

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
	 * @see <a href="https://redis.io/commands/zrevrangebyscore">Redis Documentation: ZREVRANGEBYSCORE</a>
	 */
	@Nullable
	default Set<Tuple> zRevRangeByScoreWithScores(byte[] key, double min, double max, long offset, long count) {

		return zRevRangeByScoreWithScores(key, org.springframework.data.domain.Range.closed(min, max),
				new org.springframework.data.redis.connection.Limit().offset(Long.valueOf(offset).intValue())
						.count(Long.valueOf(count).intValue()));
	}

	/**
	 * Get set of {@link Tuple} where score is between {@code Range#min} and {@code Range#max} from sorted set ordered
	 * from high to low.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 1.6
	 * @see <a href="https://redis.io/commands/zrevrangebyscore">Redis Documentation: ZREVRANGEBYSCORE</a>
	 */
	@Nullable
	default Set<Tuple> zRevRangeByScoreWithScores(byte[] key,
			org.springframework.data.domain.Range<? extends Number> range) {
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
	 * @see <a href="https://redis.io/commands/zrevrangebyscore">Redis Documentation: ZREVRANGEBYSCORE</a>
	 */
	@Nullable
	Set<Tuple> zRevRangeByScoreWithScores(byte[] key, org.springframework.data.domain.Range<? extends Number> range,
			org.springframework.data.redis.connection.Limit limit);

	/**
	 * Count number of elements within sorted set with scores between {@code min} and {@code max}.
	 *
	 * @param key must not be {@literal null}.
	 * @param min
	 * @param max
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zcount">Redis Documentation: ZCOUNT</a>
	 */
	@Nullable
	default Long zCount(byte[] key, double min, double max) {
		return zCount(key, org.springframework.data.domain.Range.closed(min, max));
	}

	/**
	 * Count number of elements within sorted set with scores between {@code Range#min} and {@code Range#max}.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 1.6
	 * @see <a href="https://redis.io/commands/zcount">Redis Documentation: ZCOUNT</a>
	 */
	@Nullable
	Long zCount(byte[] key, org.springframework.data.domain.Range<? extends Number> range);

	/**
	 * Count number of elements within sorted set with value between {@code Range#min} and {@code Range#max} applying
	 * lexicographical ordering.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.4
	 * @see <a href="https://redis.io/commands/zlexcount">Redis Documentation: ZLEXCOUNT</a>
	 */
	@Nullable
	Long zLexCount(byte[] key, org.springframework.data.domain.Range<byte[]> range);

	/**
	 * Remove and return the value with its score having the lowest score from sorted set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when the sorted set is empty or used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zpopmin">Redis Documentation: ZPOPMIN</a>
	 * @since 2.6
	 */
	@Nullable
	Tuple zPopMin(byte[] key);

	/**
	 * Remove and return {@code count} values with their score having the lowest score from sorted set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param count number of elements to pop.
	 * @return {@literal null} when the sorted set is empty or used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zpopmin">Redis Documentation: ZPOPMIN</a>
	 * @since 2.6
	 */
	@Nullable
	Set<Tuple> zPopMin(byte[] key, long count);

	/**
	 * Remove and return the value with its score having the lowest score from sorted set at {@code key}. <br />
	 * <b>Blocks connection</b> until element available or {@code timeout} reached.
	 *
	 * @param key must not be {@literal null}.
	 * @param timeout
	 * @param unit must not be {@literal null}.
	 * @return can be {@literal null}.
	 * @see <a href="https://redis.io/commands/bzpopmin">Redis Documentation: BZPOPMIN</a>
	 * @since 2.6
	 */
	@Nullable
	Tuple bZPopMin(byte[] key, long timeout, TimeUnit unit);

	/**
	 * Remove and return the value with its score having the highest score from sorted set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when the sorted set is empty or used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zpopmax">Redis Documentation: ZPOPMAX</a>
	 * @since 2.6
	 */
	@Nullable
	Tuple zPopMax(byte[] key);

	/**
	 * Remove and return {@code count} values with their score having the highest score from sorted set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param count number of elements to pop.
	 * @return {@literal null} when the sorted set is empty or used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zpopmax">Redis Documentation: ZPOPMAX</a>
	 * @since 2.6
	 */
	@Nullable
	Set<Tuple> zPopMax(byte[] key, long count);

	/**
	 * Remove and return the value with its score having the highest score from sorted set at {@code key}. <br />
	 * <b>Blocks connection</b> until element available or {@code timeout} reached.
	 *
	 * @param key must not be {@literal null}.
	 * @param timeout
	 * @param unit must not be {@literal null}.
	 * @return can be {@literal null}.
	 * @see <a href="https://redis.io/commands/bzpopmax">Redis Documentation: BZPOPMAX</a>
	 * @since 2.6
	 */
	@Nullable
	Tuple bZPopMax(byte[] key, long timeout, TimeUnit unit);

	/**
	 * Get the size of sorted set with {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zcard">Redis Documentation: ZCARD</a>
	 */
	@Nullable
	Long zCard(byte[] key);

	/**
	 * Get the score of element with {@code value} from sorted set with key {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value the value.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zscore">Redis Documentation: ZSCORE</a>
	 */
	@Nullable
	Double zScore(byte[] key, byte[] value);

	/**
	 * Get the scores of elements with {@code values} from sorted set with key {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param values the values.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zmscore">Redis Documentation: ZMSCORE</a>
	 * @since 2.6
	 */
	@Nullable
	List<Double> zMScore(byte[] key, byte[]... values);

	/**
	 * Remove elements in range between {@code start} and {@code end} from sorted set with {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param start
	 * @param end
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zremrangebyrank">Redis Documentation: ZREMRANGEBYRANK</a>
	 */
	@Nullable
	Long zRemRange(byte[] key, long start, long end);

	/**
	 * Remove all elements between the lexicographical {@link org.springframework.data.domain.Range}.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return the number of elements removed, or {@literal null} when used in pipeline / transaction.
	 * @since 2.5
	 * @see <a href="https://redis.io/commands/zremrangebylex">Redis Documentation: ZREMRANGEBYLEX</a>
	 */
	Long zRemRangeByLex(byte[] key, org.springframework.data.domain.Range<byte[]> range);

	/**
	 * Remove elements with scores between {@code min} and {@code max} from sorted set with {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param min
	 * @param max
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zremrangebyscore">Redis Documentation: ZREMRANGEBYSCORE</a>
	 */
	@Nullable
	default Long zRemRangeByScore(byte[] key, double min, double max) {
		return zRemRangeByScore(key, org.springframework.data.domain.Range.closed(min, max));
	}

	/**
	 * Remove elements with scores between {@code Range#min} and {@code Range#max} from sorted set with {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 1.6
	 * @see <a href="https://redis.io/commands/zremrangebyscore">Redis Documentation: ZREMRANGEBYSCORE</a>
	 */
	@Nullable
	Long zRemRangeByScore(byte[] key, org.springframework.data.domain.Range<? extends Number> range);

	/**
	 * Diff sorted {@code sets}.
	 *
	 * @param sets must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zdiff">Redis Documentation: ZDIFF</a>
	 */
	@Nullable
	Set<byte[]> zDiff(byte[]... sets);

	/**
	 * Diff sorted {@code sets}.
	 *
	 * @param sets must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zdiff">Redis Documentation: ZDIFF</a>
	 */
	@Nullable
	Set<Tuple> zDiffWithScores(byte[]... sets);

	/**
	 * Diff sorted {@code sets} and store result in destination {@code destKey}.
	 *
	 * @param destKey must not be {@literal null}.
	 * @param sets must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zdiffstore">Redis Documentation: ZDIFFSTORE</a>
	 */
	@Nullable
	Long zDiffStore(byte[] destKey, byte[]... sets);

	/**
	 * Intersect sorted {@code sets}.
	 *
	 * @param sets must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zinter">Redis Documentation: ZINTER</a>
	 */
	@Nullable
	Set<byte[]> zInter(byte[]... sets);

	/**
	 * Intersect sorted {@code sets}.
	 *
	 * @param sets must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zinter">Redis Documentation: ZINTER</a>
	 */
	@Nullable
	Set<Tuple> zInterWithScores(byte[]... sets);

	/**
	 * Intersect sorted {@code sets}.
	 *
	 * @param aggregate must not be {@literal null}.
	 * @param weights must not be {@literal null}.
	 * @param sets must not be {@literal null}.
	 * @return
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zinter">Redis Documentation: ZINTER</a>
	 */
	@Nullable
	default Set<Tuple> zInterWithScores(Aggregate aggregate, int[] weights, byte[]... sets) {
		return zInterWithScores(aggregate, Weights.of(weights), sets);
	}

	/**
	 * Intersect sorted {@code sets}.
	 *
	 * @param aggregate must not be {@literal null}.
	 * @param weights must not be {@literal null}.
	 * @param sets must not be {@literal null}.
	 * @return
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zinter">Redis Documentation: ZINTER</a>
	 */
	@Nullable
	Set<Tuple> zInterWithScores(Aggregate aggregate, Weights weights, byte[]... sets);

	/**
	 * Intersect sorted {@code sets} and store result in destination {@code destKey}.
	 *
	 * @param destKey must not be {@literal null}.
	 * @param sets must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zinterstore">Redis Documentation: ZINTERSTORE</a>
	 */
	@Nullable
	Long zInterStore(byte[] destKey, byte[]... sets);

	/**
	 * Intersect sorted {@code sets} and store result in destination {@code destKey}.
	 *
	 * @param destKey must not be {@literal null}.
	 * @param aggregate must not be {@literal null}.
	 * @param weights must not be {@literal null}.
	 * @param sets must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zinterstore">Redis Documentation: ZINTERSTORE</a>
	 */
	@Nullable
	default Long zInterStore(byte[] destKey, Aggregate aggregate, int[] weights, byte[]... sets) {
		return zInterStore(destKey, aggregate, Weights.of(weights), sets);
	}

	/**
	 * Intersect sorted {@code sets} and store result in destination {@code destKey}.
	 *
	 * @param destKey must not be {@literal null}.
	 * @param aggregate must not be {@literal null}.
	 * @param weights must not be {@literal null}.
	 * @param sets must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.1
	 * @see <a href="https://redis.io/commands/zinterstore">Redis Documentation: ZINTERSTORE</a>
	 */
	@Nullable
	Long zInterStore(byte[] destKey, Aggregate aggregate, Weights weights, byte[]... sets);

	/**
	 * Union sorted {@code sets}.
	 *
	 * @param sets must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zunion">Redis Documentation: ZUNION</a>
	 */
	@Nullable
	Set<byte[]> zUnion(byte[]... sets);

	/**
	 * Union sorted {@code sets}.
	 *
	 * @param sets must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zunion">Redis Documentation: ZUNION</a>
	 */
	@Nullable
	Set<Tuple> zUnionWithScores(byte[]... sets);

	/**
	 * Union sorted {@code sets}.
	 *
	 * @param aggregate must not be {@literal null}.
	 * @param weights must not be {@literal null}.
	 * @param sets must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zunion">Redis Documentation: ZUNION</a>
	 */
	@Nullable
	default Set<Tuple> zUnionWithScores(Aggregate aggregate, int[] weights, byte[]... sets) {
		return zUnionWithScores(aggregate, Weights.of(weights), sets);
	}

	/**
	 * Union sorted {@code sets}.
	 *
	 * @param aggregate must not be {@literal null}.
	 * @param weights must not be {@literal null}.
	 * @param sets must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zunion">Redis Documentation: ZUNION</a>
	 */
	@Nullable
	Set<Tuple> zUnionWithScores(Aggregate aggregate, Weights weights, byte[]... sets);

	/**
	 * Union sorted {@code sets}.
	 *
	 * @param sets must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zunionstore">Redis Documentation: ZUNIONSTORE</a>
	 */
	@Nullable
	Long zUnionStore(byte[] destKey, byte[]... sets);

	/**
	 * Union sorted {@code sets} and store result in destination {@code destKey}.
	 *
	 * @param destKey must not be {@literal null}.
	 * @param aggregate must not be {@literal null}.
	 * @param weights must not be {@literal null}.
	 * @param sets must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zunionstore">Redis Documentation: ZUNIONSTORE</a>
	 */
	@Nullable
	default Long zUnionStore(byte[] destKey, Aggregate aggregate, int[] weights, byte[]... sets) {
		return zUnionStore(destKey, aggregate, Weights.of(weights), sets);
	}

	/**
	 * Union sorted {@code sets} and store result in destination {@code destKey}.
	 *
	 * @param destKey must not be {@literal null}.
	 * @param aggregate must not be {@literal null}.
	 * @param weights must not be {@literal null}.
	 * @param sets must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.1
	 * @see <a href="https://redis.io/commands/zunionstore">Redis Documentation: ZUNIONSTORE</a>
	 */
	@Nullable
	Long zUnionStore(byte[] destKey, Aggregate aggregate, Weights weights, byte[]... sets);

	/**
	 * Use a {@link Cursor} to iterate over elements in sorted set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param options must not be {@literal null}.
	 * @return
	 * @since 1.4
	 * @see <a href="https://redis.io/commands/zscan">Redis Documentation: ZSCAN</a>
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
	 * @see <a href="https://redis.io/commands/zrangebyscore">Redis Documentation: ZRANGEBYSCORE</a>
	 * @deprecated since 3.0, use {@link #zRangeByScore(byte[], org.springframework.data.domain.Range)} instead.
	 */
	@Nullable
	@Deprecated
	default Set<byte[]> zRangeByScore(byte[] key, String min, String max) {
		return zRangeByScore(key, new Range().gte(min).lte(max).toRange());
	}

	/**
	 * Get elements where score is between {@code Range#min} and {@code Range#max} from sorted set.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 1.6
	 * @see <a href="https://redis.io/commands/zrangebyscore">Redis Documentation: ZRANGEBYSCORE</a>
	 */
	@Nullable
	default Set<byte[]> zRangeByScore(byte[] key, org.springframework.data.domain.Range<? extends Number> range) {
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
	 * @see <a href="https://redis.io/commands/zrangebyscore">Redis Documentation: ZRANGEBYSCORE</a>
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
	 * @see <a href="https://redis.io/commands/zrangebyscore">Redis Documentation: ZRANGEBYSCORE</a>
	 */
	@Nullable
	Set<byte[]> zRangeByScore(byte[] key, org.springframework.data.domain.Range<? extends Number> range,
			org.springframework.data.redis.connection.Limit limit);

	/**
	 * Get all the elements in the sorted set at {@literal key} in lexicographical ordering.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 1.6
	 * @see <a href="https://redis.io/commands/zrangebylex">Redis Documentation: ZRANGEBYLEX</a>
	 */
	@Nullable
	default Set<byte[]> zRangeByLex(byte[] key) {
		return zRangeByLex(key, org.springframework.data.domain.Range.unbounded());
	}

	/**
	 * Get all the elements in {@link org.springframework.data.domain.Range} from the sorted set at {@literal key} in
	 * lexicographical ordering.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 1.6
	 * @see <a href="https://redis.io/commands/zrangebylex">Redis Documentation: ZRANGEBYLEX</a>
	 */
	@Nullable
	default Set<byte[]> zRangeByLex(byte[] key, org.springframework.data.domain.Range<byte[]> range) {
		return zRangeByLex(key, range, Limit.unlimited());
	}

	/**
	 * Get all the elements in {@link org.springframework.data.domain.Range} from the sorted set at {@literal key} in
	 * lexicographical ordering. Result is limited via {@link Limit}.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @param limit must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 1.6
	 * @see <a href="https://redis.io/commands/zrangebylex">Redis Documentation: ZRANGEBYLEX</a>
	 */
	@Nullable
	Set<byte[]> zRangeByLex(byte[] key, org.springframework.data.domain.Range<byte[]> range,
			org.springframework.data.redis.connection.Limit limit);

	/**
	 * Get all the elements in the sorted set at {@literal key} in reversed lexicographical ordering.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.4
	 * @see <a href="https://redis.io/commands/zrevrangebylex">Redis Documentation: ZREVRANGEBYLEX</a>
	 */
	@Nullable
	default Set<byte[]> zRevRangeByLex(byte[] key) {
		return zRevRangeByLex(key, org.springframework.data.domain.Range.unbounded());
	}

	/**
	 * Get all the elements in {@link org.springframework.data.domain.Range} from the sorted set at {@literal key} in
	 * reversed lexicographical ordering.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.4
	 * @see <a href="https://redis.io/commands/zrevrangebylex">Redis Documentation: ZREVRANGEBYLEX</a>
	 */
	@Nullable
	default Set<byte[]> zRevRangeByLex(byte[] key, org.springframework.data.domain.Range<byte[]> range) {
		return zRevRangeByLex(key, range, org.springframework.data.redis.connection.Limit.unlimited());
	}

	/**
	 * Get all the elements in {@link org.springframework.data.domain.Range} from the sorted set at {@literal key} in
	 * reversed lexicographical ordering. Result is limited via {@link Limit}.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @param limit must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.4
	 * @see <a href="https://redis.io/commands/zrevrangebylex">Redis Documentation: ZREVRANGEBYLEX</a>
	 */
	@Nullable
	Set<byte[]> zRevRangeByLex(byte[] key, org.springframework.data.domain.Range<byte[]> range,
			org.springframework.data.redis.connection.Limit limit);

	/**
	 * This command is like ZRANGE , but stores the result in the {@literal dstKey} destination key.
	 *
	 * @param dstKey must not be {@literal null}.
	 * @param srcKey must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 3.0
	 * @see <a href="https://redis.io/commands/zrangestore">Redis Documentation: ZRANGESTORE</a>
	 */
	@Nullable
	default Long zRangeStoreByLex(byte[] dstKey, byte[] srcKey, org.springframework.data.domain.Range<byte[]> range) {
		return zRangeStoreByLex(dstKey, srcKey, range, org.springframework.data.redis.connection.Limit.unlimited());
	}

	/**
	 * This command is like ZRANGE , but stores the result in the {@literal dstKey} destination key.
	 *
	 * @param dstKey must not be {@literal null}.
	 * @param srcKey must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @param limit must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 3.0
	 * @see <a href="https://redis.io/commands/zrangestore">Redis Documentation: ZRANGESTORE</a>
	 */
	@Nullable
	Long zRangeStoreByLex(byte[] dstKey, byte[] srcKey, org.springframework.data.domain.Range<byte[]> range,
			org.springframework.data.redis.connection.Limit limit);

	/**
	 * This command is like ZRANGE … REV , but stores the result in the {@literal dstKey} destination key.
	 *
	 * @param dstKey must not be {@literal null}.
	 * @param srcKey must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 3.0
	 * @see <a href="https://redis.io/commands/zrangestore">Redis Documentation: ZRANGESTORE</a>
	 */
	@Nullable
	default Long zRangeStoreRevByLex(byte[] dstKey, byte[] srcKey, org.springframework.data.domain.Range<byte[]> range) {
		return zRangeStoreRevByLex(dstKey, srcKey, range, org.springframework.data.redis.connection.Limit.unlimited());
	}

	/**
	 * This command is like ZRANGE … REV , but stores the result in the {@literal dstKey} destination key.
	 *
	 * @param dstKey must not be {@literal null}.
	 * @param srcKey must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @param limit must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 3.0
	 * @see <a href="https://redis.io/commands/zrangestore">Redis Documentation: ZRANGESTORE</a>
	 */
	@Nullable
	Long zRangeStoreRevByLex(byte[] dstKey, byte[] srcKey, org.springframework.data.domain.Range<byte[]> range,
			org.springframework.data.redis.connection.Limit limit);

	/**
	 * This command is like ZRANGE, but stores the result in the {@literal dstKey} destination key.
	 *
	 * @param dstKey must not be {@literal null}.
	 * @param srcKey must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 3.0
	 * @see <a href="https://redis.io/commands/zrangestore">Redis Documentation: ZRANGESTORE</a>
	 */
	@Nullable
	default Long zRangeStoreByScore(byte[] dstKey, byte[] srcKey,
			org.springframework.data.domain.Range<? extends Number> range) {
		return zRangeStoreByScore(dstKey, srcKey, range, org.springframework.data.redis.connection.Limit.unlimited());
	}

	/**
	 * This command is like ZRANGE, but stores the result in the {@literal dstKey} destination key.
	 *
	 * @param dstKey must not be {@literal null}.
	 * @param srcKey must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @param limit must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 3.0
	 * @see <a href="https://redis.io/commands/zrangestore">Redis Documentation: ZRANGESTORE</a>
	 */
	@Nullable
	Long zRangeStoreByScore(byte[] dstKey, byte[] srcKey, org.springframework.data.domain.Range<? extends Number> range,
			org.springframework.data.redis.connection.Limit limit);

	/**
	 * This command is like ZRANGE … REV, but stores the result in the {@literal dstKey} destination key.
	 *
	 * @param dstKey must not be {@literal null}.
	 * @param srcKey must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 3.0
	 * @see <a href="https://redis.io/commands/zrangestore">Redis Documentation: ZRANGESTORE</a>
	 */
	@Nullable
	default Long zRangeStoreRevByScore(byte[] dstKey, byte[] srcKey,
			org.springframework.data.domain.Range<? extends Number> range) {
		return zRangeStoreRevByScore(dstKey, srcKey, range, org.springframework.data.redis.connection.Limit.unlimited());
	}

	/**
	 * This command is like ZRANGE … REV, but stores the result in the {@literal dstKey} destination key.
	 *
	 * @param dstKey must not be {@literal null}.
	 * @param srcKey must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @param limit must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 3.0
	 * @see <a href="https://redis.io/commands/zrangestore">Redis Documentation: ZRANGESTORE</a>
	 */
	@Nullable
	Long zRangeStoreRevByScore(byte[] dstKey, byte[] srcKey,
			org.springframework.data.domain.Range<? extends Number> range,
			org.springframework.data.redis.connection.Limit limit);

}
