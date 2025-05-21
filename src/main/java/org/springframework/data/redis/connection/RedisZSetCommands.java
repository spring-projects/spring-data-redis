/*
 * Copyright 2011-2025 the original author or authors.
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

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.NullUnmarked;
import org.jspecify.annotations.Nullable;
import org.springframework.data.redis.connection.zset.Aggregate;
import org.springframework.data.redis.connection.zset.Tuple;
import org.springframework.data.redis.connection.zset.Weights;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;
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
@NullUnmarked
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
	@NullMarked
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
		public @Nullable Boundary getMin() {
			return min;
		}

		/**
		 * @return {@literal null} if not set.
		 */
		public @Nullable Boundary getMax() {
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

			public @Nullable Object getValue() {
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
		public <T> org.springframework.data.domain.Range<@NonNull T> toRange() {

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
	@NullMarked
	class Limit extends org.springframework.data.redis.connection.Limit {

	}

	/**
	 * {@code ZADD} specific arguments. <br />
	 * Looking of the {@code INCR} flag? Use the {@code ZINCRBY} operation instead.
	 *
	 * @since 2.5
	 * @see <a href="https://redis.io/commands/zadd">Redis Documentation: ZADD</a>
	 */
	@NullMarked
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
			return flags.isEmpty();
		}

		@Override
		public boolean equals(@Nullable Object o) {

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
	default Boolean zAdd(byte @NonNull [] key, double score, byte @NonNull [] value) {
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
	Boolean zAdd(byte @NonNull [] key, double score, byte @NonNull [] value, @NonNull ZAddArgs args);

	/**
	 * Add {@code tuples} to a sorted set at {@code key}, or update its {@code score} if it already exists.
	 *
	 * @param key must not be {@literal null}.
	 * @param tuples must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zadd">Redis Documentation: ZADD</a>
	 */
	default Long zAdd(byte @NonNull [] key, @NonNull Set<@NonNull Tuple> tuples) {
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
	Long zAdd(byte @NonNull [] key, @NonNull Set<@NonNull Tuple> tuples, @NonNull ZAddArgs args);

	/**
	 * Remove {@code values} from sorted set. Return number of removed elements.
	 *
	 * @param key must not be {@literal null}.
	 * @param values must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zrem">Redis Documentation: ZREM</a>
	 */
	Long zRem(byte @NonNull [] key, byte @NonNull [] @Nullable... values);

	/**
	 * Increment the score of element with {@code value} in sorted set by {@code increment}.
	 *
	 * @param key must not be {@literal null}.
	 * @param increment
	 * @param value the value.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zincrby">Redis Documentation: ZINCRBY</a>
	 */
	Double zIncrBy(byte @NonNull [] key, double increment, byte @NonNull [] value);

	/**
	 * Get random element from sorted set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return can be {@literal null}.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zrandmember">Redis Documentation: ZRANDMEMBER</a>
	 */
	byte @NonNull [] zRandMember(byte @NonNull [] key);

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
	List<byte @NonNull []> zRandMember(byte @NonNull [] key, long count);

	/**
	 * Get random element from sorted set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return can be {@literal null}.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zrandmember">Redis Documentation: ZRANDMEMBER</a>
	 */
	Tuple zRandMemberWithScore(byte @NonNull [] key);

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
	List<@NonNull Tuple> zRandMemberWithScore(byte @NonNull [] key, long count);

	/**
	 * Determine the index of element with {@code value} in a sorted set.
	 *
	 * @param key must not be {@literal null}.
	 * @param value the value. Must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zrank">Redis Documentation: ZRANK</a>
	 */
	Long zRank(byte @NonNull [] key, byte @NonNull [] value);

	/**
	 * Determine the index of element with {@code value} in a sorted set when scored high to low.
	 *
	 * @param key must not be {@literal null}.
	 * @param value the value.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zrevrank">Redis Documentation: ZREVRANK</a>
	 */
	Long zRevRank(byte @NonNull [] key, byte @NonNull [] value);

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
	Set<byte @NonNull []> zRange(byte @NonNull [] key, long start, long end);

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
	Set<@NonNull Tuple> zRangeWithScores(byte @NonNull [] key, long start, long end);

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
	default Set<byte @NonNull []> zRangeByScore(byte @NonNull [] key, double min, double max) {
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
	default Set<@NonNull Tuple> zRangeByScoreWithScores(byte @NonNull [] key,
			org.springframework.data.domain.@NonNull Range<? extends @NonNull Number> range) {
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
	default Set<@NonNull Tuple> zRangeByScoreWithScores(byte @NonNull [] key, double min, double max) {
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
	default Set<byte @NonNull []> zRangeByScore(byte @NonNull [] key, double min, double max, long offset, long count) {
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
	default Set<@NonNull Tuple> zRangeByScoreWithScores(byte @NonNull [] key, double min, double max, long offset,
			long count) {
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
	Set<@NonNull Tuple> zRangeByScoreWithScores(byte @NonNull [] key,
			org.springframework.data.domain.@NonNull Range<? extends @NonNull Number> range,
			org.springframework.data.redis.connection.@NonNull Limit limit);

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
	Set<byte @NonNull []> zRevRange(byte @NonNull [] key, long start, long end);

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
	Set<@NonNull Tuple> zRevRangeWithScores(byte @NonNull [] key, long start, long end);

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
	default Set<byte @NonNull []> zRevRangeByScore(byte @NonNull [] key, double min, double max) {
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
	default Set<byte @NonNull []> zRevRangeByScore(byte @NonNull [] key,
			org.springframework.data.domain.@NonNull Range<? extends @NonNull Number> range) {
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
	default Set<@NonNull Tuple> zRevRangeByScoreWithScores(byte @NonNull [] key, double min, double max) {
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
	default Set<byte @NonNull []> zRevRangeByScore(byte @NonNull [] key, double min, double max, long offset,
			long count) {

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
	Set<byte @NonNull []> zRevRangeByScore(byte @NonNull [] key,
			org.springframework.data.domain.@NonNull Range<? extends @NonNull Number> range,
			org.springframework.data.redis.connection.@NonNull Limit limit);

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
	default Set<@NonNull Tuple> zRevRangeByScoreWithScores(byte @NonNull [] key, double min, double max, long offset,
			long count) {

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
	default Set<@NonNull Tuple> zRevRangeByScoreWithScores(byte @NonNull [] key,
			org.springframework.data.domain.@NonNull Range<? extends @NonNull Number> range) {
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
	Set<@NonNull Tuple> zRevRangeByScoreWithScores(byte @NonNull [] key,
			org.springframework.data.domain.@NonNull Range<? extends @NonNull Number> range,
			org.springframework.data.redis.connection.@NonNull Limit limit);

	/**
	 * Count number of elements within sorted set with scores between {@code min} and {@code max}.
	 *
	 * @param key must not be {@literal null}.
	 * @param min
	 * @param max
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zcount">Redis Documentation: ZCOUNT</a>
	 */
	default Long zCount(byte @NonNull [] key, double min, double max) {
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
	Long zCount(byte @NonNull [] key, org.springframework.data.domain.@NonNull Range<? extends @NonNull Number> range);

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
	Long zLexCount(byte @NonNull [] key, org.springframework.data.domain.@NonNull Range<byte @NonNull []> range);

	/**
	 * Remove and return the value with its score having the lowest score from sorted set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when the sorted set is empty or used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zpopmin">Redis Documentation: ZPOPMIN</a>
	 * @since 2.6
	 */
	Tuple zPopMin(byte @NonNull [] key);

	/**
	 * Remove and return {@code count} values with their score having the lowest score from sorted set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param count number of elements to pop.
	 * @return {@literal null} when the sorted set is empty or used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zpopmin">Redis Documentation: ZPOPMIN</a>
	 * @since 2.6
	 */
	Set<@NonNull Tuple> zPopMin(byte @NonNull [] key, long count);

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
	Tuple bZPopMin(byte @NonNull [] key, long timeout, @NonNull TimeUnit unit);

	/**
	 * Remove and return the value with its score having the highest score from sorted set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when the sorted set is empty or used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zpopmax">Redis Documentation: ZPOPMAX</a>
	 * @since 2.6
	 */
	Tuple zPopMax(byte @NonNull [] key);

	/**
	 * Remove and return {@code count} values with their score having the highest score from sorted set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param count number of elements to pop.
	 * @return {@literal null} when the sorted set is empty or used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zpopmax">Redis Documentation: ZPOPMAX</a>
	 * @since 2.6
	 */
	Set<@NonNull Tuple> zPopMax(byte @NonNull [] key, long count);

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
	Tuple bZPopMax(byte @NonNull [] key, long timeout, @NonNull TimeUnit unit);

	/**
	 * Get the size of sorted set with {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zcard">Redis Documentation: ZCARD</a>
	 */
	Long zCard(byte @NonNull [] key);

	/**
	 * Get the score of element with {@code value} from sorted set with key {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value the value.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zscore">Redis Documentation: ZSCORE</a>
	 */
	Double zScore(byte @NonNull [] key, byte @NonNull [] value);

	/**
	 * Get the scores of elements with {@code values} from sorted set with key {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param values the values.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zmscore">Redis Documentation: ZMSCORE</a>
	 * @since 2.6
	 */
	List<@NonNull Double> zMScore(byte @NonNull [] key, byte @NonNull [] @NonNull... values);

	/**
	 * Remove elements in range between {@code start} and {@code end} from sorted set with {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param start
	 * @param end
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zremrangebyrank">Redis Documentation: ZREMRANGEBYRANK</a>
	 */
	Long zRemRange(byte @NonNull [] key, long start, long end);

	/**
	 * Remove all elements between the lexicographical {@link org.springframework.data.domain.Range}.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return the number of elements removed, or {@literal null} when used in pipeline / transaction.
	 * @since 2.5
	 * @see <a href="https://redis.io/commands/zremrangebylex">Redis Documentation: ZREMRANGEBYLEX</a>
	 */
	Long zRemRangeByLex(byte @NonNull [] key, org.springframework.data.domain.@NonNull Range<byte @NonNull []> range);

	/**
	 * Remove elements with scores between {@code min} and {@code max} from sorted set with {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param min
	 * @param max
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zremrangebyscore">Redis Documentation: ZREMRANGEBYSCORE</a>
	 */
	default Long zRemRangeByScore(byte @NonNull [] key, double min, double max) {
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
	Long zRemRangeByScore(byte @NonNull [] key,
			org.springframework.data.domain.@NonNull Range<? extends @NonNull Number> range);

	/**
	 * Diff sorted {@code sets}.
	 *
	 * @param sets must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zdiff">Redis Documentation: ZDIFF</a>
	 */
	Set<byte[]> zDiff(byte @NonNull [] @Nullable... sets);

	/**
	 * Diff sorted {@code sets}.
	 *
	 * @param sets must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zdiff">Redis Documentation: ZDIFF</a>
	 */
	Set<@NonNull Tuple> zDiffWithScores(byte @NonNull [] @NonNull... sets);

	/**
	 * Diff sorted {@code sets} and store result in destination {@code destKey}.
	 *
	 * @param destKey must not be {@literal null}.
	 * @param sets must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zdiffstore">Redis Documentation: ZDIFFSTORE</a>
	 */
	Long zDiffStore(byte @NonNull [] destKey, byte @NonNull [] @NonNull... sets);

	/**
	 * Intersect sorted {@code sets}.
	 *
	 * @param sets must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zinter">Redis Documentation: ZINTER</a>
	 */
	Set<byte @NonNull []> zInter(byte @NonNull [] @NonNull... sets);

	/**
	 * Intersect sorted {@code sets}.
	 *
	 * @param sets must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zinter">Redis Documentation: ZINTER</a>
	 */
	Set<@NonNull Tuple> zInterWithScores(byte @NonNull [] @NonNull... sets);

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
	default Set<@NonNull Tuple> zInterWithScores(@NonNull Aggregate aggregate, int[] weights,
			byte @NonNull [] @NonNull... sets) {
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
	Set<@NonNull Tuple> zInterWithScores(@NonNull Aggregate aggregate, @NonNull Weights weights,
			byte @NonNull [] @NonNull... sets);

	/**
	 * Intersect sorted {@code sets} and store result in destination {@code destKey}.
	 *
	 * @param destKey must not be {@literal null}.
	 * @param sets must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zinterstore">Redis Documentation: ZINTERSTORE</a>
	 */
	Long zInterStore(byte @NonNull [] destKey, byte @NonNull [] @NonNull... sets);

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
	default Long zInterStore(byte @NonNull [] destKey, @NonNull Aggregate aggregate, int @NonNull [] weights,
			byte @NonNull [] @NonNull... sets) {
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
	Long zInterStore(byte @NonNull [] destKey, @NonNull Aggregate aggregate, @NonNull Weights weights,
			byte @NonNull [] @NonNull... sets);

	/**
	 * Union sorted {@code sets}.
	 *
	 * @param sets must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zunion">Redis Documentation: ZUNION</a>
	 */
	Set<byte @NonNull []> zUnion(byte @NonNull [] @NonNull... sets);

	/**
	 * Union sorted {@code sets}.
	 *
	 * @param sets must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zunion">Redis Documentation: ZUNION</a>
	 */
	Set<@NonNull Tuple> zUnionWithScores(byte @NonNull [] @NonNull... sets);

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
	default @Nullable Set<@NonNull Tuple> zUnionWithScores(@NonNull Aggregate aggregate, int @NonNull [] weights,
			byte @NonNull [] @NonNull... sets) {
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
	Set<@NonNull Tuple> zUnionWithScores(Aggregate aggregate, Weights weights, byte @NonNull [] @NonNull... sets);

	/**
	 * Union sorted {@code sets}.
	 *
	 * @param sets must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zunionstore">Redis Documentation: ZUNIONSTORE</a>
	 */
	Long zUnionStore(byte @NonNull [] destKey, byte @NonNull [] @NonNull... sets);

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
	default Long zUnionStore(byte @NonNull [] destKey, @NonNull Aggregate aggregate, int @NonNull [] weights,
			byte @NonNull [] @NonNull... sets) {
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
	Long zUnionStore(byte @NonNull [] destKey, @NonNull Aggregate aggregate, @NonNull Weights weights,
			byte @NonNull [] @NonNull... sets);

	/**
	 * Use a {@link Cursor} to iterate over elements in sorted set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param options must not be {@literal null}.
	 * @return
	 * @since 1.4
	 * @see <a href="https://redis.io/commands/zscan">Redis Documentation: ZSCAN</a>
	 */
	Cursor<@NonNull Tuple> zScan(byte @NonNull [] key, @NonNull ScanOptions options);

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
	@Deprecated
	default Set<byte @NonNull []> zRangeByScore(byte @NonNull [] key, @Nullable String min, @Nullable String max) {
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
	default Set<byte @NonNull []> zRangeByScore(byte @NonNull [] key,
			org.springframework.data.domain.@NonNull Range<? extends @NonNull Number> range) {
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
	Set<byte @NonNull []> zRangeByScore(byte @NonNull [] key, @Nullable String min, @Nullable String max, long offset,
			long count);

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
	Set<byte @NonNull []> zRangeByScore(byte @NonNull [] key,
			org.springframework.data.domain.@NonNull Range<? extends @NonNull Number> range,
			org.springframework.data.redis.connection.@NonNull Limit limit);

	/**
	 * Get all the elements in the sorted set at {@literal key} in lexicographical ordering.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 1.6
	 * @see <a href="https://redis.io/commands/zrangebylex">Redis Documentation: ZRANGEBYLEX</a>
	 */
	default Set<byte @NonNull []> zRangeByLex(byte @NonNull [] key) {
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
	default Set<byte @NonNull []> zRangeByLex(byte @NonNull [] key,
			org.springframework.data.domain.@NonNull Range<byte @NonNull []> range) {
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
	Set<byte @NonNull []> zRangeByLex(byte @NonNull [] key,
			org.springframework.data.domain.@NonNull Range<byte @NonNull []> range,
			org.springframework.data.redis.connection.@NonNull Limit limit);

	/**
	 * Get all the elements in the sorted set at {@literal key} in reversed lexicographical ordering.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.4
	 * @see <a href="https://redis.io/commands/zrevrangebylex">Redis Documentation: ZREVRANGEBYLEX</a>
	 */
	default Set<byte @NonNull []> zRevRangeByLex(byte @NonNull [] key) {
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
	default Set<byte @NonNull []> zRevRangeByLex(byte @NonNull [] key,
			org.springframework.data.domain.@NonNull Range<byte @NonNull []> range) {
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
	Set<byte @NonNull []> zRevRangeByLex(byte @NonNull [] key,
			org.springframework.data.domain.@NonNull Range<byte @NonNull []> range,
			org.springframework.data.redis.connection.@NonNull Limit limit);

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
	default Long zRangeStoreByLex(byte @NonNull [] dstKey, byte @NonNull [] srcKey,
			org.springframework.data.domain.@NonNull Range<byte @NonNull []> range) {
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
	Long zRangeStoreByLex(byte @NonNull [] dstKey, byte @NonNull [] srcKey,
			org.springframework.data.domain.@NonNull Range<byte @NonNull []> range,
			org.springframework.data.redis.connection.@NonNull Limit limit);

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
	default Long zRangeStoreRevByLex(byte @NonNull [] dstKey, byte @NonNull [] srcKey,
			org.springframework.data.domain.@NonNull Range<byte @NonNull []> range) {
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
	Long zRangeStoreRevByLex(byte @NonNull [] dstKey, byte @NonNull [] srcKey,
			org.springframework.data.domain.@NonNull Range<byte @NonNull []> range,
			org.springframework.data.redis.connection.@NonNull Limit limit);

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
	default Long zRangeStoreByScore(byte @NonNull [] dstKey, byte @NonNull [] srcKey,
			org.springframework.data.domain.@NonNull Range<? extends @NonNull Number> range) {
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
	Long zRangeStoreByScore(byte @NonNull [] dstKey, byte @NonNull [] srcKey,
			org.springframework.data.domain.@NonNull Range<? extends @NonNull Number> range,
			org.springframework.data.redis.connection.@NonNull Limit limit);

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
	default Long zRangeStoreRevByScore(byte @NonNull [] dstKey, byte @NonNull [] srcKey,
			org.springframework.data.domain.@NonNull Range<? extends @NonNull Number> range) {
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
	Long zRangeStoreRevByScore(byte @NonNull [] dstKey, byte @NonNull [] srcKey,
			org.springframework.data.domain.@NonNull Range<? extends @NonNull Number> range,
			org.springframework.data.redis.connection.@NonNull Limit limit);

}
