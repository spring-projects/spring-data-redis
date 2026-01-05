/*
 * Copyright 2011-present the original author or authors.
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
package org.springframework.data.redis.core;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.NullUnmarked;
import org.jspecify.annotations.Nullable;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.Limit;
import org.springframework.data.redis.connection.zset.Aggregate;
import org.springframework.data.redis.connection.zset.Tuple;
import org.springframework.data.redis.connection.zset.Weights;
import org.springframework.util.Assert;

/**
 * Redis ZSet/sorted set specific operations.
 *
 * @author Costin Leau
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Rosty Kerei
 * @author Wongoo (望哥)
 * @author Andrey Shlykov
 * @author Shyngys Sapraliyev
 * @author Gunha Hwang
 */
@NullUnmarked
public interface ZSetOperations<K, V> {

	/**
	 * Typed ZSet tuple.
	 */
	@NullMarked
	interface TypedTuple<V> extends Comparable<TypedTuple<@Nullable V>> {

		@Nullable
		V getValue();

		@Nullable
		Double getScore();

		/**
		 * Create a new {@link TypedTuple}.
		 *
		 * @param value must not be {@literal null}.
		 * @param score can be {@literal null}.
		 * @param <V>
		 * @return new instance of {@link TypedTuple}.
		 * @since 2.5
		 */
		static <V> TypedTuple<V> of(@NonNull V value, @Nullable Double score) {
			return new DefaultTypedTuple<>(value, score);
		}
	}

	/**
	 * Add {@code value} to a sorted set at {@code key}, or update its {@code score} if it already exists.
	 *
	 * @param key must not be {@literal null}.
	 * @param value the value.
	 * @param score the score.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zadd">Redis Documentation: ZADD</a>
	 */
	Boolean add(@NonNull K key, @NonNull V value, double score);

	/**
	 * Add {@code value} to a sorted set at {@code key} if it does not already exists.
	 *
	 * @param key must not be {@literal null}.
	 * @param value the value.
	 * @param score the score.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.5
	 * @see <a href="https://redis.io/commands/zadd">Redis Documentation: ZADD NX</a>
	 */
	Boolean addIfAbsent(@NonNull K key, @NonNull V value, double score);

	/**
	 * Add {@code tuples} to a sorted set at {@code key}, or update its {@code score} if it already exists.
	 *
	 * @param key must not be {@literal null}.
	 * @param tuples must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zadd">Redis Documentation: ZADD</a>
	 */
	Long add(@NonNull K key, @NonNull Set<@NonNull TypedTuple<V>> tuples);

	/**
	 * Add {@code tuples} to a sorted set at {@code key} if it does not already exists.
	 *
	 * @param key must not be {@literal null}.
	 * @param tuples must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.5
	 * @see <a href="https://redis.io/commands/zadd">Redis Documentation: ZADD NX</a>
	 */
	Long addIfAbsent(@NonNull K key, @NonNull Set<@NonNull TypedTuple<V>> tuples);

	/**
	 * Remove {@code values} from sorted set. Return number of removed elements.
	 *
	 * @param key must not be {@literal null}.
	 * @param values must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zrem">Redis Documentation: ZREM</a>
	 */
	Long remove(@NonNull K key, @NonNull Object @NonNull... values);

	/**
	 * Increment the score of element with {@code value} in sorted set by {@code increment}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value the value.
	 * @param delta the delta to add. Can be negative.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zincrby">Redis Documentation: ZINCRBY</a>
	 */
	Double incrementScore(@NonNull K key, @NonNull V value, double delta);

	/**
	 * Get random element from set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zrandmember">Redis Documentation: ZRANDMEMBER</a>
	 */
	V randomMember(@NonNull K key);

	/**
	 * Get {@code count} distinct random elements from set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param count number of members to return.
	 * @return empty {@link Set} if {@code key} does not exist.
	 * @throws IllegalArgumentException if count is negative.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zrandmember">Redis Documentation: ZRANDMEMBER</a>
	 */
	Set<V> distinctRandomMembers(@NonNull K key, long count);

	/**
	 * Get {@code count} random elements from set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param count number of members to return.
	 * @return empty {@link List} if {@code key} does not exist or {@literal null} when used in pipeline / transaction.
	 * @throws IllegalArgumentException if count is negative.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zrandmember">Redis Documentation: ZRANDMEMBER</a>
	 */
	List<V> randomMembers(@NonNull K key, long count);

	/**
	 * Get random element with its score from set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zrandmember">Redis Documentation: ZRANDMEMBER</a>
	 */
	TypedTuple<V> randomMemberWithScore(@NonNull K key);

	/**
	 * Get {@code count} distinct random elements with their score from set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param count number of members to return.
	 * @return empty {@link Set} if {@code key} does not exist.
	 * @throws IllegalArgumentException if count is negative.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zrandmember">Redis Documentation: ZRANDMEMBER</a>
	 */
	Set<@NonNull TypedTuple<V>> distinctRandomMembersWithScore(@NonNull K key, long count);

	/**
	 * Get {@code count} random elements with their score from set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param count number of members to return.
	 * @return empty {@link List} if {@code key} does not exist or {@literal null} when used in pipeline / transaction.
	 * @throws IllegalArgumentException if count is negative.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zrandmember">Redis Documentation: ZRANDMEMBER</a>
	 */
	List<TypedTuple<V>> randomMembersWithScore(@NonNull K key, long count);

	/**
	 * Determine the index of element with {@code value} in a sorted set.
	 *
	 * @param key must not be {@literal null}.
	 * @param value the value.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zrank">Redis Documentation: ZRANK</a>
	 */
	Long rank(@NonNull K key, @NonNull Object value);

	/**
	 * Determine the index of element with {@code value} in a sorted set when scored high to low.
	 *
	 * @param key must not be {@literal null}.
	 * @param value the value.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zrevrank">Redis Documentation: ZREVRANK</a>
	 */
	Long reverseRank(@NonNull K key, @NonNull Object value);

	/**
	 * Get elements between {@code start} and {@code end} from sorted set.
	 *
	 * @param key must not be {@literal null}.
	 * @param start
	 * @param end
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zrange">Redis Documentation: ZRANGE</a>
	 */
	Set<V> range(@NonNull K key, long start, long end);

	/**
	 * Get set of {@link Tuple}s between {@code start} and {@code end} from sorted set.
	 *
	 * @param key must not be {@literal null}.
	 * @param start
	 * @param end
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zrange">Redis Documentation: ZRANGE</a>
	 */
	Set<@NonNull TypedTuple<V>> rangeWithScores(@NonNull K key, long start, long end);

	/**
	 * Get elements where score is between {@code min} and {@code max} from sorted set.
	 *
	 * @param key must not be {@literal null}.
	 * @param min
	 * @param max
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zrangebyscore">Redis Documentation: ZRANGEBYSCORE</a>
	 */
	Set<V> rangeByScore(@NonNull K key, double min, double max);

	/**
	 * Get set of {@link Tuple}s where score is between {@code min} and {@code max} from sorted set.
	 *
	 * @param key must not be {@literal null}.
	 * @param min
	 * @param max
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zrangebyscore">Redis Documentation: ZRANGEBYSCORE</a>
	 */
	Set<@NonNull TypedTuple<V>> rangeByScoreWithScores(@NonNull K key, double min, double max);

	/**
	 * Get elements in range from {@code start} to {@code end} where score is between {@code min} and {@code max} from
	 * sorted set.
	 *
	 * @param key must not be {@literal null}.
	 * @param min
	 * @param max
	 * @param offset
	 * @param count
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zrangebyscore">Redis Documentation: ZRANGEBYSCORE</a>
	 */
	Set<V> rangeByScore(@NonNull K key, double min, double max, long offset, long count);

	/**
	 * Get set of {@link Tuple}s in range from {@code start} to {@code end} where score is between {@code min} and
	 * {@code max} from sorted set.
	 *
	 * @param key
	 * @param min
	 * @param max
	 * @param offset
	 * @param count
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zrangebyscore">Redis Documentation: ZRANGEBYSCORE</a>
	 */
	Set<@NonNull TypedTuple<V>> rangeByScoreWithScores(@NonNull K key, double min, double max, long offset, long count);

	/**
	 * Get elements in range from {@code start} to {@code end} from sorted set ordered from high to low.
	 *
	 * @param key must not be {@literal null}.
	 * @param start
	 * @param end
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zrevrange">Redis Documentation: ZREVRANGE</a>
	 */
	Set<V> reverseRange(@NonNull K key, long start, long end);

	/**
	 * Get set of {@link Tuple}s in range from {@code start} to {@code end} from sorted set ordered from high to low.
	 *
	 * @param key must not be {@literal null}.
	 * @param start
	 * @param end
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zrevrange">Redis Documentation: ZREVRANGE</a>
	 */
	Set<@NonNull TypedTuple<V>> reverseRangeWithScores(@NonNull K key, long start, long end);

	/**
	 * Get elements where score is between {@code min} and {@code max} from sorted set ordered from high to low.
	 *
	 * @param key must not be {@literal null}.
	 * @param min
	 * @param max
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zrevrangebyscore">Redis Documentation: ZREVRANGEBYSCORE</a>
	 */
	Set<V> reverseRangeByScore(@NonNull K key, double min, double max);

	/**
	 * Get set of {@link Tuple} where score is between {@code min} and {@code max} from sorted set ordered from high to
	 * low.
	 *
	 * @param key must not be {@literal null}.
	 * @param min
	 * @param max
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zrevrangebyscore">Redis Documentation: ZREVRANGEBYSCORE</a>
	 */
	Set<@NonNull TypedTuple<V>> reverseRangeByScoreWithScores(@NonNull K key, double min, double max);

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
	Set<V> reverseRangeByScore(@NonNull K key, double min, double max, long offset, long count);

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
	Set<@NonNull TypedTuple<V>> reverseRangeByScoreWithScores(@NonNull K key, double min, double max, long offset,
			long count);

	/**
	 * Count number of elements within sorted set with scores between {@code min} and {@code max}.
	 *
	 * @param key must not be {@literal null}.
	 * @param min
	 * @param max
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zcount">Redis Documentation: ZCOUNT</a>
	 */
	Long count(@NonNull K key, double min, double max);

	/**
	 * Count number of elements within sorted set with value between {@link Range#getLowerBound()} and
	 * {@link Range#getUpperBound()} applying lexicographical ordering.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 3.0
	 * @see <a href="https://redis.io/commands/zlexcount">Redis Documentation: ZLEXCOUNT</a>
	 */
	Long lexCount(@NonNull K key, @NonNull Range<String> range);

	/**
	 * Remove and return the value with its score having the lowest score from sorted set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when the sorted set is empty or used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zpopmin">Redis Documentation: ZPOPMIN</a>
	 * @since 2.6
	 */
	TypedTuple<V> popMin(@NonNull K key);

	/**
	 * Remove and return {@code count} values with their score having the lowest score from sorted set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param count number of elements to pop.
	 * @return {@literal null} when the sorted set is empty or used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zpopmin">Redis Documentation: ZPOPMIN</a>
	 * @since 2.6
	 */
	Set<@NonNull TypedTuple<V>> popMin(@NonNull K key, long count);

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
	TypedTuple<V> popMin(@NonNull K key, long timeout, @NonNull TimeUnit unit);

	/**
	 * Remove and return the value with its score having the lowest score from sorted set at {@code key}. <br />
	 * <b>Blocks connection</b> until element available or {@code timeout} reached.
	 *
	 * @param key must not be {@literal null}.
	 * @param timeout must not be {@literal null}.
	 * @return can be {@literal null}.
	 * @throws IllegalArgumentException if the timeout is {@literal null} or negative.
	 * @see <a href="https://redis.io/commands/bzpopmin">Redis Documentation: BZPOPMIN</a>
	 * @since 2.6
	 */
	default TypedTuple<@NonNull V> popMin(@NonNull K key, @NonNull Duration timeout) {

		Assert.notNull(timeout, "Timeout must not be null");
		Assert.isTrue(!timeout.isNegative(), "Timeout must not be negative");

		return popMin(key, TimeoutUtils.toSeconds(timeout), TimeUnit.SECONDS);
	}

	/**
	 * Remove and return the value with its score having the highest score from sorted set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when the sorted set is empty or used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zpopmax">Redis Documentation: ZPOPMAX</a>
	 * @since 2.6
	 */
	TypedTuple<V> popMax(@NonNull K key);

	/**
	 * Remove and return {@code count} values with their score having the highest score from sorted set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param count number of elements to pop.
	 * @return {@literal null} when the sorted set is empty or used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zpopmax">Redis Documentation: ZPOPMAX</a>
	 * @since 2.6
	 */
	Set<TypedTuple<V>> popMax(@NonNull K key, long count);

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
	TypedTuple<V> popMax(@NonNull K key, long timeout, @NonNull TimeUnit unit);

	/**
	 * Remove and return the value with its score having the highest score from sorted set at {@code key}. <br />
	 * <b>Blocks connection</b> until element available or {@code timeout} reached.
	 *
	 * @param key must not be {@literal null}.
	 * @param timeout must not be {@literal null}.
	 * @return can be {@literal null}.
	 * @throws IllegalArgumentException if the timeout is {@literal null} or negative.
	 * @see <a href="https://redis.io/commands/bzpopmax">Redis Documentation: BZPOPMAX</a>
	 * @since 2.6
	 */
	default TypedTuple<V> popMax(@NonNull K key, @NonNull Duration timeout) {

		Assert.notNull(timeout, "Timeout must not be null");
		Assert.isTrue(!timeout.isNegative(), "Timeout must not be negative");

		return popMax(key, TimeoutUtils.toSeconds(timeout), TimeUnit.SECONDS);
	}

	/**
	 * Returns the number of elements of the sorted set stored with given {@code key}.
	 *
	 * @see #zCard(Object)
	 * @param key
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zcard">Redis Documentation: ZCARD</a>
	 */
	Long size(@NonNull K key);

	/**
	 * Get the size of sorted set with {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 1.3
	 * @see <a href="https://redis.io/commands/zcard">Redis Documentation: ZCARD</a>
	 */
	Long zCard(@NonNull K key);

	/**
	 * Get the score of element with {@code value} from sorted set with key {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value the value.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zscore">Redis Documentation: ZSCORE</a>
	 */
	Double score(@NonNull K key, Object value);

	/**
	 * Get the scores of elements with {@code values} from sorted set with key {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param o the values.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zmscore">Redis Documentation: ZMSCORE</a>
	 * @since 2.6
	 */
	List<Double> score(@NonNull K key, Object... o);

	/**
	 * Remove elements in range between {@code start} and {@code end} from sorted set with {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param start
	 * @param end
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zremrangebyrank">Redis Documentation: ZREMRANGEBYRANK</a>
	 */
	Long removeRange(@NonNull K key, long start, long end);

	/**
	 * Remove elements in {@link Range} from sorted set with {@literal key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 3.0
	 * @see <a href="https://redis.io/commands/zremrangebylex">Redis Documentation: ZREMRANGEBYLEX</a>
	 */
	Long removeRangeByLex(@NonNull K key, @NonNull Range<String> range);

	/**
	 * Remove elements with scores between {@code min} and {@code max} from sorted set with {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param min
	 * @param max
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zremrangebyscore">Redis Documentation: ZREMRANGEBYSCORE</a>
	 */
	Long removeRangeByScore(@NonNull K key, double min, double max);

	/**
	 * Diff sorted {@code sets}.
	 *
	 * @param key must not be {@literal null}.
	 * @param otherKey must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zdiff">Redis Documentation: ZDIFF</a>
	 */
	default Set<V> difference(@NonNull K key, @NonNull K otherKey) {
		return difference(key, Collections.singleton(otherKey));
	}

	/**
	 * Diff sorted {@code sets}.
	 *
	 * @param key must not be {@literal null}.
	 * @param otherKeys must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zdiff">Redis Documentation: ZDIFF</a>
	 */
	Set<V> difference(@NonNull K key, @NonNull Collection<@NonNull K> otherKeys);

	/**
	 * Diff sorted {@code sets}.
	 *
	 * @param key must not be {@literal null}.
	 * @param otherKey must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zdiff">Redis Documentation: ZDIFF</a>
	 */
	default Set<@NonNull TypedTuple<V>> differenceWithScores(@NonNull K key, @NonNull K otherKey) {
		return differenceWithScores(key, Collections.singleton(otherKey));
	}

	/**
	 * Diff sorted {@code sets}.
	 *
	 * @param key must not be {@literal null}.
	 * @param otherKeys must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zdiff">Redis Documentation: ZDIFF</a>
	 */
	Set<@NonNull TypedTuple<V>> differenceWithScores(@NonNull K key, @NonNull Collection<@NonNull K> otherKeys);

	/**
	 * Diff sorted {@code sets} and store result in destination {@code destKey}.
	 *
	 * @param key must not be {@literal null}.
	 * @param otherKeys must not be {@literal null}.
	 * @param destKey must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zdiffstore">Redis Documentation: ZDIFFSTORE</a>
	 */
	Long differenceAndStore(@NonNull K key, @NonNull Collection<@NonNull K> otherKeys, @NonNull K destKey);

	/**
	 * Intersect sorted {@code sets}.
	 *
	 * @param key must not be {@literal null}.
	 * @param otherKey must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zinter">Redis Documentation: ZINTER</a>
	 */
	default Set<V> intersect(@NonNull K key, @NonNull K otherKey) {
		return intersect(key, Collections.singleton(otherKey));
	}

	/**
	 * Intersect sorted {@code sets}.
	 *
	 * @param key must not be {@literal null}.
	 * @param otherKeys must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zinter">Redis Documentation: ZINTER</a>
	 */
	Set<V> intersect(@NonNull K key, @NonNull Collection<@NonNull K> otherKeys);

	/**
	 * Intersect sorted {@code sets}.
	 *
	 * @param key must not be {@literal null}.
	 * @param otherKey must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zinter">Redis Documentation: ZINTER</a>
	 */
	default Set<@NonNull TypedTuple<V>> intersectWithScores(@NonNull K key, @NonNull K otherKey) {
		return intersectWithScores(key, Collections.singleton(otherKey));
	}

	/**
	 * Intersect sorted {@code sets}.
	 *
	 * @param key must not be {@literal null}.
	 * @param otherKeys must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zinter">Redis Documentation: ZINTER</a>
	 */
	Set<@NonNull TypedTuple<V>> intersectWithScores(@NonNull K key, @NonNull Collection<@NonNull K> otherKeys);

	/**
	 * Intersect sorted sets at {@code key} and {@code otherKeys} .
	 *
	 * @param key must not be {@literal null}.
	 * @param otherKeys must not be {@literal null}.
	 * @param aggregate must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zinter">Redis Documentation: ZINTER</a>
	 */
	default Set<TypedTuple<V>> intersectWithScores(@NonNull K key, @NonNull Collection<@NonNull K> otherKeys,
			@NonNull Aggregate aggregate) {
		return intersectWithScores(key, otherKeys, aggregate, Weights.fromSetCount(1 + otherKeys.size()));
	}

	/**
	 * Intersect sorted {@code sets}.
	 *
	 * @param key must not be {@literal null}.
	 * @param otherKeys must not be {@literal null}.
	 * @param aggregate must not be {@literal null}.
	 * @param weights must not be {@literal null}.
	 * @return
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zinter">Redis Documentation: ZINTER</a>
	 */
	Set<@NonNull TypedTuple<V>> intersectWithScores(@NonNull K key, @NonNull Collection<@NonNull K> otherKeys,
			@NonNull Aggregate aggregate, @NonNull Weights weights);

	/**
	 * Intersect sorted sets at {@code key} and {@code otherKey} and store result in destination {@code destKey}.
	 *
	 * @param key must not be {@literal null}.
	 * @param otherKey must not be {@literal null}.
	 * @param destKey must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zinterstore">Redis Documentation: ZINTERSTORE</a>
	 */
	Long intersectAndStore(@NonNull K key, @NonNull K otherKey, @NonNull K destKey);

	/**
	 * Intersect sorted sets at {@code key} and {@code otherKeys} and store result in destination {@code destKey}.
	 *
	 * @param key must not be {@literal null}.
	 * @param otherKeys must not be {@literal null}.
	 * @param destKey must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zinterstore">Redis Documentation: ZINTERSTORE</a>
	 */
	Long intersectAndStore(@NonNull K key, @NonNull Collection<@NonNull K> otherKeys, @NonNull K destKey);

	/**
	 * Intersect sorted sets at {@code key} and {@code otherKeys} and store result in destination {@code destKey}.
	 *
	 * @param key must not be {@literal null}.
	 * @param otherKeys must not be {@literal null}.
	 * @param destKey must not be {@literal null}.
	 * @param aggregate must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.1
	 * @see <a href="https://redis.io/commands/zinterstore">Redis Documentation: ZINTERSTORE</a>
	 */
	default Long intersectAndStore(@NonNull K key, @NonNull Collection<@NonNull K> otherKeys, @NonNull K destKey,
			@NonNull Aggregate aggregate) {
		return intersectAndStore(key, otherKeys, destKey, aggregate, Weights.fromSetCount(1 + otherKeys.size()));
	}

	/**
	 * Intersect sorted sets at {@code key} and {@code otherKeys} and store result in destination {@code destKey}.
	 *
	 * @param key must not be {@literal null}.
	 * @param otherKeys must not be {@literal null}.
	 * @param destKey must not be {@literal null}.
	 * @param aggregate must not be {@literal null}.
	 * @param weights must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.1
	 * @see <a href="https://redis.io/commands/zinterstore">Redis Documentation: ZINTERSTORE</a>
	 */
	Long intersectAndStore(@NonNull K key, @NonNull Collection<@NonNull K> otherKeys, @NonNull K destKey,
			@NonNull Aggregate aggregate, @NonNull Weights weights);

	/**
	 * Union sorted {@code sets}.
	 *
	 * @param key must not be {@literal null}.
	 * @param otherKey must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zunion">Redis Documentation: ZUNION</a>
	 */
	default Set<V> union(@NonNull K key, @NonNull K otherKey) {
		return union(key, Collections.singleton(otherKey));
	}

	/**
	 * Union sorted {@code sets}.
	 *
	 * @param key must not be {@literal null}.
	 * @param otherKeys must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zunion">Redis Documentation: ZUNION</a>
	 */
	Set<V> union(@NonNull K key, @NonNull Collection<@NonNull K> otherKeys);

	/**
	 * Union sorted {@code sets}.
	 *
	 * @param key must not be {@literal null}.
	 * @param otherKey must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zunion">Redis Documentation: ZUNION</a>
	 */
	default Set<@NonNull TypedTuple<V>> unionWithScores(@NonNull K key, @NonNull K otherKey) {
		return unionWithScores(key, Collections.singleton(otherKey));
	}

	/**
	 * Union sorted {@code sets}.
	 *
	 * @param key must not be {@literal null}.
	 * @param otherKeys must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zunion">Redis Documentation: ZUNION</a>
	 */
	Set<@NonNull TypedTuple<V>> unionWithScores(@NonNull K key, @NonNull Collection<@NonNull K> otherKeys);

	/**
	 * Union sorted sets at {@code key} and {@code otherKeys} .
	 *
	 * @param key must not be {@literal null}.
	 * @param otherKeys must not be {@literal null}.
	 * @param aggregate must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zunion">Redis Documentation: ZUNION</a>
	 */
	default Set<@NonNull TypedTuple<V>> unionWithScores(@NonNull K key, @NonNull Collection<@NonNull K> otherKeys,
			@NonNull Aggregate aggregate) {
		return unionWithScores(key, otherKeys, aggregate, Weights.fromSetCount(1 + otherKeys.size()));
	}

	/**
	 * Union sorted {@code sets}.
	 *
	 * @param key must not be {@literal null}.
	 * @param otherKeys must not be {@literal null}.
	 * @param aggregate must not be {@literal null}.
	 * @param weights must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zunion">Redis Documentation: ZUNION</a>
	 */
	Set<@NonNull TypedTuple<V>> unionWithScores(@NonNull K key, @NonNull Collection<@NonNull K> otherKeys,
			@NonNull Aggregate aggregate, @NonNull Weights weights);

	/**
	 * Union sorted sets at {@code key} and {@code otherKeys} and store result in destination {@code destKey}.
	 *
	 * @param key must not be {@literal null}.
	 * @param otherKey must not be {@literal null}.
	 * @param destKey must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zunionstore">Redis Documentation: ZUNIONSTORE</a>
	 */
	Long unionAndStore(@NonNull K key, @NonNull K otherKey, @NonNull K destKey);

	/**
	 * Union sorted sets at {@code key} and {@code otherKeys} and store result in destination {@code destKey}.
	 *
	 * @param key must not be {@literal null}.
	 * @param otherKeys must not be {@literal null}.
	 * @param destKey must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zunionstore">Redis Documentation: ZUNIONSTORE</a>
	 */
	Long unionAndStore(@NonNull K key, @NonNull Collection<@NonNull K> otherKeys, @NonNull K destKey);

	/**
	 * Union sorted sets at {@code key} and {@code otherKeys} and store result in destination {@code destKey}.
	 *
	 * @param key must not be {@literal null}.
	 * @param otherKeys must not be {@literal null}.
	 * @param destKey must not be {@literal null}.
	 * @param aggregate must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.1
	 * @see <a href="https://redis.io/commands/zunionstore">Redis Documentation: ZUNIONSTORE</a>
	 */
	default Long unionAndStore(@NonNull K key, @NonNull Collection<@NonNull K> otherKeys, @NonNull K destKey,
			@NonNull Aggregate aggregate) {
		return unionAndStore(key, otherKeys, destKey, aggregate, Weights.fromSetCount(1 + otherKeys.size()));
	}

	/**
	 * Union sorted sets at {@code key} and {@code otherKeys} and store result in destination {@code destKey}.
	 *
	 * @param key must not be {@literal null}.
	 * @param otherKeys must not be {@literal null}.
	 * @param destKey must not be {@literal null}.
	 * @param aggregate must not be {@literal null}.
	 * @param weights must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.1
	 * @see <a href="https://redis.io/commands/zunionstore">Redis Documentation: ZUNIONSTORE</a>
	 */
	Long unionAndStore(@NonNull K key, @NonNull Collection<@NonNull K> otherKeys, @NonNull K destKey,
			@NonNull Aggregate aggregate, @NonNull Weights weights);

	/**
	 * Use a {@link Cursor} to iterate over entries zset at {@code key}. <br />
	 * <strong>Important:</strong> Call {@link Cursor#close()} when done to avoid resource leaks.
	 *
	 * @param key
	 * @param options can be {@literal null}.
	 * @return the result cursor providing access to the scan result. Must be closed once fully processed (e.g. through a
	 *         try-with-resources clause).
	 * @see <a href="https://redis.io/commands/zscan">Redis Documentation: ZSCAN</a>
	 * @since 1.4
	 */
	@NonNull
	Cursor<@NonNull TypedTuple<V>> scan(@NonNull K key, @Nullable ScanOptions options);

	/**
	 * Get all elements with lexicographical ordering from {@literal ZSET} at {@code key} with a value between
	 * {@link Range#getLowerBound()} and {@link Range#getUpperBound()}.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 3.0
	 * @see <a href="https://redis.io/commands/zrangebylex">Redis Documentation: ZRANGEBYLEX</a>
	 */
	default Set<V> rangeByLex(@NonNull K key, @NonNull Range<String> range) {
		return rangeByLex(key, range, Limit.unlimited());
	}

	/**
	 * Get all elements {@literal n} elements, where {@literal n = } {@link Limit#getCount()}, starting at
	 * {@link Limit#getOffset()} with lexicographical ordering from {@literal ZSET} at {@code key} with a value between
	 * {@link Range#getLowerBound()} and {@link Range#getUpperBound()}.
	 *
	 * @param key must not be {@literal null}
	 * @param range must not be {@literal null}.
	 * @param limit can be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 3.0
	 * @see <a href="https://redis.io/commands/zrangebylex">Redis Documentation: ZRANGEBYLEX</a>
	 */
	Set<V> rangeByLex(@NonNull K key, @NonNull Range<String> range, @NonNull Limit limit);

	/**
	 * Get all elements with reverse lexicographical ordering from {@literal ZSET} at {@code key} with a value between
	 * {@link Range#getLowerBound()} and {@link Range#getUpperBound()}.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 3.0
	 * @see <a href="https://redis.io/commands/zrevrangebylex">Redis Documentation: ZREVRANGEBYLEX</a>
	 */
	default Set<V> reverseRangeByLex(@NonNull K key, @NonNull Range<String> range) {
		return reverseRangeByLex(key, range, Limit.unlimited());
	}

	/**
	 * Get all elements {@literal n} elements, where {@literal n = } {@link Limit#getCount()}, starting at
	 * {@link Limit#getOffset()} with reverse lexicographical ordering from {@literal ZSET} at {@code key} with a value
	 * between {@link Range#getLowerBound()} and {@link Range#getUpperBound()}.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @param limit can be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.4
	 * @see <a href="https://redis.io/commands/zrevrangebylex">Redis Documentation: ZREVRANGEBYLEX</a>
	 */
	Set<V> reverseRangeByLex(@NonNull K key, @NonNull Range<String> range, @NonNull Limit limit);

	/**
	 * Store all elements at {@code dstKey} with lexicographical ordering from {@literal ZSET} at {@code srcKey} with a
	 * value between {@link Range#getLowerBound()} and {@link Range#getUpperBound()}.
	 *
	 * @param srcKey must not be {@literal null}.
	 * @param dstKey must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return the number of stored elements or {@literal null} when used in pipeline / transaction.
	 * @since 3.0
	 * @see #rangeByLex(Object, Range)
	 * @see <a href="https://redis.io/commands/zrangestore">Redis Documentation: ZRANGESTORE</a>
	 */
	default Long rangeAndStoreByLex(@NonNull K srcKey, @NonNull K dstKey, @NonNull Range<String> range) {
		return rangeAndStoreByLex(srcKey, dstKey, range, Limit.unlimited());
	}

	/**
	 * Store {@literal n} elements at {@code dstKey}, where {@literal n = } {@link Limit#getCount()}, starting at
	 * {@link Limit#getOffset()} with lexicographical ordering from {@literal ZSET} at {@code srcKey} with a value between
	 * {@link Range#getLowerBound()} and {@link Range#getUpperBound()}.
	 *
	 * @param srcKey must not be {@literal null}.
	 * @param dstKey must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @param limit must not be {@literal null}.
	 * @return the number of stored elements or {@literal null} when used in pipeline / transaction.
	 * @since 3.0
	 * @see #rangeByLex(Object, Range, Limit)
	 * @see <a href="https://redis.io/commands/zrangestore">Redis Documentation: ZRANGESTORE</a>
	 */
	Long rangeAndStoreByLex(@NonNull K srcKey, @NonNull K dstKey, @NonNull Range<String> range, @NonNull Limit limit);

	/**
	 * Store all elements at {@code dstKey} with reverse lexicographical ordering from {@literal ZSET} at {@code srcKey}
	 * with a value between {@link Range#getLowerBound()} and {@link Range#getUpperBound()}.
	 *
	 * @param srcKey must not be {@literal null}.
	 * @param dstKey must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return the number of stored elements or {@literal null} when used in pipeline / transaction.
	 * @since 3.0
	 * @see #reverseRangeByLex(Object, Range)
	 * @see <a href="https://redis.io/commands/zrangestore">Redis Documentation: ZRANGESTORE</a>
	 */
	default Long reverseRangeAndStoreByLex(@NonNull K srcKey, @NonNull K dstKey, @NonNull Range<String> range) {
		return reverseRangeAndStoreByLex(srcKey, dstKey, range, Limit.unlimited());
	}

	/**
	 * Store {@literal n} elements at {@code dstKey}, where {@literal n = } {@link Limit#getCount()}, starting at
	 * {@link Limit#getOffset()} with reverse lexicographical ordering from {@literal ZSET} at {@code srcKey} with a value
	 * between {@link Range#getLowerBound()} and {@link Range#getUpperBound()}.
	 *
	 * @param srcKey must not be {@literal null}.
	 * @param dstKey must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @param limit must not be {@literal null}.
	 * @return the number of stored elements or {@literal null} when used in pipeline / transaction.
	 * @since 3.0
	 * @see #reverseRangeByLex(Object, Range, Limit)
	 * @see <a href="https://redis.io/commands/zrangestore">Redis Documentation: ZRANGESTORE</a>
	 */
	Long reverseRangeAndStoreByLex(@NonNull K srcKey, @NonNull K dstKey, @NonNull Range<String> range,
			@NonNull Limit limit);

	/**
	 * Store all elements at {@code dstKey} with ordering by score from {@literal ZSET} at {@code srcKey} with a score
	 * between {@link Range#getLowerBound()} and {@link Range#getUpperBound()}.
	 *
	 * @param srcKey must not be {@literal null}.
	 * @param dstKey must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return the number of stored elements or {@literal null} when used in pipeline / transaction.
	 * @since 3.0
	 * @see #rangeByScore(Object, double, double)
	 * @see <a href="https://redis.io/commands/zrangestore">Redis Documentation: ZRANGESTORE</a>
	 */
	default Long rangeAndStoreByScore(@NonNull K srcKey, @NonNull K dstKey, @NonNull Range<? extends Number> range) {
		return rangeAndStoreByScore(srcKey, dstKey, range, Limit.unlimited());
	}

	/**
	 * Store {@literal n} elements at {@code dstKey}, where {@literal n = } {@link Limit#getCount()}, starting at
	 * {@link Limit#getOffset()} with ordering by score from {@literal ZSET} at {@code srcKey} with a score between
	 * {@link Range#getLowerBound()} and {@link Range#getUpperBound()}.
	 *
	 * @param srcKey must not be {@literal null}.
	 * @param dstKey must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @param limit must not be {@literal null}.
	 * @return the number of stored elements or {@literal null} when used in pipeline / transaction.
	 * @since 3.0
	 * @see #rangeByScore(Object, double, double, long, long)
	 * @see <a href="https://redis.io/commands/zrangestore">Redis Documentation: ZRANGESTORE</a>
	 */
	Long rangeAndStoreByScore(@NonNull K srcKey, @NonNull K dstKey, @NonNull Range<? extends Number> range,
			@NonNull Limit limit);

	/**
	 * Store all elements at {@code dstKey} with reverse ordering by score from {@literal ZSET} at {@code srcKey} with a
	 * score between {@link Range#getLowerBound()} and {@link Range#getUpperBound()}.
	 *
	 * @param srcKey must not be {@literal null}.
	 * @param dstKey must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return the number of stored elements or {@literal null} when used in pipeline / transaction.
	 * @since 3.0
	 * @see #reverseRangeByScore(Object, double, double)
	 * @see <a href="https://redis.io/commands/zrangestore">Redis Documentation: ZRANGESTORE</a>
	 */
	default Long reverseRangeAndStoreByScore(@NonNull K srcKey, @NonNull K dstKey,
			@NonNull Range<? extends Number> range) {
		return reverseRangeAndStoreByScore(srcKey, dstKey, range, Limit.unlimited());
	}

	/**
	 * Store {@literal n} elements at {@code dstKey}, where {@literal n = } {@link Limit#getCount()}, starting at
	 * {@link Limit#getOffset()} with reverse ordering by score from {@literal ZSET} at {@code srcKey} with a score
	 * between {@link Range#getLowerBound()} and {@link Range#getUpperBound()}.
	 *
	 * @param srcKey must not be {@literal null}.
	 * @param dstKey must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @param limit must not be {@literal null}.
	 * @return the number of stored elements or {@literal null} when used in pipeline / transaction.
	 * @since 3.0
	 * @see #reverseRangeByScore(Object, double, double, long, long)
	 * @see <a href="https://redis.io/commands/zrangestore">Redis Documentation: ZRANGESTORE</a>
	 */
	Long reverseRangeAndStoreByScore(@NonNull K srcKey, @NonNull K dstKey, @NonNull Range<? extends Number> range,
			@NonNull Limit limit);

	/**
	 * @return the underlying {@link RedisOperations} used to execute commands.
	 */
	@NonNull
	RedisOperations<K, V> getOperations();

}
