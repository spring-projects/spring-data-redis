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
import org.jspecify.annotations.NullUnmarked;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.Limit;
import org.springframework.data.redis.connection.zset.Aggregate;
import org.springframework.data.redis.connection.zset.Tuple;
import org.springframework.data.redis.connection.zset.Weights;
import org.springframework.data.redis.core.ZSetOperations.TypedTuple;
import org.springframework.util.Assert;

/**
 * ZSet (or SortedSet) operations bound to a certain key.
 *
 * @author Costin Leau
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Wongoo (望哥)
 * @author Andrey Shlykov
 */
@NullUnmarked
public interface BoundZSetOperations<K, V> extends BoundKeyOperations<K> {

	/**
	 * Add {@code value} to a sorted set at the bound key, or update its {@code score} if it already exists.
	 *
	 * @param value the value.
	 * @param score the score.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zadd">Redis Documentation: ZADD</a>
	 */
	Boolean add(@NonNull V value, double score);

	/**
	 * Add {@code value} to a sorted set at the bound key if it does not already exists.
	 *
	 * @param value the value.
	 * @param score the score.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.5
	 * @see <a href="https://redis.io/commands/zadd">Redis Documentation: ZADD NX</a>
	 */
	Boolean addIfAbsent(@NonNull V value, double score);

	/**
	 * Add {@code tuples} to a sorted set at the bound key, or update its {@code score} if it already exists.
	 *
	 * @param tuples must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zadd">Redis Documentation: ZADD</a>
	 */
	Long add(Set<@NonNull TypedTuple<V>> tuples);

	/**
	 * Add {@code tuples} to a sorted set at the bound key if it does not already exists.
	 *
	 * @param tuples must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.5
	 * @see <a href="https://redis.io/commands/zadd">Redis Documentation: ZADD NX</a>
	 */
	Long addIfAbsent(Set<@NonNull TypedTuple<V>> tuples);

	/**
	 * Remove {@code values} from sorted set. Return number of removed elements.
	 *
	 * @param values must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zrem">Redis Documentation: ZREM</a>
	 */
	Long remove(@NonNull Object @NonNull... values);

	/**
	 * Increment the score of element with {@code value} in sorted set by {@code increment}.
	 *
	 * @param value the value.
	 * @param delta the delta to add. Can be negative.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zincrby">Redis Documentation: ZINCRBY</a>
	 */
	Double incrementScore(@NonNull V value, double delta);

	/**
	 * Get random element from set at the bound key.
	 *
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zrandmember">Redis Documentation: ZRANDMEMBER</a>
	 */
	V randomMember();

	/**
	 * Get {@code count} distinct random elements from set at the bound key.
	 *
	 * @param count number of members to return.
	 * @return empty {@link Set} if {@code key} does not exist.
	 * @throws IllegalArgumentException if count is negative.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zrandmember">Redis Documentation: ZRANDMEMBER</a>
	 */
	Set<V> distinctRandomMembers(long count);

	/**
	 * Get {@code count} random elements from set at the bound key.
	 *
	 * @param count number of members to return.
	 * @return empty {@link List} if {@code key} does not exist or {@literal null} when used in pipeline / transaction.
	 * @throws IllegalArgumentException if count is negative.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zrandmember">Redis Documentation: ZRANDMEMBER</a>
	 */
	List<V> randomMembers(long count);

	/**
	 * Get random element with its score from set at the bound key.
	 *
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zrandmember">Redis Documentation: ZRANDMEMBER</a>
	 */
	TypedTuple<V> randomMemberWithScore();

	/**
	 * Get {@code count} distinct random elements with their score from set at the bound key.
	 *
	 * @param count number of members to return.
	 * @return empty {@link Set} if {@code key} does not exist.
	 * @throws IllegalArgumentException if count is negative.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zrandmember">Redis Documentation: ZRANDMEMBER</a>
	 */
	Set<@NonNull TypedTuple<V>> distinctRandomMembersWithScore(long count);

	/**
	 * Get {@code count} random elements with their score from set at the bound key.
	 *
	 * @param count number of members to return.
	 * @return empty {@link List} if {@code key} does not exist or {@literal null} when used in pipeline / transaction.
	 * @throws IllegalArgumentException if count is negative.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zrandmember">Redis Documentation: ZRANDMEMBER</a>
	 */
	List<@NonNull TypedTuple<V>> randomMembersWithScore(long count);

	/**
	 * Determine the index of element with {@code value} in a sorted set.
	 *
	 * @param o the value.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zrank">Redis Documentation: ZRANK</a>
	 */
	Long rank(@NonNull Object o);

	/**
	 * Determine the index of element with {@code value} in a sorted set when scored high to low.
	 *
	 * @param o the value.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zrevrank">Redis Documentation: ZREVRANK</a>
	 */
	Long reverseRank(@NonNull Object o);

	/**
	 * Get elements between {@code start} and {@code end} from sorted set.
	 *
	 * @param start
	 * @param end
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zrange">Redis Documentation: ZRANGE</a>
	 */
	Set<V> range(long start, long end);

	/**
	 * Get set of {@link Tuple}s between {@code start} and {@code end} from sorted set.
	 *
	 * @param start
	 * @param end
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zrange">Redis Documentation: ZRANGE</a>
	 */
	Set<@NonNull TypedTuple<V>> rangeWithScores(long start, long end);

	/**
	 * Get elements where score is between {@code min} and {@code max} from sorted set.
	 *
	 * @param min
	 * @param max
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zrangebyscore">Redis Documentation: ZRANGEBYSCORE</a>
	 */
	Set<V> rangeByScore(double min, double max);

	/**
	 * Get set of {@link Tuple}s where score is between {@code min} and {@code max} from sorted set.
	 *
	 * @param min
	 * @param max
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zrangebyscore">Redis Documentation: ZRANGEBYSCORE</a>
	 */
	Set<@NonNull TypedTuple<V>> rangeByScoreWithScores(double min, double max);

	/**
	 * Get elements in range from {@code start} to {@code end} from sorted set ordered from high to low.
	 *
	 * @param start
	 * @param end
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zrevrange">Redis Documentation: ZREVRANGE</a>
	 */
	Set<V> reverseRange(long start, long end);

	/**
	 * Get set of {@link Tuple}s in range from {@code start} to {@code end} from sorted set ordered from high to low.
	 *
	 * @param start
	 * @param end
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zrevrange">Redis Documentation: ZREVRANGE</a>
	 */
	Set<@NonNull TypedTuple<V>> reverseRangeWithScores(long start, long end);

	/**
	 * Get elements where score is between {@code min} and {@code max} from sorted set ordered from high to low.
	 *
	 * @param min
	 * @param max
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zrevrangebyscore">Redis Documentation: ZREVRANGEBYSCORE</a>
	 */
	Set<V> reverseRangeByScore(double min, double max);

	/**
	 * Get set of {@link Tuple} where score is between {@code min} and {@code max} from sorted set ordered from high to
	 * low.
	 *
	 * @param min
	 * @param max
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zrevrangebyscore">Redis Documentation: ZREVRANGEBYSCORE</a>
	 */
	Set<@NonNull TypedTuple<V>> reverseRangeByScoreWithScores(double min, double max);

	/**
	 * Count number of elements within sorted set with scores between {@code min} and {@code max}.
	 *
	 * @param min
	 * @param max
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zcount">Redis Documentation: ZCOUNT</a>
	 */
	Long count(double min, double max);

	/**
	 * Count number of elements within sorted set with value between {@link Range#getLowerBound()} and
	 * {@link Range#getUpperBound()} applying lexicographical ordering.
	 *
	 * @param range must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 3.0
	 * @see <a href="https://redis.io/commands/zlexcount">Redis Documentation: ZLEXCOUNT</a>
	 */
	Long lexCount(@NonNull Range<String> range);

	/**
	 * Remove and return the value with its score having the lowest score from sorted set at the bound key.
	 *
	 * @return {@literal null} when the sorted set is empty or used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zpopmin">Redis Documentation: ZPOPMIN</a>
	 * @since 2.6
	 */
	TypedTuple<V> popMin();

	/**
	 * Remove and return {@code count} values with their score having the lowest score from sorted set at the bound key.
	 *
	 * @param count number of elements to pop.
	 * @return {@literal null} when the sorted set is empty or used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zpopmin">Redis Documentation: ZPOPMIN</a>
	 * @since 2.6
	 */
	Set<@NonNull TypedTuple<V>> popMin(long count);

	/**
	 * Remove and return the value with its score having the lowest score from sorted set at the bound key. <b>Blocks
	 * connection</b> until element available or {@code timeout} reached.
	 *
	 * @param timeout
	 * @param unit must not be {@literal null}.
	 * @return can be {@literal null}.
	 * @see <a href="https://redis.io/commands/bzpopmin">Redis Documentation: BZPOPMIN</a>
	 * @since 2.6
	 */
	TypedTuple<V> popMin(long timeout, @NonNull TimeUnit unit);

	/**
	 * Remove and return the value with its score having the lowest score from sorted set at the bound key. <b>Blocks
	 * connection</b> until element available or {@code timeout} reached.
	 *
	 * @param timeout must not be {@literal null}.
	 * @return can be {@literal null}.
	 * @throws IllegalArgumentException if the timeout is {@literal null} or negative.
	 * @see <a href="https://redis.io/commands/bzpopmin">Redis Documentation: BZPOPMIN</a>
	 * @since 2.6
	 */
	default TypedTuple<V> popMin(@NonNull Duration timeout) {

		Assert.notNull(timeout, "Timeout must not be null");
		Assert.isTrue(!timeout.isNegative(), "Timeout must not be negative");

		return popMin(TimeoutUtils.toSeconds(timeout), TimeUnit.SECONDS);
	}

	/**
	 * Remove and return the value with its score having the highest score from sorted set at the bound key.
	 *
	 * @return {@literal null} when the sorted set is empty or used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zpopmax">Redis Documentation: ZPOPMAX</a>
	 * @since 2.6
	 */
	TypedTuple<V> popMax();

	/**
	 * Remove and return {@code count} values with their score having the highest score from sorted set at the bound key.
	 *
	 * @param count number of elements to pop.
	 * @return {@literal null} when the sorted set is empty or used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zpopmax">Redis Documentation: ZPOPMAX</a>
	 * @since 2.6
	 */
	Set<@NonNull TypedTuple<V>> popMax(long count);

	/**
	 * Remove and return the value with its score having the highest score from sorted set at the bound key. <b>Blocks
	 * connection</b> until element available or {@code timeout} reached.
	 *
	 * @param timeout
	 * @param unit must not be {@literal null}.
	 * @return can be {@literal null}.
	 * @see <a href="https://redis.io/commands/bzpopmax">Redis Documentation: BZPOPMAX</a>
	 * @since 2.6
	 */
	TypedTuple<V> popMax(long timeout, @NonNull TimeUnit unit);

	/**
	 * Remove and return the value with its score having the highest score from sorted set at the bound key. <b>Blocks
	 * connection</b> until element available or {@code timeout} reached.
	 *
	 * @param timeout must not be {@literal null}.
	 * @return can be {@literal null}.
	 * @throws IllegalArgumentException if the timeout is {@literal null} or negative.
	 * @see <a href="https://redis.io/commands/bzpopmax">Redis Documentation: BZPOPMAX</a>
	 * @since 2.6
	 */
	default TypedTuple<V> popMax(@NonNull Duration timeout) {

		Assert.notNull(timeout, "Timeout must not be null");
		Assert.isTrue(!timeout.isNegative(), "Timeout must not be negative");

		return popMax(TimeoutUtils.toSeconds(timeout), TimeUnit.SECONDS);
	}

	/**
	 * Returns the number of elements of the sorted set stored with given the bound key.
	 *
	 * @see #zCard()
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zcard">Redis Documentation: ZCARD</a>
	 */
	Long size();

	/**
	 * Get the size of sorted set with the bound key.
	 *
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 1.3
	 * @see <a href="https://redis.io/commands/zcard">Redis Documentation: ZCARD</a>
	 */
	Long zCard();

	/**
	 * Get the score of element with {@code value} from sorted set with key the bound key.
	 *
	 * @param o the value.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zscore">Redis Documentation: ZSCORE</a>
	 */
	Double score(@NonNull Object o);

	/**
	 * Get the scores of elements with {@code values} from sorted set with key the bound key.
	 *
	 * @param o the values.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zmscore">Redis Documentation: ZMSCORE</a>
	 * @since 2.6
	 */
	List<Double> score(@NonNull Object @NonNull... o);

	/**
	 * Remove elements in range between {@code start} and {@code end} from sorted set with the bound key.
	 *
	 * @param start
	 * @param end
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zremrangebyrank">Redis Documentation: ZREMRANGEBYRANK</a>
	 */
	Long removeRange(long start, long end);

	/**
	 * Remove elements in {@link Range} from sorted set with the bound key.
	 *
	 * @param range must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 3.0
	 * @see <a href="https://redis.io/commands/zremrangebylex">Redis Documentation: ZREMRANGEBYLEX</a>
	 */
	Long removeRangeByLex(@NonNull Range<String> range);

	/**
	 * Remove elements with scores between {@code min} and {@code max} from sorted set with the bound key.
	 *
	 * @param min
	 * @param max
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zremrangebyscore">Redis Documentation: ZREMRANGEBYSCORE</a>
	 */
	Long removeRangeByScore(double min, double max);

	/**
	 * Union sorted sets at the bound key and {@code otherKeys} and store result in destination {@code destKey}.
	 *
	 * @param otherKeys must not be {@literal null}.
	 * @param destKey must not be {@literal null}.
	 * @param aggregate must not be {@literal null}.
	 * @param weights must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.1
	 * @see <a href="https://redis.io/commands/zunionstore">Redis Documentation: ZUNIONSTORE</a>
	 */
	Long unionAndStore(@NonNull Collection<@NonNull K> otherKeys, @NonNull K destKey, @NonNull Aggregate aggregate,
			@NonNull Weights weights);

	/**
	 * Diff sorted {@code sets}.
	 *
	 * @param otherKey must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zdiff">Redis Documentation: ZDIFF</a>
	 */
	default Set<V> difference(@NonNull K otherKey) {
		return difference(Collections.singleton(otherKey));
	}

	/**
	 * Diff sorted {@code sets}.
	 *
	 * @param otherKeys must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zdiff">Redis Documentation: ZDIFF</a>
	 */
	Set<V> difference(@NonNull Collection<@NonNull K> otherKeys);

	/**
	 * Diff sorted {@code sets}.
	 *
	 * @param otherKey must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zdiff">Redis Documentation: ZDIFF</a>
	 */
	default Set<@NonNull TypedTuple<V>> differenceWithScores(@NonNull K otherKey) {
		return differenceWithScores(Collections.singleton(otherKey));
	}

	/**
	 * Diff sorted {@code sets}.
	 *
	 * @param otherKeys must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zdiff">Redis Documentation: ZDIFF</a>
	 */
	Set<@NonNull TypedTuple<V>> differenceWithScores(@NonNull Collection<@NonNull K> otherKeys);

	/**
	 * Diff sorted {@code sets} and store result in destination {@code destKey}.
	 *
	 * @param otherKey must not be {@literal null}.
	 * @param destKey must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zdiffstore">Redis Documentation: ZDIFFSTORE</a>
	 */
	default Long differenceAndStore(@NonNull K otherKey, @NonNull K destKey) {
		return differenceAndStore(Collections.singleton(otherKey), destKey);
	}

	/**
	 * Diff sorted {@code sets} and store result in destination {@code destKey}.
	 *
	 * @param otherKeys must not be {@literal null}.
	 * @param destKey must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zdiffstore">Redis Documentation: ZDIFFSTORE</a>
	 */
	Long differenceAndStore(@NonNull Collection<@NonNull K> otherKeys, @NonNull K destKey);

	/**
	 * Intersect sorted {@code sets}.
	 *
	 * @param otherKey must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zinter">Redis Documentation: ZINTER</a>
	 */
	default Set<V> intersect(@NonNull K otherKey) {
		return intersect(Collections.singleton(otherKey));
	}

	/**
	 * Intersect sorted {@code sets}.
	 *
	 * @param otherKeys must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zinter">Redis Documentation: ZINTER</a>
	 */
	Set<V> intersect(@NonNull Collection<@NonNull K> otherKeys);

	/**
	 * Intersect sorted {@code sets}.
	 *
	 * @param otherKey must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zinter">Redis Documentation: ZINTER</a>
	 */
	default Set<@NonNull TypedTuple<V>> intersectWithScores(@NonNull K otherKey) {
		return intersectWithScores(Collections.singleton(otherKey));
	}

	/**
	 * Intersect sorted {@code sets}.
	 *
	 * @param otherKeys must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zinter">Redis Documentation: ZINTER</a>
	 */
	Set<@NonNull TypedTuple<V>> intersectWithScores(@NonNull Collection<@NonNull K> otherKeys);

	/**
	 * Intersect sorted {@code sets}.
	 *
	 * @param otherKeys must not be {@literal null}.
	 * @param aggregate must not be {@literal null}.
	 * @param weights must not be {@literal null}.
	 * @return
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zinter">Redis Documentation: ZINTER</a>
	 */
	Set<@NonNull TypedTuple<V>> intersectWithScores(@NonNull Collection<@NonNull K> otherKeys,
			@NonNull Aggregate aggregate, @NonNull Weights weights);

	/**
	 * Intersect sorted sets at the bound key and {@code otherKey} and store result in destination {@code destKey}.
	 *
	 * @param otherKey must not be {@literal null}.
	 * @param destKey must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zinterstore">Redis Documentation: ZINTERSTORE</a>
	 */
	Long intersectAndStore(@NonNull K otherKey, @NonNull K destKey);

	/**
	 * Intersect sorted sets at the bound key and {@code otherKeys} and store result in destination {@code destKey}.
	 *
	 * @param otherKeys must not be {@literal null}.
	 * @param destKey must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zinterstore">Redis Documentation: ZINTERSTORE</a>
	 */
	Long intersectAndStore(@NonNull Collection<@NonNull K> otherKeys, @NonNull K destKey);

	/**
	 * Intersect sorted sets at the bound key and {@code otherKeys} and store result in destination {@code destKey}.
	 *
	 * @param otherKeys must not be {@literal null}.
	 * @param destKey must not be {@literal null}.
	 * @param aggregate must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.1
	 * @see <a href="https://redis.io/commands/zinterstore">Redis Documentation: ZINTERSTORE</a>
	 */
	Long intersectAndStore(@NonNull Collection<@NonNull K> otherKeys, @NonNull K destKey, @NonNull Aggregate aggregate);

	/**
	 * Intersect sorted sets at the bound key and {@code otherKeys} and store result in destination {@code destKey}.
	 *
	 * @param otherKeys must not be {@literal null}.
	 * @param destKey must not be {@literal null}.
	 * @param aggregate must not be {@literal null}.
	 * @param weights must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.1
	 * @see <a href="https://redis.io/commands/zinterstore">Redis Documentation: ZINTERSTORE</a>
	 */
	Long intersectAndStore(@NonNull Collection<@NonNull K> otherKeys, @NonNull K destKey, @NonNull Aggregate aggregate,
			@NonNull Weights weights);

	/**
	 * Union sorted {@code sets}.
	 *
	 * @param otherKey must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zunion">Redis Documentation: ZUNION</a>
	 */
	default Set<V> union(@NonNull K otherKey) {
		return union(Collections.singleton(otherKey));
	}

	/**
	 * Union sorted {@code sets}.
	 *
	 * @param otherKeys must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zunion">Redis Documentation: ZUNION</a>
	 */
	Set<V> union(@NonNull Collection<@NonNull K> otherKeys);

	/**
	 * Union sorted {@code sets}.
	 *
	 * @param otherKey must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zunion">Redis Documentation: ZUNION</a>
	 */
	default Set<@NonNull TypedTuple<V>> unionWithScores(@NonNull K otherKey) {
		return unionWithScores(Collections.singleton(otherKey));
	}

	/**
	 * Union sorted {@code sets}.
	 *
	 * @param otherKeys must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zunion">Redis Documentation: ZUNION</a>
	 */
	Set<@NonNull TypedTuple<V>> unionWithScores(@NonNull Collection<@NonNull K> otherKeys);

	/**
	 * Union sorted sets at the bound key and {@code otherKeys}.
	 *
	 * @param otherKeys must not be {@literal null}.
	 * @param aggregate must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zunion">Redis Documentation: ZUNION</a>
	 */
	default Set<@NonNull TypedTuple<V>> unionWithScores(@NonNull Collection<@NonNull K> otherKeys,
			@NonNull Aggregate aggregate) {
		return unionWithScores(otherKeys, aggregate, Weights.fromSetCount(1 + otherKeys.size()));
	}

	/**
	 * Union sorted {@code sets}.
	 *
	 * @param otherKeys must not be {@literal null}.
	 * @param aggregate must not be {@literal null}.
	 * @param weights must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zunion">Redis Documentation: ZUNION</a>
	 */
	Set<@NonNull TypedTuple<V>> unionWithScores(@NonNull Collection<@NonNull K> otherKeys, @NonNull Aggregate aggregate,
			@NonNull Weights weights);

	/**
	 * Union sorted sets at the bound key and {@code otherKeys} and store result in destination {@code destKey}.
	 *
	 * @param otherKey must not be {@literal null}.
	 * @param destKey must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zunionstore">Redis Documentation: ZUNIONSTORE</a>
	 */
	Long unionAndStore(@NonNull K otherKey, @NonNull K destKey);

	/**
	 * Union sorted sets at the bound key and {@code otherKeys} and store result in destination {@code destKey}.
	 *
	 * @param otherKeys must not be {@literal null}.
	 * @param destKey must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zunionstore">Redis Documentation: ZUNIONSTORE</a>
	 */
	Long unionAndStore(@NonNull Collection<@NonNull K> otherKeys, @NonNull K destKey);

	/**
	 * Union sorted sets at the bound key and {@code otherKeys} and store result in destination {@code destKey}.
	 *
	 * @param otherKeys must not be {@literal null}.
	 * @param destKey must not be {@literal null}.
	 * @param aggregate must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.1
	 * @see <a href="https://redis.io/commands/zunionstore">Redis Documentation: ZUNIONSTORE</a>
	 */
	Long unionAndStore(@NonNull Collection<@NonNull K> otherKeys, @NonNull K destKey, @NonNull Aggregate aggregate);

	/**
	 * Use a {@link Cursor} to iterate over entries in zset at the bound key. <br />
	 * <strong>Important:</strong> Call {@link Cursor#close()} when done to avoid resource leaks.
	 *
	 * @param options must not be {@literal null}.
	 * @return the result cursor providing access to the scan result. Must be closed once fully processed (e.g. through a
	 *         try-with-resources clause).
	 * @since 1.4
	 */
	Cursor<@NonNull TypedTuple<V>> scan(@NonNull ScanOptions options);

	/**
	 * Get all elements with lexicographical ordering with a value between {@link Range#getLowerBound()} and
	 * {@link Range#getUpperBound()}.
	 *
	 * @param range must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 3.0
	 * @see <a href="https://redis.io/commands/zrangebylex">Redis Documentation: ZRANGEBYLEX</a>
	 */
	default Set<V> rangeByLex(@NonNull Range<String> range) {
		return rangeByLex(range, Limit.unlimited());
	}

	/**
	 * Get all elements {@literal n} elements, where {@literal n = } {@link Limit#getCount()}, starting at
	 * {@link Limit#getOffset()} with lexicographical ordering having a value between {@link Range#getLowerBound()} and
	 * {@link Range#getUpperBound()}.
	 *
	 * @param range must not be {@literal null}.
	 * @param limit can be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 3.0
	 * @see <a href="https://redis.io/commands/zrangebylex">Redis Documentation: ZRANGEBYLEX</a>
	 */
	Set<V> rangeByLex(Range<String> range, @NonNull Limit limit);

	/**
	 * Get all elements with reverse lexicographical ordering with a value between {@link Range#getLowerBound()} and
	 * {@link Range#getUpperBound()}.
	 *
	 * @param range must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 3.0
	 * @see <a href="https://redis.io/commands/zrevrangebylex">Redis Documentation: ZREVRANGEBYLEX</a>
	 */
	default Set<V> reverseRangeByLex(@NonNull Range<String> range) {
		return reverseRangeByLex(range, Limit.unlimited());
	}

	/**
	 * Get all elements {@literal n} elements, where {@literal n = } {@link Limit#getCount()}, starting at
	 * {@link Limit#getOffset()} with reverse lexicographical ordering having a value between
	 * {@link Range#getLowerBound()} and {@link Range#getUpperBound()}.
	 *
	 * @param range must not be {@literal null}.
	 * @param limit can be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 3.0
	 * @see <a href="https://redis.io/commands/zrevrangebylex">Redis Documentation: ZREVRANGEBYLEX</a>
	 */
	Set<V> reverseRangeByLex(@NonNull Range<String> range, @NonNull Limit limit);

	/**
	 * Store all elements at {@code dstKey} with lexicographical ordering from {@literal ZSET} at the bound key with a
	 * value between {@link Range#getLowerBound()} and {@link Range#getUpperBound()}.
	 *
	 * @param dstKey must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return the number of stored elements or {@literal null} when used in pipeline / transaction.
	 * @since 3.0
	 * @see #rangeByLex(Range)
	 * @see <a href="https://redis.io/commands/zrangestore">Redis Documentation: ZRANGESTORE</a>
	 */
	default Long rangeAndStoreByLex(@NonNull K dstKey, @NonNull Range<String> range) {
		return rangeAndStoreByLex(dstKey, range, Limit.unlimited());
	}

	/**
	 * Store {@literal n} elements at {@code dstKey}, where {@literal n = } {@link Limit#getCount()}, starting at
	 * {@link Limit#getOffset()} with lexicographical ordering from {@literal ZSET} at the bound key with a value between
	 * {@link Range#getLowerBound()} and {@link Range#getUpperBound()}.
	 *
	 * @param dstKey must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @param limit must not be {@literal null}.
	 * @return the number of stored elements or {@literal null} when used in pipeline / transaction.
	 * @since 3.0
	 * @see #rangeByLex(Range, Limit)
	 * @see <a href="https://redis.io/commands/zrangestore">Redis Documentation: ZRANGESTORE</a>
	 */
	Long rangeAndStoreByLex(@NonNull K dstKey, @NonNull Range<String> range, @NonNull Limit limit);

	/**
	 * Store all elements at {@code dstKey} with reverse lexicographical ordering from {@literal ZSET} at the bound key
	 * with a value between {@link Range#getLowerBound()} and {@link Range#getUpperBound()}.
	 *
	 * @param dstKey must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return the number of stored elements or {@literal null} when used in pipeline / transaction.
	 * @since 3.0
	 * @see #reverseRangeByLex(Range)
	 * @see <a href="https://redis.io/commands/zrangestore">Redis Documentation: ZRANGESTORE</a>
	 */
	default Long reverseRangeAndStoreByLex(@NonNull K dstKey, @NonNull Range<String> range) {
		return reverseRangeAndStoreByLex(dstKey, range, Limit.unlimited());
	}

	/**
	 * Store {@literal n} elements at {@code dstKey}, where {@literal n = } {@link Limit#getCount()}, starting at
	 * {@link Limit#getOffset()} with reverse lexicographical ordering from {@literal ZSET} at the bound key with a value
	 * between {@link Range#getLowerBound()} and {@link Range#getUpperBound()}.
	 *
	 * @param dstKey must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @param limit must not be {@literal null}.
	 * @return the number of stored elements or {@literal null} when used in pipeline / transaction.
	 * @since 3.0
	 * @see #reverseRangeByLex(Range, Limit)
	 * @see <a href="https://redis.io/commands/zrangestore">Redis Documentation: ZRANGESTORE</a>
	 */
	Long reverseRangeAndStoreByLex(@NonNull K dstKey, @NonNull Range<String> range, @NonNull Limit limit);

	/**
	 * Store all elements at {@code dstKey} with ordering by score from {@literal ZSET} at the bound key with a score
	 * between {@link Range#getLowerBound()} and {@link Range#getUpperBound()}.
	 *
	 * @param dstKey must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return the number of stored elements or {@literal null} when used in pipeline / transaction.
	 * @since 3.0
	 * @see #rangeByScore(double, double)
	 * @see <a href="https://redis.io/commands/zrangestore">Redis Documentation: ZRANGESTORE</a>
	 */
	default Long rangeAndStoreByScore(@NonNull K dstKey, @NonNull Range<? extends Number> range) {
		return rangeAndStoreByScore(dstKey, range, Limit.unlimited());
	}

	/**
	 * Store {@literal n} elements at {@code dstKey}, where {@literal n = } {@link Limit#getCount()}, starting at
	 * {@link Limit#getOffset()} with ordering by score from {@literal ZSET} at the bound key with a score between
	 * {@link Range#getLowerBound()} and {@link Range#getUpperBound()}.
	 *
	 * @param dstKey must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @param limit must not be {@literal null}.
	 * @return the number of stored elements or {@literal null} when used in pipeline / transaction.
	 * @since 3.0
	 * @see #rangeByScore(double, double)
	 * @see <a href="https://redis.io/commands/zrangestore">Redis Documentation: ZRANGESTORE</a>
	 */
	Long rangeAndStoreByScore(@NonNull K dstKey, @NonNull Range<? extends Number> range, @NonNull Limit limit);

	/**
	 * Store all elements at {@code dstKey} with reverse ordering by score from {@literal ZSET} at the bound key with a
	 * score between {@link Range#getLowerBound()} and {@link Range#getUpperBound()}.
	 *
	 * @param dstKey must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return the number of stored elements or {@literal null} when used in pipeline / transaction.
	 * @since 3.0
	 * @see #reverseRangeByScore(double, double)
	 * @see <a href="https://redis.io/commands/zrangestore">Redis Documentation: ZRANGESTORE</a>
	 */
	default Long reverseRangeAndStoreByScore(@NonNull K dstKey, @NonNull Range<? extends Number> range) {
		return reverseRangeAndStoreByScore(dstKey, range, Limit.unlimited());
	}

	/**
	 * Store {@literal n} elements at {@code dstKey}, where {@literal n = } {@link Limit#getCount()}, starting at
	 * {@link Limit#getOffset()} with reverse ordering by score from {@literal ZSET} at the bound key with a score between
	 * {@link Range#getLowerBound()} and {@link Range#getUpperBound()}.
	 *
	 * @param dstKey must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @param limit must not be {@literal null}.
	 * @return the number of stored elements or {@literal null} when used in pipeline / transaction.
	 * @since 3.0
	 * @see <a href="https://redis.io/commands/zrangestore">Redis Documentation: ZRANGESTORE</a>
	 */
	Long reverseRangeAndStoreByScore(@NonNull K dstKey, @NonNull Range<? extends Number> range, @NonNull Limit limit);

	/**
	 * @return the underlying {@link RedisOperations} used to execute commands.
	 */
	@NonNull
	RedisOperations<K, V> getOperations();
}
