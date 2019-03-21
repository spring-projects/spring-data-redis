/*
 * Copyright 2011-2017 the original author or authors.
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

import java.util.Collection;
import java.util.Set;

import org.springframework.data.redis.connection.RedisZSetCommands.Limit;
import org.springframework.data.redis.connection.RedisZSetCommands.Range;
import org.springframework.data.redis.connection.RedisZSetCommands.Tuple;
import org.springframework.data.redis.core.ZSetOperations.TypedTuple;

/**
 * ZSet (or SortedSet) operations bound to a certain key.
 * 
 * @author Costin Leau
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public interface BoundZSetOperations<K, V> extends BoundKeyOperations<K> {

	/**
	 * Add {@code value} to a sorted set at the bound key, or update its {@code score} if it already exists.
	 *
	 * @param score the score.
	 * @param value the value.
	 * @return
	 * @see <a href="http://redis.io/commands/zadd">Redis Documentation: ZADD</a>
	 */
	Boolean add(V value, double score);

	/**
	 * Add {@code tuples} to a sorted set at the bound key, or update its {@code score} if it already exists.
	 *
	 * @param tuples must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/zadd">Redis Documentation: ZADD</a>
	 */
	Long add(Set<TypedTuple<V>> tuples);

	/**
	 * Remove {@code values} from sorted set. Return number of removed elements.
	 *
	 * @param values must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/zrem">Redis Documentation: ZREM</a>
	 */
	Long remove(Object... values);

	/**
	 * Increment the score of element with {@code value} in sorted set by {@code increment}.
	 *
	 * @param delta
	 * @param value the value.
	 * @return
	 * @see <a href="http://redis.io/commands/zincrby">Redis Documentation: ZINCRBY</a>
	 */
	Double incrementScore(V value, double delta);

	/**
	 * Determine the index of element with {@code value} in a sorted set.
	 *
	 * @param o the value.
	 * @return
	 * @see <a href="http://redis.io/commands/zrank">Redis Documentation: ZRANK</a>
	 */
	Long rank(Object o);

	/**
	 * Determine the index of element with {@code value} in a sorted set when scored high to low.
	 *
	 * @param o the value.
	 * @return
	 * @see <a href="http://redis.io/commands/zrevrank">Redis Documentation: ZREVRANK</a>
	 */
	Long reverseRank(Object o);

	/**
	 * Get elements between {@code start} and {@code end} from sorted set.
	 *
	 * @param start
	 * @param end
	 * @return
	 * @see <a href="http://redis.io/commands/zrange">Redis Documentation: ZRANGE</a>
	 */
	Set<V> range(long start, long end);

	/**
	 * Get set of {@link Tuple}s between {@code start} and {@code end} from sorted set.
	 *
	 * @param start
	 * @param end
	 * @return
	 * @see <a href="http://redis.io/commands/zrange">Redis Documentation: ZRANGE</a>
	 */
	Set<TypedTuple<V>> rangeWithScores(long start, long end);

	/**
	 * Get elements where score is between {@code min} and {@code max} from sorted set.
	 *
	 * @param min
	 * @param max
	 * @return
	 * @see <a href="http://redis.io/commands/zrangebyscore">Redis Documentation: ZRANGEBYSCORE</a>
	 */
	Set<V> rangeByScore(double min, double max);

	/**
	 * Get set of {@link Tuple}s where score is between {@code min} and {@code max} from sorted set.
	 *
	 * @param min
	 * @param max
	 * @return
	 * @see <a href="http://redis.io/commands/zrangebyscore">Redis Documentation: ZRANGEBYSCORE</a>
	 */
	Set<TypedTuple<V>> rangeByScoreWithScores(double min, double max);

	/**
	 * Get elements in range from {@code start} to {@code end} from sorted set ordered from high to low.
	 *
	 * @param start
	 * @param end
	 * @return
	 * @see <a href="http://redis.io/commands/zrevrange">Redis Documentation: ZREVRANGE</a>
	 */
	Set<V> reverseRange(long start, long end);

	/**
	 * Get set of {@link Tuple}s in range from {@code start} to {@code end} from sorted set ordered from high to low.
	 *
	 * @param start
	 * @param end
	 * @return
	 * @see <a href="http://redis.io/commands/zrevrange">Redis Documentation: ZREVRANGE</a>
	 */
	Set<TypedTuple<V>> reverseRangeWithScores(long start, long end);

	/**
	 * Get elements where score is between {@code min} and {@code max} from sorted set ordered from high to low.
	 *
	 * @param min
	 * @param max
	 * @return
	 * @see <a href="http://redis.io/commands/zrevrange">Redis Documentation: ZREVRANGE</a>
	 */
	Set<V> reverseRangeByScore(double min, double max);

	/**
	 * Get set of {@link Tuple} where score is between {@code min} and {@code max} from sorted set ordered from high to
	 * low.
	 *
	 * @param min
	 * @param max
	 * @return
	 * @see <a href="http://redis.io/commands/zrevrangebyscore">Redis Documentation: ZREVRANGEBYSCORE</a>
	 */
	Set<TypedTuple<V>> reverseRangeByScoreWithScores(double min, double max);

	/**
	 * Count number of elements within sorted set with scores between {@code min} and {@code max}.
	 *
	 * @param min
	 * @param max
	 * @return
	 * @see <a href="http://redis.io/commands/zcount">Redis Documentation: ZCOUNT</a>
	 */
	Long count(double min, double max);

	/**
	 * Returns the number of elements of the sorted set stored with given the bound key.
	 *
	 * @see #zCard()
	 * @return
	 * @see <a href="http://redis.io/commands/zcard">Redis Documentation: ZCARD</a>
	 */
	Long size();

	/**
	 * Get the size of sorted set with the bound key.
	 *
	 * @return
	 * @since 1.3
	 * @see <a href="http://redis.io/commands/zcard">Redis Documentation: ZCARD</a>
	 */
	Long zCard();

	/**
	 * Get the score of element with {@code value} from sorted set with key the bound key.
	 *
	 * @param o the value.
	 * @return
	 * @see <a href="http://redis.io/commands/zrem">Redis Documentation: ZREM</a>
	 */
	Double score(Object o);

	/**
	 * Remove elements in range between {@code start} and {@code end} from sorted set with the bound key.
	 *
	 * @param start
	 * @param end
	 * @return
	 * @see <a href="http://redis.io/commands/zremrangebyrank">Redis Documentation: ZREMRANGEBYRANK</a>
	 */
	void removeRange(long start, long end);

	/**
	 * Remove elements with scores between {@code min} and {@code max} from sorted set with the bound key.
	 *
	 * @param min
	 * @param max
	 * @return
	 * @see <a href="http://redis.io/commands/zremrangebyscore">Redis Documentation: ZREMRANGEBYSCORE</a>
	 */
	void removeRangeByScore(double min, double max);

	/**
	 * Union sorted sets at the bound key and {@code otherKeys} and store result in destination {@code destKey}.
	 *
	 * @param otherKey must not be {@literal null}.
	 * @param destKey must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/zunionstore">Redis Documentation: ZUNIONSTORE</a>
	 */
	void unionAndStore(K otherKey, K destKey);

	/**
	 * Union sorted sets at the bound key and {@code otherKeys} and store result in destination {@code destKey}.
	 *
	 * @param otherKeys must not be {@literal null}.
	 * @param destKey must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/zunionstore">Redis Documentation: ZUNIONSTORE</a>
	 */
	void unionAndStore(Collection<K> otherKeys, K destKey);

	/**
	 * Intersect sorted sets at the bound key and {@code otherKey} and store result in destination {@code destKey}.
	 *
	 * @param otherKey must not be {@literal null}.
	 * @param destKey must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/zinterstore">Redis Documentation: ZINTERSTORE</a>
	 */
	void intersectAndStore(K otherKey, K destKey);

	/**
	 * Intersect sorted sets at the bound key and {@code otherKeys} and store result in destination {@code destKey}.
	 *
	 * @param otherKeys must not be {@literal null}.
	 * @param destKey must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/zinterstore">Redis Documentation: ZINTERSTORE</a>
	 */
	void intersectAndStore(Collection<K> otherKeys, K destKey);

	/**
	 * Iterate over elements in zset at the bound key. <br />
	 * <strong>Important:</strong> Call {@link Cursor#close()} when done to avoid resource leak.
	 *
	 * @param options
	 * @return
	 * @since 1.4
	 */
	Cursor<TypedTuple<V>> scan(ScanOptions options);

	/**
	 * Get all elements with lexicographical ordering with a value between {@link Range#getMin()} and
	 * {@link Range#getMax()}.
	 *
	 * @param range must not be {@literal null}.
	 * @since 1.7
	 * @see <a href="http://redis.io/commands/zrangebylex">Redis Documentation: ZRANGEBYLEX</a>
	 */
	Set<V> rangeByLex(Range range);

	/**
	 * Get all elements {@literal n} elements, where {@literal n = } {@link Limit#getCount()}, starting at
	 * {@link Limit#getOffset()} with lexicographical ordering having a value between {@link Range#getMin()} and
	 * {@link Range#getMax()}.
	 *
	 * @param range must not be {@literal null}.
	 * @param limit can be {@literal null}.
	 * @return
	 * @since 1.7
	 * @see <a href="http://redis.io/commands/zrangebylex">Redis Documentation: ZRANGEBYLEX</a>
	 */
	Set<V> rangeByLex(Range range, Limit limit);

	RedisOperations<K, V> getOperations();
}
