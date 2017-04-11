/*
 * Copyright 2017 the original author or authors.
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
package org.springframework.data.redis.core;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collection;

import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.RedisZSetCommands.Limit;
import org.springframework.data.redis.connection.RedisZSetCommands.Tuple;
import org.springframework.data.redis.core.ZSetOperations.TypedTuple;

/**
 * Redis ZSet/sorted set specific operations.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @see <a href="http://redis.io/commands#zset">Redis Documentation: Sorted Set Commands</a>
 * @since 2.0
 */
public interface ReactiveZSetOperations<K, V> {

	/**
	 * Add {@code value} to a sorted set at {@code key}, or update its {@code score} if it already exists.
	 *
	 * @param key must not be {@literal null}.
	 * @param score the score.
	 * @param value the value.
	 * @return
	 * @see <a href="http://redis.io/commands/zadd">Redis Documentation: ZADD</a>
	 */
	Mono<Boolean> add(K key, V value, double score);

	/**
	 * Add {@code tuples} to a sorted set at {@code key}, or update their score if it already exists.
	 *
	 * @param key must not be {@literal null}.
	 * @param tuples the score.
	 * @return
	 * @see <a href="http://redis.io/commands/zadd">Redis Documentation: ZADD</a>
	 */
	Mono<Long> addAll(K key, Collection<? extends TypedTuple<V>> tuples);

	/**
	 * Remove {@code values} from sorted set. Return number of removed elements.
	 *
	 * @param key must not be {@literal null}.
	 * @param values must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/zrem">Redis Documentation: ZREM</a>
	 */
	Mono<Long> remove(K key, Object... values);

	/**
	 * Increment the score of element with {@code value} in sorted set by {@code increment}.
	 *
	 * @param key must not be {@literal null}.
	 * @param delta
	 * @param value the value.
	 * @return
	 * @see <a href="http://redis.io/commands/zincrby">Redis Documentation: ZINCRBY</a>
	 */
	Mono<Double> incrementScore(K key, V value, double delta);

	/**
	 * Determine the index of element with {@code value} in a sorted set.
	 *
	 * @param key must not be {@literal null}.
	 * @param o the value.
	 * @return
	 * @see <a href="http://redis.io/commands/zrank">Redis Documentation: ZRANK</a>
	 */
	Mono<Long> rank(K key, Object o);

	/**
	 * Determine the index of element with {@code value} in a sorted set when scored high to low.
	 *
	 * @param key must not be {@literal null}.
	 * @param o the value.
	 * @return
	 * @see <a href="http://redis.io/commands/zrevrank">Redis Documentation: ZREVRANK</a>
	 */
	Mono<Long> reverseRank(K key, Object o);

	/**
	 * Get elements between {@code start} and {@code end} from sorted set.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/zrange">Redis Documentation: ZRANGE</a>
	 */
	Flux<V> range(K key, Range<Long> range);

	/**
	 * Get set of {@link Tuple}s between {@code start} and {@code end} from sorted set.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/zrange">Redis Documentation: ZRANGE</a>
	 */
	Flux<TypedTuple<V>> rangeWithScores(K key, Range<Long> range);

	/**
	 * Get elements where score is between {@code min} and {@code max} from sorted set.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/zrangebyscore">Redis Documentation: ZRANGEBYSCORE</a>
	 */
	Flux<V> rangeByScore(K key, Range<Double> range);

	/**
	 * Get set of {@link Tuple}s where score is between {@code min} and {@code max} from sorted set.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/zrangebyscore">Redis Documentation: ZRANGEBYSCORE</a>
	 */
	Flux<TypedTuple<V>> rangeByScoreWithScores(K key, Range<Double> range);

	/**
	 * Get elements in range from {@code start} to {@code end} where score is between {@code min} and {@code max} from
	 * sorted set.
	 *
	 * @param key must not be {@literal null}.
	 * @param range
	 * @param limit
	 * @return
	 * @see <a href="http://redis.io/commands/zrangebyscore">Redis Documentation: ZRANGEBYSCORE</a>
	 */
	Flux<V> rangeByScore(K key, Range<Double> range, Limit limit);

	/**
	 * Get set of {@link Tuple}s in range from {@code start} to {@code end} where score is between {@code min} and
	 * {@code max} from sorted set.
	 *
	 * @param key
	 * @param range
	 * @param limit
	 * @return
	 * @see <a href="http://redis.io/commands/zrangebyscore">Redis Documentation: ZRANGEBYSCORE</a>
	 */
	Flux<TypedTuple<V>> rangeByScoreWithScores(K key, Range<Double> range, Limit limit);

	/**
	 * Get elements in range from {@code start} to {@code end} from sorted set ordered from high to low.
	 *
	 * @param key must not be {@literal null}.
	 * @param range
	 * @return
	 * @see <a href="http://redis.io/commands/zrevrange">Redis Documentation: ZREVRANGE</a>
	 */
	Flux<V> reverseRange(K key, Range<Long> range);

	/**
	 * Get set of {@link Tuple}s in range from {@code start} to {@code end} from sorted set ordered from high to low.
	 *
	 * @param key must not be {@literal null}.
	 * @param range
	 * @return
	 * @see <a href="http://redis.io/commands/zrevrange">Redis Documentation: ZREVRANGE</a>
	 */
	Flux<TypedTuple<V>> reverseRangeWithScores(K key, Range<Long> range);

	/**
	 * Get elements where score is between {@code min} and {@code max} from sorted set ordered from high to low.
	 *
	 * @param key must not be {@literal null}.
	 * @param range
	 * @return
	 * @see <a href="http://redis.io/commands/zrevrange">Redis Documentation: ZREVRANGE</a>
	 */
	Flux<V> reverseRangeByScore(K key, Range<Double> range);

	/**
	 * Get set of {@link Tuple} where score is between {@code min} and {@code max} from sorted set ordered from high to
	 * low.
	 *
	 * @param key must not be {@literal null}.
	 * @param range
	 * @return
	 * @see <a href="http://redis.io/commands/zrevrangebyscore">Redis Documentation: ZREVRANGEBYSCORE</a>
	 */
	Flux<TypedTuple<V>> reverseRangeByScoreWithScores(K key, Range<Double> range);

	/**
	 * Get elements in range from {@code start} to {@code end} where score is between {@code min} and {@code max} from
	 * sorted set ordered high -> low.
	 *
	 * @param key must not be {@literal null}.
	 * @param range
	 * @param limit
	 * @return
	 * @see <a href="http://redis.io/commands/zrevrangebyscore">Redis Documentation: ZREVRANGEBYSCORE</a>
	 */
	Flux<V> reverseRangeByScore(K key, Range<Double> range, Limit limit);

	/**
	 * Get set of {@link Tuple} in range from {@code start} to {@code end} where score is between {@code min} and
	 * {@code max} from sorted set ordered high -> low.
	 *
	 * @param key must not be {@literal null}.
	 * @param range
	 * @param limit
	 * @return
	 * @see <a href="http://redis.io/commands/zrevrangebyscore">Redis Documentation: ZREVRANGEBYSCORE</a>
	 */
	Flux<TypedTuple<V>> reverseRangeByScoreWithScores(K key, Range<Double> range, Limit limit);

	/**
	 * Count number of elements within sorted set with scores between {@code min} and {@code max}.
	 *
	 * @param key must not be {@literal null}.
	 * @param range
	 * @return
	 * @see <a href="http://redis.io/commands/zcount">Redis Documentation: ZCOUNT</a>
	 */
	Mono<Long> count(K key, Range<Double> range);

	/**
	 * Returns the number of elements of the sorted set stored with given {@code key}.
	 *
	 * @param key
	 * @return
	 * @see <a href="http://redis.io/commands/zcard">Redis Documentation: ZCARD</a>
	 */
	Mono<Long> size(K key);

	/**
	 * Get the score of element with {@code value} from sorted set with key {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param o the value.
	 * @return
	 * @see <a href="http://redis.io/commands/zrem">Redis Documentation: ZREM</a>
	 */
	Mono<Double> score(K key, Object o);

	/**
	 * Remove elements in range between {@code start} and {@code end} from sorted set with {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param range
	 * @return
	 * @see <a href="http://redis.io/commands/zremrangebyrank">Redis Documentation: ZREMRANGEBYRANK</a>
	 */
	Mono<Long> removeRange(K key, Range<Long> range);

	/**
	 * Remove elements with scores between {@code min} and {@code max} from sorted set with {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param range
	 * @return
	 * @see <a href="http://redis.io/commands/zremrangebyscore">Redis Documentation: ZREMRANGEBYSCORE</a>
	 */
	Mono<Long> removeRangeByScore(K key, Range<Double> range);

	/**
	 * Union sorted sets at {@code key} and {@code otherKeys} and store result in destination {@code destKey}.
	 *
	 * @param key must not be {@literal null}.
	 * @param otherKey must not be {@literal null}.
	 * @param destKey must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/zunionstore">Redis Documentation: ZUNIONSTORE</a>
	 */
	Mono<Long> unionAndStore(K key, K otherKey, K destKey);

	/**
	 * Union sorted sets at {@code key} and {@code otherKeys} and store result in destination {@code destKey}.
	 *
	 * @param key must not be {@literal null}.
	 * @param otherKeys must not be {@literal null}.
	 * @param destKey must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/zunionstore">Redis Documentation: ZUNIONSTORE</a>
	 */
	Mono<Long> unionAndStore(K key, Collection<K> otherKeys, K destKey);

	/**
	 * Intersect sorted sets at {@code key} and {@code otherKey} and store result in destination {@code destKey}.
	 *
	 * @param key must not be {@literal null}.
	 * @param otherKey must not be {@literal null}.
	 * @param destKey must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/zinterstore">Redis Documentation: ZINTERSTORE</a>
	 */
	Mono<Long> intersectAndStore(K key, K otherKey, K destKey);

	/**
	 * Intersect sorted sets at {@code key} and {@code otherKeys} and store result in destination {@code destKey}.
	 *
	 * @param key must not be {@literal null}.
	 * @param otherKeys must not be {@literal null}.
	 * @param destKey must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/zinterstore">Redis Documentation: ZINTERSTORE</a>
	 */
	Mono<Long> intersectAndStore(K key, Collection<K> otherKeys, K destKey);

	/**
	 * Get all elements with lexicographical ordering from {@literal ZSET} at {@code key} with a value between
	 * {@link Range#getLowerBound()} and {@link Range#getUpperBound()}.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @see <a href="http://redis.io/commands/zrangebylex">Redis Documentation: ZRANGEBYLEX</a>
	 */
	Flux<V> rangeByLex(K key, Range<String> range);

	/**
	 * Get all elements {@literal n} elements, where {@literal n = } {@link Limit#getCount()}, starting at
	 * {@link Limit#getOffset()} with lexicographical ordering from {@literal ZSET} at {@code key} with a value between
	 * {@link Range#getLowerBound()} and {@link Range#getUpperBound()}.
	 *
	 * @param key must not be {@literal null}
	 * @param range must not be {@literal null}.
	 * @param limit can be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/zrangebylex">Redis Documentation: ZRANGEBYLEX</a>
	 */
	Flux<V> rangeByLex(K key, Range<String> range, Limit limit);

	/**
	 * Get all elements with reverse lexicographical ordering from {@literal ZSET} at {@code key} with a value between
	 * {@link Range#getLowerBound()} and {@link Range#getUpperBound()}.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @see <a href="http://redis.io/commands/zrevrangebylex">Redis Documentation: ZREVRANGEBYLEX</a>
	 */
	Flux<V> reverseRangeByLex(K key, Range<String> range);

	/**
	 * Get all elements {@literal n} elements, where {@literal n = } {@link Limit#getCount()}, starting at
	 * {@link Limit#getOffset()} with reverse lexicographical ordering from {@literal ZSET} at {@code key} with a value
	 * between {@link Range#getLowerBound()} and {@link Range#getUpperBound()}.
	 *
	 * @param key must not be {@literal null}
	 * @param range must not be {@literal null}.
	 * @param limit can be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/zrevrangebylex">Redis Documentation: ZREVRANGEBYLEX</a>
	 */
	Flux<V> reverseRangeByLex(K key, Range<String> range, Limit limit);

	/**
	 * Removes the given {@literal key}.
	 *
	 * @param key must not be {@literal null}.
	 */
	Mono<Boolean> delete(K key);
}
