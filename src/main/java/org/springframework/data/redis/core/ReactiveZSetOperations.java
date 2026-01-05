/*
 * Copyright 2017-present the original author or authors.
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

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.NullUnmarked;
import org.jspecify.annotations.Nullable;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.Limit;
import org.springframework.data.redis.connection.zset.Aggregate;
import org.springframework.data.redis.connection.zset.Tuple;
import org.springframework.data.redis.connection.zset.Weights;
import org.springframework.data.redis.core.ZSetOperations.TypedTuple;

/**
 * Reactive Redis operations for Sorted (ZSet) Commands.
 * <p>
 * Streams of methods returning {@code Mono<K>} or {@code Flux<M>} are terminated with
 * {@link org.springframework.dao.InvalidDataAccessApiUsageException} when
 * {@link org.springframework.data.redis.serializer.RedisElementReader#read(ByteBuffer)} returns {@literal null} for a
 * particular element as Reactive Streams prohibit the usage of {@literal null} values.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @author Andrey Shlykov
 * @see <a href="https://redis.io/commands#zset">Redis Documentation: Sorted Set Commands</a>
 * @since 2.0
 */
@NullUnmarked
public interface ReactiveZSetOperations<K, V> {

	/**
	 * Add {@code value} to a sorted set at {@code key}, or update its {@code score} if it already exists.
	 *
	 * @param key must not be {@literal null}.
	 * @param value the value.
	 * @param score the score.
	 * @return
	 * @see <a href="https://redis.io/commands/zadd">Redis Documentation: ZADD</a>
	 */
	Mono<Boolean> add(@NonNull K key, @NonNull V value, double score);

	/**
	 * Add {@code tuples} to a sorted set at {@code key}, or update their score if it already exists.
	 *
	 * @param key must not be {@literal null}.
	 * @param tuples the score.
	 * @return
	 * @see <a href="https://redis.io/commands/zadd">Redis Documentation: ZADD</a>
	 */
	Mono<Long> addAll(@NonNull K key, @NonNull Collection<? extends @NonNull TypedTuple<V>> tuples);

	/**
	 * Remove {@code values} from sorted set. Return number of removed elements.
	 *
	 * @param key must not be {@literal null}.
	 * @param values must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/zrem">Redis Documentation: ZREM</a>
	 */
	Mono<Long> remove(@NonNull K key, @NonNull Object @NonNull... values);

	/**
	 * Increment the score of element with {@code value} in sorted set by {@code increment}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value the value.
	 * @param delta the delta to add. Can be negative.
	 * @return
	 * @see <a href="https://redis.io/commands/zincrby">Redis Documentation: ZINCRBY</a>
	 */
	Mono<Double> incrementScore(@NonNull K key, @NonNull V value, double delta);

	/**
	 * Get random element from set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zrandmember">Redis Documentation: ZRANDMEMBER</a>
	 */
	Mono<V> randomMember(@NonNull K key);

	/**
	 * Get {@code count} distinct random elements from set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param count number of members to return.
	 * @return
	 * @throws IllegalArgumentException if count is negative.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zrandmember">Redis Documentation: ZRANDMEMBER</a>
	 */
	Flux<V> distinctRandomMembers(@NonNull K key, long count);

	/**
	 * Get {@code count} random elements from set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param count number of members to return.
	 * @return
	 * @throws IllegalArgumentException if count is negative.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zrandmember">Redis Documentation: ZRANDMEMBER</a>
	 */
	Flux<V> randomMembers(@NonNull K key, long count);

	/**
	 * Get random element with its score from set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zrandmember">Redis Documentation: ZRANDMEMBER</a>
	 */
	Mono<TypedTuple<V>> randomMemberWithScore(@NonNull K key);

	/**
	 * Get {@code count} distinct random elements with their score from set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param count number of members to return.
	 * @return
	 * @throws IllegalArgumentException if count is negative.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zrandmember">Redis Documentation: ZRANDMEMBER</a>
	 */
	Flux<TypedTuple<V>> distinctRandomMembersWithScore(@NonNull K key, long count);

	/**
	 * Get {@code count} random elements with their score from set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param count number of members to return.
	 * @return
	 * @throws IllegalArgumentException if count is negative.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zrandmember">Redis Documentation: ZRANDMEMBER</a>
	 */
	Flux<TypedTuple<V>> randomMembersWithScore(@NonNull K key, long count);

	/**
	 * Determine the index of element with {@code value} in a sorted set.
	 *
	 * @param key must not be {@literal null}.
	 * @param o the value.
	 * @return
	 * @see <a href="https://redis.io/commands/zrank">Redis Documentation: ZRANK</a>
	 */
	Mono<Long> rank(@NonNull K key, @NonNull Object o);

	/**
	 * Determine the index of element with {@code value} in a sorted set when scored high to low.
	 *
	 * @param key must not be {@literal null}.
	 * @param o the value.
	 * @return
	 * @see <a href="https://redis.io/commands/zrevrank">Redis Documentation: ZREVRANK</a>
	 */
	Mono<Long> reverseRank(@NonNull K key, @NonNull Object o);

	/**
	 * Get elements between {@code start} and {@code end} from sorted set.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/zrange">Redis Documentation: ZRANGE</a>
	 */
	Flux<V> range(@NonNull K key, @NonNull Range<Long> range);

	/**
	 * Get set of {@link Tuple}s between {@code start} and {@code end} from sorted set.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/zrange">Redis Documentation: ZRANGE</a>
	 */
	Flux<TypedTuple<V>> rangeWithScores(@NonNull K key, @NonNull Range<Long> range);

	/**
	 * Get elements where score is between {@code min} and {@code max} from sorted set.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/zrangebyscore">Redis Documentation: ZRANGEBYSCORE</a>
	 */
	Flux<V> rangeByScore(@NonNull K key, @NonNull Range<Double> range);

	/**
	 * Get set of {@link Tuple}s where score is between {@code min} and {@code max} from sorted set.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/zrangebyscore">Redis Documentation: ZRANGEBYSCORE</a>
	 */
	Flux<TypedTuple<V>> rangeByScoreWithScores(@NonNull K key, @NonNull Range<Double> range);

	/**
	 * Get elements in range from {@code start} to {@code end} where score is between {@code min} and {@code max} from
	 * sorted set.
	 *
	 * @param key must not be {@literal null}.
	 * @param range
	 * @param limit
	 * @return
	 * @see <a href="https://redis.io/commands/zrangebyscore">Redis Documentation: ZRANGEBYSCORE</a>
	 */
	Flux<V> rangeByScore(@NonNull K key, @NonNull Range<Double> range, @NonNull Limit limit);

	/**
	 * Get set of {@link Tuple}s in range from {@code start} to {@code end} where score is between {@code min} and
	 * {@code max} from sorted set.
	 *
	 * @param key
	 * @param range
	 * @param limit
	 * @return
	 * @see <a href="https://redis.io/commands/zrangebyscore">Redis Documentation: ZRANGEBYSCORE</a>
	 */
	Flux<TypedTuple<V>> rangeByScoreWithScores(@NonNull K key, @NonNull Range<Double> range, @NonNull Limit limit);

	/**
	 * Get elements in range from {@code start} to {@code end} from sorted set ordered from high to low.
	 *
	 * @param key must not be {@literal null}.
	 * @param range
	 * @return
	 * @see <a href="https://redis.io/commands/zrevrange">Redis Documentation: ZREVRANGE</a>
	 */
	Flux<V> reverseRange(@NonNull K key, @NonNull Range<Long> range);

	/**
	 * Get set of {@link Tuple}s in range from {@code start} to {@code end} from sorted set ordered from high to low.
	 *
	 * @param key must not be {@literal null}.
	 * @param range
	 * @return
	 * @see <a href="https://redis.io/commands/zrevrange">Redis Documentation: ZREVRANGE</a>
	 */
	Flux<TypedTuple<V>> reverseRangeWithScores(@NonNull K key, @NonNull Range<Long> range);

	/**
	 * Get elements where score is between {@code min} and {@code max} from sorted set ordered from high to low.
	 *
	 * @param key must not be {@literal null}.
	 * @param range
	 * @return
	 * @see <a href="https://redis.io/commands/zrevrangebyscore">Redis Documentation: ZREVRANGEBYSCORE</a>
	 */
	Flux<V> reverseRangeByScore(@NonNull K key, @NonNull Range<Double> range);

	/**
	 * Get set of {@link Tuple} where score is between {@code min} and {@code max} from sorted set ordered from high to
	 * low.
	 *
	 * @param key must not be {@literal null}.
	 * @param range
	 * @return
	 * @see <a href="https://redis.io/commands/zrevrangebyscore">Redis Documentation: ZREVRANGEBYSCORE</a>
	 */
	Flux<TypedTuple<V>> reverseRangeByScoreWithScores(@NonNull K key, @NonNull Range<Double> range);

	/**
	 * Get elements in range from {@code start} to {@code end} where score is between {@code min} and {@code max} from
	 * sorted set ordered high -> low.
	 *
	 * @param key must not be {@literal null}.
	 * @param range
	 * @param limit
	 * @return
	 * @see <a href="https://redis.io/commands/zrevrangebyscore">Redis Documentation: ZREVRANGEBYSCORE</a>
	 */
	Flux<V> reverseRangeByScore(@NonNull K key, @NonNull Range<Double> range, @NonNull Limit limit);

	/**
	 * Get set of {@link Tuple} in range from {@code start} to {@code end} where score is between {@code min} and
	 * {@code max} from sorted set ordered high -> low.
	 *
	 * @param key must not be {@literal null}.
	 * @param range
	 * @param limit
	 * @return
	 * @see <a href="https://redis.io/commands/zrevrangebyscore">Redis Documentation: ZREVRANGEBYSCORE</a>
	 */
	Flux<TypedTuple<V>> reverseRangeByScoreWithScores(@NonNull K key, @NonNull Range<Double> range, @NonNull Limit limit);

	/**
	 * Store all elements at {@code dstKey} with lexicographical ordering from {@literal ZSET} at {@code srcKey} with a
	 * value between {@link Range#getLowerBound()} and {@link Range#getUpperBound()}.
	 *
	 * @param srcKey must not be {@literal null}.
	 * @param dstKey must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return the number of stored elements.
	 * @since 3.0
	 * @see <a href="https://redis.io/commands/zrangestore">Redis Documentation: ZRANGESTORE</a>
	 */
	default Mono<Long> rangeAndStoreByLex(@NonNull K srcKey, @NonNull K dstKey, @NonNull Range<String> range) {
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
	 * @return the number of stored elements.
	 * @since 3.0
	 * @see <a href="https://redis.io/commands/zrangestore">Redis Documentation: ZRANGESTORE</a>
	 */
	Mono<Long> rangeAndStoreByLex(@NonNull K srcKey, @NonNull K dstKey, @NonNull Range<String> range,
			@NonNull Limit limit);

	/**
	 * Store all elements at {@code dstKey} with reverse lexicographical ordering from {@literal ZSET} at {@code srcKey}
	 * with a value between {@link Range#getLowerBound()} and {@link Range#getUpperBound()}.
	 *
	 * @param srcKey must not be {@literal null}.
	 * @param dstKey must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return the number of stored elements.
	 * @since 3.0
	 * @see <a href="https://redis.io/commands/zrangestore">Redis Documentation: ZRANGESTORE</a>
	 */
	default Mono<Long> reverseRangeAndStoreByLex(@NonNull K srcKey, @NonNull K dstKey, @NonNull Range<String> range) {
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
	 * @return the number of stored elements.
	 * @since 3.0
	 * @see #reverseRangeByLex(Object, Range, Limit)
	 * @see <a href="https://redis.io/commands/zrangestore">Redis Documentation: ZRANGESTORE</a>
	 */
	Mono<Long> reverseRangeAndStoreByLex(@NonNull K srcKey, @NonNull K dstKey, @NonNull Range<String> range,
			@NonNull Limit limit);

	/**
	 * Store all elements at {@code dstKey} with ordering by score from {@literal ZSET} at {@code srcKey} with a score
	 * between {@link Range#getLowerBound()} and {@link Range#getUpperBound()}.
	 *
	 * @param srcKey must not be {@literal null}.
	 * @param dstKey must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return the number of stored elements.
	 * @since 3.0
	 * @see <a href="https://redis.io/commands/zrangestore">Redis Documentation: ZRANGESTORE</a>
	 */
	default @Nullable Mono<Long> rangeAndStoreByScore(@NonNull K srcKey, @NonNull K dstKey,
			@NonNull Range<Double> range) {
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
	 * @return the number of stored elements.
	 * @since 3.0
	 * @see <a href="https://redis.io/commands/zrangestore">Redis Documentation: ZRANGESTORE</a>
	 */
	Mono<Long> rangeAndStoreByScore(@NonNull K srcKey, @NonNull K dstKey, @NonNull Range<Double> range,
			@NonNull Limit limit);

	/**
	 * Store all elements at {@code dstKey} with reverse ordering by score from {@literal ZSET} at {@code srcKey} with a
	 * score between {@link Range#getLowerBound()} and {@link Range#getUpperBound()}.
	 *
	 * @param srcKey must not be {@literal null}.
	 * @param dstKey must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return the number of stored elements.
	 * @since 3.0
	 * @see <a href="https://redis.io/commands/zrangestore">Redis Documentation: ZRANGESTORE</a>
	 */
	default Mono<Long> reverseRangeAndStoreByScore(@NonNull K srcKey, @NonNull K dstKey, @NonNull Range<Double> range) {
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
	 * @return the number of stored elements.
	 * @since 3.0
	 * @see <a href="https://redis.io/commands/zrangestore">Redis Documentation: ZRANGESTORE</a>
	 */
	Mono<Long> reverseRangeAndStoreByScore(@NonNull K srcKey, @NonNull K dstKey, @NonNull Range<Double> range,
			@NonNull Limit limit);

	/**
	 * Use a {@link Flux} to iterate over entries in the sorted set at {@code key}. The resulting {@link Flux} acts as a
	 * cursor and issues {@code ZSCAN} commands itself as long as the subscriber signals demand.
	 *
	 * @param key must not be {@literal null}.
	 * @return the {@link Flux} emitting the {@literal values} one by one or an {@link Flux#empty() empty Flux} if none
	 *         exist.
	 * @throws IllegalArgumentException when given {@code key} is {@literal null}.
	 * @see <a href="https://redis.io/commands/zscan">Redis Documentation: ZSCAN</a>
	 * @since 2.1
	 */
	default Flux<TypedTuple<V>> scan(@NonNull K key) {
		return scan(key, ScanOptions.NONE);
	}

	/**
	 * Use a {@link Flux} to iterate over entries in the sorted set at {@code key} given {@link ScanOptions}. The
	 * resulting {@link Flux} acts as a cursor and issues {@code ZSCAN} commands itself as long as the subscriber signals
	 * demand.
	 *
	 * @param key must not be {@literal null}.
	 * @param options must not be {@literal null}. Use {@link ScanOptions#NONE} instead.
	 * @return the {@link Flux} emitting the {@literal values} one by one or an {@link Flux#empty() empty Flux} if none
	 *         exist.
	 * @throws IllegalArgumentException when one of the required arguments is {@literal null}.
	 * @see <a href="https://redis.io/commands/zscan">Redis Documentation: ZSCAN</a>
	 * @since 2.1
	 */
	Flux<TypedTuple<V>> scan(@NonNull K key, @Nullable ScanOptions options);

	/**
	 * Count number of elements within sorted set with scores between {@code min} and {@code max}.
	 *
	 * @param key must not be {@literal null}.
	 * @param range
	 * @return
	 * @see <a href="https://redis.io/commands/zcount">Redis Documentation: ZCOUNT</a>
	 */
	Mono<Long> count(@NonNull K key, @NonNull Range<Double> range);

	/**
	 * Count number of elements within sorted set with a value between {@link Range#getLowerBound()} and
	 * {@link Range#getUpperBound()} applying lexicographical ordering.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}
	 * @return
	 * @since 2.4
	 * @see <a href="https://redis.io/commands/ZLEXCOUNT">Redis Documentation: ZLEXCOUNT</a>
	 */
	Mono<Long> lexCount(@NonNull K key, @NonNull Range<String> range);

	/**
	 * Remove and return the value with its score having the lowest score from sorted set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/zpopmin">Redis Documentation: ZPOPMIN</a>
	 * @since 2.6
	 */
	Mono<TypedTuple<V>> popMin(@NonNull K key);

	/**
	 * Remove and return {@code count} values with their score having the lowest score from sorted set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param count number of elements to pop.
	 * @return
	 * @see <a href="https://redis.io/commands/zpopmin">Redis Documentation: ZPOPMIN</a>
	 * @since 2.6
	 */
	Flux<TypedTuple<V>> popMin(@NonNull K key, long count);

	/**
	 * Remove and return the value with its score having the lowest score from sorted set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param timeout maximal duration to wait until an entry in the list at {@code key} is available. Must be either
	 *          {@link Duration#ZERO} or greater {@literal 1 second}, must not be {@literal null}. A timeout of zero can
	 *          be used to wait indefinitely. Durations between zero and one second are not supported.
	 * @return
	 * @see <a href="https://redis.io/commands/zpopmin">Redis Documentation: ZPOPMIN</a>
	 * @since 2.6
	 */
	Mono<TypedTuple<V>> popMin(@NonNull K key, Duration timeout);

	/**
	 * Remove and return the value with its score having the highest score from sorted set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/zpopmax">Redis Documentation: ZPOPMAX</a>
	 * @since 2.6
	 */
	Mono<TypedTuple<V>> popMax(@NonNull K key);

	/**
	 * Remove and return {@code count} values with their score having the highest score from sorted set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param count number of elements to pop.
	 * @return
	 * @see <a href="https://redis.io/commands/zpopmax">Redis Documentation: ZPOPMAX</a>
	 * @since 2.6
	 */
	Flux<TypedTuple<V>> popMax(@NonNull K key, long count);

	/**
	 * Remove and return the value with its score having the highest score from sorted set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param timeout maximal duration to wait until an entry in the list at {@code key} is available. Must be either
	 *          {@link Duration#ZERO} or greater {@literal 1 second}, must not be {@literal null}. A timeout of zero can
	 *          be used to wait indefinitely. Durations between zero and one second are not supported.
	 * @return
	 * @see <a href="https://redis.io/commands/zpopmin">Redis Documentation: ZPOPMIN</a>
	 * @since 2.6
	 */
	Mono<TypedTuple<V>> popMax(@NonNull K key, @NonNull Duration timeout);

	/**
	 * Returns the number of elements of the sorted set stored with given {@code key}.
	 *
	 * @param key
	 * @return
	 * @see <a href="https://redis.io/commands/zcard">Redis Documentation: ZCARD</a>
	 */
	Mono<Long> size(@NonNull K key);

	/**
	 * Get the score of element with {@code value} from sorted set with key {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param o the value.
	 * @return
	 * @see <a href="https://redis.io/commands/zscore">Redis Documentation: ZSCORE</a>
	 */
	Mono<Double> score(@NonNull K key, @NonNull Object o);

	/**
	 * Get the scores of elements with {@code values} from sorted set with key {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param o the values.
	 * @return
	 * @see <a href="https://redis.io/commands/zmscore">Redis Documentation: ZMSCORE</a>
	 * @since 2.6
	 */
	Mono<List<Double>> score(@NonNull K key, @NonNull Object @NonNull... o);

	/**
	 * Remove elements in range between {@code start} and {@code end} from sorted set with {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param range
	 * @return
	 * @see <a href="https://redis.io/commands/zremrangebyrank">Redis Documentation: ZREMRANGEBYRANK</a>
	 */
	Mono<Long> removeRange(@NonNull K key, @NonNull Range<Long> range);

	/**
	 * Remove elements in range from sorted set with {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return a {@link Mono} emitting the number or removed elements.
	 * @since 2.5
	 * @see <a href="https://redis.io/commands/zremrangebyrank">Redis Documentation: ZREMRANGEBYRANK</a>
	 */
	Mono<Long> removeRangeByLex(@NonNull K key, @NonNull Range<String> range);

	/**
	 * Remove elements with scores between {@code min} and {@code max} from sorted set with {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param range
	 * @return
	 * @see <a href="https://redis.io/commands/zremrangebyscore">Redis Documentation: ZREMRANGEBYSCORE</a>
	 */
	Mono<Long> removeRangeByScore(@NonNull K key, @NonNull Range<Double> range);

	/**
	 * Diff sorted {@code sets}.
	 *
	 * @param key must not be {@literal null}.
	 * @param otherKey must not be {@literal null}.
	 * @return
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zdiff">Redis Documentation: ZDIFF</a>
	 */
	default Flux<V> difference(@NonNull K key, @NonNull K otherKey) {
		return difference(key, Collections.singleton(otherKey));
	}

	/**
	 * Diff sorted {@code sets}.
	 *
	 * @param key must not be {@literal null}.
	 * @param otherKeys must not be {@literal null}.
	 * @return
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zdiff">Redis Documentation: ZDIFF</a>
	 */
	Flux<V> difference(@NonNull K key, @NonNull Collection<@NonNull K> otherKeys);

	/**
	 * Diff sorted {@code sets}.
	 *
	 * @param key must not be {@literal null}.
	 * @param otherKey must not be {@literal null}.
	 * @return
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zdiff">Redis Documentation: ZDIFF</a>
	 */
	default Flux<TypedTuple<V>> differenceWithScores(@NonNull K key, @NonNull K otherKey) {
		return differenceWithScores(key, Collections.singleton(otherKey));
	}

	/**
	 * Diff sorted {@code sets}.
	 *
	 * @param key must not be {@literal null}.
	 * @param otherKeys must not be {@literal null}.
	 * @return
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zdiff">Redis Documentation: ZDIFF</a>
	 */
	Flux<TypedTuple<V>> differenceWithScores(@NonNull K key, @NonNull Collection<@NonNull K> otherKeys);

	/**
	 * Diff sorted {@code sets} and store result in destination {@code destKey}.
	 *
	 * @param key must not be {@literal null}.
	 * @param otherKey must not be {@literal null}.
	 * @param destKey must not be {@literal null}.
	 * @return
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zdiffstore">Redis Documentation: ZDIFFSTORE</a>
	 */
	default Mono<Long> differenceAndStore(@NonNull K key, @NonNull K otherKey, @NonNull K destKey) {
		return differenceAndStore(key, Collections.singleton(otherKey), destKey);
	}

	/**
	 * Diff sorted {@code sets} and store result in destination {@code destKey}.
	 *
	 * @param key must not be {@literal null}.
	 * @param otherKeys must not be {@literal null}.
	 * @param destKey must not be {@literal null}.
	 * @return
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zdiffstore">Redis Documentation: ZDIFFSTORE</a>
	 */
	Mono<Long> differenceAndStore(@NonNull K key, @NonNull Collection<@NonNull K> otherKeys, @NonNull K destKey);

	/**
	 * Intersect sorted {@code sets}.
	 *
	 * @param key must not be {@literal null}.
	 * @param otherKey must not be {@literal null}.
	 * @return
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zinter">Redis Documentation: ZINTER</a>
	 */
	default Flux<V> intersect(@NonNull K key, @NonNull K otherKey) {
		return intersect(key, Collections.singleton(otherKey));
	}

	/**
	 * Intersect sorted {@code sets}.
	 *
	 * @param key must not be {@literal null}.
	 * @param otherKeys must not be {@literal null}.
	 * @return
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zinter">Redis Documentation: ZINTER</a>
	 */
	Flux<V> intersect(@NonNull K key, @NonNull Collection<@NonNull K> otherKeys);

	/**
	 * Intersect sorted {@code sets}.
	 *
	 * @param key must not be {@literal null}.
	 * @param otherKey must not be {@literal null}.
	 * @return
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zinter">Redis Documentation: ZINTER</a>
	 */
	default Flux<TypedTuple<V>> intersectWithScores(@NonNull K key, @NonNull K otherKey) {
		return intersectWithScores(key, Collections.singleton(otherKey));
	}

	/**
	 * Intersect sorted {@code sets}.
	 *
	 * @param key must not be {@literal null}.
	 * @param otherKeys must not be {@literal null}.
	 * @return
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zinter">Redis Documentation: ZINTER</a>
	 */
	Flux<TypedTuple<V>> intersectWithScores(@NonNull K key, @NonNull Collection<@NonNull K> otherKeys);

	/**
	 * Intersect sorted sets at {@code key} and {@code otherKeys} .
	 *
	 * @param key must not be {@literal null}.
	 * @param otherKeys must not be {@literal null}.
	 * @param aggregate must not be {@literal null}.
	 * @return
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zinter">Redis Documentation: ZINTER</a>
	 */
	default Flux<TypedTuple<V>> intersectWithScores(@NonNull K key, @NonNull Collection<K> otherKeys,
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
	Flux<TypedTuple<V>> intersectWithScores(@NonNull K key, @NonNull Collection<@NonNull K> otherKeys,
			@NonNull Aggregate aggregate, @NonNull Weights weights);

	/**
	 * Intersect sorted sets at {@code key} and {@code otherKey} and store result in destination {@code destKey}.
	 *
	 * @param key must not be {@literal null}.
	 * @param otherKey must not be {@literal null}.
	 * @param destKey must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/zinterstore">Redis Documentation: ZINTERSTORE</a>
	 */
	default Mono<Long> intersectAndStore(@NonNull K key, @NonNull K otherKey, @NonNull K destKey) {
		return intersectAndStore(key, Collections.singleton(otherKey), destKey);
	}

	/**
	 * Intersect sorted sets at {@code key} and {@code otherKeys} and store result in destination {@code destKey}.
	 *
	 * @param key must not be {@literal null}.
	 * @param otherKeys must not be {@literal null}.
	 * @param destKey must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/zinterstore">Redis Documentation: ZINTERSTORE</a>
	 */
	Mono<Long> intersectAndStore(@NonNull K key, @NonNull Collection<@NonNull K> otherKeys, @NonNull K destKey);

	/**
	 * Intersect sorted sets at {@code key} and {@code otherKeys} and store result in destination {@code destKey}.
	 *
	 * @param key must not be {@literal null}.
	 * @param otherKeys must not be {@literal null}.
	 * @param destKey must not be {@literal null}.
	 * @param aggregate must not be {@literal null}.
	 * @return
	 * @since 2.1
	 * @see <a href="https://redis.io/commands/zinterstore">Redis Documentation: ZINTERSTORE</a>
	 */
	default Mono<Long> intersectAndStore(@NonNull K key, @NonNull Collection<@NonNull K> otherKeys, @NonNull K destKey,
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
	 * @return
	 * @since 2.1
	 * @see <a href="https://redis.io/commands/zinterstore">Redis Documentation: ZINTERSTORE</a>
	 */
	Mono<Long> intersectAndStore(@NonNull K key, @NonNull Collection<@NonNull K> otherKeys, @NonNull K destKey,
			@NonNull Aggregate aggregate, @NonNull Weights weights);

	/**
	 * Union sorted {@code sets}.
	 *
	 * @param key must not be {@literal null}.
	 * @param otherKey must not be {@literal null}.
	 * @return
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zunion">Redis Documentation: ZUNION</a>
	 */
	default Flux<V> union(@NonNull K key, @NonNull K otherKey) {
		return union(key, Collections.singleton(otherKey));
	}

	/**
	 * Union sorted {@code sets}.
	 *
	 * @param key must not be {@literal null}.
	 * @param otherKeys must not be {@literal null}.
	 * @return
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zunion">Redis Documentation: ZUNION</a>
	 */
	Flux<V> union(@NonNull K key, @NonNull Collection<@NonNull K> otherKeys);

	/**
	 * Union sorted {@code sets}.
	 *
	 * @param key must not be {@literal null}.
	 * @param otherKey must not be {@literal null}.
	 * @return
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zunion">Redis Documentation: ZUNION</a>
	 */
	default Flux<TypedTuple<V>> unionWithScores(@NonNull K key, @NonNull K otherKey) {
		return unionWithScores(key, Collections.singleton(otherKey));
	}

	/**
	 * Union sorted {@code sets}.
	 *
	 * @param key must not be {@literal null}.
	 * @param otherKeys must not be {@literal null}.
	 * @return
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zunion">Redis Documentation: ZUNION</a>
	 */
	Flux<TypedTuple<V>> unionWithScores(@NonNull K key, @NonNull Collection<@NonNull K> otherKeys);

	/**
	 * Union sorted sets at {@code key} and {@code otherKeys} .
	 *
	 * @param key must not be {@literal null}.
	 * @param otherKeys must not be {@literal null}.
	 * @param aggregate must not be {@literal null}.
	 * @return
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zunion">Redis Documentation: ZUNION</a>
	 */
	default Flux<TypedTuple<V>> unionWithScores(@NonNull K key, @NonNull Collection<@NonNull K> otherKeys,
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
	 * @return
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zunion">Redis Documentation: ZUNION</a>
	 */
	Flux<TypedTuple<V>> unionWithScores(@NonNull K key, @NonNull Collection<@NonNull K> otherKeys,
			@NonNull Aggregate aggregate, @NonNull Weights weights);

	/**
	 * Union sorted sets at {@code key} and {@code otherKeys} and store result in destination {@code destKey}.
	 *
	 * @param key must not be {@literal null}.
	 * @param otherKey must not be {@literal null}.
	 * @param destKey must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/zunionstore">Redis Documentation: ZUNIONSTORE</a>
	 */
	Mono<Long> unionAndStore(@NonNull K key, @NonNull K otherKey, @NonNull K destKey);

	/**
	 * Union sorted sets at {@code key} and {@code otherKeys} and store result in destination {@code destKey}.
	 *
	 * @param key must not be {@literal null}.
	 * @param otherKeys must not be {@literal null}.
	 * @param destKey must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/zunionstore">Redis Documentation: ZUNIONSTORE</a>
	 */
	Mono<Long> unionAndStore(@NonNull K key, @NonNull Collection<@NonNull K> otherKeys, @NonNull K destKey);

	/**
	 * Union sorted sets at {@code key} and {@code otherKeys} and store result in destination {@code destKey}.
	 *
	 * @param key must not be {@literal null}.
	 * @param otherKeys must not be {@literal null}.
	 * @param destKey must not be {@literal null}.
	 * @param aggregate must not be {@literal null}.
	 * @return
	 * @since 2.1
	 * @see <a href="https://redis.io/commands/zunionstore">Redis Documentation: ZUNIONSTORE</a>
	 */
	default Mono<Long> unionAndStore(@NonNull K key, @NonNull Collection<@NonNull K> otherKeys, @NonNull K destKey,
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
	 * @return
	 * @since 2.1
	 * @see <a href="https://redis.io/commands/zunionstore">Redis Documentation: ZUNIONSTORE</a>
	 */
	Mono<Long> unionAndStore(@NonNull K key, @NonNull Collection<@NonNull K> otherKeys, @NonNull K destKey,
			@NonNull Aggregate aggregate, @NonNull Weights weights);

	/**
	 * Get all elements with lexicographical ordering from {@literal ZSET} at {@code key} with a value between
	 * {@link Range#getLowerBound()} and {@link Range#getUpperBound()}.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @see <a href="https://redis.io/commands/zrangebylex">Redis Documentation: ZRANGEBYLEX</a>
	 */
	Flux<V> rangeByLex(@NonNull K key, @NonNull Range<String> range);

	/**
	 * Get all elements {@literal n} elements, where {@literal n = } {@link Limit#getCount()}, starting at
	 * {@link Limit#getOffset()} with lexicographical ordering from {@literal ZSET} at {@code key} with a value between
	 * {@link Range#getLowerBound()} and {@link Range#getUpperBound()}.
	 *
	 * @param key must not be {@literal null}
	 * @param range must not be {@literal null}.
	 * @param limit can be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/zrangebylex">Redis Documentation: ZRANGEBYLEX</a>
	 */
	Flux<V> rangeByLex(@NonNull K key, @NonNull Range<String> range, @NonNull Limit limit);

	/**
	 * Get all elements with reverse lexicographical ordering from {@literal ZSET} at {@code key} with a value between
	 * {@link Range#getLowerBound()} and {@link Range#getUpperBound()}.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @see <a href="https://redis.io/commands/zrevrangebylex">Redis Documentation: ZREVRANGEBYLEX</a>
	 */
	Flux<V> reverseRangeByLex(@NonNull K key, @NonNull Range<String> range);

	/**
	 * Get all elements {@literal n} elements, where {@literal n = } {@link Limit#getCount()}, starting at
	 * {@link Limit#getOffset()} with reverse lexicographical ordering from {@literal ZSET} at {@code key} with a value
	 * between {@link Range#getLowerBound()} and {@link Range#getUpperBound()}.
	 *
	 * @param key must not be {@literal null}
	 * @param range must not be {@literal null}.
	 * @param limit can be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/zrevrangebylex">Redis Documentation: ZREVRANGEBYLEX</a>
	 */
	Flux<V> reverseRangeByLex(@NonNull K key, @NonNull Range<String> range, @NonNull Limit limit);

	/**
	 * Removes the given {@literal key}.
	 *
	 * @param key must not be {@literal null}.
	 */
	Mono<Boolean> delete(@NonNull K key);

}
