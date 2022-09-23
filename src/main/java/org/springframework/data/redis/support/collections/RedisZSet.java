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
package org.springframework.data.redis.support.collections;

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.TimeUnit;

import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.Limit;
import org.springframework.data.redis.connection.zset.Tuple;
import org.springframework.data.redis.core.BoundZSetOperations;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.ZSetOperations.TypedTuple;

/**
 * Redis ZSet (or sorted set (by weight)). Acts as a {@link SortedSet} based on the given priorities or weights
 * associated with each item.
 * <p>
 * Since using a {@link Comparator} does not apply, a ZSet implements the {@link SortedSet} methods where applicable.
 *
 * @author Costin Leau
 * @author Mark Paluch
 * @author Christoph Strobl
 * @author Andrey Shlykov
 */
public interface RedisZSet<E> extends RedisCollection<E>, Set<E> {

	/**
	 * Constructs a new {@link RedisZSet} instance with a default score of {@literal 1}.
	 *
	 * @param key Redis key of this set.
	 * @param operations {@link RedisOperations} for the value type of this set.
	 * @since 2.6
	 */
	static <E> RedisZSet<E> create(String key, RedisOperations<String, E> operations) {
		return new DefaultRedisZSet<>(key, operations, 1);
	}

	/**
	 * Constructs a new {@link RedisZSet} instance.
	 *
	 * @param key Redis key of this set.
	 * @param operations {@link RedisOperations} for the value type of this set.
	 * @param defaultScore
	 * @since 2.6
	 */
	static <E> RedisZSet<E> create(String key, RedisOperations<String, E> operations, double defaultScore) {
		return new DefaultRedisZSet<>(key, operations, defaultScore);
	}

	/**
	 * Diff this set and another {@link RedisZSet}.
	 *
	 * @param set must not be {@literal null}.
	 * @return a {@link Set} containing the values that differ.
	 * @since 2.6
	 */
	Set<E> diff(RedisZSet<?> set);

	/**
	 * Diff this set and other {@link RedisZSet}s.
	 *
	 * @param sets must not be {@literal null}.
	 * @return a {@link Set} containing the values that differ.
	 * @since 2.6
	 */
	Set<E> diff(Collection<? extends RedisZSet<?>> sets);

	/**
	 * Diff this set and another {@link RedisZSet}.
	 *
	 * @param set must not be {@literal null}.
	 * @return a {@link Set} containing the values that differ with their scores.
	 * @since 2.6
	 */
	Set<TypedTuple<E>> diffWithScores(RedisZSet<?> set);

	/**
	 * Diff this set and other {@link RedisZSet}s.
	 *
	 * @param sets must not be {@literal null}.
	 * @return a {@link Set} containing the values that differ with their scores.
	 * @since 2.6
	 */
	Set<TypedTuple<E>> diffWithScores(Collection<? extends RedisZSet<?>> sets);

	/**
	 * Create a new {@link RedisZSet} by diffing this sorted set and {@link RedisZSet} and store result in destination
	 * {@code destKey}.
	 *
	 * @param set must not be {@literal null}.
	 * @param destKey must not be {@literal null}.
	 * @return a new {@link RedisZSet} pointing at {@code destKey}.
	 * @since 2.6
	 */
	RedisZSet<E> diffAndStore(RedisZSet<?> set, String destKey);

	/**
	 * Create a new {@link RedisZSet} by diffing this sorted set and the collection {@link RedisZSet} and store result in
	 * destination {@code destKey}.
	 *
	 * @param sets must not be {@literal null}.
	 * @param destKey must not be {@literal null}.
	 * @return a new {@link RedisZSet} pointing at {@code destKey}.
	 * @since 2.6
	 */
	RedisZSet<E> diffAndStore(Collection<? extends RedisZSet<?>> sets, String destKey);

	/**
	 * Intersect this set and another {@link RedisZSet}.
	 *
	 * @param set must not be {@literal null}.
	 * @return a {@link Set} containing the intersecting values.
	 * @since 2.6
	 */
	Set<E> intersect(RedisZSet<?> set);

	/**
	 * Intersect this set and other {@link RedisZSet}s.
	 *
	 * @param sets must not be {@literal null}.
	 * @return a {@link Set} containing the intersecting values.
	 * @since 2.6
	 */
	Set<E> intersect(Collection<? extends RedisZSet<?>> sets);

	/**
	 * Intersect this set and another {@link RedisZSet}.
	 *
	 * @param set must not be {@literal null}.
	 * @return a {@link Set} containing the intersecting values with their scores.
	 * @since 2.6
	 */
	Set<TypedTuple<E>> intersectWithScores(RedisZSet<?> set);

	/**
	 * Intersect this set and other {@link RedisZSet}s.
	 *
	 * @param sets must not be {@literal null}.
	 * @return a {@link Set} containing the intersecting values with their scores.
	 * @since 2.6
	 */
	Set<TypedTuple<E>> intersectWithScores(Collection<? extends RedisZSet<?>> sets);

	/**
	 * Create a new {@link RedisZSet} by intersecting this sorted set and {@link RedisZSet} and store result in
	 * destination {@code destKey}.
	 *
	 * @param set must not be {@literal null}.
	 * @param destKey must not be {@literal null}.
	 * @return a new {@link RedisZSet} pointing at {@code destKey}
	 */
	RedisZSet<E> intersectAndStore(RedisZSet<?> set, String destKey);

	/**
	 * Create a new {@link RedisZSet} by intersecting this sorted set and the collection {@link RedisZSet} and store
	 * result in destination {@code destKey}.
	 *
	 * @param sets must not be {@literal null}.
	 * @param destKey must not be {@literal null}.
	 * @return a new {@link RedisZSet} pointing at {@code destKey}
	 */
	RedisZSet<E> intersectAndStore(Collection<? extends RedisZSet<?>> sets, String destKey);

	/**
	 * Union this set and another {@link RedisZSet}.
	 *
	 * @param set must not be {@literal null}.
	 * @return a {@link Set} containing the combined values.
	 * @since 2.6
	 */
	Set<E> union(RedisZSet<?> set);

	/**
	 * Union this set and other {@link RedisZSet}s.
	 *
	 * @param sets must not be {@literal null}.
	 * @return a {@link Set} containing the combined values.
	 * @since 2.6
	 */
	Set<E> union(Collection<? extends RedisZSet<?>> sets);

	/**
	 * Union this set and another {@link RedisZSet}.
	 *
	 * @param set must not be {@literal null}.
	 * @return a {@link Set} containing the combined values with their scores.
	 * @since 2.6
	 */
	Set<TypedTuple<E>> unionWithScores(RedisZSet<?> set);

	/**
	 * Union this set and other {@link RedisZSet}s.
	 *
	 * @param sets must not be {@literal null}.
	 * @return a {@link Set} containing the combined values with their scores.
	 * @since 2.6
	 */
	Set<TypedTuple<E>> unionWithScores(Collection<? extends RedisZSet<?>> sets);

	/**
	 * Create a new {@link RedisZSet} by union this sorted set and {@link RedisZSet} and store result in destination
	 * {@code destKey}.
	 *
	 * @param set must not be {@literal null}.
	 * @param destKey must not be {@literal null}.
	 * @return a new {@link RedisZSet} pointing at {@code destKey}
	 */
	RedisZSet<E> unionAndStore(RedisZSet<?> set, String destKey);

	/**
	 * Create a new {@link RedisZSet} by union this sorted set and the collection {@link RedisZSet} and store result in
	 * destination {@code destKey}.
	 *
	 * @param sets must not be {@literal null}.
	 * @param destKey must not be {@literal null}.
	 * @return a new {@link RedisZSet} pointing at {@code destKey}
	 */
	RedisZSet<E> unionAndStore(Collection<? extends RedisZSet<?>> sets, String destKey);

	/**
	 * Get random element from the set.
	 *
	 * @return
	 * @since 2.6
	 */
	E randomValue();

	/**
	 * Get elements between {@code start} and {@code end} from sorted set.
	 *
	 * @param start
	 * @param end
	 * @return
	 */
	Set<E> range(long start, long end);

	/**
	 * Get elements in range from {@code start} to {@code end} from sorted set ordered from high to low.
	 *
	 * @param start
	 * @param end
	 * @return
	 */
	Set<E> reverseRange(long start, long end);

	/**
	 * Get all elements with lexicographical ordering with a value between {@link Range#getLowerBound()} and
	 * {@link Range#getUpperBound()}.
	 *
	 * @param range must not be {@literal null}.
	 * @return
	 * @see BoundZSetOperations#rangeByLex(Range)
	 * @since 1.7
	 * @deprecated since 3.0. Please use {@link #rangeByLex(Range)} instead.
	 */
	@Deprecated(since = "3.0", forRemoval = true)
	default Set<E> rangeByLex(org.springframework.data.redis.connection.RedisZSetCommands.Range range) {
		return rangeByLex(range.toRange());
	}

	/**
	 * Get all elements with lexicographical ordering with a value between {@link Range#getLowerBound()} and
	 * {@link Range#getUpperBound()}.
	 *
	 * @param range must not be {@literal null}.
	 * @return
	 * @see BoundZSetOperations#rangeByLex(Range)
	 * @since 3.0
	 */
	default Set<E> rangeByLex(Range<String> range) {
		return rangeByLex(range, Limit.unlimited());
	}

	/**
	 * Get all elements {@literal n} elements, where {@literal n = } {@link Limit#getCount()}, starting at
	 * {@link Limit#getOffset()} with lexicographical ordering having a value between {@link Range#getLowerBound()} and
	 * {@link Range#getUpperBound()}.
	 *
	 * @param range must not be {@literal null}.
	 * @param limit can be {@literal null}.
	 * @return
	 * @since 1.7
	 * @see BoundZSetOperations#rangeByLex(Range, Limit)
	 * @deprecated since 3.0. Please use {@link #rangeByLex(Range, Limit)} instead.
	 */
	@Deprecated(since = "3.0", forRemoval = true)
	default Set<E> rangeByLex(org.springframework.data.redis.connection.RedisZSetCommands.Range range, Limit limit) {
		return rangeByLex(range.toRange(), limit);
	}

	/**
	 * Get all elements {@literal n} elements, where {@literal n = } {@link Limit#getCount()}, starting at
	 * {@link Limit#getOffset()} with lexicographical ordering having a value between {@link Range#getLowerBound()} and
	 * {@link Range#getUpperBound()}.
	 *
	 * @param range must not be {@literal null}.
	 * @param limit can be {@literal null}.
	 * @return
	 * @since 3.0
	 * @see BoundZSetOperations#rangeByLex(Range, Limit)
	 */
	Set<E> rangeByLex(Range<String> range, Limit limit);

	/**
	 * Get all elements with reverse lexicographical ordering with a value between {@link Range#getLowerBound()} and
	 * {@link Range#getUpperBound()}.
	 *
	 * @param range must not be {@literal null}.
	 * @return
	 * @since 2.4
	 * @see BoundZSetOperations#reverseRangeByLex(Range)
	 * @deprecated since 3.0. Please use {@link #reverseRangeByLex(Range, Limit)} instead.
	 */
	@Deprecated(since = "3.0", forRemoval = true)
	default Set<E> reverseRangeByLex(org.springframework.data.redis.connection.RedisZSetCommands.Range range) {
		return reverseRangeByLex(range.toRange());
	}

	/**
	 * Get all elements with reverse lexicographical ordering with a value between {@link Range#getLowerBound()} and
	 * {@link Range#getUpperBound()}.
	 *
	 * @param range must not be {@literal null}.
	 * @return
	 * @since 3.0
	 * @see BoundZSetOperations#reverseRangeByLex(Range)
	 */
	default Set<E> reverseRangeByLex(Range<String> range) {
		return reverseRangeByLex(range, Limit.unlimited());
	}

	/**
	 * Get all elements {@literal n} elements, where {@literal n = } {@link Limit#getCount()}, starting at
	 * {@link Limit#getOffset()} with reverse lexicographical ordering having a value between
	 * {@link Range#getLowerBound()} and {@link Range#getUpperBound()}.
	 *
	 * @param range must not be {@literal null}.
	 * @param limit can be {@literal null}.
	 * @return
	 * @since 2.4
	 * @see BoundZSetOperations#reverseRangeByLex(Range, Limit)
	 * @deprecated since 3.0. Please use {@link #reverseRangeByLex(Range, Limit)} instead.
	 */
	@Deprecated(since = "3.0", forRemoval = true)
	default Set<E> reverseRangeByLex(org.springframework.data.redis.connection.RedisZSetCommands.Range range,
			Limit limit) {
		return reverseRangeByLex(range.toRange(), limit);
	}

	/**
	 * Get all elements {@literal n} elements, where {@literal n = } {@link Limit#getCount()}, starting at
	 * {@link Limit#getOffset()} with reverse lexicographical ordering having a value between
	 * {@link Range#getLowerBound()} and {@link Range#getUpperBound()}.
	 *
	 * @param range must not be {@literal null}.
	 * @param limit can be {@literal null}.
	 * @return
	 * @since 3.0
	 * @see BoundZSetOperations#reverseRangeByLex(Range, Limit)
	 */
	Set<E> reverseRangeByLex(Range<String> range, Limit limit);

	/**
	 * Get elements where score is between {@code min} and {@code max} from sorted set.
	 *
	 * @param min
	 * @param max
	 * @return
	 */
	Set<E> rangeByScore(double min, double max);

	/**
	 * Get elements where score is between {@code min} and {@code max} from sorted set ordered from high to low.
	 *
	 * @param min
	 * @param max
	 * @return
	 */
	Set<E> reverseRangeByScore(double min, double max);

	/**
	 * Get set of {@link Tuple}s between {@code start} and {@code end} from sorted set.
	 *
	 * @param start
	 * @param end
	 * @return
	 */
	Set<TypedTuple<E>> rangeWithScores(long start, long end);

	/**
	 * Get set of {@link Tuple}s in range from {@code start} to {@code end} from sorted set ordered from high to low.
	 *
	 * @param start
	 * @param end
	 * @return
	 */
	Set<TypedTuple<E>> reverseRangeWithScores(long start, long end);

	/**
	 * Get set of {@link Tuple}s where score is between {@code min} and {@code max} from sorted set.
	 *
	 * @param min
	 * @param max
	 * @return
	 */
	Set<TypedTuple<E>> rangeByScoreWithScores(double min, double max);

	/**
	 * Get set of {@link Tuple}s where score is between {@code min} and {@code max} from sorted set ordered from high to
	 * low.
	 *
	 * @param min
	 * @param max
	 * @return
	 */
	Set<TypedTuple<E>> reverseRangeByScoreWithScores(double min, double max);

	/**
	 * Store all elements at {@code dstKey} with lexicographical ordering from {@literal ZSET} at the bound key with a
	 * value between {@link Range#getLowerBound()} and {@link Range#getUpperBound()}.
	 *
	 * @param dstKey must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return a new {@link RedisZSet} pointing at {@code destKey}
	 * @since 3.0
	 * @see #rangeByLex(Range)
	 */
	default RedisZSet<E> rangeAndStoreByLex(String dstKey, Range<String> range) {
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
	 * @return a new {@link RedisZSet} pointing at {@code destKey}
	 * @since 3.0
	 * @see #rangeByLex(Range, Limit)
	 */
	RedisZSet<E> rangeAndStoreByLex(String dstKey, Range<String> range, Limit limit);

	/**
	 * Store all elements at {@code dstKey} with reverse lexicographical ordering from {@literal ZSET} at the bound key
	 * with a value between {@link Range#getLowerBound()} and {@link Range#getUpperBound()}.
	 *
	 * @param dstKey must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return a new {@link RedisZSet} pointing at {@code destKey}
	 * @since 3.0
	 * @see #reverseRangeByLex(Range)
	 */
	default RedisZSet<E> reverseRangeAndStoreByLex(String dstKey, Range<String> range) {
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
	 * @return a new {@link RedisZSet} pointing at {@code destKey}
	 * @since 3.0
	 * @see #reverseRangeByLex(Range, Limit)
	 */
	RedisZSet<E> reverseRangeAndStoreByLex(String dstKey, Range<String> range, Limit limit);

	/**
	 * Store all elements at {@code dstKey} with ordering by score from {@literal ZSET} at the bound key with a score
	 * between {@link Range#getLowerBound()} and {@link Range#getUpperBound()}.
	 *
	 * @param dstKey must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return a new {@link RedisZSet} pointing at {@code destKey}
	 * @since 3.0
	 * @see #rangeByScore(double, double)
	 */
	default RedisZSet<E> rangeAndStoreByScore(String dstKey, Range<? extends Number> range) {
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
	 * @return a new {@link RedisZSet} pointing at {@code destKey}
	 * @since 3.0
	 * @see #rangeByScore(double, double)
	 */
	RedisZSet<E> rangeAndStoreByScore(String dstKey, Range<? extends Number> range, Limit limit);

	/**
	 * Store all elements at {@code dstKey} with reverse ordering by score from {@literal ZSET} at the bound key with a
	 * score between {@link Range#getLowerBound()} and {@link Range#getUpperBound()}.
	 *
	 * @param dstKey must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return a new {@link RedisZSet} pointing at {@code destKey}
	 * @since 3.0
	 * @see #reverseRangeByScore(double, double)
	 */
	default RedisZSet<E> reverseRangeAndStoreByScore(String dstKey, Range<? extends Number> range) {
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
	 * @return a new {@link RedisZSet} pointing at {@code destKey}
	 * @since 3.0
	 */
	RedisZSet<E> reverseRangeAndStoreByScore(String dstKey, Range<? extends Number> range, Limit limit);

	/**
	 * Remove elements in range between {@code start} and {@code end} from sorted set.
	 *
	 * @param start
	 * @param end
	 * @return {@code this} set.
	 */
	RedisZSet<E> remove(long start, long end);

	/**
	 * Remove all elements in range.
	 *
	 * @param range must not be {@literal null}.
	 * @return {@code this} set.
	 * @since 2.5
	 */
	RedisZSet<E> removeByLex(Range<String> range);

	/**
	 * Remove elements with scores between {@code min} and {@code max} from sorted set with the bound key.
	 *
	 * @param min
	 * @param max
	 * @return {@code this} set.
	 */
	RedisZSet<E> removeByScore(double min, double max);

	/**
	 * Adds an element to the set with the given score, or updates the score if the element exists.
	 *
	 * @param e element to add
	 * @param score element score
	 * @return true if a new element was added, false otherwise (only the score has been updated)
	 */
	boolean add(E e, double score);

	/**
	 * Adds an element to the set with a default score. Equivalent to {@code add(e, getDefaultScore())}. The score value
	 * is implementation specific. {@inheritDoc}
	 */
	boolean add(E e);

	/**
	 * Adds an element to the set using the {@link #getDefaultScore() default score} if the element does not already
	 * exists.
	 *
	 * @param e element to add
	 * @return true if a new element was added, false otherwise (only the score has been updated)
	 * @since 2.5
	 */
	default boolean addIfAbsent(E e) {
		return addIfAbsent(e, getDefaultScore());
	}

	/**
	 * Adds an element to the set with the given score if the element does not already exists.
	 *
	 * @param e element to add
	 * @param score element score
	 * @return true if a new element was added, false otherwise (only the score has been updated)
	 * @since 2.5
	 */
	boolean addIfAbsent(E e, double score);

	/**
	 * Count number of elements within sorted set with value between {@code Range#min} and {@code Range#max} applying
	 * lexicographical ordering.
	 *
	 * @param range must not be {@literal null}.
	 * @return
	 * @since 2.4
	 * @see <a href="https://redis.io/commands/zlexcount">Redis Documentation: ZLEXCOUNT</a>
	 */
	Long lexCount(Range<String> range);

	/**
	 * Returns the score of the given element. Returns null if the element is not contained by the set.
	 *
	 * @param o object
	 * @return the score associated with the given object
	 */
	Double score(Object o);

	/**
	 * Returns the rank (position) of the given element in the set, in ascending order. Returns null if the element is not
	 * contained by the set.
	 *
	 * @param o object
	 * @return rank of the given object
	 */
	Long rank(Object o);

	/**
	 * Returns the rank (position) of the given element in the set, in descending order. Returns null if the element is
	 * not contained by the set.
	 *
	 * @param o object
	 * @return reverse rank of the given object
	 */
	Long reverseRank(Object o);

	/**
	 * Returns the default score used by this set.
	 *
	 * @return the default score used by the implementation.
	 */
	Double getDefaultScore();

	/**
	 * Returns the first (lowest) element currently in this sorted set.
	 *
	 * @return the first (lowest) element currently in this sorted set.
	 * @throws NoSuchElementException sorted set is empty.
	 */
	E first();

	/**
	 * Removes the first (lowest) object at the top of this sorted set and returns that object as the value of this
	 * function.
	 *
	 * @return the first (lowest) element currently in this sorted set.
	 * @throws NoSuchElementException sorted set is empty.
	 * @since 2.6
	 */
	E popFirst();

	/**
	 * Removes the first (lowest) object at the top of this sorted set and returns that object as the value of this
	 * function. <b>Blocks connection</b> until element available or {@code timeout} reached.
	 *
	 * @param timeout
	 * @param unit must not be {@literal null}.
	 * @return the first (lowest) element currently in this sorted set.
	 * @throws NoSuchElementException sorted set is empty.
	 * @since 2.6
	 */
	E popFirst(long timeout, TimeUnit unit);

	/**
	 * Returns the last (highest) element currently in this sorted set.
	 *
	 * @return the last (highest) element currently in this sorted set.
	 * @throws NoSuchElementException sorted set is empty.
	 */
	E last();

	/**
	 * Removes the last (highest) object at the top of this sorted set and returns that object as the value of this
	 * function.
	 *
	 * @return the last (highest) element currently in this sorted set.
	 * @throws NoSuchElementException sorted set is empty.
	 * @since 2.6
	 */
	E popLast();

	/**
	 * Removes the last (highest) object at the top of this sorted set and returns that object as the value of this
	 * function. <b>Blocks connection</b> until element available or {@code timeout} reached.
	 *
	 * @param timeout
	 * @param unit must not be {@literal null}.
	 * @return the last (highest) element currently in this sorted set.
	 * @throws NoSuchElementException sorted set is empty.
	 * @since 2.6
	 */
	E popLast(long timeout, TimeUnit unit);

	/**
	 * @since 1.4
	 * @return
	 */
	Iterator<E> scan();
}
