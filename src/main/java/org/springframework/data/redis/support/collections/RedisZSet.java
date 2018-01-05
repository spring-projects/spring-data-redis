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
package org.springframework.data.redis.support.collections;

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.SortedSet;

import org.springframework.data.redis.connection.RedisZSetCommands.Limit;
import org.springframework.data.redis.connection.RedisZSetCommands.Range;
import org.springframework.data.redis.core.BoundZSetOperations;
import org.springframework.data.redis.core.ZSetOperations.TypedTuple;

/**
 * Redis ZSet (or sorted set (by weight)). Acts as a {@link SortedSet} based on the given priorities or weights
 * associated with each item.
 * <p/>
 * Since using a {@link Comparator} does not apply, a ZSet implements the {@link SortedSet} methods where applicable.
 *
 * @author Costin Leau
 * @author Mark Paluch
 * @author Christoph Strobl
 */
public interface RedisZSet<E> extends RedisCollection<E>, Set<E> {

	RedisZSet<E> intersectAndStore(RedisZSet<?> set, String destKey);

	RedisZSet<E> intersectAndStore(Collection<? extends RedisZSet<?>> sets, String destKey);

	RedisZSet<E> unionAndStore(RedisZSet<?> set, String destKey);

	RedisZSet<E> unionAndStore(Collection<? extends RedisZSet<?>> sets, String destKey);

	Set<E> range(long start, long end);

	Set<E> reverseRange(long start, long end);

	/**
	 * Get all elements with lexicographical ordering with a value between {@link Range#getMin()} and
	 * {@link Range#getMax()}.
	 *
	 * @param range must not be {@literal null}.
	 * @return
	 * @see BoundZSetOperations#rangeByLex(Range)
	 * @since 1.7
	 */
	Set<E> rangeByLex(Range range);

	/**
	 * Get all elements {@literal n} elements, where {@literal n = } {@link Limit#getCount()}, starting at
	 * {@link Limit#getOffset()} with lexicographical ordering having a value between {@link Range#getMin()} and
	 * {@link Range#getMax()}.
	 *
	 * @param range must not be {@literal null}.
	 * @param limit can be {@literal null}.
	 * @return
	 * @see BoundZSetOperations#rangeByLex(Range, Limit)
	 * @since 1.7
	 */
	Set<E> rangeByLex(Range range, Limit limit);

	Set<E> rangeByScore(double min, double max);

	Set<E> reverseRangeByScore(double min, double max);

	Set<TypedTuple<E>> rangeWithScores(long start, long end);

	Set<TypedTuple<E>> reverseRangeWithScores(long start, long end);

	Set<TypedTuple<E>> rangeByScoreWithScores(double min, double max);

	Set<TypedTuple<E>> reverseRangeByScoreWithScores(double min, double max);

	RedisZSet<E> remove(long start, long end);

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
	 * Returns the last (highest) element currently in this sorted set.
	 *
	 * @return the last (highest) element currently in this sorted set.
	 * @throws NoSuchElementException sorted set is empty.
	 */
	E last();

	/**
	 * @since 1.4
	 * @return
	 */
	Iterator<E> scan();
}
