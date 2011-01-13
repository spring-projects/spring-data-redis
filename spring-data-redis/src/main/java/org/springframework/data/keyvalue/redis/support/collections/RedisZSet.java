/*
 * Copyright 2010-2011  original author or authors.
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
package org.springframework.data.keyvalue.redis.support.collections;

import java.util.Collection;
import java.util.Comparator;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.SortedSet;

/**
 * Redis ZSet (or sorted set (by weight)). Acts as a {@link SortedSet} based on the given priorities or weights associated with each item.
 * <p/>
 * Since using a {@link Comparator} does not apply, a ZSet implements the {@link SortedSet} methods where applicable.
 * 
 * @author Costin Leau
 */
public interface RedisZSet<E> extends RedisCollection<E>, Set<E> {

	RedisZSet<E> intersectAndStore(String destKey, Collection<? extends RedisZSet<?>> sets);

	RedisZSet<E> unionAndStore(String destKey, Collection<? extends RedisZSet<?>> sets);

	Set<E> range(long start, long end);

	Set<E> reverseRange(long start, long end);

	Set<E> rangeByScore(double min, double max);

	RedisZSet<E> remove(long start, long end);

	RedisZSet<E> removeByScore(double min, double max);

	/**
	 * Adds an element to the set with the given score, or updates the score if
	 * the element exists.
	 * 
	 * @param e element to add
	 * @param score element score
	 * @return true if a new element was added, false otherwise (only the score has been updated)
	 */
	boolean add(E e, double score);

	/**
	 * Adds an element to the set with a default score. Equivalent to
	 * {@code add(e, getDefaultScore())}.
	 *  
	 * 
	 * The score value is implementation specific.
	 * 
	 * {@inheritDoc}
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
	 * Returns the rank (position) of the given element in the set, in ascending order. 
	 * Returns null if the element is not contained by the set.
	 * 
	 * @param o object
	 * @return rank of the given object
	 */
	Long rank(Object o);

	/**
	 * Returns the rank (position) of the given element in the set, in descending order.
	 * Returns null if the element is not contained by the set.
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
	 * @throws    NoSuchElementException sorted set is empty.
	 */
	E first();

	/**
	 * Returns the last (highest) element currently in this sorted set.
	 *
	 * @return the last (highest) element currently in this sorted set.
	 * @throws    NoSuchElementException sorted set is empty.
	 */
	E last();
}