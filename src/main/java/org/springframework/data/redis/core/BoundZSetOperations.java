/*
 * Copyright 2011-2014 the original author or authors.
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

import java.util.Collection;
import java.util.Set;

import org.springframework.data.redis.connection.RedisZSetCommands;
import org.springframework.data.redis.core.ZSetOperations.TypedTuple;

/**
 * ZSet (or SortedSet) operations bound to a certain key.
 * 
 * @author Costin Leau
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public interface BoundZSetOperations<K, V> extends BoundKeyOperations<K> {

	RedisOperations<K, V> getOperations();

	void intersectAndStore(K otherKey, K destKey);

	void intersectAndStore(Collection<K> otherKeys, K destKey);

	Set<V> range(long start, long end);

	Set<V> rangeByScore(double min, double max);

	Set<V> reverseRange(long start, long end);

	Set<V> reverseRangeByScore(double min, double max);

	Set<TypedTuple<V>> rangeWithScores(long start, long end);

	Set<TypedTuple<V>> rangeByScoreWithScores(double min, double max);

	Set<TypedTuple<V>> reverseRangeWithScores(long start, long end);

	Set<TypedTuple<V>> reverseRangeByScoreWithScores(double min, double max);

	Set<V> rangeByLex(RedisZSetCommands.Range range);

	Set<V> rangeByLex(RedisZSetCommands.Range range, RedisZSetCommands.Limit limit);

	void removeRange(long start, long end);

	void removeRangeByScore(double min, double max);

	void unionAndStore(K otherKey, K destKey);

	void unionAndStore(Collection<K> otherKeys, K destKey);

	Boolean add(V value, double score);

	Long add(Set<TypedTuple<V>> tuples);

	Double incrementScore(V value, double delta);

	Long rank(Object o);

	Long reverseRank(Object o);

	Long remove(Object... values);

	Long count(double min, double max);

	/**
	 * Returns the number of elements of the sorted set.
	 * 
	 * @return
	 * @see #zCard()
	 */
	Long size();

	/**
	 * Returns the number of elements of the sorted set.
	 * 
	 * @return
	 * @since 1.3
	 */
	Long zCard();

	Double score(Object o);

	/**
	 * @param options
	 * @return
	 * @since 1.4
	 */
	Cursor<TypedTuple<V>> scan(ScanOptions options);
}
