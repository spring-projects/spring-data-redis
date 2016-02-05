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

/**
 * Redis ZSet/sorted set specific operations.
 * 
 * @author Costin Leau
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public interface ZSetOperations<K, V> {

	/**
	 * Typed ZSet tuple.
	 */
	public interface TypedTuple<V> extends Comparable<TypedTuple<V>> {
		V getValue();

		Double getScore();
	}

	Long intersectAndStore(K key, K otherKey, K destKey);

	Long intersectAndStore(K key, Collection<K> otherKeys, K destKey);

	Long unionAndStore(K key, K otherKey, K destKey);

	Long unionAndStore(K key, Collection<K> otherKeys, K destKey);

	Set<V> range(K key, long start, long end);

	Set<V> reverseRange(K key, long start, long end);

	Set<TypedTuple<V>> rangeWithScores(K key, long start, long end);

	Set<TypedTuple<V>> reverseRangeWithScores(K key, long start, long end);

	Set<V> rangeByLex(K key, RedisZSetCommands.Range range);

	Set<V> rangeByLex(K key, RedisZSetCommands.Range range, RedisZSetCommands.Limit limit);

	Set<V> rangeByScore(K key, double min, double max);

	Set<V> rangeByScore(K key, double min, double max, long offset, long count);

	Set<V> reverseRangeByScore(K key, double min, double max);

	Set<V> reverseRangeByScore(K key, double min, double max, long offset, long count);

	Set<TypedTuple<V>> rangeByScoreWithScores(K key, double min, double max);

	Set<TypedTuple<V>> rangeByScoreWithScores(K key, double min, double max, long offset, long count);

	Set<TypedTuple<V>> reverseRangeByScoreWithScores(K key, double min, double max);

	Set<TypedTuple<V>> reverseRangeByScoreWithScores(K key, double min, double max, long offset, long count);

	Boolean add(K key, V value, double score);

	Long add(K key, Set<TypedTuple<V>> tuples);

	Double incrementScore(K key, V value, double delta);

	Long rank(K key, Object o);

	Long reverseRank(K key, Object o);

	Double score(K key, Object o);

	Long remove(K key, Object... values);

	Long removeRange(K key, long start, long end);

	Long removeRangeByScore(K key, double min, double max);

	Long count(K key, double min, double max);

	/**
	 * Returns the number of elements of the sorted set stored with given {@code key}.
	 * 
	 * @see #zCard(Object)
	 * @param key
	 * @return
	 */
	Long size(K key);

	/**
	 * Returns the number of elements of the sorted set stored with given {@code key}.
	 * 
	 * @param key
	 * @return
	 * @since 1.3
	 */
	Long zCard(K key);

	RedisOperations<K, V> getOperations();

	/**
	 * @since 1.4
	 * @param key
	 * @param options
	 * @return
	 */
	Cursor<TypedTuple<V>> scan(K key, ScanOptions options);
}
