/*
 * Copyright 2010-2011 the original author or authors.
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

/**
 * Redis ZSet/sorted set specific operations.
 * 
 * @author Costin Leau
 */
public interface ZSetOperations<K, V> {

	/**
	 * Typed ZSet tuple. 
	 */
	public interface TypedTuple<V> {
		V getValue();

		Double getScore();
	}

	void intersectAndStore(K key, K otherKey, K destKey);

	void intersectAndStore(K key, Collection<K> otherKeys, K destKey);

	void unionAndStore(K key, K otherKey, K destKey);

	void unionAndStore(K key, Collection<K> otherKeys, K destKey);

	Set<V> range(K key, long start, long end);

	Set<V> reverseRange(K key, long start, long end);

	Set<TypedTuple<V>> rangeWithScores(K key, long start, long end);

	Set<TypedTuple<V>> reverseRangeWithScores(K key, long start, long end);

	Set<V> rangeByScore(K key, double min, double max);

	Set<V> reverseRangeByScore(K key, double min, double max);

	Set<TypedTuple<V>> rangeByScoreWithScores(K key, double min, double max);

	Set<TypedTuple<V>> reverseRangeByScoreWithScores(K key, double min, double max);

	Boolean add(K key, V value, double score);

	Double incrementScore(K key, V value, double delta);

	Long rank(K key, Object o);

	Long reverseRank(K key, Object o);

	Double score(K key, Object o);

	Boolean remove(K key, Object o);

	void removeRange(K key, long start, long end);

	void removeRangeByScore(K key, double min, double max);

	Long count(K key, double min, double max);

	Long size(K key);

	RedisOperations<K, V> getOperations();
}