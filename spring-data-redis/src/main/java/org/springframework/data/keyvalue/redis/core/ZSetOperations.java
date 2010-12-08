/*
 * Copyright 2010 the original author or authors.
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

package org.springframework.data.keyvalue.redis.core;

import java.util.Collection;
import java.util.Set;

/**
 * Redis ZSet/sorted set specific operations.
 * 
 * @author Costin Leau
 */
public interface ZSetOperations<K, V> {

	void intersectAndStore(K key, K destKey, Collection<K> keys);

	Set<V> range(K key, long start, long end);

	Set<V> rangeByScore(K key, double min, double max);

	Set<V> reverseRange(K key, long start, long end);

	void unionAndStore(K key, K destKey, Collection<K> keys);

	Boolean add(K key, V value, double score);

	Long rank(K key, Object o);

	Long reverseRank(K key, Object o);

	Double score(K key, Object o);

	Boolean remove(K key, Object o);

	void removeRange(K key, long start, long end);

	void removeRangeByScore(K key, double min, double max);

	Long size(K key);

	RedisOperations<K, V> getOperations();
}
