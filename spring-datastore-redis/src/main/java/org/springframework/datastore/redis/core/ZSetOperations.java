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

package org.springframework.datastore.redis.core;

import java.util.Set;

/**
 * Redis ZSet/sorted set specific operations.
 * 
 * @author Costin Leau
 */
public interface ZSetOperations<K, V> {

	void intersectAndStore(K key, K destKey, K... keys);

	Set<V> range(K key, int start, int end);

	Set<V> rangeByScore(K key, double min, double max);

	void removeRange(K key, int start, int end);

	void removeRangeByScore(K key, double min, double max);

	void unionAndStore(K key, K destKey, K... keys);

	boolean add(K key, V value, double score);

	Integer rank(K key, Object o);

	boolean remove(K key, Object o);

	int size(K key);

	Set<V> reverseRange(K key, int start, int end);

	RedisOperations<K, V> getOperations();
}
