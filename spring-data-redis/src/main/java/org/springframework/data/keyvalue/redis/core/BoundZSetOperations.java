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

import java.util.Set;


/**
 * ZSet (or SortedSet) operations bound to a certain key.
 * 
 * @author Costin Leau
 */
public interface BoundZSetOperations<K, V> extends KeyBound<K> {

	RedisOperations<K, V> getOperations();

	void intersectAndStore(K destKey, K... keys);

	Set<V> range(int start, int end);

	Set<V> rangeByScore(double min, double max);

	void removeRange(int start, int end);

	void removeRangeByScore(double min, double max);

	void unionAndStore(K destKey, K... keys);

	boolean add(V value, double score);

	Integer rank(Object o);

	boolean remove(Object o);

	int size();

	Set<V> reverseRange(int start, int end);
}
