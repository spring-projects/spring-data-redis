/*
 * Copyright 2011-2013 the original author or authors.
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
import java.util.List;
import java.util.Set;

/**
 * Redis set specific operations.
 * 
 * @author Costin Leau
 */
public interface SetOperations<K, V> {

	Set<V> difference(K key, K otherKey);

	Set<V> difference(K key, Collection<K> otherKeys);

	Long differenceAndStore(K key, K otherKey, K destKey);

	Long differenceAndStore(K key, Collection<K> otherKeys, K destKey);

	Set<V> intersect(K key, K otherKey);

	Set<V> intersect(K key, Collection<K> otherKeys);

	Long intersectAndStore(K key, K otherKey, K destKey);

	Long intersectAndStore(K key, Collection<K> otherKeys, K destKey);

	Set<V> union(K key, K otherKey);

	Set<V> union(K key, Collection<K> otherKeys);

	Long unionAndStore(K key, K otherKey, K destKey);

	Long unionAndStore(K key, Collection<K> otherKeys, K destKey);

	Boolean add(K key, V value);

	Boolean isMember(K key, Object o);

	Set<V> members(K key);

	Boolean move(K key, V value, K destKey);

	V randomMember(K key);

	Set<V> distinctRandomMembers(K key, long count);

	List<V> randomMembers(K key, long count);

	Boolean remove(K key, Object o);

	V pop(K key);

	Long size(K key);

	RedisOperations<K, V> getOperations();
}
