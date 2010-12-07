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
 * Set operations bound to a certain key.
 * 
 * @author Costin Leau
 */
public interface BoundSetOperations<K, V> extends KeyBound<K> {

	RedisOperations<K, V> getOperations();

	Set<V> diff(Collection<K> keys);

	void diffAndStore(K destKey, Collection<K> keys);

	Set<V> intersect(Collection<K> keys);

	void intersectAndStore(K destKey, Collection<K> keys);

	Set<V> union(Collection<K> keys);

	void unionAndStore(K destKey, Collection<K> keys);

	Boolean add(V value);

	Boolean isMember(Object o);

	Set<V> members();

	Boolean remove(Object o);

	Long size();
}
