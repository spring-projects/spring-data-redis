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
 * Default implementation for {@link BoundSetOperations}.
 * 
 * @author Costin Leau
 */
class DefaultBoundSetOperations<K, V> extends DefaultKeyBound<K> implements BoundSetOperations<K, V> {

	private final SetOperations<K, V> ops;


	/**
	 * Constructs a new <code>DefaultBoundSetOperations</code> instance.
	 *
	 * @param key
	 * @param operations
	 */
	DefaultBoundSetOperations(K key, RedisOperations<K, V> operations) {
		super(key);
		this.ops = operations.getSetOps();
	}

	@Override
	public Boolean add(V value) {
		return ops.add(getKey(), value);
	}

	@Override
	public Set<V> diff(Collection<K> keys) {
		return ops.diff(getKey(), keys);
	}

	@Override
	public void diffAndStore(K destKey, Collection<K> keys) {
		ops.diffAndStore(getKey(), destKey, keys);
	}

	@Override
	public RedisOperations<K, V> getOperations() {
		return ops.getOperations();
	}

	@Override
	public Set<V> intersect(Collection<K> keys) {
		return ops.intersect(getKey(), keys);
	}

	@Override
	public void intersectAndStore(K destKey, Collection<K> keys) {
		ops.intersectAndStore(getKey(), destKey, keys);
	}

	@Override
	public Boolean isMember(Object o) {
		return ops.isMember(getKey(), o);
	}

	@Override
	public Set<V> members() {
		return ops.members(getKey());
	}

	@Override
	public Boolean remove(Object o) {
		return ops.remove(getKey(), o);
	}

	@Override
	public V pop() {
		return ops.pop(getKey());
	}

	@Override
	public Long size() {
		return ops.size(getKey());
	}

	@Override
	public Set<V> union(Collection<K> keys) {
		return ops.union(getKey(), keys);
	}

	@Override
	public void unionAndStore(K destKey, Collection<K> keys) {
		ops.unionAndStore(getKey(), destKey, keys);
	}
}