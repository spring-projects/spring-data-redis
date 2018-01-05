/*
 * Copyright 2011-2018 the original author or authors.
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

import org.springframework.data.redis.connection.DataType;

/**
 * Default implementation for {@link BoundSetOperations}.
 *
 * @author Costin Leau
 * @author Christoph Strobl
 */
class DefaultBoundSetOperations<K, V> extends DefaultBoundKeyOperations<K> implements BoundSetOperations<K, V> {

	private final SetOperations<K, V> ops;

	/**
	 * Constructs a new <code>DefaultBoundSetOperations</code> instance.
	 *
	 * @param key
	 * @param operations
	 */
	DefaultBoundSetOperations(K key, RedisOperations<K, V> operations) {

		super(key, operations);
		this.ops = operations.opsForSet();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundSetOperations#add(java.lang.Object[])
	 */
	@Override
	public Long add(V... values) {
		return ops.add(getKey(), values);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundSetOperations#diff(java.lang.Object)
	 */
	@Override
	public Set<V> diff(K key) {
		return ops.difference(getKey(), key);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundSetOperations#diff(java.util.Collection)
	 */
	@Override
	public Set<V> diff(Collection<K> keys) {
		return ops.difference(getKey(), keys);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundSetOperations#diffAndStore(java.lang.Object, java.lang.Object)
	 */
	@Override
	public void diffAndStore(K key, K destKey) {
		ops.differenceAndStore(getKey(), key, destKey);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundSetOperations#diffAndStore(java.util.Collection, java.lang.Object)
	 */
	@Override
	public void diffAndStore(Collection<K> keys, K destKey) {
		ops.differenceAndStore(getKey(), keys, destKey);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundSetOperations#getOperations()
	 */
	@Override
	public RedisOperations<K, V> getOperations() {
		return ops.getOperations();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundSetOperations#intersect(java.lang.Object)
	 */
	@Override
	public Set<V> intersect(K key) {
		return ops.intersect(getKey(), key);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundSetOperations#intersect(java.util.Collection)
	 */
	@Override
	public Set<V> intersect(Collection<K> keys) {
		return ops.intersect(getKey(), keys);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundSetOperations#intersectAndStore(java.lang.Object, java.lang.Object)
	 */
	@Override
	public void intersectAndStore(K key, K destKey) {
		ops.intersectAndStore(getKey(), key, destKey);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundSetOperations#intersectAndStore(java.util.Collection, java.lang.Object)
	 */
	@Override
	public void intersectAndStore(Collection<K> keys, K destKey) {
		ops.intersectAndStore(getKey(), keys, destKey);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundSetOperations#isMember(java.lang.Object)
	 */
	@Override
	public Boolean isMember(Object o) {
		return ops.isMember(getKey(), o);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundSetOperations#members()
	 */
	@Override
	public Set<V> members() {
		return ops.members(getKey());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundSetOperations#move(java.lang.Object, java.lang.Object)
	 */
	@Override
	public Boolean move(K destKey, V value) {
		return ops.move(getKey(), value, destKey);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundSetOperations#randomMember()
	 */
	@Override
	public V randomMember() {
		return ops.randomMember(getKey());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundSetOperations#distinctRandomMembers(long)
	 */
	@Override
	public Set<V> distinctRandomMembers(long count) {
		return ops.distinctRandomMembers(getKey(), count);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundSetOperations#randomMembers(long)
	 */
	@Override
	public List<V> randomMembers(long count) {
		return ops.randomMembers(getKey(), count);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundSetOperations#remove(java.lang.Object[])
	 */
	@Override
	public Long remove(Object... values) {
		return ops.remove(getKey(), values);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundSetOperations#pop()
	 */
	@Override
	public V pop() {
		return ops.pop(getKey());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundSetOperations#size()
	 */
	@Override
	public Long size() {
		return ops.size(getKey());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundSetOperations#union(java.lang.Object)
	 */
	@Override
	public Set<V> union(K key) {
		return ops.union(getKey(), key);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundSetOperations#union(java.util.Collection)
	 */
	@Override
	public Set<V> union(Collection<K> keys) {
		return ops.union(getKey(), keys);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundSetOperations#unionAndStore(java.lang.Object, java.lang.Object)
	 */
	@Override
	public void unionAndStore(K key, K destKey) {
		ops.unionAndStore(getKey(), key, destKey);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundSetOperations#unionAndStore(java.util.Collection, java.lang.Object)
	 */
	@Override
	public void unionAndStore(Collection<K> keys, K destKey) {
		ops.unionAndStore(getKey(), keys, destKey);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundKeyOperations#getType()
	 */
	@Override
	public DataType getType() {
		return DataType.SET;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundSetOperations#scan(org.springframework.data.redis.core.ScanOptions)
	 */
	@Override
	public Cursor<V> scan(ScanOptions options) {
		return ops.scan(getKey(), options);
	}
}
