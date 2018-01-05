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

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.springframework.data.redis.connection.DataType;

/**
 * Default implementation for {@link BoundListOperations}.
 *
 * @author Costin Leau
 */
class DefaultBoundListOperations<K, V> extends DefaultBoundKeyOperations<K> implements BoundListOperations<K, V> {

	private final ListOperations<K, V> ops;

	/**
	 * Constructs a new <code>DefaultBoundListOperations</code> instance.
	 *
	 * @param key
	 * @param operations
	 */
	DefaultBoundListOperations(K key, RedisOperations<K, V> operations) {

		super(key, operations);
		this.ops = operations.opsForList();
	}

	@Override
	public RedisOperations<K, V> getOperations() {
		return ops.getOperations();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundListOperations#index(long)
	 */
	@Override
	public V index(long index) {
		return ops.index(getKey(), index);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundListOperations#leftPop()
	 */
	@Override
	public V leftPop() {
		return ops.leftPop(getKey());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundListOperations#leftPop(long, java.util.concurrent.TimeUnit)
	 */
	@Override
	public V leftPop(long timeout, TimeUnit unit) {
		return ops.leftPop(getKey(), timeout, unit);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundListOperations#leftPush(java.lang.Object)
	 */
	@Override
	public Long leftPush(V value) {
		return ops.leftPush(getKey(), value);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundListOperations#leftPushAll(java.lang.Object[])
	 */
	@Override
	public Long leftPushAll(V... values) {
		return ops.leftPushAll(getKey(), values);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundListOperations#leftPushIfPresent(java.lang.Object)
	 */
	@Override
	public Long leftPushIfPresent(V value) {
		return ops.leftPushIfPresent(getKey(), value);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundListOperations#leftPush(java.lang.Object, java.lang.Object)
	 */
	@Override
	public Long leftPush(V pivot, V value) {
		return ops.leftPush(getKey(), pivot, value);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundListOperations#size()
	 */
	@Override
	public Long size() {
		return ops.size(getKey());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundListOperations#range(long, long)
	 */
	@Override
	public List<V> range(long start, long end) {
		return ops.range(getKey(), start, end);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundListOperations#remove(long, java.lang.Object)
	 */
	@Override
	public Long remove(long i, Object value) {
		return ops.remove(getKey(), i, value);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundListOperations#rightPop()
	 */
	@Override
	public V rightPop() {
		return ops.rightPop(getKey());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundListOperations#rightPop(long, java.util.concurrent.TimeUnit)
	 */
	@Override
	public V rightPop(long timeout, TimeUnit unit) {
		return ops.rightPop(getKey(), timeout, unit);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundListOperations#rightPushIfPresent(java.lang.Object)
	 */
	@Override
	public Long rightPushIfPresent(V value) {
		return ops.rightPushIfPresent(getKey(), value);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundListOperations#rightPush(java.lang.Object)
	 */
	@Override
	public Long rightPush(V value) {
		return ops.rightPush(getKey(), value);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundListOperations#rightPushAll(java.lang.Object[])
	 */
	@Override
	public Long rightPushAll(V... values) {
		return ops.rightPushAll(getKey(), values);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundListOperations#rightPush(java.lang.Object, java.lang.Object)
	 */
	@Override
	public Long rightPush(V pivot, V value) {
		return ops.rightPush(getKey(), pivot, value);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundListOperations#trim(long, long)
	 */
	@Override
	public void trim(long start, long end) {
		ops.trim(getKey(), start, end);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundListOperations#set(long, java.lang.Object)
	 */
	@Override
	public void set(long index, V value) {
		ops.set(getKey(), index, value);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundKeyOperations#getType()
	 */
	@Override
	public DataType getType() {
		return DataType.LIST;
	}
}
