/*
 * Copyright 2011-2017 the original author or authors.
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

import java.util.concurrent.TimeUnit;

import org.springframework.data.redis.connection.DataType;

/**
 * @author Costin Leau
 */
class DefaultBoundValueOperations<K, V> extends DefaultBoundKeyOperations<K> implements BoundValueOperations<K, V> {

	private final ValueOperations<K, V> ops;

	/**
	 * Constructs a new <code>DefaultBoundValueOperations</code> instance.
	 *
	 * @param key
	 * @param operations
	 */
	DefaultBoundValueOperations(K key, RedisOperations<K, V> operations) {

		super(key, operations);
		this.ops = operations.opsForValue();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundValueOperations#get()
	 */
	@Override
	public V get() {
		return ops.get(getKey());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundValueOperations#getAndSet(java.lang.Object)
	 */
	@Override
	public V getAndSet(V value) {
		return ops.getAndSet(getKey(), value);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundValueOperations#increment(long)
	 */
	@Override
	public Long increment(long delta) {
		return ops.increment(getKey(), delta);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundValueOperations#increment(double)
	 */
	@Override
	public Double increment(double delta) {
		return ops.increment(getKey(), delta);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundValueOperations#append(java.lang.String)
	 */
	@Override
	public Integer append(String value) {
		return ops.append(getKey(), value);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundValueOperations#get(long, long)
	 */
	@Override
	public String get(long start, long end) {
		return ops.get(getKey(), start, end);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundValueOperations#set(java.lang.Object, long, java.util.concurrent.TimeUnit)
	 */
	@Override
	public void set(V value, long timeout, TimeUnit unit) {
		ops.set(getKey(), value, timeout, unit);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundValueOperations#set(java.lang.Object)
	 */
	@Override
	public void set(V value) {
		ops.set(getKey(), value);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundValueOperations#setIfAbsent(java.lang.Object)
	 */
	@Override
	public Boolean setIfAbsent(V value) {
		return ops.setIfAbsent(getKey(), value);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundValueOperations#set(java.lang.Object, long)
	 */
	@Override
	public void set(V value, long offset) {
		ops.set(getKey(), value, offset);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundValueOperations#size()
	 */
	@Override
	public Long size() {
		return ops.size(getKey());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundValueOperations#getOperations()
	 */
	@Override
	public RedisOperations<K, V> getOperations() {
		return ops.getOperations();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundKeyOperations#getType()
	 */
	@Override
	public DataType getType() {
		return DataType.STRING;
	}
}
