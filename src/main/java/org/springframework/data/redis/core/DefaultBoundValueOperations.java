/*
 * Copyright 2011-2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.redis.core;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.springframework.data.redis.connection.DataType;
import org.springframework.lang.Nullable;

/**
 * @author Costin Leau
 * @author Jiahe Cai
 * @author Mark Paluch
 */
class DefaultBoundValueOperations<K, V> extends DefaultBoundKeyOperations<K> implements BoundValueOperations<K, V> {

	private final ValueOperations<K, V> ops;

	/**
	 * Constructs a new {@link DefaultBoundValueOperations} instance.
	 *
	 * @param key
	 * @param operations
	 */
	DefaultBoundValueOperations(K key, RedisOperations<K, V> operations) {

		super(key, operations);
		this.ops = operations.opsForValue();
	}

	@Override
	public V get() {
		return ops.get(getKey());
	}

	@Nullable
	@Override
	public V getAndDelete() {
		return ops.getAndDelete(getKey());
	}

	@Nullable
	@Override
	public V getAndExpire(long timeout, TimeUnit unit) {
		return ops.getAndExpire(getKey(), timeout, unit);
	}

	@Nullable
	@Override
	public V getAndExpire(Duration timeout) {
		return ops.getAndExpire(getKey(), timeout);
	}

	@Nullable
	@Override
	public V getAndPersist() {
		return ops.getAndPersist(getKey());
	}

	@Override
	public V getAndSet(V value) {
		return ops.getAndSet(getKey(), value);
	}

	@Override
	public Long increment() {
		return ops.increment(getKey());
	}

	@Override
	public Long increment(long delta) {
		return ops.increment(getKey(), delta);
	}

	@Override
	public Double increment(double delta) {
		return ops.increment(getKey(), delta);
	}

	@Override
	public Long decrement() {
		return ops.decrement(getKey());
	}

	@Override
	public Long decrement(long delta) {
		return ops.decrement(getKey(), delta);
	}

	@Override
	public Integer append(String value) {
		return ops.append(getKey(), value);
	}

	@Override
	public String get(long start, long end) {
		return ops.get(getKey(), start, end);
	}

	@Override
	public void set(V value, long timeout, TimeUnit unit) {
		ops.set(getKey(), value, timeout, unit);
	}

	@Override
	public void set(V value) {
		ops.set(getKey(), value);
	}

	@Override
	public Boolean setIfAbsent(V value) {
		return ops.setIfAbsent(getKey(), value);
	}

	@Override
	public Boolean setIfAbsent(V value, long timeout, TimeUnit unit) {
		return ops.setIfAbsent(getKey(), value, timeout, unit);
	}

	@Nullable
	@Override
	public Boolean setIfPresent(V value) {
		return ops.setIfPresent(getKey(), value);
	}

	@Nullable
	@Override
	public Boolean setIfPresent(V value, long timeout, TimeUnit unit) {
		return ops.setIfPresent(getKey(), value, timeout, unit);
	}

	@Override
	public void set(V value, long offset) {
		ops.set(getKey(), value, offset);
	}

	@Override
	public Long size() {
		return ops.size(getKey());
	}

	@Override
	public RedisOperations<K, V> getOperations() {
		return ops.getOperations();
	}

	@Override
	public DataType getType() {
		return DataType.STRING;
	}
}
