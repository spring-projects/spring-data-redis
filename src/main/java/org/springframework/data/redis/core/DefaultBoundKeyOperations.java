/*
 * Copyright 2011-2016 the original author or authors.
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

import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.springframework.util.Assert;

/**
 * Default {@link BoundKeyOperations} implementation. Mainly for internal use within the framework.
 * 
 * @author Costin Leau
 * @author Mark Paluch
 */
public abstract class DefaultBoundKeyOperations<K> implements BoundKeyOperations<K> {

	private K key;
	private final RedisOperations<K, ?> ops;

	/**
	 * Constructs a new {@link DefaultBoundKeyOperations} instance.
	 * 
	 * @param key must not be {@literal null}.
	 * @param operations must not be {@literal null}.
	 */
	public DefaultBoundKeyOperations(K key, RedisOperations<K, ?> operations) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(operations, "RedisOperations must not be null!");

		setKey(key);
		this.ops = operations;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundKeyOperations#getKey()
	 */
	public K getKey() {
		return key;
	}

	/**
	 * Sets the key for the key-bound operations.
	 *
	 * @param key must not be {@literal null}.
	 */
	protected void setKey(K key) {

		Assert.notNull(key, "Key must not be null!");

		this.key = key;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundKeyOperations#expire(long, java.util.concurrent.TimeUnit)
	 */
	public Boolean expire(long timeout, TimeUnit unit) {
		return ops.expire(key, timeout, unit);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundKeyOperations#expireAt(java.util.Date)
	 */
	public Boolean expireAt(Date date) {
		return ops.expireAt(key, date);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundKeyOperations#getExpire()
	 */
	public Long getExpire() {
		return ops.getExpire(key);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundKeyOperations#persist()
	 */
	public Boolean persist() {
		return ops.persist(key);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundKeyOperations#rename(java.lang.Object)
	 */
	public void rename(K newKey) {
		if (ops.hasKey(key)) {
			ops.rename(key, newKey);
		}
		key = newKey;
	}
}
