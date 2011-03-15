/*
 * Copyright 2010-2011 the original author or authors.
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

import java.util.Date;
import java.util.concurrent.TimeUnit;


/**
 * Default {@link BoundKeyOperations} implementation.
 * Meant for internal usage.
 * 
 * @author Costin Leau
 */
abstract class DefaultBoundKeyOperations<K> implements BoundKeyOperations<K> {

	private K key;
	private final RedisOperations<K, ?> ops;

	public DefaultBoundKeyOperations(K key, RedisOperations<K, ?> operations) {
		setKey(key);
		this.ops = operations;
	}

	@Override
	public K getKey() {
		return key;
	}

	protected void setKey(K key) {
		this.key = key;
	}

	@Override
	public Boolean expire(long timeout, TimeUnit unit) {
		return ops.expire(key, timeout, unit);
	}

	@Override
	public Boolean expireAt(Date date) {
		return ops.expireAt(key, date);
	}

	@Override
	public Long getExpire() {
		return ops.getExpire(key);
	}

	@Override
	public void persist() {
		ops.persist(key);
	}

	@Override
	public void rename(K newKey) {
		ops.rename(key, newKey);
		key = newKey;
	}

	@Override
	public Boolean renameIfAbsent(K newKey) {
		Boolean result = ops.renameIfAbsent(key, newKey);

		if (Boolean.TRUE.equals(result)) {
			key = newKey;
		}
		return result;
	}
}