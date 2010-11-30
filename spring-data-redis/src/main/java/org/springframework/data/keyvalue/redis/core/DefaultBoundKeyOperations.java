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

import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.springframework.data.keyvalue.redis.connection.DataType;


/**
 * Default implementation for {@link BoundKeyOperations}.
 * 
 * @author Costin Leau
 */
class DefaultBoundKeyOperations<K> extends DefaultKeyBound<K> implements BoundKeyOperations<K> {

	private final KeyOperations<K> keyOps;

	/**
	 * Constructs a new <code>DefaultBoundKeyOperations</code> instance.
	 *
	 * @param key
	 */
	public DefaultBoundKeyOperations(K key, KeyOperations<K> keyOps) {
		super(key);
		this.keyOps = keyOps;
	}

	@Override
	public void delete() {
		keyOps.delete(getKey());
	}

	@Override
	public Boolean exists() {
		return keyOps.exists(getKey());
	}

	@Override
	public Boolean expire(long timeout, TimeUnit unit) {
		return keyOps.expire(getKey(), timeout, unit);
	}

	@Override
	public Boolean expireAt(Date date) {
		return keyOps.expireAt(getKey(), date);
	}

	@Override
	public long getExpire() {
		return keyOps.getExpire(getKey());
	}

	@Override
	public void persist() {
		keyOps.persist(getKey());
	}

	@Override
	public void rename(K newKey) {
		keyOps.rename(getKey(), newKey);
		setKey(newKey);
	}

	@Override
	public Boolean renameIfAbsent(K newKey) {
		if (keyOps.renameIfAbsent(getKey(), newKey)) {
			setKey(newKey);
			return Boolean.TRUE;
		}
		return Boolean.FALSE;
	}

	@Override
	public DataType type() {
		return keyOps.type(getKey());
	}
}