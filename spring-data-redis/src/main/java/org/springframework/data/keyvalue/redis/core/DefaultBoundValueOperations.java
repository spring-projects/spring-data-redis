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

import java.util.concurrent.TimeUnit;

/**
 * @author Costin Leau
 */
class DefaultBoundValueOperations<K, V> extends DefaultKeyBound<K> implements BoundValueOperations<K, V> {

	private final ValueOperations<K, V> ops;

	/**
	 * Constructs a new <code>DefaultBoundValueOperations</code> instance.
	 *
	 * @param key
	 * @param template
	 */
	public DefaultBoundValueOperations(K key, RedisTemplate<K, V> template) {
		super(key);
		this.ops = template.valueOps();
	}

	@Override
	public V get() {
		return ops.get(getKey());
	}

	@Override
	public V getAndSet(V value) {
		return ops.getAndSet(getKey(), value);
	}

	@Override
	public V increment(int delta) {
		return ops.increment(getKey(), delta);
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
}