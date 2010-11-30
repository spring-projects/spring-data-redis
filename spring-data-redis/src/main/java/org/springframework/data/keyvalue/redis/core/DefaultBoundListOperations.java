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

import java.util.List;


/**
 * Default implementation for {@link BoundListOperations}.
 * 
 * @author Costin Leau
 */
class DefaultBoundListOperations<K, V> extends DefaultKeyBound<K> implements BoundListOperations<K, V> {

	private final ListOperations<K, V> ops;

	/**
	 * Constructs a new <code>DefaultBoundListOperations</code> instance.
	 *
	 * @param key
	 * @param template
	 */
	public DefaultBoundListOperations(K key, RedisTemplate<K, V> template) {
		super(key);
		this.ops = template.listOps();
	}


	@Override
	public RedisOperations<K, V> getOperations() {
		return ops.getOperations();
	}

	@Override
	public V index(int index) {
		return ops.index(getKey(), index);
	}

	@Override
	public V leftPop() {
		return ops.leftPop(getKey());
	}

	@Override
	public Integer leftPush(V value) {
		return ops.leftPush(getKey(), value);
	}

	@Override
	public Integer length() {
		return ops.length(getKey());
	}

	@Override
	public List<V> range(int start, int end) {
		return ops.range(getKey(), start, end);
	}

	@Override
	public Integer remove(int i, Object value) {
		return ops.remove(getKey(), i, value);
	}

	@Override
	public V rightPop() {
		return ops.rightPop(getKey());
	}

	@Override
	public Integer rightPush(V value) {
		return ops.rightPush(getKey(), value);
	}

	@Override
	public void trim(int start, int end) {
		ops.trim(getKey(), start, end);
	}

	@Override
	public void set(int index, V value) {
		ops.set(getKey(), index, value);
	}
}