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
public class DefaultBoundListOperations<K, V> extends DefaultKeyBound<K> implements BoundListOperations<K, V> {

	private final ListOperations<K, V> ops;
	
	public DefaultBoundListOperations(K key, RedisTemplate<K, V> template) {
		super(key);
		this.ops = template.listOps();
	}
	
	@Override
	public V index(int index) {
		throw new UnsupportedOperationException();
	}

	@Override
	public V leftPop() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Integer leftPush(V value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Integer length() {
		throw new UnsupportedOperationException();
	}

	@Override
	public List<V> range(int start, int end) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Integer remove(int i, Object value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public V rightPop() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Integer rightPush(V value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void trim(int start, int end) {
		throw new UnsupportedOperationException();
	}
}