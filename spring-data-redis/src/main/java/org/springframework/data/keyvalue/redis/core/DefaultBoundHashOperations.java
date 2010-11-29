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

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Default implementation for {@link HashOperations}.
 * 
 * @author Costin Leau
 */
class DefaultBoundHashOperations<H, HK, HV> extends DefaultKeyBound<H> implements BoundHashOperations<H, HK, HV> {

	private final HashOperations<H, HK, HV> ops;

	/**
	 * Constructs a new <code>DefaultBoundHashOperations</code> instance.
	 *
	 * @param key
	 * @param template
	 */
	public DefaultBoundHashOperations(H key, RedisTemplate<H, ?> template) {
		super(key);
		this.ops = template.hashOps();
	}

	@Override
	public void delete(Object key) {
		ops.delete(getKey(), key);
	}

	@Override
	public HV get(Object key) {
		return ops.get(getKey(), key);
	}

	@Override
	public Collection<HV> multiGet(Set<HK> hashKeys) {
		return ops.multiGet(getKey(), hashKeys);
	}

	@Override
	public RedisOperations<H, ?> getOperations() {
		return ops.getOperations();
	}

	@Override
	public boolean hasKey(Object key) {
		return ops.hasKey(getKey(), key);
	}

	@Override
	public Integer increment(HK key, int delta) {
		return ops.increment(getKey(), key, delta);
	}

	@Override
	public Set<HK> keys() {
		return ops.keys(getKey());
	}

	@Override
	public Integer length() {
		return ops.length(getKey());
	}

	@Override
	public void multiSet(Map<? extends HK, ? extends HV> m) {
		ops.multiSet(getKey(), m);
	}

	@Override
	public void set(HK key, HV value) {
		ops.set(getKey(), key, value);
	}

	@Override
	public Collection<HV> values() {
		return ops.values(getKey());
	}
}