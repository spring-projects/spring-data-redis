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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.springframework.data.redis.connection.DataType;

/**
 * Default implementation for {@link HashOperations}.
 *
 * @author Costin Leau
 * @author Christoph Strobl
 * @author Ninad Divadkar
 * @author Mark Paluch
 */
public class DefaultBoundHashOperations<H, HK, HV> extends DefaultBoundKeyOperations<H>
		implements BoundHashOperations<H, HK, HV> {

	private final HashOperations<H, HK, HV> ops;

	/**
	 * Constructs a new {@link DefaultBoundHashOperations} instance.
	 *
	 * @param key must not be {@literal null}.
	 * @param operations must not be {@literal null}.
	 */
	public DefaultBoundHashOperations(H key, RedisOperations<H, ?> operations) {

		super(key, operations);
		this.ops = operations.opsForHash();
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundHashOperations#delete(java.lang.Object[])
	 */
	public Long delete(Object... keys) {
		return ops.delete(getKey(), keys);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundHashOperations#get(java.lang.Object)
	 */
	public HV get(Object key) {
		return ops.get(getKey(), key);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundHashOperations#multiGet(java.util.Collection)
	 */
	public List<HV> multiGet(Collection<HK> hashKeys) {
		return ops.multiGet(getKey(), hashKeys);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundHashOperations#getOperations()
	 */
	public RedisOperations<H, ?> getOperations() {
		return ops.getOperations();
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundHashOperations#hasKey(java.lang.Object)
	 */
	public Boolean hasKey(Object key) {
		return ops.hasKey(getKey(), key);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundHashOperations#increment(java.lang.Object, long)
	 */
	public Long increment(HK key, long delta) {
		return ops.increment(getKey(), key, delta);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundHashOperations#increment(java.lang.Object, double)
	 */
	public Double increment(HK key, double delta) {
		return ops.increment(getKey(), key, delta);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundHashOperations#keys()
	 */
	public Set<HK> keys() {
		return ops.keys(getKey());
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundHashOperations#size()
	 */
	public Long size() {
		return ops.size(getKey());
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundHashOperations#putAll(java.util.Map)
	 */
	public void putAll(Map<? extends HK, ? extends HV> m) {
		ops.putAll(getKey(), m);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundHashOperations#put(java.lang.Object, java.lang.Object)
	 */
	public void put(HK key, HV value) {
		ops.put(getKey(), key, value);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundHashOperations#putIfAbsent(java.lang.Object, java.lang.Object)
	 */
	public Boolean putIfAbsent(HK key, HV value) {
		return ops.putIfAbsent(getKey(), key, value);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundHashOperations#values()
	 */
	public List<HV> values() {
		return ops.values(getKey());
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundHashOperations#entries()
	 */
	public Map<HK, HV> entries() {
		return ops.entries(getKey());
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundKeyOperations#getType()
	 */
	public DataType getType() {
		return DataType.HASH;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundHashOperations#scan(org.springframework.data.redis.core.ScanOptions)
	 */
	@Override
	public Cursor<Entry<HK, HV>> scan(ScanOptions options) {
		return ops.scan(getKey(), options);
	}
}
