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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.springframework.data.redis.connection.DataType;
import org.springframework.lang.Nullable;

/**
 * Default implementation for {@link HashOperations}.
 *
 * @author Costin Leau
 * @author Christoph Strobl
 * @author Ninad Divadkar
 */
class DefaultBoundHashOperations<H, HK, HV> extends DefaultBoundKeyOperations<H>
		implements BoundHashOperations<H, HK, HV> {

	private final HashOperations<H, HK, HV> ops;

	/**
	 * Constructs a new <code>DefaultBoundHashOperations</code> instance.
	 *
	 * @param key
	 * @param operations
	 */
	DefaultBoundHashOperations(H key, RedisOperations<H, ?> operations) {
		super(key, operations);
		this.ops = operations.opsForHash();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundHashOperations#delete(java.lang.Object[])
	 */
	@Override
	public Long delete(Object... keys) {
		return ops.delete(getKey(), keys);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundHashOperations#get(java.lang.Object)
	 */
	@Override
	public HV get(Object key) {
		return ops.get(getKey(), key);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundHashOperations#multiGet(java.util.Collection)
	 */
	@Override
	public List<HV> multiGet(Collection<HK> hashKeys) {
		return ops.multiGet(getKey(), hashKeys);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundHashOperations#getOperations()
	 */
	@Override
	public RedisOperations<H, ?> getOperations() {
		return ops.getOperations();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundHashOperations#hasKey(java.lang.Object)
	 */
	@Override
	public Boolean hasKey(Object key) {
		return ops.hasKey(getKey(), key);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundHashOperations#increment(java.lang.Object, long)
	 */
	@Override
	public Long increment(HK key, long delta) {
		return ops.increment(getKey(), key, delta);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundHashOperations#increment(java.lang.Object, double)
	 */
	@Override
	public Double increment(HK key, double delta) {
		return ops.increment(getKey(), key, delta);
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundHashOperations#randomField()
	 */
	@Nullable
	@Override
	public HK randomField() {
		return ops.randomField(getKey());
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundHashOperations#randomValue()
	 */
	@Nullable
	@Override
	public Entry<HK, HV> randomValue() {
		return ops.randomValue(getKey());
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundHashOperations#randomFields(long)
	 */
	@Nullable
	@Override
	public List<HK> randomFields(long count) {
		return ops.randomFields(getKey(), count);
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundHashOperations#randomValues(long)
	 */
	@Nullable
	@Override
	public Map<HK, HV> randomValues(long count) {
		return ops.randomValues(getKey(), count);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundHashOperations#keys()
	 */
	@Override
	public Set<HK> keys() {
		return ops.keys(getKey());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundHashOperations#lengthOfValue(java.lang.Object, java.lang.Object)
	 */
	@Nullable
	@Override
	public Long lengthOfValue(HK hashKey) {
		return ops.lengthOfValue(getKey(), hashKey);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundHashOperations#size()
	 */
	@Override
	public Long size() {
		return ops.size(getKey());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundHashOperations#putAll(java.util.Map)
	 */
	@Override
	public void putAll(Map<? extends HK, ? extends HV> m) {
		ops.putAll(getKey(), m);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundHashOperations#put(java.lang.Object, java.lang.Object)
	 */
	@Override
	public void put(HK key, HV value) {
		ops.put(getKey(), key, value);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundHashOperations#putIfAbsent(java.lang.Object, java.lang.Object)
	 */
	@Override
	public Boolean putIfAbsent(HK key, HV value) {
		return ops.putIfAbsent(getKey(), key, value);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundHashOperations#values()
	 */
	@Override
	public List<HV> values() {
		return ops.values(getKey());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundHashOperations#entries()
	 */
	@Override
	public Map<HK, HV> entries() {
		return ops.entries(getKey());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundKeyOperations#getType()
	 */
	@Override
	public DataType getType() {
		return DataType.HASH;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundHashOperations#scan(org.springframework.data.redis.core.ScanOptions)
	 */
	@Override
	public Cursor<Entry<HK, HV>> scan(ScanOptions options) {
		return ops.scan(getKey(), options);
	}
}
