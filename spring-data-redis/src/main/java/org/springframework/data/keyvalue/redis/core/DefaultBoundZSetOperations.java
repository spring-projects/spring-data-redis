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
import java.util.Set;

/**
 * Default implementation for {@link BoundZSetOperations}.
 * 
 * @author Costin Leau
 */
class DefaultBoundZSetOperations<K, V> extends DefaultKeyBound<K> implements BoundZSetOperations<K, V> {

	private final ZSetOperations<K, V> ops;

	/**
	 * Constructs a new <code>DefaultBoundZSetOperations</code> instance.
	 *
	 * @param key
	 * @param template
	 */
	public DefaultBoundZSetOperations(K key, RedisTemplate<K, V> template) {
		super(key);
		this.ops = template.zSetOps();
	}

	@Override
	public Boolean add(V value, double score) {
		return ops.add(getKey(), value, score);
	}

	@Override
	public RedisOperations<K, V> getOperations() {
		return ops.getOperations();
	}

	@Override
	public void intersectAndStore(K destKey, Collection<K> keys) {
		ops.intersectAndStore(getKey(), destKey, keys);
	}

	@Override
	public Set<V> range(long start, long end) {
		return ops.range(getKey(), start, end);
	}

	@Override
	public Set<V> rangeByScore(double min, double max) {
		return ops.rangeByScore(getKey(), min, max);
	}

	@Override
	public Long rank(Object o) {
		return ops.rank(getKey(), o);
	}

	@Override
	public Long reverseRank(Object o) {
		return ops.reverseRank(getKey(), o);
	}

	@Override
	public Double score(Object o) {
		return ops.score(getKey(), o);
	}

	@Override
	public Boolean remove(Object o) {
		return ops.remove(getKey(), o);
	}

	@Override
	public void removeRange(long start, long end) {
		ops.removeRange(getKey(), start, end);
	}

	@Override
	public void removeRangeByScore(double min, double max) {
		ops.removeRangeByScore(getKey(), min, max);
	}

	@Override
	public Set<V> reverseRange(long start, long end) {
		return ops.reverseRange(getKey(), start, end);
	}

	@Override
	public Long size() {
		return ops.size(getKey());
	}

	@Override
	public void unionAndStore(K destKey, Collection<K> keys) {
		ops.unionAndStore(getKey(), destKey, keys);
	}
}