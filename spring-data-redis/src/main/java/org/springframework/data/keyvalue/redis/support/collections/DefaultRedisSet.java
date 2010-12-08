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
package org.springframework.data.keyvalue.redis.support.collections;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;

import org.springframework.data.keyvalue.redis.core.BoundSetOperations;
import org.springframework.data.keyvalue.redis.core.RedisOperations;

/**
 * Default implementation for {@link RedisSet}.
 * 
 * @author Costin Leau
 */
public class DefaultRedisSet<E> extends AbstractRedisCollection<E> implements RedisSet<E> {

	private final BoundSetOperations<String, E> boundSetOps;

	private class DefaultRedisSetIterator extends RedisIterator<E> {

		public DefaultRedisSetIterator(Iterator<E> delegate) {
			super(delegate);
		}

		@Override
		protected void removeFromRedisStorage(E item) {
			DefaultRedisSet.this.remove(item);
		}
	}

	/**
	 * Constructs a new <code>DefaultRedisSet</code> instance.
	 *
	 * @param key
	 * @param operations
	 */
	public DefaultRedisSet(String key, RedisOperations<String, E> operations) {
		super(key, operations);
		boundSetOps = operations.boundSetOps(key);
	}

	/**
	 * Constructs a new <code>DefaultRedisSet</code> instance.
	 * 
	 * @param boundOps
	 */
	public DefaultRedisSet(BoundSetOperations<String, E> boundOps) {
		super(boundOps.getKey(), boundOps.getOperations());
		this.boundSetOps = boundOps;
	}

	@Override
	public Set<E> diff(Collection<? extends RedisSet<?>> sets) {
		return boundSetOps.diff(CollectionUtils.extractKeys(sets));
	}

	@Override
	public RedisSet<E> diffAndStore(String destKey, Collection<? extends RedisSet<?>> sets) {
		boundSetOps.diffAndStore(destKey, CollectionUtils.extractKeys(sets));
		return new DefaultRedisSet<E>(boundSetOps.getOperations().boundSetOps(destKey));
	}

	@Override
	public Set<E> intersect(Collection<? extends RedisSet<?>> sets) {
		return boundSetOps.intersect(CollectionUtils.extractKeys(sets));
	}

	@Override
	public RedisSet<E> intersectAndStore(String destKey, Collection<? extends RedisSet<?>> sets) {
		boundSetOps.intersectAndStore(destKey, CollectionUtils.extractKeys(sets));
		return new DefaultRedisSet<E>(boundSetOps.getOperations().boundSetOps(destKey));
	}

	@Override
	public Set<E> union(Collection<? extends RedisSet<?>> sets) {
		return boundSetOps.union(CollectionUtils.extractKeys(sets));
	}

	@Override
	public RedisSet<E> unionAndStore(String destKey, Collection<? extends RedisSet<?>> sets) {
		boundSetOps.unionAndStore(destKey, CollectionUtils.extractKeys(sets));
		return new DefaultRedisSet<E>(boundSetOps.getOperations().boundSetOps(destKey));
	}

	@Override
	public boolean add(E e) {
		return boundSetOps.add(e);
	}

	@Override
	public void clear() {
		// intersect the set with a non existing one
		// TODO: find a safer way to clean the set
		String randomKey = UUID.randomUUID().toString();
		boundSetOps.intersectAndStore(getKey(), Collections.singleton(randomKey));
	}

	@Override
	public boolean contains(Object o) {
		return boundSetOps.isMember(o);
	}

	@Override
	public Iterator<E> iterator() {
		return new DefaultRedisSetIterator(boundSetOps.members().iterator());
	}

	@Override
	public boolean remove(Object o) {
		return boundSetOps.remove(o);
	}

	@Override
	public int size() {
		return boundSetOps.size().intValue();
	}
}