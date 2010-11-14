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
package org.springframework.datastore.redis.util;

import java.util.Iterator;
import java.util.Set;

import org.springframework.datastore.redis.core.BoundSetOperations;
import org.springframework.datastore.redis.core.RedisOperations;

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
		boundSetOps = operations.forSet(key);
	}

	public DefaultRedisSet(BoundSetOperations<String, E> boundOps) {
		super(boundOps.getKey(), boundOps.getOperations());
		this.boundSetOps = boundOps;
	}

	@Override
	public Set<E> diff(RedisSet<? extends E>... sets) {
		return boundSetOps.diff(extractKeys(sets));
	}

	@Override
	public RedisSet<E> diffAndStore(String destKey, RedisSet<? extends E>... sets) {
		boundSetOps.diffAndStore(destKey, extractKeys(sets));
		return new DefaultRedisSet(boundSetOps);
	}

	@Override
	public Set<E> intersect(RedisSet<? extends E>... sets) {
		return boundSetOps.intersect(extractKeys(sets));
	}

	@Override
	public RedisSet<E> intersectAndStore(String destKey, RedisSet<? extends E>... sets) {
		boundSetOps.intersectAndStore(destKey, extractKeys(sets));
		return new DefaultRedisSet(boundSetOps);
	}

	@Override
	public Set<E> union(RedisSet<? extends E>... sets) {
		return boundSetOps.union(extractKeys(sets));
	}

	@Override
	public RedisSet<E> unionAndStore(String destKey, RedisSet<? extends E>... sets) {
		boundSetOps.unionAndStore(destKey, extractKeys(sets));
		return new DefaultRedisSet(boundSetOps);
	}

	@Override
	public boolean add(E e) {
		return boundSetOps.add(e);
	}

	@Override
	public void clear() {
		// intersect the set with a non existing one
		// TODO: find a safer way to clean the set
		boundSetOps.intersectAndStore(key, "NON-EXISTING");
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
		return boundSetOps.size();
	}

	private String[] extractKeys(RedisSet<?>... sets) {
		String[] keys = new String[sets.length + 1];
		for (int i = 0; i < keys.length; i++) {
			keys[i] = sets[i].getKey();
		}

		return keys;
	}
}