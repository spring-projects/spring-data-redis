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

import java.util.Comparator;
import java.util.Iterator;
import java.util.Set;
import java.util.SortedSet;

import org.springframework.datastore.redis.core.BoundZSetOperations;
import org.springframework.datastore.redis.core.RedisOperations;

/**
 * Default implementation for {@link RedisSortedSet}.
 * 
 * @author Costin Leau
 */
class DefaultRedisSortedSet<E> extends AbstractRedisCollection<E> implements RedisSortedSet<E> {

	private final BoundZSetOperations<String, E> boundZSetOps;
	
	private class DefaultRedisSortedSetIterator extends RedisIterator<E> {

		public DefaultRedisSortedSetIterator(Iterator<E> delegate) {
			super(delegate);
		}

		@Override
		protected void removeFromRedisStorage(E item) {
			DefaultRedisSortedSet.this.remove(item);
		}
	}

	/**
	 * Constructs a new <code>DefaultRedisSortedSet</code> instance.
	 *
	 * @param key
	 * @param operations
	 */
	public DefaultRedisSortedSet(String key, RedisOperations<String, E> operations) {
		super(key, operations);
		boundZSetOps = operations.forZSet(key);
	}


	public DefaultRedisSortedSet(BoundZSetOperations<String, E> boundOps) {
		super(boundOps.getKey(), boundOps.getOperations());
		this.boundZSetOps = boundOps;
	}

	@Override
	public RedisSortedSet<E> intersectAndStore(String destKey, RedisSortedSet<E>... sets) {
		boundZSetOps.intersectAndStore(destKey, extractKeys(sets));
		return new DefaultRedisSortedSet<E>(boundZSetOps.getOperations().forZSet(destKey));
	}

	@Override
	public Set<E> range(int start, int end) {
		return boundZSetOps.range(start, end);
	}

	@Override
	public Set<E> rangeByScore(double min, double max) {
		return boundZSetOps.rangeByScore(min, max);
	}

	@Override
	public RedisSortedSet<E> remove(int start, int end) {
		boundZSetOps.removeRange(start, end);
		return this;
	}

	@Override
	public RedisSortedSet<E> removeByScore(double min, double max) {
		boundZSetOps.removeRangeByScore(min, max);
		return this;
	}

	@Override
	public RedisSortedSet<E> unionAndStore(String destKey, RedisSortedSet<E>... sets) {
		boundZSetOps.unionAndStore(destKey, extractKeys(sets));
		return new DefaultRedisSortedSet<E>(boundZSetOps.getOperations().forZSet(destKey));
	}

	@Override
	public boolean add(E e) {
		return boundZSetOps.add(e, 0);
	}

	@Override
	public void clear() {
		boundZSetOps.removeRange(0, -1);
	}

	@Override
	public boolean contains(Object o) {
		return (boundZSetOps.rank(o) != null);
	}

	@Override
	public Iterator<E> iterator() {
		return new DefaultRedisSortedSetIterator(boundZSetOps.range(0, -1).iterator());
	}

	@Override
	public boolean remove(Object o) {
		return boundZSetOps.remove(o);
	}

	@Override
	public int size() {
		return boundZSetOps.size();
	}

	@Override
	public Comparator<? super E> comparator() {
		return null;
	}

	@Override
	public E first() {
		return boundZSetOps.range(0, 0).iterator().next();
	}

	@Override
	public SortedSet<E> headSet(E toElement) {
		throw new UnsupportedOperationException();
	}

	@Override
	public E last() {
		return boundZSetOps.reverseRange(0, 0).iterator().next();
	}

	@Override
	public SortedSet<E> subSet(E fromElement, E toElement) {
		throw new UnsupportedOperationException();
	}

	@Override
	public SortedSet<E> tailSet(E fromElement) {
		throw new UnsupportedOperationException();
	}

	private String[] extractKeys(RedisSortedSet<E>... sets) {
		String[] keys = new String[sets.length + 1];
		keys[0] = key;
		for (int i = 0; i < keys.length; i++) {
			keys[i + 1] = sets[i].getKey();
		}

		return keys;
	}
}