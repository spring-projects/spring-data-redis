/*
 * Copyright 2011-2018 the original author or authors.
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
package org.springframework.data.redis.support.collections;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;

import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.core.BoundSetOperations;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.ScanOptions;

/**
 * Default implementation for {@link RedisSet}. Note that the collection support works only with normal,
 * non-pipeline/multi-exec connections as it requires a reply to be sent right away.
 *
 * @author Costin Leau
 * @author Christoph Strobl
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
	 * Constructs a new {@link DefaultRedisSet} instance.
	 *
	 * @param key Redis key of this set.
	 * @param operations {@link RedisOperations} for the value type of this set.
	 */
	public DefaultRedisSet(String key, RedisOperations<String, E> operations) {

		super(key, operations);
		boundSetOps = operations.boundSetOps(key);
	}

	/**
	 * Constructs a new {@link DefaultRedisSet} instance.
	 *
	 * @param boundOps {@link BoundSetOperations} for the value type of this set.
	 */
	public DefaultRedisSet(BoundSetOperations<String, E> boundOps) {

		super(boundOps.getKey(), boundOps.getOperations());
		this.boundSetOps = boundOps;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.support.collections.RedisSet#diff(org.springframework.data.redis.support.collections.RedisSet)
	 */
	@Override
	public Set<E> diff(RedisSet<?> set) {
		return boundSetOps.diff(set.getKey());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.support.collections.RedisSet#diff(java.util.Collection)
	 */
	@Override
	public Set<E> diff(Collection<? extends RedisSet<?>> sets) {
		return boundSetOps.diff(CollectionUtils.extractKeys(sets));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.support.collections.RedisSet#diffAndStore(org.springframework.data.redis.support.collections.RedisSet, java.lang.String)
	 */
	@Override
	public RedisSet<E> diffAndStore(RedisSet<?> set, String destKey) {
		boundSetOps.diffAndStore(set.getKey(), destKey);
		return new DefaultRedisSet<>(boundSetOps.getOperations().boundSetOps(destKey));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.support.collections.RedisSet#diffAndStore(java.util.Collection, java.lang.String)
	 */
	@Override
	public RedisSet<E> diffAndStore(Collection<? extends RedisSet<?>> sets, String destKey) {
		boundSetOps.diffAndStore(CollectionUtils.extractKeys(sets), destKey);
		return new DefaultRedisSet<>(boundSetOps.getOperations().boundSetOps(destKey));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.support.collections.RedisSet#intersect(org.springframework.data.redis.support.collections.RedisSet)
	 */
	@Override
	public Set<E> intersect(RedisSet<?> set) {
		return boundSetOps.intersect(set.getKey());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.support.collections.RedisSet#intersect(java.util.Collection)
	 */
	@Override
	public Set<E> intersect(Collection<? extends RedisSet<?>> sets) {
		return boundSetOps.intersect(CollectionUtils.extractKeys(sets));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.support.collections.RedisSet#intersectAndStore(org.springframework.data.redis.support.collections.RedisSet, java.lang.String)
	 */
	@Override
	public RedisSet<E> intersectAndStore(RedisSet<?> set, String destKey) {
		boundSetOps.intersectAndStore(set.getKey(), destKey);
		return new DefaultRedisSet<>(boundSetOps.getOperations().boundSetOps(destKey));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.support.collections.RedisSet#intersectAndStore(java.util.Collection, java.lang.String)
	 */
	@Override
	public RedisSet<E> intersectAndStore(Collection<? extends RedisSet<?>> sets, String destKey) {
		boundSetOps.intersectAndStore(CollectionUtils.extractKeys(sets), destKey);
		return new DefaultRedisSet<>(boundSetOps.getOperations().boundSetOps(destKey));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.support.collections.RedisSet#union(org.springframework.data.redis.support.collections.RedisSet)
	 */
	@Override
	public Set<E> union(RedisSet<?> set) {
		return boundSetOps.union(set.getKey());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.support.collections.RedisSet#union(java.util.Collection)
	 */
	@Override
	public Set<E> union(Collection<? extends RedisSet<?>> sets) {
		return boundSetOps.union(CollectionUtils.extractKeys(sets));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.support.collections.RedisSet#unionAndStore(org.springframework.data.redis.support.collections.RedisSet, java.lang.String)
	 */
	@Override
	public RedisSet<E> unionAndStore(RedisSet<?> set, String destKey) {
		boundSetOps.unionAndStore(set.getKey(), destKey);
		return new DefaultRedisSet<>(boundSetOps.getOperations().boundSetOps(destKey));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.support.collections.RedisSet#unionAndStore(java.util.Collection, java.lang.String)
	 */
	@Override
	public RedisSet<E> unionAndStore(Collection<? extends RedisSet<?>> sets, String destKey) {
		boundSetOps.unionAndStore(CollectionUtils.extractKeys(sets), destKey);
		return new DefaultRedisSet<>(boundSetOps.getOperations().boundSetOps(destKey));
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.AbstractCollection#add(java.lang.Object)
	 */
	@Override
	@SuppressWarnings("unchecked")
	public boolean add(E e) {
		Long result = boundSetOps.add(e);
		checkResult(result);
		return result == 1;
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.AbstractCollection#clear()
	 */
	@Override
	public void clear() {
		// intersect the set with a non existing one
		// TODO: find a safer way to clean the set
		String randomKey = UUID.randomUUID().toString();
		boundSetOps.intersectAndStore(Collections.singleton(randomKey), getKey());
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.AbstractCollection#contains(java.lang.Object)
	 */
	@Override
	public boolean contains(Object o) {
		Boolean result = boundSetOps.isMember(o);
		checkResult(result);
		return result;
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.AbstractCollection#iterator()
	 */
	@Override
	public Iterator<E> iterator() {
		Set<E> members = boundSetOps.members();
		checkResult(members);
		return new DefaultRedisSetIterator(members.iterator());
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.AbstractCollection#remove(java.lang.Object)
	 */
	@Override
	public boolean remove(Object o) {
		Long result = boundSetOps.remove(o);
		checkResult(result);
		return result == 1;
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.AbstractCollection#size()
	 */
	@Override
	public int size() {
		Long result = boundSetOps.size();
		checkResult(result);
		return result.intValue();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundKeyOperations#getType()
	 */
	@Override
	public DataType getType() {
		return DataType.SET;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.support.collections.RedisSet#scan()
	 */
	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.support.collections.RedisSet#scan()
	 */
	@Override
	public Cursor<E> scan() {
		return scan(ScanOptions.NONE);
	}

	/**
	 * @since 1.4
	 * @param options
	 * @return
	 */
	public Cursor<E> scan(ScanOptions options) {
		return boundSetOps.scan(options);
	}
}
