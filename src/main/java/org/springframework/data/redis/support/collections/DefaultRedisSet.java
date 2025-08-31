/*
 * Copyright 2011-2025 the original author or authors.
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
package org.springframework.data.redis.support.collections;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

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
 * @author Mark Paluch
 * @author Mingi Lee
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

	@Override
	public Set<E> diff(RedisSet<?> set) {
		return boundSetOps.difference(set.getKey());
	}

	@Override
	public Set<E> diff(Collection<? extends RedisSet<?>> sets) {
		return boundSetOps.difference(CollectionUtils.extractKeys(sets));
	}

	@Override
	public RedisSet<E> diffAndStore(RedisSet<?> set, String destKey) {
		boundSetOps.differenceAndStore(set.getKey(), destKey);
		return new DefaultRedisSet<>(boundSetOps.getOperations().boundSetOps(destKey));
	}

	@Override
	public RedisSet<E> diffAndStore(Collection<? extends RedisSet<?>> sets, String destKey) {
		boundSetOps.differenceAndStore(CollectionUtils.extractKeys(sets), destKey);
		return new DefaultRedisSet<>(boundSetOps.getOperations().boundSetOps(destKey));
	}

	@Override
	public Set<E> intersect(RedisSet<?> set) {
		return boundSetOps.intersect(set.getKey());
	}

	@Override
	public Set<E> intersect(Collection<? extends RedisSet<?>> sets) {
		return boundSetOps.intersect(CollectionUtils.extractKeys(sets));
	}

	@Override
	public RedisSet<E> intersectAndStore(RedisSet<?> set, String destKey) {
		boundSetOps.intersectAndStore(set.getKey(), destKey);
		return new DefaultRedisSet<>(boundSetOps.getOperations().boundSetOps(destKey));
	}

	@Override
	public RedisSet<E> intersectAndStore(Collection<? extends RedisSet<?>> sets, String destKey) {
		boundSetOps.intersectAndStore(CollectionUtils.extractKeys(sets), destKey);
		return new DefaultRedisSet<>(boundSetOps.getOperations().boundSetOps(destKey));
	}

	@Override
	public Long intersectSize(RedisSet<?> set) {
		return boundSetOps.intersectSize(set.getKey());
	}

	@Override
	public Long intersectSize(Collection<? extends RedisSet<?>> sets) {
		return boundSetOps.intersectSize(CollectionUtils.extractKeys(sets));
	}

	@Override
	public Set<E> union(RedisSet<?> set) {
		return boundSetOps.union(set.getKey());
	}

	@Override
	public Set<E> union(Collection<? extends RedisSet<?>> sets) {
		return boundSetOps.union(CollectionUtils.extractKeys(sets));
	}

	@Override
	public RedisSet<E> unionAndStore(RedisSet<?> set, String destKey) {
		boundSetOps.unionAndStore(set.getKey(), destKey);
		return new DefaultRedisSet<>(boundSetOps.getOperations().boundSetOps(destKey));
	}

	@Override
	public RedisSet<E> unionAndStore(Collection<? extends RedisSet<?>> sets, String destKey) {
		boundSetOps.unionAndStore(CollectionUtils.extractKeys(sets), destKey);
		return new DefaultRedisSet<>(boundSetOps.getOperations().boundSetOps(destKey));
	}

	@Override
	public E randomValue() {
		return boundSetOps.randomMember();
	}

	@Override
	@SuppressWarnings("unchecked")
	public boolean add(E e) {
		Long result = boundSetOps.add(e);
		checkResult(result);
		return result == 1;
	}

	@Override
	public void clear() {
		boundSetOps.getOperations().delete(getKey());
	}

	@Override
	public boolean contains(Object o) {
		Boolean result = boundSetOps.isMember(o);
		checkResult(result);
		return result;
	}

	@Override
	public boolean containsAll(Collection<?> c) {

		if (c.isEmpty()) {
			return true;
		}

		Map<Object, Boolean> member = boundSetOps.isMember(c.toArray());
		checkResult(member);

		return member.values().stream().reduce(true, (left, right) -> left && right);
	}

	@Override
	public Iterator<E> iterator() {
		Set<E> members = boundSetOps.members();
		checkResult(members);
		return new DefaultRedisSetIterator(members.iterator());
	}

	@Override
	public boolean remove(Object o) {
		Long result = boundSetOps.remove(o);
		checkResult(result);
		return result == 1;
	}

	@Override
	public int size() {
		Long result = boundSetOps.size();
		checkResult(result);
		return result.intValue();
	}

	@Override
	public DataType getType() {
		return DataType.SET;
	}

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
