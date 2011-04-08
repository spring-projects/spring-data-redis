/*
 * Copyright 2010-2011 the original author or authors.
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
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

import org.springframework.data.keyvalue.redis.connection.DataType;
import org.springframework.data.keyvalue.redis.core.BoundZSetOperations;
import org.springframework.data.keyvalue.redis.core.RedisOperations;

/**
 * Default implementation for {@link RedisZSet}.
 * 
 * @author Costin Leau
 */
public class DefaultRedisZSet<E> extends AbstractRedisCollection<E> implements RedisZSet<E> {

	private final BoundZSetOperations<String, E> boundZSetOps;
	private double defaultScore = 1;

	private class DefaultRedisSortedSetIterator extends RedisIterator<E> {

		public DefaultRedisSortedSetIterator(Iterator<E> delegate) {
			super(delegate);
		}

		@Override
		protected void removeFromRedisStorage(E item) {
			DefaultRedisZSet.this.remove(item);
		}
	}

	/**
	 * Constructs a new <code>DefaultRedisZSet</code> instance with a default score of '1'.
	 *
	 * @param key
	 * @param operations
	 */
	public DefaultRedisZSet(String key, RedisOperations<String, E> operations) {
		this(key, operations, 1);
	}

	/**
	 * Constructs a new <code>DefaultRedisSortedSet</code> instance.
	 *
	 * @param key
	 * @param operations
	 * @param defaultScore
	 */
	public DefaultRedisZSet(String key, RedisOperations<String, E> operations, double defaultScore) {
		super(key, operations);
		boundZSetOps = operations.boundZSetOps(key);
		this.defaultScore = defaultScore;
	}


	/**
	 * Constructs a new <code>DefaultRedisZSet</code> instance with a default score of '1'.
	 *
	 * @param boundOps
	 */
	public DefaultRedisZSet(BoundZSetOperations<String, E> boundOps) {
		this(boundOps, 1);
	}

	/**
	 * Constructs a new <code>DefaultRedisZSet</code> instance.
	 *
	 * @param boundOps
	 * @param defaultScore
	 */
	public DefaultRedisZSet(BoundZSetOperations<String, E> boundOps, double defaultScore) {
		super(boundOps.getKey(), boundOps.getOperations());
		this.boundZSetOps = boundOps;
		this.defaultScore = defaultScore;
	}

	public RedisZSet<E> intersectAndStore(RedisZSet<?> set, String destKey) {
		boundZSetOps.intersectAndStore(set.getKey(), destKey);
		return new DefaultRedisZSet<E>(boundZSetOps.getOperations().boundZSetOps(destKey), getDefaultScore());
	}

	public RedisZSet<E> intersectAndStore(Collection<? extends RedisZSet<?>> sets, String destKey) {
		boundZSetOps.intersectAndStore(CollectionUtils.extractKeys(sets), destKey);
		return new DefaultRedisZSet<E>(boundZSetOps.getOperations().boundZSetOps(destKey), getDefaultScore());
	}

	public Set<E> range(long start, long end) {
		return boundZSetOps.range(start, end);
	}

	public Set<E> reverseRange(long start, long end) {
		return boundZSetOps.reverseRange(start, end);
	}

	public Set<E> rangeByScore(double min, double max) {
		return boundZSetOps.rangeByScore(min, max);
	}

	public RedisZSet<E> remove(long start, long end) {
		boundZSetOps.removeRange(start, end);
		return this;
	}

	public RedisZSet<E> removeByScore(double min, double max) {
		boundZSetOps.removeRangeByScore(min, max);
		return this;
	}

	public RedisZSet<E> unionAndStore(RedisZSet<?> set, String destKey) {
		boundZSetOps.unionAndStore(set.getKey(), destKey);
		return new DefaultRedisZSet<E>(boundZSetOps.getOperations().boundZSetOps(destKey), getDefaultScore());
	}

	public RedisZSet<E> unionAndStore(Collection<? extends RedisZSet<?>> sets, String destKey) {
		boundZSetOps.unionAndStore(CollectionUtils.extractKeys(sets), destKey);
		return new DefaultRedisZSet<E>(boundZSetOps.getOperations().boundZSetOps(destKey), getDefaultScore());
	}

	@Override
	public boolean add(E e) {
		return add(e, getDefaultScore());
	}

	public boolean add(E e, double score) {
		return boundZSetOps.add(e, score);
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
		return boundZSetOps.size().intValue();
	}

	public Double getDefaultScore() {
		return defaultScore;
	}

	public E first() {
		Iterator<E> iterator = boundZSetOps.range(0, 0).iterator();
		if (iterator.hasNext())
			return iterator.next();
		throw new NoSuchElementException();
	}

	public E last() {
		Iterator<E> iterator = boundZSetOps.reverseRange(0, 0).iterator();
		if (iterator.hasNext())
			return iterator.next();
		throw new NoSuchElementException();
	}

	public Long rank(Object o) {
		return boundZSetOps.rank(o);
	}

	public Long reverseRank(Object o) {
		return boundZSetOps.reverseRank(o);
	}

	public Double score(Object o) {
		return boundZSetOps.score(o);
	}

	public DataType getType() {
		return DataType.ZSET;
	}
}