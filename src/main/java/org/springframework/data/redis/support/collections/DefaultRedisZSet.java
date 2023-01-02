/*
 * Copyright 2011-2023 the original author or authors.
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
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.connection.RedisZSetCommands.Limit;
import org.springframework.data.redis.connection.RedisZSetCommands.Range;
import org.springframework.data.redis.core.BoundZSetOperations;
import org.springframework.data.redis.core.ConvertingCursor;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.ZSetOperations.TypedTuple;

/**
 * Default implementation for {@link RedisZSet}. Note that the collection support works only with normal,
 * non-pipeline/multi-exec connections as it requires a reply to be sent right away.
 *
 * @author Costin Leau
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Andrey Shlykov
 */
public class DefaultRedisZSet<E> extends AbstractRedisCollection<E> implements RedisZSet<E> {

	private final BoundZSetOperations<String, E> boundZSetOps;
	private final double defaultScore;

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
	 * Constructs a new {@link DefaultRedisZSet} instance with a default score of {@literal 1}.
	 *
	 * @param key Redis key of this set.
	 * @param operations {@link RedisOperations} for the value type of this set.
	 */
	public DefaultRedisZSet(String key, RedisOperations<String, E> operations) {
		this(key, operations, 1);
	}

	/**
	 * Constructs a new {@link DefaultRedisZSet} instance.
	 *
	 * @param key Redis key of this set.
	 * @param operations {@link RedisOperations} for the value type of this set.
	 * @param defaultScore
	 */
	public DefaultRedisZSet(String key, RedisOperations<String, E> operations, double defaultScore) {

		super(key, operations);

		boundZSetOps = operations.boundZSetOps(key);
		this.defaultScore = defaultScore;
	}

	/**
	 * Constructs a new {@link DefaultRedisZSet} instance with a default score of '1'.
	 *
	 * @param boundOps {@link BoundZSetOperations} for the value type of this set.
	 */
	public DefaultRedisZSet(BoundZSetOperations<String, E> boundOps) {
		this(boundOps, 1);
	}

	/**
	 * Constructs a new {@link DefaultRedisZSet} instance.
	 *
	 * @param boundOps {@link BoundZSetOperations} for the value type of this set.
	 * @param defaultScore
	 */
	public DefaultRedisZSet(BoundZSetOperations<String, E> boundOps, double defaultScore) {

		super(boundOps.getKey(), boundOps.getOperations());

		this.boundZSetOps = boundOps;
		this.defaultScore = defaultScore;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.support.collections.RedisZSet#difference(org.springframework.data.redis.support.collections.RedisZSet)
	 */
	@Override
	public Set<E> diff(RedisZSet<?> set) {
		return boundZSetOps.difference(set.getKey());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.support.collections.RedisZSet#difference(java.util.Collection)
	 */
	@Override
	public Set<E> diff(Collection<? extends RedisZSet<?>> sets) {
		return boundZSetOps.difference(CollectionUtils.extractKeys(sets));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.support.collections.RedisZSet#differenceWithScores(org.springframework.data.redis.support.collections.RedisZSet)
	 */
	@Override
	public Set<TypedTuple<E>> diffWithScores(RedisZSet<?> set) {
		return boundZSetOps.differenceWithScores(set.getKey());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.support.collections.RedisZSet#differenceWithScores(java.util.Collection)
	 */
	@Override
	public Set<TypedTuple<E>> diffWithScores(Collection<? extends RedisZSet<?>> sets) {
		return boundZSetOps.differenceWithScores(CollectionUtils.extractKeys(sets));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.support.collections.RedisZSet#differenceAndStore(org.springframework.data.redis.support.collections.RedisZSet, java.lang.String)
	 */
	@Override
	public RedisZSet<E> diffAndStore(RedisZSet<?> set, String destKey) {

		boundZSetOps.differenceAndStore(set.getKey(), destKey);
		return new DefaultRedisZSet<>(boundZSetOps.getOperations().boundZSetOps(destKey), getDefaultScore());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.support.collections.RedisZSet#differenceAndStore(java.util.Collection, java.lang.String)
	 */
	@Override
	public RedisZSet<E> diffAndStore(Collection<? extends RedisZSet<?>> sets, String destKey) {

		boundZSetOps.differenceAndStore(CollectionUtils.extractKeys(sets), destKey);
		return new DefaultRedisZSet<>(boundZSetOps.getOperations().boundZSetOps(destKey), getDefaultScore());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.support.collections.RedisZSet#intersect(org.springframework.data.redis.support.collections.RedisZSet)
	 */
	@Override
	public Set<E> intersect(RedisZSet<?> set) {
		return boundZSetOps.intersect(set.getKey());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.support.collections.RedisZSet#intersect(java.util.Collection)
	 */
	@Override
	public Set<E> intersect(Collection<? extends RedisZSet<?>> sets) {
		return boundZSetOps.intersect(CollectionUtils.extractKeys(sets));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.support.collections.RedisZSet#intersectWithScores(org.springframework.data.redis.support.collections.RedisZSet)
	 */
	@Override
	public Set<TypedTuple<E>> intersectWithScores(RedisZSet<?> set) {
		return boundZSetOps.intersectWithScores(set.getKey());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.support.collections.RedisZSet#intersectWithScores(java.util.Collection)
	 */
	@Override
	public Set<TypedTuple<E>> intersectWithScores(Collection<? extends RedisZSet<?>> sets) {
		return boundZSetOps.intersectWithScores(CollectionUtils.extractKeys(sets));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.support.collections.RedisZSet#intersectAndStore(org.springframework.data.redis.support.collections.RedisZSet, java.lang.String)
	 */
	@Override
	public RedisZSet<E> intersectAndStore(RedisZSet<?> set, String destKey) {

		boundZSetOps.intersectAndStore(set.getKey(), destKey);
		return new DefaultRedisZSet<>(boundZSetOps.getOperations().boundZSetOps(destKey), getDefaultScore());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.support.collections.RedisZSet#intersectAndStore(java.util.Collection, java.lang.String)
	 */
	@Override
	public RedisZSet<E> intersectAndStore(Collection<? extends RedisZSet<?>> sets, String destKey) {

		boundZSetOps.intersectAndStore(CollectionUtils.extractKeys(sets), destKey);
		return new DefaultRedisZSet<>(boundZSetOps.getOperations().boundZSetOps(destKey), getDefaultScore());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.support.collections.RedisZSet#union(org.springframework.data.redis.support.collections.RedisZSet)
	 */
	@Override
	public Set<E> union(RedisZSet<?> set) {
		return boundZSetOps.union(set.getKey());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.support.collections.RedisZSet#union(java.util.Collection)
	 */
	@Override
	public Set<E> union(Collection<? extends RedisZSet<?>> sets) {
		return boundZSetOps.union(CollectionUtils.extractKeys(sets));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.support.collections.RedisZSet#unionWithScores(org.springframework.data.redis.support.collections.RedisZSet)
	 */
	@Override
	public Set<TypedTuple<E>> unionWithScores(RedisZSet<?> set) {
		return boundZSetOps.unionWithScores(set.getKey());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.support.collections.RedisZSet#unionWithScores(java.util.Collection)
	 */
	@Override
	public Set<TypedTuple<E>> unionWithScores(Collection<? extends RedisZSet<?>> sets) {
		return boundZSetOps.unionWithScores(CollectionUtils.extractKeys(sets));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.support.collections.RedisZSet#unionAndStore(org.springframework.data.redis.support.collections.RedisZSet, java.lang.String)
	 */
	@Override
	public RedisZSet<E> unionAndStore(RedisZSet<?> set, String destKey) {
		boundZSetOps.unionAndStore(set.getKey(), destKey);
		return new DefaultRedisZSet<>(boundZSetOps.getOperations().boundZSetOps(destKey), getDefaultScore());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.support.collections.RedisZSet#unionAndStore(java.util.Collection, java.lang.String)
	 */
	@Override
	public RedisZSet<E> unionAndStore(Collection<? extends RedisZSet<?>> sets, String destKey) {
		boundZSetOps.unionAndStore(CollectionUtils.extractKeys(sets), destKey);
		return new DefaultRedisZSet<>(boundZSetOps.getOperations().boundZSetOps(destKey), getDefaultScore());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.support.collections.RedisZSet#randomElement()
	 */
	@Override
	public E randomValue() {
		return boundZSetOps.randomMember();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.support.collections.RedisZSet#range(long, long)
	 */
	@Override
	public Set<E> range(long start, long end) {
		return boundZSetOps.range(start, end);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.support.collections.RedisZSet#reverseRange(long, long)
	 */
	@Override
	public Set<E> reverseRange(long start, long end) {
		return boundZSetOps.reverseRange(start, end);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.support.collections.RedisZSet#rangeByLex(org.springframework.data.redis.connection.RedisZSetCommands.Range, org.springframework.data.redis.connection.RedisZSetCommands.Limit)
	 */
	@Override
	public Set<E> rangeByLex(Range range, Limit limit) {
		return boundZSetOps.rangeByLex(range, limit);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.support.collections.RedisZSet#reverseRangeByLex(org.springframework.data.redis.connection.RedisZSetCommands.Range, org.springframework.data.redis.connection.RedisZSetCommands.Limit)
	 */
	@Override
	public Set<E> reverseRangeByLex(Range range, Limit limit) {
		return boundZSetOps.reverseRangeByLex(range, limit);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.support.collections.RedisZSet#rangeByScore(double, double)
	 */
	@Override
	public Set<E> rangeByScore(double min, double max) {
		return boundZSetOps.rangeByScore(min, max);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.support.collections.RedisZSet#reverseRangeByScore(double, double)
	 */
	@Override
	public Set<E> reverseRangeByScore(double min, double max) {
		return boundZSetOps.reverseRangeByScore(min, max);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.support.collections.RedisZSet#rangeByScoreWithScores(double, double)
	 */
	@Override
	public Set<TypedTuple<E>> rangeByScoreWithScores(double min, double max) {
		return boundZSetOps.rangeByScoreWithScores(min, max);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.support.collections.RedisZSet#rangeWithScores(long, long)
	 */
	@Override
	public Set<TypedTuple<E>> rangeWithScores(long start, long end) {
		return boundZSetOps.rangeWithScores(start, end);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.support.collections.RedisZSet#reverseRangeByScoreWithScores(double, double)
	 */
	@Override
	public Set<TypedTuple<E>> reverseRangeByScoreWithScores(double min, double max) {
		return boundZSetOps.reverseRangeByScoreWithScores(min, max);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.support.collections.RedisZSet#reverseRangeWithScores(long, long)
	 */
	@Override
	public Set<TypedTuple<E>> reverseRangeWithScores(long start, long end) {
		return boundZSetOps.reverseRangeWithScores(start, end);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.support.collections.RedisZSet#remove(long, long)
	 */
	@Override
	public RedisZSet<E> remove(long start, long end) {
		boundZSetOps.removeRange(start, end);
		return this;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.support.collections.RedisZSet#removeRangeByLex(org.springframework.data.redis.connection.RedisZSetCommands.Range)
	 */
	@Override
	public Set<E> removeByLex(Range range) {
		boundZSetOps.removeRangeByLex(range);
		return this;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.support.collections.RedisZSet#removeByScore(double, double)
	 */
	@Override
	public RedisZSet<E> removeByScore(double min, double max) {
		boundZSetOps.removeRangeByScore(min, max);
		return this;
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.AbstractCollection#add(java.lang.Object)
	 */
	@Override
	public boolean add(E e) {
		Boolean result = add(e, getDefaultScore());
		checkResult(result);
		return result;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.support.collections.RedisZSet#add(java.lang.Object, double)
	 */
	@Override
	public boolean add(E e, double score) {
		Boolean result = boundZSetOps.add(e, score);
		checkResult(result);
		return result;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.support.collections.RedisZSet#addIfAbsent(java.lang.Object, double)
	 */
	@Override
	public boolean addIfAbsent(E e, double score) {

		Boolean result = boundZSetOps.addIfAbsent(e, score);
		checkResult(result);
		return result;
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.AbstractCollection#clear()
	 */
	@Override
	public void clear() {
		boundZSetOps.removeRange(0, -1);
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.AbstractCollection#contains(java.lang.Object)
	 */
	@Override
	public boolean contains(Object o) {
		return (boundZSetOps.rank(o) != null);
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.AbstractCollection#iterator()
	 */
	@Override
	public Iterator<E> iterator() {
		Set<E> members = boundZSetOps.range(0, -1);
		checkResult(members);
		return new DefaultRedisSortedSetIterator(members.iterator());
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.AbstractCollection#remove(java.lang.Object)
	 */
	@Override
	public boolean remove(Object o) {

		Long result = boundZSetOps.remove(o);
		checkResult(result);
		return result == 1;
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.AbstractCollection#size()
	 */
	@Override
	public int size() {

		Long result = boundZSetOps.size();
		checkResult(result);
		return result.intValue();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.support.collections.RedisZSet#getDefaultScore()
	 */
	@Override
	public Double getDefaultScore() {
		return defaultScore;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.support.collections.RedisZSet#first()
	 */
	@Override
	public E first() {

		Set<E> members = boundZSetOps.range(0, 0);
		checkResult(members);
		Iterator<E> iterator = members.iterator();

		if (iterator.hasNext()) {
			return iterator.next();
		}

		throw new NoSuchElementException();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.support.collections.RedisZSet#popFirst()
	 */
	@Override
	public E popFirst() {

		TypedTuple<E> tuple = boundZSetOps.popMin();

		if (tuple != null) {
			return tuple.getValue();
		}

		throw new NoSuchElementException();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.support.collections.RedisZSet#popFirst(long, java.util.concurrent.TimeUnit)
	 */
	@Override
	public E popFirst(long timeout, TimeUnit unit) {

		TypedTuple<E> tuple = boundZSetOps.popMin(timeout, unit);

		if (tuple != null) {
			return tuple.getValue();
		}

		throw new NoSuchElementException();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.support.collections.RedisZSet#last()
	 */
	@Override
	public E last() {

		Set<E> members = boundZSetOps.reverseRange(0, 0);
		checkResult(members);
		Iterator<E> iterator = members.iterator();
		if (iterator.hasNext()) {
			return iterator.next();
		}

		throw new NoSuchElementException();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.support.collections.RedisZSet#popLast()
	 */
	@Override
	public E popLast() {

		TypedTuple<E> tuple = boundZSetOps.popMax();

		if (tuple != null) {
			return tuple.getValue();
		}

		throw new NoSuchElementException();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.support.collections.RedisZSet#popLast(long, java.util.concurrent.TimeUnit)
	 */
	@Override
	public E popLast(long timeout, TimeUnit unit) {

		TypedTuple<E> tuple = boundZSetOps.popMax(timeout, unit);

		if (tuple != null) {
			return tuple.getValue();
		}

		throw new NoSuchElementException();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.support.collections.RedisZSet#rank(java.lang.Object)
	 */
	@Override
	public Long rank(Object o) {
		return boundZSetOps.rank(o);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.support.collections.RedisZSet#reverseRank(java.lang.Object)
	 */
	@Override
	public Long reverseRank(Object o) {
		return boundZSetOps.reverseRank(o);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.support.collections.RedisZSet#lexCount(org.springframework.data.redis.connection.RedisZSetCommands.Range)
	 */
	@Override
	public Long lexCount(Range range) {
		return boundZSetOps.lexCount(range);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.support.collections.RedisZSet#score(java.lang.Object)
	 */
	@Override
	public Double score(Object o) {
		return boundZSetOps.score(o);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundKeyOperations#getType()
	 */
	@Override
	public DataType getType() {
		return DataType.ZSET;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.support.collections.RedisZSet#scan()
	 */
	@Override
	public Cursor<E> scan() {
		return new ConvertingCursor<>(scan(ScanOptions.NONE), TypedTuple::getValue);
	}

	/**
	 * @since 1.4
	 * @param options
	 * @return
	 */
	public Cursor<TypedTuple<E>> scan(ScanOptions options) {
		return boundZSetOps.scan(options);
	}
}
