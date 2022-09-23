/*
 * Copyright 2011-2022 the original author or authors.
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

import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.connection.Limit;
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

	@Override
	public Set<E> diff(RedisZSet<?> set) {
		return boundZSetOps.difference(set.getKey());
	}

	@Override
	public Set<E> diff(Collection<? extends RedisZSet<?>> sets) {
		return boundZSetOps.difference(CollectionUtils.extractKeys(sets));
	}

	@Override
	public Set<TypedTuple<E>> diffWithScores(RedisZSet<?> set) {
		return boundZSetOps.differenceWithScores(set.getKey());
	}

	@Override
	public Set<TypedTuple<E>> diffWithScores(Collection<? extends RedisZSet<?>> sets) {
		return boundZSetOps.differenceWithScores(CollectionUtils.extractKeys(sets));
	}

	@Override
	public RedisZSet<E> diffAndStore(RedisZSet<?> set, String destKey) {

		boundZSetOps.differenceAndStore(set.getKey(), destKey);
		return new DefaultRedisZSet<>(boundZSetOps.getOperations().boundZSetOps(destKey), getDefaultScore());
	}

	@Override
	public RedisZSet<E> diffAndStore(Collection<? extends RedisZSet<?>> sets, String destKey) {

		boundZSetOps.differenceAndStore(CollectionUtils.extractKeys(sets), destKey);
		return new DefaultRedisZSet<>(boundZSetOps.getOperations().boundZSetOps(destKey), getDefaultScore());
	}

	@Override
	public Set<E> intersect(RedisZSet<?> set) {
		return boundZSetOps.intersect(set.getKey());
	}

	@Override
	public Set<E> intersect(Collection<? extends RedisZSet<?>> sets) {
		return boundZSetOps.intersect(CollectionUtils.extractKeys(sets));
	}

	@Override
	public Set<TypedTuple<E>> intersectWithScores(RedisZSet<?> set) {
		return boundZSetOps.intersectWithScores(set.getKey());
	}

	@Override
	public Set<TypedTuple<E>> intersectWithScores(Collection<? extends RedisZSet<?>> sets) {
		return boundZSetOps.intersectWithScores(CollectionUtils.extractKeys(sets));
	}

	@Override
	public RedisZSet<E> intersectAndStore(RedisZSet<?> set, String destKey) {

		boundZSetOps.intersectAndStore(set.getKey(), destKey);
		return new DefaultRedisZSet<>(boundZSetOps.getOperations().boundZSetOps(destKey), getDefaultScore());
	}

	@Override
	public RedisZSet<E> intersectAndStore(Collection<? extends RedisZSet<?>> sets, String destKey) {

		boundZSetOps.intersectAndStore(CollectionUtils.extractKeys(sets), destKey);
		return new DefaultRedisZSet<>(boundZSetOps.getOperations().boundZSetOps(destKey), getDefaultScore());
	}

	@Override
	public Set<E> union(RedisZSet<?> set) {
		return boundZSetOps.union(set.getKey());
	}

	@Override
	public Set<E> union(Collection<? extends RedisZSet<?>> sets) {
		return boundZSetOps.union(CollectionUtils.extractKeys(sets));
	}

	@Override
	public Set<TypedTuple<E>> unionWithScores(RedisZSet<?> set) {
		return boundZSetOps.unionWithScores(set.getKey());
	}

	@Override
	public Set<TypedTuple<E>> unionWithScores(Collection<? extends RedisZSet<?>> sets) {
		return boundZSetOps.unionWithScores(CollectionUtils.extractKeys(sets));
	}

	@Override
	public RedisZSet<E> unionAndStore(RedisZSet<?> set, String destKey) {
		boundZSetOps.unionAndStore(set.getKey(), destKey);
		return new DefaultRedisZSet<>(boundZSetOps.getOperations().boundZSetOps(destKey), getDefaultScore());
	}

	@Override
	public RedisZSet<E> unionAndStore(Collection<? extends RedisZSet<?>> sets, String destKey) {
		boundZSetOps.unionAndStore(CollectionUtils.extractKeys(sets), destKey);
		return new DefaultRedisZSet<>(boundZSetOps.getOperations().boundZSetOps(destKey), getDefaultScore());
	}

	@Override
	public E randomValue() {
		return boundZSetOps.randomMember();
	}

	@Override
	public Set<E> range(long start, long end) {
		return boundZSetOps.range(start, end);
	}

	@Override
	public Set<E> reverseRange(long start, long end) {
		return boundZSetOps.reverseRange(start, end);
	}

	@Override
	public Set<E> rangeByLex(Range<String> range, Limit limit) {
		return boundZSetOps.rangeByLex(range, limit);
	}

	@Override
	public Set<E> reverseRangeByLex(Range<String> range, Limit limit) {
		return boundZSetOps.reverseRangeByLex(range, limit);
	}

	@Override
	public Set<E> rangeByScore(double min, double max) {
		return boundZSetOps.rangeByScore(min, max);
	}

	@Override
	public Set<E> reverseRangeByScore(double min, double max) {
		return boundZSetOps.reverseRangeByScore(min, max);
	}

	@Override
	public Set<TypedTuple<E>> rangeByScoreWithScores(double min, double max) {
		return boundZSetOps.rangeByScoreWithScores(min, max);
	}

	@Override
	public Set<TypedTuple<E>> rangeWithScores(long start, long end) {
		return boundZSetOps.rangeWithScores(start, end);
	}

	@Override
	public Set<TypedTuple<E>> reverseRangeByScoreWithScores(double min, double max) {
		return boundZSetOps.reverseRangeByScoreWithScores(min, max);
	}

	@Override
	public Set<TypedTuple<E>> reverseRangeWithScores(long start, long end) {
		return boundZSetOps.reverseRangeWithScores(start, end);
	}

	@Override
	public RedisZSet<E> rangeAndStoreByLex(String dstKey, Range<String> range, Limit limit) {
		boundZSetOps.rangeAndStoreByLex(dstKey, range, limit);
		return new DefaultRedisZSet<>(getOperations().boundZSetOps(dstKey));
	}

	@Override
	public RedisZSet<E> reverseRangeAndStoreByLex(String dstKey, Range<String> range, Limit limit) {
		boundZSetOps.reverseRangeAndStoreByLex(dstKey, range, limit);
		return new DefaultRedisZSet<>(getOperations().boundZSetOps(dstKey));
	}

	@Override
	public RedisZSet<E> rangeAndStoreByScore(String dstKey, Range<? extends Number> range, Limit limit) {
		boundZSetOps.rangeAndStoreByScore(dstKey, range, limit);
		return new DefaultRedisZSet<>(getOperations().boundZSetOps(dstKey));
	}

	@Override
	public RedisZSet<E> reverseRangeAndStoreByScore(String dstKey, Range<? extends Number> range, Limit limit) {
		boundZSetOps.reverseRangeAndStoreByScore(dstKey, range, limit);
		return new DefaultRedisZSet<>(getOperations().boundZSetOps(dstKey));
	}

	@Override
	public RedisZSet<E> remove(long start, long end) {
		boundZSetOps.removeRange(start, end);
		return this;
	}

	@Override
	public RedisZSet<E> removeByLex(Range<String> range) {
		boundZSetOps.removeRangeByLex(range);
		return this;
	}

	@Override
	public RedisZSet<E> removeByScore(double min, double max) {
		boundZSetOps.removeRangeByScore(min, max);
		return this;
	}

	@Override
	public boolean add(E e) {
		Boolean result = add(e, getDefaultScore());
		checkResult(result);
		return result;
	}

	@Override
	public boolean add(E e, double score) {
		Boolean result = boundZSetOps.add(e, score);
		checkResult(result);
		return result;
	}

	@Override
	public boolean addIfAbsent(E e, double score) {

		Boolean result = boundZSetOps.addIfAbsent(e, score);
		checkResult(result);
		return result;
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
		Set<E> members = boundZSetOps.range(0, -1);
		checkResult(members);
		return new DefaultRedisSortedSetIterator(members.iterator());
	}

	@Override
	public boolean remove(Object o) {

		Long result = boundZSetOps.remove(o);
		checkResult(result);
		return result == 1;
	}

	@Override
	public int size() {

		Long result = boundZSetOps.size();
		checkResult(result);
		return result.intValue();
	}

	@Override
	public Double getDefaultScore() {
		return defaultScore;
	}

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

	@Override
	public E popFirst() {

		TypedTuple<E> tuple = boundZSetOps.popMin();

		if (tuple != null) {
			return tuple.getValue();
		}

		throw new NoSuchElementException();
	}

	@Override
	public E popFirst(long timeout, TimeUnit unit) {

		TypedTuple<E> tuple = boundZSetOps.popMin(timeout, unit);

		if (tuple != null) {
			return tuple.getValue();
		}

		throw new NoSuchElementException();
	}

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

	@Override
	public E popLast() {

		TypedTuple<E> tuple = boundZSetOps.popMax();

		if (tuple != null) {
			return tuple.getValue();
		}

		throw new NoSuchElementException();
	}

	@Override
	public E popLast(long timeout, TimeUnit unit) {

		TypedTuple<E> tuple = boundZSetOps.popMax(timeout, unit);

		if (tuple != null) {
			return tuple.getValue();
		}

		throw new NoSuchElementException();
	}

	@Override
	public Long rank(Object o) {
		return boundZSetOps.rank(o);
	}

	@Override
	public Long reverseRank(Object o) {
		return boundZSetOps.reverseRank(o);
	}

	@Override
	public Long lexCount(Range<String> range) {
		return boundZSetOps.lexCount(range);
	}

	@Override
	public Double score(Object o) {
		return boundZSetOps.score(o);
	}

	@Override
	public DataType getType() {
		return DataType.ZSET;
	}

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
