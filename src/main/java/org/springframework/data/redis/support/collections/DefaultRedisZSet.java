/*
 * Copyright 2011-2014 the original author or authors.
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
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.connection.RedisZSetCommands;
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
 */
public class DefaultRedisZSet<E> extends AbstractRedisCollection<E> implements RedisZSet<E> {

	private final BoundZSetOperations<String, E> boundZSetOps;
	private double defaultScore = 1;

	private class DefaultRedisSortedSetIterator extends RedisIterator<E> {

		public DefaultRedisSortedSetIterator(Iterator<E> delegate) {
			super(delegate);
		}

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

	@Override
	public Set<E> rangeByLex(RedisZSetCommands.Range range) {
		return boundZSetOps.rangeByLex(range);
	}

	@Override
	public Set<E> rangeByLex(RedisZSetCommands.Range range, RedisZSetCommands.Limit limit) {
		return boundZSetOps.rangeByLex(range, limit);
	}

	public Set<E> rangeByScore(double min, double max) {
		return boundZSetOps.rangeByScore(min, max);
	}

	public Set<E> reverseRangeByScore(double min, double max) {
		return boundZSetOps.reverseRangeByScore(min, max);
	}

	public Set<TypedTuple<E>> rangeByScoreWithScores(double min, double max) {
		return boundZSetOps.rangeByScoreWithScores(min, max);
	}

	public Set<TypedTuple<E>> rangeWithScores(long start, long end) {
		return boundZSetOps.rangeWithScores(start, end);
	}

	public Set<TypedTuple<E>> reverseRangeByScoreWithScores(double min, double max) {
		return boundZSetOps.reverseRangeByScoreWithScores(min, max);
	}

	public Set<TypedTuple<E>> reverseRangeWithScores(long start, long end) {
		return boundZSetOps.reverseRangeWithScores(start, end);
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

	public boolean add(E e) {
		Boolean result = add(e, getDefaultScore());
		checkResult(result);
		return result;
	}

	public boolean add(E e, double score) {
		Boolean result = boundZSetOps.add(e, score);
		checkResult(result);
		return result;
	}

	public void clear() {
		boundZSetOps.removeRange(0, -1);
	}

	public boolean contains(Object o) {
		return (boundZSetOps.rank(o) != null);
	}

	public Iterator<E> iterator() {
		Set<E> members = boundZSetOps.range(0, -1);
		checkResult(members);
		return new DefaultRedisSortedSetIterator(members.iterator());
	}

	public boolean remove(Object o) {
		Long result = boundZSetOps.remove(o);
		checkResult(result);
		return result == 1;
	}

	public int size() {
		Long result = boundZSetOps.size();
		checkResult(result);
		return result.intValue();
	}

	public Double getDefaultScore() {
		return defaultScore;
	}

	public E first() {
		Set<E> members = boundZSetOps.range(0, 0);
		checkResult(members);
		Iterator<E> iterator = members.iterator();

		if (iterator.hasNext())
			return iterator.next();
		throw new NoSuchElementException();
	}

	public E last() {
		Set<E> members = boundZSetOps.reverseRange(0, 0);
		checkResult(members);
		Iterator<E> iterator = members.iterator();
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

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.support.collections.RedisZSet#scan()
	 */
	@Override
	public Cursor<E> scan() {
		return new ConvertingCursor<TypedTuple<E>, E>(scan(ScanOptions.NONE), new Converter<TypedTuple<E>, E>() {

			@Override
			public E convert(TypedTuple<E> source) {
				return source.getValue();
			}
		});
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
