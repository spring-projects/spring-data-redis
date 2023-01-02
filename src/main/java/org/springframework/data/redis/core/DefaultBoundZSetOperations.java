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

package org.springframework.data.redis.core;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.connection.RedisZSetCommands.Aggregate;
import org.springframework.data.redis.connection.RedisZSetCommands.Limit;
import org.springframework.data.redis.connection.RedisZSetCommands.Range;
import org.springframework.data.redis.connection.RedisZSetCommands.Weights;
import org.springframework.data.redis.core.ZSetOperations.TypedTuple;
import org.springframework.lang.Nullable;

/**
 * Default implementation for {@link BoundZSetOperations}.
 *
 * @author Costin Leau
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Wongoo (望哥)
 * @author Andrey Shlykov
 */
class DefaultBoundZSetOperations<K, V> extends DefaultBoundKeyOperations<K> implements BoundZSetOperations<K, V> {

	private final ZSetOperations<K, V> ops;

	/**
	 * Constructs a new <code>DefaultBoundZSetOperations</code> instance.
	 *
	 * @param key
	 * @param operations
	 */
	DefaultBoundZSetOperations(K key, RedisOperations<K, V> operations) {

		super(key, operations);
		this.ops = operations.opsForZSet();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundZSetOperations#add(java.lang.Object, double)
	 */
	@Override
	public Boolean add(V value, double score) {
		return ops.add(getKey(), value, score);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundZSetOperations#addIfAbsent(java.lang.Object, double)
	 */
	@Override
	public Boolean addIfAbsent(V value, double score) {
		return ops.addIfAbsent(getKey(), value, score);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundZSetOperations#add(java.util.Set)
	 */
	@Override
	public Long add(Set<TypedTuple<V>> tuples) {
		return ops.add(getKey(), tuples);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundZSetOperations#addIfAbsent(java.util.Set)
	 */
	@Override
	public Long addIfAbsent(Set<TypedTuple<V>> tuples) {
		return ops.addIfAbsent(getKey(), tuples);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundZSetOperations#incrementScore(java.lang.Object, double)
	 */
	@Override
	public Double incrementScore(V value, double delta) {
		return ops.incrementScore(getKey(), value, delta);
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundZSetOperations#randomMember()
	 */
	@Override
	public V randomMember() {
		return ops.randomMember(getKey());
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundZSetOperations#distinctRandomMembers(long)
	 */
	@Nullable
	@Override
	public Set<V> distinctRandomMembers(long count) {
		return ops.distinctRandomMembers(getKey(), count);
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundZSetOperations#randomMembers(long)
	 */
	@Nullable
	@Override
	public List<V> randomMembers(long count) {
		return ops.randomMembers(getKey(), count);
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundZSetOperations#randomMemberWithScore()
	 */
	@Override
	public TypedTuple<V> randomMemberWithScore() {
		return ops.randomMemberWithScore(getKey());
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundZSetOperations#distinctRandomMembersWithScore(long)
	 */
	@Nullable
	@Override
	public Set<TypedTuple<V>> distinctRandomMembersWithScore(long count) {
		return ops.distinctRandomMembersWithScore(getKey(), count);
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundZSetOperations#randomMembersWithScore(long)
	 */
	@Nullable
	@Override
	public List<TypedTuple<V>> randomMembersWithScore(long count) {
		return ops.randomMembersWithScore(getKey(), count);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundZSetOperations#getOperations()
	 */
	@Override
	public RedisOperations<K, V> getOperations() {
		return ops.getOperations();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundZSetOperations#range(long, long)
	 */
	@Override
	public Set<V> range(long start, long end) {
		return ops.range(getKey(), start, end);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundZSetOperations#rangeByScore(double, double)
	 */
	@Override
	public Set<V> rangeByScore(double min, double max) {
		return ops.rangeByScore(getKey(), min, max);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundZSetOperations#rangeByScoreWithScores(double, double)
	 */
	@Override
	public Set<TypedTuple<V>> rangeByScoreWithScores(double min, double max) {
		return ops.rangeByScoreWithScores(getKey(), min, max);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundZSetOperations#rangeWithScores(long, long)
	 */
	@Override
	public Set<TypedTuple<V>> rangeWithScores(long start, long end) {
		return ops.rangeWithScores(getKey(), start, end);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundZSetOperations#reverseRangeByScore(double, double)
	 */
	@Override
	public Set<V> reverseRangeByScore(double min, double max) {
		return ops.reverseRangeByScore(getKey(), min, max);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundZSetOperations#reverseRangeByScoreWithScores(double, double)
	 */
	@Override
	public Set<TypedTuple<V>> reverseRangeByScoreWithScores(double min, double max) {
		return ops.reverseRangeByScoreWithScores(getKey(), min, max);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundZSetOperations#reverseRangeWithScores(long, long)
	 */
	@Override
	public Set<TypedTuple<V>> reverseRangeWithScores(long start, long end) {
		return ops.reverseRangeWithScores(getKey(), start, end);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundZSetOperations#rangeByLex(org.springframework.data.redis.connection.RedisZSetCommands.Range, org.springframework.data.redis.connection.RedisZSetCommands.Limit)
	 */
	@Override
	public Set<V> rangeByLex(Range range, Limit limit) {
		return ops.rangeByLex(getKey(), range, limit);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundZSetOperations#reverseRangeByLex(org.springframework.data.redis.connection.RedisZSetCommands.Range, org.springframework.data.redis.connection.RedisZSetCommands.Limit)
	 */
	@Override
	public Set<V> reverseRangeByLex(Range range, Limit limit) {
		return ops.reverseRangeByLex(getKey(), range, limit);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundZSetOperations#rank(java.lang.Object)
	 */
	@Override
	public Long rank(Object o) {
		return ops.rank(getKey(), o);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundZSetOperations#reverseRank(java.lang.Object)
	 */
	@Override
	public Long reverseRank(Object o) {
		return ops.reverseRank(getKey(), o);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundZSetOperations#score(java.lang.Object)
	 */
	@Override
	public Double score(Object o) {
		return ops.score(getKey(), o);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundZSetOperations#score(java.lang.Object[])
	 */
	@Override
	public List<Double> score(Object... o) {
		return ops.score(getKey(), o);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundZSetOperations#remove(java.lang.Object[])
	 */
	@Override
	public Long remove(Object... values) {
		return ops.remove(getKey(), values);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundZSetOperations#removeRange(long, long)
	 */
	@Override
	public Long removeRange(long start, long end) {
		return ops.removeRange(getKey(), start, end);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundZSetOperations#removeRangeByLex(org.springframework.data.redis.connection.RedisZSetCommands.Range)
	 */
	@Override
	public Long removeRangeByLex(Range range) {
		return ops.removeRangeByLex(getKey(), range);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundZSetOperations#removeRangeByScore(double, double)
	 */
	@Override
	public Long removeRangeByScore(double min, double max) {
		return ops.removeRangeByScore(getKey(), min, max);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundZSetOperations#reverseRange(long, long)
	 */
	@Override
	public Set<V> reverseRange(long start, long end) {
		return ops.reverseRange(getKey(), start, end);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundZSetOperations#count(double, double)
	 */
	@Override
	public Long count(double min, double max) {
		return ops.count(getKey(), min, max);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundZSetOperations#lexCount(org.springframework.data.redis.connection.RedisZSetCommands.Range)
	 */
	@Override
	public Long lexCount(Range range) {
		return ops.lexCount(getKey(), range);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundZSetOperations#popMin()
	 */
	@Nullable
	@Override
	public TypedTuple<V> popMin() {
		return ops.popMin(getKey());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundZSetOperations#popMin(long)
	 */
	@Nullable
	@Override
	public Set<TypedTuple<V>> popMin(long count) {
		return ops.popMin(getKey(), count);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundZSetOperations#popMin(long, java.util.concurrent.TimeUnit)
	 */
	@Nullable
	@Override
	public TypedTuple<V> popMin(long timeout, TimeUnit unit) {
		return ops.popMin(getKey(), timeout, unit);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundZSetOperations#popMax()
	 */
	@Nullable
	@Override
	public TypedTuple<V> popMax() {
		return ops.popMax(getKey());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundZSetOperations#popMax(long)
	 */
	@Nullable
	@Override
	public Set<TypedTuple<V>> popMax(long count) {
		return ops.popMax(getKey(), count);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundZSetOperations#popMax(long, java.util.concurrent.TimeUnit)
	 */
	@Nullable
	@Override
	public TypedTuple<V> popMax(long timeout, TimeUnit unit) {
		return ops.popMax(getKey(), timeout, unit);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundZSetOperations#size()
	 */
	@Override
	public Long size() {
		return zCard();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundZSetOperations#zCard()
	 */
	@Override
	public Long zCard() {
		return ops.zCard(getKey());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundZSetOperations#difference(java.util.Collection)
	 */
	@Nullable
	@Override
	public Set<V> difference(Collection<K> otherKeys) {
		return ops.difference(getKey(), otherKeys);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundZSetOperations#differenceWithScores(java.util.Collection)
	 */
	@Nullable
	@Override
	public Set<TypedTuple<V>> differenceWithScores(Collection<K> otherKeys) {
		return ops.differenceWithScores(getKey(), otherKeys);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundZSetOperations#differenceAndStore(java.util.Collection, java.lang.Object)
	 */
	@Nullable
	@Override
	public Long differenceAndStore(Collection<K> otherKeys, K destKey) {
		return ops.differenceAndStore(getKey(), otherKeys, destKey);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundZSetOperations#intersect(java.util.Collection)
	 */
	@Nullable
	@Override
	public Set<V> intersect(Collection<K> otherKeys) {
		return ops.intersect(getKey(), otherKeys);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundZSetOperations#intersectWithScores(java.util.Collection)
	 */
	@Nullable
	@Override
	public Set<TypedTuple<V>> intersectWithScores(Collection<K> otherKeys) {
		return ops.intersectWithScores(getKey(), otherKeys);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundZSetOperations#intersectWithScores(java.util.Collection, org.springframework.data.redis.connection.RedisZSetCommands.Aggregate, org.springframework.data.redis.connection.RedisZSetCommands.Weights)
	 */
	@Nullable
	@Override
	public Set<TypedTuple<V>> intersectWithScores(Collection<K> otherKeys, Aggregate aggregate, Weights weights) {
		return ops.intersectWithScores(getKey(), otherKeys, aggregate, weights);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundZSetOperations#intersectAndStore(java.lang.Object, java.lang.Object)
	 */
	@Override
	public Long intersectAndStore(K otherKey, K destKey) {
		return ops.intersectAndStore(getKey(), otherKey, destKey);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundZSetOperations#intersectAndStore(java.util.Collection, java.lang.Object)
	 */
	@Override
	public Long intersectAndStore(Collection<K> otherKeys, K destKey) {
		return ops.intersectAndStore(getKey(), otherKeys, destKey);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundZSetOperations#intersectAndStore(java.util.Collection, java.lang.Object, org.springframework.data.redis.connection.RedisZSetCommands.Aggregate)
	 */
	@Override
	public Long intersectAndStore(Collection<K> otherKeys, K destKey, Aggregate aggregate) {
		return ops.intersectAndStore(getKey(), otherKeys, destKey, aggregate);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundZSetOperations#intersectAndStore(java.util.Collection, java.lang.Object, org.springframework.data.redis.connection.RedisZSetCommands.Aggregate, org.springframework.data.redis.connection.RedisZSetCommands.Weights)
	 */
	@Override
	public Long intersectAndStore(Collection<K> otherKeys, K destKey, Aggregate aggregate, Weights weights) {
		return ops.intersectAndStore(getKey(), otherKeys, destKey, aggregate, weights);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundZSetOperations#union(java.util.Collection)
	 */
	@Nullable
	@Override
	public Set<V> union(Collection<K> otherKeys) {
		return ops.union(getKey(), otherKeys);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundZSetOperations#unionWithScores(java.util.Collection)
	 */
	@Nullable
	@Override
	public Set<TypedTuple<V>> unionWithScores(Collection<K> otherKeys) {
		return ops.unionWithScores(getKey(), otherKeys);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundZSetOperations#unionWithScores(java.util.Collection, org.springframework.data.redis.connection.RedisZSetCommands.Aggregate, org.springframework.data.redis.connection.RedisZSetCommands.Weights)
	 */
	@Nullable
	@Override
	public Set<TypedTuple<V>> unionWithScores(Collection<K> otherKeys, Aggregate aggregate, Weights weights) {
		return ops.unionWithScores(getKey(), otherKeys, aggregate, weights);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundZSetOperations#unionAndStore(java.lang.Object, java.lang.Object)
	 */
	@Override
	public Long unionAndStore(K otherKey, K destKey) {
		return ops.unionAndStore(getKey(), otherKey, destKey);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundZSetOperations#unionAndStore(java.util.Collection, java.lang.Object)
	 */
	@Override
	public Long unionAndStore(Collection<K> otherKeys, K destKey) {
		return ops.unionAndStore(getKey(), otherKeys, destKey);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundZSetOperations#unionAndStore(java.util.Collection, java.lang.Object, org.springframework.data.redis.connection.RedisZSetCommands.Aggregate)
	 */
	@Override
	public Long unionAndStore(Collection<K> otherKeys, K destKey, Aggregate aggregate) {
		return ops.unionAndStore(getKey(), otherKeys, destKey, aggregate);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundZSetOperations#unionAndStore(java.util.Collection, java.lang.Object, org.springframework.data.redis.connection.RedisZSetCommands.Aggregate, org.springframework.data.redis.connection.RedisZSetCommands.Weights)
	 */
	@Override
	public Long unionAndStore(Collection<K> otherKeys, K destKey, Aggregate aggregate, Weights weights) {
		return ops.unionAndStore(getKey(), otherKeys, destKey, aggregate, weights);
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
	 * @see org.springframework.data.redis.core.BoundZSetOperations#scan(org.springframework.data.redis.core.ScanOptions)
	 */
	@Override
	public Cursor<TypedTuple<V>> scan(ScanOptions options) {
		return ops.scan(getKey(), options);
	}
}
