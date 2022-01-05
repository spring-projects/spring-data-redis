/*
 * Copyright 2011-2021 the original author or authors.
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

	@Override
	public Boolean add(V value, double score) {
		return ops.add(getKey(), value, score);
	}

	@Override
	public Boolean addIfAbsent(V value, double score) {
		return ops.addIfAbsent(getKey(), value, score);
	}

	@Override
	public Long add(Set<TypedTuple<V>> tuples) {
		return ops.add(getKey(), tuples);
	}

	@Override
	public Long addIfAbsent(Set<TypedTuple<V>> tuples) {
		return ops.addIfAbsent(getKey(), tuples);
	}

	@Override
	public Double incrementScore(V value, double delta) {
		return ops.incrementScore(getKey(), value, delta);
	}

	@Override
	public V randomMember() {
		return ops.randomMember(getKey());
	}

	@Nullable
	@Override
	public Set<V> distinctRandomMembers(long count) {
		return ops.distinctRandomMembers(getKey(), count);
	}

	@Nullable
	@Override
	public List<V> randomMembers(long count) {
		return ops.randomMembers(getKey(), count);
	}

	@Override
	public TypedTuple<V> randomMemberWithScore() {
		return ops.randomMemberWithScore(getKey());
	}

	@Nullable
	@Override
	public Set<TypedTuple<V>> distinctRandomMembersWithScore(long count) {
		return ops.distinctRandomMembersWithScore(getKey(), count);
	}

	@Nullable
	@Override
	public List<TypedTuple<V>> randomMembersWithScore(long count) {
		return ops.randomMembersWithScore(getKey(), count);
	}

	@Override
	public RedisOperations<K, V> getOperations() {
		return ops.getOperations();
	}

	@Override
	public Set<V> range(long start, long end) {
		return ops.range(getKey(), start, end);
	}

	@Override
	public Set<V> rangeByScore(double min, double max) {
		return ops.rangeByScore(getKey(), min, max);
	}

	@Override
	public Set<TypedTuple<V>> rangeByScoreWithScores(double min, double max) {
		return ops.rangeByScoreWithScores(getKey(), min, max);
	}

	@Override
	public Set<TypedTuple<V>> rangeWithScores(long start, long end) {
		return ops.rangeWithScores(getKey(), start, end);
	}

	@Override
	public Set<V> reverseRangeByScore(double min, double max) {
		return ops.reverseRangeByScore(getKey(), min, max);
	}

	@Override
	public Set<TypedTuple<V>> reverseRangeByScoreWithScores(double min, double max) {
		return ops.reverseRangeByScoreWithScores(getKey(), min, max);
	}

	@Override
	public Set<TypedTuple<V>> reverseRangeWithScores(long start, long end) {
		return ops.reverseRangeWithScores(getKey(), start, end);
	}

	@Override
	public Set<V> rangeByLex(Range range, Limit limit) {
		return ops.rangeByLex(getKey(), range, limit);
	}

	@Override
	public Set<V> reverseRangeByLex(Range range, Limit limit) {
		return ops.reverseRangeByLex(getKey(), range, limit);
	}

	@Override
	public Long rank(Object o) {
		return ops.rank(getKey(), o);
	}

	@Override
	public Long reverseRank(Object o) {
		return ops.reverseRank(getKey(), o);
	}

	@Override
	public Double score(Object o) {
		return ops.score(getKey(), o);
	}

	@Override
	public List<Double> score(Object... o) {
		return ops.score(getKey(), o);
	}

	@Override
	public Long remove(Object... values) {
		return ops.remove(getKey(), values);
	}

	@Override
	public Long removeRange(long start, long end) {
		return ops.removeRange(getKey(), start, end);
	}

	@Override
	public Long removeRangeByLex(Range range) {
		return ops.removeRangeByLex(getKey(), range);
	}

	@Override
	public Long removeRangeByScore(double min, double max) {
		return ops.removeRangeByScore(getKey(), min, max);
	}

	@Override
	public Set<V> reverseRange(long start, long end) {
		return ops.reverseRange(getKey(), start, end);
	}

	@Override
	public Long count(double min, double max) {
		return ops.count(getKey(), min, max);
	}

	@Override
	public Long lexCount(Range range) {
		return ops.lexCount(getKey(), range);
	}

	@Nullable
	@Override
	public TypedTuple<V> popMin() {
		return ops.popMin(getKey());
	}

	@Nullable
	@Override
	public Set<TypedTuple<V>> popMin(long count) {
		return ops.popMin(getKey(), count);
	}

	@Nullable
	@Override
	public TypedTuple<V> popMin(long timeout, TimeUnit unit) {
		return ops.popMin(getKey(), timeout, unit);
	}

	@Nullable
	@Override
	public TypedTuple<V> popMax() {
		return ops.popMax(getKey());
	}

	@Nullable
	@Override
	public Set<TypedTuple<V>> popMax(long count) {
		return ops.popMax(getKey(), count);
	}

	@Nullable
	@Override
	public TypedTuple<V> popMax(long timeout, TimeUnit unit) {
		return ops.popMax(getKey(), timeout, unit);
	}

	@Override
	public Long size() {
		return zCard();
	}

	@Override
	public Long zCard() {
		return ops.zCard(getKey());
	}

	@Nullable
	@Override
	public Set<V> difference(Collection<K> otherKeys) {
		return ops.difference(getKey(), otherKeys);
	}

	@Nullable
	@Override
	public Set<TypedTuple<V>> differenceWithScores(Collection<K> otherKeys) {
		return ops.differenceWithScores(getKey(), otherKeys);
	}

	@Nullable
	@Override
	public Long differenceAndStore(Collection<K> otherKeys, K destKey) {
		return ops.differenceAndStore(getKey(), otherKeys, destKey);
	}

	@Nullable
	@Override
	public Set<V> intersect(Collection<K> otherKeys) {
		return ops.intersect(getKey(), otherKeys);
	}

	@Nullable
	@Override
	public Set<TypedTuple<V>> intersectWithScores(Collection<K> otherKeys) {
		return ops.intersectWithScores(getKey(), otherKeys);
	}

	@Nullable
	@Override
	public Set<TypedTuple<V>> intersectWithScores(Collection<K> otherKeys, Aggregate aggregate, Weights weights) {
		return ops.intersectWithScores(getKey(), otherKeys, aggregate, weights);
	}

	@Override
	public Long intersectAndStore(K otherKey, K destKey) {
		return ops.intersectAndStore(getKey(), otherKey, destKey);
	}

	@Override
	public Long intersectAndStore(Collection<K> otherKeys, K destKey) {
		return ops.intersectAndStore(getKey(), otherKeys, destKey);
	}

	@Override
	public Long intersectAndStore(Collection<K> otherKeys, K destKey, Aggregate aggregate) {
		return ops.intersectAndStore(getKey(), otherKeys, destKey, aggregate);
	}

	@Override
	public Long intersectAndStore(Collection<K> otherKeys, K destKey, Aggregate aggregate, Weights weights) {
		return ops.intersectAndStore(getKey(), otherKeys, destKey, aggregate, weights);
	}

	@Nullable
	@Override
	public Set<V> union(Collection<K> otherKeys) {
		return ops.union(getKey(), otherKeys);
	}

	@Nullable
	@Override
	public Set<TypedTuple<V>> unionWithScores(Collection<K> otherKeys) {
		return ops.unionWithScores(getKey(), otherKeys);
	}

	@Nullable
	@Override
	public Set<TypedTuple<V>> unionWithScores(Collection<K> otherKeys, Aggregate aggregate, Weights weights) {
		return ops.unionWithScores(getKey(), otherKeys, aggregate, weights);
	}

	@Override
	public Long unionAndStore(K otherKey, K destKey) {
		return ops.unionAndStore(getKey(), otherKey, destKey);
	}

	@Override
	public Long unionAndStore(Collection<K> otherKeys, K destKey) {
		return ops.unionAndStore(getKey(), otherKeys, destKey);
	}

	@Override
	public Long unionAndStore(Collection<K> otherKeys, K destKey, Aggregate aggregate) {
		return ops.unionAndStore(getKey(), otherKeys, destKey, aggregate);
	}

	@Override
	public Long unionAndStore(Collection<K> otherKeys, K destKey, Aggregate aggregate, Weights weights) {
		return ops.unionAndStore(getKey(), otherKeys, destKey, aggregate, weights);
	}

	@Override
	public DataType getType() {
		return DataType.ZSET;
	}

	@Override
	public Cursor<TypedTuple<V>> scan(ScanOptions options) {
		return ops.scan(getKey(), options);
	}
}
