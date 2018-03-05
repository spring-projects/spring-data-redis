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
package org.springframework.data.redis.core;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import org.springframework.data.redis.connection.RedisZSetCommands.Aggregate;
import org.springframework.data.redis.connection.RedisZSetCommands.Limit;
import org.springframework.data.redis.connection.RedisZSetCommands.Range;
import org.springframework.data.redis.connection.RedisZSetCommands.Tuple;
import org.springframework.data.redis.connection.RedisZSetCommands.Weights;

/**
 * Default implementation of {@link ZSetOperations}.
 *
 * @author Costin Leau
 * @author Christoph Strobl
 * @author Thomas Darimont
 * @author David Liu
 * @author Mark Paluch
 * @author Wongoo (望哥)
 */
class DefaultZSetOperations<K, V> extends AbstractOperations<K, V> implements ZSetOperations<K, V> {

	DefaultZSetOperations(RedisTemplate<K, V> template) {
		super(template);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ZSetOperations#add(java.lang.Object, java.lang.Object, double)
	 */
	@Override
	public Boolean add(K key, V value, double score) {

		byte[] rawKey = rawKey(key);
		byte[] rawValue = rawValue(value);
		return execute(connection -> connection.zAdd(rawKey, score, rawValue), true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ZSetOperations#add(java.lang.Object, java.util.Set)
	 */
	@Override
	public Long add(K key, Set<TypedTuple<V>> tuples) {

		byte[] rawKey = rawKey(key);
		Set<Tuple> rawValues = rawTupleValues(tuples);
		return execute(connection -> connection.zAdd(rawKey, rawValues), true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ZSetOperations#incrementScore(java.lang.Object, java.lang.Object, double)
	 */
	@Override
	public Double incrementScore(K key, V value, double delta) {

		byte[] rawKey = rawKey(key);
		byte[] rawValue = rawValue(value);
		return execute(connection -> connection.zIncrBy(rawKey, delta, rawValue), true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ZSetOperations#intersectAndStore(java.lang.Object, java.lang.Object, java.lang.Object)
	 */
	@Override
	public Long intersectAndStore(K key, K otherKey, K destKey) {
		return intersectAndStore(key, Collections.singleton(otherKey), destKey);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ZSetOperations#intersectAndStore(java.lang.Object, java.util.Collection, java.lang.Object)
	 */
	@Override
	public Long intersectAndStore(K key, Collection<K> otherKeys, K destKey) {

		byte[][] rawKeys = rawKeys(key, otherKeys);
		byte[] rawDestKey = rawKey(destKey);

		return execute(connection -> connection.zInterStore(rawDestKey, rawKeys), true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ZSetOperations#intersectAndStore(java.lang.Object, java.util.Collection, java.lang.Object, org.springframework.data.redis.connection.RedisZSetCommands.Aggregate, org.springframework.data.redis.connection.RedisZSetCommands.Weights)
	 */
	@Override
	public Long intersectAndStore(K key, Collection<K> otherKeys, K destKey, Aggregate aggregate, Weights weights) {

		byte[][] rawKeys = rawKeys(key, otherKeys);
		byte[] rawDestKey = rawKey(destKey);

		return execute(connection -> connection.zInterStore(rawDestKey, aggregate, weights, rawKeys), true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ZSetOperations#range(java.lang.Object, long, long)
	 */
	@Override
	public Set<V> range(K key, long start, long end) {

		byte[] rawKey = rawKey(key);
		Set<byte[]> rawValues = execute(connection -> connection.zRange(rawKey, start, end), true);

		return deserializeValues(rawValues);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ZSetOperations#reverseRange(java.lang.Object, long, long)
	 */
	@Override
	public Set<V> reverseRange(K key, long start, long end) {

		byte[] rawKey = rawKey(key);
		Set<byte[]> rawValues = execute(connection -> connection.zRevRange(rawKey, start, end), true);

		return deserializeValues(rawValues);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ZSetOperations#rangeWithScores(java.lang.Object, long, long)
	 */
	@Override
	public Set<TypedTuple<V>> rangeWithScores(K key, long start, long end) {

		byte[] rawKey = rawKey(key);
		Set<Tuple> rawValues = execute(connection -> connection.zRangeWithScores(rawKey, start, end), true);

		return deserializeTupleValues(rawValues);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ZSetOperations#reverseRangeWithScores(java.lang.Object, long, long)
	 */
	@Override
	public Set<TypedTuple<V>> reverseRangeWithScores(K key, long start, long end) {

		byte[] rawKey = rawKey(key);
		Set<Tuple> rawValues = execute(connection -> connection.zRevRangeWithScores(rawKey, start, end), true);

		return deserializeTupleValues(rawValues);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ZSetOperations#rangeByLex(java.lang.Object, org.springframework.data.redis.connection.RedisZSetCommands.Range)
	 */
	@Override
	public Set<V> rangeByLex(K key, Range range) {
		return rangeByLex(key, range, Limit.unlimited());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ZSetOperations#rangeByLex(java.lang.Object, org.springframework.data.redis.connection.RedisZSetCommands.Range, org.springframework.data.redis.connection.RedisZSetCommands.Limit)
	 */
	@Override
	public Set<V> rangeByLex(K key, Range range, Limit limit) {

		byte[] rawKey = rawKey(key);
		Set<byte[]> rawValues = execute(connection -> connection.zRangeByLex(rawKey, range, limit), true);

		return deserializeValues(rawValues);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ZSetOperations#rangeByScore(java.lang.Object, double, double)
	 */
	@Override
	public Set<V> rangeByScore(K key, double min, double max) {

		byte[] rawKey = rawKey(key);
		Set<byte[]> rawValues = execute(connection -> connection.zRangeByScore(rawKey, min, max), true);

		return deserializeValues(rawValues);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ZSetOperations#rangeByScore(java.lang.Object, double, double, long, long)
	 */
	@Override
	public Set<V> rangeByScore(K key, double min, double max, long offset, long count) {

		byte[] rawKey = rawKey(key);
		Set<byte[]> rawValues = execute(connection -> connection.zRangeByScore(rawKey, min, max, offset, count), true);

		return deserializeValues(rawValues);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ZSetOperations#reverseRangeByScore(java.lang.Object, double, double)
	 */
	@Override
	public Set<V> reverseRangeByScore(K key, double min, double max) {

		byte[] rawKey = rawKey(key);
		Set<byte[]> rawValues = execute(connection -> connection.zRevRangeByScore(rawKey, min, max), true);

		return deserializeValues(rawValues);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ZSetOperations#reverseRangeByScore(java.lang.Object, double, double, long, long)
	 */
	@Override
	public Set<V> reverseRangeByScore(K key, double min, double max, long offset, long count) {

		byte[] rawKey = rawKey(key);
		Set<byte[]> rawValues = execute(connection -> connection.zRevRangeByScore(rawKey, min, max, offset, count), true);

		return deserializeValues(rawValues);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ZSetOperations#rangeByScoreWithScores(java.lang.Object, double, double)
	 */
	@Override
	public Set<TypedTuple<V>> rangeByScoreWithScores(K key, double min, double max) {

		byte[] rawKey = rawKey(key);
		Set<Tuple> rawValues = execute(connection -> connection.zRangeByScoreWithScores(rawKey, min, max), true);

		return deserializeTupleValues(rawValues);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ZSetOperations#rangeByScoreWithScores(java.lang.Object, double, double, long, long)
	 */
	@Override
	public Set<TypedTuple<V>> rangeByScoreWithScores(K key, double min, double max, long offset, long count) {

		byte[] rawKey = rawKey(key);
		Set<Tuple> rawValues = execute(connection -> connection.zRangeByScoreWithScores(rawKey, min, max, offset, count),
				true);

		return deserializeTupleValues(rawValues);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ZSetOperations#reverseRangeByScoreWithScores(java.lang.Object, double, double)
	 */
	@Override
	public Set<TypedTuple<V>> reverseRangeByScoreWithScores(K key, double min, double max) {

		byte[] rawKey = rawKey(key);
		Set<Tuple> rawValues = execute(connection -> connection.zRevRangeByScoreWithScores(rawKey, min, max), true);

		return deserializeTupleValues(rawValues);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ZSetOperations#reverseRangeByScoreWithScores(java.lang.Object, double, double, long, long)
	 */
	@Override
	public Set<TypedTuple<V>> reverseRangeByScoreWithScores(K key, double min, double max, long offset, long count) {

		byte[] rawKey = rawKey(key);
		Set<Tuple> rawValues = execute(connection -> connection.zRevRangeByScoreWithScores(rawKey, min, max, offset, count),
				true);

		return deserializeTupleValues(rawValues);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ZSetOperations#rank(java.lang.Object, java.lang.Object)
	 */
	@Override
	public Long rank(K key, Object o) {

		byte[] rawKey = rawKey(key);
		byte[] rawValue = rawValue(o);

		return execute(connection -> {
			Long zRank = connection.zRank(rawKey, rawValue);
			return (zRank != null && zRank.longValue() >= 0 ? zRank : null);
		}, true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ZSetOperations#reverseRank(java.lang.Object, java.lang.Object)
	 */
	@Override
	public Long reverseRank(K key, Object o) {

		byte[] rawKey = rawKey(key);
		byte[] rawValue = rawValue(o);

		return execute(connection -> {
			Long zRank = connection.zRevRank(rawKey, rawValue);
			return (zRank != null && zRank.longValue() >= 0 ? zRank : null);
		}, true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ZSetOperations#remove(java.lang.Object, java.lang.Object[])
	 */
	@Override
	public Long remove(K key, Object... values) {

		byte[] rawKey = rawKey(key);
		byte[][] rawValues = rawValues(values);

		return execute(connection -> connection.zRem(rawKey, rawValues), true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ZSetOperations#removeRange(java.lang.Object, long, long)
	 */
	@Override
	public Long removeRange(K key, long start, long end) {

		byte[] rawKey = rawKey(key);
		return execute(connection -> connection.zRemRange(rawKey, start, end), true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ZSetOperations#removeRangeByScore(java.lang.Object, double, double)
	 */
	@Override
	public Long removeRangeByScore(K key, double min, double max) {

		byte[] rawKey = rawKey(key);
		return execute(connection -> connection.zRemRangeByScore(rawKey, min, max), true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ZSetOperations#score(java.lang.Object, java.lang.Object)
	 */
	@Override
	public Double score(K key, Object o) {

		byte[] rawKey = rawKey(key);
		byte[] rawValue = rawValue(o);
		return execute(connection -> connection.zScore(rawKey, rawValue), true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ZSetOperations#count(java.lang.Object, double, double)
	 */
	@Override
	public Long count(K key, double min, double max) {

		byte[] rawKey = rawKey(key);
		return execute(connection -> connection.zCount(rawKey, min, max), true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ZSetOperations#size(java.lang.Object)
	 */
	@Override
	public Long size(K key) {
		return zCard(key);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ZSetOperations#zCard(java.lang.Object)
	 */
	@Override
	public Long zCard(K key) {

		byte[] rawKey = rawKey(key);
		return execute(connection -> connection.zCard(rawKey), true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ZSetOperations#unionAndStore(java.lang.Object, java.lang.Object, java.lang.Object)
	 */
	@Override
	public Long unionAndStore(K key, K otherKey, K destKey) {
		return unionAndStore(key, Collections.singleton(otherKey), destKey);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ZSetOperations#unionAndStore(java.lang.Object, java.util.Collection, java.lang.Object)
	 */
	@Override
	public Long unionAndStore(K key, Collection<K> otherKeys, K destKey) {

		byte[][] rawKeys = rawKeys(key, otherKeys);
		byte[] rawDestKey = rawKey(destKey);

		return execute(connection -> connection.zUnionStore(rawDestKey, rawKeys), true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ZSetOperations#unionAndStore(java.lang.Object, java.util.Collection, java.lang.Object, org.springframework.data.redis.connection.RedisZSetCommands.Aggregate, org.springframework.data.redis.connection.RedisZSetCommands.Weights)
	 */
	@Override
	public Long unionAndStore(K key, Collection<K> otherKeys, K destKey, Aggregate aggregate, Weights weights) {

		byte[][] rawKeys = rawKeys(key, otherKeys);
		byte[] rawDestKey = rawKey(destKey);

		return execute(connection -> connection.zUnionStore(rawDestKey, aggregate, weights, rawKeys), true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ZSetOperations#scan(java.lang.Object, org.springframework.data.redis.core.ScanOptions)
	 */
	@Override
	public Cursor<TypedTuple<V>> scan(K key, ScanOptions options) {

		byte[] rawKey = rawKey(key);
		Cursor<Tuple> cursor = template.executeWithStickyConnection(connection -> connection.zScan(rawKey, options));

		return new ConvertingCursor<>(cursor, this::deserializeTuple);
	}

	public Set<byte[]> rangeByScore(K key, String min, String max) {

		byte[] rawKey = rawKey(key);
		return execute(connection -> connection.zRangeByScore(rawKey, min, max), true);
	}

	public Set<byte[]> rangeByScore(K key, String min, String max, long offset, long count) {

		byte[] rawKey = rawKey(key);

		return execute(connection -> connection.zRangeByScore(rawKey, min, max, offset, count), true);
	}
}
