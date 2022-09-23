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
package org.springframework.data.redis.core;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.Limit;
import org.springframework.data.redis.connection.RedisZSetCommands.ZAddArgs;
import org.springframework.data.redis.connection.zset.Aggregate;
import org.springframework.data.redis.connection.zset.Tuple;
import org.springframework.data.redis.connection.zset.Weights;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Default implementation of {@link ZSetOperations}.
 *
 * @author Costin Leau
 * @author Christoph Strobl
 * @author Thomas Darimont
 * @author David Liu
 * @author Mark Paluch
 * @author Wongoo (望哥)
 * @author Andrey Shlykov
 * @author Shyngys Sapraliyev
 */
class DefaultZSetOperations<K, V> extends AbstractOperations<K, V> implements ZSetOperations<K, V> {

	DefaultZSetOperations(RedisTemplate<K, V> template) {
		super(template);
	}

	@Override
	public Boolean add(K key, V value, double score) {

		byte[] rawKey = rawKey(key);
		byte[] rawValue = rawValue(value);
		return execute(connection -> connection.zAdd(rawKey, score, rawValue));
	}

	@Override
	public Boolean addIfAbsent(K key, V value, double score) {
		return add(key, value, score, ZAddArgs.ifNotExists());
	}

	/**
	 * @param key must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @param args never {@literal null}.
	 * @return can be {@literal null}.
	 * @since 2.5
	 */
	@Nullable
	protected Boolean add(K key, V value, double score, ZAddArgs args) {

		byte[] rawKey = rawKey(key);
		byte[] rawValue = rawValue(value);
		return execute(connection -> connection.zAdd(rawKey, score, rawValue, args));
	}

	@Override
	public Long add(K key, Set<TypedTuple<V>> tuples) {

		byte[] rawKey = rawKey(key);
		Set<Tuple> rawValues = rawTupleValues(tuples);
		return execute(connection -> connection.zAdd(rawKey, rawValues));
	}

	@Override
	public Long addIfAbsent(K key, Set<TypedTuple<V>> tuples) {
		return add(key, tuples, ZAddArgs.ifNotExists());
	}

	/**
	 * @param key must not be {@literal null}.
	 * @param tuples must not be {@literal null}.
	 * @param args never {@literal null}.
	 * @return can be {@literal null}.
	 * @since 2.5
	 */
	@Nullable
	protected Long add(K key, Set<TypedTuple<V>> tuples, ZAddArgs args) {

		byte[] rawKey = rawKey(key);
		Set<Tuple> rawValues = rawTupleValues(tuples);
		return execute(connection -> connection.zAdd(rawKey, rawValues, args));
	}

	@Override
	public Double incrementScore(K key, V value, double delta) {

		byte[] rawKey = rawKey(key);
		byte[] rawValue = rawValue(value);
		return execute(connection -> connection.zIncrBy(rawKey, delta, rawValue));
	}

	@Override
	public V randomMember(K key) {

		byte[] rawKey = rawKey(key);

		return deserializeValue(execute(connection -> connection.zRandMember(rawKey)));
	}

	@Override
	public Set<V> distinctRandomMembers(K key, long count) {

		Assert.isTrue(count > 0, "Negative count not supported; Use randomMembers to allow duplicate elements");

		byte[] rawKey = rawKey(key);

		List<byte[]> result = execute(connection -> connection.zRandMember(rawKey, count));
		return result != null ? deserializeValues(new LinkedHashSet<>(result)) : null;
	}

	@Override
	public List<V> randomMembers(K key, long count) {

		Assert.isTrue(count > 0, "Use a positive number for count; This method is already allowing duplicate elements");

		byte[] rawKey = rawKey(key);

		List<byte[]> result = execute(connection -> connection.zRandMember(rawKey, count));
		return deserializeValues(result);
	}

	@Override
	public TypedTuple<V> randomMemberWithScore(K key) {

		byte[] rawKey = rawKey(key);

		return deserializeTuple(execute(connection -> connection.zRandMemberWithScore(rawKey)));
	}

	@Override
	public Set<TypedTuple<V>> distinctRandomMembersWithScore(K key, long count) {

		Assert.isTrue(count > 0, "Negative count not supported; Use randomMembers to allow duplicate elements");

		byte[] rawKey = rawKey(key);

		List<Tuple> result = execute(connection -> connection.zRandMemberWithScore(rawKey, count));
		return result != null ? deserializeTupleValues(new LinkedHashSet<>(result)) : null;
	}

	@Override
	public List<TypedTuple<V>> randomMembersWithScore(K key, long count) {

		Assert.isTrue(count > 0, "Use a positive number for count; This method is already allowing duplicate elements");

		byte[] rawKey = rawKey(key);

		List<Tuple> result = execute(connection -> connection.zRandMemberWithScore(rawKey, count));
		return result != null ? deserializeTupleValues(result) : null;
	}

	@Override
	public Set<V> range(K key, long start, long end) {

		byte[] rawKey = rawKey(key);
		Set<byte[]> rawValues = execute(connection -> connection.zRange(rawKey, start, end));

		return deserializeValues(rawValues);
	}

	@Override
	public Set<V> reverseRange(K key, long start, long end) {

		byte[] rawKey = rawKey(key);
		Set<byte[]> rawValues = execute(connection -> connection.zRevRange(rawKey, start, end));

		return deserializeValues(rawValues);
	}

	@Override
	public Set<TypedTuple<V>> rangeWithScores(K key, long start, long end) {

		byte[] rawKey = rawKey(key);
		Set<Tuple> rawValues = execute(connection -> connection.zRangeWithScores(rawKey, start, end));

		return deserializeTupleValues(rawValues);
	}

	@Override
	public Set<TypedTuple<V>> reverseRangeWithScores(K key, long start, long end) {

		byte[] rawKey = rawKey(key);
		Set<Tuple> rawValues = execute(connection -> connection.zRevRangeWithScores(rawKey, start, end));

		return deserializeTupleValues(rawValues);
	}

	@Override
	public Set<V> rangeByLex(K key, Range<String> range, Limit limit) {

		byte[] rawKey = rawKey(key);
		Set<byte[]> rawValues = execute(connection -> connection.zRangeByLex(rawKey, serialize(range), limit));

		return deserializeValues(rawValues);
	}

	@Override
	public Set<V> reverseRangeByLex(K key, Range<String> range, Limit limit) {

		byte[] rawKey = rawKey(key);
		Set<byte[]> rawValues = execute(connection -> connection.zRevRangeByLex(rawKey, serialize(range), limit));

		return deserializeValues(rawValues);
	}

	@Override
	public Long rangeAndStoreByLex(K srcKey, K dstKey, Range<String> range, Limit limit) {
		byte[] rawDstKey = rawKey(dstKey);
		byte[] rawSrcKey = rawKey(srcKey);
		return execute(connection -> connection.zRangeStoreByLex(rawDstKey, rawSrcKey, serialize(range), limit));
	}

	@Override
	public Long reverseRangeAndStoreByLex(K srcKey, K dstKey, Range<String> range, Limit limit) {
		byte[] rawDstKey = rawKey(dstKey);
		byte[] rawSrcKey = rawKey(srcKey);
		return execute(connection -> connection.zRangeStoreRevByLex(rawDstKey, rawSrcKey, serialize(range), limit));
	}

	@Override
	public Long rangeAndStoreByScore(K srcKey, K dstKey, Range<Number> range, Limit limit) {
		byte[] rawDstKey = rawKey(dstKey);
		byte[] rawSrcKey = rawKey(srcKey);
		return execute(connection -> connection.zRangeStoreByScore(rawDstKey, rawSrcKey, range, limit));
	}

	@Override
	public Long reverseRangeAndStoreByScore(K srcKey, K dstKey, Range<Number> range, Limit limit) {
		byte[] rawDstKey = rawKey(dstKey);
		byte[] rawSrcKey = rawKey(srcKey);
		return execute(connection -> connection.zRangeStoreRevByScore(rawDstKey, rawSrcKey, range, limit));
	}

	@Override
	public Set<V> rangeByScore(K key, double min, double max) {

		byte[] rawKey = rawKey(key);
		Set<byte[]> rawValues = execute(connection -> connection.zRangeByScore(rawKey, min, max));

		return deserializeValues(rawValues);
	}

	@Override
	public Set<V> rangeByScore(K key, double min, double max, long offset, long count) {

		byte[] rawKey = rawKey(key);
		Set<byte[]> rawValues = execute(connection -> connection.zRangeByScore(rawKey, min, max, offset, count));

		return deserializeValues(rawValues);
	}

	@Override
	public Set<V> reverseRangeByScore(K key, double min, double max) {

		byte[] rawKey = rawKey(key);
		Set<byte[]> rawValues = execute(connection -> connection.zRevRangeByScore(rawKey, min, max));

		return deserializeValues(rawValues);
	}

	@Override
	public Set<V> reverseRangeByScore(K key, double min, double max, long offset, long count) {

		byte[] rawKey = rawKey(key);
		Set<byte[]> rawValues = execute(connection -> connection.zRevRangeByScore(rawKey, min, max, offset, count));

		return deserializeValues(rawValues);
	}

	@Override
	public Set<TypedTuple<V>> rangeByScoreWithScores(K key, double min, double max) {

		byte[] rawKey = rawKey(key);
		Set<Tuple> rawValues = execute(connection -> connection.zRangeByScoreWithScores(rawKey, min, max));

		return deserializeTupleValues(rawValues);
	}

	@Override
	public Set<TypedTuple<V>> rangeByScoreWithScores(K key, double min, double max, long offset, long count) {

		byte[] rawKey = rawKey(key);
		Set<Tuple> rawValues = execute(connection -> connection.zRangeByScoreWithScores(rawKey, min, max, offset, count));

		return deserializeTupleValues(rawValues);
	}

	@Override
	public Set<TypedTuple<V>> reverseRangeByScoreWithScores(K key, double min, double max) {

		byte[] rawKey = rawKey(key);
		Set<Tuple> rawValues = execute(connection -> connection.zRevRangeByScoreWithScores(rawKey, min, max));

		return deserializeTupleValues(rawValues);
	}

	@Override
	public Set<TypedTuple<V>> reverseRangeByScoreWithScores(K key, double min, double max, long offset, long count) {

		byte[] rawKey = rawKey(key);
		Set<Tuple> rawValues = execute(
				connection -> connection.zRevRangeByScoreWithScores(rawKey, min, max, offset, count));

		return deserializeTupleValues(rawValues);
	}

	@Override
	public Long rank(K key, Object o) {

		byte[] rawKey = rawKey(key);
		byte[] rawValue = rawValue(o);

		return execute(connection -> {
			Long zRank = connection.zRank(rawKey, rawValue);
			return (zRank != null && zRank.longValue() >= 0 ? zRank : null);
		});
	}

	@Override
	public Long reverseRank(K key, Object o) {

		byte[] rawKey = rawKey(key);
		byte[] rawValue = rawValue(o);

		return execute(connection -> {
			Long zRank = connection.zRevRank(rawKey, rawValue);
			return (zRank != null && zRank.longValue() >= 0 ? zRank : null);
		});
	}

	@Override
	public Long remove(K key, Object... values) {

		byte[] rawKey = rawKey(key);
		byte[][] rawValues = rawValues(values);

		return execute(connection -> connection.zRem(rawKey, rawValues));
	}

	@Override
	public Long removeRange(K key, long start, long end) {

		byte[] rawKey = rawKey(key);
		return execute(connection -> connection.zRemRange(rawKey, start, end));
	}

	@Override
	public Long removeRangeByLex(K key, Range<String> range) {

		byte[] rawKey = rawKey(key);
		return execute(connection -> connection.zRemRangeByLex(rawKey, serialize(range)));
	}

	@Override
	public Long removeRangeByScore(K key, double min, double max) {

		byte[] rawKey = rawKey(key);
		return execute(connection -> connection.zRemRangeByScore(rawKey, min, max));
	}

	@Override
	public Double score(K key, Object o) {

		byte[] rawKey = rawKey(key);
		byte[] rawValue = rawValue(o);
		return execute(connection -> connection.zScore(rawKey, rawValue));
	}

	@Override
	public List<Double> score(K key, Object... o) {

		byte[] rawKey = rawKey(key);
		byte[][] rawValues = rawValues(o);
		return execute(connection -> connection.zMScore(rawKey, rawValues));
	}

	@Override
	public Long count(K key, double min, double max) {

		byte[] rawKey = rawKey(key);
		return execute(connection -> connection.zCount(rawKey, min, max));
	}

	@Override
	public Long lexCount(K key, Range<String> range) {

		byte[] rawKey = rawKey(key);
		return execute(connection -> connection.zLexCount(rawKey, serialize(range)));
	}

	@Nullable
	@Override
	public TypedTuple<V> popMin(K key) {

		byte[] rawKey = rawKey(key);
		return deserializeTuple(execute(connection -> connection.zPopMin(rawKey)));
	}

	@Nullable
	@Override
	public Set<TypedTuple<V>> popMin(K key, long count) {

		byte[] rawKey = rawKey(key);
		Set<Tuple> result = execute(connection -> connection.zPopMin(rawKey, count));
		return deserializeTupleValues(new LinkedHashSet<>(result));
	}

	@Nullable
	@Override
	public TypedTuple<V> popMin(K key, long timeout, TimeUnit unit) {

		byte[] rawKey = rawKey(key);
		return deserializeTuple(execute(connection -> connection.bZPopMin(rawKey, timeout, unit)));
	}

	@Nullable
	@Override
	public TypedTuple<V> popMax(K key) {

		byte[] rawKey = rawKey(key);
		return deserializeTuple(execute(connection -> connection.zPopMax(rawKey)));
	}

	@Nullable
	@Override
	public Set<TypedTuple<V>> popMax(K key, long count) {

		byte[] rawKey = rawKey(key);
		Set<Tuple> result = execute(connection -> connection.zPopMax(rawKey, count));
		return deserializeTupleValues(new LinkedHashSet<>(result));
	}

	@Nullable
	@Override
	public TypedTuple<V> popMax(K key, long timeout, TimeUnit unit) {

		byte[] rawKey = rawKey(key);
		return deserializeTuple(execute(connection -> connection.bZPopMax(rawKey, timeout, unit)));
	}

	@Override
	public Long size(K key) {
		return zCard(key);
	}

	@Override
	public Long zCard(K key) {

		byte[] rawKey = rawKey(key);
		return execute(connection -> connection.zCard(rawKey));
	}

	@Override
	public Set<V> difference(K key, Collection<K> otherKeys) {

		byte[][] rawKeys = rawKeys(key, otherKeys);
		Set<byte[]> rawValues = execute(connection -> connection.zDiff(rawKeys));
		return deserializeValues(rawValues);
	}

	@Override
	public Set<TypedTuple<V>> differenceWithScores(K key, Collection<K> otherKeys) {

		byte[][] rawKeys = rawKeys(key, otherKeys);
		Set<Tuple> result = execute(connection -> connection.zDiffWithScores(rawKeys));
		return deserializeTupleValues(new LinkedHashSet<>(result));
	}

	@Override
	public Long differenceAndStore(K key, Collection<K> otherKeys, K destKey) {

		byte[][] rawKeys = rawKeys(key, otherKeys);
		byte[] rawDestKey = rawKey(destKey);

		return execute(connection -> connection.zDiffStore(rawDestKey, rawKeys));
	}

	@Override
	public Set<V> intersect(K key, Collection<K> otherKeys) {

		byte[][] rawKeys = rawKeys(key, otherKeys);
		Set<byte[]> rawValues = execute(connection -> connection.zInter(rawKeys));
		return deserializeValues(rawValues);
	}

	@Override
	public Set<TypedTuple<V>> intersectWithScores(K key, Collection<K> otherKeys) {

		byte[][] rawKeys = rawKeys(key, otherKeys);
		Set<Tuple> result = execute(connection -> connection.zInterWithScores(rawKeys));
		return deserializeTupleValues(result);
	}

	@Override
	public Set<TypedTuple<V>> intersectWithScores(K key, Collection<K> otherKeys, Aggregate aggregate, Weights weights) {

		byte[][] rawKeys = rawKeys(key, otherKeys);
		Set<Tuple> result = execute(connection -> connection.zInterWithScores(aggregate, weights, rawKeys));
		return deserializeTupleValues(result);
	}

	@Override
	public Long intersectAndStore(K key, K otherKey, K destKey) {
		return intersectAndStore(key, Collections.singleton(otherKey), destKey);
	}

	@Override
	public Long intersectAndStore(K key, Collection<K> otherKeys, K destKey) {

		byte[][] rawKeys = rawKeys(key, otherKeys);
		byte[] rawDestKey = rawKey(destKey);

		return execute(connection -> connection.zInterStore(rawDestKey, rawKeys));
	}

	@Override
	public Long intersectAndStore(K key, Collection<K> otherKeys, K destKey, Aggregate aggregate, Weights weights) {

		byte[][] rawKeys = rawKeys(key, otherKeys);
		byte[] rawDestKey = rawKey(destKey);

		return execute(connection -> connection.zInterStore(rawDestKey, aggregate, weights, rawKeys));
	}

	@Override
	public Set<V> union(K key, Collection<K> otherKeys) {

		byte[][] rawKeys = rawKeys(key, otherKeys);
		Set<byte[]> rawValues = execute(connection -> connection.zUnion(rawKeys));
		return deserializeValues(rawValues);
	}

	@Override
	public Set<TypedTuple<V>> unionWithScores(K key, Collection<K> otherKeys) {

		byte[][] rawKeys = rawKeys(key, otherKeys);
		Set<Tuple> result = execute(connection -> connection.zUnionWithScores(rawKeys));
		return deserializeTupleValues(result);
	}

	@Override
	public Set<TypedTuple<V>> unionWithScores(K key, Collection<K> otherKeys, Aggregate aggregate, Weights weights) {

		byte[][] rawKeys = rawKeys(key, otherKeys);
		Set<Tuple> result = execute(connection -> connection.zUnionWithScores(aggregate, weights, rawKeys));
		return deserializeTupleValues(result);
	}

	@Override
	public Long unionAndStore(K key, K otherKey, K destKey) {
		return unionAndStore(key, Collections.singleton(otherKey), destKey);
	}

	@Override
	public Long unionAndStore(K key, Collection<K> otherKeys, K destKey) {

		byte[][] rawKeys = rawKeys(key, otherKeys);
		byte[] rawDestKey = rawKey(destKey);

		return execute(connection -> connection.zUnionStore(rawDestKey, rawKeys));
	}

	@Override
	public Long unionAndStore(K key, Collection<K> otherKeys, K destKey, Aggregate aggregate, Weights weights) {

		byte[][] rawKeys = rawKeys(key, otherKeys);
		byte[] rawDestKey = rawKey(destKey);

		return execute(connection -> connection.zUnionStore(rawDestKey, aggregate, weights, rawKeys));
	}

	@Override
	public Cursor<TypedTuple<V>> scan(K key, ScanOptions options) {

		byte[] rawKey = rawKey(key);
		Cursor<Tuple> cursor = template.executeWithStickyConnection(connection -> connection.zScan(rawKey, options));

		return new ConvertingCursor<>(cursor, this::deserializeTuple);
	}

	public Set<byte[]> rangeByScore(K key, String min, String max) {

		byte[] rawKey = rawKey(key);
		return execute(connection -> connection.zRangeByScore(rawKey, min, max));
	}

	public Set<byte[]> rangeByScore(K key, String min, String max, long offset, long count) {

		byte[] rawKey = rawKey(key);

		return execute(connection -> connection.zRangeByScore(rawKey, min, max, offset, count));
	}

	private Range<byte[]> serialize(Range<String> range) {

		if (!range.getLowerBound().isBounded() && !range.getUpperBound().isBounded()) {
			return Range.unbounded();
		}

		Range.Bound<byte[]> lower = rawBound(range.getLowerBound());
		Range.Bound<byte[]> upper = rawBound(range.getUpperBound());

		return Range.of(lower, upper);
	}

	private Range.Bound<byte[]> rawBound(Range.Bound<String> source) {
		return source.getValue().map(this::rawString)
				.map(it -> source.isInclusive() ? Range.Bound.inclusive(it) : Range.Bound.exclusive(it))
				.orElseGet(Range.Bound::unbounded);
	}

}
