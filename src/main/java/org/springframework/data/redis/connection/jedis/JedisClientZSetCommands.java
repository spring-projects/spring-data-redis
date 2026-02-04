/*
 * Copyright 2026-present the original author or authors.
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
package org.springframework.data.redis.connection.jedis;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.NullUnmarked;
import org.jspecify.annotations.Nullable;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.connection.RedisZSetCommands;
import org.springframework.data.redis.connection.zset.Aggregate;
import org.springframework.data.redis.connection.zset.Tuple;
import org.springframework.data.redis.connection.zset.Weights;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.Cursor.CursorId;
import org.springframework.data.redis.core.KeyBoundCursor;
import org.springframework.data.redis.core.ScanIteration;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.util.Assert;

import redis.clients.jedis.Protocol;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.params.ZParams;
import redis.clients.jedis.params.ZRangeParams;
import redis.clients.jedis.resps.ScanResult;
import redis.clients.jedis.util.KeyValue;

import static java.util.stream.Collectors.*;

/**
 * {@link RedisZSetCommands} implementation for Jedis.
 *
 * @author Tihomir Mateev
 * @since 4.1
 */
@NullUnmarked
class JedisClientZSetCommands implements RedisZSetCommands {

	private final JedisClientConnection connection;

	JedisClientZSetCommands(@NonNull JedisClientConnection connection) {
		this.connection = connection;
	}

	@Override
	public Boolean zAdd(byte @NonNull [] key, double score, byte @NonNull [] value, @NonNull ZAddArgs args) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(value, "Value must not be null");

		return connection.execute(client -> client.zadd(key, score, value, JedisConverters.toZAddParams(args)),
				pipeline -> pipeline.zadd(key, score, value, JedisConverters.toZAddParams(args)), JedisConverters::toBoolean);
	}

	@Override
	public Long zAdd(byte @NonNull [] key, @NonNull Set<@NonNull Tuple> tuples, @NonNull ZAddArgs args) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(tuples, "Tuples must not be null");

		Long count = connection.execute(
				client -> client.zadd(key, JedisConverters.toTupleMap(tuples), JedisConverters.toZAddParams(args)),
				pipeline -> pipeline.zadd(key, JedisConverters.toTupleMap(tuples), JedisConverters.toZAddParams(args)));

		return count != null ? count : 0L;
	}

	@Override
	public Long zRem(byte @NonNull [] key, byte @NonNull [] @NonNull... values) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(values, "Values must not be null");
		Assert.noNullElements(values, "Values must not contain null elements");

		return connection.execute(client -> client.zrem(key, values), pipeline -> pipeline.zrem(key, values));
	}

	@Override
	public Double zIncrBy(byte @NonNull [] key, double increment, byte @NonNull [] value) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(value, "Value must not be null");

		return connection.execute(client -> client.zincrby(key, increment, value),
				pipeline -> pipeline.zincrby(key, increment, value));
	}

	@Override
	public byte[] zRandMember(byte @NonNull [] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.execute(client -> client.zrandmember(key), pipeline -> pipeline.zrandmember(key));
	}

	@Override
	public List<byte @NonNull []> zRandMember(byte @NonNull [] key, long count) {

		Assert.notNull(key, "Key must not be null");

		return connection.execute(client -> client.zrandmember(key, count), pipeline -> pipeline.zrandmember(key, count));
	}

	@Override
	public Tuple zRandMemberWithScore(byte @NonNull [] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.execute(client -> client.zrandmemberWithScores(key, 1L),
				pipeline -> pipeline.zrandmemberWithScores(key, 1L),
				result -> result.isEmpty() ? null : JedisConverters.toTuple(result.iterator().next()));
	}

	@Override
	public List<@NonNull Tuple> zRandMemberWithScore(byte @NonNull [] key, long count) {

		Assert.notNull(key, "Key must not be null");

		return connection.execute(client -> client.zrandmemberWithScores(key, count),
				pipeline -> pipeline.zrandmemberWithScores(key, count),
				result -> result.stream().map(JedisConverters::toTuple).toList());
	}

	@Override
	public Long zRank(byte @NonNull [] key, byte @NonNull [] value) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(value, "Value must not be null");

		return connection.execute(client -> client.zrank(key, value), pipeline -> pipeline.zrank(key, value));
	}

	@Override
	public Long zRevRank(byte @NonNull [] key, byte @NonNull [] value) {

		Assert.notNull(key, "Key must not be null");

		return connection.execute(client -> client.zrevrank(key, value), pipeline -> pipeline.zrevrank(key, value));
	}

	@Override
	public Set<byte @NonNull []> zRange(byte @NonNull [] key, long start, long end) {

		Assert.notNull(key, "Key must not be null");

		return connection.execute(client -> client.zrange(key, start, end), pipeline -> pipeline.zrange(key, start, end),
				JedisConverters::toSet);
	}

	@Override
	public Set<@NonNull Tuple> zRangeWithScores(byte @NonNull [] key, long start, long end) {

		Assert.notNull(key, "Key must not be null");

		return connection.execute(client -> client.zrangeWithScores(key, start, end),
				pipeline -> pipeline.zrangeWithScores(key, start, end),
				result -> result.stream().map(JedisConverters::toTuple).collect(toCollection(LinkedHashSet::new)));
	}

	@Override
	public Set<@NonNull Tuple> zRangeByScoreWithScores(byte @NonNull [] key,
			org.springframework.data.domain.@NonNull Range<? extends @NonNull Number> range,
			org.springframework.data.redis.connection.@NonNull Limit limit) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(range, "Range for ZRANGEBYSCOREWITHSCORES must not be null");
		Assert.notNull(limit, "Limit must not be null Use Limit.unlimited() instead");

		byte[] min = JedisConverters.boundaryToBytesForZRange(range.getLowerBound(),
				JedisConverters.NEGATIVE_INFINITY_BYTES);
		byte[] max = JedisConverters.boundaryToBytesForZRange(range.getUpperBound(),
				JedisConverters.POSITIVE_INFINITY_BYTES);

		if (!limit.isUnlimited()) {
			return connection.execute(
					client -> client.zrangeByScoreWithScores(key, min, max, limit.getOffset(), limit.getCount()),
					pipeline -> pipeline.zrangeByScoreWithScores(key, min, max, limit.getOffset(), limit.getCount()),
					result -> result.stream().map(JedisConverters::toTuple).collect(toCollection(LinkedHashSet::new)));
		} else {
			return connection.execute(client -> client.zrangeByScoreWithScores(key, min, max),
					pipeline -> pipeline.zrangeByScoreWithScores(key, min, max),
					result -> result.stream().map(JedisConverters::toTuple).collect(toCollection(LinkedHashSet::new)));
		}
	}

	@Override
	public Set<byte @NonNull []> zRevRange(byte @NonNull [] key, long start, long end) {

		Assert.notNull(key, "Key must not be null");

		return connection.execute(client -> client.zrevrange(key, start, end),
				pipeline -> pipeline.zrevrange(key, start, end), JedisConverters::toSet);
	}

	@Override
	public Set<@NonNull Tuple> zRevRangeWithScores(byte @NonNull [] key, long start, long end) {

		Assert.notNull(key, "Key must not be null");

		return connection.execute(client -> client.zrevrangeWithScores(key, start, end),
				pipeline -> pipeline.zrevrangeWithScores(key, start, end),
				result -> result.stream().map(JedisConverters::toTuple).collect(toCollection(LinkedHashSet::new)));
	}

	@Override
	public Set<byte @NonNull []> zRevRangeByScore(byte @NonNull [] key,
			org.springframework.data.domain.@NonNull Range<? extends @NonNull Number> range,
			org.springframework.data.redis.connection.@NonNull Limit limit) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(range, "Range for ZREVRANGEBYSCORE must not be null");
		Assert.notNull(limit, "Limit must not be null Use Limit.unlimited() instead");

		byte[] min = JedisConverters.boundaryToBytesForZRange(range.getLowerBound(),
				JedisConverters.NEGATIVE_INFINITY_BYTES);
		byte[] max = JedisConverters.boundaryToBytesForZRange(range.getUpperBound(),
				JedisConverters.POSITIVE_INFINITY_BYTES);

		if (!limit.isUnlimited()) {
			return connection.execute(client -> client.zrevrangeByScore(key, max, min, limit.getOffset(), limit.getCount()),
					pipeline -> pipeline.zrevrangeByScore(key, max, min, limit.getOffset(), limit.getCount()),
					JedisConverters::toSet);
		} else {
			return connection.execute(client -> client.zrevrangeByScore(key, max, min),
					pipeline -> pipeline.zrevrangeByScore(key, max, min), JedisConverters::toSet);
		}
	}

	@Override
	public Set<Tuple> zRevRangeByScoreWithScores(byte @NonNull [] key,
			org.springframework.data.domain.@NonNull Range<? extends @NonNull Number> range,
			org.springframework.data.redis.connection.@NonNull Limit limit) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(range, "Range for ZREVRANGEBYSCOREWITHSCORES must not be null");
		Assert.notNull(limit, "Limit must not be null Use Limit.unlimited() instead");

		byte[] min = JedisConverters.boundaryToBytesForZRange(range.getLowerBound(),
				JedisConverters.NEGATIVE_INFINITY_BYTES);
		byte[] max = JedisConverters.boundaryToBytesForZRange(range.getUpperBound(),
				JedisConverters.POSITIVE_INFINITY_BYTES);

		if (!limit.isUnlimited()) {
			return connection.execute(
					client -> client.zrevrangeByScoreWithScores(key, max, min, limit.getOffset(), limit.getCount()),
					pipeline -> pipeline.zrevrangeByScoreWithScores(key, max, min, limit.getOffset(), limit.getCount()),
					result -> result.stream().map(JedisConverters::toTuple).collect(toCollection(LinkedHashSet::new)));
		} else {
			return connection.execute(client -> client.zrevrangeByScoreWithScores(key, max, min),
					pipeline -> pipeline.zrevrangeByScoreWithScores(key, max, min),
					result -> result.stream().map(JedisConverters::toTuple).collect(toCollection(LinkedHashSet::new)));
		}
	}

	@Override
	public Long zCount(byte @NonNull [] key, double min, double max) {

		Assert.notNull(key, "Key must not be null");

		return connection.execute(client -> client.zcount(key, min, max), pipeline -> pipeline.zcount(key, min, max));
	}

	@Override
	public Long zCount(byte @NonNull [] key,
			org.springframework.data.domain.@NonNull Range<? extends @NonNull Number> range) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(range, "Range must not be null");

		byte[] min = JedisConverters.boundaryToBytesForZRange(range.getLowerBound(),
				JedisConverters.NEGATIVE_INFINITY_BYTES);
		byte[] max = JedisConverters.boundaryToBytesForZRange(range.getUpperBound(),
				JedisConverters.POSITIVE_INFINITY_BYTES);

		return connection.execute(client -> client.zcount(key, min, max), pipeline -> pipeline.zcount(key, min, max));
	}

	@Override
	public Long zLexCount(byte @NonNull [] key, org.springframework.data.domain.@NonNull Range<byte[]> range) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(range, "Range must not be null");

		byte[] min = JedisConverters.boundaryToBytesForZRangeByLex(range.getLowerBound(), JedisConverters.MINUS_BYTES);
		byte[] max = JedisConverters.boundaryToBytesForZRangeByLex(range.getUpperBound(), JedisConverters.PLUS_BYTES);

		return connection.execute(client -> client.zlexcount(key, min, max), pipeline -> pipeline.zlexcount(key, min, max));
	}

	@Override
	public Tuple zPopMin(byte @NonNull [] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.execute(client -> client.zpopmin(key), pipeline -> pipeline.zpopmin(key),
				JedisConverters::toTuple);
	}

	@Override
	public Set<Tuple> zPopMin(byte @NonNull [] key, long count) {

		Assert.notNull(key, "Key must not be null");

		return connection.execute(client -> client.zpopmin(key, Math.toIntExact(count)),
				pipeline -> pipeline.zpopmin(key, Math.toIntExact(count)),
				result -> result.stream().map(JedisConverters::toTuple).collect(toCollection(LinkedHashSet::new)));
	}

	@Override
	public Tuple bZPopMin(byte @NonNull [] key, long timeout, @NonNull TimeUnit unit) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(unit, "TimeUnit must not be null");

		return connection.execute(client -> client.bzpopmin(JedisConverters.toSeconds(timeout, unit), key),
				pipeline -> pipeline.bzpopmin(JedisConverters.toSeconds(timeout, unit), key), JedisClientZSetCommands::toTuple);
	}

	@Override
	public Tuple zPopMax(byte @NonNull [] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.execute(client -> client.zpopmax(key), pipeline -> pipeline.zpopmax(key),
				JedisConverters::toTuple);
	}

	@Override
	public Set<@NonNull Tuple> zPopMax(byte @NonNull [] key, long count) {

		Assert.notNull(key, "Key must not be null");

		return connection.execute(client -> client.zpopmax(key, Math.toIntExact(count)),
				pipeline -> pipeline.zpopmax(key, Math.toIntExact(count)),
				result -> result.stream().map(JedisConverters::toTuple).collect(toCollection(LinkedHashSet::new)));
	}

	@Override
	public Tuple bZPopMax(byte @NonNull [] key, long timeout, @NonNull TimeUnit unit) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(unit, "TimeUnit must not be null");

		return connection.execute(client -> client.bzpopmax(JedisConverters.toSeconds(timeout, unit), key),
				pipeline -> pipeline.bzpopmax(JedisConverters.toSeconds(timeout, unit), key), JedisClientZSetCommands::toTuple);
	}

	@Override
	public Long zCard(byte @NonNull [] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.execute(client -> client.zcard(key), pipeline -> pipeline.zcard(key));
	}

	@Override
	public Double zScore(byte @NonNull [] key, byte @NonNull [] value) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(value, "Value must not be null");

		return connection.execute(client -> client.zscore(key, value), pipeline -> pipeline.zscore(key, value));
	}

	@Override
	public List<@NonNull Double> zMScore(byte @NonNull [] key, byte @NonNull [] @NonNull [] values) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(values, "Value must not be null");

		return connection.execute(client -> client.zmscore(key, values), pipeline -> pipeline.zmscore(key, values));
	}

	@Override
	public Long zRemRange(byte @NonNull [] key, long start, long end) {

		Assert.notNull(key, "Key must not be null");

		return connection.execute(client -> client.zremrangeByRank(key, start, end),
				pipeline -> pipeline.zremrangeByRank(key, start, end));
	}

	@Override
	public Long zRemRangeByLex(byte @NonNull [] key, org.springframework.data.domain.@NonNull Range<byte[]> range) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(range, "Range must not be null for ZREMRANGEBYLEX");

		byte[] min = JedisConverters.boundaryToBytesForZRangeByLex(range.getLowerBound(), JedisConverters.MINUS_BYTES);
		byte[] max = JedisConverters.boundaryToBytesForZRangeByLex(range.getUpperBound(), JedisConverters.PLUS_BYTES);

		return connection.execute(client -> client.zremrangeByLex(key, min, max),
				pipeline -> pipeline.zremrangeByLex(key, min, max));
	}

	@Override
	public Long zRemRangeByScore(byte @NonNull [] key,
			org.springframework.data.domain.@NonNull Range<? extends @NonNull Number> range) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(range, "Range for ZREMRANGEBYSCORE must not be null");

		byte[] min = JedisConverters.boundaryToBytesForZRange(range.getLowerBound(),
				JedisConverters.NEGATIVE_INFINITY_BYTES);
		byte[] max = JedisConverters.boundaryToBytesForZRange(range.getUpperBound(),
				JedisConverters.POSITIVE_INFINITY_BYTES);

		return connection.execute(client -> client.zremrangeByScore(key, min, max),
				pipeline -> pipeline.zremrangeByScore(key, min, max));
	}

	@Override
	public Set<byte @NonNull []> zDiff(byte @NonNull [] @NonNull... sets) {

		Assert.notNull(sets, "Sets must not be null");

		return connection.execute(client -> client.zdiff(sets), pipeline -> pipeline.zdiff(sets), JedisConverters::toSet);
	}

	@Override
	public Set<@NonNull Tuple> zDiffWithScores(byte @NonNull [] @NonNull... sets) {

		Assert.notNull(sets, "Sets must not be null");

		return connection.execute(client -> client.zdiffWithScores(sets), pipeline -> pipeline.zdiffWithScores(sets),
				result -> result.stream().map(JedisConverters::toTuple).collect(toCollection(LinkedHashSet::new)));
	}

	@Override
	public Long zDiffStore(byte @NonNull [] destKey, byte @NonNull [] @NonNull... sets) {

		Assert.notNull(destKey, "Destination key must not be null");
		Assert.notNull(sets, "Source sets must not be null");

		return connection.execute(client -> client.zdiffstore(destKey, sets),
				pipeline -> pipeline.zdiffstore(destKey, sets));
	}

	@Override
	public Set<byte @NonNull []> zInter(byte @NonNull [] @NonNull... sets) {

		Assert.notNull(sets, "Sets must not be null");

		return connection.execute(client -> client.zinter(new ZParams(), sets),
				pipeline -> pipeline.zinter(new ZParams(), sets), JedisConverters::toSet);
	}

	@Override
	public Set<@NonNull Tuple> zInterWithScores(byte @NonNull [] @NonNull... sets) {

		Assert.notNull(sets, "Sets must not be null");

		return connection.execute(client -> client.zinterWithScores(new ZParams(), sets),
				pipeline -> pipeline.zinterWithScores(new ZParams(), sets),
				result -> result.stream().map(JedisConverters::toTuple).collect(toCollection(LinkedHashSet::new)));
	}

	@Override
	public Set<@NonNull Tuple> zInterWithScores(@NonNull Aggregate aggregate, @NonNull Weights weights,
			byte @NonNull [] @NonNull... sets) {

		Assert.notNull(sets, "Sets must not be null");
		Assert.noNullElements(sets, "Source sets must not contain null elements");
		Assert.isTrue(weights.size() == sets.length,
				"The number of weights (%d) must match the number of source sets (%d)".formatted(weights.size(), sets.length));

		return connection.execute(client -> client.zinterWithScores(toZParams(aggregate, weights), sets),
				pipeline -> pipeline.zinterWithScores(toZParams(aggregate, weights), sets),
				result -> result.stream().map(JedisConverters::toTuple).collect(toCollection(LinkedHashSet::new)));
	}

	@Override
	public Long zInterStore(byte @NonNull [] destKey, @NonNull Aggregate aggregate, @NonNull Weights weights,
			byte @NonNull [] @NonNull... sets) {

		Assert.notNull(destKey, "Destination key must not be null");
		Assert.notNull(sets, "Source sets must not be null");
		Assert.noNullElements(sets, "Source sets must not contain null elements");
		Assert.isTrue(weights.size() == sets.length,
				"The number of weights %d must match the number of source sets %d".formatted(weights.size(), sets.length));

		ZParams zparams = toZParams(aggregate, weights);

		return connection.execute(client -> client.zinterstore(destKey, zparams, sets),
				pipeline -> pipeline.zinterstore(destKey, zparams, sets));
	}

	@Override
	public Long zInterStore(byte @NonNull [] destKey, byte @NonNull [] @NonNull... sets) {

		Assert.notNull(destKey, "Destination key must not be null");
		Assert.notNull(sets, "Source sets must not be null");
		Assert.noNullElements(sets, "Source sets must not contain null elements");

		return connection.execute(client -> client.zinterstore(destKey, sets),
				pipeline -> pipeline.zinterstore(destKey, sets));
	}

	@Override
	public Set<byte @NonNull []> zUnion(byte @NonNull [] @NonNull... sets) {

		Assert.notNull(sets, "Sets must not be null");

		return connection.execute(client -> client.zunion(new ZParams(), sets),
				pipeline -> pipeline.zunion(new ZParams(), sets), JedisConverters::toSet);
	}

	@Override
	public Set<@NonNull Tuple> zUnionWithScores(byte @NonNull [] @NonNull... sets) {

		Assert.notNull(sets, "Sets must not be null");

		return connection.execute(client -> client.zunionWithScores(new ZParams(), sets),
				pipeline -> pipeline.zunionWithScores(new ZParams(), sets),
				result -> result.stream().map(JedisConverters::toTuple).collect(toCollection(LinkedHashSet::new)));
	}

	@Override
	public Set<@NonNull Tuple> zUnionWithScores(@NonNull Aggregate aggregate, @NonNull Weights weights,
			byte @NonNull [] @NonNull... sets) {

		Assert.notNull(sets, "Sets must not be null");
		Assert.noNullElements(sets, "Source sets must not contain null elements");
		Assert.isTrue(weights.size() == sets.length,
				"The number of weights %d must match the number of source sets %d".formatted(weights.size(), sets.length));

		return connection.execute(client -> client.zunionWithScores(toZParams(aggregate, weights), sets),
				pipeline -> pipeline.zunionWithScores(toZParams(aggregate, weights), sets),
				result -> result.stream().map(JedisConverters::toTuple).collect(toCollection(LinkedHashSet::new)));
	}

	@Override
	public Long zUnionStore(byte @NonNull [] destKey, @NonNull Aggregate aggregate, @NonNull Weights weights,
			byte @NonNull [] @NonNull... sets) {

		Assert.notNull(destKey, "Destination key must not be null");
		Assert.notNull(sets, "Source sets must not be null");
		Assert.notNull(weights, "Weights must not be null");
		Assert.noNullElements(sets, "Source sets must not contain null elements");
		Assert.isTrue(weights.size() == sets.length,
				"The number of weights %d must match the number of source sets %d".formatted(weights.size(), sets.length));

		ZParams zparams = toZParams(aggregate, weights);

		return connection.execute(client -> client.zunionstore(destKey, zparams, sets),
				pipeline -> pipeline.zunionstore(destKey, zparams, sets));
	}

	@Override
	public Long zUnionStore(byte @NonNull [] destKey, byte @NonNull [] @NonNull... sets) {

		Assert.notNull(destKey, "Destination key must not be null");
		Assert.notNull(sets, "Source sets must not be null");
		Assert.noNullElements(sets, "Source sets must not contain null elements");

		return connection.execute(client -> client.zunionstore(destKey, sets),
				pipeline -> pipeline.zunionstore(destKey, sets));
	}

	@Override
	public Cursor<@NonNull Tuple> zScan(byte @NonNull [] key, ScanOptions options) {
		return zScan(key, CursorId.initial(), options);
	}

	/**
	 * @param key the key to scan
	 * @param cursorId the {@link CursorId} to use
	 * @param options the {@link ScanOptions} to use
	 * @return a new {@link Cursor} responsible for tььhe provided {@link CursorId} and {@link ScanOptions}
	 */
	public Cursor<@NonNull Tuple> zScan(byte @NonNull [] key, @NonNull CursorId cursorId, @NonNull ScanOptions options) {

		Assert.notNull(key, "Key must not be null");

		return new KeyBoundCursor<Tuple>(key, cursorId, options) {

			@Override
			protected ScanIteration<@NonNull Tuple> doScan(byte @NonNull [] key, @NonNull CursorId cursorId,
					@NonNull ScanOptions options) {
				if (isQueueing() || isPipelined()) {
					throw new InvalidDataAccessApiUsageException("'ZSCAN' cannot be called in pipeline / transaction mode");
				}

				ScanParams params = JedisConverters.toScanParams(options);

				ScanResult<redis.clients.jedis.resps.Tuple> result = connection.getJedis().zscan(key,
						JedisConverters.toBytes(cursorId), params);
				return new ScanIteration<>(CursorId.of(result.getCursor()),
						JedisConverters.tuplesToTuples().convert(result.getResult()));
			}

			@Override
			protected void doClose() {
				JedisClientZSetCommands.this.connection.close();
			}

		}.open();
	}

	@Override
	public Set<byte @NonNull []> zRangeByScore(byte @NonNull [] key, @NonNull String min, @NonNull String max) {

		Assert.notNull(key, "Key must not be null");

		return connection.execute(
				client -> client.zrangeByScore(key, JedisConverters.toBytes(min), JedisConverters.toBytes(max)),
				pipeline -> pipeline.zrangeByScore(key, JedisConverters.toBytes(min), JedisConverters.toBytes(max)),
				JedisConverters::toSet);
	}

	@Override
	public Set<byte @NonNull []> zRangeByScore(byte @NonNull [] key, @NonNull String min, @NonNull String max,
			long offset, long count) {

		Assert.notNull(key, "Key must not be null");

		if (offset > Integer.MAX_VALUE || count > Integer.MAX_VALUE) {

			throw new IllegalArgumentException(
					"Offset and count must be less than Integer.MAX_VALUE for zRangeByScore in Jedis");
		}

		return connection.execute(
				client -> client.zrangeByScore(key, JedisConverters.toBytes(min), JedisConverters.toBytes(max), (int) offset,
						(int) count),
				pipeline -> pipeline.zrangeByScore(key, JedisConverters.toBytes(min), JedisConverters.toBytes(max),
						(int) offset, (int) count),
				JedisConverters::toSet);
	}

	@Override
	public Set<byte @NonNull []> zRangeByScore(byte @NonNull [] key,
			org.springframework.data.domain.@NonNull Range<? extends @NonNull Number> range,
			org.springframework.data.redis.connection.@NonNull Limit limit) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(range, "Range for ZRANGEBYSCORE must not be null");
		Assert.notNull(limit, "Limit must not be null Use Limit.unlimited() instead");

		byte[] min = JedisConverters.boundaryToBytesForZRange(range.getLowerBound(),
				JedisConverters.NEGATIVE_INFINITY_BYTES);
		byte[] max = JedisConverters.boundaryToBytesForZRange(range.getUpperBound(),
				JedisConverters.POSITIVE_INFINITY_BYTES);

		if (!limit.isUnlimited()) {
			return connection.execute(client -> client.zrangeByScore(key, min, max, limit.getOffset(), limit.getCount()),
					pipeline -> pipeline.zrangeByScore(key, min, max, limit.getOffset(), limit.getCount()),
					JedisConverters::toSet);
		} else {
			return connection.execute(client -> client.zrangeByScore(key, min, max),
					pipeline -> pipeline.zrangeByScore(key, min, max), JedisConverters::toSet);
		}
	}

	@Override
	public Set<byte @NonNull []> zRangeByLex(byte @NonNull [] key,
			org.springframework.data.domain.@NonNull Range<byte[]> range,
			org.springframework.data.redis.connection.@NonNull Limit limit) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(range, "Range for ZRANGEBYLEX must not be null");
		Assert.notNull(limit, "Limit must not be null Use Limit.unlimited() instead");

		byte[] min = JedisConverters.boundaryToBytesForZRangeByLex(range.getLowerBound(), JedisConverters.MINUS_BYTES);
		byte[] max = JedisConverters.boundaryToBytesForZRangeByLex(range.getUpperBound(), JedisConverters.PLUS_BYTES);

		if (!limit.isUnlimited()) {
			return connection.execute(client -> client.zrangeByLex(key, min, max, limit.getOffset(), limit.getCount()),
					pipeline -> pipeline.zrangeByLex(key, min, max, limit.getOffset(), limit.getCount()), JedisConverters::toSet);
		} else {
			return connection.execute(client -> client.zrangeByLex(key, min, max),
					pipeline -> pipeline.zrangeByLex(key, min, max), JedisConverters::toSet);
		}
	}

	@Override
	public Set<byte @NonNull []> zRevRangeByLex(byte @NonNull [] key,
			org.springframework.data.domain.@NonNull Range<byte[]> range,
			org.springframework.data.redis.connection.@NonNull Limit limit) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(range, "Range for ZREVRANGEBYLEX must not be null");
		Assert.notNull(limit, "Limit must not be null Use Limit.unlimited() instead.");

		byte[] min = JedisConverters.boundaryToBytesForZRangeByLex(range.getLowerBound(), JedisConverters.MINUS_BYTES);
		byte[] max = JedisConverters.boundaryToBytesForZRangeByLex(range.getUpperBound(), JedisConverters.PLUS_BYTES);

		if (!limit.isUnlimited()) {
			return connection.execute(client -> client.zrevrangeByLex(key, max, min, limit.getOffset(), limit.getCount()),
					pipeline -> pipeline.zrevrangeByLex(key, max, min, limit.getOffset(), limit.getCount()),
					JedisConverters::toSet);
		} else {
			return connection.execute(client -> client.zrevrangeByLex(key, max, min),
					pipeline -> pipeline.zrevrangeByLex(key, max, min), JedisConverters::toSet);
		}
	}

	@Override
	public Long zRangeStoreByLex(byte @NonNull [] dstKey, byte @NonNull [] srcKey,
			org.springframework.data.domain.@NonNull Range<byte[]> range,
			org.springframework.data.redis.connection.@NonNull Limit limit) {
		return zRangeStoreByLex(dstKey, srcKey, range, limit, false);
	}

	@Override
	public Long zRangeStoreRevByLex(byte @NonNull [] dstKey, byte @NonNull [] srcKey,
			org.springframework.data.domain.@NonNull Range<byte[]> range,
			org.springframework.data.redis.connection.@NonNull Limit limit) {
		return zRangeStoreByLex(dstKey, srcKey, range, limit, true);
	}

	private Long zRangeStoreByLex(byte @NonNull [] dstKey, byte @NonNull [] srcKey,
			org.springframework.data.domain.@NonNull Range<byte[]> range,
			org.springframework.data.redis.connection.@NonNull Limit limit, boolean rev) {

		Assert.notNull(dstKey, "Destination key must not be null");
		Assert.notNull(srcKey, "Source key must not be null");
		Assert.notNull(range, "Range must not be null");
		Assert.notNull(limit, "Limit must not be null. Use Limit.unlimited() instead.");

		byte[] min = JedisConverters.boundaryToBytesForZRangeByLex(range.getLowerBound(), JedisConverters.MINUS_BYTES);
		byte[] max = JedisConverters.boundaryToBytesForZRangeByLex(range.getUpperBound(), JedisConverters.PLUS_BYTES);

		ZRangeParams zRangeParams = toZRangeParams(Protocol.Keyword.BYLEX, min, max, limit, rev);

		return connection.execute(client -> client.zrangestore(dstKey, srcKey, zRangeParams),
				pipeline -> pipeline.zrangestore(dstKey, srcKey, zRangeParams));
	}

	@Override
	public Long zRangeStoreByScore(byte @NonNull [] dstKey, byte @NonNull [] srcKey,
			org.springframework.data.domain.@NonNull Range<? extends @NonNull Number> range,
			org.springframework.data.redis.connection.@NonNull Limit limit) {
		return zRangeStoreByScore(dstKey, srcKey, range, limit, false);
	}

	@Override
	public Long zRangeStoreRevByScore(byte @NonNull [] dstKey, byte @NonNull [] srcKey,
			org.springframework.data.domain.@NonNull Range<? extends @NonNull Number> range,
			org.springframework.data.redis.connection.@NonNull Limit limit) {
		return zRangeStoreByScore(dstKey, srcKey, range, limit, true);
	}

	private Long zRangeStoreByScore(byte @NonNull [] dstKey, byte @NonNull [] srcKey,
			org.springframework.data.domain.@NonNull Range<? extends @NonNull Number> range,
			org.springframework.data.redis.connection.@NonNull Limit limit, boolean rev) {

		Assert.notNull(dstKey, "Destination key must not be null");
		Assert.notNull(srcKey, "Source key must not be null");
		Assert.notNull(range, "Range must not be null");
		Assert.notNull(limit, "Limit must not be null. Use Limit.unlimited() instead.");

		byte[] min = JedisConverters.boundaryToBytesForZRange(range.getLowerBound(),
				JedisConverters.NEGATIVE_INFINITY_BYTES);
		byte[] max = JedisConverters.boundaryToBytesForZRange(range.getUpperBound(),
				JedisConverters.POSITIVE_INFINITY_BYTES);

		ZRangeParams zRangeParams = toZRangeParams(Protocol.Keyword.BYSCORE, min, max, limit, rev);

		return connection.execute(client -> client.zrangestore(dstKey, srcKey, zRangeParams),
				pipeline -> pipeline.zrangestore(dstKey, srcKey, zRangeParams));
	}

	private boolean isPipelined() {
		return connection.isPipelined();
	}

	private boolean isQueueing() {
		return connection.isQueueing();
	}

	private static ZParams toZParams(Aggregate aggregate, Weights weights) {
		return new ZParams().weights(weights.toArray()).aggregate(ZParams.Aggregate.valueOf(aggregate.name()));
	}

	static ZRangeParams toZRangeParams(Protocol.Keyword by, byte[] min, byte[] max,
			org.springframework.data.redis.connection.Limit limit, boolean rev) {

		return JedisZSetCommands.toZRangeParams(by, min, max, limit, rev);
	}

	private @Nullable static Tuple toTuple(@Nullable KeyValue<?, redis.clients.jedis.resps.Tuple> keyValue) {
		return keyValue != null ? JedisConverters.toTuple(keyValue.getValue()) : null;
	}
}
