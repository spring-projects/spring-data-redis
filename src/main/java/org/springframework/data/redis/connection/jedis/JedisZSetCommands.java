/*
 * Copyright 2017-2022 the original author or authors.
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

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.commands.PipelineBinaryCommands;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.params.ZParams;
import redis.clients.jedis.params.ZRangeParams;
import redis.clients.jedis.resps.ScanResult;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.connection.RedisZSetCommands;
import org.springframework.data.redis.connection.zset.Aggregate;
import org.springframework.data.redis.connection.zset.DefaultTuple;
import org.springframework.data.redis.connection.zset.Tuple;
import org.springframework.data.redis.connection.zset.Weights;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.KeyBoundCursor;
import org.springframework.data.redis.core.ScanIteration;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * @author Christoph Strobl
 * @author Clement Ong
 * @author Mark Paluch
 * @author Andrey Shlykov
 * @author Shyngys Sapraliyev
 * @since 2.0
 */
class JedisZSetCommands implements RedisZSetCommands {

	private final JedisConnection connection;

	JedisZSetCommands(JedisConnection connection) {
		this.connection = connection;
	}

	@Override
	public Boolean zAdd(byte[] key, double score, byte[] value, ZAddArgs args) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(value, "Value must not be null");

		return connection.invoke()
				.from(Jedis::zadd, PipelineBinaryCommands::zadd, key, score, value, JedisConverters.toZAddParams(args))
				.get(JedisConverters::toBoolean);
	}

	@Override
	public Long zAdd(byte[] key, Set<Tuple> tuples, ZAddArgs args) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(tuples, "Tuples must not be null");

		return connection.invoke().just(Jedis::zadd, PipelineBinaryCommands::zadd, key, JedisConverters.toTupleMap(tuples),
				JedisConverters.toZAddParams(args));
	}

	@Override
	public Long zRem(byte[] key, byte[]... values) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(values, "Values must not be null");
		Assert.noNullElements(values, "Values must not contain null elements");

		return connection.invoke().just(Jedis::zrem, PipelineBinaryCommands::zrem, key, values);
	}

	@Override
	public Double zIncrBy(byte[] key, double increment, byte[] value) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(value, "Value must not be null");

		return connection.invoke().just(Jedis::zincrby, PipelineBinaryCommands::zincrby, key, increment, value);
	}

	@Override
	public byte[] zRandMember(byte[] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().just(Jedis::zrandmember, PipelineBinaryCommands::zrandmember, key);
	}

	@Override
	public List<byte[]> zRandMember(byte[] key, long count) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().fromMany(Jedis::zrandmember, PipelineBinaryCommands::zrandmember, key, count).toList();
	}

	@Override
	public Tuple zRandMemberWithScore(byte[] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke()
				.from(Jedis::zrandmemberWithScores, PipelineBinaryCommands::zrandmemberWithScores, key, 1L).get(it -> {

					if (it.isEmpty()) {
						return null;
					}

					return JedisConverters.toTuple(it.iterator().next());
				});
	}

	@Override
	public List<Tuple> zRandMemberWithScore(byte[] key, long count) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke()
				.fromMany(Jedis::zrandmemberWithScores, PipelineBinaryCommands::zrandmemberWithScores, key, count)
				.toList(JedisConverters::toTuple);
	}

	@Override
	public Long zRank(byte[] key, byte[] value) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(value, "Value must not be null");

		return connection.invoke().just(Jedis::zrank, PipelineBinaryCommands::zrank, key, value);
	}

	@Override
	public Long zRevRank(byte[] key, byte[] value) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().just(Jedis::zrevrank, PipelineBinaryCommands::zrevrank, key, value);
	}

	@Override
	public Set<byte[]> zRange(byte[] key, long start, long end) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().fromMany(Jedis::zrange, PipelineBinaryCommands::zrange, key, start, end).toSet();
	}

	@Override
	public Set<Tuple> zRangeWithScores(byte[] key, long start, long end) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke()
				.fromMany(Jedis::zrangeWithScores, PipelineBinaryCommands::zrangeWithScores, key, start, end)
				.toSet(JedisConverters::toTuple);
	}

	@Override
	public Set<Tuple> zRangeByScoreWithScores(byte[] key, org.springframework.data.domain.Range<Number> range,
			org.springframework.data.redis.connection.Limit limit) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(range, "Range for ZRANGEBYSCOREWITHSCORES must not be null");
		Assert.notNull(limit, "Limit must not be null Use Limit.unlimited() instead");

		byte[] min = JedisConverters.boundaryToBytesForZRange(range.getLowerBound(),
				JedisConverters.NEGATIVE_INFINITY_BYTES);
		byte[] max = JedisConverters.boundaryToBytesForZRange(range.getUpperBound(),
				JedisConverters.POSITIVE_INFINITY_BYTES);

		if (!limit.isUnlimited()) {
			return connection.invoke().fromMany(Jedis::zrangeByScoreWithScores,
					PipelineBinaryCommands::zrangeByScoreWithScores, key, min, max, limit.getOffset(), limit.getCount())
					.toSet(JedisConverters::toTuple);
		}

		return connection.invoke()
				.fromMany(Jedis::zrangeByScoreWithScores, PipelineBinaryCommands::zrangeByScoreWithScores, key, min, max)
				.toSet(JedisConverters::toTuple);
	}

	@Override
	public Set<byte[]> zRevRange(byte[] key, long start, long end) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().fromMany(Jedis::zrevrange, PipelineBinaryCommands::zrevrange, key, start, end).toSet();
	}

	@Override
	public Set<Tuple> zRevRangeWithScores(byte[] key, long start, long end) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke()
				.fromMany(Jedis::zrevrangeWithScores, PipelineBinaryCommands::zrevrangeWithScores, key, start, end)
				.toSet(JedisConverters::toTuple);
	}

	@Override
	public Set<byte[]> zRevRangeByScore(byte[] key, org.springframework.data.domain.Range<Number> range,
			org.springframework.data.redis.connection.Limit limit) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(range, "Range for ZREVRANGEBYSCORE must not be null");
		Assert.notNull(limit, "Limit must not be null Use Limit.unlimited() instead");

		byte[] min = JedisConverters.boundaryToBytesForZRange(range.getLowerBound(),
				JedisConverters.NEGATIVE_INFINITY_BYTES);
		byte[] max = JedisConverters.boundaryToBytesForZRange(range.getUpperBound(),
				JedisConverters.POSITIVE_INFINITY_BYTES);

		if (!limit.isUnlimited()) {
			return connection.invoke().fromMany(Jedis::zrevrangeByScore, PipelineBinaryCommands::zrevrangeByScore, key, max,
					min, limit.getOffset(), limit.getCount()).toSet();
		}

		return connection.invoke()
				.fromMany(Jedis::zrevrangeByScore, PipelineBinaryCommands::zrevrangeByScore, key, max, min).toSet();
	}

	@Override
	public Set<Tuple> zRevRangeByScoreWithScores(byte[] key, org.springframework.data.domain.Range<Number> range,
			org.springframework.data.redis.connection.Limit limit) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(range, "Range for ZREVRANGEBYSCOREWITHSCORES must not be null");
		Assert.notNull(limit, "Limit must not be null Use Limit.unlimited() instead");

		byte[] min = JedisConverters.boundaryToBytesForZRange(range.getLowerBound(),
				JedisConverters.NEGATIVE_INFINITY_BYTES);
		byte[] max = JedisConverters.boundaryToBytesForZRange(range.getUpperBound(),
				JedisConverters.POSITIVE_INFINITY_BYTES);

		if (!limit.isUnlimited()) {
			return connection.invoke().fromMany(Jedis::zrevrangeByScoreWithScores,
					PipelineBinaryCommands::zrevrangeByScoreWithScores, key, max, min, limit.getOffset(), limit.getCount())
					.toSet(JedisConverters::toTuple);
		}

		return connection.invoke()
				.fromMany(Jedis::zrevrangeByScoreWithScores, PipelineBinaryCommands::zrevrangeByScoreWithScores, key, max, min)
				.toSet(JedisConverters::toTuple);
	}

	@Override
	public Long zCount(byte[] key, double min, double max) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().just(Jedis::zcount, PipelineBinaryCommands::zcount, key, min, max);
	}

	@Override
	public Long zCount(byte[] key, org.springframework.data.domain.Range<Number> range) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(range, "Range must not be null");

		byte[] min = JedisConverters.boundaryToBytesForZRange(range.getLowerBound(),
				JedisConverters.NEGATIVE_INFINITY_BYTES);
		byte[] max = JedisConverters.boundaryToBytesForZRange(range.getUpperBound(),
				JedisConverters.POSITIVE_INFINITY_BYTES);

		return connection.invoke().just(Jedis::zcount, PipelineBinaryCommands::zcount, key, min, max);
	}

	@Override
	public Long zLexCount(byte[] key, org.springframework.data.domain.Range<byte[]> range) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(range, "Range must not be null");

		byte[] min = JedisConverters.boundaryToBytesForZRangeByLex(range.getLowerBound(), JedisConverters.MINUS_BYTES);
		byte[] max = JedisConverters.boundaryToBytesForZRangeByLex(range.getUpperBound(), JedisConverters.PLUS_BYTES);

		return connection.invoke().just(Jedis::zlexcount, PipelineBinaryCommands::zlexcount, key, min, max);
	}

	@Nullable
	@Override
	public Tuple zPopMin(byte[] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().from(Jedis::zpopmin, PipelineBinaryCommands::zpopmin, key).get(JedisConverters::toTuple);
	}

	@Nullable
	@Override
	public Set<Tuple> zPopMin(byte[] key, long count) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().fromMany(Jedis::zpopmin, PipelineBinaryCommands::zpopmin, key, Math.toIntExact(count))
				.toSet(JedisConverters::toTuple);
	}

	@Nullable
	@Override
	public Tuple bZPopMin(byte[] key, long timeout, TimeUnit unit) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(unit, "TimeUnit must not be null");

		return connection.invoke()
				.from(Jedis::bzpopmin, PipelineBinaryCommands::bzpopmin, JedisConverters.toSeconds(timeout, unit), key)
				.get(JedisZSetCommands::toTuple);
	}

	@Nullable
	@Override
	public Tuple zPopMax(byte[] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().from(Jedis::zpopmax, PipelineBinaryCommands::zpopmax, key).get(JedisConverters::toTuple);
	}

	@Nullable
	@Override
	public Set<Tuple> zPopMax(byte[] key, long count) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().fromMany(Jedis::zpopmax, PipelineBinaryCommands::zpopmax, key, Math.toIntExact(count))
				.toSet(JedisConverters::toTuple);
	}

	@Nullable
	@Override
	public Tuple bZPopMax(byte[] key, long timeout, TimeUnit unit) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(unit, "TimeUnit must not be null");

		return connection.invoke()
				.from(Jedis::bzpopmax, PipelineBinaryCommands::bzpopmax, JedisConverters.toSeconds(timeout, unit), key)
				.get(JedisZSetCommands::toTuple);
	}

	@Override
	public Long zCard(byte[] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().just(Jedis::zcard, PipelineBinaryCommands::zcard, key);
	}

	@Override
	public Double zScore(byte[] key, byte[] value) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(value, "Value must not be null");

		return connection.invoke().just(Jedis::zscore, PipelineBinaryCommands::zscore, key, value);
	}

	@Override
	public List<Double> zMScore(byte[] key, byte[][] values) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(values, "Value must not be null");

		return connection.invoke().just(Jedis::zmscore, PipelineBinaryCommands::zmscore, key, values);
	}

	@Override
	public Long zRemRange(byte[] key, long start, long end) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().just(Jedis::zremrangeByRank, PipelineBinaryCommands::zremrangeByRank, key, start, end);
	}

	@Override
	public Long zRemRangeByLex(byte[] key, org.springframework.data.domain.Range<byte[]> range) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(range, "Range must not be null for ZREMRANGEBYLEX");

		byte[] min = JedisConverters.boundaryToBytesForZRangeByLex(range.getLowerBound(), JedisConverters.MINUS_BYTES);
		byte[] max = JedisConverters.boundaryToBytesForZRangeByLex(range.getUpperBound(), JedisConverters.PLUS_BYTES);

		return connection.invoke().just(Jedis::zremrangeByLex, PipelineBinaryCommands::zremrangeByLex, key, min, max);
	}

	@Override
	public Long zRemRangeByScore(byte[] key, org.springframework.data.domain.Range<Number> range) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(range, "Range for ZREMRANGEBYSCORE must not be null");

		byte[] min = JedisConverters.boundaryToBytesForZRange(range.getLowerBound(),
				JedisConverters.NEGATIVE_INFINITY_BYTES);
		byte[] max = JedisConverters.boundaryToBytesForZRange(range.getUpperBound(),
				JedisConverters.POSITIVE_INFINITY_BYTES);

		return connection.invoke().just(Jedis::zremrangeByScore, PipelineBinaryCommands::zremrangeByScore, key, min, max);
	}

	@Override
	public Set<byte[]> zDiff(byte[]... sets) {

		Assert.notNull(sets, "Sets must not be null");

		return connection.invoke().just(Jedis::zdiff, PipelineBinaryCommands::zdiff, sets);
	}

	@Override
	public Set<Tuple> zDiffWithScores(byte[]... sets) {

		Assert.notNull(sets, "Sets must not be null");

		return connection.invoke().fromMany(Jedis::zdiffWithScores, PipelineBinaryCommands::zdiffWithScores, sets)
				.toSet(JedisConverters::toTuple);
	}

	@Override
	public Long zDiffStore(byte[] destKey, byte[]... sets) {

		Assert.notNull(destKey, "Destination key must not be null");
		Assert.notNull(sets, "Source sets must not be null");

		return connection.invoke().just(Jedis::zdiffStore, PipelineBinaryCommands::zdiffStore, destKey, sets);
	}

	@Override
	public Set<byte[]> zInter(byte[]... sets) {

		Assert.notNull(sets, "Sets must not be null");

		return connection.invoke().just(Jedis::zinter, PipelineBinaryCommands::zinter, new ZParams(), sets);
	}

	@Override
	public Set<Tuple> zInterWithScores(byte[]... sets) {

		Assert.notNull(sets, "Sets must not be null");

		return connection.invoke()
				.fromMany(Jedis::zinterWithScores, PipelineBinaryCommands::zinterWithScores, new ZParams(), sets)
				.toSet(JedisConverters::toTuple);
	}

	@Override
	public Set<Tuple> zInterWithScores(Aggregate aggregate, Weights weights, byte[]... sets) {

		Assert.notNull(sets, "Sets must not be null");
		Assert.noNullElements(sets, "Source sets must not contain null elements");
		Assert.isTrue(weights.size() == sets.length, () -> String
				.format("The number of weights (%d) must match the number of source sets (%d)", weights.size(), sets.length));

		return connection.invoke().fromMany(Jedis::zinterWithScores, PipelineBinaryCommands::zinterWithScores,
				toZParams(aggregate, weights), sets).toSet(JedisConverters::toTuple);
	}

	@Override
	public Long zInterStore(byte[] destKey, Aggregate aggregate, Weights weights, byte[]... sets) {

		Assert.notNull(destKey, "Destination key must not be null");
		Assert.notNull(sets, "Source sets must not be null");
		Assert.noNullElements(sets, "Source sets must not contain null elements");
		Assert.isTrue(weights.size() == sets.length, () -> String
				.format("The number of weights (%d) must match the number of source sets (%d)", weights.size(), sets.length));

		ZParams zparams = toZParams(aggregate, weights);

		return connection.invoke().just(Jedis::zinterstore, PipelineBinaryCommands::zinterstore, destKey, zparams, sets);
	}

	@Override
	public Long zInterStore(byte[] destKey, byte[]... sets) {

		Assert.notNull(destKey, "Destination key must not be null");
		Assert.notNull(sets, "Source sets must not be null");
		Assert.noNullElements(sets, "Source sets must not contain null elements");

		return connection.invoke().just(Jedis::zinterstore, PipelineBinaryCommands::zinterstore, destKey, sets);
	}

	@Override
	public Set<byte[]> zUnion(byte[]... sets) {

		Assert.notNull(sets, "Sets must not be null");

		return connection.invoke().just(Jedis::zunion, PipelineBinaryCommands::zunion, new ZParams(), sets);
	}

	@Override
	public Set<Tuple> zUnionWithScores(byte[]... sets) {

		Assert.notNull(sets, "Sets must not be null");

		return connection.invoke()
				.fromMany(Jedis::zunionWithScores, PipelineBinaryCommands::zunionWithScores, new ZParams(), sets)
				.toSet(JedisConverters::toTuple);
	}

	@Override
	public Set<Tuple> zUnionWithScores(Aggregate aggregate, Weights weights, byte[]... sets) {

		Assert.notNull(sets, "Sets must not be null");
		Assert.noNullElements(sets, "Source sets must not contain null elements");
		Assert.isTrue(weights.size() == sets.length, () -> String
				.format("The number of weights (%d) must match the number of source sets (%d)", weights.size(), sets.length));

		return connection.invoke().fromMany(Jedis::zunionWithScores, PipelineBinaryCommands::zunionWithScores,
				toZParams(aggregate, weights), sets).toSet(JedisConverters::toTuple);
	}

	@Override
	public Long zUnionStore(byte[] destKey, Aggregate aggregate, Weights weights, byte[]... sets) {

		Assert.notNull(destKey, "Destination key must not be null");
		Assert.notNull(sets, "Source sets must not be null");
		Assert.notNull(weights, "Weights must not be null");
		Assert.noNullElements(sets, "Source sets must not contain null elements");
		Assert.isTrue(weights.size() == sets.length, () -> String
				.format("The number of weights (%d) must match the number of source sets (%d)", weights.size(), sets.length));

		ZParams zparams = toZParams(aggregate, weights);

		return connection.invoke().just(Jedis::zunionstore, PipelineBinaryCommands::zunionstore, destKey, zparams, sets);
	}

	@Override
	public Long zUnionStore(byte[] destKey, byte[]... sets) {

		Assert.notNull(destKey, "Destination key must not be null");
		Assert.notNull(sets, "Source sets must not be null");
		Assert.noNullElements(sets, "Source sets must not contain null elements");

		return connection.invoke().just(Jedis::zunionstore, PipelineBinaryCommands::zunionstore, destKey, sets);
	}

	@Override
	public Cursor<Tuple> zScan(byte[] key, ScanOptions options) {
		return zScan(key, 0L, options);
	}

	/**
	 * @since 1.4
	 * @param key
	 * @param cursorId
	 * @param options
	 * @return
	 */
	public Cursor<Tuple> zScan(byte[] key, Long cursorId, ScanOptions options) {

		Assert.notNull(key, "Key must not be null");

		return new KeyBoundCursor<Tuple>(key, cursorId, options) {

			@Override
			protected ScanIteration<Tuple> doScan(byte[] key, long cursorId, ScanOptions options) {

				if (isQueueing() || isPipelined()) {
					throw new InvalidDataAccessApiUsageException("'ZSCAN' cannot be called in pipeline / transaction mode");
				}

				ScanParams params = JedisConverters.toScanParams(options);

				ScanResult<redis.clients.jedis.resps.Tuple> result = connection.getJedis().zscan(key,
						JedisConverters.toBytes(cursorId), params);
				return new ScanIteration<>(Long.valueOf(result.getCursor()),
						JedisConverters.tuplesToTuples().convert(result.getResult()));
			}

			@Override
			protected void doClose() {
				JedisZSetCommands.this.connection.close();
			};

		}.open();
	}

	@Override
	public Set<byte[]> zRangeByScore(byte[] key, String min, String max) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().fromMany(Jedis::zrangeByScore, PipelineBinaryCommands::zrangeByScore, key,
				JedisConverters.toBytes(min), JedisConverters.toBytes(max)).toSet();
	}

	@Override
	public Set<byte[]> zRangeByScore(byte[] key, String min, String max, long offset, long count) {

		Assert.notNull(key, "Key must not be null");

		if (offset > Integer.MAX_VALUE || count > Integer.MAX_VALUE) {

			throw new IllegalArgumentException(
					"Offset and count must be less than Integer.MAX_VALUE for zRangeByScore in Jedis");
		}

		return connection.invoke().fromMany(Jedis::zrangeByScore, PipelineBinaryCommands::zrangeByScore, key,
				JedisConverters.toBytes(min), JedisConverters.toBytes(max), (int) offset, (int) count).toSet();
	}

	@Override
	public Set<byte[]> zRangeByScore(byte[] key, org.springframework.data.domain.Range<Number> range,
			org.springframework.data.redis.connection.Limit limit) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(range, "Range for ZRANGEBYSCORE must not be null");
		Assert.notNull(limit, "Limit must not be null Use Limit.unlimited() instead");

		byte[] min = JedisConverters.boundaryToBytesForZRange(range.getLowerBound(),
				JedisConverters.NEGATIVE_INFINITY_BYTES);
		byte[] max = JedisConverters.boundaryToBytesForZRange(range.getUpperBound(),
				JedisConverters.POSITIVE_INFINITY_BYTES);

		if (!limit.isUnlimited()) {
			return connection.invoke().fromMany(Jedis::zrangeByScore, PipelineBinaryCommands::zrangeByScore, key, min, max,
					limit.getOffset(), limit.getCount()).toSet();
		}

		return connection.invoke().fromMany(Jedis::zrangeByScore, PipelineBinaryCommands::zrangeByScore, key, min, max)
				.toSet();
	}

	@Override
	public Set<byte[]> zRangeByLex(byte[] key, org.springframework.data.domain.Range<byte[]> range,
			org.springframework.data.redis.connection.Limit limit) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(range, "Range for ZRANGEBYLEX must not be null");
		Assert.notNull(limit, "Limit must not be null Use Limit.unlimited() instead");

		byte[] min = JedisConverters.boundaryToBytesForZRangeByLex(range.getLowerBound(), JedisConverters.MINUS_BYTES);
		byte[] max = JedisConverters.boundaryToBytesForZRangeByLex(range.getUpperBound(), JedisConverters.PLUS_BYTES);

		if (!limit.isUnlimited()) {
			return connection.invoke().fromMany(Jedis::zrangeByLex, PipelineBinaryCommands::zrangeByLex, key, min, max,
					limit.getOffset(), limit.getCount()).toSet();
		}

		return connection.invoke().fromMany(Jedis::zrangeByLex, PipelineBinaryCommands::zrangeByLex, key, min, max).toSet();
	}

	@Override
	public Set<byte[]> zRevRangeByLex(byte[] key, org.springframework.data.domain.Range<byte[]> range,
			org.springframework.data.redis.connection.Limit limit) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(range, "Range for ZREVRANGEBYLEX must not be null");
		Assert.notNull(limit, "Limit must not be null Use Limit.unlimited() instead.");

		byte[] min = JedisConverters.boundaryToBytesForZRangeByLex(range.getLowerBound(), JedisConverters.MINUS_BYTES);
		byte[] max = JedisConverters.boundaryToBytesForZRangeByLex(range.getUpperBound(), JedisConverters.PLUS_BYTES);

		if (!limit.isUnlimited()) {
			return connection.invoke().from(Jedis::zrevrangeByLex, PipelineBinaryCommands::zrevrangeByLex, key, max, min,
					limit.getOffset(), limit.getCount()).get(LinkedHashSet::new);
		}

		return connection.invoke().from(Jedis::zrevrangeByLex, PipelineBinaryCommands::zrevrangeByLex, key, max, min)
				.get(LinkedHashSet::new);
	}

	@Override
	public Long zRangeStoreByLex(byte[] dstKey, byte[] srcKey, org.springframework.data.domain.Range<byte[]> range,
			org.springframework.data.redis.connection.Limit limit) {
		return zRangeStoreByLex(dstKey, srcKey, range, limit, false);
	}

	@Override
	public Long zRangeStoreRevByLex(byte[] dstKey, byte[] srcKey, org.springframework.data.domain.Range<byte[]> range,
			org.springframework.data.redis.connection.Limit limit) {
		return zRangeStoreByLex(dstKey, srcKey, range, limit, true);
	}

	private Long zRangeStoreByLex(byte[] dstKey, byte[] srcKey, org.springframework.data.domain.Range<byte[]> range,
			org.springframework.data.redis.connection.Limit limit, boolean rev) {

		Assert.notNull(dstKey, "Destination key must not be null");
		Assert.notNull(srcKey, "Source key must not be null");
		Assert.notNull(range, "Range must not be null");
		Assert.notNull(limit, "Limit must not be null. Use Limit.unlimited() instead.");

		byte[] min = JedisConverters.boundaryToBytesForZRangeByLex(range.getLowerBound(), JedisConverters.MINUS_BYTES);
		byte[] max = JedisConverters.boundaryToBytesForZRangeByLex(range.getUpperBound(), JedisConverters.PLUS_BYTES);

		ZRangeParams zRangeParams = toZRangeParams(Protocol.Keyword.BYLEX, min, max, limit, rev);

		return connection.invoke().just(Jedis::zrangestore, PipelineBinaryCommands::zrangestore, dstKey, srcKey,
				zRangeParams);
	}

	@Override
	public Long zRangeStoreByScore(byte[] dstKey, byte[] srcKey, org.springframework.data.domain.Range<Number> range,
			org.springframework.data.redis.connection.Limit limit) {
		return zRangeStoreByScore(dstKey, srcKey, range, limit, false);
	}

	@Override
	public Long zRangeStoreRevByScore(byte[] dstKey, byte[] srcKey, org.springframework.data.domain.Range<Number> range,
			org.springframework.data.redis.connection.Limit limit) {
		return zRangeStoreByScore(dstKey, srcKey, range, limit, true);
	}

	private Long zRangeStoreByScore(byte[] dstKey, byte[] srcKey, org.springframework.data.domain.Range<Number> range,
			org.springframework.data.redis.connection.Limit limit, boolean rev) {

		Assert.notNull(dstKey, "Destination key must not be null");
		Assert.notNull(srcKey, "Source key must not be null");
		Assert.notNull(range, "Range must not be null");
		Assert.notNull(limit, "Limit must not be null. Use Limit.unlimited() instead.");

		byte[] min = JedisConverters.boundaryToBytesForZRange(range.getLowerBound(),
				JedisConverters.NEGATIVE_INFINITY_BYTES);
		byte[] max = JedisConverters.boundaryToBytesForZRange(range.getUpperBound(),
				JedisConverters.POSITIVE_INFINITY_BYTES);

		ZRangeParams zRangeParams = toZRangeParams(Protocol.Keyword.BYSCORE, min, max, limit, rev);

		return connection.invoke().just(Jedis::zrangestore, PipelineBinaryCommands::zrangestore, dstKey, srcKey,
				zRangeParams);
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

		ZRangeParams zRangeParams;

		if (rev) {
			zRangeParams = new ZRangeParams(by, max, min).rev();
		} else {
			zRangeParams = new ZRangeParams(by, min, max);
		}

		if (limit.isLimited()) {
			zRangeParams = zRangeParams.limit(limit.getOffset(), limit.getCount());
		}
		return zRangeParams;
	}

	/**
	 * Workaround for broken Jedis BZPOP signature.
	 *
	 * @param bytes
	 * @return
	 */
	@Nullable
	@SuppressWarnings("unchecked")
	private static Tuple toTuple(List<?> bytes) {

		if (bytes.isEmpty()) {
			return null;
		}

		return new DefaultTuple((byte[]) bytes.get(1), Double.parseDouble(new String((byte[]) bytes.get(2))));
	}

}
