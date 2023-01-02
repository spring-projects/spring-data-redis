/*
 * Copyright 2017-2023 the original author or authors.
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

import redis.clients.jedis.BinaryJedis;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.MultiKeyPipelineBase;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.ZParams;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.springframework.data.redis.connection.DefaultTuple;
import org.springframework.data.redis.connection.RedisZSetCommands;
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
 * @since 2.0
 */
class JedisZSetCommands implements RedisZSetCommands {

	private final JedisConnection connection;

	JedisZSetCommands(JedisConnection connection) {
		this.connection = connection;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zAdd(byte[], double, byte[])
	 */
	@Override
	public Boolean zAdd(byte[] key, double score, byte[] value, ZAddArgs args) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(value, "Value must not be null!");

		return connection.invoke()
				.from(BinaryJedis::zadd, MultiKeyPipelineBase::zadd, key, score, value, JedisConverters.toZAddParams(args))
				.get(JedisConverters::toBoolean);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zAdd(byte[], java.util.Set, org.springframework.data.redis.connection.RedisZSetCommands.ZAddArgs)
	 */
	@Override
	public Long zAdd(byte[] key, Set<Tuple> tuples, ZAddArgs args) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(tuples, "Tuples must not be null!");

		return connection.invoke().just(BinaryJedis::zadd, MultiKeyPipelineBase::zadd, key,
				JedisConverters.toTupleMap(tuples), JedisConverters.toZAddParams(args));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRem(byte[], byte[][])
	 */
	@Override
	public Long zRem(byte[] key, byte[]... values) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(values, "Values must not be null!");
		Assert.noNullElements(values, "Values must not contain null elements!");

		return connection.invoke().just(BinaryJedis::zrem, MultiKeyPipelineBase::zrem, key, values);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zIncrBy(byte[], double, byte[])
	 */
	@Override
	public Double zIncrBy(byte[] key, double increment, byte[] value) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(value, "Value must not be null!");

		return connection.invoke().just(BinaryJedis::zincrby, MultiKeyPipelineBase::zincrby, key, increment, value);
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRandMember(byte[])
	 */
	@Override
	public byte[] zRandMember(byte[] key) {

		Assert.notNull(key, "Key must not be null!");

		return connection.invoke().just(BinaryJedis::zrandmember, MultiKeyPipelineBase::zrandmember, key);
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRandMember(byte[], long)
	 */
	@Override
	public List<byte[]> zRandMember(byte[] key, long count) {

		Assert.notNull(key, "Key must not be null!");

		return connection.invoke().fromMany(BinaryJedis::zrandmember, MultiKeyPipelineBase::zrandmember, key, count)
				.toList();
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRandMemberWithScore(byte[])
	 */
	@Override
	public Tuple zRandMemberWithScore(byte[] key) {

		Assert.notNull(key, "Key must not be null!");

		return connection.invoke()
				.from(BinaryJedis::zrandmemberWithScores, MultiKeyPipelineBase::zrandmemberWithScores, key, 1L).get(it -> {

					if (it.isEmpty()) {
						return null;
					}

					return JedisConverters.toTuple(it.iterator().next());
				});
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRandMemberWithScore(byte[], long)
	 */
	@Override
	public List<Tuple> zRandMemberWithScore(byte[] key, long count) {

		Assert.notNull(key, "Key must not be null!");

		return connection.invoke()
				.fromMany(BinaryJedis::zrandmemberWithScores, MultiKeyPipelineBase::zrandmemberWithScores, key, count)
				.toList(JedisConverters::toTuple);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRank(byte[], byte[])
	 */
	@Override
	public Long zRank(byte[] key, byte[] value) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(value, "Value must not be null!");

		return connection.invoke().just(BinaryJedis::zrank, MultiKeyPipelineBase::zrank, key, value);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRevRank(byte[], byte[])
	 */
	@Override
	public Long zRevRank(byte[] key, byte[] value) {

		Assert.notNull(key, "Key must not be null!");

		return connection.invoke().just(BinaryJedis::zrevrank, MultiKeyPipelineBase::zrevrank, key, value);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRange(byte[], long, long)
	 */
	@Override
	public Set<byte[]> zRange(byte[] key, long start, long end) {

		Assert.notNull(key, "Key must not be null!");

		return connection.invoke().just(BinaryJedis::zrange, MultiKeyPipelineBase::zrange, key, start, end);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRangeWithScores(byte[], long, long)
	 */
	@Override
	public Set<Tuple> zRangeWithScores(byte[] key, long start, long end) {

		Assert.notNull(key, "Key must not be null!");

		return connection.invoke()
				.fromMany(BinaryJedis::zrangeWithScores, MultiKeyPipelineBase::zrangeWithScores, key, start, end)
				.toSet(JedisConverters::toTuple);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRangeByScoreWithScores(byte[], org.springframework.data.redis.connection.RedisZSetCommands.Range, org.springframework.data.redis.connection.RedisZSetCommands.Limit)
	 */
	@Override
	public Set<Tuple> zRangeByScoreWithScores(byte[] key, Range range, Limit limit) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(range, "Range for ZRANGEBYSCOREWITHSCORES must not be null!");
		Assert.notNull(limit, "Limit must not be null! Use Limit.unlimited() instead.");

		byte[] min = JedisConverters.boundaryToBytesForZRange(range.getMin(), JedisConverters.NEGATIVE_INFINITY_BYTES);
		byte[] max = JedisConverters.boundaryToBytesForZRange(range.getMax(), JedisConverters.POSITIVE_INFINITY_BYTES);

		if (!limit.isUnlimited()) {
			return connection.invoke().fromMany(BinaryJedis::zrangeByScoreWithScores,
					MultiKeyPipelineBase::zrangeByScoreWithScores, key, min, max, limit.getOffset(), limit.getCount())
					.toSet(JedisConverters::toTuple);
		}

		return connection.invoke()
				.fromMany(BinaryJedis::zrangeByScoreWithScores, MultiKeyPipelineBase::zrangeByScoreWithScores, key, min, max)
				.toSet(JedisConverters::toTuple);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRevRange(byte[], long, long)
	 */
	@Override
	public Set<byte[]> zRevRange(byte[] key, long start, long end) {

		Assert.notNull(key, "Key must not be null!");

		return connection.invoke().just(BinaryJedis::zrevrange, MultiKeyPipelineBase::zrevrange, key, start, end);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRevRangeWithScores(byte[], long, long)
	 */
	@Override
	public Set<Tuple> zRevRangeWithScores(byte[] key, long start, long end) {

		Assert.notNull(key, "Key must not be null!");

		return connection.invoke()
				.fromMany(BinaryJedis::zrevrangeWithScores, MultiKeyPipelineBase::zrevrangeWithScores, key, start, end)
				.toSet(JedisConverters::toTuple);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRevRangeByScore(byte[], org.springframework.data.redis.connection.RedisZSetCommands.Range, org.springframework.data.redis.connection.RedisZSetCommands.Limit)
	 */
	@Override
	public Set<byte[]> zRevRangeByScore(byte[] key, Range range, Limit limit) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(range, "Range for ZREVRANGEBYSCORE must not be null!");
		Assert.notNull(limit, "Limit must not be null! Use Limit.unlimited() instead.");

		byte[] min = JedisConverters.boundaryToBytesForZRange(range.getMin(), JedisConverters.NEGATIVE_INFINITY_BYTES);
		byte[] max = JedisConverters.boundaryToBytesForZRange(range.getMax(), JedisConverters.POSITIVE_INFINITY_BYTES);

		if (!limit.isUnlimited()) {
			return connection.invoke().just(BinaryJedis::zrevrangeByScore, MultiKeyPipelineBase::zrevrangeByScore, key, max,
					min, limit.getOffset(), limit.getCount());
		}

		return connection.invoke().just(BinaryJedis::zrevrangeByScore, MultiKeyPipelineBase::zrevrangeByScore, key, max,
				min);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRevRangeByScoreWithScores(byte[], org.springframework.data.redis.connection.RedisZSetCommands.Range, org.springframework.data.redis.connection.RedisZSetCommands.Limit)
	 */
	@Override
	public Set<Tuple> zRevRangeByScoreWithScores(byte[] key, Range range, Limit limit) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(range, "Range for ZREVRANGEBYSCOREWITHSCORES must not be null!");
		Assert.notNull(limit, "Limit must not be null! Use Limit.unlimited() instead.");

		byte[] min = JedisConverters.boundaryToBytesForZRange(range.getMin(), JedisConverters.NEGATIVE_INFINITY_BYTES);
		byte[] max = JedisConverters.boundaryToBytesForZRange(range.getMax(), JedisConverters.POSITIVE_INFINITY_BYTES);

		if (!limit.isUnlimited()) {
			return connection.invoke().fromMany(BinaryJedis::zrevrangeByScoreWithScores,
					MultiKeyPipelineBase::zrevrangeByScoreWithScores, key, max, min, limit.getOffset(), limit.getCount())
					.toSet(JedisConverters::toTuple);
		}

		return connection.invoke().fromMany(BinaryJedis::zrevrangeByScoreWithScores,
				MultiKeyPipelineBase::zrevrangeByScoreWithScores, key, max, min).toSet(JedisConverters::toTuple);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zCount(byte[], double, double)
	 */
	@Override
	public Long zCount(byte[] key, double min, double max) {

		Assert.notNull(key, "Key must not be null!");

		return connection.invoke().just(BinaryJedis::zcount, MultiKeyPipelineBase::zcount, key, min, max);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zCount(byte[], org.springframework.data.redis.connection.RedisZSetCommands.Range)
	 */
	@Override
	public Long zCount(byte[] key, Range range) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(range, "Range must not be null!");

		byte[] min = JedisConverters.boundaryToBytesForZRange(range.getMin(), JedisConverters.NEGATIVE_INFINITY_BYTES);
		byte[] max = JedisConverters.boundaryToBytesForZRange(range.getMax(), JedisConverters.POSITIVE_INFINITY_BYTES);

		return connection.invoke().just(BinaryJedis::zcount, MultiKeyPipelineBase::zcount, key, min, max);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zLexCount(byte[], org.springframework.data.redis.connection.RedisZSetCommands.Range)
	 */
	@Override
	public Long zLexCount(byte[] key, Range range) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(range, "Range must not be null!");

		byte[] min = JedisConverters.boundaryToBytesForZRangeByLex(range.getMin(), JedisConverters.MINUS_BYTES);
		byte[] max = JedisConverters.boundaryToBytesForZRangeByLex(range.getMax(), JedisConverters.PLUS_BYTES);

		return connection.invoke().just(BinaryJedis::zlexcount, MultiKeyPipelineBase::zlexcount, key, min, max);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zPopMin(byte[])
	 */
	@Nullable
	@Override
	public Tuple zPopMin(byte[] key) {

		Assert.notNull(key, "Key must not be null!");

		return connection.invoke().from(BinaryJedis::zpopmin, MultiKeyPipelineBase::zpopmin, key)
				.get(JedisConverters::toTuple);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zPopMin(byte[], long)
	 */
	@Nullable
	@Override
	public Set<Tuple> zPopMin(byte[] key, long count) {

		Assert.notNull(key, "Key must not be null!");

		return connection.invoke()
				.fromMany(BinaryJedis::zpopmin, MultiKeyPipelineBase::zpopmin, key, Math.toIntExact(count))
				.toSet(JedisConverters::toTuple);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#bZPopMin(byte[], long, java.util.concurrent.TimeUnit)
	 */
	@Nullable
	@Override
	public Tuple bZPopMin(byte[] key, long timeout, TimeUnit unit) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(unit, "TimeUnit must not be null!");

		return connection.invoke()
				.from(BinaryJedis::bzpopmin, MultiKeyPipelineBase::bzpopmin, JedisConverters.toSeconds(timeout, unit), key)
				.get(JedisZSetCommands::toTuple);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zPopMax(byte[])
	 */
	@Nullable
	@Override
	public Tuple zPopMax(byte[] key) {

		Assert.notNull(key, "Key must not be null!");

		return connection.invoke().from(BinaryJedis::zpopmax, MultiKeyPipelineBase::zpopmax, key)
				.get(JedisConverters::toTuple);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zPopMax(byte[], long)
	 */
	@Nullable
	@Override
	public Set<Tuple> zPopMax(byte[] key, long count) {

		Assert.notNull(key, "Key must not be null!");

		return connection.invoke()
				.fromMany(BinaryJedis::zpopmax, MultiKeyPipelineBase::zpopmax, key, Math.toIntExact(count))
				.toSet(JedisConverters::toTuple);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#bZPopMax(byte[], long, java.util.concurrent.TimeUnit)
	 */
	@Nullable
	@Override
	public Tuple bZPopMax(byte[] key, long timeout, TimeUnit unit) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(unit, "TimeUnit must not be null!");

		return connection.invoke()
				.from(BinaryJedis::bzpopmax, MultiKeyPipelineBase::bzpopmax, JedisConverters.toSeconds(timeout, unit), key)
				.get(JedisZSetCommands::toTuple);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zCard(byte[])
	 */
	@Override
	public Long zCard(byte[] key) {

		Assert.notNull(key, "Key must not be null!");

		return connection.invoke().just(BinaryJedis::zcard, MultiKeyPipelineBase::zcard, key);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zScore(byte[], byte[])
	 */
	@Override
	public Double zScore(byte[] key, byte[] value) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(value, "Value must not be null!");

		return connection.invoke().just(BinaryJedis::zscore, MultiKeyPipelineBase::zscore, key, value);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zScore(byte[], byte[][])
	 */
	@Override
	public List<Double> zMScore(byte[] key, byte[][] values) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(values, "Value must not be null!");

		return connection.invoke().just(BinaryJedis::zmscore, MultiKeyPipelineBase::zmscore, key, values);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRemRange(byte[], long, long)
	 */
	@Override
	public Long zRemRange(byte[] key, long start, long end) {

		Assert.notNull(key, "Key must not be null!");

		return connection.invoke().just(BinaryJedis::zremrangeByRank, MultiKeyPipelineBase::zremrangeByRank, key, start,
				end);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRemRangeByLex(byte[], org.springframework.data.redis.connection.RedisZSetCommands.Range)
	 */
	@Override
	public Long zRemRangeByLex(byte[] key, Range range) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(range, "Range must not be null for ZREMRANGEBYLEX!");

		byte[] min = JedisConverters.boundaryToBytesForZRangeByLex(range.getMin(), JedisConverters.MINUS_BYTES);
		byte[] max = JedisConverters.boundaryToBytesForZRangeByLex(range.getMax(), JedisConverters.PLUS_BYTES);

		return connection.invoke().just(BinaryJedis::zremrangeByLex, MultiKeyPipelineBase::zremrangeByLex, key, min, max);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRemRangeByScore(byte[], org.springframework.data.redis.connection.RedisZSetCommands.Range)
	 */
	@Override
	public Long zRemRangeByScore(byte[] key, Range range) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(range, "Range for ZREMRANGEBYSCORE must not be null!");

		byte[] min = JedisConverters.boundaryToBytesForZRange(range.getMin(), JedisConverters.NEGATIVE_INFINITY_BYTES);
		byte[] max = JedisConverters.boundaryToBytesForZRange(range.getMax(), JedisConverters.POSITIVE_INFINITY_BYTES);

		return connection.invoke().just(BinaryJedis::zremrangeByScore, MultiKeyPipelineBase::zremrangeByScore, key, min,
				max);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zDiff(byte[][])
	 */
	@Override
	public Set<byte[]> zDiff(byte[]... sets) {

		Assert.notNull(sets, "Sets must not be null!");

		return connection.invoke().just(BinaryJedis::zdiff, MultiKeyPipelineBase::zdiff, sets);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zDiffWithScores(byte[][])
	 */
	@Override
	public Set<Tuple> zDiffWithScores(byte[]... sets) {

		Assert.notNull(sets, "Sets must not be null!");

		return connection.invoke().fromMany(BinaryJedis::zdiffWithScores, MultiKeyPipelineBase::zdiffWithScores, sets)
				.toSet(JedisConverters::toTuple);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zDiffStore(byte[], byte[][])
	 */
	@Override
	public Long zDiffStore(byte[] destKey, byte[]... sets) {

		Assert.notNull(destKey, "Destination key must not be null!");
		Assert.notNull(sets, "Source sets must not be null!");

		return connection.invoke().just(BinaryJedis::zdiffStore, MultiKeyPipelineBase::zdiffStore, destKey, sets);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zInter(byte[][])
	 */
	@Override
	public Set<byte[]> zInter(byte[]... sets) {

		Assert.notNull(sets, "Sets must not be null!");

		return connection.invoke().just(BinaryJedis::zinter, MultiKeyPipelineBase::zinter, new ZParams(), sets);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zInterWithScores(byte[][])
	 */
	@Override
	public Set<Tuple> zInterWithScores(byte[]... sets) {

		Assert.notNull(sets, "Sets must not be null!");

		return connection.invoke()
				.fromMany(BinaryJedis::zinterWithScores, MultiKeyPipelineBase::zinterWithScores, new ZParams(), sets)
				.toSet(JedisConverters::toTuple);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zInterWithScores(org.springframework.data.redis.connection.RedisZSetCommands.Aggregate, org.springframework.data.redis.connection.RedisZSetCommands.Weights, byte[][])
	 */
	@Override
	public Set<Tuple> zInterWithScores(Aggregate aggregate, Weights weights, byte[]... sets) {

		Assert.notNull(sets, "Sets must not be null!");
		Assert.noNullElements(sets, "Source sets must not contain null elements!");
		Assert.isTrue(weights.size() == sets.length, () -> String
				.format("The number of weights (%d) must match the number of source sets (%d)!", weights.size(), sets.length));

		return connection.invoke().fromMany(BinaryJedis::zinterWithScores, MultiKeyPipelineBase::zinterWithScores,
				toZParams(aggregate, weights), sets).toSet(JedisConverters::toTuple);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zInterStore(byte[], org.springframework.data.redis.connection.RedisZSetCommands.Aggregate, org.springframework.data.redis.connection.RedisZSetCommands.Weights, byte[][])
	 */
	@Override
	public Long zInterStore(byte[] destKey, Aggregate aggregate, Weights weights, byte[]... sets) {

		Assert.notNull(destKey, "Destination key must not be null!");
		Assert.notNull(sets, "Source sets must not be null!");
		Assert.noNullElements(sets, "Source sets must not contain null elements!");
		Assert.isTrue(weights.size() == sets.length, () -> String
				.format("The number of weights (%d) must match the number of source sets (%d)!", weights.size(), sets.length));

		ZParams zparams = toZParams(aggregate, weights);

		return connection.invoke().just(BinaryJedis::zinterstore, MultiKeyPipelineBase::zinterstore, destKey, zparams,
				sets);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zInterStore(byte[], byte[][])
	 */
	@Override
	public Long zInterStore(byte[] destKey, byte[]... sets) {

		Assert.notNull(destKey, "Destination key must not be null!");
		Assert.notNull(sets, "Source sets must not be null!");
		Assert.noNullElements(sets, "Source sets must not contain null elements!");

		return connection.invoke().just(BinaryJedis::zinterstore, MultiKeyPipelineBase::zinterstore, destKey, sets);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zUnion(byte[][])
	 */
	@Override
	public Set<byte[]> zUnion(byte[]... sets) {

		Assert.notNull(sets, "Sets must not be null!");

		return connection.invoke().just(BinaryJedis::zunion, MultiKeyPipelineBase::zunion, new ZParams(), sets);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zUnionWithScores(byte[][])
	 */
	@Override
	public Set<Tuple> zUnionWithScores(byte[]... sets) {

		Assert.notNull(sets, "Sets must not be null!");

		return connection.invoke()
				.fromMany(BinaryJedis::zunionWithScores, MultiKeyPipelineBase::zunionWithScores, new ZParams(), sets)
				.toSet(JedisConverters::toTuple);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zUnionWithScores(org.springframework.data.redis.connection.RedisZSetCommands.Aggregate, org.springframework.data.redis.connection.RedisZSetCommands.Weights, byte[][])
	 */
	@Override
	public Set<Tuple> zUnionWithScores(Aggregate aggregate, Weights weights, byte[]... sets) {

		Assert.notNull(sets, "Sets must not be null!");
		Assert.noNullElements(sets, "Source sets must not contain null elements!");
		Assert.isTrue(weights.size() == sets.length, () -> String
				.format("The number of weights (%d) must match the number of source sets (%d)!", weights.size(), sets.length));

		return connection.invoke().fromMany(BinaryJedis::zunionWithScores, MultiKeyPipelineBase::zunionWithScores,
				toZParams(aggregate, weights), sets).toSet(JedisConverters::toTuple);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zUnionStore(byte[], org.springframework.data.redis.connection.RedisZSetCommands.Aggregate, org.springframework.data.redis.connection.RedisZSetCommands.Weights, byte[][])
	 */
	@Override
	public Long zUnionStore(byte[] destKey, Aggregate aggregate, Weights weights, byte[]... sets) {

		Assert.notNull(destKey, "Destination key must not be null!");
		Assert.notNull(sets, "Source sets must not be null!");
		Assert.notNull(weights, "Weights must not be null!");
		Assert.noNullElements(sets, "Source sets must not contain null elements!");
		Assert.isTrue(weights.size() == sets.length, () -> String
				.format("The number of weights (%d) must match the number of source sets (%d)!", weights.size(), sets.length));

		ZParams zparams = toZParams(aggregate, weights);

		return connection.invoke().just(BinaryJedis::zunionstore, MultiKeyPipelineBase::zunionstore, destKey, zparams,
				sets);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zUnionStore(byte[], byte[][])
	 */
	@Override
	public Long zUnionStore(byte[] destKey, byte[]... sets) {

		Assert.notNull(destKey, "Destination key must not be null!");
		Assert.notNull(sets, "Source sets must not be null!");
		Assert.noNullElements(sets, "Source sets must not contain null elements!");

		return connection.invoke().just(BinaryJedis::zunionstore, MultiKeyPipelineBase::zunionstore, destKey, sets);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zScan(byte[], org.springframework.data.redis.core.ScanOptions)
	 */
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

		Assert.notNull(key, "Key must not be null!");

		return new KeyBoundCursor<Tuple>(key, cursorId, options) {

			@Override
			protected ScanIteration<Tuple> doScan(byte[] key, long cursorId, ScanOptions options) {

				if (isQueueing() || isPipelined()) {
					throw new UnsupportedOperationException("'ZSCAN' cannot be called in pipeline / transaction mode.");
				}

				ScanParams params = JedisConverters.toScanParams(options);

				ScanResult<redis.clients.jedis.Tuple> result = connection.getJedis().zscan(key,
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

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRangeByScore(byte[], java.lang.String, java.lang.String)
	 */
	@Override
	public Set<byte[]> zRangeByScore(byte[] key, String min, String max) {

		Assert.notNull(key, "Key must not be null!");

		String keyStr = new String(key, StandardCharsets.UTF_8);
		return connection.invoke().fromMany(Jedis::zrangeByScore, MultiKeyPipelineBase::zrangeByScore, keyStr, min, max)
				.toSet(JedisConverters::toBytes);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRangeByScore(byte[], java.lang.String, java.lang.String, long, long)
	 */
	@Override
	public Set<byte[]> zRangeByScore(byte[] key, String min, String max, long offset, long count) {

		Assert.notNull(key, "Key must not be null!");

		if (offset > Integer.MAX_VALUE || count > Integer.MAX_VALUE) {

			throw new IllegalArgumentException(
					"Offset and count must be less than Integer.MAX_VALUE for zRangeByScore in Jedis.");
		}
		String keyStr = new String(key, StandardCharsets.UTF_8);

		return connection.invoke().fromMany(Jedis::zrangeByScore, MultiKeyPipelineBase::zrangeByScore, keyStr, min, max,
				(int) offset, (int) count).toSet(JedisConverters::toBytes);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRangeByScore(byte[], org.springframework.data.redis.connection.RedisZSetCommands.Range, org.springframework.data.redis.connection.RedisZSetCommands.Limit)
	 */
	@Override
	public Set<byte[]> zRangeByScore(byte[] key, Range range, Limit limit) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(range, "Range for ZRANGEBYSCORE must not be null!");
		Assert.notNull(limit, "Limit must not be null! Use Limit.unlimited() instead.");

		byte[] min = JedisConverters.boundaryToBytesForZRange(range.getMin(), JedisConverters.NEGATIVE_INFINITY_BYTES);
		byte[] max = JedisConverters.boundaryToBytesForZRange(range.getMax(), JedisConverters.POSITIVE_INFINITY_BYTES);

		if (!limit.isUnlimited()) {
			return connection.invoke().just(BinaryJedis::zrangeByScore, MultiKeyPipelineBase::zrangeByScore, key, min, max,
					limit.getOffset(), limit.getCount());
		}

		return connection.invoke().just(BinaryJedis::zrangeByScore, MultiKeyPipelineBase::zrangeByScore, key, min, max);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRangeByLex(byte[], org.springframework.data.redis.connection.RedisZSetCommands.Range, org.springframework.data.redis.connection.RedisZSetCommands.Limit)
	 */
	@Override
	public Set<byte[]> zRangeByLex(byte[] key, Range range, Limit limit) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(range, "Range for ZRANGEBYLEX must not be null!");
		Assert.notNull(limit, "Limit must not be null! Use Limit.unlimited() instead.");

		byte[] min = JedisConverters.boundaryToBytesForZRangeByLex(range.getMin(), JedisConverters.MINUS_BYTES);
		byte[] max = JedisConverters.boundaryToBytesForZRangeByLex(range.getMax(), JedisConverters.PLUS_BYTES);

		if (!limit.isUnlimited()) {
			return connection.invoke().just(BinaryJedis::zrangeByLex, MultiKeyPipelineBase::zrangeByLex, key, min, max,
					limit.getOffset(), limit.getCount());
		}

		return connection.invoke().just(BinaryJedis::zrangeByLex, MultiKeyPipelineBase::zrangeByLex, key, min, max);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRevRangeByLex(byte[], org.springframework.data.redis.connection.RedisZSetCommands.Range, org.springframework.data.redis.connection.RedisZSetCommands.Limit)
	 */
	@Override
	public Set<byte[]> zRevRangeByLex(byte[] key, Range range, Limit limit) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(range, "Range for ZREVRANGEBYLEX must not be null!");
		Assert.notNull(limit, "Limit must not be null! Use Limit.unlimited() instead.");

		byte[] min = JedisConverters.boundaryToBytesForZRangeByLex(range.getMin(), JedisConverters.MINUS_BYTES);
		byte[] max = JedisConverters.boundaryToBytesForZRangeByLex(range.getMax(), JedisConverters.PLUS_BYTES);

		if (!limit.isUnlimited()) {
			return connection.invoke().from(BinaryJedis::zrevrangeByLex, MultiKeyPipelineBase::zrevrangeByLex, key, max, min,
					limit.getOffset(), limit.getCount()).get(LinkedHashSet::new);
		}

		return connection.invoke().from(BinaryJedis::zrevrangeByLex, MultiKeyPipelineBase::zrevrangeByLex, key, max, min)
				.get(LinkedHashSet::new);
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
