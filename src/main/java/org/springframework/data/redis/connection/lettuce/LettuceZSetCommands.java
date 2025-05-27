/*
 * Copyright 2017-2025 the original author or authors.
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
package org.springframework.data.redis.connection.lettuce;

import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.ScoredValueScanCursor;
import io.lettuce.core.ZAggregateArgs;
import io.lettuce.core.ZStoreArgs;
import io.lettuce.core.api.async.RedisSortedSetAsyncCommands;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.NullUnmarked;
import org.jspecify.annotations.Nullable;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.connection.RedisZSetCommands;
import org.springframework.data.redis.connection.RedisZSetCommands.ZAddArgs.Flag;
import org.springframework.data.redis.connection.convert.Converters;
import org.springframework.data.redis.connection.zset.Aggregate;
import org.springframework.data.redis.connection.zset.Tuple;
import org.springframework.data.redis.connection.zset.Weights;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.Cursor.CursorId;
import org.springframework.data.redis.core.KeyBoundCursor;
import org.springframework.data.redis.core.ScanIteration;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.TimeoutUtils;
import org.springframework.util.Assert;

/**
 * {@link RedisZSetCommands} implementation for {@literal Lettuce}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Andrey Shlykov
 * @author Shyngys Sapraliyev
 * @author John Blum
 * @since 2.0
 */
@NullUnmarked
class LettuceZSetCommands implements RedisZSetCommands {

	private final LettuceConnection connection;

	LettuceZSetCommands(@NonNull LettuceConnection connection) {
		this.connection = connection;
	}

	@Override
	public Boolean zAdd(byte @NonNull [] key, double score, byte @NonNull [] value, @NonNull ZAddArgs args) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(value, "Value must not be null");

		return connection.invoke()
				.from(RedisSortedSetAsyncCommands::zadd, key, LettuceZSetCommands.toZAddArgs(args), score, value)
				.get(LettuceConverters.longToBoolean());
	}

	@Override
	public Long zAdd(byte @NonNull [] key, @NonNull Set<@NonNull Tuple> tuples, @NonNull ZAddArgs args) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(tuples, "Tuples must not be null");

		return connection.invoke().just(RedisSortedSetAsyncCommands::zadd, key, LettuceZSetCommands.toZAddArgs(args),
				LettuceConverters.toObjects(tuples).toArray());
	}

	@Override
	public Long zRem(byte @NonNull [] key, byte @NonNull [] @NonNull... values) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(values, "Values must not be null");
		Assert.noNullElements(values, "Values must not contain null elements");

		return connection.invoke().just(RedisSortedSetAsyncCommands::zrem, key, values);
	}

	@Override
	public Double zIncrBy(byte @NonNull [] key, double increment, byte @NonNull [] value) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(value, "Value must not be null");

		return connection.invoke().just(RedisSortedSetAsyncCommands::zincrby, key, increment, value);
	}

	@Override
	public byte[] zRandMember(byte @NonNull [] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().just(RedisSortedSetAsyncCommands::zrandmember, key);
	}

	@Override
	public List<byte @NonNull []> zRandMember(byte @NonNull [] key, long count) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().just(RedisSortedSetAsyncCommands::zrandmember, key, count);
	}

	@Override
	public Tuple zRandMemberWithScore(byte @NonNull [] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().from(RedisSortedSetAsyncCommands::zrandmemberWithScores, key)
				.get(LettuceConverters::toTuple);
	}

	@Override
	public List<@NonNull Tuple> zRandMemberWithScore(byte @NonNull [] key, long count) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().fromMany(RedisSortedSetAsyncCommands::zrandmemberWithScores, key, count)
				.toList(LettuceConverters::toTuple);
	}

	@Override
	public Long zRank(byte @NonNull [] key, byte @NonNull [] value) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(value, "Value must not be null");

		return connection.invoke().just(RedisSortedSetAsyncCommands::zrank, key, value);
	}

	@Override
	public Long zRevRank(byte @NonNull [] key, byte @NonNull [] value) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().just(RedisSortedSetAsyncCommands::zrevrank, key, value);
	}

	@Override
	public Set<byte @NonNull []> zRange(byte @NonNull [] key, long start, long end) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().fromMany(RedisSortedSetAsyncCommands::zrange, key, start, end).toSet();
	}

	@Override
	public Set<@NonNull Tuple> zRangeWithScores(byte @NonNull [] key, long start, long end) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().fromMany(RedisSortedSetAsyncCommands::zrangeWithScores, key, start, end)
				.toSet(LettuceConverters::toTuple);
	}

	@Override
	public Set<@NonNull Tuple> zRangeByScoreWithScores(byte @NonNull [] key,
			org.springframework.data.domain.@NonNull Range<? extends Number> range,
			org.springframework.data.redis.connection.@NonNull Limit limit) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(range, "Range for ZRANGEBYSCOREWITHSCORES must not be null");
		Assert.notNull(limit, "Limit must not be null");

		if (limit.isUnlimited()) {
			return connection.invoke()
					.fromMany(RedisSortedSetAsyncCommands::zrangebyscoreWithScores, key, LettuceConverters.toRange(range))
					.toSet(LettuceConverters::toTuple);
		}

		return connection.invoke().fromMany(RedisSortedSetAsyncCommands::zrangebyscoreWithScores, key,
				LettuceConverters.toRange(range), LettuceConverters.toLimit(limit)).toSet(LettuceConverters::toTuple);

	}

	@Override
	public Set<byte @NonNull []> zRevRange(byte @NonNull [] key, long start, long end) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().fromMany(RedisSortedSetAsyncCommands::zrevrange, key, start, end)
				.toSet(Converters.identityConverter());
	}

	@Override
	public Set<@NonNull Tuple> zRevRangeWithScores(byte @NonNull [] key, long start, long end) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().fromMany(RedisSortedSetAsyncCommands::zrevrangeWithScores, key, start, end)
				.toSet(LettuceConverters::toTuple);
	}

	@Override
	public Set<byte @NonNull []> zRevRangeByScore(byte @NonNull [] key,
			org.springframework.data.domain.@NonNull Range<? extends Number> range,
			org.springframework.data.redis.connection.@NonNull Limit limit) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(range, "Range for ZREVRANGEBYSCORE must not be null");
		Assert.notNull(limit, "Limit must not be null");

		if (limit.isUnlimited()) {

			return connection.invoke()
					.fromMany(RedisSortedSetAsyncCommands::zrevrangebyscore, key, LettuceConverters.toRange(range)).toSet();
		}

		return connection.invoke().fromMany(RedisSortedSetAsyncCommands::zrevrangebyscore, key,
				LettuceConverters.toRange(range), LettuceConverters.toLimit(limit)).toSet();

	}

	@Override
	public Set<@NonNull Tuple> zRevRangeByScoreWithScores(byte @NonNull [] key,
			org.springframework.data.domain.Range<? extends Number> range,
			org.springframework.data.redis.connection.Limit limit) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(range, "Range for ZREVRANGEBYSCOREWITHSCORES must not be null");
		Assert.notNull(limit, "Limit must not be null");

		if (limit.isUnlimited()) {
			return connection.invoke()
					.fromMany(RedisSortedSetAsyncCommands::zrevrangebyscoreWithScores, key, LettuceConverters.toRange(range))
					.toSet(LettuceConverters::toTuple);
		}

		return connection.invoke().fromMany(RedisSortedSetAsyncCommands::zrevrangebyscoreWithScores, key,
				LettuceConverters.toRange(range), LettuceConverters.toLimit(limit)).toSet(LettuceConverters::toTuple);

	}

	@Override
	public Long zCount(byte @NonNull [] key, org.springframework.data.domain.@NonNull Range<? extends Number> range) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().just(RedisSortedSetAsyncCommands::zcount, key, LettuceConverters.toRange(range));
	}

	@Override
	public Long zLexCount(byte @NonNull [] key, org.springframework.data.domain.@NonNull Range<byte[]> range) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(range, "Range must not be null");

		return connection.invoke().just(RedisSortedSetAsyncCommands::zlexcount, key,
				LettuceConverters.toRange(range, true));
	}

	@Override
	public Tuple zPopMin(byte @NonNull [] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().from(RedisSortedSetAsyncCommands::zpopmin, key).get(LettuceConverters::toTuple);
	}

	@Override
	public Set<Tuple> zPopMin(byte @NonNull [] key, long count) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().fromMany(RedisSortedSetAsyncCommands::zpopmin, key, count)
				.toSet(LettuceConverters::toTuple);
	}

	@Override
	public Tuple bZPopMin(byte @NonNull [] key, long timeout, TimeUnit unit) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(unit, "TimeUnit must not be null");

		if (TimeUnit.MILLISECONDS == unit) {

			return connection.invoke(connection.getAsyncDedicatedConnection())
					.from(RedisSortedSetAsyncCommands::bzpopmin, TimeoutUtils.toDoubleSeconds(timeout, unit), key)
					.get(it -> it.map(LettuceConverters::toTuple).getValueOrElse(null));
		}

		return connection.invoke(connection.getAsyncDedicatedConnection())
				.from(RedisSortedSetAsyncCommands::bzpopmin, unit.toSeconds(timeout), key)
				.get(it -> it.map(LettuceConverters::toTuple).getValueOrElse(null));
	}

	@Override
	public Tuple zPopMax(byte @NonNull [] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().from(RedisSortedSetAsyncCommands::zpopmax, key).get(LettuceConverters::toTuple);
	}

	@Override
	public Set<@NonNull Tuple> zPopMax(byte @NonNull [] key, long count) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().fromMany(RedisSortedSetAsyncCommands::zpopmax, key, count)
				.toSet(LettuceConverters::toTuple);
	}

	@Override
	public Tuple bZPopMax(byte @NonNull [] key, long timeout, TimeUnit unit) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(unit, "TimeUnit must not be null");

		if (TimeUnit.MILLISECONDS == unit) {

			return connection.invoke(connection.getAsyncDedicatedConnection())
					.from(RedisSortedSetAsyncCommands::bzpopmax, TimeoutUtils.toDoubleSeconds(timeout, unit), key)
					.get(it -> it.map(LettuceConverters::toTuple).getValueOrElse(null));
		}

		return connection.invoke(connection.getAsyncDedicatedConnection())
				.from(RedisSortedSetAsyncCommands::bzpopmax, unit.toSeconds(timeout), key)
				.get(it -> it.map(LettuceConverters::toTuple).getValueOrElse(null));
	}

	@Override
	public Long zCard(byte @NonNull [] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().just(RedisSortedSetAsyncCommands::zcard, key);
	}

	@Override
	public Double zScore(byte @NonNull [] key, byte @NonNull [] value) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(value, "Value must not be null");

		return connection.invoke().just(RedisSortedSetAsyncCommands::zscore, key, value);
	}

	@Override
	public List<Double> zMScore(byte @NonNull [] key, byte[][] values) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(values, "Value must not be null");

		return connection.invoke().just(RedisSortedSetAsyncCommands::zmscore, key, values);
	}

	@Override
	public Long zRemRange(byte @NonNull [] key, long start, long end) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().just(RedisSortedSetAsyncCommands::zremrangebyrank, key, start, end);
	}

	@Override
	public Long zRemRangeByLex(byte @NonNull [] key, org.springframework.data.domain.Range<byte[]> range) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(range, "Range must not be null for ZREMRANGEBYLEX");

		return connection.invoke().just(RedisSortedSetAsyncCommands::zremrangebylex, key,
				LettuceConverters.toRange(range, true));
	}

	@Override
	public Long zRemRangeByScore(byte @NonNull [] key, org.springframework.data.domain.Range<? extends Number> range) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(range, "Range for ZREMRANGEBYSCORE must not be null");

		return connection.invoke().just(RedisSortedSetAsyncCommands::zremrangebyscore, key,
				LettuceConverters.toRange(range));
	}

	@Override
	public Set<byte[]> zDiff(byte @NonNull [] @NonNull... sets) {

		Assert.notNull(sets, "Sets must not be null");

		return connection.invoke().fromMany(RedisSortedSetAsyncCommands::zdiff, sets).toSet();
	}

	@Override
	public Set<@NonNull Tuple> zDiffWithScores(byte @NonNull [] @NonNull... sets) {

		Assert.notNull(sets, "Sets must not be null");

		return connection.invoke().fromMany(RedisSortedSetAsyncCommands::zdiffWithScores, sets)
				.toSet(LettuceConverters::toTuple);
	}

	@Override
	public Long zDiffStore(byte[] destKey, byte @NonNull [] @NonNull... sets) {

		Assert.notNull(destKey, "Destination key must not be null");
		Assert.notNull(sets, "Source sets must not be null");

		return connection.invoke().just(RedisSortedSetAsyncCommands::zdiffstore, destKey, sets);
	}

	@Override
	public Set<byte @NonNull []> zInter(byte @NonNull [] @NonNull... sets) {

		Assert.notNull(sets, "Sets must not be null");

		return connection.invoke().fromMany(RedisSortedSetAsyncCommands::zinter, sets).toSet();
	}

	@Override
	public Set<@NonNull Tuple> zInterWithScores(byte @NonNull [] @NonNull... sets) {

		Assert.notNull(sets, "Sets must not be null");

		return connection.invoke().fromMany(RedisSortedSetAsyncCommands::zinterWithScores, sets)
				.toSet(LettuceConverters::toTuple);
	}

	@Override
	public Set<@NonNull Tuple> zInterWithScores(@NonNull Aggregate aggregate, @NonNull Weights weights,
			byte @NonNull [] @NonNull... sets) {

		Assert.notNull(sets, "Sets must not be null");
		Assert.noNullElements(sets, "Source sets must not contain null elements");
		Assert.isTrue(weights.size() == sets.length,
				"The number of weights %d must match the number of source sets %d".formatted(weights.size(), sets.length));

		ZAggregateArgs zAggregateArgs = zAggregateArgs(aggregate, weights);

		return connection.invoke().fromMany(RedisSortedSetAsyncCommands::zinterWithScores, zAggregateArgs, sets)
				.toSet(LettuceConverters::toTuple);
	}

	@Override
	public Long zInterStore(byte @NonNull [] destKey, @NonNull Aggregate aggregate, @NonNull Weights weights,
			byte @NonNull [] @NonNull... sets) {

		Assert.notNull(destKey, "Destination key must not be null");
		Assert.notNull(sets, "Source sets must not be null");
		Assert.noNullElements(sets, "Source sets must not contain null elements");
		Assert.isTrue(weights.size() == sets.length,
				"The number of weights %d must match the number of source sets %d".formatted(weights.size(), sets.length));

		ZStoreArgs storeArgs = zStoreArgs(aggregate, weights);

		return connection.invoke().just(RedisSortedSetAsyncCommands::zinterstore, destKey, storeArgs, sets);
	}

	@Override
	public Long zInterStore(byte @NonNull [] destKey, byte @NonNull [] @NonNull... sets) {

		Assert.notNull(destKey, "Destination key must not be null");
		Assert.notNull(sets, "Source sets must not be null");
		Assert.noNullElements(sets, "Source sets must not contain null elements");

		return connection.invoke().just(RedisSortedSetAsyncCommands::zinterstore, destKey, sets);
	}

	@Override
	public Set<byte @NonNull []> zUnion(byte @NonNull [] @NonNull... sets) {

		Assert.notNull(sets, "Sets must not be null");

		return connection.invoke().fromMany(RedisSortedSetAsyncCommands::zunion, sets).toSet();
	}

	@Override
	public Set<@NonNull Tuple> zUnionWithScores(byte @NonNull [] @NonNull... sets) {

		Assert.notNull(sets, "Sets must not be null");

		return connection.invoke().fromMany(RedisSortedSetAsyncCommands::zunionWithScores, sets)
				.toSet(LettuceConverters::toTuple);
	}

	@Override
	public Set<@NonNull Tuple> zUnionWithScores(@NonNull Aggregate aggregate, @NonNull Weights weights,
			byte @NonNull [] @NonNull... sets) {

		Assert.notNull(sets, "Sets must not be null");
		Assert.noNullElements(sets, "Source sets must not contain null elements");
		Assert.isTrue(weights.size() == sets.length,
				"The number of weights %d must match the number of source sets %d".formatted(weights.size(), sets.length));

		ZAggregateArgs zAggregateArgs = zAggregateArgs(aggregate, weights);

		return connection.invoke().fromMany(RedisSortedSetAsyncCommands::zunionWithScores, zAggregateArgs, sets)
				.toSet(LettuceConverters::toTuple);
	}

	@Override
	public Long zUnionStore(byte @NonNull [] destKey, @NonNull Aggregate aggregate, @NonNull Weights weights,
			byte @NonNull [] @NonNull... sets) {

		Assert.notNull(destKey, "Destination key must not be null");
		Assert.notNull(sets, "Source sets must not be null");
		Assert.noNullElements(sets, "Source sets must not contain null elements");
		Assert.isTrue(weights.size() == sets.length,
				"The number of weights %d must match the number of source sets %d".formatted(weights.size(), sets.length));

		ZStoreArgs storeArgs = zStoreArgs(aggregate, weights);

		return connection.invoke().just(RedisSortedSetAsyncCommands::zunionstore, destKey, storeArgs, sets);
	}

	@Override
	public Long zUnionStore(byte @NonNull [] destKey, byte @NonNull [] @NonNull... sets) {

		Assert.notNull(destKey, "Destination key must not be null");
		Assert.notNull(sets, "Source sets must not be null");
		Assert.noNullElements(sets, "Source sets must not contain null elements");

		return connection.invoke().just(RedisSortedSetAsyncCommands::zunionstore, destKey, sets);
	}

	@Override
	public Cursor<@NonNull Tuple> zScan(byte @NonNull [] key, @Nullable ScanOptions options) {
		return zScan(key, CursorId.initial(), options != null ? options : ScanOptions.NONE);
	}

	/**
	 * @since 1.4
	 */
	public Cursor<@NonNull Tuple> zScan(byte @NonNull [] key, @NonNull CursorId cursorId, @NonNull ScanOptions options) {

		Assert.notNull(key, "Key must not be null");

		return new KeyBoundCursor<Tuple>(key, cursorId, options) {

			@Override
			protected ScanIteration<Tuple> doScan(byte @NonNull [] key, CursorId cursorId, ScanOptions options) {

				if (connection.isQueueing() || connection.isPipelined()) {
					throw new InvalidDataAccessApiUsageException("'ZSCAN' cannot be called in pipeline / transaction mode");
				}

				io.lettuce.core.ScanCursor scanCursor = connection.getScanCursor(cursorId);
				ScanArgs scanArgs = LettuceConverters.toScanArgs(options);

				ScoredValueScanCursor<byte[]> scoredValueScanCursor = connection.invoke()
						.just(RedisSortedSetAsyncCommands::zscan, key, scanCursor, scanArgs);
				String nextCursorId = scoredValueScanCursor.getCursor();

				List<ScoredValue<byte[]>> result = scoredValueScanCursor.getValues();

				List<Tuple> values = connection.failsafeReadScanValues(result, LettuceConverters.scoredValuesToTupleList());
				return new ScanIteration<>(CursorId.of(nextCursorId), values);
			}

			@Override
			protected void doClose() {
				LettuceZSetCommands.this.connection.close();
			}

		}.open();
	}

	@Override
	public Set<byte @NonNull []> zRangeByScore(byte @NonNull [] key, @Nullable String min, @Nullable String max) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().fromMany(RedisSortedSetAsyncCommands::zrangebyscore, key, min, max).toSet();
	}

	@Override
	public Set<byte @NonNull []> zRangeByScore(byte @NonNull [] key, @Nullable String min, @Nullable String max,
			long offset, long count) {

		Assert.notNull(key, "Key must not be null");

		return connection.invoke().fromMany(RedisSortedSetAsyncCommands::zrangebyscore, key, min, max, offset, count)
				.toSet();
	}

	@Override
	public Set<byte @NonNull []> zRangeByScore(byte @NonNull [] key,
			org.springframework.data.domain.@NonNull Range<? extends Number> range,
			org.springframework.data.redis.connection.@NonNull Limit limit) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(range, "Range for ZRANGEBYSCORE must not be null");
		Assert.notNull(limit, "Limit must not be null");

		if (limit.isUnlimited()) {
			return connection.invoke()
					.fromMany(RedisSortedSetAsyncCommands::zrangebyscore, key, LettuceConverters.toRange(range)).toSet();
		}

		return connection.invoke().fromMany(RedisSortedSetAsyncCommands::zrangebyscore, key,
				LettuceConverters.toRange(range), LettuceConverters.toLimit(limit)).toSet();
	}

	@Override
	public Set<byte @NonNull []> zRangeByLex(byte @NonNull [] key,
			org.springframework.data.domain.@NonNull Range<byte[]> range,
			org.springframework.data.redis.connection.@NonNull Limit limit) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(range, "Range for ZRANGEBYLEX must not be null");
		Assert.notNull(limit, "Limit must not be null");

		if (limit.isUnlimited()) {
			return connection.invoke()
					.fromMany(RedisSortedSetAsyncCommands::zrangebylex, key, LettuceConverters.toRange(range, true)).toSet();
		}

		return connection.invoke().fromMany(RedisSortedSetAsyncCommands::zrangebylex, key,
				LettuceConverters.toRange(range, true), LettuceConverters.toLimit(limit)).toSet();
	}

	@Override
	public Set<byte @NonNull []> zRevRangeByLex(byte @NonNull [] key,
			org.springframework.data.domain.@NonNull Range<byte[]> range,
			org.springframework.data.redis.connection.@NonNull Limit limit) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(range, "Range for must not be null");
		Assert.notNull(limit, "Limit must not be null");

		if (limit.isUnlimited()) {
			return connection.invoke()
					.fromMany(RedisSortedSetAsyncCommands::zrevrangebylex, key, LettuceConverters.toRange(range, true)).toSet();
		}

		return connection.invoke().fromMany(RedisSortedSetAsyncCommands::zrevrangebylex, key,
				LettuceConverters.toRange(range, true), LettuceConverters.toLimit(limit)).toSet();
	}

	@Override
	public Long zRangeStoreByLex(byte @NonNull [] dstKey, byte @NonNull [] srcKey,
			org.springframework.data.domain.@NonNull Range<byte[]> range,
			org.springframework.data.redis.connection.@NonNull Limit limit) {

		Assert.notNull(dstKey, "Destination key must not be null");
		Assert.notNull(srcKey, "Source key must not be null");
		Assert.notNull(range, "Range for must not be null");
		Assert.notNull(limit, "Limit must not be null. Use Limit.unlimited() instead.");

		return connection.invoke().just(RedisSortedSetAsyncCommands::zrangestorebylex, dstKey, srcKey,
				LettuceConverters.toRange(range), LettuceConverters.toLimit(limit));
	}

	@Override
	public Long zRangeStoreRevByLex(byte @NonNull [] dstKey, byte @NonNull [] srcKey,
			org.springframework.data.domain.@NonNull Range<byte[]> range,
			org.springframework.data.redis.connection.@NonNull Limit limit) {

		Assert.notNull(dstKey, "Destination key must not be null");
		Assert.notNull(srcKey, "Source key must not be null");
		Assert.notNull(range, "Range for must not be null");
		Assert.notNull(limit, "Limit must not be null. Use Limit.unlimited() instead.");

		return connection.invoke().just(RedisSortedSetAsyncCommands::zrevrangestorebylex, dstKey, srcKey,
				LettuceConverters.toRange(range), LettuceConverters.toLimit(limit));
	}

	@Override
	public Long zRangeStoreByScore(byte @NonNull [] dstKey, byte @NonNull [] srcKey,
			org.springframework.data.domain.@NonNull Range<? extends Number> range,
			org.springframework.data.redis.connection.@NonNull Limit limit) {

		Assert.notNull(dstKey, "Destination key must not be null");
		Assert.notNull(srcKey, "Source key must not be null");
		Assert.notNull(range, "Range for must not be null");
		Assert.notNull(limit, "Limit must not be null. Use Limit.unlimited() instead.");

		return connection.invoke().just(RedisSortedSetAsyncCommands::zrangestorebyscore, dstKey, srcKey,
				LettuceConverters.toRange(range), LettuceConverters.toLimit(limit));
	}

	@Override
	public Long zRangeStoreRevByScore(byte @NonNull [] dstKey, byte @NonNull [] srcKey,
			org.springframework.data.domain.@NonNull Range<? extends Number> range,
			org.springframework.data.redis.connection.@NonNull Limit limit) {

		Assert.notNull(dstKey, "Destination key must not be null");
		Assert.notNull(srcKey, "Source key must not be null");
		Assert.notNull(range, "Range for must not be null");
		Assert.notNull(limit, "Limit must not be null. Use Limit.unlimited() instead.");

		return connection.invoke().just(RedisSortedSetAsyncCommands::zrevrangestorebyscore, dstKey, srcKey,
				LettuceConverters.toRange(range), LettuceConverters.toLimit(limit));
	}

	public RedisClusterCommands<byte[], byte[]> getConnection() {
		return connection.getConnection();
	}

	private static ZStoreArgs zStoreArgs(@Nullable Aggregate aggregate, Weights weights) {

		ZStoreArgs args = new ZStoreArgs();

		if (aggregate != null) {
			switch (aggregate) {
				case MIN -> args.min();
				case MAX -> args.max();
				default -> args.sum();
			}
		}

		args.weights(weights.toArray());

		return args;
	}

	private static ZAggregateArgs zAggregateArgs(@Nullable Aggregate aggregate, Weights weights) {

		ZAggregateArgs args = new ZAggregateArgs();

		if (aggregate != null) {
			switch (aggregate) {
				case MIN -> args.min();
				case MAX -> args.max();
				default -> args.sum();
			}
		}

		args.weights(weights.toArray());

		return args;
	}

	/**
	 * Convert {@link ZAddArgs} to {@link io.lettuce.core.ZAddArgs}.
	 *
	 * @param source must not be {@literal null}.
	 * @return never {@literal null}.
	 * @since 2.5
	 */
	private static io.lettuce.core.ZAddArgs toZAddArgs(ZAddArgs source) {

		io.lettuce.core.ZAddArgs target = new io.lettuce.core.ZAddArgs();

		if (source.isEmpty()) {
			return target;
		}

		if (source.contains(Flag.XX)) {
			target.xx();
		}
		if (source.contains(Flag.NX)) {
			target.nx();
		}
		if (source.contains(Flag.GT)) {
			target.gt();
		}
		if (source.contains(Flag.LT)) {
			target.lt();
		}
		if (source.contains(Flag.CH)) {
			target.ch();
		}

		return target;
	}
}
