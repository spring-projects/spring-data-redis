/*
 * Copyright 2017-present the original author or authors.
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

import redis.clients.jedis.Protocol;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.params.ZParams;
import redis.clients.jedis.params.ZRangeParams;
import redis.clients.jedis.resps.ScanResult;
import redis.clients.jedis.util.KeyValue;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.NullUnmarked;
import org.jspecify.annotations.Nullable;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.connection.ClusterSlotHashUtil;
import org.springframework.data.redis.connection.RedisZSetCommands;
import org.springframework.data.redis.connection.convert.SetConverter;
import org.springframework.data.redis.connection.zset.Aggregate;
import org.springframework.data.redis.connection.zset.Tuple;
import org.springframework.data.redis.connection.zset.Weights;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanCursor;
import org.springframework.data.redis.core.ScanIteration;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.util.ByteUtils;
import org.springframework.lang.Contract;
import org.springframework.util.Assert;

/**
 * Cluster {@link RedisZSetCommands} implementation for Jedis.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Clement Ong
 * @author Andrey Shlykov
 * @author Jens Deppe
 * @author Shyngys Sapraliyev
 * @author John Blum
 * @since 2.0
 */
@NullUnmarked
class JedisClusterZSetCommands implements RedisZSetCommands {

	private static final SetConverter<redis.clients.jedis.resps.Tuple, Tuple> TUPLE_SET_CONVERTER = new SetConverter<>(
			JedisConverters::toTuple);

	private final JedisClusterConnection connection;

	JedisClusterZSetCommands(@NonNull JedisClusterConnection connection) {
		this.connection = connection;
	}

	@Override
	public Boolean zAdd(byte @NonNull [] key, double score, byte @NonNull [] value, @NonNull ZAddArgs args) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(value, "Value must not be null");

		try {
			return JedisConverters
					.toBoolean(connection.getCluster().zadd(key, score, value, JedisConverters.toZAddParams(args)));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long zAdd(byte @NonNull [] key, @NonNull Set<@NonNull Tuple> tuples, @NonNull ZAddArgs args) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(tuples, "Tuples must not be null");

		try {
			return connection.getCluster().zadd(key, JedisConverters.toTupleMap(tuples), JedisConverters.toZAddParams(args));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long zRem(byte @NonNull [] key, byte @NonNull [] @NonNull... values) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(values, "Values must not be null");
		Assert.noNullElements(values, "Values must not contain null elements");

		try {
			return connection.getCluster().zrem(key, values);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}

	}

	@Override
	public Double zIncrBy(byte @NonNull [] key, double increment, byte @NonNull [] value) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(value, "Value must not be null");

		try {
			return connection.getCluster().zincrby(key, increment, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public byte[] zRandMember(byte @NonNull [] key) {

		Assert.notNull(key, "Key must not be null");

		try {
			return connection.getCluster().zrandmember(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public List<byte[]> zRandMember(byte @NonNull [] key, long count) {

		Assert.notNull(key, "Key must not be null");

		try {
			return new ArrayList<>(connection.getCluster().zrandmember(key, count));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Tuple zRandMemberWithScore(byte @NonNull [] key) {

		Assert.notNull(key, "Key must not be null");

		try {
			List<redis.clients.jedis.resps.Tuple> tuples = connection.getCluster().zrandmemberWithScores(key, 1);

			return tuples.isEmpty() ? null : JedisConverters.toTuple(tuples.iterator().next());
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public List<Tuple> zRandMemberWithScore(byte @NonNull [] key, long count) {

		Assert.notNull(key, "Key must not be null");

		try {
			List<redis.clients.jedis.resps.Tuple> tuples = connection.getCluster().zrandmemberWithScores(key, count);

			return tuples.stream().map(JedisConverters::toTuple).collect(Collectors.toList());
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long zRank(byte @NonNull [] key, byte @NonNull [] value) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(value, "Value must not be null");

		try {
			return connection.getCluster().zrank(key, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long zRevRank(byte @NonNull [] key, byte @NonNull [] value) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(value, "Value must not be null");

		try {
			return connection.getCluster().zrevrank(key, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Set<byte @NonNull []> zRange(byte @NonNull [] key, long start, long end) {

		Assert.notNull(key, "Key must not be null");

		try {
			return new LinkedHashSet<>(connection.getCluster().zrange(key, start, end));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Set<Tuple> zRangeByScoreWithScores(byte @NonNull [] key,
			org.springframework.data.domain.@NonNull Range<? extends Number> range,
			org.springframework.data.redis.connection.@NonNull Limit limit) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(range, "Range cannot be null for ZRANGEBYSCOREWITHSCORES");

		byte[] min = JedisConverters.boundaryToBytesForZRange(range.getLowerBound(),
				JedisConverters.NEGATIVE_INFINITY_BYTES);
		byte[] max = JedisConverters.boundaryToBytesForZRange(range.getUpperBound(),
				JedisConverters.POSITIVE_INFINITY_BYTES);

		try {
			if (limit.isUnlimited()) {
				return toTupleSet(connection.getCluster().zrangeByScoreWithScores(key, min, max));
			}
			return toTupleSet(
					connection.getCluster().zrangeByScoreWithScores(key, min, max, limit.getOffset(), limit.getCount()));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Set<byte[]> zRevRangeByScore(byte @NonNull [] key,
			org.springframework.data.domain.@NonNull Range<? extends Number> range,
			org.springframework.data.redis.connection.@NonNull Limit limit) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(range, "Range cannot be null for ZREVRANGEBYSCORE");

		byte[] min = JedisConverters.boundaryToBytesForZRange(range.getLowerBound(),
				JedisConverters.NEGATIVE_INFINITY_BYTES);
		byte[] max = JedisConverters.boundaryToBytesForZRange(range.getUpperBound(),
				JedisConverters.POSITIVE_INFINITY_BYTES);

		try {
			if (limit.isUnlimited()) {
				return new LinkedHashSet<>(connection.getCluster().zrevrangeByScore(key, max, min));
			}
			return new LinkedHashSet<>(
					connection.getCluster().zrevrangeByScore(key, max, min, limit.getOffset(), limit.getCount()));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Set<Tuple> zRevRangeByScoreWithScores(byte @NonNull [] key,
			org.springframework.data.domain.@NonNull Range<? extends Number> range,
			org.springframework.data.redis.connection.@NonNull Limit limit) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(range, "Range cannot be null for ZREVRANGEBYSCOREWITHSCORES");

		byte[] min = JedisConverters.boundaryToBytesForZRange(range.getLowerBound(),
				JedisConverters.NEGATIVE_INFINITY_BYTES);
		byte[] max = JedisConverters.boundaryToBytesForZRange(range.getUpperBound(),
				JedisConverters.POSITIVE_INFINITY_BYTES);

		try {
			if (limit.isUnlimited()) {
				return toTupleSet(connection.getCluster().zrevrangeByScoreWithScores(key, max, min));
			}
			return toTupleSet(
					connection.getCluster().zrevrangeByScoreWithScores(key, max, min, limit.getOffset(), limit.getCount()));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long zCount(byte @NonNull [] key, org.springframework.data.domain.@NonNull Range<? extends Number> range) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(range, "Range cannot be null for ZCOUNT");

		byte[] min = JedisConverters.boundaryToBytesForZRange(range.getLowerBound(),
				JedisConverters.NEGATIVE_INFINITY_BYTES);
		byte[] max = JedisConverters.boundaryToBytesForZRange(range.getUpperBound(),
				JedisConverters.POSITIVE_INFINITY_BYTES);

		try {
			return connection.getCluster().zcount(key, min, max);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long zLexCount(byte @NonNull [] key, org.springframework.data.domain.@NonNull Range<byte[]> range) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(range, "Range must not be null");

		byte[] min = JedisConverters.boundaryToBytesForZRangeByLex(range.getLowerBound(), JedisConverters.MINUS_BYTES);
		byte[] max = JedisConverters.boundaryToBytesForZRangeByLex(range.getUpperBound(), JedisConverters.PLUS_BYTES);

		try {
			return connection.getCluster().zlexcount(key, min, max);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Tuple zPopMin(byte @NonNull [] key) {

		Assert.notNull(key, "Key must not be null");

		try {
			redis.clients.jedis.resps.Tuple tuple = connection.getCluster().zpopmin(key);
			return tuple != null ? JedisConverters.toTuple(tuple) : null;
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Set<Tuple> zPopMin(byte @NonNull [] key, long count) {

		Assert.notNull(key, "Key must not be null");

		try {
			return toTupleSet(connection.getCluster().zpopmin(key, Math.toIntExact(count)));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Tuple bZPopMin(byte @NonNull [] key, long timeout, @NonNull TimeUnit unit) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(unit, "TimeUnit must not be null");

		try {
			return toTuple(connection.getCluster().bzpopmin(JedisConverters.toSeconds(timeout, unit), key));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Tuple zPopMax(byte @NonNull [] key) {

		Assert.notNull(key, "Key must not be null");

		try {
			redis.clients.jedis.resps.Tuple tuple = connection.getCluster().zpopmax(key);
			return tuple != null ? JedisConverters.toTuple(tuple) : null;
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Set<Tuple> zPopMax(byte @NonNull [] key, long count) {

		Assert.notNull(key, "Key must not be null");

		try {
			return toTupleSet(connection.getCluster().zpopmax(key, Math.toIntExact(count)));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Tuple bZPopMax(byte @NonNull [] key, long timeout, @NonNull TimeUnit unit) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(unit, "TimeUnit must not be null");

		try {
			return toTuple(connection.getCluster().bzpopmax(JedisConverters.toSeconds(timeout, unit), key));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long zRemRangeByScore(byte @NonNull [] key,
			org.springframework.data.domain.@NonNull Range<? extends Number> range) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(range, "Range cannot be null for ZREMRANGEBYSCORE");

		byte[] min = JedisConverters.boundaryToBytesForZRange(range.getLowerBound(),
				JedisConverters.NEGATIVE_INFINITY_BYTES);
		byte[] max = JedisConverters.boundaryToBytesForZRange(range.getUpperBound(),
				JedisConverters.POSITIVE_INFINITY_BYTES);

		try {
			return connection.getCluster().zremrangeByScore(key, min, max);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}

	}

	@Override
	public Set<@NonNull byte[]> zRangeByScore(byte @NonNull [] key,
			org.springframework.data.domain.@NonNull Range<? extends Number> range,
			org.springframework.data.redis.connection.@NonNull Limit limit) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(range, "Range cannot be null for ZRANGEBYSCORE");

		byte[] min = JedisConverters.boundaryToBytesForZRange(range.getLowerBound(),
				JedisConverters.NEGATIVE_INFINITY_BYTES);
		byte[] max = JedisConverters.boundaryToBytesForZRange(range.getUpperBound(),
				JedisConverters.POSITIVE_INFINITY_BYTES);

		try {
			if (limit.isUnlimited()) {
				return new LinkedHashSet<>(connection.getCluster().zrangeByScore(key, min, max));
			}
			return new LinkedHashSet<>(
					connection.getCluster().zrangeByScore(key, min, max, limit.getOffset(), limit.getCount()));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Set<byte @NonNull []> zRangeByLex(byte @NonNull [] key,
			org.springframework.data.domain.@NonNull Range<byte[]> range,
			org.springframework.data.redis.connection.@NonNull Limit limit) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(range, "Range must not be null for ZRANGEBYLEX");
		Assert.notNull(limit, "Limit must not be null");

		byte[] min = JedisConverters.boundaryToBytesForZRangeByLex(range.getLowerBound(), JedisConverters.MINUS_BYTES);
		byte[] max = JedisConverters.boundaryToBytesForZRangeByLex(range.getUpperBound(), JedisConverters.PLUS_BYTES);

		try {
			if (limit.isUnlimited()) {
				return new LinkedHashSet<>(connection.getCluster().zrangeByLex(key, min, max));
			}
			return new LinkedHashSet<>(
					connection.getCluster().zrangeByLex(key, min, max, limit.getOffset(), limit.getCount()));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long zRemRangeByLex(byte @NonNull [] key, org.springframework.data.domain.@NonNull Range<byte[]> range) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(range, "Range must not be null for ZREMRANGEBYLEX");

		byte[] min = JedisConverters.boundaryToBytesForZRangeByLex(range.getLowerBound(), JedisConverters.MINUS_BYTES);
		byte[] max = JedisConverters.boundaryToBytesForZRangeByLex(range.getUpperBound(), JedisConverters.PLUS_BYTES);

		try {
			return connection.getCluster().zremrangeByLex(key, min, max);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Set<byte @NonNull []> zRevRangeByLex(byte @NonNull [] key,
			org.springframework.data.domain.@NonNull Range<byte[]> range,
			org.springframework.data.redis.connection.@NonNull Limit limit) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(range, "Range must not be null for ZREVRANGEBYLEX");
		Assert.notNull(limit, "Limit must not be null");

		byte[] min = JedisConverters.boundaryToBytesForZRangeByLex(range.getLowerBound(), JedisConverters.MINUS_BYTES);
		byte[] max = JedisConverters.boundaryToBytesForZRangeByLex(range.getUpperBound(), JedisConverters.PLUS_BYTES);

		try {
			if (limit.isUnlimited()) {
				return new LinkedHashSet<>(connection.getCluster().zrevrangeByLex(key, max, min));
			}
			return new LinkedHashSet<>(
					connection.getCluster().zrevrangeByLex(key, max, min, limit.getOffset(), limit.getCount()));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
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

		ZRangeParams zRangeParams = new ZRangeParams(Protocol.Keyword.BYLEX, min, max);

		if (limit.isLimited()) {
			zRangeParams = zRangeParams.limit(limit.getOffset(), limit.getCount());
		}

		if (rev) {
			zRangeParams = zRangeParams.rev();
		}

		try {
			return connection.getCluster().zrangestore(dstKey, srcKey, zRangeParams);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Nullable
	@Override
	public Long zRangeStoreByScore(byte @NonNull [] dstKey, byte @NonNull [] srcKey,
			org.springframework.data.domain.@NonNull Range<? extends Number> range,
			org.springframework.data.redis.connection.@NonNull Limit limit) {
		return zRangeStoreByScore(dstKey, srcKey, range, limit, false);
	}

	@Nullable
	@Override
	public Long zRangeStoreRevByScore(byte @NonNull [] dstKey, byte @NonNull [] srcKey,
			org.springframework.data.domain.@NonNull Range<? extends Number> range,
			org.springframework.data.redis.connection.@NonNull Limit limit) {
		return zRangeStoreByScore(dstKey, srcKey, range, limit, true);
	}

	private Long zRangeStoreByScore(byte @NonNull [] dstKey, byte @NonNull [] srcKey,
			org.springframework.data.domain.@NonNull Range<? extends Number> range,
			org.springframework.data.redis.connection.@NonNull Limit limit, boolean rev) {

		Assert.notNull(dstKey, "Destination key must not be null");
		Assert.notNull(srcKey, "Source key must not be null");
		Assert.notNull(range, "Range for must not be null");
		Assert.notNull(limit, "Limit must not be null. Use Limit.unlimited() instead.");

		byte[] min = JedisConverters.boundaryToBytesForZRange(range.getLowerBound(),
				JedisConverters.NEGATIVE_INFINITY_BYTES);
		byte[] max = JedisConverters.boundaryToBytesForZRange(range.getUpperBound(),
				JedisConverters.POSITIVE_INFINITY_BYTES);

		ZRangeParams zRangeParams = new ZRangeParams(Protocol.Keyword.BYSCORE, min, max);

		if (limit.isLimited()) {
			zRangeParams = zRangeParams.limit(limit.getOffset(), limit.getCount());
		}

		if (rev) {
			zRangeParams = zRangeParams.rev();
		}

		try {
			return connection.getCluster().zrangestore(dstKey, srcKey, zRangeParams);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Set<@NonNull Tuple> zRangeWithScores(byte @NonNull [] key, long start, long end) {

		Assert.notNull(key, "Key must not be null");

		try {
			return toTupleSet(connection.getCluster().zrangeWithScores(key, start, end));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Set<byte @NonNull []> zRangeByScore(byte @NonNull [] key, double min, double max) {

		Assert.notNull(key, "Key must not be null");

		try {
			return new LinkedHashSet<>(connection.getCluster().zrangeByScore(key, min, max));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Set<@NonNull Tuple> zRangeByScoreWithScores(byte @NonNull [] key, double min, double max) {

		Assert.notNull(key, "Key must not be null");

		try {
			return toTupleSet(connection.getCluster().zrangeByScoreWithScores(key, min, max));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Set<byte @NonNull []> zRangeByScore(byte @NonNull [] key, double min, double max, long offset, long count) {

		Assert.notNull(key, "Key must not be null");

		if (offset > Integer.MAX_VALUE || count > Integer.MAX_VALUE) {
			throw new IllegalArgumentException("Count/Offset cannot exceed Integer.MAX_VALUE");
		}

		try {
			return new LinkedHashSet<>(connection.getCluster().zrangeByScore(key, min, max, Long.valueOf(offset).intValue(),
					Long.valueOf(count).intValue()));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Set<@NonNull Tuple> zRangeByScoreWithScores(byte @NonNull [] key, double min, double max, long offset,
			long count) {

		Assert.notNull(key, "Key must not be null");

		if (offset > Integer.MAX_VALUE || count > Integer.MAX_VALUE) {
			throw new IllegalArgumentException("Count/Offset cannot exceed Integer.MAX_VALUE");
		}

		try {
			return toTupleSet(connection.getCluster().zrangeByScoreWithScores(key, min, max, Long.valueOf(offset).intValue(),
					Long.valueOf(count).intValue()));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Set<byte @NonNull []> zRevRange(byte @NonNull [] key, long start, long end) {

		Assert.notNull(key, "Key must not be null");

		try {
			return new LinkedHashSet<>(connection.getCluster().zrevrange(key, start, end));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Set<@NonNull Tuple> zRevRangeWithScores(byte @NonNull [] key, long start, long end) {

		Assert.notNull(key, "Key must not be null");

		try {
			return toTupleSet(connection.getCluster().zrevrangeWithScores(key, start, end));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Set<byte @NonNull []> zRevRangeByScore(byte @NonNull [] key, double min, double max) {

		Assert.notNull(key, "Key must not be null");

		try {
			return new LinkedHashSet<>(connection.getCluster().zrevrangeByScore(key, max, min));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Set<@NonNull Tuple> zRevRangeByScoreWithScores(byte @NonNull [] key, double min, double max) {

		Assert.notNull(key, "Key must not be null");

		try {
			return toTupleSet(connection.getCluster().zrevrangeByScoreWithScores(key, max, min));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Set<byte @NonNull []> zRevRangeByScore(byte @NonNull [] key, double min, double max, long offset, long count) {

		Assert.notNull(key, "Key must not be null");

		if (offset > Integer.MAX_VALUE || count > Integer.MAX_VALUE) {
			throw new IllegalArgumentException("Count/Offset cannot exceed Integer.MAX_VALUE");
		}

		try {
			return new LinkedHashSet<>(connection.getCluster().zrevrangeByScore(key, max, min,
					Long.valueOf(offset).intValue(), Long.valueOf(count).intValue()));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Set<@NonNull Tuple> zRevRangeByScoreWithScores(byte @NonNull [] key, double min, double max, long offset,
			long count) {

		Assert.notNull(key, "Key must not be null");

		if (offset > Integer.MAX_VALUE || count > Integer.MAX_VALUE) {
			throw new IllegalArgumentException("Count/Offset cannot exceed Integer.MAX_VALUE");
		}

		try {
			return toTupleSet(connection.getCluster().zrevrangeByScoreWithScores(key, max, min,
					Long.valueOf(offset).intValue(), Long.valueOf(count).intValue()));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long zCount(byte @NonNull [] key, double min, double max) {

		Assert.notNull(key, "Key must not be null");

		try {
			return connection.getCluster().zcount(key, min, max);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long zCard(byte @NonNull [] key) {

		Assert.notNull(key, "Key must not be null");

		try {
			return connection.getCluster().zcard(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Double zScore(byte @NonNull [] key, byte @NonNull [] value) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(value, "Value must not be null");

		try {
			return connection.getCluster().zscore(key, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public List<Double> zMScore(byte @NonNull [] key, byte @NonNull [] @NonNull [] values) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(values, "Values must not be null");

		try {
			return connection.getCluster().zmscore(key, values);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long zRemRange(byte @NonNull [] key, long start, long end) {

		Assert.notNull(key, "Key must not be null");

		try {
			return connection.getCluster().zremrangeByRank(key, start, end);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long zRemRangeByScore(byte @NonNull [] key, double min, double max) {

		Assert.notNull(key, "Key must not be null");

		try {
			return connection.getCluster().zremrangeByScore(key, min, max);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Set<byte @NonNull []> zDiff(byte @NonNull [] @NonNull... sets) {

		Assert.notNull(sets, "Sets must not be null");

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(sets)) {

			try {
				return JedisConverters.toSet(connection.getCluster().zdiff(sets));
			} catch (Exception ex) {
				throw convertJedisAccessException(ex);
			}
		}

		throw new InvalidDataAccessApiUsageException("ZDIFF can only be executed when all keys map to the same slot");
	}

	@Override
	public Set<Tuple> zDiffWithScores(byte @NonNull [] @NonNull... sets) {

		Assert.notNull(sets, "Sets must not be null");

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(sets)) {

			try {
				return JedisConverters.toSet(JedisConverters.toTupleList(connection.getCluster().zdiffWithScores(sets)));
			} catch (Exception ex) {
				throw convertJedisAccessException(ex);
			}
		}

		throw new InvalidDataAccessApiUsageException("ZDIFF can only be executed when all keys map to the same slot");
	}

	@Override
	public Long zDiffStore(byte @NonNull [] destKey, byte @NonNull [] @NonNull... sets) {

		Assert.notNull(destKey, "Destination key must not be null");
		Assert.notNull(sets, "Source sets must not be null");

		byte[][] allKeys = ByteUtils.mergeArrays(destKey, sets);

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(allKeys)) {

			try {
				return connection.getCluster().zdiffStore(destKey, sets);
			} catch (Exception ex) {
				throw convertJedisAccessException(ex);
			}
		}

		throw new InvalidDataAccessApiUsageException("ZDIFFSTORE can only be executed when all keys map to the same slot");
	}

	@Override
	public Set<byte @NonNull []> zInter(byte @NonNull [] @NonNull... sets) {

		Assert.notNull(sets, "Sets must not be null");

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(sets)) {

			try {
				return JedisConverters.toSet(connection.getCluster().zinter(new ZParams(), sets));
			} catch (Exception ex) {
				throw convertJedisAccessException(ex);
			}
		}

		throw new InvalidDataAccessApiUsageException("ZINTER can only be executed when all keys map to the same slot");
	}

	@Override
	public Set<@NonNull Tuple> zInterWithScores(byte @NonNull [] @NonNull... sets) {

		Assert.notNull(sets, "Sets must not be null");

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(sets)) {

			try {
				return JedisConverters
						.toSet(JedisConverters.toTupleList(connection.getCluster().zinterWithScores(new ZParams(), sets)));
			} catch (Exception ex) {
				throw convertJedisAccessException(ex);
			}
		}

		throw new InvalidDataAccessApiUsageException("ZINTER can only be executed when all keys map to the same slot");
	}

	@Override
	public Set<@NonNull Tuple> zInterWithScores(@NonNull Aggregate aggregate, @NonNull Weights weights,
			byte @NonNull [] @NonNull... sets) {

		Assert.notNull(sets, "Sets must not be null");
		Assert.noNullElements(sets, "Source sets must not contain null elements");
		Assert.isTrue(weights.size() == sets.length,
				() -> "The number of weights %d must match the number of source sets %d".formatted(weights.size(),
						sets.length));

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(sets)) {

			try {
				return JedisConverters.toSet(
						JedisConverters.toTupleList(connection.getCluster().zinterWithScores(toZParams(aggregate, weights), sets)));
			} catch (Exception ex) {
				throw convertJedisAccessException(ex);
			}
		}

		throw new InvalidDataAccessApiUsageException("ZINTER can only be executed when all keys map to the same slot");
	}

	@Override
	public Long zInterStore(byte @NonNull [] destKey, byte @NonNull [] @NonNull... sets) {

		Assert.notNull(destKey, "Destination key must not be null");
		Assert.notNull(sets, "Source sets must not be null");
		Assert.noNullElements(sets, "Source sets must not contain null elements");

		byte[][] allKeys = ByteUtils.mergeArrays(destKey, sets);

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(allKeys)) {

			try {
				return connection.getCluster().zinterstore(destKey, sets);
			} catch (Exception ex) {
				throw convertJedisAccessException(ex);
			}
		}

		throw new InvalidDataAccessApiUsageException("ZINTERSTORE can only be executed when all keys map to the same slot");
	}

	@Override
	public Long zInterStore(byte @NonNull [] destKey, @NonNull Aggregate aggregate, @NonNull Weights weights,
			byte @NonNull [] @NonNull... sets) {

		Assert.notNull(destKey, "Destination key must not be null");
		Assert.notNull(sets, "Source sets must not be null");
		Assert.noNullElements(sets, "Source sets must not contain null elements");
		Assert.isTrue(weights.size() == sets.length,
				"The number of weights %d must match the number of source sets %d".formatted(weights.size(), sets.length));

		byte[][] allKeys = ByteUtils.mergeArrays(destKey, sets);

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(allKeys)) {

			try {
				return connection.getCluster().zinterstore(destKey, toZParams(aggregate, weights), sets);
			} catch (Exception ex) {
				throw convertJedisAccessException(ex);
			}
		}

		throw new IllegalArgumentException("ZINTERSTORE can only be executed when all keys map to the same slot");
	}

	@Override
	public Set<byte @NonNull []> zUnion(byte @NonNull [] @NonNull... sets) {

		Assert.notNull(sets, "Sets must not be null");

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(sets)) {

			try {
				return JedisConverters.toSet(connection.getCluster().zunion(new ZParams(), sets));
			} catch (Exception ex) {
				throw convertJedisAccessException(ex);
			}
		}

		throw new InvalidDataAccessApiUsageException("ZUNION can only be executed when all keys map to the same slot");
	}

	@Override
	public Set<@NonNull Tuple> zUnionWithScores(byte @NonNull [] @NonNull... sets) {

		Assert.notNull(sets, "Sets must not be null");

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(sets)) {

			try {
				return JedisConverters
						.toSet(JedisConverters.toTupleList(connection.getCluster().zunionWithScores(new ZParams(), sets)));
			} catch (Exception ex) {
				throw convertJedisAccessException(ex);
			}
		}

		throw new InvalidDataAccessApiUsageException("ZUNION can only be executed when all keys map to the same slot");
	}

	@Override
	public Set<@NonNull Tuple> zUnionWithScores(@NonNull Aggregate aggregate, @NonNull Weights weights,
			byte @NonNull [] @NonNull... sets) {

		Assert.notNull(sets, "Sets must not be null");
		Assert.noNullElements(sets, "Source sets must not contain null elements");
		Assert.isTrue(weights.size() == sets.length,
				() -> "The number of weights %d must match the number of source sets %d".formatted(weights.size(),
						sets.length));

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(sets)) {

			try {
				return JedisConverters.toSet(
						JedisConverters.toTupleList(connection.getCluster().zunionWithScores(toZParams(aggregate, weights), sets)));
			} catch (Exception ex) {
				throw convertJedisAccessException(ex);

			}
		}

		throw new InvalidDataAccessApiUsageException("ZUNION can only be executed when all keys map to the same slot");
	}

	@Override
	public Long zUnionStore(byte @NonNull [] destKey, byte @NonNull [] @NonNull... sets) {

		Assert.notNull(destKey, "Destination key must not be null");
		Assert.notNull(sets, "Source sets must not be null");
		Assert.noNullElements(sets, "Source sets must not contain null elements");

		byte[][] allKeys = ByteUtils.mergeArrays(destKey, sets);

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(allKeys)) {

			try {
				return connection.getCluster().zunionstore(destKey, sets);
			} catch (Exception ex) {
				throw convertJedisAccessException(ex);
			}
		}

		throw new InvalidDataAccessApiUsageException("ZUNIONSTORE can only be executed when all keys map to the same slot");
	}

	@Override
	public Long zUnionStore(byte @NonNull [] destKey, @NonNull Aggregate aggregate, @NonNull Weights weights,
			byte @NonNull [] @NonNull... sets) {

		Assert.notNull(destKey, "Destination key must not be null");
		Assert.notNull(sets, "Source sets must not be null");
		Assert.noNullElements(sets, "Source sets must not contain null elements");
		Assert.isTrue(weights.size() == sets.length,
				"The number of weights %d must match the number of source sets %d".formatted(weights.size(), sets.length));

		byte[][] allKeys = ByteUtils.mergeArrays(destKey, sets);

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(allKeys)) {

			ZParams zparams = toZParams(aggregate, weights);

			try {
				return connection.getCluster().zunionstore(destKey, zparams, sets);
			} catch (Exception ex) {
				throw convertJedisAccessException(ex);
			}
		}

		throw new InvalidDataAccessApiUsageException("ZUNIONSTORE can only be executed when all keys map to the same slot");
	}

	@Override
	public Cursor<@NonNull Tuple> zScan(byte @NonNull [] key, @NonNull ScanOptions options) {

		Assert.notNull(key, "Key must not be null");

		return new ScanCursor<Tuple>(options) {

			@Override
			protected ScanIteration<Tuple> doScan(CursorId cursorId, ScanOptions options) {

				ScanParams params = JedisConverters.toScanParams(options);

				ScanResult<redis.clients.jedis.resps.Tuple> result = connection.getCluster().zscan(key,
						JedisConverters.toBytes(cursorId), params);
				return new ScanIteration<>(CursorId.of(result.getCursor()),
						JedisConverters.tuplesToTuples().convert(result.getResult()));
			}
		}.open();
	}

	@Override
	public Set<byte @NonNull []> zRangeByScore(byte @NonNull [] key, @NonNull String min, @NonNull String max) {

		Assert.notNull(key, "Key must not be null");

		try {
			return new LinkedHashSet<>(
					connection.getCluster().zrangeByScore(key, JedisConverters.toBytes(min), JedisConverters.toBytes(max)));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Set<byte @NonNull []> zRangeByScore(byte @NonNull [] key, @NonNull String min, @NonNull String max,
			long offset, long count) {

		Assert.notNull(key, "Key must not be null");

		if (offset > Integer.MAX_VALUE || count > Integer.MAX_VALUE) {
			throw new IllegalArgumentException("Count/Offset cannot exceed Integer.MAX_VALUE");
		}

		try {
			return new LinkedHashSet<>(connection.getCluster().zrangeByScore(key, JedisConverters.toBytes(min),
					JedisConverters.toBytes(max), Long.valueOf(offset).intValue(), Long.valueOf(count).intValue()));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	private DataAccessException convertJedisAccessException(Exception ex) {
		return connection.convertJedisAccessException(ex);
	}

	private static Set<Tuple> toTupleSet(List<redis.clients.jedis.resps.Tuple> source) {
		return TUPLE_SET_CONVERTER.convert(source);
	}

	private static ZParams toZParams(Aggregate aggregate, Weights weights) {
		return new ZParams().weights(weights.toArray()).aggregate(ZParams.Aggregate.valueOf(aggregate.name()));
	}

	@Contract("null -> null")
	private @Nullable static Tuple toTuple(@Nullable KeyValue<?, redis.clients.jedis.resps.Tuple> keyValue) {

		if (keyValue != null) {
			redis.clients.jedis.resps.Tuple tuple = keyValue.getValue();
			return tuple != null ? JedisConverters.toTuple(tuple) : null;
		}

		return null;
	}
}
