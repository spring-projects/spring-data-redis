/*
 * Copyright 2017 the original author or authors.
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
package org.springframework.data.redis.connection.jedis;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.ZParams;

import java.nio.charset.StandardCharsets;
import java.util.Set;

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
 * @since 2.0
 */
@RequiredArgsConstructor
class JedisZSetCommands implements RedisZSetCommands {

	private final @NonNull JedisConnection connection;

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zAdd(byte[], double, byte[])
	 */
	@Override
	public Boolean zAdd(byte[] key, double score, byte[] value) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(value, "Value must not be null!");

		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getRequiredPipeline().zadd(key, score, value),
						JedisConverters.longToBoolean()));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getRequiredTransaction().zadd(key, score, value),
						JedisConverters.longToBoolean()));
				return null;
			}
			return JedisConverters.toBoolean(connection.getJedis().zadd(key, score, value));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zAdd(byte[], java.util.Set)
	 */
	@Override
	public Long zAdd(byte[] key, Set<Tuple> tuples) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(tuples, "Tuples must not be null!");

		try {
			if (isPipelined()) {
				pipeline(
						connection.newJedisResult(connection.getRequiredPipeline().zadd(key, JedisConverters.toTupleMap(tuples))));
				return null;
			}
			if (isQueueing()) {
				transaction(connection
						.newJedisResult(connection.getRequiredTransaction().zadd(key, JedisConverters.toTupleMap(tuples))));
				return null;
			}
			return connection.getJedis().zadd(key, JedisConverters.toTupleMap(tuples));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
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

		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getRequiredPipeline().zrem(key, values)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getRequiredTransaction().zrem(key, values)));
				return null;
			}
			return connection.getJedis().zrem(key, values);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zIncrBy(byte[], double, byte[])
	 */
	@Override
	public Double zIncrBy(byte[] key, double increment, byte[] value) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(value, "Value must not be null!");

		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getRequiredPipeline().zincrby(key, increment, value)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getRequiredTransaction().zincrby(key, increment, value)));
				return null;
			}
			return connection.getJedis().zincrby(key, increment, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRank(byte[], byte[])
	 */
	@Override
	public Long zRank(byte[] key, byte[] value) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(value, "Value must not be null!");

		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getRequiredPipeline().zrank(key, value)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getRequiredTransaction().zrank(key, value)));
				return null;
			}
			return connection.getJedis().zrank(key, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRevRank(byte[], byte[])
	 */
	@Override
	public Long zRevRank(byte[] key, byte[] value) {

		Assert.notNull(key, "Key must not be null!");

		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getRequiredPipeline().zrevrank(key, value)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getRequiredTransaction().zrevrank(key, value)));
				return null;
			}
			return connection.getJedis().zrevrank(key, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRange(byte[], long, long)
	 */
	@Override
	public Set<byte[]> zRange(byte[] key, long start, long end) {

		Assert.notNull(key, "Key must not be null!");

		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getRequiredPipeline().zrange(key, start, end)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getRequiredTransaction().zrange(key, start, end)));
				return null;
			}
			return connection.getJedis().zrange(key, start, end);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRangeWithScores(byte[], long, long)
	 */
	@Override
	public Set<Tuple> zRangeWithScores(byte[] key, long start, long end) {

		Assert.notNull(key, "Key must not be null!");

		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getRequiredPipeline().zrangeWithScores(key, start, end),
						JedisConverters.tupleSetToTupleSet()));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getRequiredTransaction().zrangeWithScores(key, start, end),
						JedisConverters.tupleSetToTupleSet()));
				return null;
			}
			return JedisConverters.toTupleSet(connection.getJedis().zrangeWithScores(key, start, end));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRangeByScoreWithScores(byte[], org.springframework.data.redis.connection.RedisZSetCommands.Range, org.springframework.data.redis.connection.RedisZSetCommands.Limit)
	 */
	@Override
	public Set<Tuple> zRangeByScoreWithScores(byte[] key, Range range, @Nullable Limit limit) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(range, "Range for ZRANGEBYSCOREWITHSCORES must not be null!");

		byte[] min = JedisConverters.boundaryToBytesForZRange(range.getMin(), JedisConverters.NEGATIVE_INFINITY_BYTES);
		byte[] max = JedisConverters.boundaryToBytesForZRange(range.getMax(), JedisConverters.POSITIVE_INFINITY_BYTES);

		try {
			if (isPipelined()) {
				if (limit != null) {
					pipeline(connection.newJedisResult(connection.getRequiredPipeline().zrangeByScoreWithScores(key, min, max,
							limit.getOffset(), limit.getCount()), JedisConverters.tupleSetToTupleSet()));
				} else {
					pipeline(connection.newJedisResult(connection.getRequiredPipeline().zrangeByScoreWithScores(key, min, max),
							JedisConverters.tupleSetToTupleSet()));
				}
				return null;
			}

			if (isQueueing()) {
				if (limit != null) {
					transaction(connection.newJedisResult(connection.getRequiredTransaction().zrangeByScoreWithScores(key, min,
							max, limit.getOffset(), limit.getCount()), JedisConverters.tupleSetToTupleSet()));
				} else {
					transaction(
							connection.newJedisResult(connection.getRequiredTransaction().zrangeByScoreWithScores(key, min, max),
									JedisConverters.tupleSetToTupleSet()));
				}
				return null;
			}

			if (limit != null) {
				return JedisConverters.toTupleSet(
						connection.getJedis().zrangeByScoreWithScores(key, min, max, limit.getOffset(), limit.getCount()));
			}
			return JedisConverters.toTupleSet(connection.getJedis().zrangeByScoreWithScores(key, min, max));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRevRange(byte[], long, long)
	 */
	@Override
	public Set<byte[]> zRevRange(byte[] key, long start, long end) {

		Assert.notNull(key, "Key must not be null!");

		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getRequiredPipeline().zrevrange(key, start, end)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getRequiredTransaction().zrevrange(key, start, end)));
				return null;
			}
			return connection.getJedis().zrevrange(key, start, end);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRevRangeWithScores(byte[], long, long)
	 */
	@Override
	public Set<Tuple> zRevRangeWithScores(byte[] key, long start, long end) {

		Assert.notNull(key, "Key must not be null!");

		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getRequiredPipeline().zrevrangeWithScores(key, start, end),
						JedisConverters.tupleSetToTupleSet()));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getRequiredTransaction().zrevrangeWithScores(key, start, end),
						JedisConverters.tupleSetToTupleSet()));
				return null;
			}
			return JedisConverters.toTupleSet(connection.getJedis().zrevrangeWithScores(key, start, end));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRevRangeByScore(byte[], org.springframework.data.redis.connection.RedisZSetCommands.Range, org.springframework.data.redis.connection.RedisZSetCommands.Limit)
	 */
	@Override
	public Set<byte[]> zRevRangeByScore(byte[] key, Range range, @Nullable Limit limit) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(range, "Range for ZREVRANGEBYSCORE must not be null!");

		byte[] min = JedisConverters.boundaryToBytesForZRange(range.getMin(), JedisConverters.NEGATIVE_INFINITY_BYTES);
		byte[] max = JedisConverters.boundaryToBytesForZRange(range.getMax(), JedisConverters.POSITIVE_INFINITY_BYTES);

		try {
			if (isPipelined()) {
				if (limit != null) {
					pipeline(connection.newJedisResult(
							connection.getRequiredPipeline().zrevrangeByScore(key, max, min, limit.getOffset(), limit.getCount())));
				} else {
					pipeline(connection.newJedisResult(connection.getRequiredPipeline().zrevrangeByScore(key, max, min)));
				}
				return null;
			}

			if (isQueueing()) {
				if (limit != null) {
					transaction(connection.newJedisResult(connection.getRequiredTransaction().zrevrangeByScore(key, max, min,
							limit.getOffset(), limit.getCount())));
				} else {
					transaction(connection.newJedisResult(connection.getRequiredTransaction().zrevrangeByScore(key, max, min)));
				}
				return null;
			}

			if (limit != null) {
				return connection.getJedis().zrevrangeByScore(key, max, min, limit.getOffset(), limit.getCount());
			}
			return connection.getJedis().zrevrangeByScore(key, max, min);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRevRangeByScoreWithScores(byte[], org.springframework.data.redis.connection.RedisZSetCommands.Range, org.springframework.data.redis.connection.RedisZSetCommands.Limit)
	 */
	@Override
	public Set<Tuple> zRevRangeByScoreWithScores(byte[] key, Range range, @Nullable Limit limit) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(range, "Range for ZREVRANGEBYSCOREWITHSCORES must not be null!");

		byte[] min = JedisConverters.boundaryToBytesForZRange(range.getMin(), JedisConverters.NEGATIVE_INFINITY_BYTES);
		byte[] max = JedisConverters.boundaryToBytesForZRange(range.getMax(), JedisConverters.POSITIVE_INFINITY_BYTES);

		try {
			if (isPipelined()) {
				if (limit != null) {
					pipeline(connection.newJedisResult(connection.getRequiredPipeline().zrevrangeByScoreWithScores(key, max, min,
							limit.getOffset(), limit.getCount()), JedisConverters.tupleSetToTupleSet()));
				} else {
					pipeline(connection.newJedisResult(connection.getRequiredPipeline().zrevrangeByScoreWithScores(key, max, min),
							JedisConverters.tupleSetToTupleSet()));
				}
				return null;
			}

			if (isQueueing()) {
				if (limit != null) {
					transaction(connection.newJedisResult(connection.getRequiredTransaction().zrevrangeByScoreWithScores(key, max,
							min, limit.getOffset(), limit.getCount()), JedisConverters.tupleSetToTupleSet()));
				} else {
					transaction(
							connection.newJedisResult(connection.getRequiredTransaction().zrevrangeByScoreWithScores(key, max, min),
									JedisConverters.tupleSetToTupleSet()));
				}
				return null;
			}

			if (limit != null) {
				return JedisConverters.toTupleSet(
						connection.getJedis().zrevrangeByScoreWithScores(key, max, min, limit.getOffset(), limit.getCount()));
			}
			return JedisConverters.toTupleSet(connection.getJedis().zrevrangeByScoreWithScores(key, max, min));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zCount(byte[], double, double)
	 */
	@Override
	public Long zCount(byte[] key, double min, double max) {

		Assert.notNull(key, "Key must not be null!");

		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getRequiredPipeline().zcount(key, min, max)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getRequiredTransaction().zcount(key, min, max)));
				return null;
			}
			return connection.getJedis().zcount(key, min, max);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zCount(byte[], org.springframework.data.redis.connection.RedisZSetCommands.Range)
	 */
	@Override
	public Long zCount(byte[] key, Range range) {

		Assert.notNull(key, "Key must not be null!");

		if (isPipelined() || isQueueing()) {
			throw new UnsupportedOperationException(
					"ZCOUNT not implemented in jedis for binary protocol on transaction and pipeline");
		}

		// TODO: Implement zcount for pipeline/tx.
		byte[] min = JedisConverters.boundaryToBytesForZRange(range.getMin(), JedisConverters.NEGATIVE_INFINITY_BYTES);
		byte[] max = JedisConverters.boundaryToBytesForZRange(range.getMax(), JedisConverters.POSITIVE_INFINITY_BYTES);
		return connection.getJedis().zcount(key, min, max);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zCard(byte[])
	 */
	@Override
	public Long zCard(byte[] key) {

		Assert.notNull(key, "Key must not be null!");

		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getRequiredPipeline().zcard(key)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getRequiredTransaction().zcard(key)));
				return null;
			}
			return connection.getJedis().zcard(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zScore(byte[], byte[])
	 */
	@Override
	public Double zScore(byte[] key, byte[] value) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(value, "Value must not be null!");

		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getRequiredPipeline().zscore(key, value)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getRequiredTransaction().zscore(key, value)));
				return null;
			}
			return connection.getJedis().zscore(key, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRemRange(byte[], long, long)
	 */
	@Override
	public Long zRemRange(byte[] key, long start, long end) {

		Assert.notNull(key, "Key must not be null!");

		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getRequiredPipeline().zremrangeByRank(key, start, end)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getRequiredTransaction().zremrangeByRank(key, start, end)));
				return null;
			}
			return connection.getJedis().zremrangeByRank(key, start, end);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
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

		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getRequiredPipeline().zremrangeByScore(key, min, max)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getRequiredTransaction().zremrangeByScore(key, min, max)));
				return null;
			}
			return connection.getJedis().zremrangeByScore(key, min, max);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zUnionStore(byte[], org.springframework.data.redis.connection.RedisZSetCommands.Aggregate, int[], byte[][])
	 */
	@Override
	public Long zUnionStore(byte[] destKey, Aggregate aggregate, int[] weights, byte[]... sets) {

		Assert.notNull(destKey, "Destination key must not be null!");
		Assert.notNull(sets, "Source sets must not be null!");
		Assert.noNullElements(sets, "Source sets must not contain null elements!");

		try {
			ZParams zparams = new ZParams().weights(weights).aggregate(ZParams.Aggregate.valueOf(aggregate.name()));

			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getRequiredPipeline().zunionstore(destKey, zparams, sets)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getRequiredTransaction().zunionstore(destKey, zparams, sets)));
				return null;
			}
			return connection.getJedis().zunionstore(destKey, zparams, sets);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
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

		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getRequiredPipeline().zunionstore(destKey, sets)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getRequiredTransaction().zunionstore(destKey, sets)));
				return null;
			}
			return connection.getJedis().zunionstore(destKey, sets);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zInterStore(byte[], org.springframework.data.redis.connection.RedisZSetCommands.Aggregate, int[], byte[][])
	 */
	@Override
	public Long zInterStore(byte[] destKey, Aggregate aggregate, int[] weights, byte[]... sets) {

		Assert.notNull(destKey, "Destination key must not be null!");
		Assert.notNull(sets, "Source sets must not be null!");
		Assert.noNullElements(sets, "Source sets must not contain null elements!");

		try {
			ZParams zparams = new ZParams().weights(weights).aggregate(ZParams.Aggregate.valueOf(aggregate.name()));

			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getRequiredPipeline().zinterstore(destKey, zparams, sets)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getRequiredTransaction().zinterstore(destKey, zparams, sets)));
				return null;
			}
			return connection.getJedis().zinterstore(destKey, zparams, sets);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
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

		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getRequiredPipeline().zinterstore(destKey, sets)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getRequiredTransaction().zinterstore(destKey, sets)));
				return null;
			}
			return connection.getJedis().zinterstore(destKey, sets);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
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
				return new ScanIteration<>(Long.valueOf(result.getStringCursor()),
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

		try {
			String keyStr = new String(key, StandardCharsets.UTF_8);
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getRequiredPipeline().zrangeByScore(keyStr, min, max)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getRequiredTransaction().zrangeByScore(keyStr, min, max)));
				return null;
			}
			return JedisConverters.stringSetToByteSet().convert(connection.getJedis().zrangeByScore(keyStr, min, max));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
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

		try {
			String keyStr = new String(key, StandardCharsets.UTF_8);
			if (isPipelined()) {
				pipeline(connection.newJedisResult(
						connection.getRequiredPipeline().zrangeByScore(keyStr, min, max, (int) offset, (int) count)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(
						connection.getRequiredTransaction().zrangeByScore(keyStr, min, max, (int) offset, (int) count)));
				return null;
			}
			return JedisConverters.stringSetToByteSet()
					.convert(connection.getJedis().zrangeByScore(keyStr, min, max, (int) offset, (int) count));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRangeByScore(byte[], org.springframework.data.redis.connection.RedisZSetCommands.Range, org.springframework.data.redis.connection.RedisZSetCommands.Limit)
	 */
	@Override
	public Set<byte[]> zRangeByScore(byte[] key, Range range, @Nullable Limit limit) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(range, "Range for ZRANGEBYSCORE must not be null!");

		byte[] min = JedisConverters.boundaryToBytesForZRange(range.getMin(), JedisConverters.NEGATIVE_INFINITY_BYTES);
		byte[] max = JedisConverters.boundaryToBytesForZRange(range.getMax(), JedisConverters.POSITIVE_INFINITY_BYTES);

		try {
			if (isPipelined()) {
				if (limit != null) {
					pipeline(connection.newJedisResult(
							connection.getRequiredPipeline().zrangeByScore(key, min, max, limit.getOffset(), limit.getCount())));
				} else {
					pipeline(connection.newJedisResult(connection.getRequiredPipeline().zrangeByScore(key, min, max)));
				}
				return null;
			}

			if (isQueueing()) {
				if (limit != null) {
					transaction(connection.newJedisResult(
							connection.getRequiredTransaction().zrangeByScore(key, min, max, limit.getOffset(), limit.getCount())));
				} else {
					transaction(connection.newJedisResult(connection.getRequiredTransaction().zrangeByScore(key, min, max)));
				}
				return null;
			}

			if (limit != null) {
				return connection.getJedis().zrangeByScore(key, min, max, limit.getOffset(), limit.getCount());
			}
			return connection.getJedis().zrangeByScore(key, min, max);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRangeByLex(byte[], org.springframework.data.redis.connection.RedisZSetCommands.Range, org.springframework.data.redis.connection.RedisZSetCommands.Limit)
	 */
	@Override
	public Set<byte[]> zRangeByLex(byte[] key, Range range, @Nullable Limit limit) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(range, "Range for ZRANGEBYLEX must not be null!");

		byte[] min = JedisConverters.boundaryToBytesForZRangeByLex(range.getMin(), JedisConverters.MINUS_BYTES);
		byte[] max = JedisConverters.boundaryToBytesForZRangeByLex(range.getMax(), JedisConverters.PLUS_BYTES);

		try {
			if (isPipelined()) {
				if (limit != null) {
					pipeline(connection.newJedisResult(
							connection.getRequiredPipeline().zrangeByLex(key, min, max, limit.getOffset(), limit.getCount())));
				} else {
					pipeline(connection.newJedisResult(connection.getRequiredPipeline().zrangeByLex(key, min, max)));
				}
				return null;
			}

			if (isQueueing()) {
				if (limit != null) {
					transaction(connection.newJedisResult(
							connection.getRequiredTransaction().zrangeByLex(key, min, max, limit.getOffset(), limit.getCount())));
				} else {
					transaction(connection.newJedisResult(connection.getRequiredTransaction().zrangeByLex(key, min, max)));
				}
				return null;
			}

			if (limit != null) {
				return connection.getJedis().zrangeByLex(key, min, max, limit.getOffset(), limit.getCount());
			}
			return connection.getJedis().zrangeByLex(key, min, max);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	private boolean isPipelined() {
		return connection.isPipelined();
	}

	private void pipeline(JedisResult result) {
		connection.pipeline(result);
	}

	private boolean isQueueing() {
		return connection.isQueueing();
	}

	private void transaction(JedisResult result) {
		connection.transaction(result);
	}

	private RuntimeException convertJedisAccessException(Exception ex) {
		return connection.convertJedisAccessException(ex);
	}
}
