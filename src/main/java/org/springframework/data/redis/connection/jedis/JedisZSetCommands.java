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

import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.ZParams;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.springframework.data.redis.connection.RedisZSetCommands;
import org.springframework.data.redis.connection.jedis.JedisConnection.JedisResult;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.KeyBoundCursor;
import org.springframework.data.redis.core.ScanIteration;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.util.Assert;

/**
 * @author Christoph Strobl
 * @since 2.0
 */
class JedisZSetCommands implements RedisZSetCommands {

	private final JedisConnection connection;

	public JedisZSetCommands(JedisConnection connection) {
		this.connection = connection;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zAdd(byte[], double, byte[])
	 */
	@Override
	public Boolean zAdd(byte[] key, double score, byte[] value) {

		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getPipeline().zadd(key, score, value),
						JedisConverters.longToBoolean()));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getTransaction().zadd(key, score, value),
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

		if (isPipelined() || isQueueing()) {
			throw new UnsupportedOperationException("zAdd of multiple fields not supported " + "in pipeline or transaction");
		}

		Map<byte[], Double> args = zAddArgs(tuples);
		try {
			return connection.getJedis().zadd(key, args);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zCard(byte[])
	 */
	@Override
	public Long zCard(byte[] key) {

		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getPipeline().zcard(key)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getTransaction().zcard(key)));
				return null;
			}
			return connection.getJedis().zcard(key);
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

		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getPipeline().zcount(key, min, max)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getTransaction().zcount(key, min, max)));
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

		if (isPipelined() || isQueueing()) {
			throw new UnsupportedOperationException(
					"ZCOUNT not implemented in jedis for binary protocol on transaction and pipeline");
		}

		byte[] min = JedisConverters.boundaryToBytesForZRange(range.getMin(), JedisConverters.NEGATIVE_INFINITY_BYTES);
		byte[] max = JedisConverters.boundaryToBytesForZRange(range.getMax(), JedisConverters.POSITIVE_INFINITY_BYTES);

		return connection.getJedis().zcount(key, min, max);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zIncrBy(byte[], double, byte[])
	 */
	@Override
	public Double zIncrBy(byte[] key, double increment, byte[] value) {

		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getPipeline().zincrby(key, increment, value)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getTransaction().zincrby(key, increment, value)));
				return null;
			}
			return connection.getJedis().zincrby(key, increment, value);
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

		try {
			ZParams zparams = new ZParams().weights(weights).aggregate(ZParams.Aggregate.valueOf(aggregate.name()));

			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getPipeline().zinterstore(destKey, zparams, sets)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getTransaction().zinterstore(destKey, zparams, sets)));
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

		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getPipeline().zinterstore(destKey, sets)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getTransaction().zinterstore(destKey, sets)));
				return null;
			}
			return connection.getJedis().zinterstore(destKey, sets);
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

		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getPipeline().zrange(key, start, end)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getTransaction().zrange(key, start, end)));
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

		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getPipeline().zrangeWithScores(key, start, end),
						JedisConverters.tupleSetToTupleSet()));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getTransaction().zrangeWithScores(key, start, end),
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
	* @see org.springframework.data.redis.connection.RedisZSetCommands#zRangeByLex(byte[], org.springframework.data.redis.connection.RedisZSetCommands.Range, org.springframework.data.redis.connection.RedisZSetCommands.Limit)
	*/
	public Set<byte[]> zRangeByLex(byte[] key, Range range, Limit limit) {

		Assert.notNull(range, "Range cannot be null for ZRANGEBYLEX.");

		byte[] min = JedisConverters.boundaryToBytesForZRangeByLex(range.getMin(), JedisConverters.MINUS_BYTES);
		byte[] max = JedisConverters.boundaryToBytesForZRangeByLex(range.getMax(), JedisConverters.PLUS_BYTES);

		try {
			if (isPipelined()) {
				if (limit != null) {
					pipeline(connection.newJedisResult(
							connection.getPipeline().zrangeByLex(key, min, max, limit.getOffset(), limit.getCount())));
				} else {
					pipeline(connection.newJedisResult(connection.getPipeline().zrangeByLex(key, min, max)));
				}
				return null;
			}

			if (isQueueing()) {
				if (limit != null) {
					transaction(connection.newJedisResult(
							connection.getTransaction().zrangeByLex(key, min, max, limit.getOffset(), limit.getCount())));
				} else {
					transaction(connection.newJedisResult(connection.getTransaction().zrangeByLex(key, min, max)));
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

	/*
	* (non-Javadoc)
	* @see org.springframework.data.redis.connection.RedisZSetCommands#zRangeByScore(byte[], org.springframework.data.redis.connection.RedisZSetCommands.Range, org.springframework.data.redis.connection.RedisZSetCommands.Limit)
	*/
	@Override
	public Set<byte[]> zRangeByScore(byte[] key, Range range, Limit limit) {

		Assert.notNull(range, "Range cannot be null for ZRANGEBYSCORE.");

		byte[] min = JedisConverters.boundaryToBytesForZRange(range.getMin(), JedisConverters.NEGATIVE_INFINITY_BYTES);
		byte[] max = JedisConverters.boundaryToBytesForZRange(range.getMax(), JedisConverters.POSITIVE_INFINITY_BYTES);

		try {
			if (isPipelined()) {
				if (limit != null) {
					pipeline(connection.newJedisResult(
							connection.getPipeline().zrangeByScore(key, min, max, limit.getOffset(), limit.getCount())));
				} else {
					pipeline(connection.newJedisResult(connection.getPipeline().zrangeByScore(key, min, max)));
				}
				return null;
			}

			if (isQueueing()) {
				if (limit != null) {
					transaction(connection.newJedisResult(
							connection.getTransaction().zrangeByScore(key, min, max, limit.getOffset(), limit.getCount())));
				} else {
					transaction(connection.newJedisResult(connection.getTransaction().zrangeByScore(key, min, max)));
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
	* @see org.springframework.data.redis.connection.RedisZSetCommands#zRangeByScoreWithScores(byte[], org.springframework.data.redis.connection.RedisZSetCommands.Range, org.springframework.data.redis.connection.RedisZSetCommands.Limit)
	*/
	@Override
	public Set<Tuple> zRangeByScoreWithScores(byte[] key, Range range, Limit limit) {

		Assert.notNull(range, "Range cannot be null for ZRANGEBYSCOREWITHSCORES.");

		byte[] min = JedisConverters.boundaryToBytesForZRange(range.getMin(), JedisConverters.NEGATIVE_INFINITY_BYTES);
		byte[] max = JedisConverters.boundaryToBytesForZRange(range.getMax(), JedisConverters.POSITIVE_INFINITY_BYTES);

		try {
			if (isPipelined()) {
				if (limit != null) {
					pipeline(connection.newJedisResult(
							connection.getPipeline().zrangeByScoreWithScores(key, min, max, limit.getOffset(), limit.getCount()),
							JedisConverters.tupleSetToTupleSet()));
				} else {
					pipeline(connection.newJedisResult(connection.getPipeline().zrangeByScoreWithScores(key, min, max),
							JedisConverters.tupleSetToTupleSet()));
				}
				return null;
			}

			if (isQueueing()) {
				if (limit != null) {
					transaction(connection.newJedisResult(
							connection.getTransaction().zrangeByScoreWithScores(key, min, max, limit.getOffset(), limit.getCount()),
							JedisConverters.tupleSetToTupleSet()));
				} else {
					transaction(connection.newJedisResult(connection.getTransaction().zrangeByScoreWithScores(key, min, max),
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
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRevRangeWithScores(byte[], long, long)
	 */
	@Override
	public Set<Tuple> zRevRangeWithScores(byte[] key, long start, long end) {

		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getPipeline().zrevrangeWithScores(key, start, end),
						JedisConverters.tupleSetToTupleSet()));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getTransaction().zrevrangeWithScores(key, start, end),
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
	public Set<byte[]> zRevRangeByScore(byte[] key, Range range, Limit limit) {

		Assert.notNull(range, "Range cannot be null for ZREVRANGEBYSCORE.");

		byte[] min = JedisConverters.boundaryToBytesForZRange(range.getMin(), JedisConverters.NEGATIVE_INFINITY_BYTES);
		byte[] max = JedisConverters.boundaryToBytesForZRange(range.getMax(), JedisConverters.POSITIVE_INFINITY_BYTES);

		try {
			if (isPipelined()) {
				if (limit != null) {
					pipeline(connection.newJedisResult(
							connection.getPipeline().zrevrangeByScore(key, max, min, limit.getOffset(), limit.getCount())));
				} else {
					pipeline(connection.newJedisResult(connection.getPipeline().zrevrangeByScore(key, max, min)));
				}
				return null;
			}

			if (isQueueing()) {
				if (limit != null) {
					transaction(connection.newJedisResult(
							connection.getTransaction().zrevrangeByScore(key, max, min, limit.getOffset(), limit.getCount())));
				} else {
					transaction(connection.newJedisResult(connection.getTransaction().zrevrangeByScore(key, max, min)));
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
	public Set<Tuple> zRevRangeByScoreWithScores(byte[] key, Range range, Limit limit) {

		Assert.notNull(range, "Range cannot be null for ZREVRANGEBYSCOREWITHSCORES.");

		byte[] min = JedisConverters.boundaryToBytesForZRange(range.getMin(), JedisConverters.NEGATIVE_INFINITY_BYTES);
		byte[] max = JedisConverters.boundaryToBytesForZRange(range.getMax(), JedisConverters.POSITIVE_INFINITY_BYTES);

		try {
			if (isPipelined()) {
				if (limit != null) {
					pipeline(connection.newJedisResult(
							connection.getPipeline().zrevrangeByScoreWithScores(key, max, min, limit.getOffset(), limit.getCount()),
							JedisConverters.tupleSetToTupleSet()));
				} else {
					pipeline(connection.newJedisResult(connection.getPipeline().zrevrangeByScoreWithScores(key, max, min),
							JedisConverters.tupleSetToTupleSet()));
				}
				return null;
			}

			if (isQueueing()) {
				if (limit != null) {
					transaction(connection.newJedisResult(connection.getTransaction().zrevrangeByScoreWithScores(key, max, min,
							limit.getOffset(), limit.getCount()), JedisConverters.tupleSetToTupleSet()));
				} else {
					transaction(connection.newJedisResult(connection.getTransaction().zrevrangeByScoreWithScores(key, max, min),
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
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRank(byte[], byte[])
	 */
	@Override
	public Long zRank(byte[] key, byte[] value) {

		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getPipeline().zrank(key, value)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getTransaction().zrank(key, value)));
				return null;
			}
			return connection.getJedis().zrank(key, value);
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

		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getPipeline().zrem(key, values)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getTransaction().zrem(key, values)));
				return null;
			}
			return connection.getJedis().zrem(key, values);
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

		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getPipeline().zremrangeByRank(key, start, end)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getTransaction().zremrangeByRank(key, start, end)));
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

		Assert.notNull(range, "Range cannot be null for ZREMRANGEBYSCORE.");

		byte[] min = JedisConverters.boundaryToBytesForZRange(range.getMin(), JedisConverters.NEGATIVE_INFINITY_BYTES);
		byte[] max = JedisConverters.boundaryToBytesForZRange(range.getMax(), JedisConverters.POSITIVE_INFINITY_BYTES);

		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getPipeline().zremrangeByScore(key, min, max)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getTransaction().zremrangeByScore(key, min, max)));
				return null;
			}
			return connection.getJedis().zremrangeByScore(key, min, max);
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

		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getPipeline().zrevrange(key, start, end)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getTransaction().zrevrange(key, start, end)));
				return null;
			}
			return connection.getJedis().zrevrange(key, start, end);
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

		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getPipeline().zrevrank(key, value)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getTransaction().zrevrank(key, value)));
				return null;
			}
			return connection.getJedis().zrevrank(key, value);
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

		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getPipeline().zscore(key, value)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getTransaction().zscore(key, value)));
				return null;
			}
			return connection.getJedis().zscore(key, value);
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

		try {
			ZParams zparams = new ZParams().weights(weights).aggregate(ZParams.Aggregate.valueOf(aggregate.name()));

			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getPipeline().zunionstore(destKey, zparams, sets)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getTransaction().zunionstore(destKey, zparams, sets)));
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

		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getPipeline().zunionstore(destKey, sets)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getTransaction().zunionstore(destKey, sets)));
				return null;
			}
			return connection.getJedis().zunionstore(destKey, sets);
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

		try {
			String keyStr = new String(key, "UTF-8");
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getPipeline().zrangeByScore(keyStr, min, max)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getTransaction().zrangeByScore(keyStr, min, max)));
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

		if (offset > Integer.MAX_VALUE || count > Integer.MAX_VALUE) {

			throw new IllegalArgumentException(
					"Offset and count must be less than Integer.MAX_VALUE for zRangeByScore in Jedis.");
		}

		try {
			String keyStr = new String(key, "UTF-8");
			if (isPipelined()) {
				pipeline(connection
						.newJedisResult(connection.getPipeline().zrangeByScore(keyStr, min, max, (int) offset, (int) count)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection
						.newJedisResult(connection.getTransaction().zrangeByScore(keyStr, min, max, (int) offset, (int) count)));
				return null;
			}
			return JedisConverters.stringSetToByteSet()
					.convert(connection.getJedis().zrangeByScore(keyStr, min, max, (int) offset, (int) count));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	private Map<byte[], Double> zAddArgs(Set<Tuple> tuples) {

		Map<byte[], Double> args = new LinkedHashMap<>(tuples.size(), 1);
		Set<Double> scores = new HashSet<Double>(tuples.size(), 1);

		boolean isAtLeastJedis24 = JedisVersionUtil.atLeastJedis24();

		for (Tuple tuple : tuples) {

			if (!isAtLeastJedis24) {
				if (scores.contains(tuple.getScore())) {
					throw new UnsupportedOperationException(
							"Bulk add of multiple elements with the same score is not supported. Add the elements individually.");
				}
				scores.add(tuple.getScore());
			}

			args.put(tuple.getValue(), tuple.getScore());
		}

		return args;
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
