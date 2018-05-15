/*
 * Copyright 2017-2018 the original author or authors.
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
package org.springframework.data.redis.connection.lettuce;

import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.ScoredValueScanCursor;
import io.lettuce.core.ZStoreArgs;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.util.List;
import java.util.Set;

import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisZSetCommands;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.KeyBoundCursor;
import org.springframework.data.redis.core.ScanIteration;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.0
 */
@RequiredArgsConstructor
class LettuceZSetCommands implements RedisZSetCommands {

	private final @NonNull LettuceConnection connection;

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
				pipeline(connection.newLettuceResult(getAsyncConnection().zadd(key, score, value),
						LettuceConverters.longToBoolean()));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceResult(getAsyncConnection().zadd(key, score, value),
						LettuceConverters.longToBoolean()));
				return null;
			}
			return LettuceConverters.toBoolean(getConnection().zadd(key, score, value));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
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
						connection.newLettuceResult(getAsyncConnection().zadd(key, LettuceConverters.toObjects(tuples).toArray())));
				return null;
			}
			if (isQueueing()) {
				transaction(
						connection.newLettuceResult(getAsyncConnection().zadd(key, LettuceConverters.toObjects(tuples).toArray())));
				return null;
			}
			return getConnection().zadd(key, LettuceConverters.toObjects(tuples).toArray());
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
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
				pipeline(connection.newLettuceResult(getAsyncConnection().zrem(key, values)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceResult(getAsyncConnection().zrem(key, values)));
				return null;
			}
			return getConnection().zrem(key, values);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
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
				pipeline(connection.newLettuceResult(getAsyncConnection().zincrby(key, increment, value)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceResult(getAsyncConnection().zincrby(key, increment, value)));
				return null;
			}
			return getConnection().zincrby(key, increment, value);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
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
				pipeline(connection.newLettuceResult(getAsyncConnection().zrank(key, value)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceResult(getAsyncConnection().zrank(key, value)));
				return null;
			}
			return getConnection().zrank(key, value);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
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
				pipeline(connection.newLettuceResult(getAsyncConnection().zrevrank(key, value)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceResult(getAsyncConnection().zrevrank(key, value)));
				return null;
			}
			return getConnection().zrevrank(key, value);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
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
				pipeline(connection.newLettuceResult(getAsyncConnection().zrange(key, start, end),
						LettuceConverters.bytesListToBytesSet()));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceResult(getAsyncConnection().zrange(key, start, end),
						LettuceConverters.bytesListToBytesSet()));
				return null;
			}
			return LettuceConverters.toBytesSet(getConnection().zrange(key, start, end));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
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
				pipeline(connection.newLettuceResult(getAsyncConnection().zrangeWithScores(key, start, end),
						LettuceConverters.scoredValuesToTupleSet()));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceResult(getAsyncConnection().zrangeWithScores(key, start, end),
						LettuceConverters.scoredValuesToTupleSet()));
				return null;
			}
			return LettuceConverters.toTupleSet(getConnection().zrangeWithScores(key, start, end));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRangeByScoreWithScores(byte[], org.springframework.data.redis.connection.RedisZSetCommands.Range, org.springframework.data.redis.connection.RedisZSetCommands.Limit)
	 */
	@Override
	public Set<Tuple> zRangeByScoreWithScores(byte[] key, Range range, Limit limit) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(range, "Range for ZRANGEBYSCOREWITHSCORES must not be null!");
		Assert.notNull(limit, "Limit must not be null!");

		try {
			if (isPipelined()) {
				if (limit.isUnlimited()) {
					pipeline(connection.newLettuceResult(
							getAsyncConnection().zrangebyscoreWithScores(key, LettuceConverters.toRange(range)),
							LettuceConverters.scoredValuesToTupleSet()));
				} else {
					pipeline(connection.newLettuceResult(getAsyncConnection().zrangebyscoreWithScores(key,
							LettuceConverters.toRange(range), LettuceConverters.toLimit(limit)),
							LettuceConverters.scoredValuesToTupleSet()));
				}
				return null;
			}
			if (isQueueing()) {
				if (limit.isUnlimited()) {
					transaction(connection.newLettuceResult(
							getAsyncConnection().zrangebyscoreWithScores(key, LettuceConverters.toRange(range)),
							LettuceConverters.scoredValuesToTupleSet()));
				} else {
					transaction(connection.newLettuceResult(getAsyncConnection().zrangebyscoreWithScores(key,
							LettuceConverters.toRange(range), LettuceConverters.toLimit(limit)),
							LettuceConverters.scoredValuesToTupleSet()));
				}
				return null;
			}
			if (limit.isUnlimited()) {
				return LettuceConverters
						.toTupleSet(getConnection().zrangebyscoreWithScores(key, LettuceConverters.toRange(range)));
			}
			return LettuceConverters.toTupleSet(getConnection().zrangebyscoreWithScores(key, LettuceConverters.toRange(range),
					LettuceConverters.toLimit(limit)));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
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
				pipeline(connection.newLettuceResult(getAsyncConnection().zrevrange(key, start, end),
						LettuceConverters.bytesListToBytesSet()));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceResult(getAsyncConnection().zrevrange(key, start, end),
						LettuceConverters.bytesListToBytesSet()));
				return null;
			}
			return LettuceConverters.toBytesSet(getConnection().zrevrange(key, start, end));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
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
				pipeline(connection.newLettuceResult(getAsyncConnection().zrevrangeWithScores(key, start, end),
						LettuceConverters.scoredValuesToTupleSet()));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceResult(getAsyncConnection().zrevrangeWithScores(key, start, end),
						LettuceConverters.scoredValuesToTupleSet()));
				return null;
			}
			return LettuceConverters.toTupleSet(getConnection().zrevrangeWithScores(key, start, end));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRevRangeByScore(byte[], org.springframework.data.redis.connection.RedisZSetCommands.Range, org.springframework.data.redis.connection.RedisZSetCommands.Limit)
	 */
	@Override
	public Set<byte[]> zRevRangeByScore(byte[] key, Range range, Limit limit) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(range, "Range for ZREVRANGEBYSCORE must not be null!");
		Assert.notNull(limit, "Limit must not be null!");

		try {
			if (isPipelined()) {
				if (limit.isUnlimited()) {
					pipeline(
							connection.newLettuceResult(getAsyncConnection().zrevrangebyscore(key, LettuceConverters.toRange(range)),
									LettuceConverters.bytesListToBytesSet()));
				} else {
					pipeline(
							connection.newLettuceResult(getAsyncConnection().zrevrangebyscore(key, LettuceConverters.toRange(range),
									LettuceConverters.toLimit(limit)), LettuceConverters.bytesListToBytesSet()));
				}
				return null;
			}
			if (isQueueing()) {
				if (limit.isUnlimited()) {
					transaction(
							connection.newLettuceResult(getAsyncConnection().zrevrangebyscore(key, LettuceConverters.toRange(range)),
									LettuceConverters.bytesListToBytesSet()));
				} else {
					transaction(
							connection.newLettuceResult(getAsyncConnection().zrevrangebyscore(key, LettuceConverters.toRange(range),
									LettuceConverters.toLimit(limit)), LettuceConverters.bytesListToBytesSet()));
				}
				return null;
			}
			if (limit.isUnlimited()) {
				return LettuceConverters.toBytesSet(getConnection().zrevrangebyscore(key, LettuceConverters.toRange(range)));
			}
			return LettuceConverters.toBytesSet(
					getConnection().zrevrangebyscore(key, LettuceConverters.toRange(range), LettuceConverters.toLimit(limit)));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRevRangeByScoreWithScores(byte[], org.springframework.data.redis.connection.RedisZSetCommands.Range, org.springframework.data.redis.connection.RedisZSetCommands.Limit)
	 */
	@Override
	public Set<Tuple> zRevRangeByScoreWithScores(byte[] key, Range range, Limit limit) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(range, "Range for ZREVRANGEBYSCOREWITHSCORES must not be null!");
		Assert.notNull(limit, "Limit must not be null!");

		try {
			if (isPipelined()) {
				if (limit.isUnlimited()) {
					pipeline(connection.newLettuceResult(
							getAsyncConnection().zrevrangebyscoreWithScores(key, LettuceConverters.toRange(range)),
							LettuceConverters.scoredValuesToTupleSet()));
				} else {
					pipeline(connection.newLettuceResult(getAsyncConnection().zrevrangebyscoreWithScores(key,
							LettuceConverters.toRange(range), LettuceConverters.toLimit(limit)),
							LettuceConverters.scoredValuesToTupleSet()));
				}
				return null;
			}

			if (isQueueing()) {
				if (limit.isUnlimited()) {
					transaction(connection.newLettuceResult(
							getAsyncConnection().zrevrangebyscoreWithScores(key, LettuceConverters.toRange(range)),
							LettuceConverters.scoredValuesToTupleSet()));
				} else {
					transaction(connection.newLettuceResult(getAsyncConnection().zrevrangebyscoreWithScores(key,
							LettuceConverters.toRange(range), LettuceConverters.toLimit(limit)),
							LettuceConverters.scoredValuesToTupleSet()));
				}
				return null;
			}
			if (limit.isUnlimited()) {
				return LettuceConverters
						.toTupleSet(getConnection().zrevrangebyscoreWithScores(key, LettuceConverters.toRange(range)));
			}
			return LettuceConverters.toTupleSet(getConnection().zrevrangebyscoreWithScores(key,
					LettuceConverters.toRange(range), LettuceConverters.toLimit(limit)));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zCount(byte[], org.springframework.data.redis.connection.RedisZSetCommands.Range)
	 */
	@Override
	public Long zCount(byte[] key, Range range) {

		Assert.notNull(key, "Key must not be null!");

		try {
			if (isPipelined()) {
				pipeline(connection.newLettuceResult(getAsyncConnection().zcount(key, LettuceConverters.toRange(range))));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceResult(getAsyncConnection().zcount(key, LettuceConverters.toRange(range))));
				return null;
			}
			return getConnection().zcount(key, LettuceConverters.toRange(range));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
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
				pipeline(connection.newLettuceResult(getAsyncConnection().zcard(key)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceResult(getAsyncConnection().zcard(key)));
				return null;
			}
			return getConnection().zcard(key);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
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
				pipeline(connection.newLettuceResult(getAsyncConnection().zscore(key, value)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceResult(getAsyncConnection().zscore(key, value)));
				return null;
			}
			return getConnection().zscore(key, value);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
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
				pipeline(connection.newLettuceResult(getAsyncConnection().zremrangebyrank(key, start, end)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceResult(getAsyncConnection().zremrangebyrank(key, start, end)));
				return null;
			}
			return getConnection().zremrangebyrank(key, start, end);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
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

		try {
			if (isPipelined()) {
				pipeline(
						connection.newLettuceResult(getAsyncConnection().zremrangebyscore(key, LettuceConverters.toRange(range))));
				return null;
			}
			if (isQueueing()) {
				transaction(
						connection.newLettuceResult(getAsyncConnection().zremrangebyscore(key, LettuceConverters.toRange(range))));
				return null;
			}
			return getConnection().zremrangebyscore(key, LettuceConverters.toRange(range));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zUnionStore(byte[], org.springframework.data.redis.connection.RedisZSetCommands.Aggregate, org.springframework.data.redis.connection.RedisZSetCommands.Weights, byte[][])
	 */
	@Override
	public Long zUnionStore(byte[] destKey, Aggregate aggregate, Weights weights, byte[]... sets) {

		Assert.notNull(destKey, "Destination key must not be null!");
		Assert.notNull(sets, "Source sets must not be null!");
		Assert.noNullElements(sets, "Source sets must not contain null elements!");
		Assert.isTrue(weights.size() == sets.length, () -> String
				.format("The number of weights (%d) must match the number of source sets (%d)!", weights.size(), sets.length));

		ZStoreArgs storeArgs = zStoreArgs(aggregate, weights);

		try {
			if (isPipelined()) {
				pipeline(connection.newLettuceResult(getAsyncConnection().zunionstore(destKey, storeArgs, sets)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceResult(getAsyncConnection().zunionstore(destKey, storeArgs, sets)));
				return null;
			}
			return getConnection().zunionstore(destKey, storeArgs, sets);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
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
				pipeline(connection.newLettuceResult(getAsyncConnection().zunionstore(destKey, sets)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceResult(getAsyncConnection().zunionstore(destKey, sets)));
				return null;
			}
			return getConnection().zunionstore(destKey, sets);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
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

		ZStoreArgs storeArgs = zStoreArgs(aggregate, weights);

		try {
			if (isPipelined()) {
				pipeline(connection.newLettuceResult(getAsyncConnection().zinterstore(destKey, storeArgs, sets)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceResult(getAsyncConnection().zinterstore(destKey, storeArgs, sets)));
				return null;
			}
			return getConnection().zinterstore(destKey, storeArgs, sets);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
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
				pipeline(connection.newLettuceResult(getAsyncConnection().zinterstore(destKey, sets)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceResult(getAsyncConnection().zinterstore(destKey, sets)));
				return null;
			}
			return getConnection().zinterstore(destKey, sets);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
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
	public Cursor<Tuple> zScan(byte[] key, long cursorId, ScanOptions options) {

		Assert.notNull(key, "Key must not be null!");

		return new KeyBoundCursor<Tuple>(key, cursorId, options) {

			@Override
			protected ScanIteration<Tuple> doScan(byte[] key, long cursorId, ScanOptions options) {

				if (isQueueing() || isPipelined()) {
					throw new UnsupportedOperationException("'ZSCAN' cannot be called in pipeline / transaction mode.");
				}

				io.lettuce.core.ScanCursor scanCursor = connection.getScanCursor(cursorId);
				ScanArgs scanArgs = LettuceConverters.toScanArgs(options);

				ScoredValueScanCursor<byte[]> scoredValueScanCursor = getConnection().zscan(key, scanCursor, scanArgs);
				String nextCursorId = scoredValueScanCursor.getCursor();

				List<ScoredValue<byte[]>> result = scoredValueScanCursor.getValues();

				List<Tuple> values = connection.failsafeReadScanValues(result, LettuceConverters.scoredValuesToTupleList());
				return new ScanIteration<>(Long.valueOf(nextCursorId), values);
			}

			@Override
			protected void doClose() {
				LettuceZSetCommands.this.connection.close();
			}

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
			if (isPipelined()) {
				pipeline(connection.newLettuceResult(getAsyncConnection().zrangebyscore(key, min, max),
						LettuceConverters.bytesListToBytesSet()));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceResult(getAsyncConnection().zrangebyscore(key, min, max),
						LettuceConverters.bytesListToBytesSet()));
				return null;
			}
			return LettuceConverters.toBytesSet(getConnection().zrangebyscore(key, min, max));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRangeByScore(byte[], java.lang.String, java.lang.String, long, long)
	 */
	@Override
	public Set<byte[]> zRangeByScore(byte[] key, String min, String max, long offset, long count) {

		Assert.notNull(key, "Key must not be null!");

		try {
			if (isPipelined()) {
				pipeline(connection.newLettuceResult(getAsyncConnection().zrangebyscore(key, min, max, offset, count),
						LettuceConverters.bytesListToBytesSet()));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newLettuceResult(getAsyncConnection().zrangebyscore(key, min, max, offset, count),
						LettuceConverters.bytesListToBytesSet()));
				return null;
			}
			return LettuceConverters.toBytesSet(getConnection().zrangebyscore(key, min, max, offset, count));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRangeByScore(byte[], org.springframework.data.redis.connection.RedisZSetCommands.Range, org.springframework.data.redis.connection.RedisZSetCommands.Limit)
	 */
	@Override
	public Set<byte[]> zRangeByScore(byte[] key, Range range, Limit limit) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(range, "Range for ZRANGEBYSCORE must not be null!");
		Assert.notNull(limit, "Limit must not be null!");

		try {
			if (isPipelined()) {
				if (limit.isUnlimited()) {
					pipeline(
							connection.newLettuceResult(getAsyncConnection().zrangebyscore(key, LettuceConverters.toRange(range)),
									LettuceConverters.bytesListToBytesSet()));
				} else {
					pipeline(connection.newLettuceResult(getAsyncConnection().zrangebyscore(key, LettuceConverters.toRange(range),
							LettuceConverters.toLimit(limit)), LettuceConverters.bytesListToBytesSet()));
				}
				return null;
			}
			if (isQueueing()) {
				if (limit.isUnlimited()) {
					transaction(
							connection.newLettuceResult(getAsyncConnection().zrangebyscore(key, LettuceConverters.toRange(range)),
									LettuceConverters.bytesListToBytesSet()));
				} else {
					transaction(
							connection.newLettuceResult(getAsyncConnection().zrangebyscore(key, LettuceConverters.toRange(range),
									LettuceConverters.toLimit(limit)), LettuceConverters.bytesListToBytesSet()));
				}
				return null;
			}
			if (limit.isUnlimited()) {
				return LettuceConverters.toBytesSet(getConnection().zrangebyscore(key, LettuceConverters.toRange(range)));
			}
			return LettuceConverters.toBytesSet(
					getConnection().zrangebyscore(key, LettuceConverters.toRange(range), LettuceConverters.toLimit(limit)));
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zRangeByLex(byte[], org.springframework.data.redis.connection.RedisZSetCommands.Range, org.springframework.data.redis.connection.RedisZSetCommands.Limit)
	 */
	@Override
	public Set<byte[]> zRangeByLex(byte[] key, Range range, Limit limit) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(range, "Range for ZRANGEBYLEX must not be null!");
		Assert.notNull(limit, "Limit must not be null!");

		try {
			if (isPipelined()) {
				if (limit.isUnlimited()) {
					pipeline(connection.newLettuceResult(getAsyncConnection().zrangebylex(key, LettuceConverters.toRange(range)),
							LettuceConverters.bytesListToBytesSet()));
				} else {
					pipeline(connection.newLettuceResult(
							getAsyncConnection().zrangebylex(key, LettuceConverters.toRange(range), LettuceConverters.toLimit(limit)),
							LettuceConverters.bytesListToBytesSet()));
				}
				return null;
			}
			if (isQueueing()) {
				if (limit.isUnlimited()) {
					transaction(
							connection.newLettuceResult(getAsyncConnection().zrangebylex(key, LettuceConverters.toRange(range)),
									LettuceConverters.bytesListToBytesSet()));
				} else {
					transaction(connection.newLettuceResult(
							getAsyncConnection().zrangebylex(key, LettuceConverters.toRange(range), LettuceConverters.toLimit(limit)),
							LettuceConverters.bytesListToBytesSet()));
				}
				return null;
			}

			if (limit.isUnlimited()) {
				return LettuceConverters.bytesListToBytesSet()
						.convert(getConnection().zrangebylex(key, LettuceConverters.toRange(range)));
			}
			return LettuceConverters.bytesListToBytesSet().convert(
					getConnection().zrangebylex(key, LettuceConverters.toRange(range), LettuceConverters.toLimit(limit)));

		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	private boolean isPipelined() {
		return connection.isPipelined();
	}

	private boolean isQueueing() {
		return connection.isQueueing();
	}

	private void pipeline(LettuceResult result) {
		connection.pipeline(result);
	}

	private void transaction(LettuceResult result) {
		connection.transaction(result);
	}

	RedisClusterAsyncCommands<byte[], byte[]> getAsyncConnection() {
		return connection.getAsyncConnection();
	}

	public RedisClusterCommands<byte[], byte[]> getConnection() {
		return connection.getConnection();
	}

	private DataAccessException convertLettuceAccessException(Exception ex) {
		return connection.convertLettuceAccessException(ex);
	}

	private static ZStoreArgs zStoreArgs(@Nullable Aggregate aggregate, Weights weights) {

		ZStoreArgs args = new ZStoreArgs();

		if (aggregate != null) {
			switch (aggregate) {
				case MIN:
					args.min();
					break;
				case MAX:
					args.max();
					break;
				default:
					args.sum();
					break;
			}
		}

		args.weights(weights.toArray());

		return args;
	}

}
