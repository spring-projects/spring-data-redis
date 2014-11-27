/*
 * Copyright 2011-2014 the original author or authors.
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
package org.springframework.data.redis.connection;

import java.util.Set;

import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;

/**
 * ZSet(SortedSet)-specific commands supported by Redis.
 * 
 * @author Costin Leau
 * @author Christoph Strobl
 * @author Thomas Darimont
 * @author David Liu
 */
public interface RedisZSetCommands {

	/**
	 * Sort aggregation operations.
	 */
	public enum Aggregate {
		SUM, MIN, MAX;
	}

	/**
	 * ZSet tuple.
	 */
	public interface Tuple extends Comparable<Double> {

		byte[] getValue();

		Double getScore();
	}

	/**
	 * Add {@code value} to a sorted set at {@code key}, or update its {@code score} if it already exists.
	 * <p>
	 * See http://redis.io/commands/zadd
	 * 
	 * @param key
	 * @param score
	 * @param value
	 * @return
	 */
	Boolean zAdd(byte[] key, double score, byte[] value);

	/**
	 * Add {@code tuples} to a sorted set at {@code key}, or update its {@code score} if it already exists.
	 * <p>
	 * See http://redis.io/commands/zadd
	 * 
	 * @param key
	 * @param tuples
	 * @return
	 */
	Long zAdd(byte[] key, Set<Tuple> tuples);

	/**
	 * Remove {@code values} from sorted set. Return number of removed elements.
	 * <p>
	 * See http://redis.io/commands/zrem
	 * 
	 * @param key
	 * @param values
	 * @return
	 */
	Long zRem(byte[] key, byte[]... values);

	/**
	 * Increment the score of element with {@code value} in sorted set by {@code increment}.
	 * <p>
	 * See http://redis.io/commands/zincrby
	 * 
	 * @param key
	 * @param increment
	 * @param value
	 * @return
	 */
	Double zIncrBy(byte[] key, double increment, byte[] value);

	/**
	 * Determine the index of element with {@code value} in a sorted set.
	 * <p>
	 * See http://redis.io/commands/zrank
	 * 
	 * @param key
	 * @param value
	 * @return
	 */
	Long zRank(byte[] key, byte[] value);

	/**
	 * Determine the index of element with {@code value} in a sorted set when scored high to low.
	 * <p>
	 * See http://redis.io/commands/zrevrank
	 * 
	 * @param key
	 * @param value
	 * @return
	 */
	Long zRevRank(byte[] key, byte[] value);

	/**
	 * Get elements between {@code begin} and {@code end} from sorted set.
	 * <p>
	 * See http://redis.io/commands/zrange
	 * 
	 * @param key
	 * @param begin
	 * @param end
	 * @return
	 */
	Set<byte[]> zRange(byte[] key, long begin, long end);

	/**
	 * Get set of {@link Tuple}s between {@code begin} and {@code end} from sorted set.
	 * <p>
	 * See http://redis.io/commands/zrange
	 * 
	 * @param key
	 * @param begin
	 * @param end
	 * @return
	 */
	Set<Tuple> zRangeWithScores(byte[] key, long begin, long end);

	/**
	 * Get elements where score is between {@code min} and {@code max} from sorted set.
	 * <p>
	 * See http://redis.io/commands/zrangebyscore
	 * 
	 * @param key
	 * @param min
	 * @param max
	 * @return
	 */
	Set<byte[]> zRangeByScore(byte[] key, double min, double max);

	/**
	 * Get set of {@link Tuple}s where score is between {@code min} and {@code max} from sorted set.
	 * <p>
	 * See http://redis.io/commands/zrangebyscore
	 * 
	 * @param key
	 * @param min
	 * @param max
	 * @return
	 */
	Set<Tuple> zRangeByScoreWithScores(byte[] key, double min, double max);

	/**
	 * Get elements in range from {@code begin} to {@code end} where score is between {@code min} and {@code max} from
	 * sorted set.
	 * <p>
	 * See http://redis.io/commands/zrangebyscore
	 * 
	 * @param key
	 * @param min
	 * @param max
	 * @param offset
	 * @param count
	 * @return
	 */
	Set<byte[]> zRangeByScore(byte[] key, double min, double max, long offset, long count);

	/**
	 * Get set of {@link Tuple}s in range from {@code begin} to {@code end} where score is between {@code min} and
	 * {@code max} from sorted set.
	 * <p>
	 * See http://redis.io/commands/zrangebyscore
	 * 
	 * @param key
	 * @param min
	 * @param max
	 * @param offset
	 * @param count
	 * @return
	 */
	Set<Tuple> zRangeByScoreWithScores(byte[] key, double min, double max, long offset, long count);

	/**
	 * Get elements in range from {@code begin} to {@code end} from sorted set ordered high -> low.
	 * <p>
	 * See http://redis.io/commands/zrevrange
	 * 
	 * @param key
	 * @param begin
	 * @param end
	 * @return
	 */
	Set<byte[]> zRevRange(byte[] key, long begin, long end);

	/**
	 * Get set of {@link Tuple}s in range from {@code begin} to {@code end} from sorted set ordered high -> low.
	 * <p>
	 * See http://redis.io/commands/zrevrange
	 * 
	 * @param key
	 * @param begin
	 * @param end
	 * @return
	 */
	Set<Tuple> zRevRangeWithScores(byte[] key, long begin, long end);

	/**
	 * Get elements where score is between {@code min} and {@code max} from sorted set ordered high -> low.
	 * <p>
	 * See http://redis.io/commands/zrevrange
	 * 
	 * @param key
	 * @param min
	 * @param max
	 * @return
	 */
	Set<byte[]> zRevRangeByScore(byte[] key, double min, double max);

	/**
	 * Get set of {@link Tuple} where score is between {@code min} and {@code max} from sorted set ordered high -> low.
	 * <p>
	 * See http://redis.io/commands/zrevrange
	 * 
	 * @param key
	 * @param min
	 * @param max
	 * @return
	 */
	Set<Tuple> zRevRangeByScoreWithScores(byte[] key, double min, double max);

	/**
	 * Get elements in range from {@code begin} to {@code end} where score is between {@code min} and {@code max} from
	 * sorted set ordered high -> low.
	 * <p>
	 * See http://redis.io/commands/zrevrangebyscore
	 * 
	 * @param key
	 * @param min
	 * @param max
	 * @param offset
	 * @param count
	 * @return
	 */
	Set<byte[]> zRevRangeByScore(byte[] key, double min, double max, long offset, long count);

	/**
	 * Get set of {@link Tuple} in range from {@code begin} to {@code end} where score is between {@code min} and
	 * {@code max} from sorted set ordered high -> low.
	 * <p>
	 * See http://redis.io/commands/zrevrangebyscore
	 * 
	 * @param key
	 * @param min
	 * @param max
	 * @param offset
	 * @param count
	 * @return
	 */
	Set<Tuple> zRevRangeByScoreWithScores(byte[] key, double min, double max, long offset, long count);

	/**
	 * Count number of elements within sorted set with scores between {@code min} and {@code max}.
	 * <p>
	 * See http://redis.io/commands/zcount
	 * 
	 * @param key
	 * @param min
	 * @param max
	 * @return
	 */
	Long zCount(byte[] key, double min, double max);

	/**
	 * Get the size of sorted set with {@code key}.
	 * <p>
	 * See http://redis.io/commands/zcard
	 * 
	 * @param key
	 * @return
	 */
	Long zCard(byte[] key);

	/**
	 * Get the score of element with {@code value} from sorted set with key {@code key}.
	 * <p>
	 * See http://redis.io/commands/zrem
	 * 
	 * @param key
	 * @param value
	 * @return
	 */
	Double zScore(byte[] key, byte[] value);

	/**
	 * Remove elements in range between {@code begin} and {@code end} from sorted set with {@code key}.
	 * <p>
	 * See http://redis.io/commands/zremrange
	 * 
	 * @param key
	 * @param begin
	 * @param end
	 * @return
	 */
	Long zRemRange(byte[] key, long begin, long end);

	/**
	 * Remove elements with scores between {@code min} and {@code max} from sorted set with {@code key}.
	 * <p>
	 * See http://redis.io/commands/zremrangebyscore
	 * 
	 * @param key
	 * @param min
	 * @param max
	 * @return
	 */
	Long zRemRangeByScore(byte[] key, double min, double max);

	/**
	 * Union sorted {@code sets} and store result in destination {@code key}.
	 * <p>
	 * See http://redis.io/commands/zunionstore
	 * 
	 * @param destKey
	 * @param sets
	 * @return
	 */
	Long zUnionStore(byte[] destKey, byte[]... sets);

	/**
	 * Union sorted {@code sets} and store result in destination {@code key}.
	 * <p>
	 * See http://redis.io/commands/zunionstore
	 * 
	 * @param destKey
	 * @param aggregate
	 * @param weights
	 * @param sets
	 * @return
	 */
	Long zUnionStore(byte[] destKey, Aggregate aggregate, int[] weights, byte[]... sets);

	/**
	 * Intersect sorted {@code sets} and store result in destination {@code key}.
	 * <p>
	 * See http://redis.io/commands/zinterstore
	 * 
	 * @param destKey
	 * @param sets
	 * @return
	 */
	Long zInterStore(byte[] destKey, byte[]... sets);

	/**
	 * Intersect sorted {@code sets} and store result in destination {@code key}.
	 * <p>
	 * See http://redis.io/commands/zinterstore
	 * 
	 * @param destKey
	 * @param sets
	 * @return
	 */
	Long zInterStore(byte[] destKey, Aggregate aggregate, int[] weights, byte[]... sets);

	/**
	 * Use a {@link Cursor} to iterate over elements in sorted set at {@code key}.
	 * <p>
	 * See http://redis.io/commands/scan
	 * 
	 * @since 1.4
	 * @param key
	 * @param options
	 * @return
	 */
	Cursor<Tuple> zScan(byte[] key, ScanOptions options);

	/**
	 * Get elements where score is between {@code min} and {@code max} from sorted set.
	 * <p>
	 * See http://redis.io/commands/zrangebyscore
	 * 
	 * @since 1.5
	 * @param key
	 * @param min
	 * @param max
	 * @return
	 */
	Set<byte[]> zRangeByScore(byte[] key, String min, String max);

	/**
	 * Get elements in range from {@code begin} to {@code end} where score is between {@code min} and {@code max} from
	 * sorted set.
	 * <p>
	 * See http://redis.io/commands/zrangebyscore
	 * 
	 * @since 1.5
	 * @param key
	 * @param min
	 * @param max
	 * @param offset
	 * @param count
	 * @return
	 */
	Set<byte[]> zRangeByScore(byte[] key, String min, String max, long offset, long count);
}
