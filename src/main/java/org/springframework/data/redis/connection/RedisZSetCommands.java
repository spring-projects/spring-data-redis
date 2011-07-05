/*
 * Copyright 2010-2011 the original author or authors.
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


/**
 * ZSet(SortedSet)-specific commands supported by Redis.
 * 
 * @author Costin Leau
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
	public interface Tuple {
		byte[] getValue();

		Double getScore();
	}

	Boolean zAdd(byte[] key, double score, byte[] value);

	Boolean zRem(byte[] key, byte[] value);

	Double zIncrBy(byte[] key, double increment, byte[] value);

	Long zRank(byte[] key, byte[] value);

	Long zRevRank(byte[] key, byte[] value);

	Set<byte[]> zRange(byte[] key, long begin, long end);

	Set<Tuple> zRangeWithScores(byte[] key, long begin, long end);

	Set<byte[]> zRangeByScore(byte[] key, double min, double max);

	Set<Tuple> zRangeByScoreWithScores(byte[] key, double min, double max);

	Set<byte[]> zRangeByScore(byte[] key, double min, double max, long offset, long count);

	Set<Tuple> zRangeByScoreWithScores(byte[] key, double min, double max, long offset, long count);

	Set<byte[]> zRevRange(byte[] key, long begin, long end);

	Set<Tuple> zRevRangeWithScores(byte[] key, long begin, long end);

	Set<byte[]> zRevRangeByScore(byte[] key, double min, double max);

	Set<Tuple> zRevRangeByScoreWithScores(byte[] key, double min, double max);

	Set<byte[]> zRevRangeByScore(byte[] key, double min, double max, long offset, long count);

	Set<Tuple> zRevRangeByScoreWithScores(byte[] key, double min, double max, long offset, long count);

	Long zCount(byte[] key, double min, double max);

	Long zCard(byte[] key);

	Double zScore(byte[] key, byte[] value);

	Long zRemRange(byte[] key, long begin, long end);

	Long zRemRangeByScore(byte[] key, double min, double max);

	Long zUnionStore(byte[] destKey, byte[]... sets);

	Long zUnionStore(byte[] destKey, Aggregate aggregate, int[] weights, byte[]... sets);

	Long zInterStore(byte[] destKey, byte[]... sets);

	Long zInterStore(byte[] destKey, Aggregate aggregate, int[] weights, byte[]... sets);
}