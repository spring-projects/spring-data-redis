/*
 * Copyright 2010 the original author or authors.
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

package org.springframework.datastore.redis.connection;

import java.util.Set;


/**
 * ZSet(SortedSet)-specific commands supported by Redis.
 * 
 * @author Costin Leau
 */
public interface RedisZSetCommands {

	public enum Aggregate {
		SUM, MIN, MAX;
	}

	public interface Tuple {
		byte[] getValue();

		Double getScore();
	}

	Boolean zAdd(byte[] key, double score, byte[] value);

	Boolean zRem(byte[] key, byte[] value);

	Double zIncrBy(byte[] key, double increment, byte[] value);

	Integer zRank(byte[] key, byte[] value);

	Integer zRevRank(byte[] key, byte[] value);

	Set<byte[]> zRange(byte[] key, int start, int end);

	Set<Tuple> zRangeWithScore(byte[] key, int start, int end);

	Set<byte[]> zRevRange(byte[] key, int start, int end);

	Set<Tuple> zRevRangeWithScore(byte[] key, int start, int end);

	Set<byte[]> zRangeByScore(byte[] key, double min, double max);

	Set<Tuple> zRangeByScoreWithScore(byte[] key, double min, double max);

	Set<byte[]> zRangeByScore(byte[] key, double min, double max, int offset, int count);

	Set<Tuple> zRangeByScoreWithScore(byte[] key, double min, double max, int offset, int count);

	Integer zCount(byte[] key, double min, double max);

	Integer zCard(byte[] key);

	Double zScore(byte[] key, byte[] value);

	Integer zRemRange(byte[] key, int start, int end);

	Integer zRemRangeByScore(byte[] key, double min, double max);

	Integer zUnionStore(byte[] destKey, byte[]... sets);

	Integer zUnionStore(byte[] destKey, Aggregate aggregate, int[] weights, byte[]... sets);

	Integer zInterStore(byte[] destKey, byte[]... sets);

	Integer zInterStore(byte[] destKey, Aggregate aggregate, int[] weights, byte[]... sets);
}