/*
 * Copyright 2011 the original author or authors.
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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.serializer.RedisSerializer;

/**
 * Convenience extension of {@link RedisConnection} that accepts and returns {@link String}s instead of
 * byte arrays. Uses a {@link RedisSerializer} underneath to perform the conversion.
 * 
 * @author Costin Leau
 * @see RedisCallback
 * @see RedisSerializer
 * @see StringRedisTemplate 
 */
public interface StringRedisConnection extends RedisConnection {

	/**
	 * String-friendly ZSet tuple.
	 */
	public interface StringTuple extends Tuple {
		String getValueAsString();
	}

	Object execute(String command, String... args);

	Object execute(String command);

	Boolean exists(String key);

	Long del(String... keys);

	DataType type(String key);

	Collection<String> keys(String pattern);

	void rename(String oldName, String newName);

	Boolean renameNX(String oldName, String newName);

	Boolean expire(String key, long seconds);

	Boolean expireAt(String key, long unixTime);

	Boolean persist(String key);

	Boolean move(String key, int dbIndex);

	Long ttl(String key);

	String echo(String message);

	// sort commands
	List<String> sort(String key, SortParameters params);

	Long sort(String key, SortParameters params, String storeKey);

	String get(String key);

	String getSet(String key, String value);

	List<String> mGet(String... keys);

	void set(String key, String value);

	Boolean setNX(String key, String value);

	void setEx(String key, long seconds, String value);

	void mSetString(Map<String, String> tuple);

	void mSetNXString(Map<String, String> tuple);

	Long incr(String key);

	Long incrBy(String key, long value);

	Long decr(String key);

	Long decrBy(String key, long value);

	Long append(String key, String value);

	String getRange(String key, long start, long end);

	void setRange(String key, String value, long offset);

	Boolean getBit(String key, long offset);

	void setBit(String key, long offset, boolean value);

	Long strLen(String key);

	Long rPush(String key, String value);

	Long lPush(String key, String value);

	Long rPushX(String key, String value);

	Long lPushX(String key, String value);

	Long lLen(String key);

	List<String> lRange(String key, long start, long end);

	void lTrim(String key, long start, long end);

	String lIndex(String key, long index);

	Long lInsert(String key, Position where, String pivot, String value);

	void lSet(String key, long index, String value);

	Long lRem(String key, long count, String value);

	String lPop(String key);

	String rPop(String key);

	List<String> bLPop(int timeout, String... keys);

	List<String> bRPop(int timeout, String... keys);

	String rPopLPush(String srcKey, String dstKey);

	String bRPopLPush(int timeout, String srcKey, String dstKey);

	Boolean sAdd(String key, String value);

	Boolean sRem(String key, String value);

	String sPop(String key);

	Boolean sMove(String srcKey, String destKey, String value);

	Long sCard(String key);

	Boolean sIsMember(String key, String value);

	Set<String> sInter(String... keys);

	void sInterStore(String destKey, String... keys);

	Set<String> sUnion(String... keys);

	void sUnionStore(String destKey, String... keys);

	Set<String> sDiff(String... keys);

	void sDiffStore(String destKey, String... keys);

	Set<String> sMembers(String key);

	String sRandMember(String key);

	Boolean zAdd(String key, double score, String value);

	Boolean zRem(String key, String value);

	Double zIncrBy(String key, double increment, String value);

	Long zRank(String key, String value);

	Long zRevRank(String key, String value);

	Set<String> zRange(String key, long start, long end);

	Set<StringTuple> zRangeWithScores(String key, long start, long end);

	Set<String> zRevRange(String key, long start, long end);

	Set<StringTuple> zRevRangeWithScores(String key, long start, long end);

	Set<String> zRangeByScore(String key, double min, double max);

	Set<StringTuple> zRangeByScoreWithScores(String key, double min, double max);

	Set<String> zRangeByScore(String key, double min, double max, long offset, long count);

	Set<StringTuple> zRangeByScoreWithScores(String key, double min, double max, long offset, long count);

	Long zCount(String key, double min, double max);

	Long zCard(String key);

	Double zScore(String key, String value);

	Long zRemRange(String key, long start, long end);

	Long zRemRangeByScore(String key, double min, double max);

	Long zUnionStore(String destKey, String... sets);

	Long zUnionStore(String destKey, Aggregate aggregate, int[] weights, String... sets);

	Long zInterStore(String destKey, String... sets);

	Long zInterStore(String destKey, Aggregate aggregate, int[] weights, String... sets);

	Boolean hSet(String key, String field, String value);

	Boolean hSetNX(String key, String field, String value);

	String hGet(String key, String field);

	List<String> hMGet(String key, String... fields);

	void hMSet(String key, Map<String, String> hashes);

	Long hIncrBy(String key, String field, long delta);

	Boolean hExists(String key, String field);

	Boolean hDel(String key, String field);

	Long hLen(String key);

	Set<String> hKeys(String key);

	List<String> hVals(String key);

	Map<String, String> hGetAll(String key);

	Long publish(String channel, String message);

	void subscribe(MessageListener listener, String... channels);

	void pSubscribe(MessageListener listener, String... patterns);
}