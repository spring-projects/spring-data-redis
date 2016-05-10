/*
 * Copyright 2011-2016 the original author or authors.
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

import org.springframework.data.geo.Circle;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.geo.Metric;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.data.redis.core.types.RedisClientInfo;
import org.springframework.data.redis.serializer.RedisSerializer;

/**
 * Convenience extension of {@link RedisConnection} that accepts and returns {@link String}s instead of byte arrays.
 * Uses a {@link RedisSerializer} underneath to perform the conversion.
 * 
 * @author Costin Leau
 * @author Christoph Strobl
 * @author Thomas Darimont
 * @author David Liu
 * @author Mark Paluch
 * @author Ninad Divadkar
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

	Boolean pExpire(String key, long millis);

	Boolean expireAt(String key, long unixTime);

	Boolean pExpireAt(String key, long unixTimeInMillis);

	Boolean persist(String key);

	Boolean move(String key, int dbIndex);

	Long ttl(String key);

	Long pTtl(String key);

	String echo(String message);

	// sort commands
	List<String> sort(String key, SortParameters params);

	Long sort(String key, SortParameters params, String storeKey);

	/**
	 * Get the value of {@code key}.
	 * <p>
	 * See http://redis.io/commands/get
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 */
	String get(String key);

	/**
	 * Set value of {@code key} and return its old value.
	 * <p>
	 * See http://redis.io/commands/getset
	 *
	 * @param key must not be {@literal null}.
	 * @param value
	 * @return
	 */
	String getSet(String key, String value);

	/**
	 * Get the values of all given {@code keys}.
	 * <p>
	 * See http://redis.io/commands/mget
	 *
	 * @param keys
	 * @return
	 */
	List<String> mGet(String... keys);

	/**
	 * Set {@code value} for {@code key}.
	 * <p>
	 * See http://redis.io/commands/set
	 *
	 * @param key must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 */
	void set(String key, String value);

	/**
	 * Set {@code value} for {@code key} applying timeouts from {@code expiration} if set and inserting/updating values
	 * depending on {@code option}.
	 * <p>
	 * See http://redis.io/commands/set
	 * 
	 * @param key must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @param expiration can be {@literal null}. Defaulted to {@link Expiration#persistent()}.
	 * @param option can be {@literal null}. Defaulted to {@link SetOption#UPSERT}.
	 * @since 1.7
	 */
	void set(String key, String value, Expiration expiration, SetOption option);

	/**
	 * Set {@code value} for {@code key}, only if {@code key} does not exist.
	 * <p>
	 * See http://redis.io/commands/setnx
	 *
	 * @param key must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return
	 */
	Boolean setNX(String key, String value);

	/**
	 * Set the {@code value} and expiration in {@code seconds} for {@code key}.
	 * <p>
	 * See http://redis.io/commands/setex
	 *
	 * @param key must not be {@literal null}.
	 * @param seconds
	 * @param value must not be {@literal null}.
	 */
	void setEx(String key, long seconds, String value);

	/**
	 * Set the {@code value} and expiration in {@code milliseconds} for {@code key}.
	 * <p>
	 * See http://redis.io/commands/psetex
	 * 
	 * @param key must not be {@literal null}.
	 * @param milliseconds
	 * @param value must not be {@literal null}.
	 * @since 1.3
	 */
	void pSetEx(String key, long milliseconds, String value);

	/**
	 * Set multiple keys to multiple values using key-value pairs provided in {@code tuple}.
	 * <p>
	 * See http://redis.io/commands/mset
	 *
	 * @param tuple
	 */
	void mSetString(Map<String, String> tuple);

	/**
	 * Set multiple keys to multiple values using key-value pairs provided in {@code tuple} only if the provided key does
	 * not exist.
	 * <p>
	 * See http://redis.io/commands/msetnx
	 *
	 * @param tuple
	 */
	Boolean mSetNXString(Map<String, String> tuple);

	/**
	 * Increment value of {@code key} by 1.
	 * <p>
	 * See http://redis.io/commands/incr
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 */
	Long incr(String key);

	/**
	 * Increment value of {@code key} by {@code value}.
	 * <p>
	 * See http://redis.io/commands/incrby
	 *
	 * @param key must not be {@literal null}.
	 * @param value
	 * @return
	 */
	Long incrBy(String key, long value);

	/**
	 * Increment value of {@code key} by {@code value}.
	 * <p>
	 * See http://redis.io/commands/incrbyfloat
	 *
	 * @param key must not be {@literal null}.
	 * @param value
	 * @return
	 */
	Double incrBy(String key, double value);

	/**
	 * Decrement value of {@code key} by 1.
	 * <p>
	 * See http://redis.io/commands/decr
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 */
	Long decr(String key);

	/**
	 * Increment value of {@code key} by {@code value}.
	 * <p>
	 * See http://redis.io/commands/decrby
	 *
	 * @param key must not be {@literal null}.
	 * @param value
	 * @return
	 */
	Long decrBy(String key, long value);

	/**
	 * Append a {@code value} to {@code key}.
	 * <p>
	 * See http://redis.io/commands/append
	 *
	 * @param key must not be {@literal null}.
	 * @param value
	 * @return
	 */
	Long append(String key, String value);

	/**
	 * Get a substring of value of {@code key} between {@code begin} and {@code end}.
	 * <p>
	 * See http://redis.io/commands/getrange
	 *
	 * @param key must not be {@literal null}.
	 * @param start
	 * @param end
	 * @return
	 */
	String getRange(String key, long start, long end);

	/**
	 * Overwrite parts of {@code key} starting at the specified {@code offset} with given {@code value}.
	 * <p>
	 * See http://redis.io/commands/setrange
	 *
	 * @param key must not be {@literal null}.
	 * @param value
	 * @param offset
	 */
	void setRange(String key, String value, long offset);

	/**
	 * Get the bit value at {@code offset} of value at {@code key}.
	 * <p>
	 * See http://redis.io/commands/getbit
	 *
	 * @param key must not be {@literal null}.
	 * @param offset
	 * @return
	 */
	Boolean getBit(String key, long offset);

	/**
	 * Sets the bit at {@code offset} in value stored at {@code key}.
	 * <p>
	 * See http://redis.io/commands/setbit
	 *
	 * @param key must not be {@literal null}.
	 * @param offset
	 * @param value
	 * @return the original bit value stored at {@code offset}.
	 */
	Boolean setBit(String key, long offset, boolean value);

	/**
	 * Count the number of set bits (population counting) in value stored at {@code key}.
	 * <p>
	 * See http://redis.io/commands/bitcount
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 */
	Long bitCount(String key);

	/**
	 * Count the number of set bits (population counting) of value stored at {@code key} between {@code begin} and
	 * {@code end}.
	 * <p>
	 * See http://redis.io/commands/bitcount
	 *
	 * @param key must not be {@literal null}.
	 * @param begin
	 * @param end
	 * @return
	 */
	Long bitCount(String key, long begin, long end);

	/**
	 * Perform bitwise operations between strings.
	 * <p>
	 * See http://redis.io/commands/bitop
	 *
	 * @param op
	 * @param destination
	 * @param keys
	 * @return
	 */
	Long bitOp(BitOperation op, String destination, String... keys);

	/**
	 * Get the length of the value stored at {@code key}.
	 * <p>
	 * See http://redis.io/commands/strlen
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 */
	Long strLen(String key);

	Long rPush(String key, String... values);

	Long lPush(String key, String... values);

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

	Long sAdd(String key, String... values);

	Long sRem(String key, String... values);

	String sPop(String key);

	Boolean sMove(String srcKey, String destKey, String value);

	Long sCard(String key);

	Boolean sIsMember(String key, String value);

	Set<String> sInter(String... keys);

	Long sInterStore(String destKey, String... keys);

	Set<String> sUnion(String... keys);

	Long sUnionStore(String destKey, String... keys);

	Set<String> sDiff(String... keys);

	Long sDiffStore(String destKey, String... keys);

	Set<String> sMembers(String key);

	String sRandMember(String key);

	List<String> sRandMember(String key, long count);

	Boolean zAdd(String key, double score, String value);

	Long zAdd(String key, Set<StringTuple> tuples);

	Long zRem(String key, String... values);

	Double zIncrBy(String key, double increment, String value);

	Long zRank(String key, String value);

	Long zRevRank(String key, String value);

	Set<String> zRange(String key, long start, long end);

	Set<StringTuple> zRangeWithScores(String key, long start, long end);

	Set<String> zRevRange(String key, long start, long end);

	Set<StringTuple> zRevRangeWithScores(String key, long start, long end);

	Set<String> zRevRangeByScore(String key, double min, double max);

	Set<StringTuple> zRevRangeByScoreWithScores(String key, double min, double max);

	Set<String> zRevRangeByScore(String key, double min, double max, long offset, long count);

	Set<StringTuple> zRevRangeByScoreWithScores(String key, double min, double max, long offset, long count);

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

	Double hIncrBy(String key, String field, double delta);

	Boolean hExists(String key, String field);

	Long hDel(String key, String... fields);

	Long hLen(String key);

	Set<String> hKeys(String key);

	List<String> hVals(String key);

	Map<String, String> hGetAll(String key);

	Long publish(String channel, String message);

	void subscribe(MessageListener listener, String... channels);

	void pSubscribe(MessageListener listener, String... patterns);

	String scriptLoad(String script);

	<T> T eval(String script, ReturnType returnType, int numKeys, String... keysAndArgs);

	<T> T evalSha(String scriptSha1, ReturnType returnType, int numKeys, String... keysAndArgs);

	/**
	 * Assign given {@code name} to connection using registered {@link RedisSerializer} for name conversion.
	 * 
	 * @param name
	 * @see #setClientName(byte[])
	 * @since 1.3
	 */
	void setClientName(String name);

	/**
	 * @see RedisConnection#getClientList()
	 * @since 1.3
	 */
	List<RedisClientInfo> getClientList();

	/**
	 * @since 1.4
	 * @see RedisHashCommands#hScan(byte[], ScanOptions)
	 * @param key must not be {@literal null}.
	 * @param options
	 * @return
	 */
	Cursor<Map.Entry<String, String>> hScan(String key, ScanOptions options);

	/**
	 * @since 1.4
	 * @see RedisSetCommands#sScan(byte[], ScanOptions)
	 * @param key must not be {@literal null}.
	 * @param options
	 * @return
	 */
	Cursor<String> sScan(String key, ScanOptions options);

	/**
	 * @since 1.4
	 * @see RedisZSetCommands#zScan(byte[], ScanOptions)
	 * @param key must not be {@literal null}.
	 * @param options
	 * @return
	 */
	Cursor<StringTuple> zScan(String key, ScanOptions options);

	/**
	 * @since 1.5
	 * @param key must not be {@literal null}.
	 * @param min
	 * @param max
	 * @return
	 */
	Set<byte[]> zRangeByScore(String key, String min, String max);

	/**
	 * @since 1.5
	 * @param key must not be {@literal null}.
	 * @param min
	 * @param max
	 * @param offset
	 * @param count
	 * @return
	 */
	Set<byte[]> zRangeByScore(String key, String min, String max, long offset, long count);

	/**
	 * Adds given {@literal values} to the HyperLogLog stored at given {@literal key}.
	 * 
	 * @param key must not be {@literal null}.
	 * @param values
	 * @return
	 * @since 1.5
	 */
	Long pfAdd(String key, String... values);

	/**
	 * @param keys
	 * @return
	 * @since 1.5
	 */
	Long pfCount(String... keys);

	/**
	 * @param destinationKey
	 * @param sourceKeys
	 * @since 1.5
	 */
	void pfMerge(String destinationKey, String... sourceKeys);

	/**
	 * Get all elements in the sorted set at {@literal key} in lexicographical ordering.
	 * 
	 * @param key must not be {@literal null}.
	 * @return
	 * @since 1.6
	 */
	Set<String> zRangeByLex(String key);

	/**
	 * Get the elements in {@link Range} from the sorted set at {@literal key} in lexicographical ordering
	 * 
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return
	 * @since 1.6
	 */
	Set<String> zRangeByLex(String key, Range range);

	/**
	 * Get the elements in {@link Range} from the sorted set at {@literal key} in lexicographical ordering. Result is
	 * limited via {@link Limit}.
	 * 
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @param range can be {@literal null}.
	 * @return
	 * @since 1.6
	 */
	Set<String> zRangeByLex(String key, Range range, Limit limit);

	/**
	 * Add {@link Point} with given member {@literal name} to {@literal key}.
	 * 
	 * @param key must not be {@literal null}.
	 * @param point must not be {@literal null}.
	 * @param member must not be {@literal null}.
	 * @return Number of elements added.
	 * @see <a href="http://redis.io/commands/geoadd">http://redis.io/commands/geoadd</a>
	 * @since 1.8
	 */
	Long geoAdd(String key, Point point, String member);

	/**
	 * Add {@link GeoLocation} to {@literal key}.
	 * 
	 * @param key must not be {@literal null}.
	 * @param location must not be {@literal null}.
	 * @return Number of elements added.
	 * @see <a href="http://redis.io/commands/geoadd">http://redis.io/commands/geoadd</a>
	 * @since 1.8
	 */
	Long geoAdd(String key, GeoLocation<String> location);

	/**
	 * Add {@link Map} of member / {@link Point} pairs to {@literal key}.
	 * 
	 * @param key must not be {@literal null}.
	 * @param memberCoordinateMap must not be {@literal null}.
	 * @return Number of elements added.
	 * @see <a href="http://redis.io/commands/geoadd">http://redis.io/commands/geoadd</a>
	 * @since 1.8
	 */
	Long geoAdd(String key, Map<String, Point> memberCoordinateMap);

	/**
	 * Add {@link GeoLocation}s to {@literal key}
	 * 
	 * @param key must not be {@literal null}.
	 * @param locations must not be {@literal null}.
	 * @return Number of elements added.
	 * @see <a href="http://redis.io/commands/geoadd">http://redis.io/commands/geoadd</a>
	 * @since 1.8
	 */
	Long geoAdd(String key, Iterable<GeoLocation<String>> locations);

	/**
	 * Get the {@link Distance} between {@literal member1} and {@literal member2}.
	 *
	 * @param key must not be {@literal null}.
	 * @param member1 must not be {@literal null}.
	 * @param member2 must not be {@literal null}.
	 * @return can be {@literal null}.
	 * @see <a href="http://redis.io/commands/geodist">http://redis.io/commands/geodist</a>
	 * @since 1.8
	 */
	Distance geoDist(String key, String member1, String member2);

	/**
	 * Get the {@link Distance} between {@literal member1} and {@literal member2} in the given {@link Metric}.
	 *
	 * @param key must not be {@literal null}.
	 * @param member1 must not be {@literal null}.
	 * @param member2 must not be {@literal null}.
	 * @param metric must not be {@literal null}.
	 * @return can be {@literal null}.
	 * @see <a href="http://redis.io/commands/geodist">http://redis.io/commands/geodist</a>
	 * @since 1.8
	 */
	Distance geoDist(String key, String member1, String member2, Metric metric);

	/**
	 * Get geohash representation of the position for one or more {@literal member}s.
	 *
	 * @param key must not be {@literal null}.
	 * @param members must not be {@literal null}.
	 * @return never {@literal null}.
	 * @see <a href="http://redis.io/commands/geohash">http://redis.io/commands/geohash</a>
	 * @since 1.8
	 */
	List<String> geoHash(String key, String... members);

	/**
	 * Get the {@link Point} representation of positions for one or more {@literal member}s.
	 *
	 * @param key must not be {@literal null}.
	 * @param members must not be {@literal null}.
	 * @return never {@literal null}.
	 * @see <a href="http://redis.io/commands/geopos">http://redis.io/commands/geopos</a>
	 * @since 1.8
	 */
	List<Point> geoPos(String key, String... members);

	/**
	 * Get the {@literal member}s within the boundaries of a given {@link Circle}.
	 *
	 * @param key must not be {@literal null}.
	 * @param within must not be {@literal null}.
	 * @return never {@literal null}.
	 * @see <a href="http://redis.io/commands/georadius">http://redis.io/commands/georadius</a>
	 * @since 1.8
	 */
	GeoResults<GeoLocation<String>> georadius(String key, Circle within);

	/**
	 * Get the {@literal member}s within the boundaries of a given {@link Circle} applying {@link GeoRadiusCommandArgs}.
	 *
	 * @param key must not be {@literal null}.
	 * @param within must not be {@literal null}.
	 * @param args must not be {@literal null}.
	 * @return never {@literal null}.
	 * @see <a href="http://redis.io/commands/georadius">http://redis.io/commands/georadius</a>
	 * @since 1.8
	 */
	GeoResults<GeoLocation<String>> georadius(String key, Circle within, GeoRadiusCommandArgs args);

	/**
	 * Get the {@literal member}s within the circle defined by the {@literal members} coordinates and given
	 * {@literal radius}.
	 *
	 * @param key must not be {@literal null}.
	 * @param member must not be {@literal null}.
	 * @param radius
	 * @return never {@literal null}.
	 * @see <a href="http://redis.io/commands/georadiusbymember">http://redis.io/commands/georadiusbymember</a>
	 * @since 1.8
	 */
	GeoResults<GeoLocation<String>> georadiusByMember(String key, String member, double radius);

	/**
	 * Get the {@literal member}s within the circle defined by the {@literal members} coordinates and given
	 * {@link Distance}.
	 *
	 * @param key must not be {@literal null}.
	 * @param member must not be {@literal null}.
	 * @param radius must not be {@literal null}.
	 * @return never {@literal null}.
	 * @see <a href="http://redis.io/commands/georadiusbymember">http://redis.io/commands/georadiusbymember</a>
	 * @since 1.8
	 */
	GeoResults<GeoLocation<String>> georadiusByMember(String key, String member, Distance radius);

	/**
	 * Get the {@literal member}s within the circle defined by the {@literal members} coordinates and given
	 * {@link Distance} and {@link GeoRadiusCommandArgs}.
	 *
	 * @param key must not be {@literal null}.
	 * @param member must not be {@literal null}.
	 * @param radius must not be {@literal null}.
	 * @param args must not be {@literal null}.
	 * @return never {@literal null}.
	 * @see <a href="http://redis.io/commands/georadiusbymember">http://redis.io/commands/georadiusbymember</a>
	 * @since 1.8
	 */
	GeoResults<GeoLocation<String>> georadiusByMember(String key, String member, Distance radius,
			GeoRadiusCommandArgs args);

	/**
	 * Remove the {@literal member}s.
	 *
	 * @param key must not be {@literal null}.
	 * @param members must not be {@literal null}.
	 * @return Number of members elements removed.
	 * @since 1.8
	 */
	Long geoRemove(String key, String... members);
}
