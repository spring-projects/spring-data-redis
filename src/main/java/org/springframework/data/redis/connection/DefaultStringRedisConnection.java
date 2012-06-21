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
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.SerializationUtils;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.util.Assert;

/**
 * Default implementation of {@link StringRedisConnection}.
 * 
 * @author Costin Leau
 */
public class DefaultStringRedisConnection implements StringRedisConnection {

	private final RedisConnection delegate;
	private final RedisSerializer<String> serializer;

	/**
	 * Constructs a new <code>DefaultStringRedisConnection</code> instance.
	 * Uses {@link StringRedisSerializer} as underlying serializer.
	 *
	 * @param connection Redis connection
	 */
	public DefaultStringRedisConnection(RedisConnection connection) {
		Assert.notNull(connection, "connection is required");
		this.delegate = connection;
		this.serializer = new StringRedisSerializer();
	}

	/**
	 * Constructs a new <code>DefaultStringRedisConnection</code> instance.
	 *
	 * @param connection Redis connection
	 * @param serializer String serializer
	 */
	public DefaultStringRedisConnection(RedisConnection connection, RedisSerializer<String> serializer) {
		Assert.notNull(connection, "connection is required");
		Assert.notNull(connection, "serializer is required");
		this.delegate = connection;
		this.serializer = serializer;
	}

	public Long append(byte[] key, byte[] value) {
		return delegate.append(key, value);
	}

	public void bgSave() {
		delegate.bgSave();
	}

	public void bgWriteAof() {
		delegate.bgWriteAof();
	}

	public List<byte[]> bLPop(int timeout, byte[]... keys) {
		return delegate.bLPop(timeout, keys);
	}

	public List<byte[]> bRPop(int timeout, byte[]... keys) {
		return delegate.bRPop(timeout, keys);
	}

	public byte[] bRPopLPush(int timeout, byte[] srcKey, byte[] dstKey) {
		return delegate.bRPopLPush(timeout, srcKey, dstKey);
	}

	public void close() throws RedisSystemException {
		delegate.close();
	}

	public Long dbSize() {
		return delegate.dbSize();
	}

	public Long decr(byte[] key) {
		return delegate.decr(key);
	}

	public Long decrBy(byte[] key, long value) {
		return delegate.decrBy(key, value);
	}

	public Long del(byte[]... keys) {
		return delegate.del(keys);
	}

	public void discard() {
		delegate.discard();
	}

	public byte[] echo(byte[] message) {
		return delegate.echo(message);
	}

	public List<Object> exec() {
		return delegate.exec();
	}

	public Boolean exists(byte[] key) {
		return delegate.exists(key);
	}

	public Boolean expire(byte[] key, long seconds) {
		return delegate.expire(key, seconds);
	}

	public Boolean expireAt(byte[] key, long unixTime) {
		return delegate.expireAt(key, unixTime);
	}

	public void flushAll() {
		delegate.flushAll();
	}

	public void flushDb() {
		delegate.flushDb();
	}

	public byte[] get(byte[] key) {
		return delegate.get(key);
	}

	public Boolean getBit(byte[] key, long offset) {
		return delegate.getBit(key, offset);
	}

	public List<String> getConfig(String pattern) {
		return delegate.getConfig(pattern);
	}

	public Object getNativeConnection() {
		return delegate.getNativeConnection();
	}

	public byte[] getRange(byte[] key, long start, long end) {
		return delegate.getRange(key, start, end);
	}

	public byte[] getSet(byte[] key, byte[] value) {
		return delegate.getSet(key, value);
	}

	public Subscription getSubscription() {
		return delegate.getSubscription();
	}

	public Boolean hDel(byte[] key, byte[] field) {
		return delegate.hDel(key, field);
	}

	public Boolean hExists(byte[] key, byte[] field) {
		return delegate.hExists(key, field);
	}

	public byte[] hGet(byte[] key, byte[] field) {
		return delegate.hGet(key, field);
	}

	public Map<byte[], byte[]> hGetAll(byte[] key) {
		return delegate.hGetAll(key);
	}

	public Long hIncrBy(byte[] key, byte[] field, long delta) {
		return delegate.hIncrBy(key, field, delta);
	}

	public Set<byte[]> hKeys(byte[] key) {
		return delegate.hKeys(key);
	}

	public Long hLen(byte[] key) {
		return delegate.hLen(key);
	}

	public List<byte[]> hMGet(byte[] key, byte[]... fields) {
		return delegate.hMGet(key, fields);
	}

	public void hMSet(byte[] key, Map<byte[], byte[]> hashes) {
		delegate.hMSet(key, hashes);
	}

	public Boolean hSet(byte[] key, byte[] field, byte[] value) {
		return delegate.hSet(key, field, value);
	}

	public Boolean hSetNX(byte[] key, byte[] field, byte[] value) {
		return delegate.hSetNX(key, field, value);
	}

	public List<byte[]> hVals(byte[] key) {
		return delegate.hVals(key);
	}

	public Long incr(byte[] key) {
		return delegate.incr(key);
	}

	public Long incrBy(byte[] key, long value) {
		return delegate.incrBy(key, value);
	}

	public Properties info() {
		return delegate.info();
	}

	public boolean isClosed() {
		return delegate.isClosed();
	}

	public boolean isQueueing() {
		return delegate.isQueueing();
	}

	public boolean isSubscribed() {
		return delegate.isSubscribed();
	}

	public Set<byte[]> keys(byte[] pattern) {
		return delegate.keys(pattern);
	}

	public Long lastSave() {
		return delegate.lastSave();
	}

	public byte[] lIndex(byte[] key, long index) {
		return delegate.lIndex(key, index);
	}

	public Long lInsert(byte[] key, Position where, byte[] pivot, byte[] value) {
		return delegate.lInsert(key, where, pivot, value);
	}

	public Long lLen(byte[] key) {
		return delegate.lLen(key);
	}

	public byte[] lPop(byte[] key) {
		return delegate.lPop(key);
	}

	public Long lPush(byte[] key, byte[] value) {
		return delegate.lPush(key, value);
	}

	public Long lPushX(byte[] key, byte[] value) {
		return delegate.lPushX(key, value);
	}

	public List<byte[]> lRange(byte[] key, long start, long end) {
		return delegate.lRange(key, start, end);
	}

	public Long lRem(byte[] key, long count, byte[] value) {
		return delegate.lRem(key, count, value);
	}

	public void lSet(byte[] key, long index, byte[] value) {
		delegate.lSet(key, index, value);
	}

	public void lTrim(byte[] key, long start, long end) {
		delegate.lTrim(key, start, end);
	}

	public List<byte[]> mGet(byte[]... keys) {
		return delegate.mGet(keys);
	}

	public void mSet(Map<byte[], byte[]> tuple) {
		delegate.mSet(tuple);
	}

	public void mSetNX(Map<byte[], byte[]> tuple) {
		delegate.mSetNX(tuple);
	}

	public void multi() {
		delegate.multi();
	}

	public Boolean persist(byte[] key) {
		return delegate.persist(key);
	}

	public Boolean move(byte[] key, int dbIndex) {
		return delegate.move(key, dbIndex);
	}

	public String ping() {
		return delegate.ping();
	}

	public void pSubscribe(MessageListener listener, byte[]... patterns) {
		delegate.pSubscribe(listener, patterns);
	}

	public Long publish(byte[] channel, byte[] message) {
		return delegate.publish(channel, message);
	}

	public byte[] randomKey() {
		return delegate.randomKey();
	}

	public void rename(byte[] oldName, byte[] newName) {
		delegate.rename(oldName, newName);
	}

	public Boolean renameNX(byte[] oldName, byte[] newName) {
		return delegate.renameNX(oldName, newName);
	}

	public void resetConfigStats() {
		delegate.resetConfigStats();
	}

	public byte[] rPop(byte[] key) {
		return delegate.rPop(key);
	}

	public byte[] rPopLPush(byte[] srcKey, byte[] dstKey) {
		return delegate.rPopLPush(srcKey, dstKey);
	}

	public Long rPush(byte[] key, byte[] value) {
		return delegate.rPush(key, value);
	}

	public Long rPushX(byte[] key, byte[] value) {
		return delegate.rPushX(key, value);
	}

	public Boolean sAdd(byte[] key, byte[] value) {
		return delegate.sAdd(key, value);
	}

	public void save() {
		delegate.save();
	}

	public Long sCard(byte[] key) {
		return delegate.sCard(key);
	}

	public Set<byte[]> sDiff(byte[]... keys) {
		return delegate.sDiff(keys);
	}

	public Long sDiffStore(byte[] destKey, byte[]... keys) {
		return delegate.sDiffStore(destKey, keys);
	}

	public void select(int dbIndex) {
		delegate.select(dbIndex);
	}

	public void set(byte[] key, byte[] value) {
		delegate.set(key, value);
	}

	public void setBit(byte[] key, long offset, boolean value) {
		delegate.setBit(key, offset, value);
	}

	public void setConfig(String param, String value) {
		delegate.setConfig(param, value);
	}

	public void setEx(byte[] key, long seconds, byte[] value) {
		delegate.setEx(key, seconds, value);
	}

	public Boolean setNX(byte[] key, byte[] value) {
		return delegate.setNX(key, value);
	}

	public void setRange(byte[] key, byte[] value, long start) {
		delegate.setRange(key, value, start);
	}

	public void shutdown() {
		delegate.shutdown();
	}

	public Set<byte[]> sInter(byte[]... keys) {
		return delegate.sInter(keys);
	}

	public Long sInterStore(byte[] destKey, byte[]... keys) {
		return delegate.sInterStore(destKey, keys);
	}

	public Boolean sIsMember(byte[] key, byte[] value) {
		return delegate.sIsMember(key, value);
	}

	public Set<byte[]> sMembers(byte[] key) {
		return delegate.sMembers(key);
	}

	public Boolean sMove(byte[] srcKey, byte[] destKey, byte[] value) {
		return delegate.sMove(srcKey, destKey, value);
	}

	public Long sort(byte[] key, SortParameters params, byte[] storeKey) {
		return delegate.sort(key, params, storeKey);
	}

	public List<byte[]> sort(byte[] key, SortParameters params) {
		return delegate.sort(key, params);
	}

	public byte[] sPop(byte[] key) {
		return delegate.sPop(key);
	}

	public byte[] sRandMember(byte[] key) {
		return delegate.sRandMember(key);
	}

	public Boolean sRem(byte[] key, byte[] value) {
		return delegate.sRem(key, value);
	}

	public Long strLen(byte[] key) {
		return delegate.strLen(key);
	}

	public void subscribe(MessageListener listener, byte[]... channels) {
		delegate.subscribe(listener, channels);
	}

	public Set<byte[]> sUnion(byte[]... keys) {
		return delegate.sUnion(keys);
	}

	public Long sUnionStore(byte[] destKey, byte[]... keys) {
		return delegate.sUnionStore(destKey, keys);
	}

	public Long ttl(byte[] key) {
		return delegate.ttl(key);
	}

	public DataType type(byte[] key) {
		return delegate.type(key);
	}

	public void unwatch() {
		delegate.unwatch();
	}

	public void watch(byte[]... keys) {
		delegate.watch(keys);
	}

	public Boolean zAdd(byte[] key, double score, byte[] value) {
		return delegate.zAdd(key, score, value);
	}

	public Long zCard(byte[] key) {
		return delegate.zCard(key);
	}

	public Long zCount(byte[] key, double min, double max) {
		return delegate.zCount(key, min, max);
	}

	public Double zIncrBy(byte[] key, double increment, byte[] value) {
		return delegate.zIncrBy(key, increment, value);
	}

	public Long zInterStore(byte[] destKey, Aggregate aggregate, int[] weights, byte[]... sets) {
		return delegate.zInterStore(destKey, aggregate, weights, sets);
	}

	public Long zInterStore(byte[] destKey, byte[]... sets) {
		return delegate.zInterStore(destKey, sets);
	}

	public Set<byte[]> zRange(byte[] key, long start, long end) {
		return delegate.zRange(key, start, end);
	}

	public Set<byte[]> zRangeByScore(byte[] key, double min, double max, long offset, long count) {
		return delegate.zRangeByScore(key, min, max, offset, count);
	}

	public Set<byte[]> zRangeByScore(byte[] key, double min, double max) {
		return delegate.zRangeByScore(key, min, max);
	}

	public Set<Tuple> zRangeByScoreWithScores(byte[] key, double min, double max, long offset, long count) {
		return delegate.zRangeByScoreWithScores(key, min, max, offset, count);
	}

	public Set<Tuple> zRangeByScoreWithScores(byte[] key, double min, double max) {
		return delegate.zRangeByScoreWithScores(key, min, max);
	}

	public Set<Tuple> zRangeWithScores(byte[] key, long start, long end) {
		return delegate.zRangeWithScores(key, start, end);
	}

	public Set<byte[]> zRevRangeByScore(byte[] key, double min, double max, long offset, long count) {
		return delegate.zRevRangeByScore(key, min, max, offset, count);
	}

	public Set<byte[]> zRevRangeByScore(byte[] key, double min, double max) {
		return delegate.zRevRangeByScore(key, min, max);
	}

	public Set<Tuple> zRevRangeByScoreWithScores(byte[] key, double min, double max, long offset, long count) {
		return delegate.zRevRangeByScoreWithScores(key, min, max, offset, count);
	}

	public Set<Tuple> zRevRangeByScoreWithScores(byte[] key, double min, double max) {
		return delegate.zRevRangeByScoreWithScores(key, min, max);
	}

	public Long zRank(byte[] key, byte[] value) {
		return delegate.zRank(key, value);
	}

	public Boolean zRem(byte[] key, byte[] value) {
		return delegate.zRem(key, value);
	}

	public Long zRemRange(byte[] key, long start, long end) {
		return delegate.zRemRange(key, start, end);
	}

	public Long zRemRangeByScore(byte[] key, double min, double max) {
		return delegate.zRemRangeByScore(key, min, max);
	}

	public Set<byte[]> zRevRange(byte[] key, long start, long end) {
		return delegate.zRevRange(key, start, end);
	}

	public Set<Tuple> zRevRangeWithScores(byte[] key, long start, long end) {
		return delegate.zRevRangeWithScores(key, start, end);
	}

	public Long zRevRank(byte[] key, byte[] value) {
		return delegate.zRevRank(key, value);
	}

	public Double zScore(byte[] key, byte[] value) {
		return delegate.zScore(key, value);
	}

	public Long zUnionStore(byte[] destKey, Aggregate aggregate, int[] weights, byte[]... sets) {
		return delegate.zUnionStore(destKey, aggregate, weights, sets);
	}

	public Long zUnionStore(byte[] destKey, byte[]... sets) {
		return delegate.zUnionStore(destKey, sets);
	}

	//
	// String methods
	//

	private byte[] serialize(String data) {
		return serializer.serialize(data);
	}

	private byte[][] serializeMulti(String... keys) {
		byte[][] ret = new byte[keys.length][];

		for (int i = 0; i < ret.length; i++) {
			ret[i] = serializer.serialize(keys[i]);
		}

		return ret;
	}

	private Map<byte[], byte[]> serialize(Map<String, String> hashes) {
		Map<byte[], byte[]> ret = new LinkedHashMap<byte[], byte[]>(hashes.size());
		
		for (Map.Entry<String, String> entry : hashes.entrySet()) {
			ret.put(serializer.serialize(entry.getKey()), serializer.serialize(entry.getValue()));
		}

		return ret;
	}


	private List<String> deserialize(List<byte[]> data) {
		return SerializationUtils.deserialize(data, serializer);
	}

	private Set<String> deserialize(Set<byte[]> data) {
		return SerializationUtils.deserialize(data, serializer);
	}

	private String deserialize(byte[] data) {
		return serializer.deserialize(data);
	}

	private Set<StringTuple> deserializeTuple(Set<Tuple> data) {
		if (data == null) {
			return null;
		}
		Set<StringTuple> result = new LinkedHashSet<StringTuple>(data.size());
		for (Tuple raw : data) {
			result.add(new DefaultStringTuple(raw, serializer.deserialize(raw.getValue())));
		}

		return result;
	}

	
	public Long append(String key, String value) {
		return delegate.append(serialize(key), serialize(value));
	}

	
	public List<String> bLPop(int timeout, String... keys) {
		return deserialize(delegate.bLPop(timeout, serializeMulti(keys)));
	}

	
	public List<String> bRPop(int timeout, String... keys) {
		return deserialize(delegate.bRPop(timeout, serializeMulti(keys)));
	}

	
	public String bRPopLPush(int timeout, String srcKey, String dstKey) {
		return deserialize(delegate.bRPopLPush(timeout, serialize(srcKey), serialize(dstKey)));
	}

	
	public Long decr(String key) {
		return delegate.decr(serialize(key));
	}

	
	public Long decrBy(String key, long value) {
		return delegate.decrBy(serialize(key), value);
	}

	
	public Long del(String... keys) {
		return delegate.del(serializeMulti(keys));
	}

	
	public String echo(String message) {
		return deserialize(delegate.echo(serialize(message)));
	}

	
	public Boolean exists(String key) {
		return delegate.exists(serialize(key));
	}

	
	public Boolean expire(String key, long seconds) {
		return delegate.expire(serialize(key), seconds);
	}

	
	public Boolean expireAt(String key, long unixTime) {
		return delegate.expireAt(serialize(key), unixTime);
	}

	
	public String get(String key) {
		return deserialize(delegate.get(serialize(key)));
	}

	
	public Boolean getBit(String key, long offset) {
		return delegate.getBit(serialize(key), offset);
	}

	
	public String getRange(String key, long start, long end) {
		return deserialize(delegate.getRange(serialize(key), start, end));
	}

	
	public String getSet(String key, String value) {
		return deserialize(delegate.getSet(serialize(key), serialize(value)));
	}

	
	public Boolean hDel(String key, String field) {
		return delegate.hDel(serialize(key), serialize(field));
	}

	
	public Boolean hExists(String key, String field) {
		return delegate.hExists(serialize(key), serialize(field));
	}

	
	public String hGet(String key, String field) {
		return deserialize(delegate.hGet(serialize(key), serialize(field)));
	}

	
	public Map<String, String> hGetAll(String key) {
		throw new UnsupportedOperationException();
	}

	
	public Long hIncrBy(String key, String field, long delta) {
		return delegate.hIncrBy(serialize(key), serialize(field), delta);
	}

	
	public Set<String> hKeys(String key) {
		return deserialize(delegate.hKeys(serialize(key)));
	}

	
	public Long hLen(String key) {
		return delegate.hLen(serialize(key));
	}

	
	public List<String> hMGet(String key, String... fields) {
		return deserialize(delegate.hMGet(serialize(key), serializeMulti(fields)));
	}


	
	public void hMSet(String key, Map<String, String> hashes) {
		delegate.hMSet(serialize(key), serialize(hashes));
	}

	
	public Boolean hSet(String key, String field, String value) {
		return delegate.hSet(serialize(key), serialize(field), serialize(value));
	}

	
	public Boolean hSetNX(String key, String field, String value) {
		return delegate.hSetNX(serialize(key), serialize(field), serialize(value));
	}

	
	public List<String> hVals(String key) {
		return deserialize(delegate.hVals(serialize(key)));
	}

	
	public Long incr(String key) {
		return delegate.incr(serialize(key));
	}

	
	public Long incrBy(String key, long value) {
		return delegate.incrBy(serialize(key), value);
	}

	
	public Collection<String> keys(String pattern) {
		return deserialize(delegate.keys(serialize(pattern)));
	}

	
	public String lIndex(String key, long index) {
		return deserialize(delegate.lIndex(serialize(key), index));
	}

	
	public Long lInsert(String key, Position where, String pivot, String value) {
		return delegate.lInsert(serialize(key), where, serialize(pivot), serialize(value));
	}

	
	public Long lLen(String key) {
		return delegate.lLen(serialize(key));
	}

	
	public String lPop(String key) {
		return deserialize(delegate.lPop(serialize(key)));
	}

	
	public Long lPush(String key, String value) {
		return delegate.lPush(serialize(key), serialize(value));
	}

	
	public Long lPushX(String key, String value) {
		return delegate.lPushX(serialize(key), serialize(value));
	}

	
	public List<String> lRange(String key, long start, long end) {
		return deserialize(delegate.lRange(serialize(key), start, end));
	}

	
	public Long lRem(String key, long count, String value) {
		return delegate.lRem(serialize(key), count, serialize(value));
	}

	
	public void lSet(String key, long index, String value) {
		delegate.lSet(serialize(key), index, serialize(value));
	}

	
	public void lTrim(String key, long start, long end) {
		delegate.lTrim(serialize(key), start, end);
	}

	
	public List<String> mGet(String... keys) {
		return deserialize(delegate.mGet(serializeMulti(keys)));
	}

	
	public void mSetNXString(Map<String, String> tuple) {
		delegate.mSetNX(serialize(tuple));
	}

	
	public void mSetString(Map<String, String> tuple) {
		delegate.mSet(serialize(tuple));
	}

	
	public Boolean persist(String key) {
		return delegate.persist(serialize(key));
	}

	
	public Boolean move(String key, int dbIndex) {
		return delegate.move(serialize(key), dbIndex);
	}

	
	public void pSubscribe(MessageListener listener, String... patterns) {
		delegate.pSubscribe(listener, serializeMulti(patterns));
	}

	
	public Long publish(String channel, String message) {
		return delegate.publish(serialize(channel), serialize(message));
	}

	
	public void rename(String oldName, String newName) {
		delegate.rename(serialize(oldName), serialize(newName));
	}

	
	public Boolean renameNX(String oldName, String newName) {
		return delegate.renameNX(serialize(oldName), serialize(newName));
	}

	
	public String rPop(String key) {
		return deserialize(delegate.rPop(serialize(key)));
	}

	
	public String rPopLPush(String srcKey, String dstKey) {
		return deserialize(delegate.rPopLPush(serialize(srcKey), serialize(dstKey)));
	}

	
	public Long rPush(String key, String value) {
		return delegate.rPush(serialize(key), serialize(value));
	}

	
	public Long rPushX(String key, String value) {
		return delegate.rPushX(serialize(key), serialize(value));
	}

	
	public Boolean sAdd(String key, String value) {
		return delegate.sAdd(serialize(key), serialize(value));
	}

	
	public Long sCard(String key) {
		return delegate.sCard(serialize(key));
	}

	
	public Set<String> sDiff(String... keys) {
		return deserialize(delegate.sDiff(serializeMulti(keys)));
	}

	
	public void sDiffStore(String destKey, String... keys) {
		delegate.sDiffStore(serialize(destKey), serializeMulti(keys));
	}

	
	public void set(String key, String value) {
		delegate.set(serialize(key), serialize(value));
	}

	
	public void setBit(String key, long offset, boolean value) {
		delegate.setBit(serialize(key), offset, value);
	}

	
	public void setEx(String key, long seconds, String value) {
		delegate.setEx(serialize(key), seconds, serialize(value));
	}

	
	public Boolean setNX(String key, String value) {
		return delegate.setNX(serialize(key), serialize(value));
	}

	
	public void setRange(String key, String value, long start) {
		delegate.setRange(serialize(key), serialize(value), start);
	}

	
	public Set<String> sInter(String... keys) {
		return deserialize(delegate.sInter(serializeMulti(keys)));
	}

	
	public void sInterStore(String destKey, String... keys) {
		delegate.sInterStore(serialize(destKey), serializeMulti(keys));
	}

	
	public Boolean sIsMember(String key, String value) {
		return delegate.sIsMember(serialize(key), serialize(value));
	}

	
	public Set<String> sMembers(String key) {
		return deserialize(delegate.sMembers(serialize(key)));
	}

	
	public Boolean sMove(String srcKey, String destKey, String value) {
		return delegate.sMove(serialize(srcKey), serialize(destKey), serialize(value));
	}

	
	public Long sort(String key, SortParameters params, String storeKey) {
		return delegate.sort(serialize(key), params, serialize(storeKey));
	}

	
	public List<String> sort(String key, SortParameters params) {
		return deserialize(delegate.sort(serialize(key), params));
	}

	
	public String sPop(String key) {
		return deserialize(delegate.sPop(serialize(key)));
	}

	
	public String sRandMember(String key) {
		return deserialize(delegate.sRandMember(serialize(key)));
	}

	
	public Boolean sRem(String key, String value) {
		return delegate.sRem(serialize(key), serialize(value));
	}

	
	public Long strLen(String key) {
		return delegate.strLen(serialize(key));
	}

	
	public void subscribe(MessageListener listener, String... channels) {
		delegate.subscribe(listener, serializeMulti(channels));
	}

	
	public Set<String> sUnion(String... keys) {
		return deserialize(delegate.sUnion(serializeMulti(keys)));
	}

	
	public void sUnionStore(String destKey, String... keys) {
		delegate.sUnionStore(serialize(destKey), serializeMulti(keys));
	}

	
	public Long ttl(String key) {
		return delegate.ttl(serialize(key));
	}

	
	public DataType type(String key) {
		return delegate.type(serialize(key));
	}

	
	public Boolean zAdd(String key, double score, String value) {
		return delegate.zAdd(serialize(key), score, serialize(value));
	}

	
	public Long zCard(String key) {
		return delegate.zCard(serialize(key));
	}

	
	public Long zCount(String key, double min, double max) {
		return delegate.zCount(serialize(key), min, max);
	}

	
	public Double zIncrBy(String key, double increment, String value) {
		return delegate.zIncrBy(serialize(key), increment, serialize(value));
	}

	
	public Long zInterStore(String destKey, Aggregate aggregate, int[] weights, String... sets) {
		return delegate.zInterStore(serialize(destKey), aggregate, weights, serializeMulti(sets));
	}

	
	public Long zInterStore(String destKey, String... sets) {
		return delegate.zInterStore(serialize(destKey), serializeMulti(sets));
	}

	
	public Set<String> zRange(String key, long start, long end) {
		return deserialize(delegate.zRange(serialize(key), start, end));
	}

	
	public Set<String> zRangeByScore(String key, double min, double max, long offset, long count) {
		return deserialize(delegate.zRangeByScore(serialize(key), min, max, offset, count));
	}

	
	public Set<String> zRangeByScore(String key, double min, double max) {
		return deserialize(delegate.zRangeByScore(serialize(key), min, max));
	}

	
	public Set<StringTuple> zRangeByScoreWithScores(String key, double min, double max, long offset, long count) {
		return deserializeTuple(delegate.zRangeByScoreWithScores(serialize(key), min, max, offset, count));
	}

	
	public Set<StringTuple> zRangeByScoreWithScores(String key, double min, double max) {
		return deserializeTuple(delegate.zRangeByScoreWithScores(serialize(key), min, max));
	}

	
	public Set<StringTuple> zRangeWithScores(String key, long start, long end) {
		return deserializeTuple(delegate.zRangeWithScores(serialize(key), start, end));
	}

	
	public Long zRank(String key, String value) {
		return delegate.zRank(serialize(key), serialize(value));
	}

	
	public Boolean zRem(String key, String value) {
		return delegate.zRem(serialize(key), serialize(value));
	}

	
	public Long zRemRange(String key, long start, long end) {
		return delegate.zRemRange(serialize(key), start, end);
	}

	
	public Long zRemRangeByScore(String key, double min, double max) {
		return delegate.zRemRangeByScore(serialize(key), min, max);
	}

	
	public Set<String> zRevRange(String key, long start, long end) {
		return deserialize(delegate.zRevRange(serialize(key), start, end));
	}

	
	public Set<StringTuple> zRevRangeWithScores(String key, long start, long end) {
		return deserializeTuple(delegate.zRevRangeWithScores(serialize(key), start, end));
	}

	
	public Long zRevRank(String key, String value) {
		return delegate.zRevRank(serialize(key), serialize(value));
	}

	
	public Double zScore(String key, String value) {
		return delegate.zScore(serialize(key), serialize(value));
	}

	
	public Long zUnionStore(String destKey, Aggregate aggregate, int[] weights, String... sets) {
		return delegate.zUnionStore(serialize(destKey), aggregate, weights, serializeMulti(sets));
	}

	
	public Long zUnionStore(String destKey, String... sets) {
		return delegate.zUnionStore(serialize(destKey), serializeMulti(sets));
	}

	
	public List<Object> closePipeline() {
		return delegate.closePipeline();
	}

	
	public boolean isPipelined() {
		return delegate.isPipelined();
	}

	
	public void openPipeline() {
		delegate.openPipeline();
	}


	public Object execute(String command) {
		return execute(command, (byte[][]) null);
	}

	public Object execute(String command, byte[]... args) {
		return delegate.execute(command, args);
	}

	public Object execute(String command, String... args) {
		return execute(command, serializeMulti(args));
	}
}