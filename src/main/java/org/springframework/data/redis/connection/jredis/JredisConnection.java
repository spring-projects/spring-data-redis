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
package org.springframework.data.redis.connection.jredis;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.jredis.ClientRuntimeException;
import org.jredis.JRedis;
import org.jredis.Query.Support;
import org.jredis.RedisException;
import org.jredis.Sort;
import org.jredis.ri.alphazero.JRedisService;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.SortParameters;
import org.springframework.data.redis.connection.Subscription;
import org.springframework.util.Assert;

/**
 * {@code RedisConnection} implementation on top of <a href="http://github.com/alphazero/jredis">JRedis</a> library.
 * 
 * @author Costin Leau
 */
public class JredisConnection implements RedisConnection {

	private final JRedis jredis;
	private final boolean isPool;
	private boolean isClosed = false;

	/**
	 * Constructs a new <code>JredisConnection</code> instance.
	 *
	 * @param jredis JRedis connection
	 */
	public JredisConnection(JRedis jredis) {
		Assert.notNull(jredis, "a not-null instance required");
		this.jredis = jredis;
		// required since Jredis combines the pool and the connection under the same interface/class
		this.isPool = (jredis instanceof JRedisService);
	}

	protected DataAccessException convertJredisAccessException(Exception ex) {
		if (ex instanceof RedisException) {
			return JredisUtils.convertJredisAccessException((RedisException) ex);
		}

		if (ex instanceof ClientRuntimeException) {
			return JredisUtils.convertJredisAccessException((ClientRuntimeException) ex);
		}

		return new RedisSystemException("Unknown JRedis exception", ex);
	}

	
	public void close() throws RedisSystemException {
		isClosed = true;

		// don't actually close the connection
		// if a pool is used
		if (!isPool) {
			try {
				jredis.quit();
			} catch (Exception ex) {
				throw convertJredisAccessException(ex);
			}
		}
	}

	
	public JRedis getNativeConnection() {
		return jredis;
	}

	
	public boolean isClosed() {
		return isClosed;
	}

	
	public boolean isQueueing() {
		return false;
	}

	
	public boolean isPipelined() {
		return false;
	}

	
	public void openPipeline() {
		throw new UnsupportedOperationException("Pipelining not supported by JRedis");
	}

	
	public List<Object> closePipeline() {
		return Collections.emptyList();
	}

	
	public List<byte[]> sort(byte[] key, SortParameters params) {
		Sort sort = jredis.sort(JredisUtils.decode(key));
		JredisUtils.applySortingParams(sort, params, null);
		try {
			return sort.exec();
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	
	public Long sort(byte[] key, SortParameters params, byte[] storeKey) {
		Sort sort = jredis.sort(JredisUtils.decode(key));
		JredisUtils.applySortingParams(sort, params, null);
		try {
			return Support.unpackValue(sort.exec());
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	
	public Long dbSize() {
		try {
			return jredis.dbsize();
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	
	public void flushDb() {
		try {
			jredis.flushdb();
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	
	public void flushAll() {
		try {
			jredis.flushall();
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	
	public byte[] echo(byte[] message) {
		try {
			return jredis.echo(message);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	
	public String ping() {
		try {
			jredis.ping();
			return "PONG";
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	
	public void bgSave() {
		try {
			jredis.bgsave();
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	
	public void bgWriteAof() {
		try {
			jredis.bgrewriteaof();
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	
	public void save() {
		try {
			jredis.save();
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	
	public List<String> getConfig(String pattern) {
		throw new UnsupportedOperationException();
	}

	
	public Properties info() {
		try {
			return JredisUtils.info(jredis.info());
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	
	public Long lastSave() {
		try {
			return jredis.lastsave();
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	
	public void setConfig(String param, String value) {
		throw new UnsupportedOperationException();
	}

	
	public void resetConfigStats() {
		throw new UnsupportedOperationException();
	}

	
	public void shutdown() {
		throw new UnsupportedOperationException();
	}

	
	public Long del(byte[]... keys) {
		try {
			return jredis.del(JredisUtils.decodeMultiple(keys));
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	
	public void discard() {
		try {
			jredis.discard();
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	
	public List<Object> exec() {
		throw new UnsupportedOperationException();
	}

	
	public Boolean exists(byte[] key) {
		try {
			return jredis.exists(JredisUtils.decode(key));
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	
	public Boolean expire(byte[] key, long seconds) {
		try {
			return jredis.expire(JredisUtils.decode(key), (int) seconds);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	
	public Boolean expireAt(byte[] key, long unixTime) {
		try {
			return jredis.expireat(JredisUtils.decode(key), unixTime);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	
	public Set<byte[]> keys(byte[] pattern) {
		try {
			return JredisUtils.convertToSet(jredis.keys(JredisUtils.decode(pattern)));
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	
	public void multi() {
		throw new UnsupportedOperationException();
	}

	
	public Boolean persist(byte[] key) {
		throw new UnsupportedOperationException();
	}


	
	public Boolean move(byte[] key, int dbIndex) {
		try {
			return jredis.move(JredisUtils.decode(key), dbIndex);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	
	public byte[] randomKey() {
		try {
			return JredisUtils.encode(jredis.randomkey());
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	
	public void rename(byte[] oldName, byte[] newName) {
		try {
			jredis.rename(JredisUtils.decode(oldName), JredisUtils.decode(newName));
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	
	public Boolean renameNX(byte[] oldName, byte[] newName) {
		try {
			return jredis.renamenx(JredisUtils.decode(oldName), JredisUtils.decode(newName));
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	
	public void select(int dbIndex) {
		throw new UnsupportedOperationException();
	}

	
	public Long ttl(byte[] key) {
		try {
			return jredis.ttl(JredisUtils.decode(key));
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	
	public DataType type(byte[] key) {
		try {
			return JredisUtils.convertDataType(jredis.type(JredisUtils.decode(key)));
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	
	public void unwatch() {
		throw new UnsupportedOperationException();
	}

	
	public void watch(byte[]... keys) {
		throw new UnsupportedOperationException();
	}

	//
	// String operations
	//

	
	public byte[] get(byte[] key) {
		try {
			return jredis.get(JredisUtils.decode(key));
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	
	public void set(byte[] key, byte[] value) {
		try {
			jredis.set(JredisUtils.decode(key), value);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	
	public byte[] getSet(byte[] key, byte[] value) {
		try {
			return jredis.getset(JredisUtils.decode(key), value);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	
	public Long append(byte[] key, byte[] value) {
		try {
			return jredis.append(JredisUtils.decode(key), value);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	
	public List<byte[]> mGet(byte[]... keys) {
		try {
			return jredis.mget(JredisUtils.decodeMultiple(keys));
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	
	public void mSet(Map<byte[], byte[]> tuple) {
		try {
			jredis.mset(JredisUtils.decodeMap(tuple));
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	
	public void mSetNX(Map<byte[], byte[]> tuple) {
		try {
			jredis.msetnx(JredisUtils.decodeMap(tuple));
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	
	public void setEx(byte[] key, long seconds, byte[] value) {
		throw new UnsupportedOperationException();
	}

	
	public Boolean setNX(byte[] key, byte[] value) {
		try {
			return jredis.setnx(JredisUtils.decode(key), value);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	
	public byte[] getRange(byte[] key, long start, long end) {
		try {
			return jredis.substr(JredisUtils.decode(key), start, end);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	
	public Long decr(byte[] key) {
		try {
			return jredis.decr(JredisUtils.decode(key));
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	
	public Long decrBy(byte[] key, long value) {
		try {
			return jredis.decrby(JredisUtils.decode(key), (int) value);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	
	public Long incr(byte[] key) {
		try {
			return jredis.incr(JredisUtils.decode(key));
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	
	public Long incrBy(byte[] key, long value) {
		try {
			return jredis.incrby(JredisUtils.decode(key), (int) value);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	
	public Boolean getBit(byte[] key, long offset) {
		throw new UnsupportedOperationException();
	}

	
	public void setBit(byte[] key, long offset, boolean value) {
		throw new UnsupportedOperationException();
	}

	
	public void setRange(byte[] key, byte[] value, long start) {
		throw new UnsupportedOperationException();
	}

	
	public Long strLen(byte[] key) {
		throw new UnsupportedOperationException();
	}

	//
	// List commands
	//

	
	public List<byte[]> bLPop(int timeout, byte[]... keys) {
		throw new UnsupportedOperationException();
	}

	
	public List<byte[]> bRPop(int timeout, byte[]... keys) {
		throw new UnsupportedOperationException();
	}

	
	public byte[] lIndex(byte[] key, long index) {
		try {
			return jredis.lindex(JredisUtils.decode(key), index);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	
	public Long lLen(byte[] key) {
		try {
			return jredis.llen(JredisUtils.decode(key));
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	
	public byte[] lPop(byte[] key) {
		try {
			return jredis.lpop(JredisUtils.decode(key));
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	
	public Long lPush(byte[] key, byte[] value) {
		try {
			jredis.lpush(JredisUtils.decode(key), value);
			return null;
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	
	public List<byte[]> lRange(byte[] key, long start, long end) {
		try {
			List<byte[]> lrange = jredis.lrange(JredisUtils.decode(key), start, end);

			return lrange;
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	
	public Long lRem(byte[] key, long count, byte[] value) {
		try {
			return jredis.lrem(JredisUtils.decode(key), value, (int) count);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	
	public void lSet(byte[] key, long index, byte[] value) {
		try {
			jredis.lset(JredisUtils.decode(key), index, value);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	
	public void lTrim(byte[] key, long start, long end) {
		try {
			jredis.ltrim(JredisUtils.decode(key), start, end);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	
	public byte[] rPop(byte[] key) {
		try {
			return jredis.rpop(JredisUtils.decode(key));
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	
	public byte[] rPopLPush(byte[] srcKey, byte[] dstKey) {
		try {
			return jredis.rpoplpush(JredisUtils.decode(srcKey), JredisUtils.decode(dstKey));
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	
	public Long rPush(byte[] key, byte[] value) {
		try {
			jredis.rpush(JredisUtils.decode(key), value);
			return null;
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	
	public Long lInsert(byte[] key, Position where, byte[] pivot, byte[] value) {
		throw new UnsupportedOperationException();
	}

	
	public byte[] bRPopLPush(int timeout, byte[] srcKey, byte[] dstKey) {
		throw new UnsupportedOperationException();
	}

	
	public Long lPushX(byte[] key, byte[] value) {
		throw new UnsupportedOperationException();
	}

	
	public Long rPushX(byte[] key, byte[] value) {
		throw new UnsupportedOperationException();
	}


	//
	// Set commands
	//

	
	public Boolean sAdd(byte[] key, byte[] value) {
		try {
			return jredis.sadd(JredisUtils.decode(key), value);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	
	public Long sCard(byte[] key) {
		try {
			return jredis.scard(JredisUtils.decode(key));
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	
	public Set<byte[]> sDiff(byte[]... keys) {
		String destKey = JredisUtils.decode(keys[0]);
		String[] sets = JredisUtils.decodeMultiple(Arrays.copyOfRange(keys, 1, keys.length));

		try {
			List<byte[]> result = jredis.sdiff(destKey, sets);
			return new LinkedHashSet<byte[]>(result);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	
	public void sDiffStore(byte[] destKey, byte[]... keys) {
		String destSet = JredisUtils.decode(destKey);
		String[] sets = JredisUtils.decodeMultiple(keys);

		try {
			jredis.sdiffstore(destSet, sets);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	
	public Set<byte[]> sInter(byte[]... keys) {
		String set1 = JredisUtils.decode(keys[0]);
		String[] sets = JredisUtils.decodeMultiple(Arrays.copyOfRange(keys, 1, keys.length));

		try {
			List<byte[]> result = jredis.sinter(set1, sets);
			return new LinkedHashSet<byte[]>(result);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	
	public void sInterStore(byte[] destKey, byte[]... keys) {
		String destSet = JredisUtils.decode(destKey);
		String[] sets = JredisUtils.decodeMultiple(keys);

		try {
			jredis.sinterstore(destSet, sets);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	
	public Boolean sIsMember(byte[] key, byte[] value) {
		try {
			return jredis.sismember(JredisUtils.decode(key), value);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	
	public Set<byte[]> sMembers(byte[] key) {
		try {
			return new LinkedHashSet<byte[]>(jredis.smembers(JredisUtils.decode(key)));
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	
	public Boolean sMove(byte[] srcKey, byte[] destKey, byte[] value) {
		try {
			return jredis.smove(JredisUtils.decode(srcKey), JredisUtils.decode(destKey), value);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	
	public byte[] sPop(byte[] key) {
		try {
			return jredis.spop(JredisUtils.decode(key));
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	
	public byte[] sRandMember(byte[] key) {
		try {
			return jredis.srandmember(JredisUtils.decode(key));
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	
	public Boolean sRem(byte[] key, byte[] value) {
		try {
			return jredis.srem(JredisUtils.decode(key), value);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	
	public Set<byte[]> sUnion(byte[]... keys) {
		String set1 = JredisUtils.decode(keys[0]);
		String[] sets = JredisUtils.decodeMultiple(Arrays.copyOfRange(keys, 1, keys.length));

		try {
			return new LinkedHashSet<byte[]>(jredis.sunion(set1, sets));
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	
	public void sUnionStore(byte[] destKey, byte[]... keys) {
		String destSet = JredisUtils.decode(destKey);
		String[] sets = JredisUtils.decodeMultiple(keys);

		try {
			jredis.sunionstore(destSet, sets);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}


	//
	// ZSet commands
	//

	
	public Boolean zAdd(byte[] key, double score, byte[] value) {
		try {
			return jredis.zadd(JredisUtils.decode(key), score, value);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	
	public Long zCard(byte[] key) {
		try {
			return jredis.zcard(JredisUtils.decode(key));
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	
	public Long zCount(byte[] key, double min, double max) {
		try {
			return jredis.zcount(JredisUtils.decode(key), min, max);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	
	public Double zIncrBy(byte[] key, double increment, byte[] value) {
		try {
			return jredis.zincrby(JredisUtils.decode(key), increment, value);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	
	public Long zInterStore(byte[] destKey, Aggregate aggregate, int[] weights, byte[]... sets) {
		throw new UnsupportedOperationException();
	}

	
	public Long zInterStore(byte[] destKey, byte[]... sets) {
		throw new UnsupportedOperationException();
	}

	
	public Set<byte[]> zRange(byte[] key, long start, long end) {
		try {
			return new LinkedHashSet<byte[]>(jredis.zrange(JredisUtils.decode(key), start, end));
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	
	public Set<Tuple> zRangeWithScores(byte[] key, long start, long end) {
		throw new UnsupportedOperationException();
	}

	
	public Set<byte[]> zRangeByScore(byte[] key, double min, double max) {
		try {
			return new LinkedHashSet<byte[]>(jredis.zrangebyscore(JredisUtils.decode(key), min, max));
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	
	public Set<Tuple> zRangeByScoreWithScores(byte[] key, double min, double max) {
		throw new UnsupportedOperationException();
	}

	
	public Set<byte[]> zRangeByScore(byte[] key, double min, double max, long offset, long count) {
		throw new UnsupportedOperationException();
	}

	
	public Set<Tuple> zRangeByScoreWithScores(byte[] key, double min, double max, long offset, long count) {
		throw new UnsupportedOperationException();
	}

	
	public Set<byte[]> zRevRangeByScore(byte[] key, double min, double max, long offset, long count) {
		throw new UnsupportedOperationException();
	}

	
	public Set<byte[]> zRevRangeByScore(byte[] key, double min, double max) {
		throw new UnsupportedOperationException();
	}

	
	public Set<Tuple> zRevRangeByScoreWithScores(byte[] key, double min, double max, long offset, long count) {
		throw new UnsupportedOperationException();
	}

	
	public Set<Tuple> zRevRangeByScoreWithScores(byte[] key, double min, double max) {
		throw new UnsupportedOperationException();
	}

	
	public Long zRank(byte[] key, byte[] value) {
		try {
			return jredis.zrank(JredisUtils.decode(key), value);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	
	public Boolean zRem(byte[] key, byte[] value) {
		try {
			return jredis.zrem(JredisUtils.decode(key), value);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	
	public Long zRemRange(byte[] key, long start, long end) {
		try {
			return jredis.zremrangebyrank(JredisUtils.decode(key), start, end);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	
	public Long zRemRangeByScore(byte[] key, double min, double max) {
		try {
			return jredis.zremrangebyscore(JredisUtils.decode(key), min, max);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	
	public Set<byte[]> zRevRange(byte[] key, long start, long end) {
		try {
			return new LinkedHashSet<byte[]>(jredis.zrevrange(JredisUtils.decode(key), start, end));
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	
	public Set<Tuple> zRevRangeWithScores(byte[] key, long start, long end) {
		throw new UnsupportedOperationException();
	}

	
	public Long zRevRank(byte[] key, byte[] value) {
		try {
			return jredis.zrevrank(JredisUtils.decode(key), value);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	
	public Double zScore(byte[] key, byte[] value) {
		try {
			return jredis.zscore(JredisUtils.decode(key), value);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}


	//
	// Hash commands
	//

	
	public Long zUnionStore(byte[] destKey, Aggregate aggregate, int[] weights, byte[]... sets) {
		throw new UnsupportedOperationException();
	}

	
	public Long zUnionStore(byte[] destKey, byte[]... sets) {
		throw new UnsupportedOperationException();
	}

	
	public Boolean hDel(byte[] key, byte[] field) {
		try {
			return jredis.hdel(JredisUtils.decode(key), JredisUtils.decode(field));
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	
	public Boolean hExists(byte[] key, byte[] field) {
		try {
			return jredis.hexists(JredisUtils.decode(key), JredisUtils.decode(field));
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	
	public byte[] hGet(byte[] key, byte[] field) {
		try {
			return jredis.hget(JredisUtils.decode(key), JredisUtils.decode(field));
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	
	public Map<byte[], byte[]> hGetAll(byte[] key) {
		try {
			return JredisUtils.encodeMap(jredis.hgetall(JredisUtils.decode(key)));
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	
	public Long hIncrBy(byte[] key, byte[] field, long delta) {
		throw new UnsupportedOperationException();
	}

	
	public Set<byte[]> hKeys(byte[] key) {
		try {
			return new LinkedHashSet<byte[]>(JredisUtils.convertToSet(jredis.hkeys(JredisUtils.decode(key))));
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	
	public Long hLen(byte[] key) {
		try {
			return jredis.hlen(JredisUtils.decode(key));
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	
	public List<byte[]> hMGet(byte[] key, byte[]... fields) {
		throw new UnsupportedOperationException();
	}

	
	public void hMSet(byte[] key, Map<byte[], byte[]> values) {
		throw new UnsupportedOperationException();
	}

	
	public Boolean hSet(byte[] key, byte[] field, byte[] value) {
		try {
			return jredis.hset(JredisUtils.decode(key), JredisUtils.decode(field), value);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	
	public Boolean hSetNX(byte[] key, byte[] field, byte[] value) {
		throw new UnsupportedOperationException();
	}

	
	public List<byte[]> hVals(byte[] key) {
		try {
			return jredis.hvals(JredisUtils.decode(key));
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	//
	// PubSub commands
	//

	
	public Subscription getSubscription() {
		return null;
	}

	
	public boolean isSubscribed() {
		return false;
	}

	
	public void pSubscribe(MessageListener listener, byte[]... patterns) {
		throw new UnsupportedOperationException();
	}

	
	public Long publish(byte[] channel, byte[] message) {
		throw new UnsupportedOperationException();
	}

	
	public void subscribe(MessageListener listener, byte[]... channels) {
		throw new UnsupportedOperationException();
	}
}