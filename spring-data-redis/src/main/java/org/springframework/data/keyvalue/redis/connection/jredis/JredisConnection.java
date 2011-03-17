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
package org.springframework.data.keyvalue.redis.connection.jredis;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.jredis.ClientRuntimeException;
import org.jredis.JRedis;
import org.jredis.RedisException;
import org.jredis.Sort;
import org.jredis.Query.Support;
import org.jredis.ri.alphazero.JRedisService;
import org.springframework.dao.DataAccessException;
import org.springframework.data.keyvalue.UncategorizedKeyvalueStoreException;
import org.springframework.data.keyvalue.redis.RedisSystemException;
import org.springframework.data.keyvalue.redis.connection.DataType;
import org.springframework.data.keyvalue.redis.connection.MessageListener;
import org.springframework.data.keyvalue.redis.connection.RedisConnection;
import org.springframework.data.keyvalue.redis.connection.SortParameters;
import org.springframework.data.keyvalue.redis.connection.Subscription;
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

		return new UncategorizedKeyvalueStoreException("Unknown JRedis exception", ex);
	}

	@Override
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

	@Override
	public JRedis getNativeConnection() {
		return jredis;
	}

	@Override
	public boolean isClosed() {
		return isClosed;
	}

	@Override
	public boolean isQueueing() {
		return false;
	}

	@Override
	public boolean isPipelined() {
		return false;
	}

	@Override
	public void openPipeline() {
		throw new UnsupportedOperationException("Pipelining not supported by JRedis");
	}

	@Override
	public List<byte[]> closePipeline() {
		return Collections.emptyList();
	}

	@Override
	public List<byte[]> sort(byte[] key, SortParameters params) {
		Sort sort = jredis.sort(JredisUtils.decode(key));
		JredisUtils.applySortingParams(sort, params, null);
		try {
			return sort.exec();
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	@Override
	public Long sort(byte[] key, SortParameters params, byte[] storeKey) {
		Sort sort = jredis.sort(JredisUtils.decode(key));
		JredisUtils.applySortingParams(sort, params, null);
		try {
			return Support.unpackValue(sort.exec());
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	@Override
	public Long dbSize() {
		try {
			return jredis.dbsize();
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	@Override
	public void flushDb() {
		try {
			jredis.flushdb();
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	@Override
	public void flushAll() {
		try {
			jredis.flushall();
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	@Override
	public byte[] echo(byte[] message) {
		try {
			return jredis.echo(message);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	@Override
	public String ping() {
		try {
			jredis.ping();
			return "PONG";
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	@Override
	public void bgSave() {
		try {
			jredis.bgsave();
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	@Override
	public void bgWriteAof() {
		try {
			jredis.bgrewriteaof();
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	@Override
	public void save() {
		try {
			jredis.save();
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	@Override
	public List<String> getConfig(String pattern) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Properties info() {
		try {
			return JredisUtils.info(jredis.info());
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	@Override
	public Long lastSave() {
		try {
			return jredis.lastsave();
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	@Override
	public void setConfig(String param, String value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void resetConfigStats() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void shutdown() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Long del(byte[]... keys) {
		try {
			return jredis.del(JredisUtils.decodeMultiple(keys));
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	@Override
	public void discard() {
		try {
			jredis.discard();
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	@Override
	public List<Object> exec() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Boolean exists(byte[] key) {
		try {
			return jredis.exists(JredisUtils.decode(key));
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	@Override
	public Boolean expire(byte[] key, long seconds) {
		try {
			return jredis.expire(JredisUtils.decode(key), (int) seconds);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	@Override
	public Boolean expireAt(byte[] key, long unixTime) {
		try {
			return jredis.expireat(JredisUtils.decode(key), unixTime);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	@Override
	public Set<byte[]> keys(byte[] pattern) {
		try {
			return JredisUtils.convertToSet(jredis.keys(JredisUtils.decode(pattern)));
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	@Override
	public void multi() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Boolean persist(byte[] key) {
		throw new UnsupportedOperationException();
	}

	@Override
	public byte[] randomKey() {
		try {
			return JredisUtils.encode(jredis.randomkey());
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	@Override
	public void rename(byte[] oldName, byte[] newName) {
		try {
			jredis.rename(JredisUtils.decode(oldName), JredisUtils.decode(newName));
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	@Override
	public Boolean renameNX(byte[] oldName, byte[] newName) {
		try {
			return jredis.renamenx(JredisUtils.decode(oldName), JredisUtils.decode(newName));
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	@Override
	public void select(int dbIndex) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Long ttl(byte[] key) {
		try {
			return jredis.ttl(JredisUtils.decode(key));
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	@Override
	public DataType type(byte[] key) {
		try {
			return JredisUtils.convertDataType(jredis.type(JredisUtils.decode(key)));
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	@Override
	public void unwatch() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void watch(byte[]... keys) {
		throw new UnsupportedOperationException();
	}

	//
	// String operations
	//

	@Override
	public byte[] get(byte[] key) {
		try {
			return jredis.get(JredisUtils.decode(key));
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	@Override
	public void set(byte[] key, byte[] value) {
		try {
			jredis.set(JredisUtils.decode(key), value);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	@Override
	public byte[] getSet(byte[] key, byte[] value) {
		try {
			return jredis.getset(JredisUtils.decode(key), value);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	@Override
	public Long append(byte[] key, byte[] value) {
		try {
			return jredis.append(JredisUtils.decode(key), value);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	@Override
	public List<byte[]> mGet(byte[]... keys) {
		try {
			return jredis.mget(JredisUtils.decodeMultiple(keys));
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	@Override
	public void mSet(Map<byte[], byte[]> tuple) {
		try {
			jredis.mset(JredisUtils.decodeMap(tuple));
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	@Override
	public void mSetNX(Map<byte[], byte[]> tuple) {
		try {
			jredis.msetnx(JredisUtils.decodeMap(tuple));
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	@Override
	public void setEx(byte[] key, long seconds, byte[] value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Boolean setNX(byte[] key, byte[] value) {
		try {
			return jredis.setnx(JredisUtils.decode(key), value);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	@Override
	public byte[] getRange(byte[] key, long start, long end) {
		try {
			return jredis.substr(JredisUtils.decode(key), start, end);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	@Override
	public Long decr(byte[] key) {
		try {
			return jredis.decr(JredisUtils.decode(key));
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	@Override
	public Long decrBy(byte[] key, long value) {
		try {
			return jredis.decrby(JredisUtils.decode(key), (int) value);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	@Override
	public Long incr(byte[] key) {
		try {
			return jredis.incr(JredisUtils.decode(key));
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	@Override
	public Long incrBy(byte[] key, long value) {
		try {
			return jredis.incrby(JredisUtils.decode(key), (int) value);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	@Override
	public Boolean getBit(byte[] key, long offset) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setBit(byte[] key, long offset, boolean value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setRange(byte[] key, byte[] value, long start) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Long strLen(byte[] key) {
		throw new UnsupportedOperationException();
	}

	//
	// List commands
	//

	@Override
	public List<byte[]> bLPop(int timeout, byte[]... keys) {
		throw new UnsupportedOperationException();
	}

	@Override
	public List<byte[]> bRPop(int timeout, byte[]... keys) {
		throw new UnsupportedOperationException();
	}

	@Override
	public byte[] lIndex(byte[] key, long index) {
		try {
			return jredis.lindex(JredisUtils.decode(key), index);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	@Override
	public Long lLen(byte[] key) {
		try {
			return jredis.llen(JredisUtils.decode(key));
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	@Override
	public byte[] lPop(byte[] key) {
		try {
			return jredis.lpop(JredisUtils.decode(key));
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	@Override
	public Long lPush(byte[] key, byte[] value) {
		try {
			jredis.lpush(JredisUtils.decode(key), value);
			return null;
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	@Override
	public List<byte[]> lRange(byte[] key, long start, long end) {
		try {
			List<byte[]> lrange = jredis.lrange(JredisUtils.decode(key), start, end);

			return lrange;
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	@Override
	public Long lRem(byte[] key, long count, byte[] value) {
		try {
			return jredis.lrem(JredisUtils.decode(key), value, (int) count);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	@Override
	public void lSet(byte[] key, long index, byte[] value) {
		try {
			jredis.lset(JredisUtils.decode(key), index, value);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	@Override
	public void lTrim(byte[] key, long start, long end) {
		try {
			jredis.ltrim(JredisUtils.decode(key), start, end);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	@Override
	public byte[] rPop(byte[] key) {
		try {
			return jredis.rpop(JredisUtils.decode(key));
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	@Override
	public byte[] rPopLPush(byte[] srcKey, byte[] dstKey) {
		try {
			return jredis.rpoplpush(JredisUtils.decode(srcKey), JredisUtils.decode(dstKey));
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	@Override
	public Long rPush(byte[] key, byte[] value) {
		try {
			jredis.rpush(JredisUtils.decode(key), value);
			return null;
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	@Override
	public Long lInsert(byte[] key, Position where, byte[] pivot, byte[] value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public byte[] bRPopLPush(int timeout, byte[] srcKey, byte[] dstKey) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Long lPushX(byte[] key, byte[] value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Long rPushX(byte[] key, byte[] value) {
		throw new UnsupportedOperationException();
	}


	//
	// Set commands
	//

	@Override
	public Boolean sAdd(byte[] key, byte[] value) {
		try {
			return jredis.sadd(JredisUtils.decode(key), value);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	@Override
	public Long sCard(byte[] key) {
		try {
			return jredis.scard(JredisUtils.decode(key));
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	@Override
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

	@Override
	public void sDiffStore(byte[] destKey, byte[]... keys) {
		String destSet = JredisUtils.decode(destKey);
		String[] sets = JredisUtils.decodeMultiple(keys);

		try {
			jredis.sdiffstore(destSet, sets);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	@Override
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

	@Override
	public void sInterStore(byte[] destKey, byte[]... keys) {
		String destSet = JredisUtils.decode(destKey);
		String[] sets = JredisUtils.decodeMultiple(keys);

		try {
			jredis.sinterstore(destSet, sets);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	@Override
	public Boolean sIsMember(byte[] key, byte[] value) {
		try {
			return jredis.sismember(JredisUtils.decode(key), value);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	@Override
	public Set<byte[]> sMembers(byte[] key) {
		try {
			return new LinkedHashSet<byte[]>(jredis.smembers(JredisUtils.decode(key)));
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	@Override
	public Boolean sMove(byte[] srcKey, byte[] destKey, byte[] value) {
		try {
			return jredis.smove(JredisUtils.decode(srcKey), JredisUtils.decode(destKey), value);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	@Override
	public byte[] sPop(byte[] key) {
		try {
			return jredis.spop(JredisUtils.decode(key));
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	@Override
	public byte[] sRandMember(byte[] key) {
		try {
			return jredis.srandmember(JredisUtils.decode(key));
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	@Override
	public Boolean sRem(byte[] key, byte[] value) {
		try {
			return jredis.srem(JredisUtils.decode(key), value);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	@Override
	public Set<byte[]> sUnion(byte[]... keys) {
		String set1 = JredisUtils.decode(keys[0]);
		String[] sets = JredisUtils.decodeMultiple(Arrays.copyOfRange(keys, 1, keys.length));

		try {
			return new LinkedHashSet<byte[]>(jredis.sunion(set1, sets));
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	@Override
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

	@Override
	public Boolean zAdd(byte[] key, double score, byte[] value) {
		try {
			return jredis.zadd(JredisUtils.decode(key), score, value);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	@Override
	public Long zCard(byte[] key) {
		try {
			return jredis.zcard(JredisUtils.decode(key));
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	@Override
	public Long zCount(byte[] key, double min, double max) {
		try {
			return jredis.zcount(JredisUtils.decode(key), min, max);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	@Override
	public Double zIncrBy(byte[] key, double increment, byte[] value) {
		try {
			return jredis.zincrby(JredisUtils.decode(key), increment, value);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	@Override
	public Long zInterStore(byte[] destKey, Aggregate aggregate, int[] weights, byte[]... sets) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Long zInterStore(byte[] destKey, byte[]... sets) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Set<byte[]> zRange(byte[] key, long start, long end) {
		try {
			return new LinkedHashSet<byte[]>(jredis.zrange(JredisUtils.decode(key), start, end));
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	@Override
	public Set<Tuple> zRangeWithScore(byte[] key, long start, long end) {
		throw new UnsupportedOperationException();

	}

	@Override
	public Set<byte[]> zRangeByScore(byte[] key, double min, double max) {
		try {
			return new LinkedHashSet<byte[]>(jredis.zrangebyscore(JredisUtils.decode(key), min, max));
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	@Override
	public Set<Tuple> zRangeByScoreWithScore(byte[] key, double min, double max) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Set<byte[]> zRangeByScore(byte[] key, double min, double max, long offset, long count) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Set<Tuple> zRangeByScoreWithScore(byte[] key, double min, double max, long offset, long count) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Long zRank(byte[] key, byte[] value) {
		try {
			return jredis.zrank(JredisUtils.decode(key), value);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	@Override
	public Boolean zRem(byte[] key, byte[] value) {
		try {
			return jredis.zrem(JredisUtils.decode(key), value);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	@Override
	public Long zRemRange(byte[] key, long start, long end) {
		try {
			return jredis.zremrangebyrank(JredisUtils.decode(key), start, end);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	@Override
	public Long zRemRangeByScore(byte[] key, double min, double max) {
		try {
			return jredis.zremrangebyscore(JredisUtils.decode(key), min, max);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	@Override
	public Set<byte[]> zRevRange(byte[] key, long start, long end) {
		try {
			return new LinkedHashSet<byte[]>(jredis.zrevrange(JredisUtils.decode(key), start, end));
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	@Override
	public Set<Tuple> zRevRangeWithScore(byte[] key, long start, long end) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Long zRevRank(byte[] key, byte[] value) {
		try {
			return jredis.zrevrank(JredisUtils.decode(key), value);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	@Override
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

	@Override
	public Long zUnionStore(byte[] destKey, Aggregate aggregate, int[] weights, byte[]... sets) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Long zUnionStore(byte[] destKey, byte[]... sets) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Boolean hDel(byte[] key, byte[] field) {
		try {
			return jredis.hdel(JredisUtils.decode(key), JredisUtils.decode(field));
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	@Override
	public Boolean hExists(byte[] key, byte[] field) {
		try {
			return jredis.hexists(JredisUtils.decode(key), JredisUtils.decode(field));
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	@Override
	public byte[] hGet(byte[] key, byte[] field) {
		try {
			return jredis.hget(JredisUtils.decode(key), JredisUtils.decode(field));
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	@Override
	public Map<byte[], byte[]> hGetAll(byte[] key) {
		try {
			return JredisUtils.encodeMap(jredis.hgetall(JredisUtils.decode(key)));
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	@Override
	public Long hIncrBy(byte[] key, byte[] field, long delta) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Set<byte[]> hKeys(byte[] key) {
		try {
			return new LinkedHashSet<byte[]>(JredisUtils.convertToSet(jredis.hkeys(JredisUtils.decode(key))));
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	@Override
	public Long hLen(byte[] key) {
		try {
			return jredis.hlen(JredisUtils.decode(key));
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	@Override
	public List<byte[]> hMGet(byte[] key, byte[]... fields) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void hMSet(byte[] key, Map<byte[], byte[]> values) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Boolean hSet(byte[] key, byte[] field, byte[] value) {
		try {
			return jredis.hset(JredisUtils.decode(key), JredisUtils.decode(field), value);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	@Override
	public Boolean hSetNX(byte[] key, byte[] field, byte[] value) {
		throw new UnsupportedOperationException();
	}

	@Override
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

	@Override
	public Subscription getSubscription() {
		return null;
	}

	@Override
	public boolean isSubscribed() {
		return false;
	}

	@Override
	public void pSubscribe(MessageListener listener, byte[]... patterns) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Long publish(byte[] channel, byte[] message) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void subscribe(MessageListener listener, byte[]... channels) {
		throw new UnsupportedOperationException();
	}
}