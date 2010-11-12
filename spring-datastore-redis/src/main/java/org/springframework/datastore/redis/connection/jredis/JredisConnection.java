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
package org.springframework.datastore.redis.connection.jredis;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jredis.JRedis;
import org.jredis.RedisException;
import org.springframework.dao.DataAccessException;
import org.springframework.datastore.keyvalue.UncategorizedKeyvalueStoreException;
import org.springframework.datastore.redis.UncategorizedRedisException;
import org.springframework.datastore.redis.connection.DataType;
import org.springframework.datastore.redis.connection.RedisConnection;

/**
 * JRedis based implementation.
 * 
 * @author Costin Leau
 */
public class JredisConnection implements RedisConnection {

	private final JRedis jredis;

	private final String charset;

	public JredisConnection(JRedis jredis, String charset) {
		this.jredis = jredis;
		this.charset = charset;
	}

	protected DataAccessException convertJedisAccessException(Exception ex) {
		if (ex instanceof RedisException) {
			return JredisUtils.convertJredisAccessException((RedisException) ex);
		}
		throw new UncategorizedKeyvalueStoreException("Unknown JRedis exception", ex);
	}

	@Override
	public void close() throws UncategorizedRedisException {
		jredis.quit();

	}

	@Override
	public JRedis getNativeConnection() {
		return jredis;
	}

	@Override
	public boolean isClosed() {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean isQueueing() {
		return false;
	}

	@Override
	public Integer dbSize() {
		try {
			return Integer.valueOf((int) jredis.dbsize());
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public Integer del(byte[]... keys) {
		try {
			return Integer.valueOf((int) jredis.del(JredisUtils.convertMultiple(charset, keys)));
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public void discard() {
		try {
			jredis.discard();
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public List<Object> exec() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Boolean exists(byte[] key) {
		try {
			return jredis.exists(JredisUtils.convert(charset, key));
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public Boolean expire(byte[] key, int seconds) {
		try {
			return jredis.expire(JredisUtils.convert(charset, key), seconds);
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public Collection<byte[]> keys(byte[] pattern) {
		try {
			return JredisUtils.convert(charset, jredis.keys(pattern));
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
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
			return JredisUtils.convert(charset, jredis.randomkey());
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public void rename(byte[] oldName, byte[] newName) {
		try {
			jredis.rename(JredisUtils.convert(charset, oldName), JredisUtils.convert(charset, newName));
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public Boolean renameNX(byte[] oldName, byte[] newName) {
		try {
			return jredis.renamenx(JredisUtils.convert(charset, oldName), JredisUtils.convert(charset, newName));
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public void select(int dbIndex) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Integer ttl(byte[] key) {
		try {
			return Integer.valueOf((int) jredis.ttl(JredisUtils.convert(charset, key)));
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public DataType type(byte[] key) {
		try {
			return JredisUtils.convertDataType(jredis.type(JredisUtils.convert(charset, key)));
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
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
			return jredis.get(JredisUtils.convert(charset, key));
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public void set(byte[] key, byte[] value) {
		try {
			jredis.set(JredisUtils.convert(charset, key), value);
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public byte[] getSet(byte[] key, byte[] value) {
		try {
			return jredis.getset(JredisUtils.convert(charset, key), value);
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public Integer append(byte[] key, byte[] value) {
		try {
			return Integer.valueOf((int) jredis.append(JredisUtils.convert(charset, key), value));
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public List<byte[]> mGet(byte[]... keys) {
		try {
			return jredis.mget(JredisUtils.convertMultiple(charset, keys));
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public void mSet(Map<byte[], byte[]> tuple) {
		try {
			jredis.mset(JredisUtils.convert(charset, tuple));
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public void mSetNX(Map<byte[], byte[]> tuple) {
		try {
			jredis.msetnx(JredisUtils.convert(charset, tuple));
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public void setEx(byte[] key, int seconds, byte[] value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Boolean setNX(byte[] key, byte[] value) {
		try {
			return jredis.setnx(JredisUtils.convert(charset, key), value);
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public byte[] substr(byte[] key, int start, int end) {
		try {
			return jredis.substr(JredisUtils.convert(charset, key), (long) start, (long) end);
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public Integer decr(byte[] key) {
		try {
			return (int) jredis.decr(JredisUtils.convert(charset, key));
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public Integer decrBy(byte[] key, int value) {
		try {
			return (int) jredis.decrby(JredisUtils.convert(charset, key), value);
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public Integer incr(byte[] key) {
		try {
			return (int) jredis.incr(JredisUtils.convert(charset, key));
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public Integer incrBy(byte[] key, int value) {
		try {
			return (int) jredis.incrby(JredisUtils.convert(charset, key), value);
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
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
	public byte[] lIndex(byte[] key, int index) {
		try {
			return jredis.lindex(JredisUtils.convert(charset, key), (long) index);
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public Integer lLen(byte[] key) {
		try {
			return Integer.valueOf((int) jredis.llen(JredisUtils.convert(charset, key)));
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public byte[] lPop(byte[] key) {
		try {
			return jredis.lpop(JredisUtils.convert(charset, key));
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public Integer lPush(byte[] key, byte[] value) {
		try {
			jredis.lpush(JredisUtils.convert(charset, key), value);
			return null;
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public List<byte[]> lRange(byte[] key, int start, int end) {
		try {
			List<byte[]> lrange = jredis.lrange(JredisUtils.convert(charset, key), start, end);

			return lrange;
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public Integer lRem(byte[] key, int count, byte[] value) {
		try {
			return Integer.valueOf((int) jredis.lrem(JredisUtils.convert(charset, key), value, count));
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public void lSet(byte[] key, int index, byte[] value) {
		try {
			jredis.lset(JredisUtils.convert(charset, key), index, value);
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public void lTrim(byte[] key, int start, int end) {
		try {
			jredis.ltrim(JredisUtils.convert(charset, key), start, end);
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public byte[] rPop(byte[] key) {
		try {
			return jredis.rpop(JredisUtils.convert(charset, key));
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public byte[] rPopLPush(byte[] srcKey, byte[] dstKey) {
		try {
			return jredis.rpoplpush(JredisUtils.convert(charset, srcKey), JredisUtils.convert(charset, dstKey));
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public Integer rPush(byte[] key, byte[] value) {
		try {
			jredis.rpush(JredisUtils.convert(charset, key), value);
			return null;
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	//
	// Set commands
	//

	@Override
	public Boolean sAdd(byte[] key, byte[] value) {
		try {
			return jredis.sadd(JredisUtils.convert(charset, key), value);
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public Integer sCard(byte[] key) {
		try {
			return Integer.valueOf((int) jredis.scard(JredisUtils.convert(charset, key)));
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public Set<byte[]> sDiff(byte[]... keys) {
		String set1 = JredisUtils.convert(charset, keys[0]);
		String[] sets = JredisUtils.convertMultiple(charset, Arrays.copyOfRange(keys, 1, keys.length));

		try {
			List<byte[]> result = jredis.sdiff(set1, sets);
			return JredisUtils.convertToStringCollection(result, Set.class);
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public void sDiffStore(byte[] destKey, byte[]... keys) {
		String set1 = JredisUtils.convert(charset, keys[0]);
		String[] sets = JredisUtils.convertMultiple(charset, Arrays.copyOfRange(keys, 1, keys.length));

		try {
			jredis.sdiffstore(set1, sets);
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public Set<byte[]> sInter(byte[]... keys) {
		String set1 = JredisUtils.convert(charset, keys[0]);
		String[] sets = JredisUtils.convertMultiple(charset, Arrays.copyOfRange(keys, 1, keys.length));

		try {
			List<byte[]> result = jredis.sinter(set1, sets);
			return JredisUtils.convertToStringCollection(result, Set.class);
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public void sInterStore(byte[] destKey, byte[]... keys) {
		String set1 = JredisUtils.convert(charset, keys[0]);
		String[] sets = JredisUtils.convertMultiple(charset, Arrays.copyOfRange(keys, 1, keys.length));

		try {
			jredis.sinterstore(set1, sets);
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public Boolean sIsMember(byte[] key, byte[] value) {
		try {
			return jredis.sismember(JredisUtils.convert(charset, key), value);
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public Set<byte[]> sMembers(byte[] key) {
		try {
			return new LinkedHashSet<byte[]>(jredis.smembers(JredisUtils.convert(charset, key)));
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public Boolean sMove(byte[] srcKey, byte[] destKey, byte[] value) {
		try {
			return jredis.smove(JredisUtils.convert(charset, srcKey), JredisUtils.convert(charset, destKey), value);
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public byte[] sPop(byte[] key) {
		try {
			return jredis.spop(JredisUtils.convert(charset, key));
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public byte[] sRandMember(byte[] key) {
		try {
			return jredis.srandmember(JredisUtils.convert(charset, key));
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public Boolean sRem(byte[] key, byte[] value) {
		try {
			return jredis.srem(JredisUtils.convert(charset, key), value);
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public Set<byte[]> sUnion(byte[]... keys) {
		String set1 = JredisUtils.convert(charset, keys[0]);
		String[] sets = JredisUtils.convertMultiple(charset, Arrays.copyOfRange(keys, 1, keys.length));

		try {
			return new LinkedHashSet<byte[]>(jredis.sunion(set1, sets));
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public void sUnionStore(byte[] destKey, byte[]... keys) {
		String set1 = JredisUtils.convert(charset, keys[0]);
		String[] sets = JredisUtils.convertMultiple(charset, Arrays.copyOfRange(keys, 1, keys.length));

		try {
			jredis.sunionstore(set1, sets);
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}


	//
	// ZSet commands
	//

	@Override
	public Boolean zAdd(byte[] key, double score, byte[] value) {
		try {
			return jredis.zadd(JredisUtils.convert(charset, key), score, value);
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public Integer zCard(byte[] key) {
		try {
			return Integer.valueOf((int) jredis.zcard(JredisUtils.convert(charset, key)));
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public Integer zCount(byte[] key, double min, double max) {
		try {
			return Integer.valueOf((int) jredis.zcount(JredisUtils.convert(charset, key), min, max));
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public Double zIncrBy(byte[] key, double increment, byte[] value) {
		try {
			return jredis.zincrby(JredisUtils.convert(charset, key), increment, value);
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public Integer zInterStore(byte[] destKey, Aggregate aggregate, int[] weights, byte[]... sets) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Integer zInterStore(byte[] destKey, byte[]... sets) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Set<byte[]> zRange(byte[] key, int start, int end) {
		try {
			return JredisUtils.convertToStringCollection(jredis.zrange(JredisUtils.convert(charset, key), (long) start,
					(long) end), Set.class);
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public Set<Tuple> zRangeWithScore(byte[] key, int start, int end) {
		throw new UnsupportedOperationException();

	}

	@Override
	public Set<byte[]> zRangeByScore(byte[] key, double min, double max) {
		try {
			return JredisUtils.convertToStringCollection(jredis.zrangebyscore(JredisUtils.convert(charset, key), min,
					max), Set.class);
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public Set<Tuple> zRangeByScoreWithScore(byte[] key, double min, double max) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Set<byte[]> zRangeByScore(byte[] key, double min, double max, int offset, int count) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Set<Tuple> zRangeByScoreWithScore(byte[] key, double min, double max, int offset, int count) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Integer zRank(byte[] key, byte[] value) {
		try {
			return Integer.valueOf((int) jredis.zrank(JredisUtils.convert(charset, key), value));
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public Boolean zRem(byte[] key, byte[] value) {
		try {
			return jredis.zrem(JredisUtils.convert(charset, key), value);
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public Integer zRemRange(byte[] key, int start, int end) {
		try {
			return Integer.valueOf((int) jredis.zremrangebyrank(JredisUtils.convert(charset, key), start, end));
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public Integer zRemRangeByScore(byte[] key, double min, double max) {
		try {
			return Integer.valueOf((int) jredis.zremrangebyscore(JredisUtils.convert(charset, key), min, max));
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public Set<byte[]> zRevRange(byte[] key, int start, int end) {
		try {
			return JredisUtils.convertToStringCollection(
					jredis.zrevrange(JredisUtils.convert(charset, key), start, end), Set.class);
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public Set<Tuple> zRevRangeWithScore(byte[] key, int start, int end) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Integer zRevRank(byte[] key, byte[] value) {
		try {
			return Integer.valueOf((int) jredis.zrevrank(JredisUtils.convert(charset, key), value));
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public Double zScore(byte[] key, byte[] value) {
		try {
			return jredis.zscore(JredisUtils.convert(charset, key), value);
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}


	//
	// Hash commands
	//

	@Override
	public Integer zUnionStore(byte[] destKey, Aggregate aggregate, int[] weights, byte[]... sets) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Integer zUnionStore(byte[] destKey, byte[]... sets) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Boolean hDel(byte[] key, byte[] field) {
		try {
			return jredis.hdel(JredisUtils.convert(charset, key), JredisUtils.convert(charset, field));
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public Boolean hExists(byte[] key, byte[] field) {
		try {
			return jredis.hexists(JredisUtils.convert(charset, key), JredisUtils.convert(charset, field));
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public byte[] hGet(byte[] key, byte[] field) {
		try {
			return jredis.hget(JredisUtils.convert(charset, key), JredisUtils.convert(charset, field));
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public Set<Entry> hGetAll(byte[] key) {
		try {
			return JredisUtils.convert(jredis.hgetall(JredisUtils.convert(charset, key)));
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public Integer hIncrBy(byte[] key, byte[] field, int delta) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Set<byte[]> hKeys(byte[] key) {
		try {
			return new LinkedHashSet<byte[]>(JredisUtils.convert(charset,
					jredis.hkeys(JredisUtils.convert(charset, key))));
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public Integer hLen(byte[] key) {
		try {
			return Integer.valueOf((int) jredis.hlen(JredisUtils.convert(charset, key)));
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public List<byte[]> hMGet(byte[] key, byte[]... fields) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void hMSet(byte[] key, byte[][] fields, byte[][] values) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Boolean hSet(byte[] key, byte[] field, byte[] value) {
		try {
			return jredis.hset(JredisUtils.convert(charset, key), JredisUtils.convert(charset, field), value);
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public Boolean hSetNX(byte[] key, byte[] field, byte[] value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public List<byte[]> hVals(byte[] key) {
		try {
			return jredis.hvals(JredisUtils.convert(charset, key));
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}
}