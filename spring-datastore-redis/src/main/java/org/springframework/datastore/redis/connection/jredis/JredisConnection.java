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
import java.util.Set;

import org.jredis.JRedis;
import org.jredis.RedisException;
import org.springframework.dao.DataAccessException;
import org.springframework.datastore.keyvalue.UncategorizedKeyvalueStoreException;
import org.springframework.datastore.redis.UncategorizedRedisException;
import org.springframework.datastore.redis.connection.DataType;
import org.springframework.datastore.redis.connection.RedisConnection;

/**
 * @author Costin Leau
 */
public class JredisConnection implements RedisConnection {

	private final JRedis jredis;
	private final String encoding;

	public JredisConnection(JRedis jredis, String encoding) {
		this.jredis = jredis;
		this.encoding = encoding;
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
	public String getEncoding() {
		return encoding;
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
	public Integer del(String... keys) {
		try {
			return Integer.valueOf((int) jredis.del(keys));
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
	public Boolean exists(String key) {
		try {
			return jredis.exists(key);
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public Boolean expire(String key, int seconds) {
		try {
			return jredis.expire(key, seconds);
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public Collection<String> keys(String pattern) {
		try {
			return jredis.keys(pattern);
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public void multi() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Boolean persist(String key) {
		throw new UnsupportedOperationException();
	}

	@Override
	public String randomKey() {
		try {
			return jredis.randomkey();
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public void rename(String oldName, String newName) {
		try {
			jredis.rename(oldName, newName);
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public Boolean renameNx(String oldName, String newName) {
		try {
			return jredis.renamenx(oldName, newName);
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public void select(int dbIndex) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Integer ttl(String key) {
		try {
			return Integer.valueOf((int) jredis.ttl(key));
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public DataType type(String key) {
		try {
			return JredisUtils.convertDataType(jredis.type(key));
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public void unwatch() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void watch(String... keys) {
		throw new UnsupportedOperationException();
	}

	@Override
	public String get(String key) {
		try {
			return JredisUtils.convertToString(jredis.get(key), encoding);
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public void set(String key, String value) {
		try {
			jredis.set(key, value);
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public String getSet(String key, String value) {
		try {
			return JredisUtils.convertToString(jredis.getset(key, value), encoding);
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public Integer decr(String key) {
		try {
			return (int) jredis.decr(key);
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public Integer decrBy(String key, int value) {
		try {
			return (int) jredis.decrby(key, value);
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public Integer incr(String key) {
		try {
			return (int) jredis.incr(key);
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public Integer incrBy(String key, int value) {
		try {
			return (int) jredis.incrby(key, value);
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	//
	// List commands
	//

	@Override
	public List<String> bLPop(int timeout, String... keys) {
		throw new UnsupportedOperationException();
	}

	@Override
	public List<String> bRPop(int timeout, String... keys) {
		throw new UnsupportedOperationException();
	}

	@Override
	public String lIndex(String key, int index) {
		try {
			return JredisUtils.convertToString(jredis.lindex(key, (long) index), encoding);
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public Integer lLen(String key) {
		try {
			return Integer.valueOf((int) jredis.llen(key));
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public String lPop(String key) {
		try {
			return JredisUtils.convertToString(jredis.lpop(key), encoding);
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public Integer lPush(String key, String value) {
		try {
			jredis.lpush(key, value);
			return null;
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public List<String> lRange(String key, int start, int end) {
		try {
			List<byte[]> lrange = jredis.lrange(key, start, end);

			return JredisUtils.convertToStringCollection(lrange, encoding, List.class);
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public Integer lRem(String key, int count, String value) {
		try {
			Integer.valueOf((int) jredis.lrem(key, value, count));
			return null;
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public void lSet(String key, int index, String value) {
		try {
			jredis.lset(key, index, value);
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public void lTrim(String key, int start, int end) {
		try {
			jredis.ltrim(key, start, end);
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public String rPop(String key) {
		try {
			return JredisUtils.convertToString(jredis.rpop(key), encoding);
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public String rPopLPush(String srcKey, String dstKey) {
		try {
			return JredisUtils.convertToString(jredis.rpoplpush(srcKey, dstKey), encoding);
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public Integer rPush(String key, String value) {
		try {
			jredis.rpush(key, value);
			return null;
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	//
	// Set commands
	//

	@Override
	public Boolean sAdd(String key, String value) {
		try {
			return jredis.sadd(key, value);
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public Integer sCard(String key) {
		try {
			return Integer.valueOf((int) jredis.scard(key));
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public Set<String> sDiff(String... keys) {
		String set1 = keys[0];
		String[] sets = Arrays.copyOfRange(keys, 1, keys.length);

		try {
			List<byte[]> result = jredis.sdiff(set1, sets);
			return JredisUtils.convertToStringCollection(result, encoding, Set.class);
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public void sDiffStore(String destKey, String... keys) {
		String set1 = keys[0];
		String[] sets = Arrays.copyOfRange(keys, 1, keys.length);

		try {
			jredis.sdiffstore(set1, sets);
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public Set<String> sInter(String... keys) {
		String set1 = keys[0];
		String[] sets = Arrays.copyOfRange(keys, 1, keys.length);

		try {
			List<byte[]> result = jredis.sinter(set1, sets);
			return JredisUtils.convertToStringCollection(result, encoding, Set.class);
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public void sInterStore(String destKey, String... keys) {
		String set1 = keys[0];
		String[] sets = Arrays.copyOfRange(keys, 1, keys.length);

		try {
			jredis.sinterstore(set1, sets);
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public Boolean sIsMember(String key, String value) {
		try {
			return jredis.sismember(key, value);
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public Set<String> sMembers(String key) {
		try {
			return JredisUtils.convertToStringCollection(jredis.smembers(key), encoding, Set.class);
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public Boolean sMove(String srcKey, String destKey, String value) {
		try {
			return jredis.smove(srcKey, destKey, value);
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public String sPop(String key) {
		try {
			return JredisUtils.convertToString(jredis.spop(key), encoding);
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public String sRandMember(String key) {
		try {
			return JredisUtils.convertToString(jredis.srandmember(key), encoding);
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public Boolean sRem(String key, String value) {
		try {
			return jredis.srem(key, value);
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public Set<String> sUnion(String... keys) {
		String set1 = keys[0];
		String[] sets = Arrays.copyOfRange(keys, 1, keys.length);

		try {
			List<byte[]> result = jredis.sunion(set1, sets);
			return JredisUtils.convertToStringCollection(result, encoding, Set.class);
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public void sUnionStore(String destKey, String... keys) {
		String set1 = keys[0];
		String[] sets = Arrays.copyOfRange(keys, 1, keys.length);

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
	public Boolean zAdd(String key, double score, String value) {
		try {
			return jredis.zadd(key, score, value);
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public Integer zCard(String key) {
		try {
			return Integer.valueOf((int) jredis.zcard(key));
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public Integer zCount(String key, double min, double max) {
		try {
			return Integer.valueOf((int) jredis.zcount(key, min, max));
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public Double zIncrBy(String key, double increment, String value) {
		try {
			return jredis.zincrby(key, increment, value);
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public Integer zInterStore(String destKey, Aggregate aggregate, int[] weights, String... sets) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Integer zInterStore(String destKey, String... sets) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Set<String> zRange(String key, int start, int end) {
		try {
			return JredisUtils.convertToStringCollection(jredis.zrange(key, (long) start, (long) end), encoding,
					Set.class);
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public Set<Tuple> zRangeWithScore(String key, int start, int end) {
		throw new UnsupportedOperationException();

	}

	@Override
	public Set<String> zRangeByScore(String key, double min, double max) {
		try {
			return JredisUtils.convertToStringCollection(jredis.zrangebyscore(key, min, max), encoding, Set.class);
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public Set<Tuple> zRangeByScoreWithScore(String key, double min, double max) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Set<String> zRangeByScore(String key, double min, double max, int offset, int count) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Set<Tuple> zRangeByScoreWithScore(String key, double min, double max, int offset, int count) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Integer zRank(String key, String value) {
		try {
			return Integer.valueOf((int) jredis.zrank(key, value));
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public Boolean zRem(String key, String value) {
		try {
			return jredis.zrem(key, value);
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public Integer zRemRange(String key, int start, int end) {
		try {
			return Integer.valueOf((int) jredis.zremrangebyrank(key, start, end));
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public Integer zRemRangeByScore(String key, double min, double max) {
		try {
			return Integer.valueOf((int) jredis.zremrangebyscore(key, min, max));
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public Set<String> zRevRange(String key, int start, int end) {
		try {
			return JredisUtils.convertToStringCollection(jredis.zrevrange(key, start, end), encoding, Set.class);
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public Set<Tuple> zRevRangeWithScore(String key, int start, int end) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Integer zRevRank(String key, String value) {
		try {
			return Integer.valueOf((int) jredis.zrevrank(key, value));
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public Double zScore(String key, String value) {
		try {
			return jredis.zscore(key, value);
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}


	//
	// Hash commands
	//

	@Override
	public Integer zUnionStore(String destKey, Aggregate aggregate, int[] weights, String... sets) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Integer zUnionStore(String destKey, String... sets) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Boolean hDel(String key, String field) {
		try {
			return jredis.hdel(key, field);
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public Boolean hExists(String key, String field) {
		try {
			return jredis.hexists(key, field);
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public String hGet(String key, String field) {
		try {
			return JredisUtils.convertToString(jredis.hget(key, field), encoding);
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public Set<Entry> hGetAll(String key) {
		try {
			return JredisUtils.convert(jredis.hgetall(key), encoding);
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public Integer hIncrBy(String key, String field, int delta) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Set<String> hKeys(String key) {
		try {
			return new LinkedHashSet<String>(jredis.hkeys(key));
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public Integer hLen(String key) {
		try {
			return Integer.valueOf((int) jredis.hlen(key));
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public List<String> hMGet(String key, String... fields) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void hMSet(String key, String[] fields, String[] values) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Boolean hSet(String key, String field, String value) {
		try {
			return jredis.hset(key, field, value);
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}

	@Override
	public List<String> hVals(String key) {
		try {
			return JredisUtils.convertToStringCollection(jredis.hvals(key), encoding, List.class);
		} catch (RedisException ex) {
			throw JredisUtils.convertJredisAccessException(ex);
		}
	}
}