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
package org.springframework.datastore.redis.connection.jedis;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.dao.DataAccessException;
import org.springframework.datastore.keyvalue.UncategorizedKeyvalueStoreException;
import org.springframework.datastore.redis.UncategorizedRedisException;
import org.springframework.datastore.redis.connection.DataType;
import org.springframework.datastore.redis.connection.RedisConnection;
import org.springframework.util.ReflectionUtils;

import redis.clients.jedis.Client;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisException;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.ZParams;

/**
 * Jedis based {@link RedisConnection}.
 * 
 * @author Costin Leau
 */
public class JedisConnection implements RedisConnection {

	private static final Field CLIENT_FIELD;

	static {
		CLIENT_FIELD = ReflectionUtils.findField(Jedis.class, "client", Client.class);
		ReflectionUtils.makeAccessible(CLIENT_FIELD);
	}

	private final Jedis jedis;
	private final Client client;
	private final Transaction transaction;

	public JedisConnection(Jedis jedis) {
		this.jedis = jedis;
		// extract underlying connection for batch operations
		client = (Client) ReflectionUtils.getField(CLIENT_FIELD, jedis);
		transaction = new Transaction(client);
	}

	protected DataAccessException convertJedisAccessException(Exception ex) {
		if (ex instanceof JedisException) {
			return JedisUtils.convertJedisAccessException((JedisException) ex);
		}
		if (ex instanceof IOException) {
			return JedisUtils.convertJedisAccessException((IOException) ex);
		}

		throw new UncategorizedKeyvalueStoreException("Unknown jedis exception", ex);
	}

	@Override
	public void close() throws UncategorizedRedisException {
		try {
			if (isQueueing()) {
				client.quit();
				client.disconnect();
			}
			jedis.quit();
			jedis.disconnect();
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Jedis getNativeConnection() {
		return jedis;
	}

	@Override
	public boolean isClosed() {
		try {
			return !jedis.isConnected();
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public boolean isQueueing() {
		return client.isInMulti();
	}

	@Override
	public Integer dbSize() {
		try {
			if (isQueueing()) {
				transaction.dbSize();
				return null;
			}
			return jedis.dbSize();
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Integer del(String... keys) {
		try {
			if (isQueueing()) {
				transaction.del(keys);
				return null;
			}
			return jedis.del(keys);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public void discard() {
		try {
			client.discard();
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public List<Object> exec() {
		try {
			return transaction.exec();
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Boolean exists(String key) {
		try {
			if (isQueueing()) {
				transaction.exists(key);
				return null;
			}
			return (jedis.exists(key) == 1);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Boolean expire(String key, int seconds) {
		try {
			if (isQueueing()) {
				transaction.expire(key, seconds);
				return null;
			}
			return (jedis.expire(key, (int) seconds) == 1);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Collection<String> keys(String pattern) {
		try {
			if (isQueueing()) {
				transaction.keys(pattern);
				return null;
			}
			return (jedis.keys(pattern));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public void multi() {
		try {
			client.multi();
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Boolean persist(String key) {
		try {
			if (isQueueing()) {
				client.persist(key);
				return null;
			}
			return (jedis.persist(key) == 1);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public String randomKey() {
		try {
			if (isQueueing()) {
				transaction.randomKey();
				return null;
			}
			return jedis.randomKey();
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public void rename(String oldName, String newName) {
		try {
			if (isQueueing()) {
				transaction.rename(oldName, newName);
			}
			jedis.rename(oldName, newName);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Boolean renameNX(String oldName, String newName) {
		try {
			if (isQueueing()) {
				transaction.renamenx(oldName, newName);
				return null;
			}
			return (jedis.renamenx(oldName, newName) == 1);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public void select(int dbIndex) {
		try {
			if (isQueueing()) {
				transaction.select(dbIndex);
			}
			jedis.select(dbIndex);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Integer ttl(String key) {
		try {
			if (isQueueing()) {
				transaction.ttl(key);
				return null;
			}
			return jedis.ttl(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public DataType type(String key) {
		try {
			if (isQueueing()) {
				transaction.type(key);
				return null;
			}
			return DataType.fromCode(jedis.type(key));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public void unwatch() {
		try {
			jedis.unwatch();
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public void watch(String... keys) {
		if (isQueueing()) {
			// ignore (as watch not allowed in multi)
			return;
		}

		try {
			for (String key : keys) {
				jedis.watch(key);
			}
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	//
	// String commands
	//

	@Override
	public String get(String key) {
		try {
			if (isQueueing()) {
				transaction.get(key);
				return null;
			}

			return jedis.get(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public void set(String key, String value) {
		try {
			jedis.set(key, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}


	@Override
	public String getSet(String key, String value) {
		try {
			if (isQueueing()) {
				transaction.getSet(key, value);
				return null;
			}
			return jedis.getSet(key, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Integer append(String key, String value) {
		try {
			if (isQueueing()) {
				transaction.append(key, value);
				return null;
			}
			return jedis.append(key, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public List<String> mGet(String... keys) {
		try {
			if (isQueueing()) {
				transaction.mget(keys);
				return null;
			}
			return jedis.mget(keys);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public void mSet(String[] keys, String[] values) {
		try {
			if (isQueueing()) {
				transaction.mset(JedisUtils.arrange(keys, values));
			}
			jedis.mset(JedisUtils.arrange(keys, values));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public void mSetNX(String[] keys, String[] values) {
		try {
			if (isQueueing()) {
				transaction.msetnx(JedisUtils.arrange(keys, values));
			}
			jedis.msetnx(JedisUtils.arrange(keys, values));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public void setEx(String key, int time, String value) {
		try {
			if (isQueueing()) {
				transaction.setex(key, time, value);
			}
			jedis.setex(key, time, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Boolean setNX(String key, String value) {
		try {
			if (isQueueing()) {
				transaction.setnx(key, value);
			}
			return JedisUtils.convertCodeReply(jedis.setnx(key, value));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public String substr(String key, int start, int end) {
		try {
			if (isQueueing()) {
				transaction.substr(key, start, end);
				return null;
			}
			return jedis.substr(key, start, end);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Integer decr(String key) {
		try {
			if (isQueueing()) {
				transaction.decr(key);
				return null;
			}
			return jedis.decr(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Integer decrBy(String key, int value) {
		try {
			if (isQueueing()) {
				transaction.decrBy(key, value);
				return null;
			}
			return jedis.decrBy(key, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Integer incr(String key) {
		try {
			if (isQueueing()) {
				transaction.incr(key);
				return null;
			}
			return jedis.incr(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Integer incrBy(String key, int value) {
		try {
			if (isQueueing()) {
				transaction.incrBy(key, value);
				return null;
			}
			return jedis.incrBy(key, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	//
	// List commands
	//


	@Override
	public Integer lPush(String key, String value) {
		try {
			if (isQueueing()) {
				transaction.lpush(key, value);
				return null;
			}
			return jedis.lpush(key, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Integer rPush(String key, String value) {
		try {
			if (isQueueing()) {
				transaction.rpush(key, value);
				return null;
			}
			return jedis.rpush(key, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public List<String> bLPop(int timeout, String... keys) {
		try {
			if (isQueueing()) {
				throw new UnsupportedOperationException();
			}
			return jedis.blpop(timeout, keys);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public List<String> bRPop(int timeout, String... keys) {
		try {
			if (isQueueing()) {
				throw new UnsupportedOperationException();
			}
			return jedis.brpop(timeout, keys);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public String lIndex(String key, int index) {
		try {
			if (isQueueing()) {
				transaction.lindex(key, index);
				return null;
			}
			return jedis.lindex(key, index);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Integer lLen(String key) {
		try {
			if (isQueueing()) {
				transaction.llen(key);
				return null;
			}
			return jedis.llen(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public String lPop(String key) {
		try {
			if (isQueueing()) {
				transaction.lpop(key);
				return null;
			}
			return jedis.lpop(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public List<String> lRange(String key, int start, int end) {
		try {
			if (isQueueing()) {
				transaction.lrange(key, start, end);
				return null;
			}
			return jedis.lrange(key, start, end);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Integer lRem(String key, int count, String value) {
		try {
			if (isQueueing()) {
				transaction.lrem(key, count, value);
				return null;
			}
			return jedis.lrem(key, count, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public void lSet(String key, int index, String value) {
		try {
			if (isQueueing()) {
				transaction.lset(key, index, value);
			}
			jedis.lset(key, index, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public void lTrim(String key, int start, int end) {
		try {
			if (isQueueing()) {
				transaction.ltrim(key, start, end);
			}
			jedis.ltrim(key, start, end);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public String rPop(String key) {
		try {
			if (isQueueing()) {
				transaction.rpop(key);
				return null;
			}
			return jedis.lpop(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public String rPopLPush(String srcKey, String dstKey) {
		try {
			if (isQueueing()) {
				transaction.rpoplpush(srcKey, dstKey);
				return null;
			}
			return jedis.rpoplpush(srcKey, dstKey);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}


	//
	// Set commands
	//

	@Override
	public Boolean sAdd(String key, String value) {
		try {
			if (isQueueing()) {
				transaction.sadd(key, value);
				return null;
			}
			return (jedis.sadd(key, value) == 1);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Integer sCard(String key) {
		try {
			if (isQueueing()) {
				transaction.scard(key);
				return null;
			}
			return jedis.scard(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Set<String> sDiff(String... keys) {
		try {
			if (isQueueing()) {
				transaction.sdiff(keys);
				return null;
			}
			return jedis.sdiff(keys);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public void sDiffStore(String destKey, String... keys) {
		try {
			if (isQueueing()) {
				transaction.sdiffstore(destKey, keys);
			}
			jedis.sdiffstore(destKey, keys);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Set<String> sInter(String... keys) {
		try {
			if (isQueueing()) {
				transaction.sinter(keys);
				return null;
			}
			return jedis.sinter(keys);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public void sInterStore(String destKey, String... keys) {
		try {
			if (isQueueing()) {
				transaction.sinterstore(destKey, keys);
			}
			jedis.sinterstore(destKey, keys);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Boolean sIsMember(String key, String value) {
		try {
			if (isQueueing()) {
				transaction.sismember(key, value);
				return null;
			}
			return JedisUtils.convertCodeReply(jedis.sismember(key, value));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Set<String> sMembers(String key) {
		try {
			if (isQueueing()) {
				transaction.smembers(key);
				return null;
			}
			return jedis.smembers(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Boolean sMove(String srcKey, String destKey, String value) {
		try {
			if (isQueueing()) {
				transaction.smove(srcKey, destKey, value);
				return null;
			}
			return JedisUtils.convertCodeReply(jedis.smove(srcKey, destKey, value));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public String sPop(String key) {
		try {
			if (isQueueing()) {
				transaction.spop(key);
				return null;
			}
			return jedis.spop(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public String sRandMember(String key) {
		try {
			if (isQueueing()) {
				transaction.srandmember(key);
				return null;
			}
			return jedis.srandmember(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Boolean sRem(String key, String value) {
		try {
			if (isQueueing()) {
				transaction.srem(key, value);
				return null;
			}
			return JedisUtils.convertCodeReply(jedis.srem(key, value));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Set<String> sUnion(String... keys) {
		try {
			if (isQueueing()) {
				transaction.sunion(keys);
				return null;
			}
			return jedis.sunion(keys);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public void sUnionStore(String destKey, String... keys) {
		try {
			if (isQueueing()) {
				transaction.sunionstore(destKey, keys);
			}
			jedis.sunionstore(destKey, keys);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	//
	// ZSet commands
	//

	@Override
	public Boolean zAdd(String key, double score, String value) {
		try {
			if (isQueueing()) {
				transaction.zadd(key, score, value);
				return null;
			}
			return JedisUtils.convertCodeReply(jedis.zadd(key, score, value));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Integer zCard(String key) {
		try {
			if (isQueueing()) {
				transaction.zcard(key);
				return null;
			}
			return jedis.zcard(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Integer zCount(String key, double min, double max) {
		try {
			if (isQueueing()) {
				throw new UnsupportedOperationException();
			}
			return jedis.zcount(key, min, max);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Double zIncrBy(String key, double increment, String value) {
		try {
			if (isQueueing()) {
				transaction.zincrby(key, increment, value);
				return null;
			}
			return jedis.zincrby(key, increment, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Integer zInterStore(String destKey, Aggregate aggregate, int[] weights, String... sets) {
		try {
			if (isQueueing()) {
				throw new UnsupportedOperationException();
			}
			ZParams zparams = new ZParams().weights(weights).aggregate(
					redis.clients.jedis.ZParams.Aggregate.valueOf(aggregate.name()));
			return jedis.zinterstore(destKey, zparams, sets);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Integer zInterStore(String destKey, String... sets) {
		try {
			if (isQueueing()) {
				throw new UnsupportedOperationException();
			}
			return jedis.zinterstore(destKey, sets);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Set<String> zRange(String key, int start, int end) {
		try {
			if (isQueueing()) {
				transaction.zrange(key, start, end);
				return null;
			}
			return jedis.zrange(key, start, end);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Set<Tuple> zRangeWithScore(String key, int start, int end) {
		try {
			if (isQueueing()) {
				transaction.zrangeWithScores(key, start, end);
				return null;
			}
			return JedisUtils.convertJedisTuple(jedis.zrangeWithScores(key, start, end));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Set<String> zRangeByScore(String key, double min, double max) {
		try {
			if (isQueueing()) {
				throw new UnsupportedOperationException();
			}
			return jedis.zrangeByScore(key, min, max);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Set<Tuple> zRangeByScoreWithScore(String key, double min, double max) {
		try {
			if (isQueueing()) {
				throw new UnsupportedOperationException();
			}
			return JedisUtils.convertJedisTuple(jedis.zrangeByScoreWithScores(key, min, max));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Set<Tuple> zRevRangeWithScore(String key, int start, int end) {
		try {
			if (isQueueing()) {
				transaction.zrangeWithScores(key, start, end);
				return null;
			}
			return JedisUtils.convertJedisTuple(jedis.zrangeByScoreWithScores(key, start, end));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Set<String> zRangeByScore(String key, double min, double max, int offset, int count) {
		try {
			if (isQueueing()) {
				throw new UnsupportedOperationException();
			}
			return jedis.zrangeByScore(key, min, max, offset, count);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Set<Tuple> zRangeByScoreWithScore(String key, double min, double max, int offset, int count) {
		try {
			if (isQueueing()) {
				throw new UnsupportedOperationException();
			}
			return JedisUtils.convertJedisTuple(jedis.zrangeByScoreWithScores(key, min, max, offset, count));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Integer zRank(String key, String value) {
		try {
			if (isQueueing()) {
				transaction.zrank(key, value);
				return null;
			}
			return jedis.zrank(key, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Boolean zRem(String key, String value) {
		try {
			if (isQueueing()) {
				transaction.zrem(key, value);
				return null;
			}
			return JedisUtils.convertCodeReply(jedis.zrem(key, value));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Integer zRemRange(String key, int start, int end) {
		try {
			if (isQueueing()) {
				throw new UnsupportedOperationException();
			}
			return jedis.zremrangeByRank(key, start, end);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Integer zRemRangeByScore(String key, double min, double max) {
		try {
			if (isQueueing()) {
				throw new UnsupportedOperationException();
			}
			return jedis.zremrangeByScore(key, min, max);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Set<String> zRevRange(String key, int start, int end) {
		try {
			if (isQueueing()) {
				transaction.zrevrange(key, start, end);
				return null;
			}
			return jedis.zrevrange(key, start, end);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Integer zRevRank(String key, String value) {
		try {
			if (isQueueing()) {
				transaction.zrevrank(key, value);
				return null;
			}
			return jedis.zrevrank(key, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Double zScore(String key, String value) {
		try {
			if (isQueueing()) {
				transaction.zscore(key, value);
				return null;
			}
			return jedis.zscore(key, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Integer zUnionStore(String destKey, Aggregate aggregate, int[] weights, String... sets) {
		try {
			if (isQueueing()) {
				throw new UnsupportedOperationException();
			}
			ZParams zparams = new ZParams().weights(weights).aggregate(
					redis.clients.jedis.ZParams.Aggregate.valueOf(aggregate.name()));
			return jedis.zunionstore(destKey, zparams, sets);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Integer zUnionStore(String destKey, String... sets) {
		try {
			if (isQueueing()) {
				throw new UnsupportedOperationException();
			}
			return jedis.zunionstore(destKey, sets);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	//
	// Hash commands
	//

	@Override
	public Boolean hSet(String key, String field, String value) {
		try {
			if (isQueueing()) {
				transaction.hset(key, field, value);
				return null;
			}
			return JedisUtils.convertCodeReply(jedis.hset(key, field, value));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Boolean hSetNX(String key, String field, String value) {
		try {
			if (isQueueing()) {
				transaction.hsetnx(key, field, value);
				return null;
			}
			return JedisUtils.convertCodeReply(jedis.hsetnx(key, field, value));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Boolean hDel(String key, String field) {
		try {
			if (isQueueing()) {
				transaction.hdel(key, field);
				return null;
			}
			return JedisUtils.convertCodeReply(jedis.hdel(key, field));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Boolean hExists(String key, String field) {
		try {
			if (isQueueing()) {
				transaction.hexists(key, field);
				return null;
			}
			return JedisUtils.convertCodeReply(jedis.hexists(key, field));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public String hGet(String key, String field) {
		try {
			if (isQueueing()) {
				transaction.hget(key, field);
				return null;
			}
			return jedis.hget(key, field);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Set<Entry> hGetAll(String key) {
		try {
			if (isQueueing()) {
				transaction.hgetAll(key);
				return null;
			}
			return JedisUtils.convert(jedis.hgetAll(key));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Integer hIncrBy(String key, String field, int delta) {
		try {
			if (isQueueing()) {
				transaction.hincrBy(key, field, delta);
				return null;
			}
			return jedis.hincrBy(key, field, delta);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Set<String> hKeys(String key) {
		try {
			if (isQueueing()) {
				transaction.hkeys(key);
				return null;
			}
			return new LinkedHashSet<String>(jedis.hkeys(key));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Integer hLen(String key) {
		try {
			if (isQueueing()) {
				transaction.hlen(key);
				return null;
			}
			return jedis.hlen(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public List<String> hMGet(String key, String... fields) {
		try {
			if (isQueueing()) {
				transaction.hmget(key, fields);
				return null;
			}
			return jedis.hmget(key, fields);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public void hMSet(String key, String[] fields, String[] values) {
		Map<String, String> param = JedisUtils.convert(fields, values);
		try {
			if (isQueueing()) {
				transaction.hmset(key, param);
			}
			jedis.hmset(key, param);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public List<String> hVals(String key) {
		try {
			if (isQueueing()) {
				transaction.hvals(key);
				return null;
			}
			return jedis.hvals(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}
}