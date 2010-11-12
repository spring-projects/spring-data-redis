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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.dao.DataAccessException;
import org.springframework.datastore.keyvalue.UncategorizedKeyvalueStoreException;
import org.springframework.datastore.redis.UncategorizedRedisException;
import org.springframework.datastore.redis.connection.DataType;
import org.springframework.datastore.redis.connection.RedisConnection;
import org.springframework.util.ReflectionUtils;

import redis.clients.jedis.BinaryJedis;
import redis.clients.jedis.BinaryTransaction;
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
		CLIENT_FIELD = ReflectionUtils.findField(BinaryJedis.class, "client", Client.class);
		ReflectionUtils.makeAccessible(CLIENT_FIELD);
	}

	private final Jedis jedis;
	private final Client client;
	private final BinaryTransaction transaction;

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
	public Integer del(byte[]... keys) {
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
	public Boolean exists(byte[] key) {
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
	public Boolean expire(byte[] key, int seconds) {
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
	public Collection<byte[]> keys(byte[] pattern) {
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
	public Boolean persist(byte[] key) {
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
	public byte[] randomKey() {
		try {
			if (isQueueing()) {
				transaction.randomBinaryKey();
				return null;
			}
			return jedis.randomBinaryKey();
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public void rename(byte[] oldName, byte[] newName) {
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
	public Boolean renameNX(byte[] oldName, byte[] newName) {
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
	public Integer ttl(byte[] key) {
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
	public DataType type(byte[] key) {
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
	public void watch(byte[]... keys) {
		if (isQueueing()) {
			// ignore (as watch not allowed in multi)
			return;
		}

		try {
			for (byte[] key : keys) {
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
	public byte[] get(byte[] key) {
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
	public void set(byte[] key, byte[] value) {
		try {
			jedis.set(key, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}


	@Override
	public byte[] getSet(byte[] key, byte[] value) {
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
	public Integer append(byte[] key, byte[] value) {
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
	public List<byte[]> mGet(byte[]... keys) {
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
	public void mSet(Map<byte[], byte[]> tuples) {
		try {
			if (isQueueing()) {
				transaction.mset(JedisUtils.convert(tuples));
			}
			jedis.mset(JedisUtils.convert(tuples));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public void mSetNX(Map<byte[], byte[]> tuples) {
		try {
			if (isQueueing()) {
				transaction.msetnx(JedisUtils.convert(tuples));
			}
			jedis.msetnx(JedisUtils.convert(tuples));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public void setEx(byte[] key, int time, byte[] value) {
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
	public Boolean setNX(byte[] key, byte[] value) {
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
	public byte[] substr(byte[] key, int start, int end) {
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
	public Integer decr(byte[] key) {
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
	public Integer decrBy(byte[] key, int value) {
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
	public Integer incr(byte[] key) {
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
	public Integer incrBy(byte[] key, int value) {
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
	public Integer lPush(byte[] key, byte[] value) {
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
	public Integer rPush(byte[] key, byte[] value) {
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
	public List<byte[]> bLPop(int timeout, byte[]... keys) {
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
	public List<byte[]> bRPop(int timeout, byte[]... keys) {
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
	public byte[] lIndex(byte[] key, int index) {
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
	public Integer lLen(byte[] key) {
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
	public byte[] lPop(byte[] key) {
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
	public List<byte[]> lRange(byte[] key, int start, int end) {
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
	public Integer lRem(byte[] key, int count, byte[] value) {
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
	public void lSet(byte[] key, int index, byte[] value) {
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
	public void lTrim(byte[] key, int start, int end) {
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
	public byte[] rPop(byte[] key) {
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
	public byte[] rPopLPush(byte[] srcKey, byte[] dstKey) {
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
	public Boolean sAdd(byte[] key, byte[] value) {
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
	public Integer sCard(byte[] key) {
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
	public Set<byte[]> sDiff(byte[]... keys) {
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
	public void sDiffStore(byte[] destKey, byte[]... keys) {
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
	public Set<byte[]> sInter(byte[]... keys) {
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
	public void sInterStore(byte[] destKey, byte[]... keys) {
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
	public Boolean sIsMember(byte[] key, byte[] value) {
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
	public Set<byte[]> sMembers(byte[] key) {
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
	public Boolean sMove(byte[] srcKey, byte[] destKey, byte[] value) {
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
	public byte[] sPop(byte[] key) {
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
	public byte[] sRandMember(byte[] key) {
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
	public Boolean sRem(byte[] key, byte[] value) {
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
	public Set<byte[]> sUnion(byte[]... keys) {
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
	public void sUnionStore(byte[] destKey, byte[]... keys) {
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
	public Boolean zAdd(byte[] key, double score, byte[] value) {
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
	public Integer zCard(byte[] key) {
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
	public Integer zCount(byte[] key, double min, double max) {
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
	public Double zIncrBy(byte[] key, double increment, byte[] value) {
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
	public Integer zInterStore(byte[] destKey, Aggregate aggregate, int[] weights, byte[]... sets) {
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
	public Integer zInterStore(byte[] destKey, byte[]... sets) {
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
	public Set<byte[]> zRange(byte[] key, int start, int end) {
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
	public Set<Tuple> zRangeWithScore(byte[] key, int start, int end) {
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
	public Set<byte[]> zRangeByScore(byte[] key, double min, double max) {
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
	public Set<Tuple> zRangeByScoreWithScore(byte[] key, double min, double max) {
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
	public Set<Tuple> zRevRangeWithScore(byte[] key, int start, int end) {
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
	public Set<byte[]> zRangeByScore(byte[] key, double min, double max, int offset, int count) {
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
	public Set<Tuple> zRangeByScoreWithScore(byte[] key, double min, double max, int offset, int count) {
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
	public Integer zRank(byte[] key, byte[] value) {
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
	public Boolean zRem(byte[] key, byte[] value) {
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
	public Integer zRemRange(byte[] key, int start, int end) {
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
	public Integer zRemRangeByScore(byte[] key, double min, double max) {
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
	public Set<byte[]> zRevRange(byte[] key, int start, int end) {
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
	public Integer zRevRank(byte[] key, byte[] value) {
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
	public Double zScore(byte[] key, byte[] value) {
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
	public Integer zUnionStore(byte[] destKey, Aggregate aggregate, int[] weights, byte[]... sets) {
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
	public Integer zUnionStore(byte[] destKey, byte[]... sets) {
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
	public Boolean hSet(byte[] key, byte[] field, byte[] value) {
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
	public Boolean hSetNX(byte[] key, byte[] field, byte[] value) {
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
	public Boolean hDel(byte[] key, byte[] field) {
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
	public Boolean hExists(byte[] key, byte[] field) {
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
	public byte[] hGet(byte[] key, byte[] field) {
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
	public Map<byte[], byte[]> hGetAll(byte[] key) {
		try {
			if (isQueueing()) {
				transaction.hgetAll(key);
				return null;
			}
			return jedis.hgetAll(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Integer hIncrBy(byte[] key, byte[] field, int delta) {
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
	public Set<byte[]> hKeys(byte[] key) {
		try {
			if (isQueueing()) {
				transaction.hkeys(key);
				return null;
			}
			return jedis.hkeys(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Integer hLen(byte[] key) {
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
	public List<byte[]> hMGet(byte[] key, byte[]... fields) {
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
	public void hMSet(byte[] key, Map<byte[], byte[]> tuple) {
		try {
			if (isQueueing()) {
				transaction.hmset(key, tuple);
			}
			jedis.hmset(key, tuple);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public List<byte[]> hVals(byte[] key) {
		try {
			if (isQueueing()) {
				transaction.hvals(key);
				return null;
			}
			return new ArrayList<byte[]>(jedis.hvals(key));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}
}