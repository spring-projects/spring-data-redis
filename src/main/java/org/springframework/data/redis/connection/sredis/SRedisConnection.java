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
package org.springframework.data.redis.connection.sredis;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisSubscribedConnectionException;
import org.springframework.data.redis.connection.SortParameters;
import org.springframework.data.redis.connection.Subscription;

import redis.client.RedisClient;
import redis.client.RedisClient.Pipeline;
import redis.client.RedisException;
import redis.client.SocketPool;
import redis.reply.MultiBulkReply;

import com.google.common.util.concurrent.ListenableFuture;

/**
 * {@code RedisConnection} implementation on top of <a href="https://github.com/spullara/redis-protocol">spullara Redis Protocol</a> library.
 * 
 * @author Costin Leau
 */
public class SRedisConnection implements RedisConnection {

	private boolean isClosed = false;
	private boolean isMulti = false;
	private final RedisClient client;
	private Pipeline pipeline;
	private volatile SRedisSubscription subscription;

	public SRedisConnection(SocketPool pool) {
		try {
			this.client = new RedisClient(pool);
		} catch (IOException e) {
			throw new RedisConnectionFailureException("Could not connect", e);
		}
	}

	protected DataAccessException convertSRAccessException(Exception ex) {
		if (ex instanceof RedisException) {
			return SRedisUtils.convertSRedisAccessException((RedisException) ex);
		}
		if (ex instanceof IOException) {
			return new RedisConnectionFailureException("Redis connection failed", (IOException) ex);
		}

		return new RedisSystemException("Unknown SRedis exception", ex);
	}


	public void close() throws DataAccessException {
		isClosed = true;

		try {
			client.close();
		} catch (IOException ex) {
			throw convertSRAccessException(ex);
		}
	}

	public boolean isClosed() {
		return isClosed;
	}

	public RedisClient getNativeConnection() {
		return client;
	}


	public boolean isQueueing() {
		return isMulti;
	}

	public boolean isPipelined() {
		return (pipeline != null);
	}


	public void openPipeline() {
		if (pipeline == null) {
			pipeline = client.pipeline();
		}
	}

	@SuppressWarnings("unchecked")
	public List<Object> closePipeline() {
		if (pipeline != null) {
			ListenableFuture<MultiBulkReply> reply = pipeline.exec();
			pipeline = null;
			if (reply != null) {
				try {
					return SRedisUtils.toList(reply.get().byteArrays);
				} catch (Exception ex) {
					throw convertSRAccessException(ex);
				}
			}
		}
		return Collections.emptyList();
	}


	public List<byte[]> sort(byte[] key, SortParameters params) {

		byte[] sort = SRedisUtils.sort(params);

		try {
			if (isPipelined()) {
				pipeline.sort(key, sort, null, null);
				return null;
			}
			return SRedisUtils.toBytesList(client.sort(key, sort, null, null).byteArrays);
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}

	public Long sort(byte[] key, SortParameters params, byte[] sortKey) {

		byte[] sort = SRedisUtils.sort(params, sortKey);

		try {
			if (isPipelined()) {
				pipeline.sort(key, sort, null, null);
				return null;
			}
			return SRedisUtils.toLong(client.sort(key, sort, null, null).byteArrays);
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}

	public Long dbSize() {
		try {
			if (isPipelined()) {
				pipeline.dbsize();
				return null;
			}
			return client.dbsize().integer;
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}



	public void flushDb() {
		try {
			if (isPipelined()) {
				pipeline.flushdb();
				return;
			}
			client.flushdb();
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public void flushAll() {
		try {
			if (isPipelined()) {
				pipeline.flushall();
				return;
			}
			client.flushall();
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public void bgSave() {
		try {
			if (isPipelined()) {
				pipeline.bgsave();
				return;
			}
			client.bgsave();
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public void bgWriteAof() {
		try {
			if (isPipelined()) {
				pipeline.bgrewriteaof();
				return;
			}
			client.bgrewriteaof();
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public void save() {
		try {
			if (isPipelined()) {
				pipeline.save();
				return;
			}
			client.save();
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public List<String> getConfig(String param) {
		try {
			if (isPipelined()) {
				pipeline.config_get(param);
				return null;
			}
			return Collections.singletonList(client.config_get(param).toString());
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public Properties info() {
		try {
			if (isPipelined()) {
				pipeline.info();
				return null;
			}
			return SRedisUtils.info(client.info());
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public Long lastSave() {
		try {
			if (isPipelined()) {
				pipeline.lastsave();
				return null;
			}
			return client.lastsave().integer;
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public void setConfig(String param, String value) {
		try {
			if (isPipelined()) {
				pipeline.config_set(param, value);
				return;
			}
			client.config_set(param, value);
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}



	public void resetConfigStats() {
		try {
			if (isPipelined()) {
				pipeline.config_resetstat();
				return;
			}
			client.config_resetstat();
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public void shutdown() {
		try {
			if (isPipelined()) {
				pipeline.shutdown();
				return;
			}
			client.shutdown();
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public byte[] echo(byte[] message) {
		try {
			if (isPipelined()) {
				pipeline.echo(message);
				return null;
			}
			return client.echo(message).bytes;
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public String ping() {
		try {
			if (isPipelined()) {
				pipeline.ping();
			}
			return client.ping().status;
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public Long del(byte[]... keys) {
		try {
			if (isPipelined()) {
				pipeline.del(keys);
				return null;
			}
			return client.del(keys).integer;
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public void discard() {
		isMulti = false;
		try {
			if (isPipelined()) {
				pipeline.discard();
				return;
			}

			client.discard();
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public List<Object> exec() {
		isMulti = false;
		try {
			if (isPipelined()) {
				pipeline.exec();
				return null;
			}
			return SRedisUtils.toList(client.exec().byteArrays);
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public Boolean exists(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline.exists(key);
				return null;
			}
			return client.exists(key).integer == 1;
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public Boolean expire(byte[] key, long seconds) {
		try {
			if (isPipelined()) {
				pipeline.expire(key, seconds);
				return null;
			}
			return client.expire(key, seconds).integer == 1;
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public Boolean expireAt(byte[] key, long unixTime) {
		try {
			if (isPipelined()) {
				pipeline.expireat(key, unixTime);
				return null;
			}
			return client.expireat(key, unixTime).integer == 1;
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public Set<byte[]> keys(byte[] pattern) {
		try {
			if (isPipelined()) {
				pipeline.keys(pattern);
				return null;
			}
			return SRedisUtils.toSet(client.keys(pattern).byteArrays);
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public void multi() {
		if (isQueueing()) {
			return;
		}
		isMulti = true;
		try {
			if (isPipelined()) {
				pipeline.multi();
				return;
			}
			client.multi();
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public Boolean persist(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline.persist(key);
				return null;
			}
			return client.persist(key).integer == 1;
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public Boolean move(byte[] key, int dbIndex) {
		try {
			if (isPipelined()) {
				pipeline.move(key, dbIndex);
				return null;
			}
			return client.move(key, dbIndex).integer == 1;
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public byte[] randomKey() {
		try {
			if (isPipelined()) {
				pipeline.randomkey();
				return null;
			}
			return client.randomkey().bytes;
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public void rename(byte[] oldName, byte[] newName) {
		try {
			if (isPipelined()) {
				pipeline.rename(oldName, newName);
				return;
			}
			client.rename(oldName, newName);
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public Boolean renameNX(byte[] oldName, byte[] newName) {
		try {
			if (isPipelined()) {
				pipeline.renamenx(oldName, newName);
				return null;
			}
			return (client.renamenx(oldName, newName).integer == 1);
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public void select(int dbIndex) {
		try {
			if (isPipelined()) {
				throw new UnsupportedOperationException();
			}
			client.select(dbIndex);
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public Long ttl(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline.ttl(key);
				return null;
			}
			return client.ttl(key).integer;
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public DataType type(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline.type(key);
				return null;
			}
			return DataType.fromCode(client.type(key).status);
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public void unwatch() {
		try {
			client.unwatch();
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public void watch(byte[]... keys) {
		if (isQueueing()) {
			throw new UnsupportedOperationException();
		}
		try {
			for (byte[] key : keys) {
				if (isPipelined()) {
					pipeline.watch(key);
				}
				else {
					client.watch(key);
				}
			}
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}

	//
	// String commands
	//


	public byte[] get(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline.get(key);
				return null;
			}

			return client.get(key).bytes;
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public void set(byte[] key, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline.set(key, value);
				return;
			}
			client.set(key, value);
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}



	public byte[] getSet(byte[] key, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline.getset(key, value);
				return null;
			}
			return client.getset(key, value).bytes;
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public Long append(byte[] key, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline.append(key, value);
				return null;
			}
			return client.append(key, value).integer;
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public List<byte[]> mGet(byte[]... keys) {
		try {
			if (isPipelined()) {
				pipeline.mget(keys);
				return null;
			}
			return SRedisUtils.toBytesList(client.mget(keys).byteArrays);
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public void mSet(Map<byte[], byte[]> tuples) {
		try {
			if (isPipelined()) {
				pipeline.mset(SRedisUtils.convert(tuples));
				return;
			}
			client.mset(SRedisUtils.convert(tuples));
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public void mSetNX(Map<byte[], byte[]> tuples) {
		try {
			if (isPipelined()) {
				pipeline.msetnx(SRedisUtils.convert(tuples));
				return;
			}
			client.msetnx(SRedisUtils.convert(tuples));
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public void setEx(byte[] key, long time, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline.setex(key, time, value);
				return;
			}
			client.setex(key, time, value);
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public Boolean setNX(byte[] key, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline.setnx(key, value);
				return null;
			}
			return client.setnx(key, value).integer == 1;
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public byte[] getRange(byte[] key, long start, long end) {
		try {
			if (isPipelined()) {
				pipeline.getrange(key, start, end);
				return null;
			}
			return client.getrange(key, start, end).bytes;
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public Long decr(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline.decr(key);
				return null;
			}
			return client.decr(key).integer;
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public Long decrBy(byte[] key, long value) {
		try {
			if (isPipelined()) {
				pipeline.decrby(key, value);
				return null;
			}
			return client.decrby(key, value).integer;
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public Long incr(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline.incr(key);
				return null;
			}
			return client.incr(key).integer;
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public Long incrBy(byte[] key, long value) {
		try {
			if (isPipelined()) {
				pipeline.incrby(key, value);
				return null;
			}
			return client.incrby(key, value).integer;
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public Boolean getBit(byte[] key, long offset) {
		try {
			if (isQueueing()) {
				throw new UnsupportedOperationException();
			}
			if (isPipelined()) {
				throw new UnsupportedOperationException();
			}
			return (client.getbit(key, offset).integer == 1);
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public void setBit(byte[] key, long offset, boolean value) {
		try {
			if (isQueueing()) {
				throw new UnsupportedOperationException();
			}
			if (isPipelined()) {
				throw new UnsupportedOperationException();
			}
			client.setbit(key, offset, SRedisUtils.asBit(value));
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public void setRange(byte[] key, byte[] value, long start) {
		try {
			if (isQueueing()) {
				throw new UnsupportedOperationException();
			}
			if (isPipelined()) {
				throw new UnsupportedOperationException();
			}
			client.setrange(key, start, value);
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public Long strLen(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline.strlen(key);
				return null;
			}
			return client.strlen(key).integer;
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}

	//
	// List commands
	//


	public Long lPush(byte[] key, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline.lpush(key, value);
				return null;
			}
			return client.lpush(key, value).integer;
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public Long rPush(byte[] key, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline.rpush(key, value);
				return null;
			}
			return client.rpush(key, value).integer;
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public List<byte[]> bLPop(int timeout, byte[]... keys) {
		try {
			if (isPipelined()) {
				pipeline.blpop(timeout, keys);
				return null;
			}
			return SRedisUtils.toBytesList(client.blpop(timeout, keys).byteArrays);
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public List<byte[]> bRPop(int timeout, byte[]... keys) {
		try {
			if (isPipelined()) {
				pipeline.brpop(timeout, keys);
				return null;
			}
			return SRedisUtils.toBytesList(client.brpop(timeout, keys).byteArrays);
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public byte[] lIndex(byte[] key, long index) {
		try {
			if (isPipelined()) {
				pipeline.lindex(key, index);
				return null;
			}
			return client.lindex(key, index).bytes;
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public Long lInsert(byte[] key, Position where, byte[] pivot, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline.linsert(key, SRedisUtils.convertPosition(where), pivot, value);
				return null;
			}
			return client.linsert(key, SRedisUtils.convertPosition(where), pivot, value).integer;
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public Long lLen(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline.llen(key);
				return null;
			}
			return client.llen(key).integer;
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public byte[] lPop(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline.lpop(key);
				return null;
			}
			return client.lpop(key).bytes;
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public List<byte[]> lRange(byte[] key, long start, long end) {
		try {
			if (isPipelined()) {
				pipeline.lrange(key, start, end);
				return null;
			}
			return SRedisUtils.toBytesList(client.lrange(key, start, end).byteArrays);
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public Long lRem(byte[] key, long count, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline.lrem(key, count, value);
				return null;
			}
			return client.lrem(key, count, value).integer;
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public void lSet(byte[] key, long index, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline.lset(key, index, value);
				return;
			}
			client.lset(key, index, value);
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public void lTrim(byte[] key, long start, long end) {
		try {
			if (isPipelined()) {
				pipeline.ltrim(key, start, end);
				return;
			}
			client.ltrim(key, start, end);
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public byte[] rPop(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline.rpop(key);
				return null;
			}
			return client.rpop(key).bytes;
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public byte[] rPopLPush(byte[] srcKey, byte[] dstKey) {
		try {
			if (isPipelined()) {
				pipeline.rpoplpush(srcKey, dstKey);
				return null;
			}
			return client.rpoplpush(srcKey, dstKey).bytes;
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public byte[] bRPopLPush(int timeout, byte[] srcKey, byte[] dstKey) {
		try {
			if (isPipelined()) {
				pipeline.brpoplpush(srcKey, dstKey, timeout);
				return null;
			}
			return client.brpoplpush(srcKey, dstKey, timeout).bytes;
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public Long lPushX(byte[] key, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline.lpushx(key, value);
				return null;
			}
			return client.lpushx(key, value).integer;
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public Long rPushX(byte[] key, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline.rpushx(key, value);
				return null;
			}
			return client.rpushx(key, value).integer;
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	//
	// Set commands
	//


	public Boolean sAdd(byte[] key, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline.sadd(key, value);
				return null;
			}
			return (client.sadd(key, value).integer == 1);
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public Long sCard(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline.scard(key);
				return null;
			}
			return client.scard(key).integer;
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public Set<byte[]> sDiff(byte[]... keys) {
		try {
			if (isPipelined()) {
				pipeline.sdiff(keys);
				return null;
			}
			return SRedisUtils.toSet(client.sdiff(keys).byteArrays);
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public void sDiffStore(byte[] destKey, byte[]... keys) {
		try {
			if (isPipelined()) {
				pipeline.sdiffstore(destKey, keys);
				return;
			}
			client.sdiffstore(destKey, keys);
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public Set<byte[]> sInter(byte[]... keys) {
		try {
			if (isPipelined()) {
				pipeline.sinter(keys);
				return null;
			}
			return SRedisUtils.toSet(client.sinter(keys).byteArrays);
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public void sInterStore(byte[] destKey, byte[]... keys) {
		try {
			if (isPipelined()) {
				pipeline.sinterstore(destKey, keys);
				return;
			}
			client.sinterstore(destKey, keys);
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public Boolean sIsMember(byte[] key, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline.sismember(key, value);
				return null;
			}
			return client.sismember(key, value).integer == 1;
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public Set<byte[]> sMembers(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline.smembers(key);
				return null;
			}
			return SRedisUtils.toSet(client.smembers(key).byteArrays);
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public Boolean sMove(byte[] srcKey, byte[] destKey, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline.smove(srcKey, destKey, value);
				return null;
			}
			return client.smove(srcKey, destKey, value).integer == 1;
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public byte[] sPop(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline.spop(key);
				return null;
			}
			return client.spop(key).bytes;
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public byte[] sRandMember(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline.srandmember(key);
				return null;
			}
			return client.srandmember(key).bytes;
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public Boolean sRem(byte[] key, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline.srem(key, value);
				return null;
			}
			return client.srem(key, value).integer == 1;
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public Set<byte[]> sUnion(byte[]... keys) {
		try {
			if (isPipelined()) {
				pipeline.sunion(keys);
				return null;
			}
			return SRedisUtils.toSet(client.sunion(keys).byteArrays);
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public void sUnionStore(byte[] destKey, byte[]... keys) {
		try {
			if (isPipelined()) {
				pipeline.sunionstore(destKey, keys);
				return;
			}
			client.sunionstore(destKey, keys);
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}

	//
	// ZSet commands
	//


	public Boolean zAdd(byte[] key, double score, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline.zadd(key, score, value, null, null);
				return null;
			}
			return client.zadd(key, score, value, null, null).integer == 1;
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public Long zCard(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline.zcard(key);
				return null;
			}
			return client.zcard(key).integer;
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public Long zCount(byte[] key, double min, double max) {
		try {
			if (isQueueing()) {
				pipeline.zcount(key, min, max);
				return null;
			}
			return client.zcount(key, min, max).integer;
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public Double zIncrBy(byte[] key, double increment, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline.zincrby(key, increment, value);
				return null;
			}
			return SRedisUtils.toDouble(client.zincrby(key, increment, value).bytes);
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public Long zInterStore(byte[] destKey, Aggregate aggregate, int[] weights, byte[]... sets) {
		throw new UnsupportedOperationException();
	}


	public Long zInterStore(byte[] destKey, byte[]... sets) {
		try {
			if (isQueueing()) {
				pipeline.zinterstore(destKey, sets);
				return null;
			}
			return client.zinterstore(destKey, sets).integer;
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public Set<byte[]> zRange(byte[] key, long start, long end) {
		try {
			if (isPipelined()) {
				pipeline.zrange(key, start, end, null);
				return null;
			}
			return SRedisUtils.toSet(client.zrange(key, start, end, null).byteArrays);
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public Set<Tuple> zRangeWithScores(byte[] key, long start, long end) {
		try {
			if (isPipelined()) {
				pipeline.zrange(key, start, end, SRedisUtils.WITHSCORES);
				return null;
			}
			return SRedisUtils.convertTuple(client.zrange(key, start, end, SRedisUtils.WITHSCORES));
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public Set<byte[]> zRangeByScore(byte[] key, double min, double max) {
		try {
			if (isPipelined()) {
				pipeline.zrangebyscore(key, min, max, null, null);
				return null;
			}
			return SRedisUtils.toSet(client.zrangebyscore(key, min, max, null, null).byteArrays);
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public Set<Tuple> zRangeByScoreWithScores(byte[] key, double min, double max) {
		try {
			if (isPipelined()) {
				pipeline.zrangebyscore(key, min, max, SRedisUtils.WITHSCORES, null);
				return null;
			}
			return SRedisUtils.convertTuple(client.zrangebyscore(key, min, max, SRedisUtils.WITHSCORES, null));
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public Set<Tuple> zRevRangeWithScores(byte[] key, long start, long end) {
		try {
			if (isPipelined()) {
				pipeline.zrevrange(key, start, end, SRedisUtils.WITHSCORES);
				return null;
			}
			return SRedisUtils.convertTuple(client.zrevrange(key, start, end, SRedisUtils.WITHSCORES));
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public Set<byte[]> zRangeByScore(byte[] key, double min, double max, long offset, long count) {
		try {
			byte[] limit = SRedisUtils.limit(offset, count);
			if (isPipelined()) {
				pipeline.zrangebyscore(key, min, max, null, limit);
				return null;
			}
			return SRedisUtils.toSet(client.zrangebyscore(key, min, max, null, limit).byteArrays);
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public Set<Tuple> zRangeByScoreWithScores(byte[] key, double min, double max, long offset, long count) {
		try {
			byte[] limit = SRedisUtils.limit(offset, count);
			if (isPipelined()) {
				pipeline.zrangebyscore(key, min, max, SRedisUtils.WITHSCORES, limit);
				return null;
			}
			return SRedisUtils.convertTuple(client.zrangebyscore(key, min, max, SRedisUtils.WITHSCORES, limit));
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public Set<byte[]> zRevRangeByScore(byte[] key, double min, double max, long offset, long count) {
		try {
			byte[] limit = SRedisUtils.limit(offset, count);
			if (isPipelined()) {
				client.zrevrangebyscore(key, min, max, null, limit);
			}
			return SRedisUtils.toSet(client.zrevrangebyscore(key, min, max, null, limit).byteArrays);
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public Set<byte[]> zRevRangeByScore(byte[] key, double min, double max) {
		try {
			if (isPipelined()) {
				client.zrevrangebyscore(key, min, max, null, null);
			}
			return SRedisUtils.toSet(client.zrevrangebyscore(key, min, max, null, null).byteArrays);
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public Set<Tuple> zRevRangeByScoreWithScores(byte[] key, double min, double max, long offset, long count) {
		try {
			byte[] limit = SRedisUtils.limit(offset, count);
			if (isPipelined()) {
				client.zrevrangebyscore(key, min, max, SRedisUtils.WITHSCORES, limit);
			}
			return SRedisUtils.convertTuple(client.zrevrangebyscore(key, min, max, SRedisUtils.WITHSCORES, limit));
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public Set<Tuple> zRevRangeByScoreWithScores(byte[] key, double min, double max) {
		try {
			if (isPipelined()) {
				client.zrevrangebyscore(key, min, max, SRedisUtils.WITHSCORES, null);
			}
			return SRedisUtils.convertTuple(client.zrevrangebyscore(key, min, max, SRedisUtils.WITHSCORES, null));
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public Long zRank(byte[] key, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline.zrank(key, value);
				return null;
			}
			return client.zrank(key, value).integer;
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public Boolean zRem(byte[] key, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline.zrem(key, value);
				return null;
			}
			return client.zrem(key, value).integer == 1;
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public Long zRemRange(byte[] key, long start, long end) {
		try {
			if (isPipelined()) {
				pipeline.zremrangebyrank(key, start, end);
				return null;
			}
			return client.zremrangebyrank(key, start, end).integer;
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public Long zRemRangeByScore(byte[] key, double min, double max) {
		try {
			if (isPipelined()) {
				pipeline.zremrangebyscore(key, min, max);
				return null;
			}
			return client.zremrangebyscore(key, min, max).integer;
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public Set<byte[]> zRevRange(byte[] key, long start, long end) {
		try {
			if (isPipelined()) {
				pipeline.zrevrange(key, start, end, null);
				return null;
			}
			return SRedisUtils.toSet(client.zrevrange(key, start, end, null).byteArrays);
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public Long zRevRank(byte[] key, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline.zrevrank(key, value);
				return null;
			}
			return client.zrevrank(key, value).integer;
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public Double zScore(byte[] key, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline.zscore(key, value);
				return null;
			}
			return SRedisUtils.toDouble(client.zscore(key, value).bytes);
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public Long zUnionStore(byte[] destKey, Aggregate aggregate, int[] weights, byte[]... sets) {
		throw new UnsupportedOperationException();
	}


	public Long zUnionStore(byte[] destKey, byte[]... sets) {
		try {
			if (isPipelined()) {
				pipeline.zunionstore(destKey, sets);
				return null;
			}
			return client.zunionstore(destKey, sets).integer;
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}

	//
	// Hash commands
	//


	public Boolean hSet(byte[] key, byte[] field, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline.hset(key, field, value);
				return null;
			}
			return client.hset(key, field, value).integer == 1;
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public Boolean hSetNX(byte[] key, byte[] field, byte[] value) {
		try {
			if (isPipelined()) {
				pipeline.hsetnx(key, field, value);
				return null;
			}
			return client.hsetnx(key, field, value).integer == 1;
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public Boolean hDel(byte[] key, byte[] field) {
		try {
			if (isPipelined()) {
				pipeline.hdel(key, field);
				return null;
			}
			return client.hdel(key, field).integer == 1;
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public Boolean hExists(byte[] key, byte[] field) {
		try {
			if (isPipelined()) {
				pipeline.hexists(key, field);
				return null;
			}
			return client.hexists(key, field).integer == 1;
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public byte[] hGet(byte[] key, byte[] field) {
		try {
			if (isPipelined()) {
				pipeline.hget(key, field);
				return null;
			}
			return client.hget(key, field).bytes;
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public Map<byte[], byte[]> hGetAll(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline.hgetall(key);
				return null;
			}
			return SRedisUtils.toMap(client.hgetall(key).byteArrays);
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public Long hIncrBy(byte[] key, byte[] field, long delta) {
		try {
			if (isPipelined()) {
				pipeline.hincrby(key, field, delta);
				return null;
			}
			return client.hincrby(key, field, delta).integer;
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public Set<byte[]> hKeys(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline.hkeys(key);
				return null;
			}
			return SRedisUtils.toSet(client.hkeys(key).byteArrays);
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public Long hLen(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline.hlen(key);
				return null;
			}
			return client.hlen(key).integer;
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public List<byte[]> hMGet(byte[] key, byte[]... fields) {
		try {
			if (isPipelined()) {
				pipeline.hmget(key, fields);
				return null;
			}
			return SRedisUtils.toBytesList(client.hmget(key, fields).byteArrays);
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public void hMSet(byte[] key, Map<byte[], byte[]> tuple) {
		try {
			if (isPipelined()) {
				pipeline.hmset(key, tuple);
				return;
			}
			client.hmset(key, tuple);
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public List<byte[]> hVals(byte[] key) {
		try {
			if (isPipelined()) {
				pipeline.hvals(key);
				return null;
			}
			return SRedisUtils.toBytesList(client.hvals(key).byteArrays);
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	//
	// Pub/Sub functionality
	//

	public Long publish(byte[] channel, byte[] message) {
		try {
			if (isQueueing()) {
				throw new UnsupportedOperationException();
			}
			if (isPipelined()) {
				pipeline.publish(channel, message);
				return null;
			}
			return client.publish(channel, message).integer;
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public Subscription getSubscription() {
		return subscription;
	}


	public boolean isSubscribed() {
		return (subscription != null && subscription.isAlive());
	}


	public void pSubscribe(MessageListener listener, byte[]... patterns) {
		checkSubscription();

		try {
			if (isQueueing()) {
				throw new UnsupportedOperationException();
			}
			if (isPipelined()) {
				throw new UnsupportedOperationException();
			}

			subscription = new SRedisSubscription(listener, client);
			subscription.pSubscribe(patterns);
		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}


	public void subscribe(MessageListener listener, byte[]... channels) {
		checkSubscription();

		try {
			if (isPipelined()) {
				throw new UnsupportedOperationException();
			}

			subscription = new SRedisSubscription(listener, client);
			subscription.subscribe(channels);

		} catch (Exception ex) {
			throw convertSRAccessException(ex);
		}
	}

	private void checkSubscription() {
		if (isSubscribed()) {
			throw new RedisSubscribedConnectionException(
					"Connection already subscribed; use the connection Subscription to cancel or add new channels");
		}
	}
}