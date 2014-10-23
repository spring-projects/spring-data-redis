/*
 * Copyright 2011-2014 the original author or authors.
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

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.jredis.ClientRuntimeException;
import org.jredis.JRedis;
import org.jredis.Query.Support;
import org.jredis.RedisException;
import org.jredis.Sort;
import org.jredis.connector.ConnectionException;
import org.jredis.connector.NotConnectedException;
import org.jredis.protocol.Command;
import org.jredis.ri.alphazero.JRedisSupport;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.AbstractRedisConnection;
import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.Pool;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.connection.SortParameters;
import org.springframework.data.redis.connection.Subscription;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.types.RedisClientInfo;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;
import org.springframework.util.ReflectionUtils;

/**
 * {@code RedisConnection} implementation on top of <a href="http://github.com/alphazero/jredis">JRedis</a> library.
 * 
 * @author Costin Leau
 * @author Jennifer Hickey
 * @author Christoph Strobl
 * @author Thomas Darimont
 * @author David Liu
 */
public class JredisConnection extends AbstractRedisConnection {

	private static final Method SERVICE_REQUEST;

	private final JRedis jredis;
	private final Pool<JRedis> pool;
	private boolean isClosed = false;
	/** flag indicating whether the connection needs to be dropped or not */
	private boolean broken = false;

	static {
		SERVICE_REQUEST = ReflectionUtils.findMethod(JRedisSupport.class, "serviceRequest", Command.class, byte[][].class);
		ReflectionUtils.makeAccessible(SERVICE_REQUEST);
	}

	/**
	 * Constructs a new <code>JredisConnection</code> instance.
	 * 
	 * @param jredis JRedis connection
	 */
	public JredisConnection(JRedis jredis) {
		this(jredis, null);
	}

	public JredisConnection(JRedis jredis, Pool<JRedis> pool) {
		Assert.notNull(jredis, "a not-null instance required");
		this.jredis = jredis;
		this.pool = pool;
	}

	protected DataAccessException convertJredisAccessException(Exception ex) {
		if (ex instanceof RedisException) {
			return JredisUtils.convertJredisAccessException((RedisException) ex);
		}

		if (ex instanceof ClientRuntimeException) {
			if (ex instanceof NotConnectedException || ex instanceof ConnectionException) {
				broken = true;
			}
			return JredisUtils.convertJredisAccessException((ClientRuntimeException) ex);
		}

		return new RedisSystemException("Unknown JRedis exception", ex);
	}

	public Object execute(String command, byte[]... args) {
		Assert.hasText(command, "a valid command needs to be specified");
		try {
			List<byte[]> mArgs = new ArrayList<byte[]>();
			if (!ObjectUtils.isEmpty(args)) {
				Collections.addAll(mArgs, args);
			}

			return ReflectionUtils.invokeMethod(SERVICE_REQUEST, jredis, Command.valueOf(command.trim().toUpperCase()),
					mArgs.toArray(new byte[mArgs.size()][]));
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	public void close() throws RedisSystemException {
		super.close();

		if (isClosed()) {
			return;
		}
		isClosed = true;

		if (pool != null) {
			if (!broken) {
				pool.returnResource(jredis);
			} else {
				pool.returnBrokenResource(jredis);
			}
			return;
		}

		try {
			jredis.quit();
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
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
		Sort sort = jredis.sort(key);
		JredisUtils.applySortingParams(sort, params, null);
		try {
			return sort.exec();
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	public Long sort(byte[] key, SortParameters params, byte[] storeKey) {
		Sort sort = jredis.sort(key);
		JredisUtils.applySortingParams(sort, params, storeKey);
		try {
			return Support.unpackValue(sort.exec());
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	public Long dbSize() {
		try {
			return (Long) jredis.dbsize();
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

	public void bgReWriteAof() {
		try {
			jredis.bgrewriteaof();
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	/**
	 * @deprecated As of 1.3, use {@link #bgReWriteAof}.
	 */
	@Deprecated
	public void bgWriteAof() {
		bgReWriteAof();
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

	public Properties info(String section) {
		throw new UnsupportedOperationException();
	}

	public Long lastSave() {
		try {
			return (Long) jredis.lastsave();
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

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#shutdown(org.springframework.data.redis.connection.RedisServerCommands.ShutdownOption)
	 */
	@Override
	public void shutdown(ShutdownOption option) {
		throw new UnsupportedOperationException();
	}

	public Long del(byte[]... keys) {
		try {
			return jredis.del(keys);
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
			return jredis.exists(key);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	public Boolean expire(byte[] key, long seconds) {
		try {
			return jredis.expire(key, (int) seconds);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	public Boolean expireAt(byte[] key, long unixTime) {
		try {
			return jredis.expireat(key, unixTime);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	public Boolean pExpire(byte[] key, long millis) {
		throw new UnsupportedOperationException();
	}

	public Boolean pExpireAt(byte[] key, long unixTimeInMillis) {
		throw new UnsupportedOperationException();
	}

	public Long pTtl(byte[] key) {
		throw new UnsupportedOperationException();
	}

	public byte[] dump(byte[] key) {
		throw new UnsupportedOperationException();
	}

	public void restore(byte[] key, long ttlInMillis, byte[] serializedValue) {
		throw new UnsupportedOperationException();
	}

	public Set<byte[]> keys(byte[] pattern) {
		try {
			return new LinkedHashSet<byte[]>(jredis.keys(pattern));
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
			return jredis.move(key, dbIndex);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	public byte[] randomKey() {
		try {
			return jredis.randomkey();
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	public void rename(byte[] oldName, byte[] newName) {
		try {
			jredis.rename(oldName, newName);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	public Boolean renameNX(byte[] oldName, byte[] newName) {
		try {
			return jredis.renamenx(oldName, newName);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	public void select(int dbIndex) {
		throw new UnsupportedOperationException();
	}

	public Long ttl(byte[] key) {
		try {
			return jredis.ttl(key);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	public DataType type(byte[] key) {
		try {
			return JredisUtils.convertDataType(jredis.type(key));
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
			return jredis.get(key);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	public void set(byte[] key, byte[] value) {
		try {
			jredis.set(key, value);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	public byte[] getSet(byte[] key, byte[] value) {
		try {
			return jredis.getset(key, value);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	public Long append(byte[] key, byte[] value) {
		try {
			return jredis.append(key, value);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	public List<byte[]> mGet(byte[]... keys) {
		try {
			return jredis.mget(keys);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	public void mSet(Map<byte[], byte[]> tuple) {
		try {
			jredis.mset(tuple);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	public Boolean mSetNX(Map<byte[], byte[]> tuple) {
		try {
			return jredis.msetnx(tuple);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	public void setEx(byte[] key, long seconds, byte[] value) {
		throw new UnsupportedOperationException();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisStringCommands#pSetEx(byte[], long, byte[])
	 */
	@Override
	public void pSetEx(byte[] key, long milliseconds, byte[] value) {
		throw new UnsupportedOperationException();
	}

	public Boolean setNX(byte[] key, byte[] value) {
		try {
			return jredis.setnx(key, value);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	public byte[] getRange(byte[] key, long start, long end) {
		try {
			return jredis.substr(key, start, end);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	public Long decr(byte[] key) {
		try {
			return jredis.decr(key);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	public Long decrBy(byte[] key, long value) {
		try {
			return jredis.decrby(key, (int) value);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	public Long incr(byte[] key) {
		try {
			return jredis.incr(key);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	public Long incrBy(byte[] key, long value) {
		try {
			return jredis.incrby(key, (int) value);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	public Double incrBy(byte[] key, double value) {
		throw new UnsupportedOperationException();
	}

	public Boolean getBit(byte[] key, long offset) {
		try {
			return jredis.getbit(key, (int) offset);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	public Boolean setBit(byte[] key, long offset, boolean value) {
		try {
			return jredis.setbit(key, (int) offset, value);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	public void setRange(byte[] key, byte[] value, long start) {
		throw new UnsupportedOperationException();
	}

	public Long strLen(byte[] key) {
		throw new UnsupportedOperationException();
	}

	public Long bitCount(byte[] key) {
		throw new UnsupportedOperationException();
	}

	public Long bitCount(byte[] key, long begin, long end) {
		throw new UnsupportedOperationException();
	}

	public Long bitOp(BitOperation op, byte[] destination, byte[]... keys) {
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
			return jredis.lindex(key, index);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	public Long lLen(byte[] key) {
		try {
			return jredis.llen(key);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	public byte[] lPop(byte[] key) {
		try {
			return jredis.lpop(key);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	public Long lPush(byte[] key, byte[]... values) {
		if (values.length > 1) {
			throw new UnsupportedOperationException("lPush of multiple fields not supported");
		}
		try {
			jredis.lpush(key, values[0]);
			return null;
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	public List<byte[]> lRange(byte[] key, long start, long end) {
		try {
			List<byte[]> lrange = jredis.lrange(key, start, end);

			return lrange;
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	public Long lRem(byte[] key, long count, byte[] value) {
		try {
			return jredis.lrem(key, value, (int) count);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	public void lSet(byte[] key, long index, byte[] value) {
		try {
			jredis.lset(key, index, value);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	public void lTrim(byte[] key, long start, long end) {
		try {
			jredis.ltrim(key, start, end);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	public byte[] rPop(byte[] key) {
		try {
			return jredis.rpop(key);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	public byte[] rPopLPush(byte[] srcKey, byte[] dstKey) {
		try {
			return jredis.rpoplpush(srcKey, dstKey);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	public Long rPush(byte[] key, byte[]... values) {
		if (values.length > 1) {
			throw new UnsupportedOperationException("rPush of multiple fields not supported");
		}
		try {
			jredis.rpush(key, values[0]);
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

	public Long sAdd(byte[] key, byte[]... values) {
		if (values.length > 1) {
			throw new UnsupportedOperationException("sAdd of multiple fields not supported");
		}
		try {
			return JredisUtils.toLong(jredis.sadd(key, values[0]));
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	public Long sCard(byte[] key) {
		try {
			return jredis.scard(key);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	public Set<byte[]> sDiff(byte[]... keys) {
		byte[] destKey = keys[0];
		byte[][] sets = Arrays.copyOfRange(keys, 1, keys.length);

		try {
			List<byte[]> result = jredis.sdiff(destKey, sets);
			return new LinkedHashSet<byte[]>(result);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	public Long sDiffStore(byte[] destKey, byte[]... keys) {
		try {
			jredis.sdiffstore(destKey, keys);
			return Long.valueOf(-1);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	public Set<byte[]> sInter(byte[]... keys) {
		byte[] set1 = keys[0];
		byte[][] sets = Arrays.copyOfRange(keys, 1, keys.length);

		try {
			List<byte[]> result = jredis.sinter(set1, sets);
			return new LinkedHashSet<byte[]>(result);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	public Long sInterStore(byte[] destKey, byte[]... keys) {
		try {
			jredis.sinterstore(destKey, keys);
			return Long.valueOf(-1);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	public Boolean sIsMember(byte[] key, byte[] value) {
		try {
			return jredis.sismember(key, value);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	public Set<byte[]> sMembers(byte[] key) {
		try {
			return new LinkedHashSet<byte[]>(jredis.smembers(key));
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	public Boolean sMove(byte[] srcKey, byte[] destKey, byte[] value) {
		try {
			return jredis.smove(srcKey, destKey, value);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	public byte[] sPop(byte[] key) {
		try {
			return jredis.spop(key);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	public byte[] sRandMember(byte[] key) {
		try {
			return jredis.srandmember(key);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	public List<byte[]> sRandMember(byte[] key, long count) {
		throw new UnsupportedOperationException();
	}

	public Long sRem(byte[] key, byte[]... values) {
		if (values.length > 1) {
			throw new UnsupportedOperationException("sRem of multiple fields not supported");
		}
		try {
			return JredisUtils.toLong(jredis.srem(key, values[0]));
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	public Set<byte[]> sUnion(byte[]... keys) {
		byte[] set1 = keys[0];
		byte[][] sets = Arrays.copyOfRange(keys, 1, keys.length);

		try {
			return new LinkedHashSet<byte[]>(jredis.sunion(set1, sets));
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	public Long sUnionStore(byte[] destKey, byte[]... keys) {
		try {
			jredis.sunionstore(destKey, keys);
			return Long.valueOf(-1);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	//
	// ZSet commands
	//

	public Boolean zAdd(byte[] key, double score, byte[] value) {
		try {
			return jredis.zadd(key, score, value);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	public Long zAdd(byte[] key, Set<Tuple> tuples) {
		throw new UnsupportedOperationException();
	}

	public Long zCard(byte[] key) {
		try {
			return jredis.zcard(key);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	public Long zCount(byte[] key, double min, double max) {
		try {
			return jredis.zcount(key, min, max);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	public Double zIncrBy(byte[] key, double increment, byte[] value) {
		try {
			return jredis.zincrby(key, increment, value);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	public Long zInterStore(byte[] destKey, Aggregate aggregate, double[] weights, byte[]... sets) {
		throw new UnsupportedOperationException();
	}

	public Long zInterStore(byte[] destKey, byte[]... sets) {
		throw new UnsupportedOperationException();
	}

	public Set<byte[]> zRange(byte[] key, long start, long end) {
		try {
			return new LinkedHashSet<byte[]>(jredis.zrange(key, start, end));
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	public Set<Tuple> zRangeWithScores(byte[] key, long start, long end) {
		throw new UnsupportedOperationException();
	}

	public Set<byte[]> zRangeByScore(byte[] key, double min, double max) {
		try {
			return new LinkedHashSet<byte[]>(jredis.zrangebyscore(key, min, max));
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
			return jredis.zrank(key, value);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	public Long zRem(byte[] key, byte[]... values) {
		if (values.length > 1) {
			throw new UnsupportedOperationException("zRem of multiple fields not supported");
		}
		try {
			return JredisUtils.toLong(jredis.zrem(key, values[0]));
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	public Long zRemRange(byte[] key, long start, long end) {
		try {
			return jredis.zremrangebyrank(key, start, end);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	public Long zRemRangeByScore(byte[] key, double min, double max) {
		try {
			return jredis.zremrangebyscore(key, min, max);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	public Set<byte[]> zRevRange(byte[] key, long start, long end) {
		try {
			return new LinkedHashSet<byte[]>(jredis.zrevrange(key, start, end));
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	public Set<Tuple> zRevRangeWithScores(byte[] key, long start, long end) {
		throw new UnsupportedOperationException();
	}

	public Long zRevRank(byte[] key, byte[] value) {
		try {
			return jredis.zrevrank(key, value);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	public Double zScore(byte[] key, byte[] value) {
		try {
			return jredis.zscore(key, value);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	//
	// Hash commands
	//

	public Long zUnionStore(byte[] destKey, Aggregate aggregate, double[] weights, byte[]... sets) {
		throw new UnsupportedOperationException();
	}

	public Long zUnionStore(byte[] destKey, byte[]... sets) {
		throw new UnsupportedOperationException();
	}

	public Long hDel(byte[] key, byte[]... fields) {
		if (fields.length > 1) {
			throw new UnsupportedOperationException("hDel of multiple fields not supported");
		}
		try {
			return JredisUtils.toLong(jredis.hdel(key, fields[0]));
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	public Boolean hExists(byte[] key, byte[] field) {
		try {
			return jredis.hexists(key, field);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	public byte[] hGet(byte[] key, byte[] field) {
		try {
			return jredis.hget(key, field);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	public Map<byte[], byte[]> hGetAll(byte[] key) {
		try {
			return jredis.hgetall(key);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	public Long hIncrBy(byte[] key, byte[] field, long delta) {
		throw new UnsupportedOperationException();
	}

	public Double hIncrBy(byte[] key, byte[] field, double delta) {
		throw new UnsupportedOperationException();
	}

	public Set<byte[]> hKeys(byte[] key) {
		try {
			return new LinkedHashSet<byte[]>(jredis.hkeys(key));
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	public Long hLen(byte[] key) {
		try {
			return jredis.hlen(key);
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
			return jredis.hset(key, field, value);
		} catch (Exception ex) {
			throw convertJredisAccessException(ex);
		}
	}

	public Boolean hSetNX(byte[] key, byte[] field, byte[] value) {
		throw new UnsupportedOperationException();
	}

	public List<byte[]> hVals(byte[] key) {
		try {
			return jredis.hvals(key);
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

	//
	// Scripting commands
	//

	public void subscribe(MessageListener listener, byte[]... channels) {
		throw new UnsupportedOperationException();
	}

	public void scriptFlush() {
		throw new UnsupportedOperationException();
	}

	public void scriptKill() {
		throw new UnsupportedOperationException();
	}

	public String scriptLoad(byte[] script) {
		throw new UnsupportedOperationException();
	}

	public List<Boolean> scriptExists(String... scriptSha1) {
		throw new UnsupportedOperationException();
	}

	public <T> T eval(byte[] script, ReturnType returnType, int numKeys, byte[]... keysAndArgs) {
		throw new UnsupportedOperationException();
	}

	public <T> T evalSha(String scriptSha1, ReturnType returnType, int numKeys, byte[]... keysAndArgs) {
		throw new UnsupportedOperationException();
	}

	public <T> T evalSha(byte[] scriptSha1, ReturnType returnType, int numKeys, byte[]... keysAndArgs) {
		throw new UnsupportedOperationException();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#time()
	 */
	@Override
	public Long time() {
		throw new UnsupportedOperationException("The 'TIME' command is not supported by the JRedis driver.");
	}

	@Override
	public void killClient(String host, int port) {
		throw new UnsupportedOperationException("The 'CLIENT KILL' command is not supported by the JRedis driver.");
	}

	@Override
	public void setClientName(byte[] name) {
		throw new UnsupportedOperationException("'CLIENT SETNAME' is not supported by the JRedis driver.");
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#slaveOf(java.lang.String, int)
	 */
	@Override
	public void slaveOf(String host, int port) {

		try {
			this.jredis.slaveof(host, port);
		} catch (Exception e) {
			throw convertJredisAccessException(e);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#getClientName()
	 */
	@Override
	public String getClientName() {
		throw new UnsupportedOperationException("The 'CLIENT GETNAME' command is not supported by the JRedis driver.");
	}

	public List<RedisClientInfo> getClientList() {
		throw new UnsupportedOperationException();
	}

	/*
	 * @see org.springframework.data.redis.connection.RedisServerCommands#slaveOfNoOne()
	 */
	@Override
	public void slaveOfNoOne() {

		try {
			this.jredis.slaveofnone();
		} catch (Exception e) {
			throw convertJredisAccessException(e);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisKeyCommands#scan(org.springframework.data.redis.core.ScanOptions)
	 */
	@Override
	public Cursor<byte[]> scan(ScanOptions options) {
		throw new UnsupportedOperationException("'SCAN' command is not supported for jredis.");
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisZSetCommands#zScan(byte[], org.springframework.data.redis.core.ScanOptions)
	 */
	@Override
	public Cursor<Tuple> zScan(byte[] key, ScanOptions options) {
		throw new UnsupportedOperationException("'ZSCAN' command is not supported for jredis.");
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisSetCommands#sScan(byte[], org.springframework.data.redis.core.ScanOptions)
	 */
	@Override
	public Cursor<byte[]> sScan(byte[] key, ScanOptions options) {
		throw new UnsupportedOperationException("'SSCAN' command is not uspported for jredis");
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisHashCommands#hscan(byte[], org.springframework.data.redis.core.ScanOptions)
	 */
	@Override
	public Cursor<Entry<byte[], byte[]>> hScan(byte[] key, ScanOptions options) {
		throw new UnsupportedOperationException("'HSCAN' command is not uspported for jredis");
	}

}
