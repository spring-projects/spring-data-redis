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
package org.springframework.data.keyvalue.redis.connection.jedis;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.springframework.dao.DataAccessException;
import org.springframework.data.keyvalue.UncategorizedKeyvalueStoreException;
import org.springframework.data.keyvalue.redis.SubscribedRedisConnectionException;
import org.springframework.data.keyvalue.redis.connection.DataType;
import org.springframework.data.keyvalue.redis.connection.MessageListener;
import org.springframework.data.keyvalue.redis.connection.RedisConnection;
import org.springframework.data.keyvalue.redis.connection.SortParameters;
import org.springframework.data.keyvalue.redis.connection.Subscription;
import org.springframework.util.ReflectionUtils;

import redis.clients.jedis.BinaryJedis;
import redis.clients.jedis.BinaryJedisPubSub;
import redis.clients.jedis.BinaryTransaction;
import redis.clients.jedis.Client;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.SortingParams;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.ZParams;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.util.Pool;

/**
 * {@code RedisConnection} implementation on top of <a href="http://github.com/xetorthio/jedis">Jedis</a> library.
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
	private final Pool<Jedis> pool;
	/** flag indicating whether the connection needs to be dropped or not */
	private boolean broken = false;

	private volatile JedisSubscription subscription;
	private volatile Pipeline pipeline;

	/**
	 * Constructs a new <code>JedisConnection</code> instance.
	 *
	 * @param jedis Jedis entity
	 */
	public JedisConnection(Jedis jedis) {
		this(jedis, null);
	}

	/**
	 * 
	 * Constructs a new <code>JedisConnection</code> instance backed by a jedis pool.
	 *
	 * @param jedis
	 * @param pool can be null, if no pool is used
	 */
	public JedisConnection(Jedis jedis, Pool<Jedis> pool) {
		this.jedis = jedis;
		// extract underlying connection for batch operations
		client = (Client) ReflectionUtils.getField(CLIENT_FIELD, jedis);
		transaction = new Transaction(client);

		this.pool = pool;
	}

	protected DataAccessException convertJedisAccessException(Exception ex) {
		if (ex instanceof JedisException) {
			// check connection flag
			if (ex instanceof JedisConnectionException) {
				broken = true;
			}
			return JedisUtils.convertJedisAccessException((JedisException) ex);
		}
		if (ex instanceof IOException) {
			return JedisUtils.convertJedisAccessException((IOException) ex);
		}

		throw new UncategorizedKeyvalueStoreException("Unknown jedis exception", ex);
	}

	@Override
	public void close() throws DataAccessException {
		// return the connection to the pool
		try {
			if (pool != null) {
				if (broken) {
					pool.returnBrokenResource(jedis);
				}
				else {
					pool.returnResource(jedis);
				}
			}
		} catch (Exception ex) {
			pool.returnBrokenResource(jedis);
		}

		if (pool != null) {
			return;
		}

		// else close the connection normally
		try {
			if (isQueueing()) {
				client.quit();
				client.disconnect();
				return;
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
	public boolean isPipelined() {
		return (pipeline != null);
	}

	@Override
	public void openPipeline() {
		if (pipeline == null) {
			pipeline = jedis.pipelined();
		}
	}

	@Override
	public List<Object> closePipeline() {
		if (pipeline != null) {
			return pipeline.execute();
		}
		return Collections.emptyList();
	}

	@Override
	public List<byte[]> sort(byte[] key, SortParameters params) {

		SortingParams sortParams = JedisUtils.convertSortParams(params);

		try {
			if (isQueueing()) {
				if (sortParams != null) {
					transaction.sort(key, sortParams);
				}
				else {
					transaction.sort(key);
				}

				return null;
			}
			if (isPipelined()) {
				if (sortParams != null) {
					pipeline.sort(key, sortParams);
				}
				else {
					pipeline.sort(key);
				}
				
				return null;
			}
			return (sortParams != null ? jedis.sort(key, sortParams) : jedis.sort(key));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long sort(byte[] key, SortParameters params, byte[] sortKey) {

		SortingParams sortParams = JedisUtils.convertSortParams(params);

		try {
			if (isQueueing()) {
				if (sortParams != null) {
					transaction.sort(key, sortParams, sortKey);
				}
				else {
					transaction.sort(key, sortKey);
				}

				return null;
			}
			if (isPipelined()) {
				if (sortParams != null) {
					pipeline.sort(key, sortParams, sortKey);
				}
				else {
					pipeline.sort(key, sortKey);
				}

				return null;
			}
			return (sortParams != null ? jedis.sort(key, sortParams, sortKey) : jedis.sort(key, sortKey));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long dbSize() {
		try {
			if (isQueueing()) {
				transaction.dbSize();
				return null;
			}
			if (isPipelined()) {
				throw new UnsupportedOperationException();
			}
			return jedis.dbSize();
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}


	@Override
	public void flushDb() {
		try {
			if (isQueueing()) {
				transaction.flushDB();
				return;
			}
			if (isPipelined()) {
				throw new UnsupportedOperationException();
			}
			jedis.flushDB();
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public void flushAll() {
		try {
			if (isQueueing()) {
				transaction.flushAll();
				return;
			}
			if (isPipelined()) {
				throw new UnsupportedOperationException();
			}
			jedis.flushAll();
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public void bgSave() {
		try {
			if (isQueueing()) {
				throw new UnsupportedOperationException();
			}
			if (isPipelined()) {
				pipeline.bgsave();
				return;
			}
			jedis.bgsave();
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public void bgWriteAof() {
		try {
			if (isQueueing()) {
				throw new UnsupportedOperationException();
			}
			if (isPipelined()) {
				pipeline.bgrewriteaof();
				return;
			}
			jedis.bgrewriteaof();
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public void save() {
		try {
			if (isQueueing()) {
				throw new UnsupportedOperationException();
			}
			if (isPipelined()) {
				pipeline.save();
				return;
			}
			jedis.save();
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public List<String> getConfig(String param) {
		try {
			if (isQueueing()) {
				throw new UnsupportedOperationException();
			}
			if (isPipelined()) {
				pipeline.configGet(param);
				return null;
			}
			return jedis.configGet(param);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Properties info() {
		try {
			if (isQueueing()) {
				throw new UnsupportedOperationException();
			}
			if (isPipelined()) {
				throw new UnsupportedOperationException();
			}
			return JedisUtils.info(jedis.info());
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long lastSave() {
		try {
			if (isQueueing()) {
				throw new UnsupportedOperationException();
			}
			if (isPipelined()) {
				pipeline.lastsave();
				return null;
			}
			return jedis.lastsave();
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public void setConfig(String param, String value) {
		try {
			if (isQueueing()) {
				throw new UnsupportedOperationException();
			}
			if (isPipelined()) {
				pipeline.configSet(param, value);
				return;
			}
			jedis.configSet(param, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}


	@Override
	public void resetConfigStats() {
		try {
			if (isQueueing()) {
				throw new UnsupportedOperationException();
			}
			if (isPipelined()) {
				pipeline.configResetStat();
				return;
			}
			jedis.configResetStat();
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public void shutdown() {
		try {
			if (isQueueing()) {
				throw new UnsupportedOperationException();
			}
			if (isPipelined()) {
				throw new UnsupportedOperationException();
			}
			jedis.shutdown();
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public byte[] echo(byte[] message) {
		try {
			if (isQueueing()) {
				throw new UnsupportedOperationException();
			}
			if (isPipelined()) {
				pipeline.echo(message);
				return null;
			}
			return jedis.echo(message);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public String ping() {
		try {
			if (isQueueing()) {
				transaction.ping();
				return null;
			}
			if (isPipelined()) {
				throw new UnsupportedOperationException();
			}
			return jedis.ping();
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long del(byte[]... keys) {
		try {
			if (isQueueing()) {
				transaction.del(keys);
				return null;
			}
			if (isPipelined()) {
				pipeline.del(keys);
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
			if (isPipelined()) {
				pipeline.exec();
				return null;
			}
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
			if (isPipelined()) {
				pipeline.exists(key);
				return null;
			}
			return jedis.exists(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Boolean expire(byte[] key, long seconds) {
		try {
			if (isQueueing()) {
				transaction.expire(key, (int) seconds);
				return null;
			}
			if (isPipelined()) {
				pipeline.expire(key, (int) seconds);
				return null;
			}
			return (jedis.expire(key, (int) seconds) == 1);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Boolean expireAt(byte[] key, long unixTime) {
		try {
			if (isQueueing()) {
				transaction.expireAt(key, unixTime);
				return null;
			}
			if (isPipelined()) {
				pipeline.expireAt(key, unixTime);
				return null;
			}
			return (jedis.expireAt(key, unixTime) == 1);
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
			if (isPipelined()) {
				pipeline.keys(pattern);
				return null;
			}
			return (jedis.keys(pattern));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public void multi() {
		if (isQueueing()) {
			return;
		}
		try {
			if (isPipelined()) {
				pipeline.multi();
				return;
			}
			jedis.multi();
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
			if (isPipelined()) {
				pipeline.persist(key);
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
			if (isPipelined()) {
				throw new UnsupportedOperationException();
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
				return;
			}
			if (isPipelined()) {
				pipeline.rename(oldName, newName);
				return;
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
			if (isPipelined()) {
				pipeline.renamenx(oldName, newName);
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
				return;
			}
			if (isPipelined()) {
				throw new UnsupportedOperationException();
			}
			jedis.select(dbIndex);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long ttl(byte[] key) {
		try {
			if (isQueueing()) {
				transaction.ttl(key);
				return null;
			}
			if (isPipelined()) {
				pipeline.ttl(key);
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
			if (isPipelined()) {
				pipeline.type(key);
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
				if (isPipelined()) {
					pipeline.watch(key);
				} else {
					jedis.watch(key);
				}
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
			if (isPipelined()) {
				pipeline.get(key);
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
			if (isQueueing()) {
				transaction.set(key, value);
				return;
			}
			if (isPipelined()) {
				pipeline.set(key, value);
				return;
			}
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
			if (isPipelined()) {
				pipeline.getSet(key, value);
				return null;
			}
			return jedis.getSet(key, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long append(byte[] key, byte[] value) {
		try {
			if (isQueueing()) {
				transaction.append(key, value);
				return null;
			}
			if (isPipelined()) {
				pipeline.append(key, value);
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
			if (isPipelined()) {
				pipeline.mget(keys);
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
				return;
			}
			if (isPipelined()) {
				pipeline.mset(JedisUtils.convert(tuples));
				return;
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
				return;
			}
			if (isPipelined()) {
				pipeline.msetnx(JedisUtils.convert(tuples));
				return;
			}
			jedis.msetnx(JedisUtils.convert(tuples));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public void setEx(byte[] key, long time, byte[] value) {
		try {
			if (isQueueing()) {
				transaction.setex(key, (int) time, value);
				return;
			}
			if (isPipelined()) {
				pipeline.setex(key, (int) time, value);
				return;
			}
			jedis.setex(key, (int) time, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Boolean setNX(byte[] key, byte[] value) {
		try {
			if (isQueueing()) {
				transaction.setnx(key, value);
				return null;
			}
			if (isPipelined()) {
				pipeline.setnx(key, value);
				return null;
			}
			return JedisUtils.convertCodeReply(jedis.setnx(key, value));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public byte[] getRange(byte[] key, int start, int end) {
		try {
			if (isQueueing()) {
				transaction.substr(key, (int) start, (int) end);
				return null;
			}
			if (isPipelined()) {
				pipeline.substr(key, (int) start, (int) end);
				return null;
			}
			return jedis.substr(key, (int) start, (int) end);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long decr(byte[] key) {
		try {
			if (isQueueing()) {
				transaction.decr(key);
				return null;
			}
			if (isPipelined()) {
				pipeline.decr(key);
				return null;
			}
			return jedis.decr(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long decrBy(byte[] key, long value) {
		try {
			if (isQueueing()) {
				transaction.decrBy(key, (int) value);
				return null;
			}
			if (isPipelined()) {
				pipeline.decrBy(key, (int) value);
				return null;
			}
			return jedis.decrBy(key, (int) value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long incr(byte[] key) {
		try {
			if (isQueueing()) {
				transaction.incr(key);
				return null;
			}
			if (isPipelined()) {
				pipeline.incr(key);
				return null;
			}
			return jedis.incr(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long incrBy(byte[] key, long value) {
		try {
			if (isQueueing()) {
				transaction.incrBy(key, (int) value);
				return null;
			}
			if (isPipelined()) {
				pipeline.incrBy(key, (int) value);
				return null;
			}
			return jedis.incrBy(key, (int) value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Boolean getBit(byte[] key, long offset) {
		try {
			if (isQueueing()) {
				//				transaction.getbit(key, (int) offset);
				//				return null;
				throw new UnsupportedOperationException();
			}
			if (isPipelined()) {
				throw new UnsupportedOperationException();
			}
			return (jedis.getbit(key, offset) == 0 ? Boolean.FALSE : Boolean.TRUE);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public void setBit(byte[] key, long offset, boolean value) {
		try {
			if (isQueueing()) {
				//				transaction.setbit(key, (int) offset, JedisUtils.asBit(value));
				//				return;
				throw new UnsupportedOperationException();
			}
			if (isPipelined()) {
				throw new UnsupportedOperationException();
			}
			jedis.setbit(key, offset, JedisUtils.asBit(value));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public void setRange(byte[] key, int start, int end) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Long strLen(byte[] key) {
		try {
			if (isQueueing()) {
				throw new UnsupportedOperationException();
			}
			if (isPipelined()) {
				pipeline.strlen(key);
				return null;
			}
			return jedis.strlen(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	//
	// List commands
	//

	@Override
	public Long lPush(byte[] key, byte[] value) {
		try {
			if (isQueueing()) {
				transaction.lpush(key, value);
				return null;
			}
			if (isPipelined()) {
				pipeline.lpush(key, value);
				return null;
			}
			return jedis.lpush(key, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long rPush(byte[] key, byte[] value) {
		try {
			if (isQueueing()) {
				transaction.rpush(key, value);
				return null;
			}
			if (isPipelined()) {
				pipeline.rpush(key, value);
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
			if (isPipelined()) {
				final List<byte[]> args = new ArrayList<byte[]>();
				for (final byte[] arg : keys) {
		            args.add(arg);
		        }
		        args.add(Protocol.toByteArray(timeout));
		        pipeline.blpop(args.toArray(new byte[args.size()][]));
		        return null;
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
			if (isPipelined()) {
				final List<byte[]> args = new ArrayList<byte[]>();
				for (final byte[] arg : keys) {
		            args.add(arg);
		        }
		        args.add(Protocol.toByteArray(timeout));
		        pipeline.brpop(args.toArray(new byte[args.size()][]));
		        return null;
			}
			return jedis.brpop(timeout, keys);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public byte[] lIndex(byte[] key, long index) {
		try {
			if (isQueueing()) {
				transaction.lindex(key, (int) index);
				return null;
			}
			if (isPipelined()) {
				pipeline.lindex(key, (int) index);
				return null;
			}
			return jedis.lindex(key, (int) index);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long lInsert(byte[] key, POSITION where, byte[] pivot, byte[] value) {
		try {
			if (isQueueing()) {
				//				transaction.linsert(key, JedisUtils.convertPosition(where), pivot, value);
				//				return null;
				throw new UnsupportedOperationException();
			}
			if (isPipelined()) {
				pipeline.linsert(key, JedisUtils.convertPosition(where), pivot, value);
				return null;
			}
			return jedis.linsert(key, JedisUtils.convertPosition(where), pivot, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long lLen(byte[] key) {
		try {
			if (isQueueing()) {
				transaction.llen(key);
				return null;
			}
			if (isPipelined()) {
				pipeline.llen(key);
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
			if (isPipelined()) {
				pipeline.lpop(key);
				return null;
			}
			return jedis.lpop(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public List<byte[]> lRange(byte[] key, long start, long end) {
		try {
			if (isQueueing()) {
				transaction.lrange(key, (int) start, (int) end);
				return null;
			}
			if (isPipelined()) {
				pipeline.lrange(key, (int) start, (int) end);
				return null;
			}
			return jedis.lrange(key, (int) start, (int) end);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long lRem(byte[] key, long count, byte[] value) {
		try {
			if (isQueueing()) {
				transaction.lrem(key, (int) count, value);
				return null;
			}
			if (isPipelined()) {
				pipeline.lrem(key, (int) count, value);
				return null;
			}
			return jedis.lrem(key, (int) count, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public void lSet(byte[] key, long index, byte[] value) {
		try {
			if (isQueueing()) {
				transaction.lset(key, (int) index, value);
				return;
			}
			if (isPipelined()) {
				pipeline.lset(key, (int) index, value);
				return;
			}
			jedis.lset(key, (int) index, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public void lTrim(byte[] key, long start, long end) {
		try {
			if (isQueueing()) {
				transaction.ltrim(key, (int) start, (int) end);
				return;
			}
			if (isPipelined()) {
				pipeline.ltrim(key, (int) start, (int) end);
				return;
			}
			jedis.ltrim(key, (int) start, (int) end);
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
			if (isPipelined()) {
				pipeline.rpop(key);
				return null;
			}
			return jedis.rpop(key);
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
			if (isPipelined()) {
				pipeline.rpoplpush(srcKey, dstKey);
				return null;
			}
			return jedis.rpoplpush(srcKey, dstKey);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public byte[] bRPopLPush(int timeout, byte[] srcKey, byte[] dstKey) {
		try {
			if (isQueueing()) {
				throw new UnsupportedOperationException();
			}
			if (isPipelined()) {
				pipeline.brpoplpush(srcKey, dstKey, timeout);
				return null;
			}
			return jedis.brpoplpush(srcKey, dstKey, timeout);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long lPushX(byte[] key, byte[] value) {
		try {
			if (isQueueing()) {
				throw new UnsupportedOperationException();
			}
			if (isPipelined()) {
				pipeline.lpushx(key, value);
				return null;
			}
			return jedis.lpushx(key, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long rPushX(byte[] key, byte[] value) {
		try {
			if (isQueueing()) {
				throw new UnsupportedOperationException();
			}
			if (isPipelined()) {
				pipeline.rpushx(key, value);
				return null;
			}
			return jedis.rpushx(key, value);
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
			if (isPipelined()) {
				pipeline.sadd(key, value);
				return null;
			}
			return (jedis.sadd(key, value) == 1);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long sCard(byte[] key) {
		try {
			if (isQueueing()) {
				transaction.scard(key);
				return null;
			}
			if (isPipelined()) {
				pipeline.scard(key);
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
			if (isPipelined()) {
				pipeline.sdiff(keys);
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
				return;
			}
			if (isPipelined()) {
				pipeline.sdiffstore(destKey, keys);
				return;
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
			if (isPipelined()) {
				pipeline.sinter(keys);
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
				return;
			}
			if (isPipelined()) {
				pipeline.sinterstore(destKey, keys);
				return;
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
			if (isPipelined()) {
				pipeline.sismember(key, value);
				return null;
			}
			return jedis.sismember(key, value);
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
			if (isPipelined()) {
				pipeline.smembers(key);
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
			if (isPipelined()) {
				pipeline.smove(srcKey, destKey, value);
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
			if (isPipelined()) {
				pipeline.spop(key);
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
			if (isPipelined()) {
				pipeline.srandmember(key);
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
			if (isPipelined()) {
				pipeline.srem(key, value);
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
			if (isPipelined()) {
				pipeline.sunion(keys);
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
				return;
			}
			if (isPipelined()) {
				pipeline.sunionstore(destKey, keys);
				return;
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
			if (isPipelined()) {
				pipeline.zadd(key, score, value);
				return null;
			}
			return JedisUtils.convertCodeReply(jedis.zadd(key, score, value));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long zCard(byte[] key) {
		try {
			if (isQueueing()) {
				transaction.zcard(key);
				return null;
			}
			if (isPipelined()) {
				pipeline.zcard(key);
				return null;
			}
			return jedis.zcard(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long zCount(byte[] key, double min, double max) {
		try {
			if (isQueueing()) {
				throw new UnsupportedOperationException();
			}
			if (isQueueing()) {
				pipeline.zcount(key, min, max);
				return null;
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
			if (isPipelined()) {
				pipeline.zincrby(key, increment, value);
				return null;
			}
			return jedis.zincrby(key, increment, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long zInterStore(byte[] destKey, Aggregate aggregate, int[] weights, byte[]... sets) {
		try {
			if (isQueueing()) {
				throw new UnsupportedOperationException();
			}
			ZParams zparams = new ZParams().weights(weights).aggregate(
					redis.clients.jedis.ZParams.Aggregate.valueOf(aggregate.name()));
			if (isPipelined()) {
				pipeline.zinterstore(destKey, zparams, sets);
				return null;
			}
			return jedis.zinterstore(destKey, zparams, sets);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long zInterStore(byte[] destKey, byte[]... sets) {
		try {
			if (isQueueing()) {
				throw new UnsupportedOperationException();
			}
			if (isQueueing()) {
				pipeline.zinterstore(destKey, sets);
				return null;
			}
			return jedis.zinterstore(destKey, sets);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Set<byte[]> zRange(byte[] key, long start, long end) {
		try {
			if (isQueueing()) {
				transaction.zrange(key, (int) start, (int) end);
				return null;
			}
			if (isPipelined()) {
				pipeline.zrange(key, (int) start, (int) end);
				return null;
			}
			return jedis.zrange(key, (int) start, (int) end);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Set<Tuple> zRangeWithScore(byte[] key, long start, long end) {
		try {
			if (isQueueing()) {
				transaction.zrangeWithScores(key, (int) start, (int) end);
				return null;
			}
			if (isPipelined()) {
				pipeline.zrangeWithScores(key, (int) start, (int) end);
				return null;
			}
			return JedisUtils.convertJedisTuple(jedis.zrangeWithScores(key, (int) start, (int) end));
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
			if (isPipelined()) {
				pipeline.zrangeByScore(key, min, max);
				return null;
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
			if (isPipelined()) {
				pipeline.zrangeByScoreWithScores(key, min, max);
				return null;
			}
			return JedisUtils.convertJedisTuple(jedis.zrangeByScoreWithScores(key, min, max));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Set<Tuple> zRevRangeWithScore(byte[] key, long start, long end) {
		try {
			if (isQueueing()) {
				transaction.zrangeWithScores(key, (int) start, (int) end);
				return null;
			}
			if (isPipelined()) {
				pipeline.zrangeWithScores(key, (int) start, (int) end);
				return null;
			}
			return JedisUtils.convertJedisTuple(jedis.zrangeByScoreWithScores(key, (int) start, (int) end));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Set<byte[]> zRangeByScore(byte[] key, double min, double max, long offset, long count) {
		try {
			if (isQueueing()) {
				throw new UnsupportedOperationException();
			}
			if (isPipelined()) {
				pipeline.zrangeByScore(key, min, max, (int) offset, (int) count);
				return null;
			}
			return jedis.zrangeByScore(key, min, max, (int) offset, (int) count);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Set<Tuple> zRangeByScoreWithScore(byte[] key, double min, double max, long offset, long count) {
		try {
			if (isQueueing()) {
				throw new UnsupportedOperationException();
			}
			if (isPipelined()) {
				pipeline.zrangeByScoreWithScores(key, min, max, (int) offset, (int) count);
				return null;
			}
			return JedisUtils.convertJedisTuple(jedis.zrangeByScoreWithScores(key, min, max, (int) offset, (int) count));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long zRank(byte[] key, byte[] value) {
		try {
			if (isQueueing()) {
				transaction.zrank(key, value);
				return null;
			}
			if (isPipelined()) {
				pipeline.zrank(key, value);
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
			if (isPipelined()) {
				pipeline.zrem(key, value);
				return null;
			}
			return JedisUtils.convertCodeReply(jedis.zrem(key, value));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long zRemRange(byte[] key, long start, long end) {
		try {
			if (isQueueing()) {
				throw new UnsupportedOperationException();
			}
			if (isPipelined()) {
				pipeline.zremrangeByRank(key, (int) start, (int) end);
				return null;
			}
			return jedis.zremrangeByRank(key, (int) start, (int) end);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long zRemRangeByScore(byte[] key, double min, double max) {
		try {
			if (isQueueing()) {
				throw new UnsupportedOperationException();
			}
			if (isPipelined()) {
				pipeline.zremrangeByScore(key, min, max);
				return null;
			}
			return jedis.zremrangeByScore(key, min, max);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Set<byte[]> zRevRange(byte[] key, long start, long end) {
		try {
			if (isQueueing()) {
				transaction.zrevrange(key, (int) start, (int) end);
				return null;
			}
			if (isPipelined()) {
				pipeline.zrevrange(key, (int) start, (int) end);
				return null;
			}
			return jedis.zrevrange(key, (int) start, (int) end);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long zRevRank(byte[] key, byte[] value) {
		try {
			if (isQueueing()) {
				transaction.zrevrank(key, value);
				return null;
			}
			if (isPipelined()) {
				pipeline.zrevrank(key, value);
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
			if (isPipelined()) {
				pipeline.zscore(key, value);
				return null;
			}
			return jedis.zscore(key, value);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long zUnionStore(byte[] destKey, Aggregate aggregate, int[] weights, byte[]... sets) {
		try {
			if (isQueueing()) {
				throw new UnsupportedOperationException();
			}
			ZParams zparams = new ZParams().weights(weights).aggregate(
					redis.clients.jedis.ZParams.Aggregate.valueOf(aggregate.name()));
			if (isPipelined()) {
				pipeline.zunionstore(destKey, zparams, sets);
				return null;
			}
			return jedis.zunionstore(destKey, zparams, sets);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long zUnionStore(byte[] destKey, byte[]... sets) {
		try {
			if (isQueueing()) {
				throw new UnsupportedOperationException();
			}
			if (isPipelined()) {
				pipeline.zunionstore(destKey, sets);
				return null;
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
			if (isPipelined()) {
				pipeline.hset(key, field, value);
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
			if (isPipelined()) {
				pipeline.hsetnx(key, field, value);
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
			if (isPipelined()) {
				pipeline.hdel(key, field);
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
			if (isPipelined()) {
				pipeline.hexists(key, field);
				return null;
			}
			return jedis.hexists(key, field);
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
			if (isPipelined()) {
				pipeline.hget(key, field);
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
			if (isPipelined()) {
				pipeline.hgetAll(key);
				return null;
			}
			return jedis.hgetAll(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long hIncrBy(byte[] key, byte[] field, long delta) {
		try {
			if (isQueueing()) {
				transaction.hincrBy(key, field, (int) delta);
				return null;
			}
			if (isPipelined()) {
				pipeline.hincrBy(key, field, (int) delta);
				return null;
			}
			return jedis.hincrBy(key, field, (int) delta);
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
			if (isPipelined()) {
				pipeline.hkeys(key);
				return null;
			}
			return jedis.hkeys(key);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long hLen(byte[] key) {
		try {
			if (isQueueing()) {
				transaction.hlen(key);
				return null;
			}
			if (isPipelined()) {
				pipeline.hlen(key);
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
			if (isPipelined()) {
				pipeline.hmget(key, fields);
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
				return;
			}
			if (isPipelined()) {
				pipeline.hmset(key, tuple);
				return;
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
			if (isPipelined()) {
				pipeline.hvals(key);
				return null;
			}
			return new ArrayList<byte[]>(jedis.hvals(key));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}


	//
	// Pub/Sub functionality
	//
	@Override
	public Long publish(byte[] channel, byte[] message) {
		try {
			if (isQueueing()) {
				throw new UnsupportedOperationException();
			}
			if (isPipelined()) {
				throw new UnsupportedOperationException();
			}
			return jedis.publish(channel, message);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Subscription getSubscription() {
		return subscription;
	}

	@Override
	public boolean isSubscribed() {
		return (subscription != null && subscription.isAlive());
	}

	@Override
	public void pSubscribe(MessageListener listener, byte[]... patterns) {
		if (isSubscribed()) {
			throw new SubscribedRedisConnectionException(
					"Connection already subscribed; use the connection Subscription to cancel or add new channels");
		}

		try {
			if (isQueueing()) {
				throw new UnsupportedOperationException();
			}
			if (isPipelined()) {
				throw new UnsupportedOperationException();
			}

			BinaryJedisPubSub jedisPubSub = JedisUtils.adaptPubSub(listener);

			subscription = new JedisSubscription(listener, jedisPubSub, null, patterns);
			jedis.psubscribe(jedisPubSub, patterns);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public void subscribe(MessageListener listener, byte[]... channels) {
		if (isSubscribed()) {
			throw new SubscribedRedisConnectionException(
					"Connection already subscribed; use the connection Subscription to cancel or add new channels");
		}

		try {
			if (isQueueing()) {
				throw new UnsupportedOperationException();
			}
			if (isPipelined()) {
				throw new UnsupportedOperationException();
			}

			BinaryJedisPubSub jedisPubSub = JedisUtils.adaptPubSub(listener);

			subscription = new JedisSubscription(listener, jedisPubSub, channels, null);
			jedis.subscribe(jedisPubSub, channels);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	private void checkSubscription() {
		if (isSubscribed()) {
			throw new SubscribedRedisConnectionException("Cannot execute command - connection is subscribed");
		}
	}
}