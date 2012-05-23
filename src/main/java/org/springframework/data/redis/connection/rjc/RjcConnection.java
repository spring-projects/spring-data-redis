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
package org.springframework.data.redis.connection.rjc;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.idevlab.rjc.Client;
import org.idevlab.rjc.RedisException;
import org.idevlab.rjc.Session;
import org.idevlab.rjc.SessionFactoryImpl;
import org.idevlab.rjc.SortingParams;
import org.idevlab.rjc.ZParams;
import org.idevlab.rjc.message.RedisNodeSubscriber;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisSubscribedConnectionException;
import org.springframework.data.redis.connection.SortParameters;
import org.springframework.data.redis.connection.Subscription;

/**
 * {@code RedisConnection} implementation on top of <a href="http://github.com/e-mzungu/rjc">rjc</a> library.
 * 
 * @author Costin Leau
 */
public class RjcConnection implements RedisConnection {

	private final int dbIndex;
	private boolean isClosed = false;

	private final Client client;
	private final Session session;
	private volatile Client pipeline;

	private volatile RjcSubscription subscription;
	private volatile RedisNodeSubscriber subscriber;

	public RjcConnection(org.idevlab.rjc.ds.RedisConnection connection, int dbIndex) {
		SingleDataSource connectionDataSource = new SingleDataSource(connection);
		session = new SessionFactoryImpl(connectionDataSource).create();
		subscriber = new RedisNodeSubscriber();
		subscriber.setDataSource(new SingleDataSource(new CloseSuppressingRjcConnection(connection)));
		client = new Client(connection);

		this.dbIndex = dbIndex;

		// select the db
		if (dbIndex > 0) {
			select(dbIndex);
		}
	}

	protected DataAccessException convertRjcAccessException(Exception ex) {
		if (ex instanceof RedisException) {
			return RjcUtils.convertRjcAccessException((RedisException) ex);
		}
		return new RedisSystemException("Unknown rjc exception", ex);
	}

	
	public void close() throws DataAccessException {
		isClosed = true;

		// reset the connection (in case a pool is being used)
		if (dbIndex > 0) {
			select(0);
		}

		try {
			subscriber.close();
			session.close();
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}


	
	public boolean isClosed() {
		return isClosed;
	}

	
	public Session getNativeConnection() {
		return session;
	}

	
	public boolean isQueueing() {
		return client.isInMulti();
	}

	
	public boolean isPipelined() {
		return (pipeline != null);
	}

	
	public void openPipeline() {
		if (pipeline == null) {
			pipeline = client;
		}
	}

	@SuppressWarnings("unchecked")
	
	public List<Object> closePipeline() {
		if (pipeline != null) {
			List execute = client.getAll();
			if (execute != null && !execute.isEmpty()) {
				return execute;
			}
		}
		return Collections.emptyList();
	}

	
	public List<byte[]> sort(byte[] key, SortParameters params) {

		SortingParams sortParams = RjcUtils.convertSortParams(params);
		final String stringKey = RjcUtils.decode(key);

		try {
			if (isPipelined()) {
				if (sortParams != null) {
					pipeline.sort(stringKey, sortParams);
				}
				else {
					pipeline.sort(stringKey);
				}

				return null;
			}
			return RjcUtils.convertToList((sortParams != null ? session.sort(stringKey, sortParams)
					: session.sort(stringKey)));
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public Long sort(byte[] key, SortParameters params, byte[] sortKey) {

		SortingParams sortParams = RjcUtils.convertSortParams(params);
		final String stringKey = RjcUtils.decode(key);
		final String stringSortKey = RjcUtils.decode(sortKey);

		try {
			if (isPipelined()) {
				if (sortParams != null) {
					pipeline.sort(stringKey, sortParams, stringSortKey);
				}
				else {
					pipeline.sort(stringKey, stringSortKey);
				}

				return null;
			}
			return (sortParams != null ? session.sort(stringKey, sortParams, stringSortKey) : session.sort(stringKey,
					stringSortKey));
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public Long dbSize() {
		try {
			if (isPipelined()) {
				pipeline.dbSize();
				return null;
			}
			return session.dbSize();
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}


	
	public void flushDb() {
		try {
			if (isPipelined()) {
				pipeline.flushDB();
				return;
			}
			session.flushDB();
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public void flushAll() {
		try {
			if (isPipelined()) {
				pipeline.flushAll();
				return;
			}
			session.flushAll();
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public void bgSave() {
		try {
			if (isPipelined()) {
				pipeline.bgsave();
				return;
			}
			session.bgsave();
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public void bgWriteAof() {
		try {
			if (isPipelined()) {
				pipeline.bgrewriteaof();
				return;
			}
			session.bgrewriteaof();
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public void save() {
		try {
			if (isPipelined()) {
				pipeline.save();
				return;
			}
			session.save();
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public List<String> getConfig(String param) {
		try {
			if (isPipelined()) {
				pipeline.configGet(param);
				return null;
			}
			return session.configGet(param);
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public Properties info() {
		try {
			if (isPipelined()) {
				pipeline.info();
				return null;
			}
			return RjcUtils.info(session.info());
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public Long lastSave() {
		try {
			if (isPipelined()) {
				pipeline.lastsave();
				return null;
			}
			return session.lastsave();
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public void setConfig(String param, String value) {
		try {
			if (isPipelined()) {
				pipeline.configSet(param, value);
				return;
			}
			session.configSet(param, value);
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}


	
	public void resetConfigStats() {
		try {
			if (isPipelined()) {
				pipeline.configResetStat();
				return;
			}
			client.configResetStat();
			client.getStatusCodeReply();
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public void shutdown() {
		try {
			if (isPipelined()) {
				pipeline.shutdown();
				return;
			}
			session.shutdown();
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public byte[] echo(byte[] message) {
		String stringMsg = RjcUtils.decode(message);
		try {
			if (isPipelined()) {
				pipeline.echo(stringMsg);
				return null;
			}
			return RjcUtils.encode(session.echo(stringMsg));
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public String ping() {
		try {
			if (isPipelined()) {
				pipeline.ping();
			}
			return session.ping();
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public Long del(byte[]... keys) {
		String[] stringKeys = RjcUtils.decodeMultiple(keys);

		try {
			if (isPipelined()) {
				pipeline.del(stringKeys);
				return null;
			}
			return session.del(stringKeys);
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public void discard() {
		try {
			if (isPipelined()) {
				pipeline.discard();
				return;
			}

			session.discard();
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public List<Object> exec() {
		try {
			if (isPipelined()) {
				pipeline.exec();
				return null;
			}
			return session.exec();
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public Boolean exists(byte[] key) {
		String stringKey = RjcUtils.decode(key);

		try {
			if (isPipelined()) {
				pipeline.exists(stringKey);
				return null;
			}
			return session.exists(stringKey);
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public Boolean expire(byte[] key, long seconds) {
		String stringKey = RjcUtils.decode(key);

		try {
			if (isPipelined()) {
				pipeline.expire(stringKey, (int) seconds);
				return null;
			}
			return session.expire(stringKey, (int) seconds);
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public Boolean expireAt(byte[] key, long unixTime) {
		String stringKey = RjcUtils.decode(key);

		try {
			if (isPipelined()) {
				pipeline.expireAt(stringKey, unixTime);
				return null;
			}
			return session.expireAt(stringKey, unixTime);
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public Set<byte[]> keys(byte[] pattern) {
		String stringKey = RjcUtils.decode(pattern);

		try {
			if (isPipelined()) {
				pipeline.keys(stringKey);
				return null;
			}
			return RjcUtils.convertToSet(session.keys(stringKey));
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public void multi() {
		if (isQueueing()) {
			return;
		}
		try {
			if (isPipelined()) {
				pipeline.multi();
				return;
			}
			session.multi();
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public Boolean persist(byte[] key) {
		String stringKey = RjcUtils.decode(key);

		try {
			if (isPipelined()) {
				pipeline.persist(stringKey);
				return null;
			}
			return session.persist(stringKey);
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public Boolean move(byte[] key, int dbIndex) {
		String stringKey = RjcUtils.decode(key);

		try {
			if (isPipelined()) {
				pipeline.move(stringKey, dbIndex);
				return null;
			}
			return session.move(stringKey, dbIndex);
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public byte[] randomKey() {
		try {
			if (isPipelined()) {
				pipeline.randomKey();
				return null;
			}
			return RjcUtils.encode(session.randomKey());
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public void rename(byte[] oldName, byte[] newName) {
		String stringOldKey = RjcUtils.decode(oldName);
		String stringNewKey = RjcUtils.decode(newName);

		try {
			if (isPipelined()) {
				pipeline.rename(stringOldKey, stringNewKey);
				return;
			}
			session.rename(stringOldKey, stringNewKey);
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public Boolean renameNX(byte[] oldName, byte[] newName) {
		String stringOldKey = RjcUtils.decode(oldName);
		String stringNewKey = RjcUtils.decode(newName);

		try {
			if (isPipelined()) {
				pipeline.renamenx(stringOldKey, stringNewKey);
				return null;
			}
			return session.renamenx(stringOldKey, stringNewKey);
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public void select(int dbIndex) {
		try {
			if (isPipelined()) {
				pipeline.select(dbIndex);
				return;
			}
			session.select(dbIndex);
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public Long ttl(byte[] key) {
		String stringKey = RjcUtils.decode(key);

		try {
			if (isPipelined()) {
				pipeline.ttl(stringKey);
				return null;
			}
			return session.ttl(stringKey);
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public DataType type(byte[] key) {
		String stringKey = RjcUtils.decode(key);

		try {
			if (isPipelined()) {
				pipeline.type(stringKey);
				return null;
			}
			return DataType.fromCode(session.type(stringKey));
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public void unwatch() {
		try {
			if (isPipelined()) {
				pipeline.unwatch();
				return;
			}

			session.unwatch();
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public void watch(byte[]... keys) {
		String[] stringKeys = RjcUtils.decodeMultiple(keys);

		if (isQueueing()) {
			return;
		}
		try {
			if (isPipelined()) {
				pipeline.watch(stringKeys);
				return;
			}
			else {
				session.watch(stringKeys);
			}
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	//
	// String commands
	//

	
	public byte[] get(byte[] key) {
		String stringKey = RjcUtils.decode(key);

		try {
			if (isPipelined()) {
				pipeline.get(stringKey);
				return null;
			}

			return RjcUtils.encode(session.get(stringKey));
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public void set(byte[] key, byte[] value) {
		String stringKey = RjcUtils.decode(key);
		String stringValue = RjcUtils.decode(value);

		try {
			if (isPipelined()) {
				pipeline.set(stringKey, stringValue);
				return;
			}
			session.set(stringKey, stringValue);
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}


	
	public byte[] getSet(byte[] key, byte[] value) {
		String stringKey = RjcUtils.decode(key);
		String stringValue = RjcUtils.decode(value);

		try {
			if (isPipelined()) {
				pipeline.getSet(stringKey, stringValue);
				return null;
			}
			return RjcUtils.encode(session.getSet(stringKey, stringValue));
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public Long append(byte[] key, byte[] value) {
		String stringKey = RjcUtils.decode(key);
		String stringValue = RjcUtils.decode(value);

		try {
			if (isPipelined()) {
				pipeline.append(stringKey, stringValue);
				return null;
			}
			return session.append(stringKey, stringValue);
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public List<byte[]> mGet(byte[]... keys) {
		String[] stringKeys = RjcUtils.decodeMultiple(keys);

		try {
			if (isPipelined()) {
				pipeline.mget(stringKeys);
				return null;
			}
			return RjcUtils.convertToList(session.mget(stringKeys));
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public void mSet(Map<byte[], byte[]> tuples) {
		String[] decodeMap = RjcUtils.flatten(tuples);

		try {
			if (isPipelined()) {
				pipeline.mset(decodeMap);
				return;
			}
			session.mset(decodeMap);
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public void mSetNX(Map<byte[], byte[]> tuples) {
		String[] decodeMap = RjcUtils.flatten(tuples);

		try {

			if (isPipelined()) {
				pipeline.msetnx(decodeMap);
				return;
			}
			session.msetnx(decodeMap);
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public void setEx(byte[] key, long time, byte[] value) {
		String stringKey = RjcUtils.decode(key);
		String stringValue = RjcUtils.decode(value);

		try {
			if (isPipelined()) {
				pipeline.setex(stringKey, (int) time, stringValue);
				return;
			}
			session.setex(stringKey, (int) time, stringValue);
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public Boolean setNX(byte[] key, byte[] value) {
		String stringKey = RjcUtils.decode(key);
		String stringValue = RjcUtils.decode(value);

		try {
			if (isPipelined()) {
				pipeline.setnx(stringKey, stringValue);
				return null;
			}
			return session.setnx(stringKey, stringValue);
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public byte[] getRange(byte[] key, long start, long end) {
		String stringKey = RjcUtils.decode(key);

		try {
			if (isPipelined()) {
				pipeline.getRange(stringKey, (int) start, (int) end);
				return null;
			}
			return RjcUtils.encode(session.getRange(stringKey, (int) start, (int) end));
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public Long decr(byte[] key) {
		String stringKey = RjcUtils.decode(key);
		try {

			if (isPipelined()) {
				pipeline.decr(stringKey);
				return null;
			}
			return session.decr(stringKey);
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public Long decrBy(byte[] key, long value) {
		String stringKey = RjcUtils.decode(key);
		try {

			if (isPipelined()) {
				pipeline.decrBy(stringKey, (int) value);
				return null;
			}
			return session.decrBy(stringKey, (int) value);
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public Long incr(byte[] key) {
		String stringKey = RjcUtils.decode(key);

		try {

			if (isPipelined()) {
				pipeline.incr(stringKey);
				return null;
			}
			return session.incr(stringKey);
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public Long incrBy(byte[] key, long value) {
		String stringKey = RjcUtils.decode(key);


		try {
			if (isPipelined()) {
				pipeline.incrBy(stringKey, (int) value);
				return null;
			}
			return session.incrBy(stringKey, (int) value);
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public Boolean getBit(byte[] key, long offset) {
		String stringKey = RjcUtils.decode(key);

		try {
			if (isPipelined()) {
				pipeline.getbit(stringKey, (int) offset);
				return null;
			}
			return (session.getBit(stringKey, (int) offset) == 0 ? Boolean.FALSE : Boolean.TRUE);
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public void setBit(byte[] key, long offset, boolean value) {
		String stringKey = RjcUtils.decode(key);

		try {
			if (isPipelined()) {
				pipeline.setbit(stringKey, (int) offset, RjcUtils.asBit(value));
				return;
			}
			session.setBit(stringKey, (int) offset, RjcUtils.asBit(value));
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public void setRange(byte[] key, byte[] value, long offset) {
		String stringKey = RjcUtils.decode(key);
		String stringValue = RjcUtils.decode(value);

		try {
			if (isPipelined()) {
				pipeline.setRange(stringKey, (int) offset, stringValue);
				return;
			}
			session.setRange(stringKey, (int) offset, stringValue);
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public Long strLen(byte[] key) {
		String stringKey = RjcUtils.decode(key);

		try {
			if (isPipelined()) {
				pipeline.strlen(stringKey);
				return null;
			}
			return session.strlen(stringKey);
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	//
	// List commands
	//

	
	public Long lPush(byte[] key, byte[] value) {
		String stringKey = RjcUtils.decode(key);
		String stringValue = RjcUtils.decode(value);

		try {
			if (isPipelined()) {
				pipeline.lpush(stringKey, stringValue);
				return null;
			}
			return session.lpush(stringKey, stringValue);
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public Long rPush(byte[] key, byte[] value) {
		String stringKey = RjcUtils.decode(key);
		String stringValue = RjcUtils.decode(value);

		try {

			if (isPipelined()) {
				pipeline.rpush(stringKey, stringValue);
				return null;
			}
			return session.rpush(stringKey, stringValue);
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public List<byte[]> bLPop(int timeout, byte[]... keys) {
		String[] stringKeys = RjcUtils.decodeMultiple(keys);

		try {
			if (isPipelined()) {
				pipeline.blpop(stringKeys);
				return null;
			}
			return RjcUtils.convertToList(session.blpop(timeout, stringKeys));
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public List<byte[]> bRPop(int timeout, byte[]... keys) {
		String[] stringKeys = RjcUtils.decodeMultiple(keys);

		try {
			if (isPipelined()) {
				pipeline.brpop(stringKeys);
				return null;
			}
			return RjcUtils.convertToList(session.brpop(timeout, stringKeys));
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public byte[] lIndex(byte[] key, long index) {
		String stringKey = RjcUtils.decode(key);


		try {
			if (isPipelined()) {
				pipeline.lindex(stringKey, (int) index);
				return null;
			}
			return RjcUtils.encode(session.lindex(stringKey, (int) index));
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public Long lInsert(byte[] key, Position where, byte[] pivot, byte[] value) {
		String stringKey = RjcUtils.decode(key);
		String stringValue = RjcUtils.decode(value);
		String stringPivot = RjcUtils.decode(pivot);
		Client.LIST_POSITION position = RjcUtils.convertPosition(where);

		try {
			if (isPipelined()) {
				pipeline.linsert(stringKey, position, stringPivot, stringValue);
				return null;
			}
			return session.linsert(stringKey, position, stringPivot, stringValue);
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public Long lLen(byte[] key) {
		String stringKey = RjcUtils.decode(key);

		try {

			if (isPipelined()) {
				pipeline.llen(stringKey);
				return null;
			}
			return session.llen(stringKey);
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public byte[] lPop(byte[] key) {
		String stringKey = RjcUtils.decode(key);

		try {

			if (isPipelined()) {
				pipeline.lpop(stringKey);
				return null;
			}
			return RjcUtils.encode(session.lpop(stringKey));
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public List<byte[]> lRange(byte[] key, long start, long end) {
		String stringKey = RjcUtils.decode(key);

		try {

			if (isPipelined()) {
				pipeline.lrange(stringKey, (int) start, (int) end);
				return null;
			}
			return RjcUtils.convertToList(session.lrange(stringKey, (int) start, (int) end));
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public Long lRem(byte[] key, long count, byte[] value) {
		String stringKey = RjcUtils.decode(key);
		String stringValue = RjcUtils.decode(value);

		try {

			if (isPipelined()) {
				pipeline.lrem(stringKey, (int) count, stringValue);
				return null;
			}
			return session.lrem(stringKey, (int) count, stringValue);
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public void lSet(byte[] key, long index, byte[] value) {
		String stringKey = RjcUtils.decode(key);
		String stringValue = RjcUtils.decode(value);
		try {

			if (isPipelined()) {
				pipeline.lset(stringKey, (int) index, stringValue);
				return;
			}
			session.lset(stringKey, (int) index, stringValue);
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public void lTrim(byte[] key, long start, long end) {
		String stringKey = RjcUtils.decode(key);

		try {

			if (isPipelined()) {
				pipeline.ltrim(stringKey, (int) start, (int) end);
				return;
			}
			session.ltrim(stringKey, (int) start, (int) end);
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public byte[] rPop(byte[] key) {
		String stringKey = RjcUtils.decode(key);

		try {

			if (isPipelined()) {
				pipeline.rpop(stringKey);
				return null;
			}
			return RjcUtils.encode(session.rpop(stringKey));
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public byte[] rPopLPush(byte[] srcKey, byte[] dstKey) {
		String stringKey = RjcUtils.decode(srcKey);
		String stringDest = RjcUtils.decode(dstKey);

		try {

			if (isPipelined()) {
				pipeline.rpoplpush(stringKey, stringDest);
				return null;
			}
			return RjcUtils.encode(session.rpoplpush(stringKey, stringDest));
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public byte[] bRPopLPush(int timeout, byte[] srcKey, byte[] dstKey) {
		String stringKey = RjcUtils.decode(srcKey);
		String stringDest = RjcUtils.decode(dstKey);

		try {
			if (isPipelined()) {
				pipeline.brpoplpush(stringKey, stringDest, timeout);
				return null;
			}
			return RjcUtils.encode(session.brpoplpush(stringKey, stringDest, timeout));
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public Long lPushX(byte[] key, byte[] value) {
		String stringKey = RjcUtils.decode(key);
		String stringValue = RjcUtils.decode(value);
		try {
			if (isPipelined()) {
				pipeline.lpushx(stringKey, stringValue);
				return null;
			}
			return session.lpushx(stringKey, stringValue);
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public Long rPushX(byte[] key, byte[] value) {
		String stringKey = RjcUtils.decode(key);
		String stringValue = RjcUtils.decode(value);
		try {
			if (isPipelined()) {
				pipeline.rpushx(stringKey, stringValue);
				return null;
			}
			return session.rpushx(stringKey, stringValue);
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}


	//
	// Set commands
	//

	
	public Boolean sAdd(byte[] key, byte[] value) {
		String stringKey = RjcUtils.decode(key);
		String stringValue = RjcUtils.decode(value);

		try {

			if (isPipelined()) {
				pipeline.sadd(stringKey, stringValue);
				return null;
			}
			return session.sadd(stringKey, stringValue);
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public Long sCard(byte[] key) {
		String stringKey = RjcUtils.decode(key);

		try {

			if (isPipelined()) {
				pipeline.scard(stringKey);
				return null;
			}
			return session.scard(stringKey);
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public Set<byte[]> sDiff(byte[]... keys) {
		String[] stringKeys = RjcUtils.decodeMultiple(keys);

		try {

			if (isPipelined()) {
				pipeline.sdiff(stringKeys);
				return null;
			}
			return RjcUtils.convertToSet(session.sdiff(stringKeys));
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public Long sDiffStore(byte[] destKey, byte[]... keys) {
		String stringKey = RjcUtils.decode(destKey);
		String[] stringKeys = RjcUtils.decodeMultiple(keys);

		try {

			if (isPipelined()) {
				pipeline.sdiffstore(stringKey, stringKeys);
				return null;
			}
			return session.sdiffstore(stringKey, stringKeys);
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public Set<byte[]> sInter(byte[]... keys) {
		String[] stringKeys = RjcUtils.decodeMultiple(keys);
		try {

			if (isPipelined()) {
				pipeline.sinter(stringKeys);
				return null;
			}
			return RjcUtils.convertToSet(session.sinter(stringKeys));
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public Long sInterStore(byte[] destKey, byte[]... keys) {
		String stringKey = RjcUtils.decode(destKey);
		String[] stringKeys = RjcUtils.decodeMultiple(keys);
		try {

			if (isPipelined()) {
				pipeline.sinterstore(stringKey, stringKeys);
				return null;
			}
			return session.sinterstore(stringKey, stringKeys);
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public Boolean sIsMember(byte[] key, byte[] value) {
		String stringKey = RjcUtils.decode(key);
		String stringValue = RjcUtils.decode(value);

		try {

			if (isPipelined()) {
				pipeline.sismember(stringKey, stringValue);
				return null;
			}
			return session.sismember(stringKey, stringValue);
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public Set<byte[]> sMembers(byte[] key) {
		String stringKey = RjcUtils.decode(key);
		try {

			if (isPipelined()) {
				pipeline.smembers(stringKey);
				return null;
			}
			return RjcUtils.convertToSet(session.smembers(stringKey));
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public Boolean sMove(byte[] srcKey, byte[] destKey, byte[] value) {
		String stringSrc = RjcUtils.decode(srcKey);
		String stringDest = RjcUtils.decode(destKey);
		String stringValue = RjcUtils.decode(value);

		try {

			if (isPipelined()) {
				pipeline.smove(stringSrc, stringDest, stringValue);
				return null;
			}
			return session.smove(stringSrc, stringDest, stringValue);
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public byte[] sPop(byte[] key) {
		String stringKey = RjcUtils.decode(key);
		try {

			if (isPipelined()) {
				pipeline.spop(stringKey);
				return null;
			}
			return RjcUtils.encode(session.spop(stringKey));
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public byte[] sRandMember(byte[] key) {
		String stringKey = RjcUtils.decode(key);
		try {

			if (isPipelined()) {
				pipeline.srandmember(stringKey);
				return null;
			}
			return RjcUtils.encode(session.srandmember(stringKey));
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public Boolean sRem(byte[] key, byte[] value) {
		String stringKey = RjcUtils.decode(key);
		String stringValue = RjcUtils.decode(value);

		try {

			if (isPipelined()) {
				pipeline.srem(stringKey, stringValue);
				return null;
			}
			return session.srem(stringKey, stringValue);
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public Set<byte[]> sUnion(byte[]... keys) {
		String[] stringKeys = RjcUtils.decodeMultiple(keys);

		try {

			if (isPipelined()) {
				pipeline.sunion(stringKeys);
				return null;
			}
			return RjcUtils.convertToSet(session.sunion(stringKeys));
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public Long sUnionStore(byte[] destKey, byte[]... keys) {
		String stringKey = RjcUtils.decode(destKey);
		String[] stringKeys = RjcUtils.decodeMultiple(keys);

		try {

			if (isPipelined()) {
				pipeline.sunionstore(stringKey, stringKeys);
				return null;
			}
			return session.sunionstore(stringKey, stringKeys);
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	//
	// ZSet commands
	//

	
	public Boolean zAdd(byte[] key, double score, byte[] value) {
		String stringKey = RjcUtils.decode(key);
		String stringValue = RjcUtils.decode(value);

		try {
			if (isPipelined()) {
				pipeline.zadd(stringKey, score, stringValue);
				return null;
			}
			return session.zadd(stringKey, score, stringValue);
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public Long zCard(byte[] key) {
		String stringKey = RjcUtils.decode(key);

		try {
			if (isPipelined()) {
				pipeline.zcard(stringKey);
				return null;
			}
			return session.zcard(stringKey);
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public Long zCount(byte[] key, double min, double max) {
		String stringKey = RjcUtils.decode(key);
		try {
			if (isPipelined()) {
				pipeline.zcount(stringKey, min, max);
				return null;
			}

			return session.zcount(stringKey, min, max);
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public Double zIncrBy(byte[] key, double increment, byte[] value) {
		String stringKey = RjcUtils.decode(key);
		String stringValue = RjcUtils.decode(value);

		try {
			if (isPipelined()) {
				pipeline.zincrby(stringKey, increment, stringValue);
				return null;
			}
			return Double.valueOf(session.zincrby(stringKey, increment, stringValue));
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public Long zInterStore(byte[] destKey, Aggregate aggregate, int[] weights, byte[]... sets) {
		String stringKey = RjcUtils.decode(destKey);
		String[] stringKeys = RjcUtils.decodeMultiple(sets);

		ZParams zparams = RjcUtils.toZParams(aggregate, weights);

		try {
			if (isPipelined()) {
				pipeline.zinterstore(stringKey, zparams, stringKeys);
				return null;
			}
			return session.zinterstore(stringKey, zparams, stringKeys);
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public Long zInterStore(byte[] destKey, byte[]... sets) {
		String stringKey = RjcUtils.decode(destKey);
		String[] stringKeys = RjcUtils.decodeMultiple(sets);
		try {
			if (isPipelined()) {
				pipeline.zinterstore(stringKey, stringKeys);
				return null;
			}

			return session.zinterstore(stringKey, stringKeys);
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public Set<byte[]> zRange(byte[] key, long start, long end) {
		String stringKey = RjcUtils.decode(key);
		try {

			if (isPipelined()) {
				pipeline.zrange(stringKey, (int) start, (int) end);
				return null;
			}
			return RjcUtils.convertToSet(session.zrange(stringKey, (int) start, (int) end));
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public Set<Tuple> zRangeWithScores(byte[] key, long start, long end) {
		String stringKey = RjcUtils.decode(key);
		try {

			if (isPipelined()) {
				pipeline.zrangeWithScores(stringKey, (int) start, (int) end);
				return null;
			}
			return RjcUtils.convertElementScore(session.zrangeWithScores(stringKey, (int) start, (int) end));
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public Set<byte[]> zRangeByScore(byte[] key, double min, double max) {
		String stringKey = RjcUtils.decode(key);
		String minString = Double.toString(min);
		String maxString = Double.toString(max);

		try {
			if (isPipelined()) {
				pipeline.zrangeByScore(stringKey, minString, maxString);
				return null;
			}
			return RjcUtils.convertToSet(session.zrangeByScore(stringKey, minString, maxString));
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public Set<byte[]> zRangeByScore(byte[] key, double min, double max, long offset, long count) {
		String stringKey = RjcUtils.decode(key);
		String minString = Double.toString(min);
		String maxString = Double.toString(max);

		try {
			if (isPipelined()) {
				pipeline.zrangeByScore(stringKey, minString, maxString, (int) offset, (int) count);
				return null;
			}
			return RjcUtils.convertToSet(session.zrangeByScore(stringKey, minString, maxString, (int) offset,
					(int) count));
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}


	
	public Set<byte[]> zRevRangeByScore(byte[] key, double min, double max, long offset, long count) {
		String stringKey = RjcUtils.decode(key);
		String minString = Double.toString(min);
		String maxString = Double.toString(max);

		try {
			if (isPipelined()) {
				pipeline.zrevrangeByScore(stringKey, minString, maxString, (int) offset, (int) count);
				return null;
			}
			return RjcUtils.convertToSet(session.zrevrangeByScore(stringKey, minString, maxString, (int) offset,
					(int) count));
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public Set<byte[]> zRevRangeByScore(byte[] key, double min, double max) {
		String stringKey = RjcUtils.decode(key);
		String minString = Double.toString(min);
		String maxString = Double.toString(max);

		try {
			if (isPipelined()) {
				pipeline.zrevrangeByScore(stringKey, minString, maxString);
				return null;
			}
			return RjcUtils.convertToSet(session.zrevrangeByScore(stringKey, minString, maxString));
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public Set<Tuple> zRangeByScoreWithScores(byte[] key, double min, double max) {
		String stringKey = RjcUtils.decode(key);
		String minString = Double.toString(min);
		String maxString = Double.toString(max);

		try {
			if (isPipelined()) {
				pipeline.zrangeByScoreWithScores(stringKey, minString, maxString);
				return null;
			}
			return RjcUtils.convertElementScore(session.zrangeByScoreWithScores(stringKey, minString, maxString));
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public Set<Tuple> zRevRangeWithScores(byte[] key, long start, long end) {
		String stringKey = RjcUtils.decode(key);
		String minString = Long.toString(start);
		String maxString = Long.toString(end);

		try {

			if (isPipelined()) {
				pipeline.zrevrangeByScoreWithScores(stringKey, minString, maxString);
				return null;
			}
			return RjcUtils.convertElementScore(session.zrevrangeByScoreWithScores(stringKey, minString, maxString));
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public Set<Tuple> zRangeByScoreWithScores(byte[] key, double min, double max, long offset, long count) {
		String stringKey = RjcUtils.decode(key);
		String minString = Double.toString(min);
		String maxString = Double.toString(max);

		try {
			if (isPipelined()) {
				pipeline.zrangeByScoreWithScores(stringKey, minString, maxString, (int) offset, (int) count);
				return null;
			}
			return RjcUtils.convertElementScore(session.zrangeByScoreWithScores(stringKey, minString, maxString,
					(int) offset, (int) count));
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}


	
	public Set<Tuple> zRevRangeByScoreWithScores(byte[] key, double min, double max, long offset, long count) {
		String stringKey = RjcUtils.decode(key);
		String minString = Double.toString(min);
		String maxString = Double.toString(max);

		try {

			if (isPipelined()) {
				pipeline.zrevrangeByScoreWithScores(stringKey, minString, maxString, (int) offset, (int) count);
				return null;
			}
			return RjcUtils.convertElementScore(session.zrevrangeByScoreWithScores(stringKey, minString, maxString,
					(int) offset, (int) count));
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public Set<Tuple> zRevRangeByScoreWithScores(byte[] key, double min, double max) {
		String stringKey = RjcUtils.decode(key);
		String minString = Double.toString(min);
		String maxString = Double.toString(max);

		try {

			if (isPipelined()) {
				pipeline.zrevrangeByScoreWithScores(stringKey, minString, maxString);
				return null;
			}
			return RjcUtils.convertElementScore(session.zrevrangeByScoreWithScores(stringKey, minString, maxString));
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public Long zRank(byte[] key, byte[] value) {
		String stringKey = RjcUtils.decode(key);
		String stringValue = RjcUtils.decode(value);

		try {
			if (isPipelined()) {
				pipeline.zrank(stringKey, stringValue);
				return null;
			}
			return session.zrank(stringKey, stringValue);
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public Boolean zRem(byte[] key, byte[] value) {
		String stringKey = RjcUtils.decode(key);
		String stringValue = RjcUtils.decode(value);

		try {
			if (isPipelined()) {
				pipeline.zrem(stringKey, stringValue);
				return null;
			}
			return session.zrem(stringKey, stringValue);
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public Long zRemRange(byte[] key, long start, long end) {
		String stringKey = RjcUtils.decode(key);
		try {
			if (isPipelined()) {
				pipeline.zremrangeByRank(stringKey, (int) start, (int) end);
				return null;
			}
			return session.zremrangeByRank(stringKey, (int) start, (int) end);
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public Long zRemRangeByScore(byte[] key, double min, double max) {
		String stringKey = RjcUtils.decode(key);
		String minString = Double.toString(min);
		String maxString = Double.toString(max);

		try {
			if (isPipelined()) {
				pipeline.zremrangeByScore(stringKey, minString, maxString);
				return null;
			}
			return session.zremrangeByScore(stringKey, minString, maxString);
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public Set<byte[]> zRevRange(byte[] key, long start, long end) {
		String stringKey = RjcUtils.decode(key);
		try {

			if (isPipelined()) {
				pipeline.zrevrange(stringKey, (int) start, (int) end);
				return null;
			}
			return RjcUtils.convertToSet(session.zrevrange(stringKey, (int) start, (int) end));
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public Long zRevRank(byte[] key, byte[] value) {
		String stringKey = RjcUtils.decode(key);
		String stringValue = RjcUtils.decode(value);

		try {
			if (isPipelined()) {
				pipeline.zrevrank(stringKey, stringValue);
				return null;
			}
			return session.zrevrank(stringKey, stringValue);
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public Double zScore(byte[] key, byte[] value) {
		String stringKey = RjcUtils.decode(key);
		String stringValue = RjcUtils.decode(value);

		try {
			if (isPipelined()) {
				pipeline.zscore(stringKey, stringValue);
				return null;
			}
			return RjcUtils.convert(session.zscore(stringKey, stringValue));
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public Long zUnionStore(byte[] destKey, Aggregate aggregate, int[] weights, byte[]... sets) {
		String stringKey = RjcUtils.decode(destKey);
		String[] stringKeys = RjcUtils.decodeMultiple(destKey);

		ZParams zparams = RjcUtils.toZParams(aggregate, weights);

		try {
			if (isPipelined()) {
				pipeline.zunionstore(stringKey, zparams, stringKeys);
				return null;
			}
			return session.zunionstore(stringKey, zparams, stringKeys);
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public Long zUnionStore(byte[] destKey, byte[]... sets) {
		String stringKey = RjcUtils.decode(destKey);
		String[] stringKeys = RjcUtils.decodeMultiple(sets);

		try {
			if (isPipelined()) {
				pipeline.zunionstore(stringKey, stringKeys);
				return null;
			}
			return session.zunionstore(stringKey, stringKeys);
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	//
	// Hash commands
	//

	
	public Boolean hSet(byte[] key, byte[] field, byte[] value) {
		String stringKey = RjcUtils.decode(key);
		String stringField = RjcUtils.decode(field);
		String stringValue = RjcUtils.decode(value);

		try {
			if (isPipelined()) {
				pipeline.hset(stringKey, stringField, stringValue);
				return null;
			}
			return session.hset(stringKey, stringField, stringValue);
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public Boolean hSetNX(byte[] key, byte[] field, byte[] value) {
		String stringKey = RjcUtils.decode(key);
		String stringField = RjcUtils.decode(field);
		String stringValue = RjcUtils.decode(value);

		try {
			if (isPipelined()) {
				pipeline.hsetnx(stringKey, stringField, stringValue);
				return null;
			}
			return session.hsetnx(stringKey, stringField, stringValue);
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public Boolean hDel(byte[] key, byte[] field) {
		String stringKey = RjcUtils.decode(key);
		String stringField = RjcUtils.decode(field);

		try {
			if (isPipelined()) {
				pipeline.hdel(stringKey, stringField);
				return null;
			}
			return session.hdel(stringKey, stringField);
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public Boolean hExists(byte[] key, byte[] field) {
		String stringKey = RjcUtils.decode(key);
		String stringField = RjcUtils.decode(field);

		try {
			if (isPipelined()) {
				pipeline.hexists(stringKey, stringField);
				return null;
			}
			return session.hexists(stringKey, stringField);
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public byte[] hGet(byte[] key, byte[] field) {
		String stringKey = RjcUtils.decode(key);
		String stringField = RjcUtils.decode(field);

		try {
			if (isPipelined()) {
				pipeline.hget(stringKey, stringField);
				return null;
			}
			return RjcUtils.encode(session.hget(stringKey, stringField));
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public Map<byte[], byte[]> hGetAll(byte[] key) {
		String stringKey = RjcUtils.decode(key);

		try {
			if (isPipelined()) {
				pipeline.hgetAll(stringKey);
				return null;
			}
			return RjcUtils.encodeMap(session.hgetAll(stringKey));
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public Long hIncrBy(byte[] key, byte[] field, long delta) {
		String stringKey = RjcUtils.decode(key);
		String stringField = RjcUtils.decode(field);

		try {
			if (isPipelined()) {
				pipeline.hincrBy(stringKey, stringField, (int) delta);
				return null;
			}
			return session.hincrBy(stringKey, stringField, (int) delta);
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public Set<byte[]> hKeys(byte[] key) {
		String stringKey = RjcUtils.decode(key);
		try {
			if (isPipelined()) {
				pipeline.hkeys(stringKey);
				return null;
			}
			return RjcUtils.convertToSet(session.hkeys(stringKey));
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public Long hLen(byte[] key) {
		String stringKey = RjcUtils.decode(key);
		try {
			if (isPipelined()) {
				pipeline.hlen(stringKey);
				return null;
			}
			return session.hlen(stringKey);
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public List<byte[]> hMGet(byte[] key, byte[]... fields) {
		String stringKey = RjcUtils.decode(key);
		String[] stringKeys = RjcUtils.decodeMultiple(fields);

		try {
			if (isPipelined()) {
				pipeline.hmget(stringKey, stringKeys);
				return null;
			}
			return RjcUtils.convertToList(session.hmget(stringKey, stringKeys));
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public void hMSet(byte[] key, Map<byte[], byte[]> tuple) {
		String stringKey = RjcUtils.decode(key);
		Map<String, String> stringTuple = RjcUtils.decodeMap(tuple);

		try {
			if (isPipelined()) {
				pipeline.hmset(stringKey, stringTuple);
				return;
			}
			session.hmset(stringKey, stringTuple);
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public List<byte[]> hVals(byte[] key) {
		String stringKey = RjcUtils.decode(key);
		try {

			if (isPipelined()) {
				pipeline.hvals(stringKey);
				return null;
			}
			return RjcUtils.convertToList(session.hvals(stringKey));
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
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
				throw new UnsupportedOperationException();
			}
			return session.publish(RjcUtils.decode(channel), RjcUtils.decode(message));
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public Subscription getSubscription() {
		return subscription;
	}

	
	public boolean isSubscribed() {
		return (subscription != null && subscription.isAlive());
	}

	
	public void pSubscribe(MessageListener listener, byte[]... patterns) {
		if (isSubscribed()) {
			throw new RedisSubscribedConnectionException(
					"Connection already subscribed; use the connection Subscription to cancel or add new channels");
		}

		try {
			if (isQueueing()) {
				throw new UnsupportedOperationException();
			}
			if (isPipelined()) {
				throw new UnsupportedOperationException();
			}

			subscription = new RjcSubscription(listener, subscriber);
			subscription.pSubscribe(patterns);
			subscriber.runSubscription();

		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	
	public void subscribe(MessageListener listener, byte[]... channels) {
		if (isSubscribed()) {
			throw new RedisSubscribedConnectionException(
					"Connection already subscribed; use the connection Subscription to cancel or add new channels");
		}

		try {
			if (isQueueing()) {
				throw new UnsupportedOperationException();
			}
			if (isPipelined()) {
				throw new UnsupportedOperationException();
			}

			subscription = new RjcSubscription(listener, subscriber);
			subscription.subscribe(channels);
			subscriber.runSubscription();

		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}

	private void checkSubscription() {
		if (isSubscribed()) {
			throw new RedisSubscribedConnectionException("Cannot execute command - connection is subscribed");
		}
	}
}