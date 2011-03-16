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
package org.springframework.data.keyvalue.redis.connection.rjc;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.idevlab.rjc.RedisException;
import org.idevlab.rjc.Session;
import org.idevlab.rjc.SessionFactoryImpl;
import org.springframework.dao.DataAccessException;
import org.springframework.data.keyvalue.UncategorizedKeyvalueStoreException;
import org.springframework.data.keyvalue.redis.connection.DataType;
import org.springframework.data.keyvalue.redis.connection.MessageListener;
import org.springframework.data.keyvalue.redis.connection.RedisConnection;
import org.springframework.data.keyvalue.redis.connection.SortParameters;
import org.springframework.data.keyvalue.redis.connection.Subscription;

/**
 * {@code RedisConnection} implementation on top of <a href="http://github.com/e-mzungu/rjc">rjc</a> library.
 * 
 * @author Costin Leau
 */
public class RjcConnection implements RedisConnection {

	private final int dbIndex;
	private final Session session;
	private boolean isClosed = false;

	public RjcConnection(org.idevlab.rjc.ds.RedisConnection connection, int dbIndex) {
		session = new SessionFactoryImpl(new SingleDataSource(connection)).create();
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
		return new UncategorizedKeyvalueStoreException("Unknown rjc exception", ex);
	}

	@Override
	public void close() throws DataAccessException {
		isClosed = true;
		try {
			session.close();
		} catch (Exception ex) {
			throw convertRjcAccessException(ex);
		}
	}


	@Override
	public boolean isClosed() {
		return isClosed;
	}

	@Override
	public Session getNativeConnection() {
		return session;
	}

	@Override
	public List<byte[]> closePipeline() {
		throw new UnsupportedOperationException();
	}


	@Override
	public boolean isPipelined() {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean isQueueing() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void openPipeline() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Long del(byte[]... keys) {
		throw new UnsupportedOperationException();
	}

	@Override
	public byte[] echo(byte[] message) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Boolean exists(byte[] key) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Boolean expire(byte[] key, long seconds) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Boolean expireAt(byte[] key, long unixTime) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Set<byte[]> keys(byte[] pattern) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Boolean persist(byte[] key) {
		throw new UnsupportedOperationException();
	}

	@Override
	public String ping() {
		throw new UnsupportedOperationException();
	}

	@Override
	public byte[] randomKey() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void rename(byte[] oldName, byte[] newName) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Boolean renameNX(byte[] oldName, byte[] newName) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void select(int dbIndex) {
		throw new UnsupportedOperationException();
	}

	@Override
	public List<byte[]> sort(byte[] key, SortParameters params) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Long sort(byte[] key, SortParameters params, byte[] storeKey) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Long ttl(byte[] key) {
		throw new UnsupportedOperationException();
	}

	@Override
	public DataType type(byte[] key) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void discard() {
		throw new UnsupportedOperationException();
	}

	@Override
	public List<Object> exec() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void multi() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void unwatch() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void watch(byte[]... keys) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Long append(byte[] key, byte[] value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Long decr(byte[] key) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Long decrBy(byte[] key, long value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public byte[] get(byte[] key) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Boolean getBit(byte[] key, long offset) {
		throw new UnsupportedOperationException();
	}

	@Override
	public byte[] getRange(byte[] key, int begin, int end) {
		throw new UnsupportedOperationException();
	}

	@Override
	public byte[] getSet(byte[] key, byte[] value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Long incr(byte[] key) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Long incrBy(byte[] key, long value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public List<byte[]> mGet(byte[]... keys) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void mSet(Map<byte[], byte[]> tuple) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void mSetNX(Map<byte[], byte[]> tuple) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void set(byte[] key, byte[] value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setBit(byte[] key, long offset, boolean value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setEx(byte[] key, long seconds, byte[] value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Boolean setNX(byte[] key, byte[] value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setRange(byte[] key, int begin, int end) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Long strLen(byte[] key) {
		throw new UnsupportedOperationException();
	}

	@Override
	public List<byte[]> bLPop(int timeout, byte[]... keys) {
		throw new UnsupportedOperationException();
	}

	@Override
	public List<byte[]> bRPop(int timeout, byte[]... keys) {
		throw new UnsupportedOperationException();
	}

	@Override
	public byte[] bRPopLPush(int timeout, byte[] srcKey, byte[] dstKey) {
		throw new UnsupportedOperationException();
	}

	@Override
	public byte[] lIndex(byte[] key, long index) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Long lInsert(byte[] key, Position where, byte[] pivot, byte[] value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Long lLen(byte[] key) {
		throw new UnsupportedOperationException();
	}

	@Override
	public byte[] lPop(byte[] key) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Long lPush(byte[] key, byte[] value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Long lPushX(byte[] key, byte[] value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public List<byte[]> lRange(byte[] key, long begin, long end) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Long lRem(byte[] key, long count, byte[] value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void lSet(byte[] key, long index, byte[] value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void lTrim(byte[] key, long begin, long end) {
		throw new UnsupportedOperationException();
	}

	@Override
	public byte[] rPop(byte[] key) {
		throw new UnsupportedOperationException();
	}

	@Override
	public byte[] rPopLPush(byte[] srcKey, byte[] dstKey) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Long rPush(byte[] key, byte[] value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Long rPushX(byte[] key, byte[] value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Boolean sAdd(byte[] key, byte[] value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Long sCard(byte[] key) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Set<byte[]> sDiff(byte[]... keys) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void sDiffStore(byte[] destKey, byte[]... keys) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Set<byte[]> sInter(byte[]... keys) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void sInterStore(byte[] destKey, byte[]... keys) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Boolean sIsMember(byte[] key, byte[] value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Set<byte[]> sMembers(byte[] key) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Boolean sMove(byte[] srcKey, byte[] destKey, byte[] value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public byte[] sPop(byte[] key) {
		throw new UnsupportedOperationException();
	}

	@Override
	public byte[] sRandMember(byte[] key) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Boolean sRem(byte[] key, byte[] value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Set<byte[]> sUnion(byte[]... keys) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void sUnionStore(byte[] destKey, byte[]... keys) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Boolean zAdd(byte[] key, double score, byte[] value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Long zCard(byte[] key) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Long zCount(byte[] key, double min, double max) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Double zIncrBy(byte[] key, double increment, byte[] value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Long zInterStore(byte[] destKey, byte[]... sets) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Long zInterStore(byte[] destKey, Aggregate aggregate, int[] weights, byte[]... sets) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Set<byte[]> zRange(byte[] key, long begin, long end) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Set<byte[]> zRangeByScore(byte[] key, double min, double max) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Set<byte[]> zRangeByScore(byte[] key, double min, double max, long offset, long count) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Set<Tuple> zRangeByScoreWithScore(byte[] key, double min, double max) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Set<Tuple> zRangeByScoreWithScore(byte[] key, double min, double max, long offset, long count) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Set<Tuple> zRangeWithScore(byte[] key, long begin, long end) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Long zRank(byte[] key, byte[] value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Boolean zRem(byte[] key, byte[] value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Long zRemRange(byte[] key, long begin, long end) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Long zRemRangeByScore(byte[] key, double min, double max) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Set<byte[]> zRevRange(byte[] key, long begin, long end) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Set<Tuple> zRevRangeWithScore(byte[] key, long begin, long end) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Long zRevRank(byte[] key, byte[] value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Double zScore(byte[] key, byte[] value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Long zUnionStore(byte[] destKey, byte[]... sets) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Long zUnionStore(byte[] destKey, Aggregate aggregate, int[] weights, byte[]... sets) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Boolean hDel(byte[] key, byte[] field) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Boolean hExists(byte[] key, byte[] field) {
		throw new UnsupportedOperationException();
	}

	@Override
	public byte[] hGet(byte[] key, byte[] field) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Map<byte[], byte[]> hGetAll(byte[] key) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Long hIncrBy(byte[] key, byte[] field, long delta) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Set<byte[]> hKeys(byte[] key) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Long hLen(byte[] key) {
		throw new UnsupportedOperationException();
	}

	@Override
	public List<byte[]> hMGet(byte[] key, byte[]... fields) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void hMSet(byte[] key, Map<byte[], byte[]> hashes) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Boolean hSet(byte[] key, byte[] field, byte[] value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Boolean hSetNX(byte[] key, byte[] field, byte[] value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public List<byte[]> hVals(byte[] key) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void bgSave() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void bgWriteAof() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Long dbSize() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void flushAll() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void flushDb() {
		throw new UnsupportedOperationException();
	}

	@Override
	public List<String> getConfig(String pattern) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Properties info() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Long lastSave() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void resetConfigStats() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void save() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setConfig(String param, String value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void shutdown() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Subscription getSubscription() {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean isSubscribed() {
		throw new UnsupportedOperationException();
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
