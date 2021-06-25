/*
 * Copyright 2014-2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.redis.connection;

import static org.mockito.Mockito.*;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.dao.DataAccessException;
import org.springframework.data.geo.Circle;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.geo.Metric;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisNode.RedisNodeBuilder;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.data.redis.core.types.RedisClientInfo;
import org.springframework.util.ObjectUtils;

/**
 * @author Christoph Strobl
 * @author Thomas Darimont
 * @author David Liu
 * @author Ninad Divadkar
 * @author Mark Paluch
 */
class RedisConnectionUnitTests {

	private final RedisNode SENTINEL_1 = new RedisNodeBuilder().listeningAt("localhost", 23679).build();
	private AbstractDelegatingRedisConnectionStub connection;
	private RedisSentinelConnection sentinelConnectionMock;

	@BeforeEach
	void setUp() {
		sentinelConnectionMock = mock(RedisSentinelConnection.class);

		connection = new AbstractDelegatingRedisConnectionStub(mock(AbstractRedisConnection.class, CALLS_REAL_METHODS));
		connection.setSentinelConfiguration(new RedisSentinelConfiguration().master("mymaster").sentinel(SENTINEL_1));
		connection.setSentinelConnection(sentinelConnectionMock);
	}

	@Test // DATAREDIS-330
	void shouldCloseSentinelConnectionAlongWithRedisConnection() throws IOException {

		when(sentinelConnectionMock.isOpen()).thenReturn(true).thenReturn(false);

		connection.setActiveNode(SENTINEL_1);
		connection.getSentinelConnection();
		connection.close();

		verify(sentinelConnectionMock, times(1)).close();
	}

	@Test // DATAREDIS-330
	void shouldNotTryToCloseSentinelConnectionsWhenAlreadyClosed() throws IOException {

		when(sentinelConnectionMock.isOpen()).thenReturn(true);
		when(sentinelConnectionMock.isOpen()).thenReturn(false);

		connection.setActiveNode(SENTINEL_1);
		connection.getSentinelConnection();
		connection.close();

		verify(sentinelConnectionMock, never()).close();
	}

	static class AbstractDelegatingRedisConnectionStub extends AbstractRedisConnection {

		RedisConnection delegate;
		RedisNode activeNode;
		RedisSentinelConnection sentinelConnection;

		AbstractDelegatingRedisConnectionStub(RedisConnection delegate) {
			this.delegate = delegate;
		}

		@Override
		protected boolean isActive(RedisNode node) {
			return ObjectUtils.nullSafeEquals(activeNode, node);
		}

		void setActiveNode(RedisNode activeNode) {
			this.activeNode = activeNode;
		}

		void setSentinelConnection(RedisSentinelConnection sentinelConnection) {
			this.sentinelConnection = sentinelConnection;
		}

		public boolean isSubscribed() {
			return delegate.isSubscribed();
		}

		public void scriptFlush() {
			delegate.scriptFlush();
		}

		public void select(int dbIndex) {
			delegate.select(dbIndex);
		}

		public void multi() {
			delegate.multi();
		}

		public Long rPush(byte[] key, byte[]... values) {
			return delegate.rPush(key, values);
		}

		public byte[] get(byte[] key) {
			return delegate.get(key);
		}

		public void scriptKill() {
			delegate.scriptKill();
		}

		public Long sAdd(byte[] key, byte[]... values) {
			return delegate.sAdd(key, values);
		}

		public Boolean exists(byte[] key) {
			return delegate.exists(key);
		}

		public Subscription getSubscription() {
			return delegate.getSubscription();
		}

		public byte[] echo(byte[] message) {
			return delegate.echo(message);
		}

		public Boolean hSet(byte[] key, byte[] field, byte[] value) {
			return delegate.hSet(key, field, value);
		}

		public void bgWriteAof() {
			delegate.bgWriteAof();
		}

		public Object execute(String command, byte[]... args) {
			return delegate.execute(command, args);
		}

		public String scriptLoad(byte[] script) {
			return delegate.scriptLoad(script);
		}

		public byte[] getSet(byte[] key, byte[] value) {
			return delegate.getSet(key, value);
		}

		public List<Object> exec() {
			return delegate.exec();
		}

		public Long lPush(byte[] key, byte[]... value) {
			return delegate.lPush(key, value);
		}

		public Long del(byte[]... keys) {
			return delegate.del(keys);
		}

		public Boolean copy(byte[] sourceKey, byte[] targetKey, boolean replace) {
			return delegate.copy(sourceKey, targetKey, replace);
		}

		public void close() throws DataAccessException {
			super.close();
		}

		public String ping() {
			return delegate.ping();
		}

		public Long sRem(byte[] key, byte[]... values) {
			return delegate.sRem(key, values);
		}

		public Boolean zAdd(byte[] key, double score, byte[] value) {
			return delegate.zAdd(key, score, value);
		}

		public Long publish(byte[] channel, byte[] message) {
			return delegate.publish(channel, message);
		}

		public Boolean hSetNX(byte[] key, byte[] field, byte[] value) {
			return delegate.hSetNX(key, field, value);
		}

		public void bgReWriteAof() {
			delegate.bgReWriteAof();
		}

		public List<byte[]> mGet(byte[]... keys) {
			return delegate.mGet(keys);
		}

		public boolean isClosed() {
			return delegate.isClosed();
		}

		public Long rPushX(byte[] key, byte[] value) {
			return delegate.rPushX(key, value);
		}

		public DataType type(byte[] key) {
			return delegate.type(key);
		}

		public List<Boolean> scriptExists(String... scriptShas) {
			return delegate.scriptExists(scriptShas);
		}

		public byte[] sPop(byte[] key) {
			return delegate.sPop(key);
		}

		public void bgSave() {
			delegate.bgSave();
		}

		public Boolean set(byte[] key, byte[] value) {
			return delegate.set(key, value);
		}

		public void discard() {
			delegate.discard();
		}

		public Object getNativeConnection() {
			return delegate.getNativeConnection();
		}

		public Long zAdd(byte[] key, Set<Tuple> tuples, ZAddArgs args) {
			return delegate.zAdd(key, tuples, args);
		}

		public void subscribe(MessageListener listener, byte[]... channels) {
			delegate.subscribe(listener, channels);
		}

		public Long geoAdd(byte[] key, Point point, byte[] member) {
			return delegate.geoAdd(key, point, member);
		}

		@Override
		public Long geoAdd(byte[] key, GeoLocation<byte[]> location) {
			return delegate.geoAdd(key, location);
		}

		@Override
		public Long geoAdd(byte[] key, Map<byte[], Point> memberCoordinateMap) {
			return delegate.geoAdd(key, memberCoordinateMap);
		}

		@Override
		public Long geoAdd(byte[] key, Iterable<GeoLocation<byte[]>> locations) {
			return delegate.geoAdd(key, locations);
		}

		@Override
		public Distance geoDist(byte[] key, byte[] member1, byte[] member2) {
			return delegate.geoDist(key, member1, member2);
		}

		@Override
		public Distance geoDist(byte[] key, byte[] member1, byte[] member2, Metric unit) {
			return delegate.geoDist(key, member1, member2, unit);
		}

		@Override
		public List<String> geoHash(byte[] key, byte[]... members) {
			return delegate.geoHash(key, members);
		}

		@Override
		public List<Point> geoPos(byte[] key, byte[]... members) {
			return delegate.geoPos(key, members);
		}

		@Override
		public GeoResults<GeoLocation<byte[]>> geoRadius(byte[] key, Circle within) {
			return delegate.geoRadius(key, null);
		}

		@Override
		public GeoResults<GeoLocation<byte[]>> geoRadius(byte[] key, Circle within, GeoRadiusCommandArgs param) {
			return delegate.geoRadius(key, null, param);
		}

		@Override
		public GeoResults<GeoLocation<byte[]>> geoRadiusByMember(byte[] key, byte[] member, double radius) {
			return delegate.geoRadiusByMember(key, member, radius);
		}

		@Override
		public GeoResults<GeoLocation<byte[]>> geoRadiusByMember(byte[] key, byte[] member, Distance radius) {
			return delegate.geoRadiusByMember(key, member, radius);
		}

		@Override
		public GeoResults<GeoLocation<byte[]>> geoRadiusByMember(byte[] key, byte[] member, Distance radius,
				GeoRadiusCommandArgs param) {
			return delegate.geoRadiusByMember(key, member, radius, param);
		}

		@Override
		public Long geoRemove(byte[] key, byte[]... values) {
			return zRem(key, values);
		}

		public Set<byte[]> keys(byte[] pattern) {
			return delegate.keys(pattern);
		}

		public byte[] hGet(byte[] key, byte[] field) {
			return delegate.hGet(key, field);
		}

		public Long lPushX(byte[] key, byte[] value) {
			return delegate.lPushX(key, value);
		}

		public Long lastSave() {
			return delegate.lastSave();
		}

		public void watch(byte[]... keys) {
			delegate.watch(keys);
		}

		public Boolean sMove(byte[] srcKey, byte[] destKey, byte[] value) {
			return delegate.sMove(srcKey, destKey, value);
		}

		public Boolean setNX(byte[] key, byte[] value) {
			return delegate.setNX(key, value);
		}

		public <T> T eval(byte[] script, ReturnType returnType, int numKeys, byte[]... keysAndArgs) {
			return delegate.eval(script, returnType, numKeys, keysAndArgs);
		}

		public boolean isQueueing() {
			return delegate.isQueueing();
		}

		public Cursor<byte[]> scan(ScanOptions options) {
			return delegate.scan(options);
		}

		public void save() {
			delegate.save();
		}

		public List<byte[]> hMGet(byte[] key, byte[]... fields) {
			return delegate.hMGet(key, fields);
		}

		public Long zRem(byte[] key, byte[]... values) {
			return delegate.zRem(key, values);
		}

		public Long lLen(byte[] key) {
			return delegate.lLen(key);
		}

		public void unwatch() {
			delegate.unwatch();
		}

		public Long dbSize() {
			return delegate.dbSize();
		}

		public Boolean setEx(byte[] key, long seconds, byte[] value) {
			return delegate.setEx(key, seconds, value);
		}

		public Long sCard(byte[] key) {
			return delegate.sCard(key);
		}

		public byte[] randomKey() {
			return delegate.randomKey();
		}

		public <T> T evalSha(String scriptSha, ReturnType returnType, int numKeys, byte[]... keysAndArgs) {
			return delegate.evalSha(scriptSha, returnType, numKeys, keysAndArgs);
		}

		public List<byte[]> lRange(byte[] key, long begin, long end) {
			return delegate.lRange(key, begin, end);
		}

		public void hMSet(byte[] key, Map<byte[], byte[]> hashes) {
			delegate.hMSet(key, hashes);
		}

		public Double zIncrBy(byte[] key, double increment, byte[] value) {
			return delegate.zIncrBy(key, increment, value);
		}

		public void flushDb() {
			delegate.flushDb();
		}

		public Boolean sIsMember(byte[] key, byte[] value) {
			return delegate.sIsMember(key, value);
		}

		public void pSubscribe(MessageListener listener, byte[]... patterns) {
			delegate.pSubscribe(listener, patterns);
		}

		public void rename(byte[] sourceKey, byte[] targetKey) {
			delegate.rename(sourceKey, targetKey);
		}

		public boolean isPipelined() {
			return delegate.isPipelined();
		}

		public Boolean pSetEx(byte[] key, long milliseconds, byte[] value) {
			return delegate.pSetEx(key, milliseconds, value);
		}

		public void flushAll() {
			delegate.flushAll();
		}

		public void lTrim(byte[] key, long begin, long end) {
			delegate.lTrim(key, begin, end);
		}

		public Long hIncrBy(byte[] key, byte[] field, long delta) {
			return delegate.hIncrBy(key, field, delta);
		}

		public Set<byte[]> sInter(byte[]... keys) {
			return delegate.sInter(keys);
		}

		public Boolean renameNX(byte[] sourceKey, byte[] targetKey) {
			return delegate.renameNX(sourceKey, targetKey);
		}

		public Long zRank(byte[] key, byte[] value) {
			return delegate.zRank(key, value);
		}

		public Properties info() {
			return delegate.info();
		}

		public void openPipeline() {
			delegate.openPipeline();
		}

		public Boolean mSet(Map<byte[], byte[]> tuple) {
			return delegate.mSet(tuple);
		}

		public byte[] lIndex(byte[] key, long index) {
			return delegate.lIndex(key, index);
		}

		public Long sInterStore(byte[] destKey, byte[]... keys) {
			return delegate.sInterStore(destKey, keys);
		}

		public Double hIncrBy(byte[] key, byte[] field, double delta) {
			return delegate.hIncrBy(key, field, delta);
		}

		public Long zRevRank(byte[] key, byte[] value) {
			return delegate.zRevRank(key, value);
		}

		public Boolean expire(byte[] key, long seconds) {
			return delegate.expire(key, seconds);
		}

		public Properties info(String section) {
			return delegate.info(section);
		}

		public Boolean mSetNX(Map<byte[], byte[]> tuple) {
			return delegate.mSetNX(tuple);
		}

		public Long lInsert(byte[] key, Position where, byte[] pivot, byte[] value) {
			return delegate.lInsert(key, where, pivot, value);
		}

		public Set<byte[]> sUnion(byte[]... keys) {
			return delegate.sUnion(keys);
		}

		public void shutdown() {
			delegate.shutdown();
		}

		public Boolean pExpire(byte[] key, long millis) {
			return delegate.pExpire(key, millis);
		}

		public Boolean hExists(byte[] key, byte[] field) {
			return delegate.hExists(key, field);
		}

		public Set<byte[]> zRange(byte[] key, long begin, long end) {
			return delegate.zRange(key, begin, end);
		}

		public void shutdown(ShutdownOption option) {
			delegate.shutdown(option);
		}

		public Long sUnionStore(byte[] destKey, byte[]... keys) {
			return delegate.sUnionStore(destKey, keys);
		}

		public Long incr(byte[] key) {
			return delegate.incr(key);
		}

		public Long hDel(byte[] key, byte[]... fields) {
			return delegate.hDel(key, fields);
		}

		public Boolean expireAt(byte[] key, long unixTime) {
			return delegate.expireAt(key, unixTime);
		}

		public Properties getConfig(String pattern) {
			return delegate.getConfig(pattern);
		}

		public void lSet(byte[] key, long index, byte[] value) {
			delegate.lSet(key, index, value);
		}

		public Set<Tuple> zRangeWithScores(byte[] key, long begin, long end) {
			return delegate.zRangeWithScores(key, begin, end);
		}

		public Long incrBy(byte[] key, long value) {
			return delegate.incrBy(key, value);
		}

		public Set<byte[]> sDiff(byte[]... keys) {
			return delegate.sDiff(keys);
		}

		public Long hLen(byte[] key) {
			return delegate.hLen(key);
		}

		public List<Object> closePipeline() throws RedisPipelineException {
			return delegate.closePipeline();
		}

		public void setConfig(String param, String value) {
			delegate.setConfig(param, value);
		}

		public Boolean pExpireAt(byte[] key, long unixTimeInMillis) {
			return delegate.pExpireAt(key, unixTimeInMillis);
		}

		public Long lRem(byte[] key, long count, byte[] value) {
			return delegate.lRem(key, count, value);
		}

		public Double incrBy(byte[] key, double value) {
			return delegate.incrBy(key, value);
		}

		public Long sDiffStore(byte[] destKey, byte[]... keys) {
			return delegate.sDiffStore(destKey, keys);
		}

		public Set<byte[]> zRangeByScore(byte[] key, double min, double max) {
			return delegate.zRangeByScore(key, min, max);
		}

		public Set<byte[]> hKeys(byte[] key) {
			return delegate.hKeys(key);
		}

		public void resetConfigStats() {
			delegate.resetConfigStats();
		}

		public Long decr(byte[] key) {
			return delegate.decr(key);
		}

		public List<byte[]> hVals(byte[] key) {
			return delegate.hVals(key);
		}

		public Boolean persist(byte[] key) {
			return delegate.persist(key);
		}

		public byte[] lPop(byte[] key) {
			return delegate.lPop(key);
		}

		public Set<byte[]> sMembers(byte[] key) {
			return delegate.sMembers(key);
		}

		public Long decrBy(byte[] key, long value) {
			return delegate.decrBy(key, value);
		}

		public Set<Tuple> zRangeByScoreWithScores(byte[] key, double min, double max) {
			return delegate.zRangeByScoreWithScores(key, min, max);
		}

		public Long time() {
			return delegate.time();
		}

		public Map<byte[], byte[]> hGetAll(byte[] key) {
			return delegate.hGetAll(key);
		}

		public byte[] hRandField(byte[] key) {
			return delegate.hRandField(key);
		}

		public Entry<byte[], byte[]> hRandFieldWithValues(byte[] key) {
			return delegate.hRandFieldWithValues(key);
		}

		public List<byte[]> hRandField(byte[] key, long count) {
			return delegate.hRandField(key, count);
		}

		public List<Entry<byte[], byte[]>> hRandFieldWithValues(byte[] key, long count) {
			return delegate.hRandFieldWithValues(key, count);
		}

		public Boolean move(byte[] key, int dbIndex) {
			return delegate.move(key, dbIndex);
		}

		public byte[] sRandMember(byte[] key) {
			return delegate.sRandMember(key);
		}

		public byte[] rPop(byte[] key) {
			return delegate.rPop(key);
		}

		public void killClient(String host, int port) {
			delegate.killClient(host, port);
		}

		public Long append(byte[] key, byte[] value) {
			return delegate.append(key, value);
		}

		public Cursor<Entry<byte[], byte[]>> hScan(byte[] key, ScanOptions options) {
			return delegate.hScan(key, options);
		}

		public List<byte[]> sRandMember(byte[] key, long count) {
			return delegate.sRandMember(key, count);
		}

		public Long ttl(byte[] key) {
			return delegate.ttl(key);
		}

		public Long ttl(byte[] key, TimeUnit timeUnit) {
			return delegate.pTtl(key, timeUnit);
		}

		public List<byte[]> bLPop(int timeout, byte[]... keys) {
			return delegate.bLPop(timeout, keys);
		}

		public Set<byte[]> zRangeByScore(byte[] key, double min, double max, long offset, long count) {
			return delegate.zRangeByScore(key, min, max, offset, count);
		}

		public byte[] getRange(byte[] key, long start, long end) {
			return delegate.getRange(key, start, end);
		}

		public void setClientName(byte[] name) {
			delegate.setClientName(name);
		}

		public Long pTtl(byte[] key) {
			return delegate.pTtl(key);
		}

		public Long pTtl(byte[] key, TimeUnit timeUnit) {
			return delegate.pTtl(key, timeUnit);
		}

		public Cursor<byte[]> sScan(byte[] key, ScanOptions options) {
			return delegate.sScan(key, options);
		}

		public String getClientName() {
			return delegate.getClientName();
		}

		public List<byte[]> sort(byte[] key, SortParameters params) {
			return delegate.sort(key, params);
		}

		public List<byte[]> bRPop(int timeout, byte[]... keys) {
			return delegate.bRPop(timeout, keys);
		}

		public void setRange(byte[] key, byte[] value, long offset) {
			delegate.setRange(key, value, offset);
		}

		public Set<Tuple> zRangeByScoreWithScores(byte[] key, double min, double max, long offset, long count) {
			return delegate.zRangeByScoreWithScores(key, min, max, offset, count);
		}

		public List<RedisClientInfo> getClientList() {
			return delegate.getClientList();
		}

		public Long sort(byte[] key, SortParameters params, byte[] storeKey) {
			return delegate.sort(key, params, storeKey);
		}

		public Boolean getBit(byte[] key, long offset) {
			return delegate.getBit(key, offset);
		}

		public void slaveOf(String host, int port) {
			delegate.slaveOf(host, port);
		}

		public byte[] rPopLPush(byte[] srcKey, byte[] dstKey) {
			return delegate.rPopLPush(srcKey, dstKey);
		}

		public Set<byte[]> zRevRange(byte[] key, long begin, long end) {
			return delegate.zRevRange(key, begin, end);
		}

		public byte[] dump(byte[] key) {
			return delegate.dump(key);
		}

		public Boolean setBit(byte[] key, long offset, boolean value) {
			return delegate.setBit(key, offset, value);
		}

		public void slaveOfNoOne() {
			delegate.slaveOfNoOne();
		}

		public void restore(byte[] key, long ttlInMillis, byte[] serializedValue, boolean replace) {
			delegate.restore(key, ttlInMillis, serializedValue, replace);
		}

		public byte[] bRPopLPush(int timeout, byte[] srcKey, byte[] dstKey) {
			return delegate.bRPopLPush(timeout, srcKey, dstKey);
		}

		public Set<Tuple> zRevRangeWithScores(byte[] key, long begin, long end) {
			return delegate.zRevRangeWithScores(key, begin, end);
		}

		public Long bitCount(byte[] key) {
			return delegate.bitCount(key);
		}

		public Set<byte[]> zRevRangeByScore(byte[] key, double min, double max) {
			return delegate.zRevRangeByScore(key, min, max);
		}

		public Long bitCount(byte[] key, long begin, long end) {
			return delegate.bitCount(key, begin, end);
		}

		public Set<Tuple> zRevRangeByScoreWithScores(byte[] key, double min, double max) {
			return delegate.zRevRangeByScoreWithScores(key, min, max);
		}

		public Long bitOp(BitOperation op, byte[] destination, byte[]... keys) {
			return delegate.bitOp(op, destination, keys);
		}

		public Long strLen(byte[] key) {
			return delegate.strLen(key);
		}

		public Set<byte[]> zRevRangeByScore(byte[] key, double min, double max, long offset, long count) {
			return delegate.zRevRangeByScore(key, min, max, offset, count);
		}

		public Set<Tuple> zRevRangeByScoreWithScores(byte[] key, double min, double max, long offset, long count) {
			return delegate.zRevRangeByScoreWithScores(key, min, max, offset, count);
		}

		public Long zCount(byte[] key, double min, double max) {
			return delegate.zCount(key, min, max);
		}

		public Long zCard(byte[] key) {
			return delegate.zCard(key);
		}

		public Double zScore(byte[] key, byte[] value) {
			return delegate.zScore(key, value);
		}

		public Long zRemRange(byte[] key, long begin, long end) {
			return delegate.zRemRange(key, begin, end);
		}

		public Long zRemRangeByScore(byte[] key, double min, double max) {
			return delegate.zRemRangeByScore(key, min, max);
		}

		public Long zUnionStore(byte[] destKey, byte[]... sets) {
			return delegate.zUnionStore(destKey, sets);
		}

		public Long zUnionStore(byte[] destKey, Aggregate aggregate, Weights weights, byte[]... sets) {
			return delegate.zUnionStore(destKey, aggregate, weights, sets);
		}

		public Long zInterStore(byte[] destKey, byte[]... sets) {
			return delegate.zInterStore(destKey, sets);
		}

		public Long zInterStore(byte[] destKey, Aggregate aggregate, int[] weights, byte[]... sets) {
			return delegate.zInterStore(destKey, aggregate, weights, sets);
		}

		public Long zInterStore(byte[] destKey, Aggregate aggregate, Weights weights, byte[]... sets) {
			return delegate.zInterStore(destKey, aggregate, weights, sets);
		}

		public Cursor<Tuple> zScan(byte[] key, ScanOptions options) {
			return delegate.zScan(key, options);
		}

		public RedisConnection getDelegate() {
			return delegate;
		}

		@Override
		protected RedisSentinelConnection getSentinelConnection(RedisNode sentinel) {
			if (ObjectUtils.nullSafeEquals(this.activeNode, sentinel)) {
				return this.sentinelConnection;
			}
			return null;
		}

		@Override
		public <T> T evalSha(byte[] scriptSha, ReturnType returnType, int numKeys, byte[]... keysAndArgs) {
			return delegate.evalSha(scriptSha, returnType, numKeys, keysAndArgs);
		}

		@Override
		public Set<byte[]> zRangeByScore(byte[] key, String min, String max) {
			return delegate.zRangeByScore(key, min, max);
		}

		@Override
		public Set<byte[]> zRangeByScore(byte[] key, String min, String max, long offset, long count) {
			return delegate.zRangeByScore(key, min, max, offset, count);
		}

		@Override
		public Long pfAdd(byte[] key, byte[]... values) {
			return delegate.pfAdd(key, values);
		}

		@Override
		public Long pfCount(byte[]... keys) {
			return delegate.pfCount(keys);
		}

		@Override
		public void pfMerge(byte[] destinationKey, byte[]... sourceKeys) {
			delegate.pfMerge(destinationKey, sourceKeys);
		}

		@Override
		public Set<byte[]> zRangeByLex(byte[] key) {
			return delegate.zRangeByLex(key);
		}

		@Override
		public Set<byte[]> zRangeByLex(byte[] key, Range range) {
			return delegate.zRangeByLex(key, range);
		}

		@Override
		public Set<byte[]> zRangeByLex(byte[] key, Range range, Limit limit) {
			return delegate.zRangeByLex(key, range, limit);
		}

		@Override
		public Set<Tuple> zRangeByScoreWithScores(byte[] key, Range range, Limit limit) {
			return delegate.zRangeByScoreWithScores(key, range, limit);
		}

		@Override
		public Set<byte[]> zRevRangeByScore(byte[] key, Range range) {
			return delegate.zRevRangeByScore(key, range);
		}

		@Override
		public Set<byte[]> zRevRangeByScore(byte[] key, Range range, Limit limit) {
			return delegate.zRevRangeByScore(key, range, limit);
		}

		@Override
		public Set<Tuple> zRevRangeByScoreWithScores(byte[] key, Range range, Limit limit) {
			return delegate.zRevRangeByScoreWithScores(key, range, limit);
		}

		@Override
		public Long zCount(byte[] key, Range range) {
			return delegate.zCount(key, range);
		}

		@Override
		public Long zRemRangeByScore(byte[] key, Range range) {
			return delegate.zRemRangeByScore(key, range);
		}

		@Override
		public Long zRemRangeByLex(byte[] key, Range range) {
			return delegate.zRemRangeByLex(key, range);
		}

		@Override
		public Set<byte[]> zRangeByScore(byte[] key, Range range) {
			return delegate.zRangeByScore(key, range);
		}

		@Override
		public Set<byte[]> zRangeByScore(byte[] key, Range range, Limit limit) {
			return delegate.zRangeByScore(key, range, limit);
		}

		@Override
		public Set<Tuple> zRangeByScoreWithScores(byte[] key, Range range) {
			return delegate.zRangeByScoreWithScores(key, range);
		}

		@Override
		public Set<Tuple> zRevRangeByScoreWithScores(byte[] key, Range range) {
			return delegate.zRevRangeByScoreWithScores(key, range);
		}

		@Override
		public void migrate(byte[] key, RedisNode target, int dbIndex, MigrateOption option) {
			delegate.migrate(key, target, dbIndex, option);
		}

		@Override
		public void migrate(byte[] key, RedisNode target, int dbIndex, MigrateOption option, long timeout) {
			delegate.migrate(key, target, dbIndex, option, timeout);
		}

		@Override
		public void rewriteConfig() {
			delegate.rewriteConfig();
		}

		@Override
		public Boolean set(byte[] key, byte[] value, Expiration expiration, SetOption options) {
			return delegate.set(key, value, expiration, options);
		}

		@Override
		public List<Long> bitField(byte[] key, BitFieldSubCommands subCommands) {
			return delegate.bitField(key, subCommands);
		}
	}
}
