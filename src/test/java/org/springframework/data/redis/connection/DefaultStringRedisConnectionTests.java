/*
 * Copyright 2013-2017 the original author or authors.
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
package org.springframework.data.redis.connection;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.util.*;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.data.geo.Circle;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands.DistanceUnit;
import org.springframework.data.redis.connection.RedisGeoCommands.GeoLocation;
import org.springframework.data.redis.connection.RedisGeoCommands.GeoRadiusCommandArgs;
import org.springframework.data.redis.connection.RedisListCommands.Position;
import org.springframework.data.redis.connection.RedisServerCommands.ShutdownOption;
import org.springframework.data.redis.connection.RedisStringCommands.BitOperation;
import org.springframework.data.redis.connection.RedisZSetCommands.Aggregate;
import org.springframework.data.redis.connection.RedisZSetCommands.Tuple;
import org.springframework.data.redis.connection.StringRedisConnection.StringTuple;
import org.springframework.data.redis.connection.convert.Converters;
import org.springframework.data.redis.serializer.StringRedisSerializer;

/**
 * Unit test of {@link DefaultStringRedisConnection}
 *
 * @author Jennifer Hickey
 * @author Christoph Strobl
 * @author Ninad Divadkar
 * @author Mark Paluch
 */
public class DefaultStringRedisConnectionTests {

	protected List<Object> actual = new ArrayList<>();

	@Mock protected RedisConnection nativeConnection;

	protected DefaultStringRedisConnection connection;

	protected StringRedisSerializer serializer = StringRedisSerializer.UTF_8;

	protected String foo = "foo";

	protected String bar = "bar";

	protected String bar2 = "bar2";

	protected byte[] fooBytes = serializer.serialize(foo);

	protected byte[] barBytes = serializer.serialize(bar);

	protected byte[] bar2Bytes = serializer.serialize(bar2);

	protected List<byte[]> bytesList = Collections.singletonList(barBytes);

	protected List<String> stringList = Collections.singletonList(bar);

	protected Set<byte[]> bytesSet = new LinkedHashSet<>(Collections.singletonList(barBytes));

	protected Set<String> stringSet = new LinkedHashSet<>(Collections.singletonList(bar));

	protected Map<byte[], byte[]> bytesMap = new HashMap<>();

	protected Map<String, String> stringMap = new HashMap<>();

	protected Set<Tuple> tupleSet = new HashSet<>(Collections.singletonList(new DefaultTuple(barBytes, 3d)));

	protected Set<StringTuple> stringTupleSet = new HashSet<>(
			Collections.singletonList(new DefaultStringTuple(new DefaultTuple(barBytes, 3d), bar)));

	protected Point point = new Point(213.00, 324.343);
	protected List<Point> points = new ArrayList<>();

	protected List<GeoResult<GeoLocation<byte[]>>> geoResultList = Collections
			.singletonList(new GeoResult<>(new GeoLocation<>(barBytes, null), new Distance(0D)));
	protected GeoResults<GeoLocation<byte[]>> geoResults = new GeoResults<>(geoResultList);

	@Before
	public void setUp() {
		MockitoAnnotations.initMocks(this);
		this.connection = new DefaultStringRedisConnection(nativeConnection);
		bytesMap.put(fooBytes, barBytes);
		stringMap.put(foo, bar);
		points.add(point);
	}

	@Test
	public void testAppend() {
		doReturn(1l).when(nativeConnection).append(fooBytes, barBytes);
		actual.add(connection.append(foo, bar));
		verifyResults(Arrays.asList(new Object[] { 1l }));
	}

	@Test
	public void testAppendBytes() {
		doReturn(1l).when(nativeConnection).append(fooBytes, barBytes);
		actual.add(connection.append(fooBytes, barBytes));
		verifyResults(Arrays.asList(new Object[] { 1l }));
	}

	@Test
	public void testBlPopBytes() {
		doReturn(bytesList).when(nativeConnection).bLPop(1, fooBytes);
		actual.add(connection.bLPop(1, fooBytes));
		verifyResults(Arrays.asList(new Object[] { bytesList }));
	}

	@Test
	public void testBlPop() {
		doReturn(bytesList).when(nativeConnection).bLPop(1, fooBytes);
		actual.add(connection.bLPop(1, foo));
		verifyResults(Arrays.asList(new Object[] { stringList }));
	}

	@Test
	public void testBrPopBytes() {
		doReturn(bytesList).when(nativeConnection).bRPop(1, fooBytes);
		actual.add(connection.bRPop(1, fooBytes));
		verifyResults(Arrays.asList(new Object[] { bytesList }));
	}

	@Test
	public void testBrPop() {
		doReturn(bytesList).when(nativeConnection).bRPop(1, fooBytes);
		actual.add(connection.bRPop(1, foo));
		verifyResults(Arrays.asList(new Object[] { stringList }));
	}

	@Test
	public void testBrPopLPushBytes() {
		doReturn(barBytes).when(nativeConnection).bRPopLPush(1, fooBytes, barBytes);
		actual.add(connection.bRPopLPush(1, fooBytes, barBytes));
		verifyResults(Arrays.asList(new Object[] { barBytes }));
	}

	@Test
	public void testBrPopLPush() {
		doReturn(barBytes).when(nativeConnection).bRPopLPush(1, fooBytes, barBytes);
		actual.add(connection.bRPopLPush(1, foo, bar));
		verifyResults(Arrays.asList(new Object[] { bar }));
	}

	@Test
	public void testDbSize() {
		doReturn(3l).when(nativeConnection).dbSize();
		actual.add(connection.dbSize());
		verifyResults(Arrays.asList(new Object[] { 3l }));
	}

	@Test
	public void testDecrBytes() {
		doReturn(3l).when(nativeConnection).decr(fooBytes);
		actual.add(connection.decr(fooBytes));
		verifyResults(Arrays.asList(new Object[] { 3l }));
	}

	@Test
	public void testDecr() {
		doReturn(3l).when(nativeConnection).decr(fooBytes);
		actual.add(connection.decr(foo));
		verifyResults(Arrays.asList(new Object[] { 3l }));
	}

	@Test
	public void testDecrByBytes() {
		doReturn(3l).when(nativeConnection).decrBy(fooBytes, 2l);
		actual.add(connection.decrBy(fooBytes, 2l));
		verifyResults(Arrays.asList(new Object[] { 3l }));
	}

	@Test
	public void testDecrBy() {
		doReturn(3l).when(nativeConnection).decrBy(fooBytes, 2l);
		actual.add(connection.decrBy(foo, 2l));
		verifyResults(Arrays.asList(new Object[] { 3l }));
	}

	@Test
	public void testDelBytes() {
		doReturn(1l).when(nativeConnection).del(fooBytes);
		actual.add(connection.del(fooBytes));
		verifyResults(Arrays.asList(new Object[] { 1l }));
	}

	@Test
	public void testDel() {
		doReturn(1l).when(nativeConnection).del(fooBytes);
		actual.add(connection.del(foo));
		verifyResults(Arrays.asList(new Object[] { 1l }));
	}

	@Test
	public void testEchoBytes() {
		doReturn(barBytes).when(nativeConnection).echo(fooBytes);
		actual.add(connection.echo(fooBytes));
		verifyResults(Arrays.asList(new Object[] { barBytes }));
	}

	@Test
	public void testEcho() {
		doReturn(barBytes).when(nativeConnection).echo(fooBytes);
		actual.add(connection.echo(foo));
		verifyResults(Arrays.asList(new Object[] { bar }));
	}

	@Test
	public void testExistsBytes() {
		doReturn(true).when(nativeConnection).exists(fooBytes);
		actual.add(connection.exists(fooBytes));
		verifyResults(Arrays.asList(new Object[] { true }));
	}

	@Test
	public void testExists() {
		doReturn(true).when(nativeConnection).exists(fooBytes);
		actual.add(connection.exists(foo));
		verifyResults(Arrays.asList(new Object[] { true }));
	}

	@Test
	public void testExpireBytes() {
		doReturn(true).when(nativeConnection).expire(fooBytes, 1l);
		actual.add(connection.expire(fooBytes, 1l));
		verifyResults(Arrays.asList(new Object[] { true }));
	}

	@Test
	public void testExpire() {
		doReturn(true).when(nativeConnection).expire(fooBytes, 1l);
		actual.add(connection.expire(foo, 1l));
		verifyResults(Arrays.asList(new Object[] { true }));
	}

	@Test
	public void testExpireAtBytes() {
		doReturn(true).when(nativeConnection).expireAt(fooBytes, 1l);
		actual.add(connection.expireAt(fooBytes, 1l));
		verifyResults(Arrays.asList(new Object[] { true }));
	}

	@Test
	public void testExpireAt() {
		doReturn(true).when(nativeConnection).expireAt(fooBytes, 1l);
		actual.add(connection.expireAt(foo, 1l));
		verifyResults(Arrays.asList(new Object[] { true }));
	}

	@Test
	public void testGetBytes() {
		doReturn(barBytes).when(nativeConnection).get(fooBytes);
		actual.add(connection.get(fooBytes));
		verifyResults(Arrays.asList(new Object[] { barBytes }));
	}

	@Test
	public void testGet() {
		doReturn(barBytes).when(nativeConnection).get(fooBytes);
		actual.add(connection.get(foo));
		verifyResults(Arrays.asList(new Object[] { bar }));
	}

	@Test
	public void testGetBitBytes() {
		doReturn(true).when(nativeConnection).getBit(fooBytes, 1l);
		actual.add(connection.getBit(fooBytes, 1l));
		verifyResults(Arrays.asList(new Object[] { true }));
	}

	@Test
	public void testGetBit() {
		doReturn(true).when(nativeConnection).getBit(fooBytes, 1l);
		actual.add(connection.getBit(foo, 1l));
		verifyResults(Arrays.asList(new Object[] { true }));
	}

	@Test // DATAREDIS-661
	public void testGetConfig() {

		Properties results = new Properties();
		results.put("foo", "bar");

		doReturn(results).when(nativeConnection).getConfig("foo");
		actual.add(connection.getConfig("foo"));
		verifyResults(Arrays.asList(new Object[] { results }));
	}

	@Test
	public void testGetNativeConnection() {
		doReturn("foo").when(nativeConnection).getNativeConnection();
		actual.add(connection.getNativeConnection());
		verifyResults(Arrays.asList(new Object[] { "foo" }));
	}

	@Test
	public void testGetRangeBytes() {
		doReturn(barBytes).when(nativeConnection).getRange(fooBytes, 1l, 2l);
		actual.add(connection.getRange(fooBytes, 1l, 2l));
		verifyResults(Arrays.asList(new Object[] { barBytes }));
	}

	@Test
	public void testGetRange() {
		doReturn(barBytes).when(nativeConnection).getRange(fooBytes, 1l, 2l);
		actual.add(connection.getRange(foo, 1l, 2l));
		verifyResults(Arrays.asList(new Object[] { bar }));
	}

	@Test
	public void testGetSetBytes() {
		doReturn(barBytes).when(nativeConnection).getSet(fooBytes, barBytes);
		actual.add(connection.getSet(fooBytes, barBytes));
		verifyResults(Arrays.asList(new Object[] { barBytes }));
	}

	@Test
	public void testGetSet() {
		doReturn(barBytes).when(nativeConnection).getSet(fooBytes, barBytes);
		actual.add(connection.getSet(foo, bar));
		verifyResults(Arrays.asList(new Object[] { bar }));
	}

	@Test
	public void testHDelBytes() {
		doReturn(1l).when(nativeConnection).hDel(fooBytes, barBytes);
		actual.add(connection.hDel(fooBytes, barBytes));
		verifyResults(Arrays.asList(new Object[] { 1l }));
	}

	@Test
	public void testHDel() {
		doReturn(1l).when(nativeConnection).hDel(fooBytes, barBytes);
		actual.add(connection.hDel(foo, bar));
		verifyResults(Arrays.asList(new Object[] { 1l }));
	}

	@Test
	public void testHExistsBytes() {
		doReturn(true).when(nativeConnection).hExists(fooBytes, barBytes);
		actual.add(connection.hExists(fooBytes, barBytes));
		verifyResults(Arrays.asList(new Object[] { true }));
	}

	@Test
	public void testHExists() {
		doReturn(true).when(nativeConnection).hExists(fooBytes, barBytes);
		actual.add(connection.hExists(foo, bar));
		verifyResults(Arrays.asList(new Object[] { true }));
	}

	@Test
	public void testHGetBytes() {
		doReturn(barBytes).when(nativeConnection).hGet(fooBytes, barBytes);
		actual.add(connection.hGet(fooBytes, barBytes));
		verifyResults(Arrays.asList(new Object[] { barBytes }));
	}

	@Test
	public void testHGet() {
		doReturn(barBytes).when(nativeConnection).hGet(fooBytes, barBytes);
		actual.add(connection.hGet(foo, bar));
		verifyResults(Arrays.asList(new Object[] { bar }));
	}

	@Test
	public void testHGetAllBytes() {
		doReturn(bytesMap).when(nativeConnection).hGetAll(barBytes);
		actual.add(connection.hGetAll(barBytes));
		verifyResults(Arrays.asList(new Object[] { bytesMap }));
	}

	@Test
	public void testHGetAll() {
		doReturn(bytesMap).when(nativeConnection).hGetAll(barBytes);
		actual.add(connection.hGetAll(bar));
		verifyResults(Arrays.asList(new Object[] { stringMap }));
	}

	@Test
	public void testHIncrByBytes() {
		doReturn(3l).when(nativeConnection).hIncrBy(fooBytes, barBytes, 1l);
		actual.add(connection.hIncrBy(fooBytes, barBytes, 1l));
		verifyResults(Arrays.asList(new Object[] { 3l }));
	}

	@Test
	public void testHIncrBy() {
		doReturn(3l).when(nativeConnection).hIncrBy(fooBytes, barBytes, 1l);
		actual.add(connection.hIncrBy(foo, bar, 1l));
		verifyResults(Arrays.asList(new Object[] { 3l }));
	}

	@Test
	public void testHIncrByDoubleBytes() {
		doReturn(3d).when(nativeConnection).hIncrBy(fooBytes, barBytes, 1d);
		actual.add(connection.hIncrBy(fooBytes, barBytes, 1d));
		verifyResults(Arrays.asList(new Object[] { 3d }));
	}

	@Test
	public void testHIncrByDouble() {
		doReturn(3d).when(nativeConnection).hIncrBy(fooBytes, barBytes, 1d);
		actual.add(connection.hIncrBy(foo, bar, 1d));
		verifyResults(Arrays.asList(new Object[] { 3d }));
	}

	@Test
	public void testHKeysBytes() {
		doReturn(bytesSet).when(nativeConnection).hKeys(fooBytes);
		actual.add(connection.hKeys(fooBytes));
		verifyResults(Arrays.asList(new Object[] { bytesSet }));
	}

	@Test
	public void testHKeys() {
		doReturn(bytesSet).when(nativeConnection).hKeys(fooBytes);
		actual.add(connection.hKeys(foo));
		verifyResults(Arrays.asList(new Object[] { stringSet }));
	}

	@Test
	public void testHLenBytes() {
		doReturn(3l).when(nativeConnection).hLen(fooBytes);
		actual.add(connection.hLen(fooBytes));
		verifyResults(Arrays.asList(new Object[] { 3l }));
	}

	@Test
	public void testHLen() {
		doReturn(3l).when(nativeConnection).hLen(fooBytes);
		actual.add(connection.hLen(foo));
		verifyResults(Arrays.asList(new Object[] { 3l }));
	}

	@Test
	public void testHMGetBytes() {
		doReturn(bytesList).when(nativeConnection).hMGet(fooBytes, barBytes);
		actual.add(connection.hMGet(fooBytes, barBytes));
		verifyResults(Arrays.asList(new Object[] { bytesList }));
	}

	@Test
	public void testHMGet() {
		doReturn(bytesList).when(nativeConnection).hMGet(fooBytes, barBytes);
		actual.add(connection.hMGet(foo, bar));
		verifyResults(Arrays.asList(new Object[] { stringList }));
	}

	@Test
	public void testHSetBytes() {
		doReturn(true).when(nativeConnection).hSet(fooBytes, barBytes, fooBytes);
		actual.add(connection.hSet(fooBytes, barBytes, fooBytes));
		verifyResults(Arrays.asList(new Object[] { true }));
	}

	@Test
	public void testHSet() {
		doReturn(true).when(nativeConnection).hSet(fooBytes, barBytes, fooBytes);
		actual.add(connection.hSet(foo, bar, foo));
		verifyResults(Arrays.asList(new Object[] { true }));
	}

	@Test
	public void testHSetNXBytes() {
		doReturn(true).when(nativeConnection).hSetNX(fooBytes, barBytes, fooBytes);
		actual.add(connection.hSetNX(fooBytes, barBytes, fooBytes));
		verifyResults(Arrays.asList(new Object[] { true }));
	}

	@Test
	public void testHSetNX() {
		doReturn(true).when(nativeConnection).hSetNX(fooBytes, barBytes, fooBytes);
		actual.add(connection.hSetNX(foo, bar, foo));
		verifyResults(Arrays.asList(new Object[] { true }));
	}

	@Test
	public void testHValsBytes() {
		doReturn(bytesList).when(nativeConnection).hVals(fooBytes);
		actual.add(connection.hVals(fooBytes));
		verifyResults(Arrays.asList(new Object[] { bytesList }));
	}

	@Test
	public void testHVals() {
		doReturn(bytesList).when(nativeConnection).hVals(fooBytes);
		actual.add(connection.hVals(foo));
		verifyResults(Arrays.asList(new Object[] { stringList }));
	}

	@Test
	public void testIncrBytes() {
		doReturn(2l).when(nativeConnection).incr(fooBytes);
		actual.add(connection.incr(fooBytes));
		verifyResults(Arrays.asList(new Object[] { 2l }));
	}

	@Test
	public void testIncr() {
		doReturn(2l).when(nativeConnection).incr(fooBytes);
		actual.add(connection.incr(foo));
		verifyResults(Arrays.asList(new Object[] { 2l }));
	}

	@Test
	public void testIncrByBytes() {
		doReturn(2l).when(nativeConnection).incrBy(fooBytes, 1l);
		actual.add(connection.incrBy(fooBytes, 1l));
		verifyResults(Arrays.asList(new Object[] { 2l }));
	}

	@Test
	public void testIncrBy() {
		doReturn(2l).when(nativeConnection).incrBy(fooBytes, 1l);
		actual.add(connection.incrBy(foo, 1l));
		verifyResults(Arrays.asList(new Object[] { 2l }));
	}

	@Test
	public void testIncrByDoubleBytes() {
		doReturn(2d).when(nativeConnection).incrBy(fooBytes, 1d);
		actual.add(connection.incrBy(fooBytes, 1d));
		verifyResults(Arrays.asList(new Object[] { 2d }));
	}

	@Test
	public void testIncrByDouble() {
		doReturn(2d).when(nativeConnection).incrBy(fooBytes, 1d);
		actual.add(connection.incrBy(foo, 1d));
		verifyResults(Arrays.asList(new Object[] { 2d }));
	}

	@Test
	public void testInfo() {
		Properties props = new Properties();
		props.put("foo", "bar");
		doReturn(props).when(nativeConnection).info();
		actual.add(connection.info());
		verifyResults(Arrays.asList(new Object[] { props }));
	}

	@Test
	public void testInfoBySection() {
		Properties props = new Properties();
		props.put("foo", "bar");
		doReturn(props).when(nativeConnection).info("foo");
		actual.add(connection.info("foo"));
		verifyResults(Arrays.asList(new Object[] { props }));
	}

	@Test
	public void testKeysBytes() {
		doReturn(bytesSet).when(nativeConnection).keys(fooBytes);
		actual.add(connection.keys(fooBytes));
		verifyResults(Arrays.asList(new Object[] { bytesSet }));
	}

	@Test
	public void testKeys() {
		doReturn(bytesSet).when(nativeConnection).keys(fooBytes);
		actual.add(connection.keys(foo));
		verifyResults(Arrays.asList(new Object[] { stringSet }));
	}

	@Test
	public void testLastSave() {
		doReturn(6l).when(nativeConnection).lastSave();
		actual.add(connection.lastSave());
		verifyResults(Arrays.asList(new Object[] { 6l }));
	}

	@Test
	public void testLIndexBytes() {
		doReturn(barBytes).when(nativeConnection).lIndex(fooBytes, 8l);
		actual.add(connection.lIndex(fooBytes, 8l));
		verifyResults(Arrays.asList(new Object[] { barBytes }));
	}

	@Test
	public void testLIndex() {
		doReturn(barBytes).when(nativeConnection).lIndex(fooBytes, 8l);
		actual.add(connection.lIndex(foo, 8l));
		verifyResults(Arrays.asList(new Object[] { bar }));
	}

	@Test
	public void testLInsertBytes() {
		doReturn(8l).when(nativeConnection).lInsert(fooBytes, Position.AFTER, fooBytes, barBytes);
		actual.add(connection.lInsert(fooBytes, Position.AFTER, fooBytes, barBytes));
		verifyResults(Arrays.asList(new Object[] { 8l }));
	}

	@Test
	public void testLInsert() {
		doReturn(8l).when(nativeConnection).lInsert(fooBytes, Position.AFTER, fooBytes, barBytes);
		actual.add(connection.lInsert(foo, Position.AFTER, foo, bar));
		verifyResults(Arrays.asList(new Object[] { 8l }));
	}

	@Test
	public void testLLenBytes() {
		doReturn(8l).when(nativeConnection).lLen(fooBytes);
		actual.add(connection.lLen(fooBytes));
		verifyResults(Arrays.asList(new Object[] { 8l }));
	}

	@Test
	public void testLLen() {
		doReturn(8l).when(nativeConnection).lLen(fooBytes);
		actual.add(connection.lLen(foo));
		verifyResults(Arrays.asList(new Object[] { 8l }));
	}

	@Test
	public void testLPopBytes() {
		doReturn(barBytes).when(nativeConnection).lPop(fooBytes);
		actual.add(connection.lPop(fooBytes));
		verifyResults(Arrays.asList(new Object[] { barBytes }));
	}

	@Test
	public void testLPop() {
		doReturn(barBytes).when(nativeConnection).lPop(fooBytes);
		actual.add(connection.lPop(foo));
		verifyResults(Arrays.asList(new Object[] { bar }));
	}

	@Test
	public void testLPushBytes() {
		doReturn(8l).when(nativeConnection).lPush(fooBytes, barBytes);
		actual.add(connection.lPush(fooBytes, barBytes));
		verifyResults(Arrays.asList(new Object[] { 8l }));
	}

	@Test
	public void testLPush() {
		doReturn(8l).when(nativeConnection).lPush(fooBytes, barBytes);
		actual.add(connection.lPush(foo, bar));
		verifyResults(Arrays.asList(new Object[] { 8l }));
	}

	@Test
	public void testLPushXBytes() {
		doReturn(8l).when(nativeConnection).lPushX(fooBytes, barBytes);
		actual.add(connection.lPushX(fooBytes, barBytes));
		verifyResults(Arrays.asList(new Object[] { 8l }));
	}

	@Test
	public void testLPushX() {
		doReturn(8l).when(nativeConnection).lPushX(fooBytes, barBytes);
		actual.add(connection.lPushX(foo, bar));
		verifyResults(Arrays.asList(new Object[] { 8l }));
	}

	@Test
	public void testLRangeBytes() {
		doReturn(bytesList).when(nativeConnection).lRange(fooBytes, 1l, 2l);
		actual.add(connection.lRange(fooBytes, 1l, 2l));
		verifyResults(Arrays.asList(new Object[] { bytesList }));
	}

	@Test
	public void testLRange() {
		doReturn(bytesList).when(nativeConnection).lRange(fooBytes, 1l, 2l);
		actual.add(connection.lRange(foo, 1l, 2l));
		verifyResults(Arrays.asList(new Object[] { stringList }));
	}

	@Test
	public void testLRemBytes() {
		doReturn(8l).when(nativeConnection).lRem(fooBytes, 1l, barBytes);
		actual.add(connection.lRem(fooBytes, 1l, barBytes));
		verifyResults(Arrays.asList(new Object[] { 8l }));
	}

	@Test
	public void testLRem() {
		doReturn(8l).when(nativeConnection).lRem(fooBytes, 1l, barBytes);
		actual.add(connection.lRem(foo, 1l, bar));
		verifyResults(Arrays.asList(new Object[] { 8l }));
	}

	@Test
	public void testMGetBytes() {
		doReturn(bytesList).when(nativeConnection).mGet(fooBytes);
		actual.add(connection.mGet(fooBytes));
		verifyResults(Arrays.asList(new Object[] { bytesList }));
	}

	@Test
	public void testMGet() {
		doReturn(bytesList).when(nativeConnection).mGet(fooBytes);
		actual.add(connection.mGet(foo));
		verifyResults(Arrays.asList(new Object[] { stringList }));
	}

	@Test
	public void testMSetNXBytes() {
		doReturn(true).when(nativeConnection).mSetNX(bytesMap);
		actual.add(connection.mSetNX(bytesMap));
		verifyResults(Arrays.asList(new Object[] { true }));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testMSetNXString() {
		doReturn(true).when(nativeConnection).mSetNX(anyMap());
		actual.add(connection.mSetNXString(stringMap));
		verifyResults(Arrays.asList(new Object[] { true }));
	}

	@Test
	public void testPersistBytes() {
		doReturn(true).when(nativeConnection).persist(fooBytes);
		actual.add(connection.persist(fooBytes));
		verifyResults(Arrays.asList(new Object[] { true }));
	}

	@Test
	public void testPersist() {
		doReturn(true).when(nativeConnection).persist(fooBytes);
		actual.add(connection.persist(foo));
		verifyResults(Arrays.asList(new Object[] { true }));
	}

	@Test
	public void testMoveBytes() {
		doReturn(true).when(nativeConnection).move(fooBytes, 1);
		actual.add(connection.move(fooBytes, 1));
		verifyResults(Arrays.asList(new Object[] { true }));
	}

	@Test
	public void testMove() {
		doReturn(true).when(nativeConnection).move(fooBytes, 1);
		actual.add(connection.move(foo, 1));
		verifyResults(Arrays.asList(new Object[] { true }));
	}

	@Test
	public void testPing() {
		doReturn("pong").when(nativeConnection).ping();
		actual.add(connection.ping());
		verifyResults(Arrays.asList(new Object[] { "pong" }));
	}

	@Test
	public void testPublishBytes() {
		doReturn(2l).when(nativeConnection).publish(fooBytes, barBytes);
		actual.add(connection.publish(fooBytes, barBytes));
		verifyResults(Arrays.asList(new Object[] { 2l }));
	}

	@Test
	public void testPublish() {
		doReturn(2l).when(nativeConnection).publish(fooBytes, barBytes);
		actual.add(connection.publish(foo, bar));
		verifyResults(Arrays.asList(new Object[] { 2l }));
	}

	@Test
	public void testRandomKey() {
		doReturn(fooBytes).when(nativeConnection).randomKey();
		actual.add(connection.randomKey());
		verifyResults(Arrays.asList(new Object[] { fooBytes }));
	}

	@Test
	public void testRenameNXBytes() {
		doReturn(true).when(nativeConnection).renameNX(fooBytes, barBytes);
		actual.add(connection.renameNX(fooBytes, barBytes));
		verifyResults(Arrays.asList(new Object[] { true }));
	}

	@Test
	public void testRenameNX() {
		doReturn(true).when(nativeConnection).renameNX(fooBytes, barBytes);
		actual.add(connection.renameNX(foo, bar));
		verifyResults(Arrays.asList(new Object[] { true }));
	}

	@Test
	public void testRPopBytes() {
		doReturn(barBytes).when(nativeConnection).rPop(fooBytes);
		actual.add(connection.rPop(fooBytes));
		verifyResults(Arrays.asList(new Object[] { barBytes }));
	}

	@Test
	public void testRPop() {
		doReturn(barBytes).when(nativeConnection).rPop(fooBytes);
		actual.add(connection.rPop(foo));
		verifyResults(Arrays.asList(new Object[] { bar }));
	}

	@Test
	public void testRPopLPushBytes() {
		doReturn(barBytes).when(nativeConnection).rPopLPush(fooBytes, barBytes);
		actual.add(connection.rPopLPush(fooBytes, barBytes));
		verifyResults(Arrays.asList(new Object[] { barBytes }));
	}

	@Test
	public void testRPopLPush() {
		doReturn(barBytes).when(nativeConnection).rPopLPush(fooBytes, barBytes);
		actual.add(connection.rPopLPush(foo, bar));
		verifyResults(Arrays.asList(new Object[] { bar }));
	}

	@Test
	public void testRPushBytes() {
		doReturn(4l).when(nativeConnection).rPush(fooBytes, barBytes);
		actual.add(connection.rPush(fooBytes, barBytes));
		verifyResults(Arrays.asList(new Object[] { 4l }));
	}

	@Test
	public void testRPush() {
		doReturn(4l).when(nativeConnection).rPush(fooBytes, barBytes);
		actual.add(connection.rPush(foo, bar));
		verifyResults(Arrays.asList(new Object[] { 4l }));
	}

	@Test
	public void testRPushXBytes() {
		doReturn(4l).when(nativeConnection).rPushX(fooBytes, barBytes);
		actual.add(connection.rPushX(fooBytes, barBytes));
		verifyResults(Arrays.asList(new Object[] { 4l }));
	}

	@Test
	public void testRPushX() {
		doReturn(4l).when(nativeConnection).rPushX(fooBytes, barBytes);
		actual.add(connection.rPushX(foo, bar));
		verifyResults(Arrays.asList(new Object[] { 4l }));
	}

	@Test
	public void testSAddBytes() {
		doReturn(1l).when(nativeConnection).sAdd(fooBytes, barBytes);
		actual.add(connection.sAdd(fooBytes, barBytes));
		verifyResults(Arrays.asList(new Object[] { 1l }));
	}

	@Test
	public void testSAdd() {
		doReturn(1l).when(nativeConnection).sAdd(fooBytes, barBytes);
		actual.add(connection.sAdd(foo, bar));
		verifyResults(Arrays.asList(new Object[] { 1l }));
	}

	@Test
	public void testSCardBytes() {
		doReturn(4l).when(nativeConnection).sCard(fooBytes);
		actual.add(connection.sCard(fooBytes));
		verifyResults(Arrays.asList(new Object[] { 4l }));
	}

	@Test
	public void testSCard() {
		doReturn(4l).when(nativeConnection).sCard(fooBytes);
		actual.add(connection.sCard(foo));
		verifyResults(Arrays.asList(new Object[] { 4l }));
	}

	@Test
	public void testSDiffBytes() {
		doReturn(bytesSet).when(nativeConnection).sDiff(fooBytes);
		actual.add(connection.sDiff(fooBytes));
		verifyResults(Arrays.asList(new Object[] { bytesSet }));
	}

	@Test
	public void testSDiff() {
		doReturn(bytesSet).when(nativeConnection).sDiff(fooBytes);
		actual.add(connection.sDiff(foo));
		verifyResults(Arrays.asList(new Object[] { stringSet }));
	}

	@Test
	public void testSDiffStoreBytes() {
		doReturn(3l).when(nativeConnection).sDiffStore(fooBytes, barBytes);
		actual.add(connection.sDiffStore(fooBytes, barBytes));
		verifyResults(Arrays.asList(new Object[] { 3l }));
	}

	@Test
	public void testSDiffStore() {
		doReturn(3l).when(nativeConnection).sDiffStore(fooBytes, barBytes);
		actual.add(connection.sDiffStore(foo, bar));
		verifyResults(Arrays.asList(new Object[] { 3l }));
	}

	@Test
	public void testSetNXBytes() {
		doReturn(true).when(nativeConnection).setNX(fooBytes, barBytes);
		actual.add(connection.setNX(fooBytes, barBytes));
		verifyResults(Arrays.asList(new Object[] { true }));
	}

	@Test
	public void testSetNX() {
		doReturn(true).when(nativeConnection).setNX(fooBytes, barBytes);
		actual.add(connection.setNX(foo, bar));
		verifyResults(Arrays.asList(new Object[] { true }));
	}

	@Test // DATAREDIS-271
	public void testPSetExShouldDelegateCallToNativeConnection() {

		connection.pSetEx(fooBytes, 10L, barBytes);
		verify(nativeConnection, times(1)).pSetEx(eq(fooBytes), eq(10L), eq(barBytes));
	}

	@Test
	public void testSInterBytes() {
		doReturn(bytesSet).when(nativeConnection).sInter(fooBytes, barBytes);
		actual.add(connection.sInter(fooBytes, barBytes));
		verifyResults(Arrays.asList(new Object[] { bytesSet }));
	}

	@Test
	public void testSInter() {
		doReturn(bytesSet).when(nativeConnection).sInter(fooBytes, barBytes);
		actual.add(connection.sInter(foo, bar));
		verifyResults(Arrays.asList(new Object[] { stringSet }));
	}

	@Test
	public void testSInterStoreBytes() {
		doReturn(3l).when(nativeConnection).sInterStore(fooBytes, barBytes);
		actual.add(connection.sInterStore(fooBytes, barBytes));
		verifyResults(Arrays.asList(new Object[] { 3l }));
	}

	@Test
	public void testSInterStore() {
		doReturn(3l).when(nativeConnection).sInterStore(fooBytes, barBytes);
		actual.add(connection.sInterStore(foo, bar));
		verifyResults(Arrays.asList(new Object[] { 3l }));
	}

	@Test
	public void testSIsMemberBytes() {
		doReturn(true).when(nativeConnection).sIsMember(fooBytes, barBytes);
		actual.add(connection.sIsMember(fooBytes, barBytes));
		verifyResults(Arrays.asList(new Object[] { true }));
	}

	@Test
	public void testSIsMember() {
		doReturn(true).when(nativeConnection).sIsMember(fooBytes, barBytes);
		actual.add(connection.sIsMember(foo, bar));
		verifyResults(Arrays.asList(new Object[] { true }));
	}

	@Test
	public void testSMembersBytes() {
		doReturn(bytesSet).when(nativeConnection).sMembers(fooBytes);
		actual.add(connection.sMembers(fooBytes));
		verifyResults(Arrays.asList(new Object[] { bytesSet }));
	}

	@Test
	public void testSMembers() {
		doReturn(bytesSet).when(nativeConnection).sMembers(fooBytes);
		actual.add(connection.sMembers(foo));
		verifyResults(Arrays.asList(new Object[] { stringSet }));
	}

	@Test
	public void testSMoveBytes() {
		doReturn(true).when(nativeConnection).sMove(fooBytes, barBytes, fooBytes);
		actual.add(connection.sMove(fooBytes, barBytes, fooBytes));
		verifyResults(Arrays.asList(new Object[] { true }));
	}

	@Test
	public void testSMove() {
		doReturn(true).when(nativeConnection).sMove(fooBytes, barBytes, fooBytes);
		actual.add(connection.sMove(foo, bar, foo));
		verifyResults(Arrays.asList(new Object[] { true }));
	}

	@Test
	public void testSortStoreBytes() {
		doReturn(3l).when(nativeConnection).sort(fooBytes, null, barBytes);
		actual.add(connection.sort(fooBytes, null, barBytes));
		verifyResults(Arrays.asList(new Object[] { 3l }));
	}

	@Test
	public void testSortStore() {
		doReturn(3l).when(nativeConnection).sort(fooBytes, null, barBytes);
		actual.add(connection.sort(foo, null, bar));
		verifyResults(Arrays.asList(new Object[] { 3l }));
	}

	@Test
	public void testSortBytes() {
		doReturn(bytesList).when(nativeConnection).sort(fooBytes, null);
		actual.add(connection.sort(fooBytes, null));
		verifyResults(Arrays.asList(new Object[] { bytesList }));
	}

	@Test
	public void testSort() {
		doReturn(bytesList).when(nativeConnection).sort(fooBytes, null);
		actual.add(connection.sort(foo, null));
		verifyResults(Arrays.asList(new Object[] { stringList }));
	}

	@Test
	public void testSPopBytes() {
		doReturn(barBytes).when(nativeConnection).sPop(fooBytes);
		actual.add(connection.sPop(fooBytes));
		verifyResults(Arrays.asList(new Object[] { barBytes }));
	}

	@Test
	public void testSPop() {
		doReturn(barBytes).when(nativeConnection).sPop(fooBytes);
		actual.add(connection.sPop(foo));
		verifyResults(Arrays.asList(new Object[] { bar }));
	}

	@Test
	public void testSRandMemberBytes() {
		doReturn(barBytes).when(nativeConnection).sRandMember(fooBytes);
		actual.add(connection.sRandMember(fooBytes));
		verifyResults(Arrays.asList(new Object[] { barBytes }));
	}

	@Test
	public void testSRandMember() {
		doReturn(barBytes).when(nativeConnection).sRandMember(fooBytes);
		actual.add(connection.sRandMember(foo));
		verifyResults(Arrays.asList(new Object[] { bar }));
	}

	@Test
	public void testSRandMemberCountBytes() {
		doReturn(bytesList).when(nativeConnection).sRandMember(fooBytes, 5l);
		actual.add(connection.sRandMember(fooBytes, 5l));
		verifyResults(Arrays.asList(new Object[] { bytesList }));
	}

	@Test
	public void testSRandMemberCount() {
		doReturn(bytesList).when(nativeConnection).sRandMember(fooBytes, 5l);
		actual.add(connection.sRandMember(foo, 5l));
		verifyResults(Arrays.asList(new Object[] { stringList }));
	}

	@Test
	public void testSRemBytes() {
		doReturn(1l).when(nativeConnection).sRem(fooBytes, barBytes);
		actual.add(connection.sRem(fooBytes, barBytes));
		verifyResults(Arrays.asList(new Object[] { 1l }));
	}

	@Test
	public void testSRem() {
		doReturn(1l).when(nativeConnection).sRem(fooBytes, barBytes);
		actual.add(connection.sRem(foo, bar));
		verifyResults(Arrays.asList(new Object[] { 1l }));
	}

	@Test
	public void testStrLenBytes() {
		doReturn(5l).when(nativeConnection).strLen(fooBytes);
		actual.add(connection.strLen(fooBytes));
		verifyResults(Arrays.asList(new Object[] { 5l }));
	}

	@Test
	public void testStrLen() {
		doReturn(5l).when(nativeConnection).strLen(fooBytes);
		actual.add(connection.strLen(foo));
		verifyResults(Arrays.asList(new Object[] { 5l }));
	}

	@Test
	public void testBitCountBytes() {
		doReturn(5l).when(nativeConnection).bitCount(fooBytes);
		actual.add(connection.bitCount(fooBytes));
		verifyResults(Arrays.asList(new Object[] { 5l }));
	}

	@Test
	public void testBitCount() {
		doReturn(5l).when(nativeConnection).bitCount(fooBytes);
		actual.add(connection.bitCount(foo));
		verifyResults(Arrays.asList(new Object[] { 5l }));
	}

	@Test
	public void testBitCountRangeBytes() {
		doReturn(5l).when(nativeConnection).bitCount(fooBytes, 2l, 5l);
		actual.add(connection.bitCount(fooBytes, 2l, 5l));
		verifyResults(Arrays.asList(new Object[] { 5l }));
	}

	@Test
	public void testBitCountRange() {
		doReturn(5l).when(nativeConnection).bitCount(fooBytes, 2l, 5l);
		actual.add(connection.bitCount(foo, 2l, 5l));
		verifyResults(Arrays.asList(new Object[] { 5l }));
	}

	@Test
	public void testBitOpBytes() {
		doReturn(5l).when(nativeConnection).bitOp(BitOperation.AND, fooBytes, barBytes);
		actual.add(connection.bitOp(BitOperation.AND, fooBytes, barBytes));
		verifyResults(Arrays.asList(new Object[] { 5l }));
	}

	@Test
	public void testBitOp() {
		doReturn(5l).when(nativeConnection).bitOp(BitOperation.AND, fooBytes, barBytes);
		actual.add(connection.bitOp(BitOperation.AND, foo, bar));
		verifyResults(Arrays.asList(new Object[] { 5l }));
	}

	@Test
	public void testSUnionBytes() {
		doReturn(bytesSet).when(nativeConnection).sUnion(fooBytes, barBytes);
		actual.add(connection.sUnion(fooBytes, barBytes));
		verifyResults(Arrays.asList(new Object[] { bytesSet }));
	}

	@Test
	public void testSUnion() {
		doReturn(bytesSet).when(nativeConnection).sUnion(fooBytes, barBytes);
		actual.add(connection.sUnion(foo, bar));
		verifyResults(Arrays.asList(new Object[] { stringSet }));
	}

	@Test
	public void testSUnionStoreBytes() {
		doReturn(5l).when(nativeConnection).sUnionStore(fooBytes, barBytes);
		actual.add(connection.sUnionStore(fooBytes, barBytes));
		verifyResults(Arrays.asList(new Object[] { 5l }));
	}

	@Test
	public void testSUnionStore() {
		doReturn(5l).when(nativeConnection).sUnionStore(fooBytes, barBytes);
		actual.add(connection.sUnionStore(foo, bar));
		verifyResults(Arrays.asList(new Object[] { 5l }));
	}

	@Test
	public void testTtlBytes() {
		doReturn(5l).when(nativeConnection).ttl(fooBytes);
		actual.add(connection.ttl(fooBytes));
		verifyResults(Arrays.asList(new Object[] { 5l }));
	}

	@Test
	public void testTtl() {
		doReturn(5l).when(nativeConnection).ttl(fooBytes);
		actual.add(connection.ttl(foo));
		verifyResults(Arrays.asList(new Object[] { 5l }));
	}

	@Test // DATAREDIS-526
	public void testTtlWithTimeUnit() {

		doReturn(5L).when(nativeConnection).ttl(fooBytes, TimeUnit.SECONDS);

		actual.add(connection.ttl(foo, TimeUnit.SECONDS));
		verifyResults(Arrays.asList(new Object[] { 5L }));
	}

	@Test
	public void testTypeBytes() {
		doReturn(DataType.HASH).when(nativeConnection).type(fooBytes);
		actual.add(connection.type(fooBytes));
		verifyResults(Arrays.asList(new Object[] { DataType.HASH }));
	}

	@Test
	public void testType() {
		doReturn(DataType.HASH).when(nativeConnection).type(fooBytes);
		actual.add(connection.type(foo));
		verifyResults(Arrays.asList(new Object[] { DataType.HASH }));
	}

	@Test
	public void testZAddBytes() {
		doReturn(true).when(nativeConnection).zAdd(fooBytes, 3d, barBytes);
		actual.add(connection.zAdd(fooBytes, 3d, barBytes));
		verifyResults(Arrays.asList(new Object[] { true }));
	}

	@Test
	public void testZAdd() {
		doReturn(true).when(nativeConnection).zAdd(fooBytes, 3d, barBytes);
		actual.add(connection.zAdd(foo, 3d, bar));
		verifyResults(Arrays.asList(new Object[] { true }));
	}

	@Test
	public void testZAddMultipleBytes() {
		Set<Tuple> tuples = new HashSet<>();
		tuples.add(new DefaultTuple(barBytes, 3.0));
		doReturn(1l).when(nativeConnection).zAdd(fooBytes, tuples);
		actual.add(connection.zAdd(fooBytes, tuples));
		verifyResults(Arrays.asList(new Object[] { 1l }));
	}

	@Test
	public void testZAddMultiple() {
		Set<Tuple> tuples = new HashSet<>();
		tuples.add(new DefaultTuple(barBytes, 3.0));
		Set<StringTuple> strTuples = new HashSet<>();
		strTuples.add(new DefaultStringTuple(barBytes, bar, 3.0));
		doReturn(1l).when(nativeConnection).zAdd(fooBytes, tuples);
		actual.add(connection.zAdd(foo, strTuples));
		verifyResults(Arrays.asList(new Object[] { 1l }));
	}

	@Test
	public void testZCardBytes() {
		doReturn(5l).when(nativeConnection).zCard(fooBytes);
		actual.add(connection.zCard(fooBytes));
		verifyResults(Arrays.asList(new Object[] { 5l }));
	}

	@Test
	public void testZCard() {
		doReturn(5l).when(nativeConnection).zCard(fooBytes);
		actual.add(connection.zCard(foo));
		verifyResults(Arrays.asList(new Object[] { 5l }));
	}

	@Test
	public void testZCountBytes() {
		doReturn(5l).when(nativeConnection).zCount(fooBytes, 2d, 3d);
		actual.add(connection.zCount(fooBytes, 2d, 3d));
		verifyResults(Arrays.asList(new Object[] { 5l }));
	}

	@Test
	public void testZCount() {
		doReturn(5l).when(nativeConnection).zCount(fooBytes, 2d, 3d);
		actual.add(connection.zCount(foo, 2d, 3d));
		verifyResults(Arrays.asList(new Object[] { 5l }));
	}

	@Test
	public void testZIncrByBytes() {
		doReturn(3d).when(nativeConnection).zIncrBy(fooBytes, 2d, barBytes);
		actual.add(connection.zIncrBy(fooBytes, 2d, barBytes));
		verifyResults(Arrays.asList(new Object[] { 3d }));
	}

	@Test
	public void testZIncrBy() {
		doReturn(3d).when(nativeConnection).zIncrBy(fooBytes, 2d, barBytes);
		actual.add(connection.zIncrBy(foo, 2d, bar));
		verifyResults(Arrays.asList(new Object[] { 3d }));
	}

	@Test
	public void testZInterStoreAggWeightsBytes() {
		doReturn(5l).when(nativeConnection).zInterStore(fooBytes, Aggregate.MAX, new int[0], fooBytes);
		actual.add(connection.zInterStore(fooBytes, Aggregate.MAX, new int[0], fooBytes));
		verifyResults(Arrays.asList(new Object[] { 5l }));
	}

	@Test
	public void testZInterStoreAggWeights() {
		doReturn(5l).when(nativeConnection).zInterStore(fooBytes, Aggregate.MAX, new int[0], fooBytes);
		actual.add(connection.zInterStore(foo, Aggregate.MAX, new int[0], foo));
		verifyResults(Arrays.asList(new Object[] { 5l }));
	}

	@Test
	public void testZInterStoreBytes() {
		doReturn(5l).when(nativeConnection).zInterStore(fooBytes, barBytes);
		actual.add(connection.zInterStore(fooBytes, barBytes));
		verifyResults(Arrays.asList(new Object[] { 5l }));
	}

	@Test
	public void testZInterStore() {
		doReturn(5l).when(nativeConnection).zInterStore(fooBytes, barBytes);
		actual.add(connection.zInterStore(foo, bar));
		verifyResults(Arrays.asList(new Object[] { 5l }));
	}

	@Test
	public void testZRangeBytes() {
		doReturn(bytesSet).when(nativeConnection).zRange(fooBytes, 1l, 3l);
		actual.add(connection.zRange(fooBytes, 1l, 3l));
		verifyResults(Arrays.asList(new Object[] { bytesSet }));
	}

	@Test
	public void testZRange() {
		doReturn(bytesSet).when(nativeConnection).zRange(fooBytes, 1l, 3l);
		actual.add(connection.zRange(foo, 1l, 3l));
		verifyResults(Arrays.asList(new Object[] { stringSet }));
	}

	@Test
	public void testZRangeByScoreOffsetCountBytes() {
		doReturn(bytesSet).when(nativeConnection).zRangeByScore(fooBytes, 1d, 3d, 5l, 7l);
		actual.add(connection.zRangeByScore(fooBytes, 1d, 3d, 5l, 7l));
		verifyResults(Arrays.asList(new Object[] { bytesSet }));
	}

	@Test
	public void testZRangeByScoreOffsetCount() {
		doReturn(bytesSet).when(nativeConnection).zRangeByScore(fooBytes, 1d, 3d, 5l, 7l);
		actual.add(connection.zRangeByScore(foo, 1d, 3d, 5l, 7l));
		verifyResults(Arrays.asList(new Object[] { stringSet }));
	}

	@Test
	public void testZRangeByScoreBytes() {
		doReturn(bytesSet).when(nativeConnection).zRangeByScore(fooBytes, 1d, 3d);
		actual.add(connection.zRangeByScore(fooBytes, 1d, 3d));
		verifyResults(Arrays.asList(new Object[] { bytesSet }));
	}

	@Test
	public void testZRangeByScore() {
		doReturn(bytesSet).when(nativeConnection).zRangeByScore(fooBytes, 1d, 3d);
		actual.add(connection.zRangeByScore(foo, 1d, 3d));
		verifyResults(Arrays.asList(new Object[] { stringSet }));
	}

	@Test
	public void testZRangeByScoreWithScoresOffsetCountBytes() {
		doReturn(tupleSet).when(nativeConnection).zRangeByScoreWithScores(fooBytes, 1d, 3d, 5l, 7l);
		actual.add(connection.zRangeByScoreWithScores(fooBytes, 1d, 3d, 5l, 7l));
		verifyResults(Arrays.asList(new Object[] { tupleSet }));
	}

	@Test
	public void testZRangeByScoreWithScoresOffsetCount() {
		doReturn(tupleSet).when(nativeConnection).zRangeByScoreWithScores(fooBytes, 1d, 3d, 5l, 7l);
		actual.add(connection.zRangeByScoreWithScores(foo, 1d, 3d, 5l, 7l));
		verifyResults(Arrays.asList(new Object[] { stringTupleSet }));
	}

	@Test
	public void testZRangeByScoreWithScoresBytes() {
		doReturn(tupleSet).when(nativeConnection).zRangeByScoreWithScores(fooBytes, 1d, 3d);
		actual.add(connection.zRangeByScoreWithScores(fooBytes, 1d, 3d));
		verifyResults(Arrays.asList(new Object[] { tupleSet }));
	}

	@Test
	public void testZRangeByScoreWithScores() {
		doReturn(tupleSet).when(nativeConnection).zRangeByScoreWithScores(fooBytes, 1d, 3d);
		actual.add(connection.zRangeByScoreWithScores(foo, 1d, 3d));
		verifyResults(Arrays.asList(new Object[] { stringTupleSet }));
	}

	@Test
	public void testZRangeWithScoresBytes() {
		doReturn(tupleSet).when(nativeConnection).zRangeWithScores(fooBytes, 1l, 3l);
		actual.add(connection.zRangeWithScores(fooBytes, 1l, 3l));
		verifyResults(Arrays.asList(new Object[] { tupleSet }));
	}

	@Test
	public void testZRangeWithScores() {
		doReturn(tupleSet).when(nativeConnection).zRangeWithScores(fooBytes, 1l, 3l);
		actual.add(connection.zRangeWithScores(foo, 1l, 3l));
		verifyResults(Arrays.asList(new Object[] { stringTupleSet }));
	}

	@Test
	public void testZRevRangeByScoreOffsetCountBytes() {
		doReturn(bytesSet).when(nativeConnection).zRevRangeByScore(fooBytes, 1d, 3d, 5l, 7l);
		actual.add(connection.zRevRangeByScore(fooBytes, 1d, 3d, 5l, 7l));
		verifyResults(Arrays.asList(new Object[] { bytesSet }));
	}

	@Test
	public void testZRevRangeByScoreOffsetCount() {
		doReturn(bytesSet).when(nativeConnection).zRevRangeByScore(fooBytes, 1d, 3d, 5l, 7l);
		actual.add(connection.zRevRangeByScore(foo, 1d, 3d, 5l, 7l));
		verifyResults(Arrays.asList(new Object[] { stringSet }));
	}

	@Test
	public void testZRevRangeByScoreBytes() {
		doReturn(bytesSet).when(nativeConnection).zRevRangeByScore(fooBytes, 1d, 3d);
		actual.add(connection.zRevRangeByScore(fooBytes, 1d, 3d));
		verifyResults(Arrays.asList(new Object[] { bytesSet }));
	}

	@Test
	public void testZRevRangeByScore() {
		doReturn(bytesSet).when(nativeConnection).zRevRangeByScore(fooBytes, 1d, 3d);
		actual.add(connection.zRevRangeByScore(foo, 1d, 3d));
		verifyResults(Arrays.asList(new Object[] { stringSet }));
	}

	@Test
	public void testZRevRangeByScoreWithScoresOffsetCountBytes() {
		doReturn(tupleSet).when(nativeConnection).zRevRangeByScoreWithScores(fooBytes, 1d, 3d, 5l, 7l);
		actual.add(connection.zRevRangeByScoreWithScores(fooBytes, 1d, 3d, 5l, 7l));
		verifyResults(Arrays.asList(new Object[] { tupleSet }));
	}

	@Test
	public void testZRevRangeByScoreWithScoresOffsetCount() {
		doReturn(tupleSet).when(nativeConnection).zRevRangeByScoreWithScores(fooBytes, 1d, 3d, 5l, 7l);
		actual.add(connection.zRevRangeByScoreWithScores(foo, 1d, 3d, 5l, 7l));
		verifyResults(Arrays.asList(new Object[] { stringTupleSet }));
	}

	@Test
	public void testZRevRangeByScoreWithScoresBytes() {
		doReturn(tupleSet).when(nativeConnection).zRevRangeByScoreWithScores(fooBytes, 1d, 3d);
		actual.add(connection.zRevRangeByScoreWithScores(fooBytes, 1d, 3d));
		verifyResults(Arrays.asList(new Object[] { tupleSet }));
	}

	@Test
	public void testZRevRangeByScoreWithScores() {
		doReturn(tupleSet).when(nativeConnection).zRevRangeByScoreWithScores(fooBytes, 1d, 3d);
		actual.add(connection.zRevRangeByScoreWithScores(foo, 1d, 3d));
		verifyResults(Arrays.asList(new Object[] { stringTupleSet }));
	}

	@Test
	public void testZRankBytes() {
		doReturn(5l).when(nativeConnection).zRank(fooBytes, barBytes);
		actual.add(connection.zRank(fooBytes, barBytes));
		verifyResults(Arrays.asList(new Object[] { 5l }));
	}

	@Test
	public void testZRank() {
		doReturn(5l).when(nativeConnection).zRank(fooBytes, barBytes);
		actual.add(connection.zRank(foo, bar));
		verifyResults(Arrays.asList(new Object[] { 5l }));
	}

	@Test
	public void testZRemBytes() {
		doReturn(1l).when(nativeConnection).zRem(fooBytes, barBytes);
		actual.add(connection.zRem(fooBytes, barBytes));
		verifyResults(Arrays.asList(new Object[] { 1l }));
	}

	@Test
	public void testZRem() {
		doReturn(1l).when(nativeConnection).zRem(fooBytes, barBytes);
		actual.add(connection.zRem(foo, bar));
		verifyResults(Arrays.asList(new Object[] { 1l }));
	}

	@Test
	public void testZRemRangeBytes() {
		doReturn(5l).when(nativeConnection).zRemRange(fooBytes, 2l, 5l);
		actual.add(connection.zRemRange(fooBytes, 2l, 5l));
		verifyResults(Arrays.asList(new Object[] { 5l }));
	}

	@Test
	public void testZRemRange() {
		doReturn(5l).when(nativeConnection).zRemRange(fooBytes, 2l, 5l);
		actual.add(connection.zRemRange(foo, 2l, 5l));
		verifyResults(Arrays.asList(new Object[] { 5l }));
	}

	@Test
	public void testZRemRangeByScoreBytes() {
		doReturn(5l).when(nativeConnection).zRemRangeByScore(fooBytes, 2l, 5l);
		actual.add(connection.zRemRangeByScore(fooBytes, 2l, 5l));
		verifyResults(Arrays.asList(new Object[] { 5l }));
	}

	@Test
	public void testZRemRangeByScore() {
		doReturn(5l).when(nativeConnection).zRemRangeByScore(fooBytes, 2l, 5l);
		actual.add(connection.zRemRangeByScore(foo, 2l, 5l));
		verifyResults(Arrays.asList(new Object[] { 5l }));
	}

	@Test
	public void testZRevRangeBytes() {
		doReturn(bytesSet).when(nativeConnection).zRevRange(fooBytes, 3l, 4l);
		actual.add(connection.zRevRange(fooBytes, 3l, 4l));
		verifyResults(Arrays.asList(new Object[] { bytesSet }));
	}

	@Test
	public void testZRevRange() {
		doReturn(bytesSet).when(nativeConnection).zRevRange(fooBytes, 3l, 4l);
		actual.add(connection.zRevRange(foo, 3l, 4l));
		verifyResults(Arrays.asList(new Object[] { stringSet }));
	}

	@Test
	public void testZRevRangeWithScoresBytes() {
		doReturn(tupleSet).when(nativeConnection).zRevRangeWithScores(fooBytes, 3l, 4l);
		actual.add(connection.zRevRangeWithScores(fooBytes, 3l, 4l));
		verifyResults(Arrays.asList(new Object[] { tupleSet }));
	}

	@Test
	public void testZRevRangeWithScores() {
		doReturn(tupleSet).when(nativeConnection).zRevRangeWithScores(fooBytes, 3l, 4l);
		actual.add(connection.zRevRangeWithScores(foo, 3l, 4l));
		verifyResults(Arrays.asList(new Object[] { stringTupleSet }));
	}

	@Test
	public void testZRevRankBytes() {
		doReturn(5l).when(nativeConnection).zRevRank(fooBytes, barBytes);
		actual.add(connection.zRevRank(fooBytes, barBytes));
		verifyResults(Arrays.asList(new Object[] { 5l }));
	}

	@Test
	public void testZRevRank() {
		doReturn(5l).when(nativeConnection).zRevRank(fooBytes, barBytes);
		actual.add(connection.zRevRank(foo, bar));
		verifyResults(Arrays.asList(new Object[] { 5l }));
	}

	@Test
	public void testZScoreBytes() {
		doReturn(3d).when(nativeConnection).zScore(fooBytes, barBytes);
		actual.add(connection.zScore(fooBytes, barBytes));
		verifyResults(Arrays.asList(new Object[] { 3d }));
	}

	@Test
	public void testZScore() {
		doReturn(3d).when(nativeConnection).zScore(fooBytes, barBytes);
		actual.add(connection.zScore(foo, bar));
		verifyResults(Arrays.asList(new Object[] { 3d }));
	}

	@Test
	public void testZUnionStoreAggWeightsBytes() {
		doReturn(5l).when(nativeConnection).zUnionStore(fooBytes, Aggregate.MAX, new int[0], fooBytes);
		actual.add(connection.zUnionStore(fooBytes, Aggregate.MAX, new int[0], fooBytes));
		verifyResults(Arrays.asList(new Object[] { 5l }));
	}

	@Test
	public void testZUnionStoreAggWeights() {
		doReturn(5l).when(nativeConnection).zUnionStore(fooBytes, Aggregate.MAX, new int[0], fooBytes);
		actual.add(connection.zUnionStore(foo, Aggregate.MAX, new int[0], foo));
		verifyResults(Arrays.asList(new Object[] { 5l }));
	}

	@Test
	public void testZUnionStoreBytes() {
		doReturn(5l).when(nativeConnection).zUnionStore(fooBytes, barBytes);
		actual.add(connection.zUnionStore(fooBytes, barBytes));
		verifyResults(Arrays.asList(new Object[] { 5l }));
	}

	@Test
	public void testZUnionStore() {
		doReturn(5l).when(nativeConnection).zUnionStore(fooBytes, barBytes);
		actual.add(connection.zUnionStore(foo, bar));
		verifyResults(Arrays.asList(new Object[] { 5l }));
	}

	@Test
	public void testPExpireBytes() {
		doReturn(true).when(nativeConnection).pExpire(fooBytes, 34l);
		actual.add(connection.pExpire(fooBytes, 34l));
		verifyResults(Arrays.asList(new Object[] { true }));
	}

	@Test
	public void testPExpire() {
		doReturn(true).when(nativeConnection).pExpire(fooBytes, 34l);
		actual.add(connection.pExpire(foo, 34l));
		verifyResults(Arrays.asList(new Object[] { true }));
	}

	@Test
	public void testPExpireAtBytes() {
		doReturn(true).when(nativeConnection).pExpireAt(fooBytes, 34l);
		actual.add(connection.pExpireAt(fooBytes, 34l));
		verifyResults(Arrays.asList(new Object[] { true }));
	}

	@Test
	public void testPExpireAt() {
		doReturn(true).when(nativeConnection).pExpireAt(fooBytes, 34l);
		actual.add(connection.pExpireAt(foo, 34l));
		verifyResults(Arrays.asList(new Object[] { true }));
	}

	@Test
	public void testPTtlBytes() {
		doReturn(5l).when(nativeConnection).pTtl(fooBytes);
		actual.add(connection.pTtl(fooBytes));
		verifyResults(Arrays.asList(new Object[] { 5l }));
	}

	@Test
	public void testPTtl() {
		doReturn(5l).when(nativeConnection).pTtl(fooBytes);
		actual.add(connection.pTtl(foo));
		verifyResults(Arrays.asList(new Object[] { 5l }));
	}

	@Test
	public void testDump() {
		doReturn(barBytes).when(nativeConnection).dump(fooBytes);
		actual.add(connection.dump(fooBytes));
		verifyResults(Arrays.asList(new Object[] { barBytes }));
	}

	@Test
	public void testScriptLoadBytes() {
		doReturn("foo").when(nativeConnection).scriptLoad(fooBytes);
		actual.add(connection.scriptLoad(fooBytes));
		verifyResults(Arrays.asList(new Object[] { "foo" }));
	}

	@Test
	public void testScriptLoad() {
		doReturn("foo").when(nativeConnection).scriptLoad(fooBytes);
		actual.add(connection.scriptLoad(foo));
		verifyResults(Arrays.asList(new Object[] { "foo" }));
	}

	@Test
	public void testScriptExists() {
		List<Boolean> results = Collections.singletonList(true);
		doReturn(results).when(nativeConnection).scriptExists("456");
		actual.add(connection.scriptExists("456"));
		verifyResults(Arrays.asList(new Object[] { results }));
	}

	@Test
	public void testEvalBytes() {
		doReturn("foo").when(nativeConnection).eval(fooBytes, ReturnType.VALUE, 3, fooBytes);
		actual.add(connection.eval(fooBytes, ReturnType.VALUE, 3, fooBytes));
		verifyResults(Arrays.asList(new Object[] { "foo" }));
	}

	@Test
	public void testEval() {
		doReturn("foo").when(nativeConnection).eval(fooBytes, ReturnType.VALUE, 3, fooBytes);
		actual.add(connection.eval(foo, ReturnType.VALUE, 3, foo));
		verifyResults(Arrays.asList(new Object[] { "foo" }));
	}

	@Test
	public void testEvalShaBytes() {
		doReturn("foo").when(nativeConnection).evalSha("456", ReturnType.VALUE, 3, fooBytes);
		actual.add(connection.evalSha("456", ReturnType.VALUE, 3, fooBytes));
		verifyResults(Arrays.asList(new Object[] { "foo" }));
	}

	@Test
	public void testEvalSha() {
		doReturn("foo").when(nativeConnection).evalSha("456", ReturnType.VALUE, 3, fooBytes);
		actual.add(connection.evalSha("456", ReturnType.VALUE, 3, foo));
		verifyResults(Arrays.asList(new Object[] { "foo" }));
	}

	@Test
	public void testExecute() {
		doReturn("foo").when(nativeConnection).execute("something", new byte[0][]);
		actual.add(connection.execute("something"));
		verifyResults(Arrays.asList(new Object[] { "foo" }));
	}

	@Test
	public void testExecuteByteArgs() {
		doReturn("foo").when(nativeConnection).execute("something", fooBytes);
		actual.add(connection.execute("something", fooBytes));
		verifyResults(Arrays.asList(new Object[] { "foo" }));
	}

	@Test
	public void testExecuteStringArgs() {
		doReturn("foo").when(nativeConnection).execute("something", fooBytes);
		actual.add(connection.execute("something", foo));
		verifyResults(Arrays.asList(new Object[] { "foo" }));
	}

	@Test // DATAREDIS-206
	public void testTimeIsDelegatedCorrectlyToNativeConnection() {

		doReturn(1L).when(nativeConnection).time();
		actual.add(connection.time());
		verifyResults(Arrays.asList(1L));
	}

	@Test // DATAREDIS-184
	public void testShutdownInDelegatedCorrectlyToNativeConnection() {

		connection.shutdown(ShutdownOption.NOSAVE);
		verify(nativeConnection, times(1)).shutdown(eq(ShutdownOption.NOSAVE));
	}

	@Test // DATAREDIS-269
	public void settingClientNameShouldDelegateToNativeConnection() {

		connection.setClientName("foo");
		verify(nativeConnection, times(1)).setClientName(eq("foo".getBytes()));
	}

	@Test // DATAREDIS-308
	public void pfAddShouldDelegateToNativeConnectionCorrectly() {

		connection.pfAdd("hll", "spring", "data", "redis");
		verify(nativeConnection, times(1)).pfAdd("hll".getBytes(), "spring".getBytes(), "data".getBytes(),
				"redis".getBytes());
	}

	@Test // DATAREDIS-308
	public void pfCountShouldDelegateToNativeConnectionCorrectly() {

		connection.pfCount("hll", "hyperLogLog");
		verify(nativeConnection, times(1)).pfCount("hll".getBytes(), "hyperLogLog".getBytes());
	}

	@Test // DATAREDIS-308
	public void pfMergeShouldDelegateToNativeConnectionCorrectly() {

		connection.pfMerge("merged", "spring", "data", "redis");
		verify(nativeConnection, times(1)).pfMerge("merged".getBytes(), "spring".getBytes(), "data".getBytes(),
				"redis".getBytes());
	}

	@Test // DATAREDIS-270
	public void testGetClientNameIsDelegatedCorrectlyToNativeConnection() {

		actual.add(connection.getClientName());
		verify(nativeConnection, times(1)).getClientName();
	}

	@Test // DATAREDIS-438
	public void testGeoAddBytes() {

		doReturn(1l).when(nativeConnection).geoAdd(fooBytes, new Point(1.23232, 34.2342434), barBytes);

		actual.add(connection.geoAdd(fooBytes, new Point(1.23232, 34.2342434), barBytes));
		verifyResults(Collections.singletonList(1L));
	}

	@Test // DATAREDIS-438
	public void testGeoAdd() {

		doReturn(1l).when(nativeConnection).geoAdd(fooBytes, new Point(1.23232, 34.2342434), barBytes);

		actual.add(connection.geoAdd(foo, new Point(1.23232, 34.2342434), bar));
		verifyResults(Collections.singletonList(1L));
	}

	@Test // DATAREDIS-438
	public void testGeoAddWithGeoLocationBytes() {

		doReturn(1l).when(nativeConnection).geoAdd(fooBytes,
				new GeoLocation<>(barBytes, new Point(1.23232, 34.2342434)));

		actual.add(connection.geoAdd(fooBytes, new GeoLocation<>(barBytes, new Point(1.23232, 34.2342434))));
		verifyResults(Collections.singletonList(1L));
	}

	@Test // DATAREDIS-438
	public void testGeoAddWithGeoLocation() {

		doReturn(1l).when(nativeConnection).geoAdd(fooBytes, new Point(1.23232, 34.2342434), barBytes);

		actual.add(connection.geoAdd(foo, new GeoLocation<>(bar, new Point(1.23232, 34.2342434))));
		verifyResults(Collections.singletonList(1L));
	}

	@Test // DATAREDIS-438
	public void testGeoAddCoordinateMapBytes() {

		Map<byte[], Point> memberGeoCoordinateMap = Collections.singletonMap(barBytes, new Point(1.23232, 34.2342434));
		doReturn(1l).when(nativeConnection).geoAdd(fooBytes, memberGeoCoordinateMap);

		actual.add(connection.geoAdd(fooBytes, memberGeoCoordinateMap));
		verifyResults(Collections.singletonList(1L));
	}

	@Test // DATAREDIS-438
	public void testGeoAddCoordinateMap() {

		doReturn(1l).when(nativeConnection).geoAdd(any(byte[].class), anyMap());

		actual.add(connection.geoAdd(foo, Collections.singletonMap(bar, new Point(1.23232, 34.2342434))));
		verifyResults(Collections.singletonList(1L));
	}

	@Test // DATAREDIS-438
	public void testGeoAddWithIterableOfGeoLocationBytes() {

		List<GeoLocation<byte[]>> values = Collections.singletonList(new GeoLocation<>(barBytes, new Point(1, 2)));
		doReturn(1l).when(nativeConnection).geoAdd(fooBytes, values);

		actual.add(connection.geoAdd(fooBytes, values));
		verifyResults(Collections.singletonList(1L));
	}

	@Test // DATAREDIS-438
	public void testGeoAddWithIterableOfGeoLocation() {

		doReturn(1l).when(nativeConnection).geoAdd(eq(fooBytes), anyMap());

		actual.add(connection.geoAdd(foo, Collections.singletonList(new GeoLocation<>(bar, new Point(1, 2)))));
		verifyResults(Collections.singletonList(1L));
	}

	@Test // DATAREDIS-438
	public void testGeoDistBytes() {

		doReturn(new Distance(102121.12d, DistanceUnit.METERS)).when(nativeConnection).geoDist(fooBytes, barBytes,
				bar2Bytes, DistanceUnit.METERS);

		actual.add(connection.geoDist(fooBytes, barBytes, bar2Bytes, DistanceUnit.METERS));
		verifyResults(Collections.singletonList(new Distance(102121.12d, DistanceUnit.METERS)));
	}

	@Test // DATAREDIS-438
	public void testGeoDist() {

		doReturn(new Distance(102121.12d, DistanceUnit.METERS)).when(nativeConnection).geoDist(fooBytes, barBytes,
				bar2Bytes, DistanceUnit.METERS);

		actual.add(connection.geoDist(foo, bar, bar2, DistanceUnit.METERS));
		verifyResults(Collections.singletonList(new Distance(102121.12d, DistanceUnit.METERS)));
	}

	@Test // DATAREDIS-438
	public void testGeoHashBytes() {

		doReturn(stringList).when(nativeConnection).geoHash(fooBytes, barBytes);

		actual.add(connection.geoHash(fooBytes, barBytes));
		verifyResults(Arrays.asList(Collections.singletonList(bar)));
	}

	@Test // DATAREDIS-438
	public void testGeoHash() {

		doReturn(stringList).when(nativeConnection).geoHash(fooBytes, barBytes);

		actual.add(connection.geoHash(foo, bar));
		verifyResults(Arrays.asList(Collections.singletonList(bar)));
	}

	@Test // DATAREDIS-438
	public void testGeoPosBytes() {

		doReturn(points).when(nativeConnection).geoPos(fooBytes, barBytes);

		actual.add(connection.geoPos(fooBytes, barBytes));
		verifyResults(Arrays.asList(points));
	}

	@Test // DATAREDIS-438
	public void testGeoPos() {

		doReturn(points).when(nativeConnection).geoPos(fooBytes, barBytes);
		actual.add(connection.geoPos(foo, bar));
		verifyResults(Arrays.asList(points));
	}

	@Test // DATAREDIS-438
	public void testGeoRadiusWithoutParamBytes() {

		doReturn(geoResults).when(nativeConnection).geoRadius(eq(fooBytes), any());

		actual.add(connection.geoRadius(fooBytes, null));
		verifyResults(Arrays.asList(geoResults));
	}

	@Test // DATAREDIS-438
	public void testGeoRadiusWithoutParam() {

		doReturn(geoResults).when(nativeConnection).geoRadius(eq(fooBytes), any(Circle.class));

		actual.add(
				connection.geoRadius(foo, new Circle(new Point(13.361389, 38.115556), new Distance(10, DistanceUnit.FEET))));
		verifyResults(Arrays.asList(Converters.deserializingGeoResultsConverter(serializer).convert(geoResults)));
	}

	@Test // DATAREDIS-438
	public void testGeoRadiusWithDistBytes() {

		GeoRadiusCommandArgs geoRadiusParam = GeoRadiusCommandArgs.newGeoRadiusArgs().includeDistance();
		doReturn(geoResults).when(nativeConnection).geoRadius(eq(fooBytes), any(Circle.class), eq(geoRadiusParam));

		actual.add(connection.geoRadius(fooBytes,
				new Circle(new Point(13.361389, 38.115556), new Distance(10, DistanceUnit.FEET)), geoRadiusParam));
		verifyResults(Arrays.asList(geoResults));
	}

	@Test // DATAREDIS-438
	public void testGeoRadiusWithDist() {

		GeoRadiusCommandArgs geoRadiusParam = GeoRadiusCommandArgs.newGeoRadiusArgs().includeDistance();
		doReturn(geoResults).when(nativeConnection).geoRadius(eq(fooBytes), any(Circle.class), eq(geoRadiusParam));

		actual.add(connection.geoRadius(foo,
				new Circle(new Point(13.361389, 38.115556), new Distance(10, DistanceUnit.FEET)), geoRadiusParam));
		verifyResults(Arrays.asList(Converters.deserializingGeoResultsConverter(serializer).convert(geoResults)));
	}

	@Test // DATAREDIS-438
	public void testGeoRadiusWithCoordAndDescBytes() {

		GeoRadiusCommandArgs geoRadiusParam = GeoRadiusCommandArgs.newGeoRadiusArgs().includeCoordinates().sortDescending();
		doReturn(geoResults).when(nativeConnection).geoRadius(eq(fooBytes), any(Circle.class), eq(geoRadiusParam));

		actual.add(connection.geoRadius(fooBytes,
				new Circle(new Point(13.361389, 38.115556), new Distance(10, DistanceUnit.FEET)), geoRadiusParam));
		verifyResults(Arrays.asList(geoResults));
	}

	@Test // DATAREDIS-438
	public void testGeoRadiusWithCoordAndDesc() {
		GeoRadiusCommandArgs geoRadiusParam = GeoRadiusCommandArgs.newGeoRadiusArgs().includeCoordinates().sortDescending();
		doReturn(geoResults).when(nativeConnection).geoRadius(eq(fooBytes), any(Circle.class), eq(geoRadiusParam));

		actual.add(connection.geoRadius(foo,
				new Circle(new Point(13.361389, 38.115556), new Distance(10, DistanceUnit.FEET)), geoRadiusParam));
		verifyResults(Arrays.asList(Converters.deserializingGeoResultsConverter(serializer).convert(geoResults)));
	}

	@Test // DATAREDIS-438
	public void testGeoRadiusByMemberWithoutParamBytes() {

		doReturn(geoResults).when(nativeConnection).geoRadiusByMember(fooBytes, barBytes,
				new Distance(38.115556, DistanceUnit.FEET));

		actual.add(connection.geoRadiusByMember(fooBytes, barBytes, new Distance(38.115556, DistanceUnit.FEET)));
		verifyResults(Arrays.asList(geoResults));
	}

	@Test // DATAREDIS-438
	public void testGeoRadiusByMemberWithoutParam() {

		doReturn(geoResults).when(nativeConnection).geoRadiusByMember(fooBytes, barBytes,
				new Distance(38.115556, DistanceUnit.FEET));

		actual.add(connection.geoRadiusByMember(foo, bar, new Distance(38.115556, DistanceUnit.FEET)));
		verifyResults(Arrays.asList(Converters.deserializingGeoResultsConverter(serializer).convert(geoResults)));
	}

	@Test // DATAREDIS-438
	public void testGeoRadiusByMemberWithDistAndAscBytes() {

		GeoRadiusCommandArgs geoRadiusParam = GeoRadiusCommandArgs.newGeoRadiusArgs().includeDistance().sortAscending();
		doReturn(geoResults).when(nativeConnection).geoRadiusByMember(fooBytes, barBytes,
				new Distance(38.115556, DistanceUnit.FEET), geoRadiusParam);

		actual.add(
				connection.geoRadiusByMember(fooBytes, barBytes, new Distance(38.115556, DistanceUnit.FEET), geoRadiusParam));
		verifyResults(Arrays.asList(geoResults));
	}

	@Test // DATAREDIS-438
	public void testGeoRadiusByMemberWithDistAndAsc() {

		GeoRadiusCommandArgs geoRadiusParam = GeoRadiusCommandArgs.newGeoRadiusArgs().includeDistance().sortAscending();
		doReturn(geoResults).when(nativeConnection).geoRadiusByMember(fooBytes, barBytes,
				new Distance(38.115556, DistanceUnit.FEET), geoRadiusParam);

		actual.add(connection.geoRadiusByMember(foo, bar, new Distance(38.115556, DistanceUnit.FEET), geoRadiusParam));
		verifyResults(Arrays.asList(Converters.deserializingGeoResultsConverter(serializer).convert(geoResults)));
	}

	@Test // DATAREDIS-438
	public void testGeoRadiusByMemberWithCoordAndCountBytes() {

		GeoRadiusCommandArgs geoRadiusParam = GeoRadiusCommandArgs.newGeoRadiusArgs().includeDistance().limit(23);
		doReturn(geoResults).when(nativeConnection).geoRadiusByMember(fooBytes, barBytes,
				new Distance(38.115556, DistanceUnit.FEET), geoRadiusParam);

		actual.add(
				connection.geoRadiusByMember(fooBytes, barBytes, new Distance(38.115556, DistanceUnit.FEET), geoRadiusParam));
		verifyResults(Arrays.asList(geoResults));
	}

	@Test // DATAREDIS-438
	public void testGeoRadiusByMemberWithCoordAndCount() {

		GeoRadiusCommandArgs geoRadiusParam = GeoRadiusCommandArgs.newGeoRadiusArgs().includeDistance().limit(23);
		doReturn(geoResults).when(nativeConnection).geoRadiusByMember(fooBytes, barBytes,
				new Distance(38.115556, DistanceUnit.FEET), geoRadiusParam);

		actual.add(connection.geoRadiusByMember(foo, bar, new Distance(38.115556, DistanceUnit.FEET), geoRadiusParam));
		verifyResults(Arrays.asList(Converters.deserializingGeoResultsConverter(serializer).convert(geoResults)));
	}

	protected List<Object> getResults() {
		return actual;
	}

	protected void verifyResults(List<?> expected) {
		assertEquals(expected, getResults());
	}
}
