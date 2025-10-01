/*
 * Copyright 2013-2025 the original author or authors.
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

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.data.geo.Distance;
import org.springframework.data.redis.connection.RedisGeoCommands.DistanceUnit;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.connection.zset.RankAndScore;

/**
 * @author Jennifer Hickey
 * @author Christoph Strobl
 * @author Ninad Divadkar
 * @author Mark Paluch
 * @author dengliming
 * @author Seongil Kim
 */
public class DefaultStringRedisConnectionPipelineTxTests extends DefaultStringRedisConnectionTxTests {

	@BeforeEach
	public void setUp() {
		super.setUp();
		when(nativeConnection.isPipelined()).thenReturn(true);
	}

	@Test
	public void testAppend() {
		doReturn(Collections.singletonList(Collections.singletonList(1L))).when(nativeConnection).closePipeline();
		super.testAppend();
	}

	@Test
	public void testAppendBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(1L))).when(nativeConnection).closePipeline();
		super.testAppendBytes();
	}

	@Test
	public void testBlPopBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(bytesList))).when(nativeConnection)
				.closePipeline();
		super.testBlPopBytes();
	}

	@Test
	public void testBlPop() {
		doReturn(Collections.singletonList(Collections.singletonList(bytesList))).when(nativeConnection)
				.closePipeline();
		super.testBlPop();
	}

	@Test
	public void testBrPopBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(bytesList))).when(nativeConnection)
				.closePipeline();
		super.testBrPopBytes();
	}

	@Test
	public void testBrPop() {
		doReturn(Collections.singletonList(Collections.singletonList(bytesList))).when(nativeConnection)
				.closePipeline();
		super.testBrPop();
	}

	@Test
	public void testBrPopLPushBytes() {
		doReturn(Collections.singletonList(Arrays.asList(new Object[] { barBytes }))).when(nativeConnection)
				.closePipeline();
		super.testBrPopLPushBytes();
	}

	@Test
	public void testBrPopLPush() {
		doReturn(Collections.singletonList(Arrays.asList(new Object[] { barBytes }))).when(nativeConnection)
				.closePipeline();
		super.testBrPopLPush();
	}

	@Test
	public void testCopy() {
		doReturn(Collections.singletonList(Arrays.asList(Boolean.TRUE))).when(nativeConnection).closePipeline();
		super.testCopy();
	}

	@Test
	public void testDbSize() {
		doReturn(Collections.singletonList(Collections.singletonList(3L))).when(nativeConnection).closePipeline();
		super.testDbSize();
	}

	@Test
	public void testDecrBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(3L))).when(nativeConnection).closePipeline();
		super.testDecrBytes();
	}

	@Test
	public void testDecr() {
		doReturn(Collections.singletonList(Collections.singletonList(3L))).when(nativeConnection).closePipeline();
		super.testDecr();
	}

	@Test
	public void testDecrByBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(3L))).when(nativeConnection).closePipeline();
		super.testDecrByBytes();
	}

	@Test
	public void testDecrBy() {
		doReturn(Collections.singletonList(Collections.singletonList(3L))).when(nativeConnection).closePipeline();
		super.testDecrBy();
	}

	@Test
	public void testDelBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(1L))).when(nativeConnection).closePipeline();
		super.testDelBytes();
	}

	@Test
	public void testDel() {
		doReturn(Collections.singletonList(Collections.singletonList(1L))).when(nativeConnection).closePipeline();
		super.testDel();
	}

	@Test
	public void testEchoBytes() {
		doReturn(Collections.singletonList(Arrays.asList(new Object[] { barBytes }))).when(nativeConnection)
				.closePipeline();
		super.testEchoBytes();
	}

	@Test
	public void testEcho() {
		doReturn(Collections.singletonList(Arrays.asList(new Object[] { barBytes }))).when(nativeConnection)
				.closePipeline();
		super.testEcho();
	}

	@Test
	public void testExistsBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(true))).when(nativeConnection)
				.closePipeline();
		super.testExistsBytes();
	}

	@Test
	public void testExists() {
		doReturn(Collections.singletonList(Collections.singletonList(true))).when(nativeConnection)
				.closePipeline();
		super.testExists();
	}

	@Test
	public void testExpireBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(true))).when(nativeConnection)
				.closePipeline();
		super.testExpireBytes();
	}

	@Test
	public void testExpire() {
		doReturn(Collections.singletonList(Collections.singletonList(true))).when(nativeConnection)
				.closePipeline();
		super.testExpire();
	}

	@Test
	public void testExpireAtBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(true))).when(nativeConnection)
				.closePipeline();
		super.testExpireAtBytes();
	}

	@Test
	public void testExpireAt() {
		doReturn(Collections.singletonList(Collections.singletonList(true))).when(nativeConnection)
				.closePipeline();
		super.testExpireAt();
	}

	@Test
	public void testGetBytes() {
		doReturn(Collections.singletonList(Arrays.asList(new Object[] { barBytes }))).when(nativeConnection)
				.closePipeline();
		super.testGetBytes();
	}

	@Test
	public void testGet() {
		doReturn(Collections.singletonList(Arrays.asList(new Object[] { barBytes }))).when(nativeConnection)
				.closePipeline();
		super.testGet();
	}

	@Test
	public void testGetBitBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(true))).when(nativeConnection)
				.closePipeline();
		super.testGetBitBytes();
	}

	@Test
	public void testGetBit() {
		doReturn(Collections.singletonList(Collections.singletonList(true))).when(nativeConnection)
				.closePipeline();
		super.testGetBit();
	}

	@Test // DATAREDIS-661
	public void testGetConfig() {

		Properties results = new Properties();
		results.put("foo", "bar");

		doReturn(Collections.singletonList(Collections.singletonList(results))).when(nativeConnection)
				.closePipeline();
		super.testGetConfig();
	}

	@Test
	public void testGetNativeConnection() {
		doReturn(Collections.singletonList(Collections.singletonList("foo"))).when(nativeConnection)
				.closePipeline();
		super.testGetNativeConnection();
	}

	@Test
	public void testGetRangeBytes() {
		doReturn(Collections.singletonList(Arrays.asList(new Object[] { barBytes }))).when(nativeConnection)
				.closePipeline();
		super.testGetRangeBytes();
	}

	@Test
	public void testGetRange() {
		doReturn(Collections.singletonList(Arrays.asList(new Object[] { barBytes }))).when(nativeConnection)
				.closePipeline();
		super.testGetRange();
	}

	@Test
	public void testGetSetBytes() {
		doReturn(Collections.singletonList(Arrays.asList(new Object[] { barBytes }))).when(nativeConnection)
				.closePipeline();
		super.testGetSetBytes();
	}

	@Test
	public void testGetSet() {
		doReturn(Collections.singletonList(Arrays.asList(new Object[] { barBytes }))).when(nativeConnection)
				.closePipeline();
		super.testGetSet();
	}

	@Test
	public void testHDelBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(1L))).when(nativeConnection).closePipeline();
		super.testHDelBytes();
	}

	@Test
	public void testHDel() {
		doReturn(Collections.singletonList(Collections.singletonList(1L))).when(nativeConnection).closePipeline();
		super.testHDel();
	}

	@Test
	public void testHExistsBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(true))).when(nativeConnection)
				.closePipeline();
		super.testHExistsBytes();
	}

	@Test
	public void testHExists() {
		doReturn(Collections.singletonList(Collections.singletonList(true))).when(nativeConnection)
				.closePipeline();
		super.testHExists();
	}

	@Test
	public void testHGetBytes() {
		doReturn(Collections.singletonList(Arrays.asList(new Object[] { barBytes }))).when(nativeConnection)
				.closePipeline();
		super.testHGetBytes();
	}

	@Test
	public void testHGet() {
		doReturn(Collections.singletonList(Arrays.asList(new Object[] { barBytes }))).when(nativeConnection)
				.closePipeline();
		super.testHGet();
	}

	@Test
	public void testHGetAllBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(bytesMap))).when(nativeConnection)
				.closePipeline();
		super.testHGetAllBytes();
	}

	@Test
	public void testHGetAll() {
		doReturn(Collections.singletonList(Collections.singletonList(bytesMap))).when(nativeConnection)
				.closePipeline();
		super.testHGetAll();
	}

	@Test
	public void testHIncrByBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(3L))).when(nativeConnection).closePipeline();
		super.testHIncrByBytes();
	}

	@Test
	public void testHIncrBy() {
		doReturn(Collections.singletonList(Collections.singletonList(3L))).when(nativeConnection).closePipeline();
		super.testHIncrBy();
	}

	@Test
	public void testHIncrByDoubleBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(3d))).when(nativeConnection).closePipeline();
		super.testHIncrByDoubleBytes();
	}

	@Test
	public void testHIncrByDouble() {
		doReturn(Collections.singletonList(Collections.singletonList(3d))).when(nativeConnection).closePipeline();
		super.testHIncrByDouble();
	}

	@Test
	public void testHKeysBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(bytesSet))).when(nativeConnection)
				.closePipeline();
		super.testHKeysBytes();
	}

	@Test
	public void testHKeys() {
		doReturn(Collections.singletonList(Collections.singletonList(bytesSet))).when(nativeConnection)
				.closePipeline();
		super.testHKeys();
	}

	@Test
	public void testHLenBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(3L))).when(nativeConnection).closePipeline();
		super.testHLenBytes();
	}

	@Test
	public void testHLen() {
		doReturn(Collections.singletonList(Collections.singletonList(3L))).when(nativeConnection).closePipeline();
		super.testHLen();
	}

	@Test
	public void testHMGetBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(bytesList))).when(nativeConnection)
				.closePipeline();
		super.testHMGetBytes();
	}

	@Test
	public void testHMGet() {
		doReturn(Collections.singletonList(Collections.singletonList(bytesList))).when(nativeConnection)
				.closePipeline();
		super.testHMGet();
	}

	@Test
	public void testHSetBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(true))).when(nativeConnection)
				.closePipeline();
		super.testHSetBytes();
	}

	@Test
	public void testHSet() {
		doReturn(Collections.singletonList(Collections.singletonList(true))).when(nativeConnection)
				.closePipeline();
		super.testHSet();
	}

	@Test
	public void testHSetNXBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(true))).when(nativeConnection)
				.closePipeline();
		super.testHSetNXBytes();
	}

	@Test
	public void testHSetNX() {
		doReturn(Collections.singletonList(Collections.singletonList(true))).when(nativeConnection)
				.closePipeline();
		super.testHSetNX();
	}

	@Test
	public void testHValsBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(bytesList))).when(nativeConnection)
				.closePipeline();
		super.testHValsBytes();
	}

	@Test
	public void testHVals() {
		doReturn(Collections.singletonList(Collections.singletonList(bytesList))).when(nativeConnection)
				.closePipeline();
		super.testHVals();
	}

	@Test
	public void testIncrBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(2L))).when(nativeConnection).closePipeline();
		super.testIncrBytes();
	}

	@Test
	public void testIncr() {
		doReturn(Collections.singletonList(Collections.singletonList(2L))).when(nativeConnection).closePipeline();
		super.testIncr();
	}

	@Test
	public void testIncrByBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(2L))).when(nativeConnection).closePipeline();
		super.testIncrByBytes();
	}

	@Test
	public void testIncrBy() {
		doReturn(Collections.singletonList(Collections.singletonList(2L))).when(nativeConnection).closePipeline();
		super.testIncrBy();
	}

	@Test
	public void testIncrByDoubleBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(2d))).when(nativeConnection).closePipeline();
		super.testIncrByDoubleBytes();
	}

	@Test
	public void testIncrByDouble() {
		doReturn(Collections.singletonList(Collections.singletonList(2d))).when(nativeConnection).closePipeline();
		super.testIncrByDouble();
	}

	@Test
	public void testInfo() {
		Properties props = new Properties();
		props.put("foo", "bar");
		doReturn(Collections.singletonList(Collections.singletonList(props))).when(nativeConnection)
				.closePipeline();
		super.testInfo();
	}

	@Test
	public void testInfoBySection() {
		Properties props = new Properties();
		props.put("foo", "bar");
		doReturn(Collections.singletonList(Collections.singletonList(props))).when(nativeConnection)
				.closePipeline();
		super.testInfoBySection();
	}

	@Test
	public void testKeysBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(bytesSet))).when(nativeConnection)
				.closePipeline();
		super.testKeysBytes();
	}

	@Test
	public void testKeys() {
		doReturn(Collections.singletonList(Collections.singletonList(bytesSet))).when(nativeConnection)
				.closePipeline();
		super.testKeys();
	}

	@Test
	public void testLastSave() {
		doReturn(Collections.singletonList(Collections.singletonList(6L))).when(nativeConnection).closePipeline();
		super.testLastSave();
	}

	@Test
	public void testLIndexBytes() {
		doReturn(Collections.singletonList(Arrays.asList(new Object[] { barBytes }))).when(nativeConnection)
				.closePipeline();
		super.testLIndexBytes();
	}

	@Test
	public void testLIndex() {
		doReturn(Collections.singletonList(Arrays.asList(new Object[] { barBytes }))).when(nativeConnection)
				.closePipeline();
		super.testLIndex();
	}

	@Test
	public void testLInsertBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(8L))).when(nativeConnection).closePipeline();
		super.testLInsertBytes();
	}

	@Test
	public void testLInsert() {
		doReturn(Collections.singletonList(Collections.singletonList(8L))).when(nativeConnection).closePipeline();
		super.testLInsert();
	}

	@Test
	public void testLLenBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(8L))).when(nativeConnection).closePipeline();
		super.testLLenBytes();
	}

	@Test
	public void testLLen() {
		doReturn(Collections.singletonList(Collections.singletonList(8L))).when(nativeConnection).closePipeline();
		super.testLLen();
	}

	@Test
	public void testLPopBytes() {
		doReturn(Collections.singletonList(Arrays.asList(new Object[] { barBytes }))).when(nativeConnection)
				.closePipeline();
		super.testLPopBytes();
	}

	@Test
	public void testLPop() {
		doReturn(Collections.singletonList(Arrays.asList(new Object[] { barBytes }))).when(nativeConnection)
				.closePipeline();
		super.testLPop();
	}

	@Test
	public void testLPopCountBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(bytesList))).when(nativeConnection).closePipeline();
		super.testLPopCountBytes();
	}

	@Test
	public void testLPopCount() {
		doReturn(Collections.singletonList(Collections.singletonList(bytesList))).when(nativeConnection).closePipeline();
		super.testLPopCount();
	}

	@Test
	public void testLPushBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(8L))).when(nativeConnection).closePipeline();
		super.testLPushBytes();
	}

	@Test
	public void testLPush() {
		doReturn(Collections.singletonList(Collections.singletonList(8L))).when(nativeConnection).closePipeline();
		super.testLPush();
	}

	@Test
	public void testLPushXBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(8L))).when(nativeConnection).closePipeline();
		super.testLPushXBytes();
	}

	@Test
	public void testLPushX() {
		doReturn(Collections.singletonList(Collections.singletonList(8L))).when(nativeConnection).closePipeline();
		super.testLPushX();
	}

	@Test
	public void testLRangeBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(bytesList))).when(nativeConnection)
				.closePipeline();
		super.testLRangeBytes();
	}

	@Test
	public void testLRange() {
		doReturn(Collections.singletonList(Collections.singletonList(bytesList))).when(nativeConnection)
				.closePipeline();
		super.testLRange();
	}

	@Test
	public void testLRemBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(8L))).when(nativeConnection).closePipeline();
		super.testLRemBytes();
	}

	@Test
	public void testLRem() {
		doReturn(Collections.singletonList(Collections.singletonList(8L))).when(nativeConnection).closePipeline();
		super.testLRem();
	}

	@Test
	public void testMGetBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(bytesList))).when(nativeConnection)
				.closePipeline();
		super.testMGetBytes();
	}

	@Test
	public void testMGet() {
		doReturn(Collections.singletonList(Collections.singletonList(bytesList))).when(nativeConnection)
				.closePipeline();
		super.testMGet();
	}

	@Test
	public void testMSetNXBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(true))).when(nativeConnection)
				.closePipeline();
		super.testMSetNXBytes();
	}

	@Test
	public void testMSetNXString() {
		doReturn(Collections.singletonList(Collections.singletonList(true))).when(nativeConnection)
				.closePipeline();
		super.testMSetNXString();
	}

	@Test
	public void testPersistBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(true))).when(nativeConnection)
				.closePipeline();
		super.testPersistBytes();
	}

	@Test
	public void testPersist() {
		doReturn(Collections.singletonList(Collections.singletonList(true))).when(nativeConnection)
				.closePipeline();
		super.testPersist();
	}

	@Test
	public void testMoveBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(true))).when(nativeConnection)
				.closePipeline();
		super.testMoveBytes();
	}

	@Test
	public void testMove() {
		doReturn(Collections.singletonList(Collections.singletonList(true))).when(nativeConnection)
				.closePipeline();
		super.testMove();
	}

	@Test
	public void testPing() {
		doReturn(Collections.singletonList(Collections.singletonList("pong"))).when(nativeConnection)
				.closePipeline();
		super.testPing();
	}

	@Test
	public void testPublishBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(2L))).when(nativeConnection).closePipeline();
		super.testPublishBytes();
	}

	@Test
	public void testPublish() {
		doReturn(Collections.singletonList(Collections.singletonList(2L))).when(nativeConnection).closePipeline();
		super.testPublish();
	}

	@Test
	public void testRandomKey() {
		doReturn(Collections.singletonList(Arrays.asList(new Object[] { fooBytes }))).when(nativeConnection)
				.closePipeline();
		super.testRandomKey();
	}

	@Test
	public void testRenameNXBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(true))).when(nativeConnection)
				.closePipeline();
		super.testRenameNXBytes();
	}

	@Test
	public void testRenameNX() {
		doReturn(Collections.singletonList(Collections.singletonList(true))).when(nativeConnection)
				.closePipeline();
		super.testRenameNX();
	}

	@Test
	public void testRPopBytes() {
		doReturn(Collections.singletonList(Arrays.asList(new Object[] { barBytes }))).when(nativeConnection)
				.closePipeline();
		super.testRPopBytes();
	}

	@Test
	public void testRPop() {
		doReturn(Collections.singletonList(Arrays.asList(new Object[] { barBytes }))).when(nativeConnection)
				.closePipeline();
		super.testRPop();
	}

	@Test
	public void testRPopCountBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(bytesList))).when(nativeConnection).closePipeline();
		super.testRPopCountBytes();
	}

	@Test
	public void testRPopCount() {
		doReturn(Collections.singletonList(Collections.singletonList(bytesList))).when(nativeConnection).closePipeline();
		super.testRPopCount();
	}

	@Test
	public void testRPopLPushBytes() {
		doReturn(Collections.singletonList(Arrays.asList(new Object[] { barBytes }))).when(nativeConnection)
				.closePipeline();
		super.testRPopLPushBytes();
	}

	@Test
	public void testRPopLPush() {
		doReturn(Collections.singletonList(Arrays.asList(new Object[] { barBytes }))).when(nativeConnection)
				.closePipeline();
		super.testRPopLPush();
	}

	@Test
	public void testRPushBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(4L))).when(nativeConnection).closePipeline();
		super.testRPushBytes();
	}

	@Test
	public void testRPush() {
		doReturn(Collections.singletonList(Collections.singletonList(4L))).when(nativeConnection).closePipeline();
		super.testRPush();
	}

	@Test
	public void testRPushXBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(4L))).when(nativeConnection).closePipeline();
		super.testRPushXBytes();
	}

	@Test
	public void testRPushX() {
		doReturn(Collections.singletonList(Collections.singletonList(4L))).when(nativeConnection).closePipeline();
		super.testRPushX();
	}

	@Test
	public void testSAddBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(1L))).when(nativeConnection).closePipeline();
		super.testSAddBytes();
	}

	@Test
	public void testSAdd() {
		doReturn(Collections.singletonList(Collections.singletonList(1L))).when(nativeConnection).closePipeline();
		super.testSAdd();
	}

	@Test
	public void testSCardBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(4L))).when(nativeConnection).closePipeline();
		super.testSCardBytes();
	}

	@Test
	public void testSCard() {
		doReturn(Collections.singletonList(Collections.singletonList(4L))).when(nativeConnection).closePipeline();
		super.testSCard();
	}

	@Test
	public void testSDiffBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(bytesSet))).when(nativeConnection)
				.closePipeline();
		super.testSDiffBytes();
	}

	@Test
	public void testSDiff() {
		doReturn(Collections.singletonList(Collections.singletonList(bytesSet))).when(nativeConnection)
				.closePipeline();
		super.testSDiff();
	}

	@Test
	public void testSDiffStoreBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(3L))).when(nativeConnection).closePipeline();
		super.testSDiffStoreBytes();
	}

	@Test
	public void testSDiffStore() {
		doReturn(Collections.singletonList(Collections.singletonList(3L))).when(nativeConnection).closePipeline();
		super.testSDiffStore();
	}

	@Test
	public void testSetNXBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(true))).when(nativeConnection)
				.closePipeline();
		super.testSetNXBytes();
	}

	@Test
	public void testSetNX() {
		doReturn(Collections.singletonList(Collections.singletonList(true))).when(nativeConnection)
				.closePipeline();
		super.testSetNX();
	}

	@Test
	public void testSInterBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(bytesSet))).when(nativeConnection)
				.closePipeline();
		super.testSInterBytes();
	}

	@Test
	public void testSInter() {
		doReturn(Collections.singletonList(Collections.singletonList(bytesSet))).when(nativeConnection)
				.closePipeline();
		super.testSInter();
	}

	@Test
	public void testSInterStoreBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(3L))).when(nativeConnection).closePipeline();
		super.testSInterStoreBytes();
	}

	@Test
	public void testSInterStore() {
		doReturn(Collections.singletonList(Collections.singletonList(3L))).when(nativeConnection).closePipeline();
		super.testSInterStore();
	}

	@Test
	public void testSIsMemberBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(true))).when(nativeConnection)
				.closePipeline();
		super.testSIsMemberBytes();
	}

	@Test
	public void testSIsMember() {
		doReturn(Collections.singletonList(Collections.singletonList(true))).when(nativeConnection)
				.closePipeline();
		super.testSIsMember();
	}

	@Test
	public void testSMembersBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(bytesSet))).when(nativeConnection)
				.closePipeline();
		super.testSMembersBytes();
	}

	@Test
	public void testSMembers() {
		doReturn(Collections.singletonList(Collections.singletonList(bytesSet))).when(nativeConnection)
				.closePipeline();
		super.testSMembers();
	}

	@Test
	public void testSMoveBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(true))).when(nativeConnection)
				.closePipeline();
		super.testSMoveBytes();
	}

	@Test
	public void testSMove() {
		doReturn(Collections.singletonList(Collections.singletonList(true))).when(nativeConnection)
				.closePipeline();
		super.testSMove();
	}

	@Test
	public void testSortStoreBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(3L))).when(nativeConnection).closePipeline();
		super.testSortStoreBytes();
	}

	@Test
	public void testSortStore() {
		doReturn(Collections.singletonList(Collections.singletonList(3L))).when(nativeConnection).closePipeline();
		super.testSortStore();
	}

	@Test
	public void testSortBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(bytesList))).when(nativeConnection)
				.closePipeline();
		super.testSortBytes();
	}

	@Test
	public void testSort() {
		doReturn(Collections.singletonList(Collections.singletonList(bytesList))).when(nativeConnection)
				.closePipeline();
		super.testSort();
	}

	@Test
	public void testSPopBytes() {
		doReturn(Collections.singletonList(Arrays.asList(new Object[] { barBytes }))).when(nativeConnection)
				.closePipeline();
		super.testSPopBytes();
	}

	@Test
	public void testSPop() {
		doReturn(Collections.singletonList(Arrays.asList(new Object[] { barBytes }))).when(nativeConnection)
				.closePipeline();
		super.testSPop();
	}

	@Test
	public void testSRandMemberBytes() {
		doReturn(Collections.singletonList(Arrays.asList(new Object[] { barBytes }))).when(nativeConnection)
				.closePipeline();
		super.testSRandMemberBytes();
	}

	@Test
	public void testSRandMember() {
		doReturn(Collections.singletonList(Arrays.asList(new Object[] { barBytes }))).when(nativeConnection)
				.closePipeline();
		super.testSRandMember();
	}

	@Test
	public void testSRandMemberCountBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(bytesList))).when(nativeConnection)
				.closePipeline();
		super.testSRandMemberCountBytes();
	}

	@Test
	public void testSRandMemberCount() {
		doReturn(Collections.singletonList(Collections.singletonList(bytesList))).when(nativeConnection)
				.closePipeline();
		super.testSRandMemberCount();
	}

	@Test
	public void testSRemBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(1L))).when(nativeConnection).closePipeline();
		super.testSRemBytes();
	}

	@Test
	public void testSRem() {
		doReturn(Collections.singletonList(Collections.singletonList(1L))).when(nativeConnection).closePipeline();
		super.testSRem();
	}

	@Test
	public void testStrLenBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(5L))).when(nativeConnection).closePipeline();
		super.testStrLenBytes();
	}

	@Test
	public void testStrLen() {
		doReturn(Collections.singletonList(Collections.singletonList(5L))).when(nativeConnection).closePipeline();
		super.testStrLen();
	}

	@Test
	public void testBitCountBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(5L))).when(nativeConnection).closePipeline();
		super.testBitCountBytes();
	}

	@Test
	public void testBitCount() {
		doReturn(Collections.singletonList(Collections.singletonList(5L))).when(nativeConnection).closePipeline();
		super.testBitCount();
	}

	@Test
	public void testBitCountRangeBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(5L))).when(nativeConnection).closePipeline();
		super.testBitCountRangeBytes();
	}

	@Test
	public void testBitCountRange() {
		doReturn(Collections.singletonList(Collections.singletonList(5L))).when(nativeConnection).closePipeline();
		super.testBitCountRange();
	}

	@Test
	public void testBitOpBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(5L))).when(nativeConnection).closePipeline();
		super.testBitOpBytes();
	}

	@Test
	public void testBitOp() {
		doReturn(Collections.singletonList(Collections.singletonList(5L))).when(nativeConnection).closePipeline();
		super.testBitOp();
	}

	@Test
	public void testSUnionBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(bytesSet))).when(nativeConnection)
				.closePipeline();
		super.testSUnionBytes();
	}

	@Test
	public void testSUnion() {
		doReturn(Collections.singletonList(Collections.singletonList(bytesSet))).when(nativeConnection)
				.closePipeline();
		super.testSUnion();
	}

	@Test
	public void testSUnionStoreBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(5L))).when(nativeConnection).closePipeline();
		super.testSUnionStoreBytes();
	}

	@Test
	public void testSUnionStore() {
		doReturn(Collections.singletonList(Collections.singletonList(5L))).when(nativeConnection).closePipeline();
		super.testSUnionStore();
	}

	@Test
	public void testTtlBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(5L))).when(nativeConnection).closePipeline();
		super.testTtlBytes();
	}

	@Test
	public void testTtl() {
		doReturn(Collections.singletonList(Collections.singletonList(5L))).when(nativeConnection).closePipeline();
		super.testTtl();
	}

	// DATAREDIS-526
	@Override
	@Test
	public void testTtlWithTimeUnit() {

		doReturn(Collections.singletonList(Collections.singletonList(5L))).when(nativeConnection).closePipeline();
		super.testTtlWithTimeUnit();
	}

	@Test
	public void testTypeBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(DataType.HASH))).when(nativeConnection)
				.closePipeline();
		super.testTypeBytes();
	}

	@Test
	public void testType() {
		doReturn(Collections.singletonList(Collections.singletonList(DataType.HASH))).when(nativeConnection)
				.closePipeline();
		super.testType();
	}

	@Test
	public void testZAddBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(true))).when(nativeConnection)
				.closePipeline();
		super.testZAddBytes();
	}

	@Test
	public void testZAdd() {
		doReturn(Collections.singletonList(Collections.singletonList(true))).when(nativeConnection)
				.closePipeline();
		super.testZAdd();
	}

	@Test
	public void testZAddMultipleBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(1L))).when(nativeConnection).closePipeline();
		super.testZAddMultipleBytes();
	}

	@Test
	public void testZAddMultiple() {
		doReturn(Collections.singletonList(Collections.singletonList(1L))).when(nativeConnection).closePipeline();
		super.testZAddMultiple();
	}

	@Test
	public void testZCardBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(5L))).when(nativeConnection).closePipeline();
		super.testZCardBytes();
	}

	@Test
	public void testZCard() {
		doReturn(Collections.singletonList(Collections.singletonList(5L))).when(nativeConnection).closePipeline();
		super.testZCard();
	}

	@Test
	public void testZCountBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(5L))).when(nativeConnection).closePipeline();
		super.testZCountBytes();
	}

	@Test
	public void testZCount() {
		doReturn(Collections.singletonList(Collections.singletonList(5L))).when(nativeConnection).closePipeline();
		super.testZCount();
	}

	@Test
	public void testZIncrByBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(3d))).when(nativeConnection).closePipeline();
		super.testZIncrByBytes();
	}

	@Test
	public void testZIncrBy() {
		doReturn(Collections.singletonList(Collections.singletonList(3d))).when(nativeConnection).closePipeline();
		super.testZIncrBy();
	}

	@Test
	public void testZInterStoreAggWeightsBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(5L))).when(nativeConnection).closePipeline();
		super.testZInterStoreAggWeightsBytes();
	}

	@Test
	public void testZInterStoreAggWeights() {
		doReturn(Collections.singletonList(Collections.singletonList(5L))).when(nativeConnection).closePipeline();
		super.testZInterStoreAggWeights();
	}

	@Test
	public void testZInterStoreBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(5L))).when(nativeConnection).closePipeline();
		super.testZInterStoreBytes();
	}

	@Test
	public void testZInterStore() {
		doReturn(Collections.singletonList(Collections.singletonList(5L))).when(nativeConnection).closePipeline();
		super.testZInterStore();
	}

	@Test
	public void testZRangeBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(bytesSet))).when(nativeConnection)
				.closePipeline();
		super.testZRangeBytes();
	}

	@Test
	public void testZRange() {
		doReturn(Collections.singletonList(Collections.singletonList(bytesSet))).when(nativeConnection)
				.closePipeline();
		super.testZRange();
	}

	@Test
	public void testZRangeByScoreOffsetCountBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(bytesSet))).when(nativeConnection)
				.closePipeline();
		super.testZRangeByScoreOffsetCountBytes();
	}

	@Test
	public void testZRangeByScoreOffsetCount() {
		doReturn(Collections.singletonList(Collections.singletonList(bytesSet))).when(nativeConnection)
				.closePipeline();
		super.testZRangeByScoreOffsetCount();
	}

	@Test
	public void testZRangeByScoreBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(bytesSet))).when(nativeConnection)
				.closePipeline();
		super.testZRangeByScoreBytes();
	}

	@Test
	public void testZRangeByScore() {
		doReturn(Collections.singletonList(Collections.singletonList(bytesSet))).when(nativeConnection)
				.closePipeline();
		super.testZRangeByScore();
	}

	@Test
	public void testZRangeByScoreWithScoresOffsetCountBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(tupleSet))).when(nativeConnection)
				.closePipeline();
		super.testZRangeByScoreWithScoresOffsetCountBytes();
	}

	@Test
	public void testZRangeByScoreWithScoresOffsetCount() {
		doReturn(Collections.singletonList(Collections.singletonList(tupleSet))).when(nativeConnection)
				.closePipeline();
		super.testZRangeByScoreWithScoresOffsetCount();
	}

	@Test
	public void testZRangeByScoreWithScoresBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(tupleSet))).when(nativeConnection)
				.closePipeline();
		super.testZRangeByScoreWithScoresBytes();
	}

	@Test
	public void testZRangeByScoreWithScores() {
		doReturn(Collections.singletonList(Collections.singletonList(tupleSet))).when(nativeConnection)
				.closePipeline();
		super.testZRangeByScoreWithScores();
	}

	@Test
	public void testZRangeWithScoresBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(tupleSet))).when(nativeConnection)
				.closePipeline();
		super.testZRangeWithScoresBytes();
	}

	@Test
	public void testZRangeWithScores() {
		doReturn(Collections.singletonList(Collections.singletonList(tupleSet))).when(nativeConnection)
				.closePipeline();
		super.testZRangeWithScores();
	}

	@Test
	public void testZRevRangeByScoreOffsetCountBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(bytesSet))).when(nativeConnection)
				.closePipeline();
		super.testZRevRangeByScoreOffsetCountBytes();
	}

	@Test
	public void testZRevRangeByScoreOffsetCount() {
		doReturn(Collections.singletonList(Collections.singletonList(bytesSet))).when(nativeConnection)
				.closePipeline();
		super.testZRevRangeByScoreOffsetCount();
	}

	@Test
	public void testZRevRangeByScoreBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(bytesSet))).when(nativeConnection)
				.closePipeline();
		super.testZRevRangeByScoreBytes();
	}

	@Test
	public void testZRevRangeByScore() {
		doReturn(Collections.singletonList(Collections.singletonList(bytesSet))).when(nativeConnection)
				.closePipeline();
		super.testZRevRangeByScore();
	}

	@Test
	public void testZRevRangeByScoreWithScoresOffsetCountBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(tupleSet))).when(nativeConnection)
				.closePipeline();
		super.testZRevRangeByScoreWithScoresOffsetCountBytes();
	}

	@Test
	public void testZRevRangeByScoreWithScoresOffsetCount() {
		doReturn(Collections.singletonList(Collections.singletonList(tupleSet))).when(nativeConnection)
				.closePipeline();
		super.testZRevRangeByScoreWithScoresOffsetCount();
	}

	@Test
	public void testZRevRangeByScoreWithScoresBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(tupleSet))).when(nativeConnection)
				.closePipeline();
		super.testZRevRangeByScoreWithScoresBytes();
	}

	@Test
	public void testZRevRangeByScoreWithScores() {
		doReturn(Collections.singletonList(Collections.singletonList(tupleSet))).when(nativeConnection)
				.closePipeline();
		super.testZRevRangeByScoreWithScores();
	}

	@Test
	public void testZRankBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(5L))).when(nativeConnection).closePipeline();
		super.testZRankBytes();
	}

	@Test
	public void testZRankWithScoreBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(new RankAndScore(0L, 3.0)))).when(nativeConnection).closePipeline();
		super.testZRankWithScoreBytes();
	}

	@Test
	public void testZRank() {
		doReturn(Collections.singletonList(Collections.singletonList(5L))).when(nativeConnection).closePipeline();
		super.testZRank();
	}

	@Test
	public void testZRankWithScore() {
		doReturn(Collections.singletonList(Collections.singletonList(new RankAndScore(0L, 3.0)))).when(nativeConnection).closePipeline();
		super.testZRankWithScore();
	}

	@Test
	public void testZRemBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(1L))).when(nativeConnection).closePipeline();
		super.testZRemBytes();
	}

	@Test
	public void testZRem() {
		doReturn(Collections.singletonList(Collections.singletonList(1L))).when(nativeConnection).closePipeline();
		super.testZRem();
	}

	@Test
	public void testZRemRangeBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(5L))).when(nativeConnection).closePipeline();
		super.testZRemRangeBytes();
	}

	@Test
	public void testZRemRange() {
		doReturn(Collections.singletonList(Collections.singletonList(5L))).when(nativeConnection).closePipeline();
		super.testZRemRange();
	}

	@Test
	public void testZRemRangeByScoreBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(5L))).when(nativeConnection).closePipeline();
		super.testZRemRangeByScoreBytes();
	}

	@Test
	public void testZRemRangeByScore() {
		doReturn(Collections.singletonList(Collections.singletonList(5L))).when(nativeConnection).closePipeline();
		super.testZRemRangeByScore();
	}

	@Test
	public void testZRevRangeBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(bytesSet))).when(nativeConnection)
				.closePipeline();
		super.testZRevRangeBytes();
	}

	@Test
	public void testZRevRange() {
		doReturn(Collections.singletonList(Collections.singletonList(bytesSet))).when(nativeConnection)
				.closePipeline();
		super.testZRevRange();
	}

	@Test
	public void testZRevRangeWithScoresBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(tupleSet))).when(nativeConnection)
				.closePipeline();
		super.testZRevRangeWithScoresBytes();
	}

	@Test
	public void testZRevRangeWithScores() {
		doReturn(Collections.singletonList(Collections.singletonList(tupleSet))).when(nativeConnection)
				.closePipeline();
		super.testZRevRangeWithScores();
	}

	@Test
	public void testZRevRankBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(5L))).when(nativeConnection).closePipeline();
		super.testZRevRankBytes();
	}

	@Test
	public void testZRevRankWithScoreBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(new RankAndScore(0L, 3.0)))).when(nativeConnection).closePipeline();
		super.testZRevRankWithScoreBytes();
	}

	@Test
	public void testZRevRank() {
		doReturn(Collections.singletonList(Collections.singletonList(5L))).when(nativeConnection).closePipeline();
		super.testZRevRank();
	}

	@Test
	public void testZRevRankWithScore() {
		doReturn(Collections.singletonList(Collections.singletonList(new RankAndScore(0L, 3.0)))).when(nativeConnection).closePipeline();
		super.testZRevRankWithScore();
	}

	@Test
	public void testZScoreBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(3d))).when(nativeConnection).closePipeline();
		super.testZScoreBytes();
	}

	@Test
	public void testZScore() {
		doReturn(Collections.singletonList(Collections.singletonList(3d))).when(nativeConnection).closePipeline();
		super.testZScore();
	}

	@Test
	public void testZMScore() {

		doReturn(Collections.singletonList(Collections.singletonList(Arrays.asList(1d, 3d)))).when(nativeConnection)
				.closePipeline();
		super.testZMScore();
	}

	@Test
	public void testZUnionStoreAggWeightsBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(5L))).when(nativeConnection).closePipeline();
		super.testZUnionStoreAggWeightsBytes();
	}

	@Test
	public void testZUnionStoreAggWeights() {
		doReturn(Collections.singletonList(Collections.singletonList(5L))).when(nativeConnection).closePipeline();
		super.testZUnionStoreAggWeights();
	}

	@Test
	public void testZUnionStoreBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(5L))).when(nativeConnection).closePipeline();
		super.testZUnionStoreBytes();
	}

	@Test
	public void testZUnionStore() {
		doReturn(Collections.singletonList(Collections.singletonList(5L))).when(nativeConnection).closePipeline();
		super.testZUnionStore();
	}

	@Test
	public void testPExpireBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(true))).when(nativeConnection)
				.closePipeline();
		super.testPExpireBytes();
	}

	@Test
	public void testPExpire() {
		doReturn(Collections.singletonList(Collections.singletonList(true))).when(nativeConnection)
				.closePipeline();
		super.testPExpire();
	}

	@Test
	public void testPExpireAtBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(true))).when(nativeConnection)
				.closePipeline();
		super.testPExpireAtBytes();
	}

	@Test
	public void testPExpireAt() {
		doReturn(Collections.singletonList(Collections.singletonList(true))).when(nativeConnection)
				.closePipeline();
		super.testPExpireAt();
	}

	@Test
	public void testPTtlBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(5L))).when(nativeConnection).closePipeline();
		super.testPTtlBytes();
	}

	@Test
	public void testPTtl() {
		doReturn(Collections.singletonList(Collections.singletonList(5L))).when(nativeConnection).closePipeline();
		super.testPTtl();
	}

	@Test
	public void testDump() {
		doReturn(Collections.singletonList(Arrays.asList(new Object[] { barBytes }))).when(nativeConnection)
				.closePipeline();
		super.testDump();
	}

	@Test
	public void testScriptLoadBytes() {
		doReturn(Collections.singletonList(Collections.singletonList("foo"))).when(nativeConnection)
				.closePipeline();
		super.testScriptLoadBytes();
	}

	@Test
	public void testScriptLoad() {
		doReturn(Collections.singletonList(Collections.singletonList("foo"))).when(nativeConnection)
				.closePipeline();
		super.testScriptLoad();
	}

	@Test
	public void testScriptExists() {
		List<Boolean> results = Collections.singletonList(true);
		doReturn(Collections.singletonList(Collections.singletonList(results))).when(nativeConnection)
				.closePipeline();
		super.testScriptExists();
	}

	@Test
	public void testEvalBytes() {
		doReturn(Collections.singletonList(Collections.singletonList("foo"))).when(nativeConnection)
				.closePipeline();
		super.testEvalBytes();
	}

	@Test
	public void testEval() {
		doReturn(Collections.singletonList(Collections.singletonList("foo"))).when(nativeConnection)
				.closePipeline();
		super.testEval();
	}

	@Test
	public void testEvalShaBytes() {
		doReturn(Collections.singletonList(Collections.singletonList("foo"))).when(nativeConnection)
				.closePipeline();
		super.testEvalShaBytes();
	}

	@Test
	public void testEvalSha() {
		doReturn(Collections.singletonList(Collections.singletonList("foo"))).when(nativeConnection)
				.closePipeline();
		super.testEvalSha();
	}

	@Test
	public void testExecute() {
		doReturn(Collections.singletonList(Collections.singletonList("foo"))).when(nativeConnection)
				.closePipeline();
		super.testExecute();
	}

	@Test
	public void testExecuteByteArgs() {
		doReturn(Collections.singletonList(Collections.singletonList("foo"))).when(nativeConnection)
				.closePipeline();
		super.testExecuteByteArgs();
	}

	@Test
	public void testExecuteStringArgs() {
		doReturn(Collections.singletonList(Collections.singletonList("foo"))).when(nativeConnection)
				.closePipeline();
		super.testExecuteStringArgs();
	}

	@Test
	public void testTxResultsNotSameSizeAsResults() {
		// Only call one method, but return 2 results from nativeConnection.exec()
		// Emulates scenario where user has called some methods directly on the native connection
		// while tx is open
		doReturn(Collections.singletonList(Arrays.asList(barBytes, 3L))).when(nativeConnection)
				.closePipeline();
		doReturn(barBytes).when(nativeConnection).get(fooBytes);
		connection.get(foo);
		verifyResults(Arrays.asList(barBytes, 3L));
	}

	@Test
	void testTwoTxs() {
		doReturn(Arrays
				.asList(Arrays.asList(new Object[] { barBytes }), Arrays.asList(new Object[] { fooBytes })))
						.when(nativeConnection).closePipeline();
		connection.get(foo);
		connection.exec();
		connection.get(bar);
		connection.exec();
		List<Object> results = connection.closePipeline();
		assertThat(results).isEqualTo(
				Arrays.asList(Collections.singletonList(bar), Collections.singletonList(foo)));
	}

	@Test // DATAREDIS-438
	public void testGeoAddBytes() {

		doReturn(Collections.singletonList(Collections.singletonList(1L))).when(nativeConnection).closePipeline();
		super.testGeoAddBytes();
	}

	@Test // DATAREDIS-438
	public void testGeoAdd() {

		doReturn(Collections.singletonList(Collections.singletonList(1L))).when(nativeConnection).closePipeline();
		super.testGeoAddBytes();
	}

	@Test // DATAREDIS-438
	public void testGeoAddWithGeoLocationBytes() {

		doReturn(Collections.singletonList(Collections.singletonList(1L))).when(nativeConnection).closePipeline();
		super.testGeoAddWithGeoLocationBytes();
	}

	@Test // DATAREDIS-438
	public void testGeoAddWithGeoLocation() {

		doReturn(Collections.singletonList(Collections.singletonList(1L))).when(nativeConnection).closePipeline();
		super.testGeoAddWithGeoLocation();
	}

	@Test // DATAREDIS-438
	public void testGeoAddCoordinateMapBytes() {

		doReturn(Collections.singletonList(Collections.singletonList(1L))).when(nativeConnection).closePipeline();
		super.testGeoAddCoordinateMapBytes();
	}

	@Test // DATAREDIS-438
	public void testGeoAddCoordinateMap() {

		doReturn(Collections.singletonList(Collections.singletonList(1L))).when(nativeConnection).closePipeline();
		super.testGeoAddCoordinateMap();
	}

	@Test // DATAREDIS-438
	public void testGeoAddWithIterableOfGeoLocationBytes() {

		doReturn(Collections.singletonList(Collections.singletonList(1L))).when(nativeConnection).closePipeline();
		super.testGeoAddWithIterableOfGeoLocationBytes();
	}

	@Test // DATAREDIS-438
	public void testGeoAddWithIterableOfGeoLocation() {

		doReturn(Collections.singletonList(Collections.singletonList(1L))).when(nativeConnection).closePipeline();
		super.testGeoAddWithIterableOfGeoLocation();
	}

	@Test // DATAREDIS-438
	public void testGeoDistBytes() {

		doReturn(Collections.singletonList(Collections.singletonList(new Distance(102121.12d, DistanceUnit.METERS))))
				.when(nativeConnection)
				.closePipeline();
		super.testGeoDistBytes();
	}

	@Test // DATAREDIS-438
	public void testGeoDist() {

		doReturn(Collections.singletonList(Collections.singletonList(new Distance(102121.12d, DistanceUnit.METERS))))
				.when(nativeConnection)
				.closePipeline();
		super.testGeoDist();
	}

	@Test // DATAREDIS-438
	public void testGeoHashBytes() {

		doReturn(Collections.singletonList(Collections.singletonList(Collections.singletonList(bar))))
				.when(nativeConnection).closePipeline();
		super.testGeoHashBytes();
	}

	@Test // DATAREDIS-438
	public void testGeoHash() {

		doReturn(Collections.singletonList(Collections.singletonList(Collections.singletonList(bar))))
				.when(nativeConnection).closePipeline();
		super.testGeoHash();
	}

	@Test // DATAREDIS-438
	public void testGeoPosBytes() {

		doReturn(Collections.singletonList(Collections.singletonList(points))).when(nativeConnection).closePipeline();
		super.testGeoPosBytes();
	}

	@Test // DATAREDIS-438
	public void testGeoPos() {

		doReturn(Collections.singletonList(Collections.singletonList(points))).when(nativeConnection).closePipeline();
		super.testGeoPos();
	}

	@Test // DATAREDIS-438
	public void testGeoRadiusWithoutParamBytes() {

		doReturn(Collections.singletonList(Collections.singletonList(geoResults))).when(nativeConnection).closePipeline();
		super.testGeoRadiusWithoutParamBytes();
	}

	@Test // DATAREDIS-438
	public void testGeoRadiusWithoutParam() {

		doReturn(Collections.singletonList(Collections.singletonList(geoResults))).when(nativeConnection).closePipeline();
		super.testGeoRadiusWithoutParam();
	}

	@Test // DATAREDIS-438
	public void testGeoRadiusWithDistBytes() {

		doReturn(Collections.singletonList(Collections.singletonList(geoResults))).when(nativeConnection).closePipeline();
		super.testGeoRadiusWithDistBytes();
	}

	@Test // DATAREDIS-438
	public void testGeoRadiusWithDist() {

		doReturn(Collections.singletonList(Collections.singletonList(geoResults))).when(nativeConnection).closePipeline();
		super.testGeoRadiusWithDist();
	}

	@Test // DATAREDIS-438
	public void testGeoRadiusWithCoordAndDescBytes() {

		doReturn(Collections.singletonList(Collections.singletonList(geoResults))).when(nativeConnection).closePipeline();
		super.testGeoRadiusWithCoordAndDescBytes();
	}

	@Test // DATAREDIS-438
	public void testGeoRadiusWithCoordAndDesc() {

		doReturn(Collections.singletonList(Collections.singletonList(geoResults))).when(nativeConnection).closePipeline();
		super.testGeoRadiusWithCoordAndDesc();
	}

	@Test // DATAREDIS-438
	public void testGeoRadiusByMemberWithoutParamBytes() {

		doReturn(Collections.singletonList(Collections.singletonList(geoResults))).when(nativeConnection).closePipeline();
		super.testGeoRadiusByMemberWithoutParamBytes();
	}

	@Test // DATAREDIS-438
	public void testGeoRadiusByMemberWithoutParam() {

		doReturn(Collections.singletonList(Collections.singletonList(geoResults))).when(nativeConnection).closePipeline();
		super.testGeoRadiusByMemberWithoutParam();
	}

	@Test // DATAREDIS-438
	public void testGeoRadiusByMemberWithDistAndAscBytes() {

		doReturn(Collections.singletonList(Collections.singletonList(geoResults))).when(nativeConnection).closePipeline();
		super.testGeoRadiusByMemberWithDistAndAscBytes();
	}

	@Test // DATAREDIS-438
	public void testGeoRadiusByMemberWithDistAndAsc() {

		doReturn(Collections.singletonList(Collections.singletonList(geoResults))).when(nativeConnection).closePipeline();
		super.testGeoRadiusByMemberWithDistAndAsc();
	}

	@Test // DATAREDIS-438
	public void testGeoRadiusByMemberWithCoordAndCountBytes() {

		doReturn(Collections.singletonList(Collections.singletonList(geoResults))).when(nativeConnection).closePipeline();
		super.testGeoRadiusByMemberWithCoordAndCountBytes();
	}

	@Test // DATAREDIS-438
	public void testGeoRadiusByMemberWithCoordAndCount() {

		doReturn(Collections.singletonList(Collections.singletonList(geoResults))).when(nativeConnection).closePipeline();
		super.testGeoRadiusByMemberWithCoordAndCount();
	}

	@Test // DATAREDIS-864
	public void xAckShouldDelegateAndConvertCorrectly() {

		doReturn(Collections.singletonList(Collections.singletonList(1L))).when(nativeConnection).closePipeline();
		super.xAckShouldDelegateAndConvertCorrectly();
	}

	@Override // DATAREDIS-864
	public void xAddShouldAppendRecordCorrectly() {

		doReturn(Collections.singletonList(Collections.singletonList(RecordId.of("1-1")))).when(nativeConnection)
				.closePipeline();
		super.xAddShouldAppendRecordCorrectly();
	}

	@Test // DATAREDIS-864
	public void xDelShouldDelegateAndConvertCorrectly() {

		doReturn(Collections.singletonList(Collections.singletonList(1L))).when(nativeConnection).closePipeline();
		super.xAckShouldDelegateAndConvertCorrectly();
	}

	@Test // DATAREDIS-864
	public void xGroupCreateShouldDelegateAndConvertCorrectly() {

		doReturn(Collections.singletonList(Collections.singletonList("OK"))).when(nativeConnection).closePipeline();
		super.xGroupCreateShouldDelegateAndConvertCorrectly();
	}

	@Test // DATAREDIS-864
	public void xGroupDelConsumerShouldDelegateAndConvertCorrectly() {

		doReturn(Collections.singletonList(Collections.singletonList(Boolean.TRUE))).when(nativeConnection).closePipeline();
		super.xGroupDelConsumerShouldDelegateAndConvertCorrectly();
	}

	@Test // DATAREDIS-864
	public void xLenShouldDelegateAndConvertCorrectly() {

		doReturn(Collections.singletonList(Collections.singletonList(1L))).when(nativeConnection).closePipeline();
		super.xLenShouldDelegateAndConvertCorrectly();
	}

	@Test // DATAREDIS-864
	public void xGroupDestroyShouldDelegateAndConvertCorrectly() {

		doReturn(Collections.singletonList(Collections.singletonList(Boolean.TRUE))).when(nativeConnection).closePipeline();
		super.xGroupDestroyShouldDelegateAndConvertCorrectly();
	}

	@Test // DATAREDIS-864
	public void xRangeShouldDelegateAndConvertCorrectly() {

		doReturn(Collections.singletonList(Collections.singletonList(
				Collections.singletonList(StreamRecords.newRecord().in(bar2Bytes).withId("stream-1").ofBytes(bytesMap)))))
						.when(nativeConnection).closePipeline();
		super.xRangeShouldDelegateAndConvertCorrectly();
	}

	@Test // DATAREDIS-864
	public void xReadShouldDelegateAndConvertCorrectly() {

		doReturn(Collections.singletonList(Collections.singletonList(
				Collections.singletonList(StreamRecords.newRecord().in(bar2Bytes).withId("stream-1").ofBytes(bytesMap)))))
						.when(nativeConnection).closePipeline();
		super.xReadShouldDelegateAndConvertCorrectly();
	}

	@Test // DATAREDIS-864
	public void xReadGroupShouldDelegateAndConvertCorrectly() {

		doReturn(Collections.singletonList(Collections.singletonList(
				Collections.singletonList(StreamRecords.newRecord().in(bar2Bytes).withId("stream-1").ofBytes(bytesMap)))))
						.when(nativeConnection).closePipeline();
		super.xReadGroupShouldDelegateAndConvertCorrectly();
	}

	@Test // DATAREDIS-864
	public void xRevRangeShouldDelegateAndConvertCorrectly() {

		doReturn(Collections.singletonList(Collections.singletonList(
				Collections.singletonList(StreamRecords.newRecord().in(bar2Bytes).withId("stream-1").ofBytes(bytesMap)))))
						.when(nativeConnection).closePipeline();
		super.xRevRangeShouldDelegateAndConvertCorrectly();
	}

	@Test // DATAREDIS-864
	public void xTrimShouldDelegateAndConvertCorrectly() {

		doReturn(Collections.singletonList(Collections.singletonList(1L))).when(nativeConnection).closePipeline();
		super.xTrimShouldDelegateAndConvertCorrectly();
	}

	@Test
	public void xTrimApproximateShouldDelegateAndConvertCorrectly() {

		doReturn(Collections.singletonList(Collections.singletonList(1L))).when(nativeConnection).closePipeline();
		super.xTrimApproximateShouldDelegateAndConvertCorrectly();
	}

	@SuppressWarnings("unchecked")
	protected List<Object> getResults() {
		connection.exec();
		return (List<Object>) connection.closePipeline().get(0);
	}
}
