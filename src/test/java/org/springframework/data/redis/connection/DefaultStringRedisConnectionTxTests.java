/*
 * Copyright 2013-2021 the original author or authors.
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

/**
 * @author Jennifer Hickey
 * @author Christoph Strobl
 * @author Ninad Divadkar
 */
public class DefaultStringRedisConnectionTxTests extends DefaultStringRedisConnectionTests {

	@BeforeEach
	public void setUp() {
		super.setUp();
		connection.setDeserializePipelineAndTxResults(true);
		when(nativeConnection.isQueueing()).thenReturn(true);
	}

	@Test
	public void testAppend() {
		doReturn(Collections.singletonList(1L)).when(nativeConnection).exec();
		super.testAppend();
	}

	@Test
	public void testAppendBytes() {
		doReturn(Collections.singletonList(1L)).when(nativeConnection).exec();
		super.testAppendBytes();
	}

	@Test
	public void testBlPopBytes() {
		doReturn(Collections.singletonList(bytesList)).when(nativeConnection).exec();
		super.testBlPopBytes();
	}

	@Test
	public void testBlPop() {
		doReturn(Collections.singletonList(bytesList)).when(nativeConnection).exec();
		super.testBlPop();
	}

	@Test
	public void testBrPopBytes() {
		doReturn(Collections.singletonList(bytesList)).when(nativeConnection).exec();
		super.testBrPopBytes();
	}

	@Test
	public void testBrPop() {
		doReturn(Collections.singletonList(bytesList)).when(nativeConnection).exec();
		super.testBrPop();
	}

	@Test
	public void testBrPopLPushBytes() {
		doReturn(Arrays.asList(new Object[] { barBytes })).when(nativeConnection).exec();
		super.testBrPopLPushBytes();
	}

	@Test
	public void testBrPopLPush() {
		doReturn(Arrays.asList(new Object[] { barBytes })).when(nativeConnection).exec();
		super.testBrPopLPush();
	}

	@Test
	public void testCopy() {
		doReturn(Collections.singletonList(Boolean.TRUE)).when(nativeConnection).exec();
		super.testCopy();
	}

	@Test
	public void testDbSize() {
		doReturn(Collections.singletonList(3L)).when(nativeConnection).exec();
		super.testDbSize();
	}

	@Test
	public void testDecrBytes() {
		doReturn(Collections.singletonList(3L)).when(nativeConnection).exec();
		super.testDecrBytes();
	}

	@Test
	public void testDecr() {
		doReturn(Collections.singletonList(3L)).when(nativeConnection).exec();
		super.testDecr();
	}

	@Test
	public void testDecrByBytes() {
		doReturn(Collections.singletonList(3L)).when(nativeConnection).exec();
		super.testDecrByBytes();
	}

	@Test
	public void testDecrBy() {
		doReturn(Collections.singletonList(3L)).when(nativeConnection).exec();
		super.testDecrBy();
	}

	@Test
	public void testDelBytes() {
		doReturn(Collections.singletonList(1L)).when(nativeConnection).exec();
		super.testDelBytes();
	}

	@Test
	public void testDel() {
		doReturn(Collections.singletonList(1L)).when(nativeConnection).exec();
		super.testDel();
	}

	@Test
	public void testEchoBytes() {
		doReturn(Arrays.asList(new Object[] { barBytes })).when(nativeConnection).exec();
		super.testEchoBytes();
	}

	@Test
	public void testEcho() {
		doReturn(Arrays.asList(new Object[] { barBytes })).when(nativeConnection).exec();
		super.testEcho();
	}

	@Test
	public void testExistsBytes() {
		doReturn(Collections.singletonList(true)).when(nativeConnection).exec();
		super.testExistsBytes();
	}

	@Test
	public void testExists() {
		doReturn(Collections.singletonList(true)).when(nativeConnection).exec();
		super.testExists();
	}

	@Test
	public void testExpireBytes() {
		doReturn(Collections.singletonList(true)).when(nativeConnection).exec();
		super.testExpireBytes();
	}

	@Test
	public void testExpire() {
		doReturn(Collections.singletonList(true)).when(nativeConnection).exec();
		super.testExpire();
	}

	@Test
	public void testExpireAtBytes() {
		doReturn(Collections.singletonList(true)).when(nativeConnection).exec();
		super.testExpireAtBytes();
	}

	@Test
	public void testExpireAt() {
		doReturn(Collections.singletonList(true)).when(nativeConnection).exec();
		super.testExpireAt();
	}

	@Test
	public void testGetBytes() {
		doReturn(Arrays.asList(new Object[] { barBytes })).when(nativeConnection).exec();
		super.testGetBytes();
	}

	@Test
	public void testGet() {
		doReturn(Arrays.asList(new Object[] { barBytes })).when(nativeConnection).exec();
		super.testGet();
	}

	@Test
	public void testGetBitBytes() {
		doReturn(Collections.singletonList(true)).when(nativeConnection).exec();
		super.testGetBitBytes();
	}

	@Test
	public void testGetBit() {
		doReturn(Collections.singletonList(true)).when(nativeConnection).exec();
		super.testGetBit();
	}

	@Test // DATAREDIS-661
	public void testGetConfig() {

		Properties results = new Properties();
		results.put("foo", "bar");

		doReturn(Collections.singletonList(results)).when(nativeConnection).exec();
		super.testGetConfig();
	}

	@Test
	public void testGetNativeConnection() {
		doReturn(Collections.singletonList("foo")).when(nativeConnection).exec();
		super.testGetNativeConnection();
	}

	@Test
	public void testGetRangeBytes() {
		doReturn(Arrays.asList(new Object[] { barBytes })).when(nativeConnection).exec();
		super.testGetRangeBytes();
	}

	@Test
	public void testGetRange() {
		doReturn(Arrays.asList(new Object[] { barBytes })).when(nativeConnection).exec();
		super.testGetRange();
	}

	@Test
	public void testGetSetBytes() {
		doReturn(Arrays.asList(new Object[] { barBytes })).when(nativeConnection).exec();
		super.testGetSetBytes();
	}

	@Test
	public void testGetSet() {
		doReturn(Arrays.asList(new Object[] { barBytes })).when(nativeConnection).exec();
		super.testGetSet();
	}

	@Test
	public void testHDelBytes() {
		doReturn(Collections.singletonList(1L)).when(nativeConnection).exec();
		super.testHDelBytes();
	}

	@Test
	public void testHDel() {
		doReturn(Collections.singletonList(1L)).when(nativeConnection).exec();
		super.testHDel();
	}

	@Test
	public void testHExistsBytes() {
		doReturn(Collections.singletonList(true)).when(nativeConnection).exec();
		super.testHExistsBytes();
	}

	@Test
	public void testHExists() {
		doReturn(Collections.singletonList(true)).when(nativeConnection).exec();
		super.testHExists();
	}

	@Test
	public void testHGetBytes() {
		doReturn(Arrays.asList(new Object[] { barBytes })).when(nativeConnection).exec();
		super.testHGetBytes();
	}

	@Test
	public void testHGet() {
		doReturn(Arrays.asList(new Object[] { barBytes })).when(nativeConnection).exec();
		super.testHGet();
	}

	@Test
	public void testHGetAllBytes() {
		doReturn(Collections.singletonList(bytesMap)).when(nativeConnection).exec();
		super.testHGetAllBytes();
	}

	@Test
	public void testHGetAll() {
		doReturn(Collections.singletonList(bytesMap)).when(nativeConnection).exec();
		super.testHGetAll();
	}

	@Test
	public void testHIncrByBytes() {
		doReturn(Collections.singletonList(3L)).when(nativeConnection).exec();
		super.testHIncrByBytes();
	}

	@Test
	public void testHIncrBy() {
		doReturn(Collections.singletonList(3L)).when(nativeConnection).exec();
		super.testHIncrBy();
	}

	@Test
	public void testHIncrByDoubleBytes() {
		doReturn(Collections.singletonList(3d)).when(nativeConnection).exec();
		super.testHIncrByDoubleBytes();
	}

	@Test
	public void testHIncrByDouble() {
		doReturn(Collections.singletonList(3d)).when(nativeConnection).exec();
		super.testHIncrByDouble();
	}

	@Test
	public void testHKeysBytes() {
		doReturn(Collections.singletonList(bytesSet)).when(nativeConnection).exec();
		super.testHKeysBytes();
	}

	@Test
	public void testHKeys() {
		doReturn(Collections.singletonList(bytesSet)).when(nativeConnection).exec();
		super.testHKeys();
	}

	@Test
	public void testHLenBytes() {
		doReturn(Collections.singletonList(3L)).when(nativeConnection).exec();
		super.testHLenBytes();
	}

	@Test
	public void testHLen() {
		doReturn(Collections.singletonList(3L)).when(nativeConnection).exec();
		super.testHLen();
	}

	@Test
	public void testHMGetBytes() {
		doReturn(Collections.singletonList(bytesList)).when(nativeConnection).exec();
		super.testHMGetBytes();
	}

	@Test
	public void testHMGet() {
		doReturn(Collections.singletonList(bytesList)).when(nativeConnection).exec();
		super.testHMGet();
	}

	@Test
	public void testHSetBytes() {
		doReturn(Collections.singletonList(true)).when(nativeConnection).exec();
		super.testHSetBytes();
	}

	@Test
	public void testHSet() {
		doReturn(Collections.singletonList(true)).when(nativeConnection).exec();
		super.testHSet();
	}

	@Test
	public void testHSetNXBytes() {
		doReturn(Collections.singletonList(true)).when(nativeConnection).exec();
		super.testHSetNXBytes();
	}

	@Test
	public void testHSetNX() {
		doReturn(Collections.singletonList(true)).when(nativeConnection).exec();
		super.testHSetNX();
	}

	@Test
	public void testHValsBytes() {
		doReturn(Collections.singletonList(bytesList)).when(nativeConnection).exec();
		super.testHValsBytes();
	}

	@Test
	public void testHVals() {
		doReturn(Collections.singletonList(bytesList)).when(nativeConnection).exec();
		super.testHVals();
	}

	@Test
	public void testIncrBytes() {
		doReturn(Collections.singletonList(2L)).when(nativeConnection).exec();
		super.testIncrBytes();
	}

	@Test
	public void testIncr() {
		doReturn(Collections.singletonList(2L)).when(nativeConnection).exec();
		super.testIncr();
	}

	@Test
	public void testIncrByBytes() {
		doReturn(Collections.singletonList(2L)).when(nativeConnection).exec();
		super.testIncrByBytes();
	}

	@Test
	public void testIncrBy() {
		doReturn(Collections.singletonList(2L)).when(nativeConnection).exec();
		super.testIncrBy();
	}

	@Test
	public void testIncrByDoubleBytes() {
		doReturn(Collections.singletonList(2d)).when(nativeConnection).exec();
		super.testIncrByDoubleBytes();
	}

	@Test
	public void testIncrByDouble() {
		doReturn(Collections.singletonList(2d)).when(nativeConnection).exec();
		super.testIncrByDouble();
	}

	@Test
	public void testInfo() {
		Properties props = new Properties();
		props.put("foo", "bar");
		doReturn(Collections.singletonList(props)).when(nativeConnection).exec();
		super.testInfo();
	}

	@Test
	public void testInfoBySection() {
		Properties props = new Properties();
		props.put("foo", "bar");
		doReturn(Collections.singletonList(props)).when(nativeConnection).exec();
		super.testInfoBySection();
	}

	@Test
	public void testKeysBytes() {
		doReturn(Collections.singletonList(bytesSet)).when(nativeConnection).exec();
		super.testKeysBytes();
	}

	@Test
	public void testKeys() {
		doReturn(Collections.singletonList(bytesSet)).when(nativeConnection).exec();
		super.testKeys();
	}

	@Test
	public void testLastSave() {
		doReturn(Collections.singletonList(6L)).when(nativeConnection).exec();
		super.testLastSave();
	}

	@Test
	public void testLIndexBytes() {
		doReturn(Arrays.asList(new Object[] { barBytes })).when(nativeConnection).exec();
		super.testLIndexBytes();
	}

	@Test
	public void testLIndex() {
		doReturn(Arrays.asList(new Object[] { barBytes })).when(nativeConnection).exec();
		super.testLIndex();
	}

	@Test
	public void testLInsertBytes() {
		doReturn(Collections.singletonList(8L)).when(nativeConnection).exec();
		super.testLInsertBytes();
	}

	@Test
	public void testLInsert() {
		doReturn(Collections.singletonList(8L)).when(nativeConnection).exec();
		super.testLInsert();
	}

	@Test
	public void testLLenBytes() {
		doReturn(Collections.singletonList(8L)).when(nativeConnection).exec();
		super.testLLenBytes();
	}

	@Test
	public void testLLen() {
		doReturn(Collections.singletonList(8L)).when(nativeConnection).exec();
		super.testLLen();
	}

	@Test
	public void testLPopBytes() {
		doReturn(Arrays.asList(new Object[] { barBytes })).when(nativeConnection).exec();
		actual.add(connection.lPop(fooBytes));
		verifyResults(Arrays.asList(new Object[] { barBytes }));
	}

	@Test
	public void testLPop() {
		doReturn(Arrays.asList(new Object[] { barBytes })).when(nativeConnection).exec();
		super.testLPop();
	}

	@Test // GH-1987
	public void testLPopCountBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(barBytes))).when(nativeConnection).exec();
		super.testLPopCountBytes();
	}

	@Test // GH-1987
	public void testLPopCount() {
		doReturn(Collections.singletonList(Collections.singletonList(barBytes))).when(nativeConnection).exec();
		super.testLPopCount();
	}

	@Test
	public void testLPushBytes() {
		doReturn(Collections.singletonList(8L)).when(nativeConnection).exec();
		super.testLPushBytes();
	}

	@Test
	public void testLPush() {
		doReturn(Collections.singletonList(8L)).when(nativeConnection).exec();
		super.testLPush();
	}

	@Test
	public void testLPushXBytes() {
		doReturn(Collections.singletonList(8L)).when(nativeConnection).exec();
		super.testLPushXBytes();
	}

	@Test
	public void testLPushX() {
		doReturn(Collections.singletonList(8L)).when(nativeConnection).exec();
		super.testLPushX();
	}

	@Test
	public void testLRangeBytes() {
		doReturn(Collections.singletonList(bytesList)).when(nativeConnection).exec();
		super.testLRangeBytes();
	}

	@Test
	public void testLRange() {
		doReturn(Collections.singletonList(bytesList)).when(nativeConnection).exec();
		super.testLRange();
	}

	@Test
	public void testLRemBytes() {
		doReturn(Collections.singletonList(8L)).when(nativeConnection).exec();
		super.testLRemBytes();
	}

	@Test
	public void testLRem() {
		doReturn(Collections.singletonList(8L)).when(nativeConnection).exec();
		super.testLRem();
	}

	@Test
	public void testMGetBytes() {
		doReturn(Collections.singletonList(bytesList)).when(nativeConnection).exec();
		super.testMGetBytes();
	}

	@Test
	public void testMGet() {
		doReturn(Collections.singletonList(bytesList)).when(nativeConnection).exec();
		super.testMGet();
	}

	@Test
	public void testMSetNXBytes() {
		doReturn(Collections.singletonList(true)).when(nativeConnection).exec();
		super.testMSetNXBytes();
	}

	@Test
	public void testMSetNXString() {
		doReturn(Collections.singletonList(true)).when(nativeConnection).exec();
		super.testMSetNXString();
	}

	@Test
	public void testPersistBytes() {
		doReturn(Collections.singletonList(true)).when(nativeConnection).exec();
		super.testPersistBytes();
	}

	@Test
	public void testPersist() {
		doReturn(Collections.singletonList(true)).when(nativeConnection).exec();
		super.testPersist();
	}

	@Test
	public void testMoveBytes() {
		doReturn(Collections.singletonList(true)).when(nativeConnection).exec();
		super.testMoveBytes();
	}

	@Test
	public void testMove() {
		doReturn(Collections.singletonList(true)).when(nativeConnection).exec();
		super.testMove();
	}

	@Test
	public void testPing() {
		doReturn(Collections.singletonList("pong")).when(nativeConnection).exec();
		super.testPing();
	}

	@Test
	public void testPublishBytes() {
		doReturn(Collections.singletonList(2L)).when(nativeConnection).exec();
		super.testPublishBytes();
	}

	@Test
	public void testPublish() {
		doReturn(Collections.singletonList(2L)).when(nativeConnection).exec();
		super.testPublish();
	}

	@Test
	public void testRandomKey() {
		doReturn(Arrays.asList(new Object[] { fooBytes })).when(nativeConnection).exec();
		super.testRandomKey();
	}

	@Test
	public void testRenameNXBytes() {
		doReturn(Collections.singletonList(true)).when(nativeConnection).exec();
		super.testRenameNXBytes();
	}

	@Test
	public void testRenameNX() {
		doReturn(Collections.singletonList(true)).when(nativeConnection).exec();
		super.testRenameNX();
	}

	@Test
	public void testRPopBytes() {
		doReturn(Arrays.asList(new Object[] { barBytes })).when(nativeConnection).exec();
		super.testRPopBytes();
	}

	@Test
	public void testRPop() {
		doReturn(Arrays.asList(new Object[] { barBytes })).when(nativeConnection).exec();
		super.testRPop();
	}

	@Test // GH-1987
	public void testRPopCountBytes() {
		doReturn(Collections.singletonList(Collections.singletonList(barBytes))).when(nativeConnection).exec();
		super.testRPopCountBytes();
	}

	@Test // GH-1987
	public void testRPopCount() {
		doReturn(Collections.singletonList(Collections.singletonList(barBytes))).when(nativeConnection).exec();
		super.testRPopCount();
	}

	@Test
	public void testRPopLPushBytes() {
		doReturn(Arrays.asList(new Object[] { barBytes })).when(nativeConnection).exec();
		super.testRPopLPushBytes();
	}

	@Test
	public void testRPopLPush() {
		doReturn(Arrays.asList(new Object[] { barBytes })).when(nativeConnection).exec();
		super.testRPopLPush();
	}

	@Test
	public void testRPushBytes() {
		doReturn(Collections.singletonList(4L)).when(nativeConnection).exec();
		super.testRPushBytes();
	}

	@Test
	public void testRPush() {
		doReturn(Collections.singletonList(4L)).when(nativeConnection).exec();
		super.testRPush();
	}

	@Test
	public void testRPushXBytes() {
		doReturn(Collections.singletonList(4L)).when(nativeConnection).exec();
		super.testRPushXBytes();
	}

	@Test
	public void testRPushX() {
		doReturn(Collections.singletonList(4L)).when(nativeConnection).exec();
		super.testRPushX();
	}

	@Test
	public void testSAddBytes() {
		doReturn(Collections.singletonList(1L)).when(nativeConnection).exec();
		super.testSAddBytes();
	}

	@Test
	public void testSAdd() {
		doReturn(Collections.singletonList(1L)).when(nativeConnection).exec();
		super.testSAdd();
	}

	@Test
	public void testSCardBytes() {
		doReturn(Collections.singletonList(4L)).when(nativeConnection).exec();
		super.testSCardBytes();
	}

	@Test
	public void testSCard() {
		doReturn(Collections.singletonList(4L)).when(nativeConnection).exec();
		super.testSCard();
	}

	@Test
	public void testSDiffBytes() {
		doReturn(Collections.singletonList(bytesSet)).when(nativeConnection).exec();
		super.testSDiffBytes();
	}

	@Test
	public void testSDiff() {
		doReturn(Collections.singletonList(bytesSet)).when(nativeConnection).exec();
		super.testSDiff();
	}

	@Test
	public void testSDiffStoreBytes() {
		doReturn(Collections.singletonList(3L)).when(nativeConnection).exec();
		super.testSDiffStoreBytes();
	}

	@Test
	public void testSDiffStore() {
		doReturn(Collections.singletonList(3L)).when(nativeConnection).exec();
		super.testSDiffStore();
	}

	@Test
	public void testSetNXBytes() {
		doReturn(Collections.singletonList(true)).when(nativeConnection).exec();
		super.testSetNXBytes();
	}

	@Test
	public void testSetNX() {
		doReturn(Collections.singletonList(true)).when(nativeConnection).exec();
		super.testSetNX();
	}

	@Test
	public void testSInterBytes() {
		doReturn(Collections.singletonList(bytesSet)).when(nativeConnection).exec();
		super.testSInterBytes();
	}

	@Test
	public void testSInter() {
		doReturn(Collections.singletonList(bytesSet)).when(nativeConnection).exec();
		super.testSInter();
	}

	@Test
	public void testSInterStoreBytes() {
		doReturn(Collections.singletonList(3L)).when(nativeConnection).exec();
		super.testSInterStoreBytes();
	}

	@Test
	public void testSInterStore() {
		doReturn(Collections.singletonList(3L)).when(nativeConnection).exec();
		super.testSInterStore();
	}

	@Test
	public void testSIsMemberBytes() {
		doReturn(Collections.singletonList(true)).when(nativeConnection).exec();
		super.testSIsMemberBytes();
	}

	@Test
	public void testSIsMember() {
		doReturn(Collections.singletonList(true)).when(nativeConnection).exec();
		super.testSIsMember();
	}

	@Test
	public void testSMembersBytes() {
		doReturn(Collections.singletonList(bytesSet)).when(nativeConnection).exec();
		super.testSMembersBytes();
	}

	@Test
	public void testSMembers() {
		doReturn(Collections.singletonList(bytesSet)).when(nativeConnection).exec();
		super.testSMembers();
	}

	@Test
	public void testSMoveBytes() {
		doReturn(Collections.singletonList(true)).when(nativeConnection).exec();
		super.testSMoveBytes();
	}

	@Test
	public void testSMove() {
		doReturn(Collections.singletonList(true)).when(nativeConnection).exec();
		super.testSMove();
	}

	@Test
	public void testSortStoreBytes() {
		doReturn(Collections.singletonList(3L)).when(nativeConnection).exec();
		super.testSortStoreBytes();
	}

	@Test
	public void testSortStore() {
		doReturn(Collections.singletonList(3L)).when(nativeConnection).exec();
		super.testSortStore();
	}

	@Test
	public void testSortBytes() {
		doReturn(Collections.singletonList(bytesList)).when(nativeConnection).exec();
		super.testSortBytes();
	}

	@Test
	public void testSort() {
		doReturn(Collections.singletonList(bytesList)).when(nativeConnection).exec();
		super.testSort();
	}

	@Test
	public void testSPopBytes() {
		doReturn(Arrays.asList(new Object[] { barBytes })).when(nativeConnection).exec();
		super.testSPopBytes();
	}

	@Test
	public void testSPop() {
		doReturn(Arrays.asList(new Object[] { barBytes })).when(nativeConnection).exec();
		super.testSPop();
	}

	@Test
	public void testSRandMemberBytes() {
		doReturn(Arrays.asList(new Object[] { barBytes })).when(nativeConnection).exec();
		super.testSRandMemberBytes();
	}

	@Test
	public void testSRandMember() {
		doReturn(Arrays.asList(new Object[] { barBytes })).when(nativeConnection).exec();
		super.testSRandMember();
	}

	@Test
	public void testSRandMemberCountBytes() {
		doReturn(Collections.singletonList(bytesList)).when(nativeConnection).exec();
		super.testSRandMemberCountBytes();
	}

	@Test
	public void testSRandMemberCount() {
		doReturn(Collections.singletonList(bytesList)).when(nativeConnection).exec();
		super.testSRandMemberCount();
	}

	@Test
	public void testSRemBytes() {
		doReturn(Collections.singletonList(1L)).when(nativeConnection).exec();
		super.testSRemBytes();
	}

	@Test
	public void testSRem() {
		doReturn(Collections.singletonList(1L)).when(nativeConnection).exec();
		super.testSRem();
	}

	@Test
	public void testStrLenBytes() {
		doReturn(Collections.singletonList(5L)).when(nativeConnection).exec();
		super.testStrLenBytes();
	}

	@Test
	public void testStrLen() {
		doReturn(Collections.singletonList(5L)).when(nativeConnection).exec();
		super.testStrLen();
	}

	@Test
	public void testBitCountBytes() {
		doReturn(Collections.singletonList(5L)).when(nativeConnection).exec();
		super.testBitCountBytes();
	}

	@Test
	public void testBitCount() {
		doReturn(Collections.singletonList(5L)).when(nativeConnection).exec();
		super.testBitCount();
	}

	@Test
	public void testBitCountRangeBytes() {
		doReturn(Collections.singletonList(5L)).when(nativeConnection).exec();
		super.testBitCountRangeBytes();
	}

	@Test
	public void testBitCountRange() {
		doReturn(Collections.singletonList(5L)).when(nativeConnection).exec();
		super.testBitCountRange();
	}

	@Test
	public void testBitOpBytes() {
		doReturn(Collections.singletonList(5L)).when(nativeConnection).exec();
		super.testBitOpBytes();
	}

	@Test
	public void testBitOp() {
		doReturn(Collections.singletonList(5L)).when(nativeConnection).exec();
		super.testBitOp();
	}

	@Test
	public void testSUnionBytes() {
		doReturn(Collections.singletonList(bytesSet)).when(nativeConnection).exec();
		super.testSUnionBytes();
	}

	@Test
	public void testSUnion() {
		doReturn(Collections.singletonList(bytesSet)).when(nativeConnection).exec();
		super.testSUnion();
	}

	@Test
	public void testSUnionStoreBytes() {
		doReturn(Collections.singletonList(5L)).when(nativeConnection).exec();
		super.testSUnionStoreBytes();
	}

	@Test
	public void testSUnionStore() {
		doReturn(Collections.singletonList(5L)).when(nativeConnection).exec();
		super.testSUnionStore();
	}

	@Test
	public void testTtlBytes() {
		doReturn(Collections.singletonList(5L)).when(nativeConnection).exec();
		super.testTtlBytes();
	}

	@Test
	public void testTtl() {
		doReturn(Collections.singletonList(5L)).when(nativeConnection).exec();
		super.testTtl();
	}

	// DATAREDIS-526
	@Override
	@Test
	public void testTtlWithTimeUnit() {

		doReturn(Collections.singletonList(5L)).when(nativeConnection).exec();
		super.testTtl();
	}

	@Test
	public void testTypeBytes() {
		doReturn(Collections.singletonList(DataType.HASH)).when(nativeConnection).exec();
		super.testTypeBytes();
	}

	@Test
	public void testType() {
		doReturn(Collections.singletonList(DataType.HASH)).when(nativeConnection).exec();
		super.testType();
	}

	@Test
	public void testZAddBytes() {
		doReturn(Collections.singletonList(true)).when(nativeConnection).exec();
		super.testZAddBytes();
	}

	@Test
	public void testZAdd() {
		doReturn(Collections.singletonList(true)).when(nativeConnection).exec();
		super.testZAdd();
	}

	@Test
	public void testZAddMultipleBytes() {
		doReturn(Collections.singletonList(1L)).when(nativeConnection).exec();
		super.testZAddMultipleBytes();
	}

	@Test
	public void testZAddMultiple() {
		doReturn(Collections.singletonList(1L)).when(nativeConnection).exec();
		super.testZAddMultiple();
	}

	@Test
	public void testZCardBytes() {
		doReturn(Collections.singletonList(5L)).when(nativeConnection).exec();
		super.testZCardBytes();
	}

	@Test
	public void testZCard() {
		doReturn(Collections.singletonList(5L)).when(nativeConnection).exec();
		super.testZCard();
	}

	@Test
	public void testZCountBytes() {
		doReturn(Collections.singletonList(5L)).when(nativeConnection).exec();
		super.testZCountBytes();
	}

	@Test
	public void testZCount() {
		doReturn(Collections.singletonList(5L)).when(nativeConnection).exec();
		super.testZCount();
	}

	@Test
	public void testZIncrByBytes() {
		doReturn(Collections.singletonList(3d)).when(nativeConnection).exec();
		super.testZIncrByBytes();
	}

	@Test
	public void testZIncrBy() {
		doReturn(Collections.singletonList(3d)).when(nativeConnection).exec();
		super.testZIncrBy();
	}

	@Test
	public void testZInterStoreAggWeightsBytes() {
		doReturn(Collections.singletonList(5L)).when(nativeConnection).exec();
		super.testZInterStoreAggWeightsBytes();
	}

	@Test
	public void testZInterStoreAggWeights() {
		doReturn(Collections.singletonList(5L)).when(nativeConnection).exec();
		super.testZInterStoreAggWeights();
	}

	@Test
	public void testZInterStoreBytes() {
		doReturn(Collections.singletonList(5L)).when(nativeConnection).exec();
		super.testZInterStoreBytes();
	}

	@Test
	public void testZInterStore() {
		doReturn(Collections.singletonList(5L)).when(nativeConnection).exec();
		super.testZInterStore();
	}

	@Test
	public void testZRangeBytes() {
		doReturn(Collections.singletonList(bytesSet)).when(nativeConnection).exec();
		super.testZRangeBytes();
	}

	@Test
	public void testZRange() {
		doReturn(Collections.singletonList(bytesSet)).when(nativeConnection).exec();
		super.testZRange();
	}

	@Test
	public void testZRangeByScoreOffsetCountBytes() {
		doReturn(Collections.singletonList(bytesSet)).when(nativeConnection).exec();
		super.testZRangeByScoreOffsetCountBytes();
	}

	@Test
	public void testZRangeByScoreOffsetCount() {
		doReturn(Collections.singletonList(bytesSet)).when(nativeConnection).exec();
		super.testZRangeByScoreOffsetCount();
	}

	@Test
	public void testZRangeByScoreBytes() {
		doReturn(Collections.singletonList(bytesSet)).when(nativeConnection).exec();
		super.testZRangeByScoreBytes();
	}

	@Test
	public void testZRangeByScore() {
		doReturn(Collections.singletonList(bytesSet)).when(nativeConnection).exec();
		super.testZRangeByScore();
	}

	@Test
	public void testZRangeByScoreWithScoresOffsetCountBytes() {
		doReturn(Collections.singletonList(tupleSet)).when(nativeConnection).exec();
		super.testZRangeByScoreWithScoresOffsetCountBytes();
	}

	@Test
	public void testZRangeByScoreWithScoresOffsetCount() {
		doReturn(Collections.singletonList(tupleSet)).when(nativeConnection).exec();
		super.testZRangeByScoreWithScoresOffsetCount();
	}

	@Test
	public void testZRangeByScoreWithScoresBytes() {
		doReturn(Collections.singletonList(tupleSet)).when(nativeConnection).exec();
		super.testZRangeByScoreWithScoresBytes();
	}

	@Test
	public void testZRangeByScoreWithScores() {
		doReturn(Collections.singletonList(tupleSet)).when(nativeConnection).exec();
		super.testZRangeByScoreWithScores();
	}

	@Test
	public void testZRangeWithScoresBytes() {
		doReturn(Collections.singletonList(tupleSet)).when(nativeConnection).exec();
		super.testZRangeWithScoresBytes();
	}

	@Test
	public void testZRangeWithScores() {
		doReturn(Collections.singletonList(tupleSet)).when(nativeConnection).exec();
		super.testZRangeWithScores();
	}

	@Test
	public void testZRevRangeByScoreOffsetCountBytes() {
		doReturn(Collections.singletonList(bytesSet)).when(nativeConnection).exec();
		super.testZRevRangeByScoreOffsetCountBytes();
	}

	@Test
	public void testZRevRangeByScoreOffsetCount() {
		doReturn(Collections.singletonList(bytesSet)).when(nativeConnection).exec();
		super.testZRevRangeByScoreOffsetCount();
	}

	@Test
	public void testZRevRangeByScoreBytes() {
		doReturn(Collections.singletonList(bytesSet)).when(nativeConnection).exec();
		super.testZRevRangeByScoreBytes();
	}

	@Test
	public void testZRevRangeByScore() {
		doReturn(Collections.singletonList(bytesSet)).when(nativeConnection).exec();
		super.testZRevRangeByScore();
	}

	@Test
	public void testZRevRangeByScoreWithScoresOffsetCountBytes() {
		doReturn(Collections.singletonList(tupleSet)).when(nativeConnection).exec();
		super.testZRevRangeByScoreWithScoresOffsetCountBytes();
	}

	@Test
	public void testZRevRangeByScoreWithScoresOffsetCount() {
		doReturn(Collections.singletonList(tupleSet)).when(nativeConnection).exec();
		super.testZRevRangeByScoreWithScoresOffsetCount();
	}

	@Test
	public void testZRevRangeByScoreWithScoresBytes() {
		doReturn(Collections.singletonList(tupleSet)).when(nativeConnection).exec();
		super.testZRevRangeByScoreWithScoresBytes();
	}

	@Test
	public void testZRevRangeByScoreWithScores() {
		doReturn(Collections.singletonList(tupleSet)).when(nativeConnection).exec();
		super.testZRevRangeByScoreWithScores();
	}

	@Test
	public void testZRankBytes() {
		doReturn(Collections.singletonList(5L)).when(nativeConnection).exec();
		super.testZRankBytes();
	}

	@Test
	public void testZRank() {
		doReturn(Collections.singletonList(5L)).when(nativeConnection).exec();
		super.testZRank();
	}

	@Test
	public void testZRemBytes() {
		doReturn(Collections.singletonList(1L)).when(nativeConnection).exec();
		super.testZRemBytes();
	}

	@Test
	public void testZRem() {
		doReturn(Collections.singletonList(1L)).when(nativeConnection).exec();
		super.testZRem();
	}

	@Test
	public void testZRemRangeBytes() {
		doReturn(Collections.singletonList(5L)).when(nativeConnection).exec();
		super.testZRemRangeBytes();
	}

	@Test
	public void testZRemRange() {
		doReturn(Collections.singletonList(5L)).when(nativeConnection).exec();
		super.testZRemRange();
	}

	@Test
	public void testZRemRangeByScoreBytes() {
		doReturn(Collections.singletonList(5L)).when(nativeConnection).exec();
		super.testZRemRangeByScoreBytes();
	}

	@Test
	public void testZRemRangeByScore() {
		doReturn(Collections.singletonList(5L)).when(nativeConnection).exec();
		super.testZRemRangeByScore();
	}

	@Test
	public void testZRevRangeBytes() {
		doReturn(Collections.singletonList(bytesSet)).when(nativeConnection).exec();
		super.testZRevRangeBytes();
	}

	@Test
	public void testZRevRange() {
		doReturn(Collections.singletonList(bytesSet)).when(nativeConnection).exec();
		super.testZRevRange();
	}

	@Test
	public void testZRevRangeWithScoresBytes() {
		doReturn(Collections.singletonList(tupleSet)).when(nativeConnection).exec();
		super.testZRevRangeWithScoresBytes();
	}

	@Test
	public void testZRevRangeWithScores() {
		doReturn(Collections.singletonList(tupleSet)).when(nativeConnection).exec();
		super.testZRevRangeWithScores();
	}

	@Test
	public void testZRevRankBytes() {
		doReturn(Collections.singletonList(5L)).when(nativeConnection).exec();
		super.testZRevRankBytes();
	}

	@Test
	public void testZRevRank() {
		doReturn(Collections.singletonList(5L)).when(nativeConnection).exec();
		super.testZRevRank();
	}

	@Test
	public void testZScoreBytes() {
		doReturn(Collections.singletonList(3d)).when(nativeConnection).exec();
		super.testZScoreBytes();
	}

	@Test
	public void testZScore() {
		doReturn(Collections.singletonList(3d)).when(nativeConnection).exec();
		super.testZScore();
	}

	@Test
	public void testZMScore() {
		
		doReturn(Collections.singletonList(Arrays.asList(1d, 3d))).when(nativeConnection).exec();
		super.testZMScore();
	}

	@Test
	public void testZUnionStoreAggWeightsBytes() {
		doReturn(Collections.singletonList(5L)).when(nativeConnection).exec();
		super.testZUnionStoreAggWeightsBytes();
	}

	@Test
	public void testZUnionStoreAggWeights() {
		doReturn(Collections.singletonList(5L)).when(nativeConnection).exec();
		super.testZUnionStoreAggWeights();
	}

	@Test
	public void testZUnionStoreBytes() {
		doReturn(Collections.singletonList(5L)).when(nativeConnection).exec();
		super.testZUnionStoreBytes();
	}

	@Test
	public void testZUnionStore() {
		doReturn(Collections.singletonList(5L)).when(nativeConnection).exec();
		super.testZUnionStore();
	}

	@Test
	public void testPExpireBytes() {
		doReturn(Collections.singletonList(true)).when(nativeConnection).exec();
		super.testPExpireBytes();
	}

	@Test
	public void testPExpire() {
		doReturn(Collections.singletonList(true)).when(nativeConnection).exec();
		super.testPExpire();
	}

	@Test
	public void testPExpireAtBytes() {
		doReturn(Collections.singletonList(true)).when(nativeConnection).exec();
		super.testPExpireAtBytes();
	}

	@Test
	public void testPExpireAt() {
		doReturn(Collections.singletonList(true)).when(nativeConnection).exec();
		super.testPExpireAt();
	}

	@Test
	public void testPTtlBytes() {
		doReturn(Collections.singletonList(5L)).when(nativeConnection).exec();
		super.testPTtlBytes();
	}

	@Test
	public void testPTtl() {
		doReturn(Collections.singletonList(5L)).when(nativeConnection).exec();
		super.testPTtl();
	}

	@Test
	public void testDump() {
		doReturn(Arrays.asList(new Object[] { barBytes })).when(nativeConnection).exec();
		super.testDump();
	}

	@Test
	public void testScriptLoadBytes() {
		doReturn(Collections.singletonList("foo")).when(nativeConnection).exec();
		super.testScriptLoadBytes();
	}

	@Test
	public void testScriptLoad() {
		doReturn(Collections.singletonList("foo")).when(nativeConnection).exec();
		super.testScriptLoad();
	}

	@Test
	public void testScriptExists() {
		List<Boolean> results = Collections.singletonList(true);
		doReturn(Collections.singletonList(results)).when(nativeConnection).exec();
		super.testScriptExists();
	}

	@Test
	public void testEvalBytes() {
		doReturn(Collections.singletonList("foo")).when(nativeConnection).exec();
		super.testEvalBytes();
	}

	@Test
	public void testEval() {
		doReturn(Collections.singletonList("foo")).when(nativeConnection).exec();
		super.testEval();
	}

	@Test
	public void testEvalShaBytes() {
		doReturn(Collections.singletonList("foo")).when(nativeConnection).exec();
		super.testEvalShaBytes();
	}

	@Test
	public void testEvalSha() {
		doReturn(Collections.singletonList("foo")).when(nativeConnection).exec();
		super.testEvalSha();
	}

	@Test
	public void testExecute() {
		doReturn(Collections.singletonList("foo")).when(nativeConnection).exec();
		super.testExecute();
	}

	@Test
	public void testExecuteByteArgs() {
		doReturn(Collections.singletonList("foo")).when(nativeConnection).exec();
		super.testExecuteByteArgs();
	}

	@Test
	public void testExecuteStringArgs() {
		doReturn(Collections.singletonList("foo")).when(nativeConnection).exec();
		super.testExecuteStringArgs();
	}

	@Test
	void testDisablePipelineAndTxDeserialize() {
		connection.setDeserializePipelineAndTxResults(false);
		doReturn(Arrays.asList(new Object[] { barBytes })).when(nativeConnection).exec();
		doReturn(Collections.singletonList(Arrays.asList(new Object[] { barBytes }))).when(nativeConnection)
				.closePipeline();
		doReturn(barBytes).when(nativeConnection).get(fooBytes);
		connection.get(foo);
		verifyResults(Arrays.asList(new Object[] { barBytes }));
	}

	@Test
	public void testTxResultsNotSameSizeAsResults() {
		// Only call one method, but return 2 results from nativeConnection.exec()
		// Emulates scenario where user has called some methods directly on the native connection
		// while tx is open
		doReturn(Arrays.asList(barBytes, 3L)).when(nativeConnection).exec();
		doReturn(barBytes).when(nativeConnection).get(fooBytes);
		connection.get(foo);
		verifyResults(Arrays.asList(barBytes, 3L));
	}

	@Test
	void testDiscard() {
		doReturn(Arrays.asList(new Object[] { fooBytes })).when(nativeConnection).exec();
		doReturn(Collections.singletonList(Arrays.asList(new Object[] { fooBytes }))).when(nativeConnection)
				.closePipeline();
		doReturn(barBytes).when(nativeConnection).get(fooBytes);
		doReturn(fooBytes).when(nativeConnection).get(barBytes);
		connection.get(foo);
		connection.discard();
		connection.get(bar);
		// Converted results of get(bar) should be included
		verifyResults(Collections.singletonList(foo));
	}

	@Test // DATAREDIS-206
	@Override
	public void testTimeIsDelegatedCorrectlyToNativeConnection() {

		doReturn(Collections.singletonList(1L)).when(nativeConnection).exec();
		doReturn(Collections.singletonList(Collections.singletonList(1L))).when(nativeConnection).closePipeline();
		super.testTimeIsDelegatedCorrectlyToNativeConnection();
	}

	@Test // DATAREDIS-438
	public void testGeoAddBytes() {

		doReturn(Collections.singletonList(1L)).when(nativeConnection).exec();
		super.testGeoAddBytes();
	}

	@Test // DATAREDIS-438
	public void testGeoAdd() {
		doReturn(Collections.singletonList(1L)).when(nativeConnection).exec();
		super.testGeoAddBytes();
	}

	@Test // DATAREDIS-438
	@Override
	public void testGeoAddWithGeoLocationBytes() {

		doReturn(Collections.singletonList(1L)).when(nativeConnection).exec();
		super.testGeoAddWithGeoLocationBytes();
	}

	@Test // DATAREDIS-438
	@Override
	public void testGeoAddWithGeoLocation() {

		doReturn(Collections.singletonList(1L)).when(nativeConnection).exec();
		super.testGeoAddWithGeoLocation();
	}

	@Test // DATAREDIS-438
	public void testGeoAddCoordinateMapBytes() {
		doReturn(Collections.singletonList(1L)).when(nativeConnection).exec();
		super.testGeoAddCoordinateMapBytes();
	}

	@Test // DATAREDIS-438
	public void testGeoAddCoordinateMap() {
		doReturn(Collections.singletonList(1L)).when(nativeConnection).exec();
		super.testGeoAddCoordinateMap();
	}

	@Test // DATAREDIS-438
	@Override
	public void testGeoAddWithIterableOfGeoLocationBytes() {

		doReturn(Collections.singletonList(1L)).when(nativeConnection).exec();
		super.testGeoAddWithIterableOfGeoLocationBytes();
	}

	@Test // DATAREDIS-438
	@Override
	public void testGeoAddWithIterableOfGeoLocation() {

		doReturn(Collections.singletonList(1L)).when(nativeConnection).exec();
		super.testGeoAddWithIterableOfGeoLocation();
	}

	@Test // DATAREDIS-438
	public void testGeoDistBytes() {

		doReturn(Collections.singletonList(new Distance(102121.12d, DistanceUnit.METERS))).when(nativeConnection).exec();
		super.testGeoDistBytes();
	}

	@Test // DATAREDIS-438
	public void testGeoDist() {

		doReturn(Collections.singletonList(new Distance(102121.12d, DistanceUnit.METERS))).when(nativeConnection).exec();
		super.testGeoDist();
	}

	@Test // DATAREDIS-438
	public void testGeoHashBytes() {

		doReturn(Collections.singletonList(Collections.singletonList(bar))).when(nativeConnection).exec();
		super.testGeoHashBytes();
	}

	@Test // DATAREDIS-438
	public void testGeoHash() {

		doReturn(Collections.singletonList(Collections.singletonList(bar))).when(nativeConnection).exec();
		super.testGeoHash();
	}

	@Test // DATAREDIS-438
	public void testGeoPosBytes() {

		doReturn(Collections.singletonList(points)).when(nativeConnection).exec();
		super.testGeoPosBytes();
	}

	@Test // DATAREDIS-438
	public void testGeoPos() {

		doReturn(Collections.singletonList(points)).when(nativeConnection).exec();
		super.testGeoPos();
	}

	@Test // DATAREDIS-438
	public void testGeoRadiusWithoutParamBytes() {

		doReturn(Collections.singletonList(geoResults)).when(nativeConnection).exec();
		super.testGeoRadiusWithoutParamBytes();
	}

	@Test // DATAREDIS-438
	public void testGeoRadiusWithoutParam() {

		doReturn(Collections.singletonList(geoResults)).when(nativeConnection).exec();
		super.testGeoRadiusWithoutParam();
	}

	@Test // DATAREDIS-438
	public void testGeoRadiusWithDistBytes() {

		doReturn(Collections.singletonList(geoResults)).when(nativeConnection).exec();
		super.testGeoRadiusWithDistBytes();
	}

	@Test // DATAREDIS-438
	public void testGeoRadiusWithDist() {

		doReturn(Collections.singletonList(geoResults)).when(nativeConnection).exec();
		super.testGeoRadiusWithDist();
	}

	@Test
	public void testGeoRadiusWithCoordAndDescBytes() {

		doReturn(Collections.singletonList(geoResults)).when(nativeConnection).exec();
		super.testGeoRadiusWithCoordAndDescBytes();
	}

	@Test // DATAREDIS-438
	public void testGeoRadiusWithCoordAndDesc() {

		doReturn(Collections.singletonList(geoResults)).when(nativeConnection).exec();
		super.testGeoRadiusWithCoordAndDesc();
	}

	@Test // DATAREDIS-438
	public void testGeoRadiusByMemberWithoutParamBytes() {

		doReturn(Collections.singletonList(geoResults)).when(nativeConnection).exec();
		super.testGeoRadiusByMemberWithoutParamBytes();
	}

	@Test // DATAREDIS-438
	public void testGeoRadiusByMemberWithoutParam() {

		doReturn(Collections.singletonList(geoResults)).when(nativeConnection).exec();
		super.testGeoRadiusByMemberWithoutParam();
	}

	@Test // DATAREDIS-438
	public void testGeoRadiusByMemberWithDistAndAscBytes() {

		doReturn(Collections.singletonList(geoResults)).when(nativeConnection).exec();
		super.testGeoRadiusByMemberWithDistAndAscBytes();
	}

	@Test // DATAREDIS-438
	public void testGeoRadiusByMemberWithDistAndAsc() {

		doReturn(Collections.singletonList(geoResults)).when(nativeConnection).exec();
		super.testGeoRadiusByMemberWithDistAndAsc();
	}

	@Test // DATAREDIS-438
	public void testGeoRadiusByMemberWithCoordAndCountBytes() {

		doReturn(Collections.singletonList(geoResults)).when(nativeConnection).exec();
		super.testGeoRadiusByMemberWithCoordAndCountBytes();
	}

	@Test // DATAREDIS-438
	public void testGeoRadiusByMemberWithCoordAndCount() {

		doReturn(Collections.singletonList(geoResults)).when(nativeConnection).exec();
		super.testGeoRadiusByMemberWithCoordAndCount();
	}

	@Test // DATAREDIS-864
	public void xAckShouldDelegateAndConvertCorrectly() {

		doReturn(Collections.singletonList(1L)).when(nativeConnection).exec();
		super.xAckShouldDelegateAndConvertCorrectly();
	}

	@Override // DATAREDIS-864
	public void xAddShouldAppendRecordCorrectly() {

		doReturn(Collections.singletonList(RecordId.of("1-1"))).when(nativeConnection).exec();
		super.xAddShouldAppendRecordCorrectly();
	}

	@Test // DATAREDIS-864
	public void xDelShouldDelegateAndConvertCorrectly() {

		doReturn(Collections.singletonList(1L)).when(nativeConnection).exec();
		super.xAckShouldDelegateAndConvertCorrectly();
	}

	@Test // DATAREDIS-864
	public void xGroupCreateShouldDelegateAndConvertCorrectly() {

		doReturn(Collections.singletonList("OK")).when(nativeConnection).exec();
		super.xGroupCreateShouldDelegateAndConvertCorrectly();
	}

	@Test // DATAREDIS-864
	public void xGroupDelConsumerShouldDelegateAndConvertCorrectly() {

		doReturn(Collections.singletonList(Boolean.TRUE)).when(nativeConnection).exec();
		super.xGroupDelConsumerShouldDelegateAndConvertCorrectly();
	}

	@Test // DATAREDIS-864
	public void xLenShouldDelegateAndConvertCorrectly() {

		doReturn(Collections.singletonList(1L)).when(nativeConnection).exec();
		super.xLenShouldDelegateAndConvertCorrectly();
	}

	@Test // DATAREDIS-864
	public void xGroupDestroyShouldDelegateAndConvertCorrectly() {

		doReturn(Collections.singletonList(Boolean.TRUE)).when(nativeConnection).exec();
		super.xGroupDestroyShouldDelegateAndConvertCorrectly();
	}

	@Test // DATAREDIS-864
	public void xRangeShouldDelegateAndConvertCorrectly() {

		doReturn(Collections.singletonList(
				Collections.singletonList(StreamRecords.newRecord().in(bar2Bytes).withId("stream-1").ofBytes(bytesMap))))
						.when(nativeConnection).exec();
		super.xRangeShouldDelegateAndConvertCorrectly();
	}

	@Test // DATAREDIS-864
	public void xReadShouldDelegateAndConvertCorrectly() {

		doReturn(Collections.singletonList(
				Collections.singletonList(StreamRecords.newRecord().in(bar2Bytes).withId("stream-1").ofBytes(bytesMap))))
						.when(nativeConnection).exec();
		super.xReadShouldDelegateAndConvertCorrectly();
	}

	@Test // DATAREDIS-864
	public void xReadGroupShouldDelegateAndConvertCorrectly() {

		doReturn(Collections.singletonList(
				Collections.singletonList(StreamRecords.newRecord().in(bar2Bytes).withId("stream-1").ofBytes(bytesMap))))
						.when(nativeConnection).exec();
		super.xReadGroupShouldDelegateAndConvertCorrectly();
	}

	@Test // DATAREDIS-864
	public void xRevRangeShouldDelegateAndConvertCorrectly() {

		doReturn(Collections.singletonList(
				Collections.singletonList(StreamRecords.newRecord().in(bar2Bytes).withId("stream-1").ofBytes(bytesMap))))
						.when(nativeConnection).exec();
		super.xRevRangeShouldDelegateAndConvertCorrectly();
	}

	@Test // DATAREDIS-864
	public void xTrimShouldDelegateAndConvertCorrectly() {

		doReturn(Collections.singletonList(1L)).when(nativeConnection).exec();
		super.xTrimShouldDelegateAndConvertCorrectly();
	}

	@Test
	public void xTrimApproximateShouldDelegateAndConvertCorrectly() {

		doReturn(Collections.singletonList(1L)).when(nativeConnection).exec();
		super.xTrimApproximateShouldDelegateAndConvertCorrectly();
	}

	protected List<Object> getResults() {
		return connection.exec();
	}
}
