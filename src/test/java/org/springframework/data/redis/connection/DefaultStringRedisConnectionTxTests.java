/*
 * Copyright 2013-2018 the original author or authors.
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

import static org.mockito.Mockito.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.junit.Before;
import org.junit.Test;
import org.springframework.data.geo.Distance;
import org.springframework.data.redis.connection.RedisGeoCommands.DistanceUnit;

/**
 * @author Jennifer Hickey
 * @author Christoph Strobl
 * @author Ninad Divadkar
 */
public class DefaultStringRedisConnectionTxTests extends DefaultStringRedisConnectionTests {

	@Before
	public void setUp() {
		super.setUp();
		connection.setDeserializePipelineAndTxResults(true);
		when(nativeConnection.isQueueing()).thenReturn(true);
	}

	@Test
	public void testAppend() {
		doReturn(Arrays.asList(new Object[] { 1l })).when(nativeConnection).exec();
		super.testAppend();
	}

	@Test
	public void testAppendBytes() {
		doReturn(Arrays.asList(new Object[] { 1l })).when(nativeConnection).exec();
		super.testAppendBytes();
	}

	@Test
	public void testBlPopBytes() {
		doReturn(Arrays.asList(new Object[] { bytesList })).when(nativeConnection).exec();
		super.testBlPopBytes();
	}

	@Test
	public void testBlPop() {
		doReturn(Arrays.asList(new Object[] { bytesList })).when(nativeConnection).exec();
		super.testBlPop();
	}

	@Test
	public void testBrPopBytes() {
		doReturn(Arrays.asList(new Object[] { bytesList })).when(nativeConnection).exec();
		super.testBrPopBytes();
	}

	@Test
	public void testBrPop() {
		doReturn(Arrays.asList(new Object[] { bytesList })).when(nativeConnection).exec();
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
	public void testDbSize() {
		doReturn(Arrays.asList(new Object[] { 3l })).when(nativeConnection).exec();
		super.testDbSize();
	}

	@Test
	public void testDecrBytes() {
		doReturn(Arrays.asList(new Object[] { 3l })).when(nativeConnection).exec();
		super.testDecrBytes();
	}

	@Test
	public void testDecr() {
		doReturn(Arrays.asList(new Object[] { 3l })).when(nativeConnection).exec();
		super.testDecr();
	}

	@Test
	public void testDecrByBytes() {
		doReturn(Arrays.asList(new Object[] { 3l })).when(nativeConnection).exec();
		super.testDecrByBytes();
	}

	@Test
	public void testDecrBy() {
		doReturn(Arrays.asList(new Object[] { 3l })).when(nativeConnection).exec();
		super.testDecrBy();
	}

	@Test
	public void testDelBytes() {
		doReturn(Arrays.asList(new Object[] { 1l })).when(nativeConnection).exec();
		super.testDelBytes();
	}

	@Test
	public void testDel() {
		doReturn(Arrays.asList(new Object[] { 1l })).when(nativeConnection).exec();
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
		doReturn(Arrays.asList(new Object[] { true })).when(nativeConnection).exec();
		super.testExistsBytes();
	}

	@Test
	public void testExists() {
		doReturn(Arrays.asList(new Object[] { true })).when(nativeConnection).exec();
		super.testExists();
	}

	@Test
	public void testExpireBytes() {
		doReturn(Arrays.asList(new Object[] { true })).when(nativeConnection).exec();
		super.testExpireBytes();
	}

	@Test
	public void testExpire() {
		doReturn(Arrays.asList(new Object[] { true })).when(nativeConnection).exec();
		super.testExpire();
	}

	@Test
	public void testExpireAtBytes() {
		doReturn(Arrays.asList(new Object[] { true })).when(nativeConnection).exec();
		super.testExpireAtBytes();
	}

	@Test
	public void testExpireAt() {
		doReturn(Arrays.asList(new Object[] { true })).when(nativeConnection).exec();
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
		doReturn(Arrays.asList(new Object[] { true })).when(nativeConnection).exec();
		super.testGetBitBytes();
	}

	@Test
	public void testGetBit() {
		doReturn(Arrays.asList(new Object[] { true })).when(nativeConnection).exec();
		super.testGetBit();
	}

	@Test // DATAREDIS-661
	public void testGetConfig() {

		Properties results = new Properties();
		results.put("foo", "bar");

		doReturn(Arrays.asList(new Object[] { results })).when(nativeConnection).exec();
		super.testGetConfig();
	}

	@Test
	public void testGetNativeConnection() {
		doReturn(Arrays.asList(new Object[] { "foo" })).when(nativeConnection).exec();
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
		doReturn(Arrays.asList(new Object[] { 1l })).when(nativeConnection).exec();
		super.testHDelBytes();
	}

	@Test
	public void testHDel() {
		doReturn(Arrays.asList(new Object[] { 1l })).when(nativeConnection).exec();
		super.testHDel();
	}

	@Test
	public void testHExistsBytes() {
		doReturn(Arrays.asList(new Object[] { true })).when(nativeConnection).exec();
		super.testHExistsBytes();
	}

	@Test
	public void testHExists() {
		doReturn(Arrays.asList(new Object[] { true })).when(nativeConnection).exec();
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
		doReturn(Arrays.asList(new Object[] { bytesMap })).when(nativeConnection).exec();
		super.testHGetAllBytes();
	}

	@Test
	public void testHGetAll() {
		doReturn(Arrays.asList(new Object[] { bytesMap })).when(nativeConnection).exec();
		super.testHGetAll();
	}

	@Test
	public void testHIncrByBytes() {
		doReturn(Arrays.asList(new Object[] { 3l })).when(nativeConnection).exec();
		super.testHIncrByBytes();
	}

	@Test
	public void testHIncrBy() {
		doReturn(Arrays.asList(new Object[] { 3l })).when(nativeConnection).exec();
		super.testHIncrBy();
	}

	@Test
	public void testHIncrByDoubleBytes() {
		doReturn(Arrays.asList(new Object[] { 3d })).when(nativeConnection).exec();
		super.testHIncrByDoubleBytes();
	}

	@Test
	public void testHIncrByDouble() {
		doReturn(Arrays.asList(new Object[] { 3d })).when(nativeConnection).exec();
		super.testHIncrByDouble();
	}

	@Test
	public void testHKeysBytes() {
		doReturn(Arrays.asList(new Object[] { bytesSet })).when(nativeConnection).exec();
		super.testHKeysBytes();
	}

	@Test
	public void testHKeys() {
		doReturn(Arrays.asList(new Object[] { bytesSet })).when(nativeConnection).exec();
		super.testHKeys();
	}

	@Test
	public void testHLenBytes() {
		doReturn(Arrays.asList(new Object[] { 3l })).when(nativeConnection).exec();
		super.testHLenBytes();
	}

	@Test
	public void testHLen() {
		doReturn(Arrays.asList(new Object[] { 3l })).when(nativeConnection).exec();
		super.testHLen();
	}

	@Test
	public void testHMGetBytes() {
		doReturn(Arrays.asList(new Object[] { bytesList })).when(nativeConnection).exec();
		super.testHMGetBytes();
	}

	@Test
	public void testHMGet() {
		doReturn(Arrays.asList(new Object[] { bytesList })).when(nativeConnection).exec();
		super.testHMGet();
	}

	@Test
	public void testHSetBytes() {
		doReturn(Arrays.asList(new Object[] { true })).when(nativeConnection).exec();
		super.testHSetBytes();
	}

	@Test
	public void testHSet() {
		doReturn(Arrays.asList(new Object[] { true })).when(nativeConnection).exec();
		super.testHSet();
	}

	@Test
	public void testHSetNXBytes() {
		doReturn(Arrays.asList(new Object[] { true })).when(nativeConnection).exec();
		super.testHSetNXBytes();
	}

	@Test
	public void testHSetNX() {
		doReturn(Arrays.asList(new Object[] { true })).when(nativeConnection).exec();
		super.testHSetNX();
	}

	@Test
	public void testHValsBytes() {
		doReturn(Arrays.asList(new Object[] { bytesList })).when(nativeConnection).exec();
		super.testHValsBytes();
	}

	@Test
	public void testHVals() {
		doReturn(Arrays.asList(new Object[] { bytesList })).when(nativeConnection).exec();
		super.testHVals();
	}

	@Test
	public void testIncrBytes() {
		doReturn(Arrays.asList(new Object[] { 2l })).when(nativeConnection).exec();
		super.testIncrBytes();
	}

	@Test
	public void testIncr() {
		doReturn(Arrays.asList(new Object[] { 2l })).when(nativeConnection).exec();
		super.testIncr();
	}

	@Test
	public void testIncrByBytes() {
		doReturn(Arrays.asList(new Object[] { 2l })).when(nativeConnection).exec();
		super.testIncrByBytes();
	}

	@Test
	public void testIncrBy() {
		doReturn(Arrays.asList(new Object[] { 2l })).when(nativeConnection).exec();
		super.testIncrBy();
	}

	@Test
	public void testIncrByDoubleBytes() {
		doReturn(Arrays.asList(new Object[] { 2d })).when(nativeConnection).exec();
		super.testIncrByDoubleBytes();
	}

	@Test
	public void testIncrByDouble() {
		doReturn(Arrays.asList(new Object[] { 2d })).when(nativeConnection).exec();
		super.testIncrByDouble();
	}

	@Test
	public void testInfo() {
		Properties props = new Properties();
		props.put("foo", "bar");
		doReturn(Arrays.asList(new Object[] { props })).when(nativeConnection).exec();
		super.testInfo();
	}

	@Test
	public void testInfoBySection() {
		Properties props = new Properties();
		props.put("foo", "bar");
		doReturn(Arrays.asList(new Object[] { props })).when(nativeConnection).exec();
		super.testInfoBySection();
	}

	@Test
	public void testKeysBytes() {
		doReturn(Arrays.asList(new Object[] { bytesSet })).when(nativeConnection).exec();
		super.testKeysBytes();
	}

	@Test
	public void testKeys() {
		doReturn(Arrays.asList(new Object[] { bytesSet })).when(nativeConnection).exec();
		super.testKeys();
	}

	@Test
	public void testLastSave() {
		doReturn(Arrays.asList(new Object[] { 6l })).when(nativeConnection).exec();
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
		doReturn(Arrays.asList(new Object[] { 8l })).when(nativeConnection).exec();
		super.testLInsertBytes();
	}

	@Test
	public void testLInsert() {
		doReturn(Arrays.asList(new Object[] { 8l })).when(nativeConnection).exec();
		super.testLInsert();
	}

	@Test
	public void testLLenBytes() {
		doReturn(Arrays.asList(new Object[] { 8l })).when(nativeConnection).exec();
		super.testLLenBytes();
	}

	@Test
	public void testLLen() {
		doReturn(Arrays.asList(new Object[] { 8l })).when(nativeConnection).exec();
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

	@Test
	public void testLPushBytes() {
		doReturn(Arrays.asList(new Object[] { 8l })).when(nativeConnection).exec();
		super.testLPushBytes();
	}

	@Test
	public void testLPush() {
		doReturn(Arrays.asList(new Object[] { 8l })).when(nativeConnection).exec();
		super.testLPush();
	}

	@Test
	public void testLPushXBytes() {
		doReturn(Arrays.asList(new Object[] { 8l })).when(nativeConnection).exec();
		super.testLPushXBytes();
	}

	@Test
	public void testLPushX() {
		doReturn(Arrays.asList(new Object[] { 8l })).when(nativeConnection).exec();
		super.testLPushX();
	}

	@Test
	public void testLRangeBytes() {
		doReturn(Arrays.asList(new Object[] { bytesList })).when(nativeConnection).exec();
		super.testLRangeBytes();
	}

	@Test
	public void testLRange() {
		doReturn(Arrays.asList(new Object[] { bytesList })).when(nativeConnection).exec();
		super.testLRange();
	}

	@Test
	public void testLRemBytes() {
		doReturn(Arrays.asList(new Object[] { 8l })).when(nativeConnection).exec();
		super.testLRemBytes();
	}

	@Test
	public void testLRem() {
		doReturn(Arrays.asList(new Object[] { 8l })).when(nativeConnection).exec();
		super.testLRem();
	}

	@Test
	public void testMGetBytes() {
		doReturn(Arrays.asList(new Object[] { bytesList })).when(nativeConnection).exec();
		super.testMGetBytes();
	}

	@Test
	public void testMGet() {
		doReturn(Arrays.asList(new Object[] { bytesList })).when(nativeConnection).exec();
		super.testMGet();
	}

	@Test
	public void testMSetNXBytes() {
		doReturn(Arrays.asList(new Object[] { true })).when(nativeConnection).exec();
		super.testMSetNXBytes();
	}

	@Test
	public void testMSetNXString() {
		doReturn(Arrays.asList(new Object[] { true })).when(nativeConnection).exec();
		super.testMSetNXString();
	}

	@Test
	public void testPersistBytes() {
		doReturn(Arrays.asList(new Object[] { true })).when(nativeConnection).exec();
		super.testPersistBytes();
	}

	@Test
	public void testPersist() {
		doReturn(Arrays.asList(new Object[] { true })).when(nativeConnection).exec();
		super.testPersist();
	}

	@Test
	public void testMoveBytes() {
		doReturn(Arrays.asList(new Object[] { true })).when(nativeConnection).exec();
		super.testMoveBytes();
	}

	@Test
	public void testMove() {
		doReturn(Arrays.asList(new Object[] { true })).when(nativeConnection).exec();
		super.testMove();
	}

	@Test
	public void testPing() {
		doReturn(Arrays.asList(new Object[] { "pong" })).when(nativeConnection).exec();
		super.testPing();
	}

	@Test
	public void testPublishBytes() {
		doReturn(Arrays.asList(new Object[] { 2l })).when(nativeConnection).exec();
		super.testPublishBytes();
	}

	@Test
	public void testPublish() {
		doReturn(Arrays.asList(new Object[] { 2l })).when(nativeConnection).exec();
		super.testPublish();
	}

	@Test
	public void testRandomKey() {
		doReturn(Arrays.asList(new Object[] { fooBytes })).when(nativeConnection).exec();
		super.testRandomKey();
	}

	@Test
	public void testRenameNXBytes() {
		doReturn(Arrays.asList(new Object[] { true })).when(nativeConnection).exec();
		super.testRenameNXBytes();
	}

	@Test
	public void testRenameNX() {
		doReturn(Arrays.asList(new Object[] { true })).when(nativeConnection).exec();
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
		doReturn(Arrays.asList(new Object[] { 4l })).when(nativeConnection).exec();
		super.testRPushBytes();
	}

	@Test
	public void testRPush() {
		doReturn(Arrays.asList(new Object[] { 4l })).when(nativeConnection).exec();
		super.testRPush();
	}

	@Test
	public void testRPushXBytes() {
		doReturn(Arrays.asList(new Object[] { 4l })).when(nativeConnection).exec();
		super.testRPushXBytes();
	}

	@Test
	public void testRPushX() {
		doReturn(Arrays.asList(new Object[] { 4l })).when(nativeConnection).exec();
		super.testRPushX();
	}

	@Test
	public void testSAddBytes() {
		doReturn(Arrays.asList(new Object[] { 1l })).when(nativeConnection).exec();
		super.testSAddBytes();
	}

	@Test
	public void testSAdd() {
		doReturn(Arrays.asList(new Object[] { 1l })).when(nativeConnection).exec();
		super.testSAdd();
	}

	@Test
	public void testSCardBytes() {
		doReturn(Arrays.asList(new Object[] { 4l })).when(nativeConnection).exec();
		super.testSCardBytes();
	}

	@Test
	public void testSCard() {
		doReturn(Arrays.asList(new Object[] { 4l })).when(nativeConnection).exec();
		super.testSCard();
	}

	@Test
	public void testSDiffBytes() {
		doReturn(Arrays.asList(new Object[] { bytesSet })).when(nativeConnection).exec();
		super.testSDiffBytes();
	}

	@Test
	public void testSDiff() {
		doReturn(Arrays.asList(new Object[] { bytesSet })).when(nativeConnection).exec();
		super.testSDiff();
	}

	@Test
	public void testSDiffStoreBytes() {
		doReturn(Arrays.asList(new Object[] { 3l })).when(nativeConnection).exec();
		super.testSDiffStoreBytes();
	}

	@Test
	public void testSDiffStore() {
		doReturn(Arrays.asList(new Object[] { 3l })).when(nativeConnection).exec();
		super.testSDiffStore();
	}

	@Test
	public void testSetNXBytes() {
		doReturn(Arrays.asList(new Object[] { true })).when(nativeConnection).exec();
		super.testSetNXBytes();
	}

	@Test
	public void testSetNX() {
		doReturn(Arrays.asList(new Object[] { true })).when(nativeConnection).exec();
		super.testSetNX();
	}

	@Test
	public void testSInterBytes() {
		doReturn(Arrays.asList(new Object[] { bytesSet })).when(nativeConnection).exec();
		super.testSInterBytes();
	}

	@Test
	public void testSInter() {
		doReturn(Arrays.asList(new Object[] { bytesSet })).when(nativeConnection).exec();
		super.testSInter();
	}

	@Test
	public void testSInterStoreBytes() {
		doReturn(Arrays.asList(new Object[] { 3l })).when(nativeConnection).exec();
		super.testSInterStoreBytes();
	}

	@Test
	public void testSInterStore() {
		doReturn(Arrays.asList(new Object[] { 3l })).when(nativeConnection).exec();
		super.testSInterStore();
	}

	@Test
	public void testSIsMemberBytes() {
		doReturn(Arrays.asList(new Object[] { true })).when(nativeConnection).exec();
		super.testSIsMemberBytes();
	}

	@Test
	public void testSIsMember() {
		doReturn(Arrays.asList(new Object[] { true })).when(nativeConnection).exec();
		super.testSIsMember();
	}

	@Test
	public void testSMembersBytes() {
		doReturn(Arrays.asList(new Object[] { bytesSet })).when(nativeConnection).exec();
		super.testSMembersBytes();
	}

	@Test
	public void testSMembers() {
		doReturn(Arrays.asList(new Object[] { bytesSet })).when(nativeConnection).exec();
		super.testSMembers();
	}

	@Test
	public void testSMoveBytes() {
		doReturn(Arrays.asList(new Object[] { true })).when(nativeConnection).exec();
		super.testSMoveBytes();
	}

	@Test
	public void testSMove() {
		doReturn(Arrays.asList(new Object[] { true })).when(nativeConnection).exec();
		super.testSMove();
	}

	@Test
	public void testSortStoreBytes() {
		doReturn(Arrays.asList(new Object[] { 3l })).when(nativeConnection).exec();
		super.testSortStoreBytes();
	}

	@Test
	public void testSortStore() {
		doReturn(Arrays.asList(new Object[] { 3l })).when(nativeConnection).exec();
		super.testSortStore();
	}

	@Test
	public void testSortBytes() {
		doReturn(Arrays.asList(new Object[] { bytesList })).when(nativeConnection).exec();
		super.testSortBytes();
	}

	@Test
	public void testSort() {
		doReturn(Arrays.asList(new Object[] { bytesList })).when(nativeConnection).exec();
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
		doReturn(Arrays.asList(new Object[] { bytesList })).when(nativeConnection).exec();
		super.testSRandMemberCountBytes();
	}

	@Test
	public void testSRandMemberCount() {
		doReturn(Arrays.asList(new Object[] { bytesList })).when(nativeConnection).exec();
		super.testSRandMemberCount();
	}

	@Test
	public void testSRemBytes() {
		doReturn(Arrays.asList(new Object[] { 1l })).when(nativeConnection).exec();
		super.testSRemBytes();
	}

	@Test
	public void testSRem() {
		doReturn(Arrays.asList(new Object[] { 1l })).when(nativeConnection).exec();
		super.testSRem();
	}

	@Test
	public void testStrLenBytes() {
		doReturn(Arrays.asList(new Object[] { 5l })).when(nativeConnection).exec();
		super.testStrLenBytes();
	}

	@Test
	public void testStrLen() {
		doReturn(Arrays.asList(new Object[] { 5l })).when(nativeConnection).exec();
		super.testStrLen();
	}

	@Test
	public void testBitCountBytes() {
		doReturn(Arrays.asList(new Object[] { 5l })).when(nativeConnection).exec();
		super.testBitCountBytes();
	}

	@Test
	public void testBitCount() {
		doReturn(Arrays.asList(new Object[] { 5l })).when(nativeConnection).exec();
		super.testBitCount();
	}

	@Test
	public void testBitCountRangeBytes() {
		doReturn(Arrays.asList(new Object[] { 5l })).when(nativeConnection).exec();
		super.testBitCountRangeBytes();
	}

	@Test
	public void testBitCountRange() {
		doReturn(Arrays.asList(new Object[] { 5l })).when(nativeConnection).exec();
		super.testBitCountRange();
	}

	@Test
	public void testBitOpBytes() {
		doReturn(Arrays.asList(new Object[] { 5l })).when(nativeConnection).exec();
		super.testBitOpBytes();
	}

	@Test
	public void testBitOp() {
		doReturn(Arrays.asList(new Object[] { 5l })).when(nativeConnection).exec();
		super.testBitOp();
	}

	@Test
	public void testSUnionBytes() {
		doReturn(Arrays.asList(new Object[] { bytesSet })).when(nativeConnection).exec();
		super.testSUnionBytes();
	}

	@Test
	public void testSUnion() {
		doReturn(Arrays.asList(new Object[] { bytesSet })).when(nativeConnection).exec();
		super.testSUnion();
	}

	@Test
	public void testSUnionStoreBytes() {
		doReturn(Arrays.asList(new Object[] { 5l })).when(nativeConnection).exec();
		super.testSUnionStoreBytes();
	}

	@Test
	public void testSUnionStore() {
		doReturn(Arrays.asList(new Object[] { 5l })).when(nativeConnection).exec();
		super.testSUnionStore();
	}

	@Test
	public void testTtlBytes() {
		doReturn(Arrays.asList(new Object[] { 5l })).when(nativeConnection).exec();
		super.testTtlBytes();
	}

	@Test
	public void testTtl() {
		doReturn(Arrays.asList(new Object[] { 5l })).when(nativeConnection).exec();
		super.testTtl();
	}

	// DATAREDIS-526
	@Override
	public void testTtlWithTimeUnit() {

		doReturn(Arrays.asList(new Object[] { 5L })).when(nativeConnection).exec();
		super.testTtl();
	}

	@Test
	public void testTypeBytes() {
		doReturn(Arrays.asList(new Object[] { DataType.HASH })).when(nativeConnection).exec();
		super.testTypeBytes();
	}

	@Test
	public void testType() {
		doReturn(Arrays.asList(new Object[] { DataType.HASH })).when(nativeConnection).exec();
		super.testType();
	}

	@Test
	public void testZAddBytes() {
		doReturn(Arrays.asList(new Object[] { true })).when(nativeConnection).exec();
		super.testZAddBytes();
	}

	@Test
	public void testZAdd() {
		doReturn(Arrays.asList(new Object[] { true })).when(nativeConnection).exec();
		super.testZAdd();
	}

	@Test
	public void testZAddMultipleBytes() {
		doReturn(Arrays.asList(new Object[] { 1l })).when(nativeConnection).exec();
		super.testZAddMultipleBytes();
	}

	@Test
	public void testZAddMultiple() {
		doReturn(Arrays.asList(new Object[] { 1l })).when(nativeConnection).exec();
		super.testZAddMultiple();
	}

	@Test
	public void testZCardBytes() {
		doReturn(Arrays.asList(new Object[] { 5l })).when(nativeConnection).exec();
		super.testZCardBytes();
	}

	@Test
	public void testZCard() {
		doReturn(Arrays.asList(new Object[] { 5l })).when(nativeConnection).exec();
		super.testZCard();
	}

	@Test
	public void testZCountBytes() {
		doReturn(Arrays.asList(new Object[] { 5l })).when(nativeConnection).exec();
		super.testZCountBytes();
	}

	@Test
	public void testZCount() {
		doReturn(Arrays.asList(new Object[] { 5l })).when(nativeConnection).exec();
		super.testZCount();
	}

	@Test
	public void testZIncrByBytes() {
		doReturn(Arrays.asList(new Object[] { 3d })).when(nativeConnection).exec();
		super.testZIncrByBytes();
	}

	@Test
	public void testZIncrBy() {
		doReturn(Arrays.asList(new Object[] { 3d })).when(nativeConnection).exec();
		super.testZIncrBy();
	}

	@Test
	public void testZInterStoreAggWeightsBytes() {
		doReturn(Arrays.asList(new Object[] { 5l })).when(nativeConnection).exec();
		super.testZInterStoreAggWeightsBytes();
	}

	@Test
	public void testZInterStoreAggWeights() {
		doReturn(Arrays.asList(new Object[] { 5l })).when(nativeConnection).exec();
		super.testZInterStoreAggWeights();
	}

	@Test
	public void testZInterStoreBytes() {
		doReturn(Arrays.asList(new Object[] { 5l })).when(nativeConnection).exec();
		super.testZInterStoreBytes();
	}

	@Test
	public void testZInterStore() {
		doReturn(Arrays.asList(new Object[] { 5l })).when(nativeConnection).exec();
		super.testZInterStore();
	}

	@Test
	public void testZRangeBytes() {
		doReturn(Arrays.asList(new Object[] { bytesSet })).when(nativeConnection).exec();
		super.testZRangeBytes();
	}

	@Test
	public void testZRange() {
		doReturn(Arrays.asList(new Object[] { bytesSet })).when(nativeConnection).exec();
		super.testZRange();
	}

	@Test
	public void testZRangeByScoreOffsetCountBytes() {
		doReturn(Arrays.asList(new Object[] { bytesSet })).when(nativeConnection).exec();
		super.testZRangeByScoreOffsetCountBytes();
	}

	@Test
	public void testZRangeByScoreOffsetCount() {
		doReturn(Arrays.asList(new Object[] { bytesSet })).when(nativeConnection).exec();
		super.testZRangeByScoreOffsetCount();
	}

	@Test
	public void testZRangeByScoreBytes() {
		doReturn(Arrays.asList(new Object[] { bytesSet })).when(nativeConnection).exec();
		super.testZRangeByScoreBytes();
	}

	@Test
	public void testZRangeByScore() {
		doReturn(Arrays.asList(new Object[] { bytesSet })).when(nativeConnection).exec();
		super.testZRangeByScore();
	}

	@Test
	public void testZRangeByScoreWithScoresOffsetCountBytes() {
		doReturn(Arrays.asList(new Object[] { tupleSet })).when(nativeConnection).exec();
		super.testZRangeByScoreWithScoresOffsetCountBytes();
	}

	@Test
	public void testZRangeByScoreWithScoresOffsetCount() {
		doReturn(Arrays.asList(new Object[] { tupleSet })).when(nativeConnection).exec();
		super.testZRangeByScoreWithScoresOffsetCount();
	}

	@Test
	public void testZRangeByScoreWithScoresBytes() {
		doReturn(Arrays.asList(new Object[] { tupleSet })).when(nativeConnection).exec();
		super.testZRangeByScoreWithScoresBytes();
	}

	@Test
	public void testZRangeByScoreWithScores() {
		doReturn(Arrays.asList(new Object[] { tupleSet })).when(nativeConnection).exec();
		super.testZRangeByScoreWithScores();
	}

	@Test
	public void testZRangeWithScoresBytes() {
		doReturn(Arrays.asList(new Object[] { tupleSet })).when(nativeConnection).exec();
		super.testZRangeWithScoresBytes();
	}

	@Test
	public void testZRangeWithScores() {
		doReturn(Arrays.asList(new Object[] { tupleSet })).when(nativeConnection).exec();
		super.testZRangeWithScores();
	}

	@Test
	public void testZRevRangeByScoreOffsetCountBytes() {
		doReturn(Arrays.asList(new Object[] { bytesSet })).when(nativeConnection).exec();
		super.testZRevRangeByScoreOffsetCountBytes();
	}

	@Test
	public void testZRevRangeByScoreOffsetCount() {
		doReturn(Arrays.asList(new Object[] { bytesSet })).when(nativeConnection).exec();
		super.testZRevRangeByScoreOffsetCount();
	}

	@Test
	public void testZRevRangeByScoreBytes() {
		doReturn(Arrays.asList(new Object[] { bytesSet })).when(nativeConnection).exec();
		super.testZRevRangeByScoreBytes();
	}

	@Test
	public void testZRevRangeByScore() {
		doReturn(Arrays.asList(new Object[] { bytesSet })).when(nativeConnection).exec();
		super.testZRevRangeByScore();
	}

	@Test
	public void testZRevRangeByScoreWithScoresOffsetCountBytes() {
		doReturn(Arrays.asList(new Object[] { tupleSet })).when(nativeConnection).exec();
		super.testZRevRangeByScoreWithScoresOffsetCountBytes();
	}

	@Test
	public void testZRevRangeByScoreWithScoresOffsetCount() {
		doReturn(Arrays.asList(new Object[] { tupleSet })).when(nativeConnection).exec();
		super.testZRevRangeByScoreWithScoresOffsetCount();
	}

	@Test
	public void testZRevRangeByScoreWithScoresBytes() {
		doReturn(Arrays.asList(new Object[] { tupleSet })).when(nativeConnection).exec();
		super.testZRevRangeByScoreWithScoresBytes();
	}

	@Test
	public void testZRevRangeByScoreWithScores() {
		doReturn(Arrays.asList(new Object[] { tupleSet })).when(nativeConnection).exec();
		super.testZRevRangeByScoreWithScores();
	}

	@Test
	public void testZRankBytes() {
		doReturn(Arrays.asList(new Object[] { 5l })).when(nativeConnection).exec();
		super.testZRankBytes();
	}

	@Test
	public void testZRank() {
		doReturn(Arrays.asList(new Object[] { 5l })).when(nativeConnection).exec();
		super.testZRank();
	}

	@Test
	public void testZRemBytes() {
		doReturn(Arrays.asList(new Object[] { 1l })).when(nativeConnection).exec();
		super.testZRemBytes();
	}

	@Test
	public void testZRem() {
		doReturn(Arrays.asList(new Object[] { 1l })).when(nativeConnection).exec();
		super.testZRem();
	}

	@Test
	public void testZRemRangeBytes() {
		doReturn(Arrays.asList(new Object[] { 5l })).when(nativeConnection).exec();
		super.testZRemRangeBytes();
	}

	@Test
	public void testZRemRange() {
		doReturn(Arrays.asList(new Object[] { 5l })).when(nativeConnection).exec();
		super.testZRemRange();
	}

	@Test
	public void testZRemRangeByScoreBytes() {
		doReturn(Arrays.asList(new Object[] { 5l })).when(nativeConnection).exec();
		super.testZRemRangeByScoreBytes();
	}

	@Test
	public void testZRemRangeByScore() {
		doReturn(Arrays.asList(new Object[] { 5l })).when(nativeConnection).exec();
		super.testZRemRangeByScore();
	}

	@Test
	public void testZRevRangeBytes() {
		doReturn(Arrays.asList(new Object[] { bytesSet })).when(nativeConnection).exec();
		super.testZRevRangeBytes();
	}

	@Test
	public void testZRevRange() {
		doReturn(Arrays.asList(new Object[] { bytesSet })).when(nativeConnection).exec();
		super.testZRevRange();
	}

	@Test
	public void testZRevRangeWithScoresBytes() {
		doReturn(Arrays.asList(new Object[] { tupleSet })).when(nativeConnection).exec();
		super.testZRevRangeWithScoresBytes();
	}

	@Test
	public void testZRevRangeWithScores() {
		doReturn(Arrays.asList(new Object[] { tupleSet })).when(nativeConnection).exec();
		super.testZRevRangeWithScores();
	}

	@Test
	public void testZRevRankBytes() {
		doReturn(Arrays.asList(new Object[] { 5l })).when(nativeConnection).exec();
		super.testZRevRankBytes();
	}

	@Test
	public void testZRevRank() {
		doReturn(Arrays.asList(new Object[] { 5l })).when(nativeConnection).exec();
		super.testZRevRank();
	}

	@Test
	public void testZScoreBytes() {
		doReturn(Arrays.asList(new Object[] { 3d })).when(nativeConnection).exec();
		super.testZScoreBytes();
	}

	@Test
	public void testZScore() {
		doReturn(Arrays.asList(new Object[] { 3d })).when(nativeConnection).exec();
		super.testZScore();
	}

	@Test
	public void testZUnionStoreAggWeightsBytes() {
		doReturn(Arrays.asList(new Object[] { 5l })).when(nativeConnection).exec();
		super.testZUnionStoreAggWeightsBytes();
	}

	@Test
	public void testZUnionStoreAggWeights() {
		doReturn(Arrays.asList(new Object[] { 5l })).when(nativeConnection).exec();
		super.testZUnionStoreAggWeights();
	}

	@Test
	public void testZUnionStoreBytes() {
		doReturn(Arrays.asList(new Object[] { 5l })).when(nativeConnection).exec();
		super.testZUnionStoreBytes();
	}

	@Test
	public void testZUnionStore() {
		doReturn(Arrays.asList(new Object[] { 5l })).when(nativeConnection).exec();
		super.testZUnionStore();
	}

	@Test
	public void testPExpireBytes() {
		doReturn(Arrays.asList(new Object[] { true })).when(nativeConnection).exec();
		super.testPExpireBytes();
	}

	@Test
	public void testPExpire() {
		doReturn(Arrays.asList(new Object[] { true })).when(nativeConnection).exec();
		super.testPExpire();
	}

	@Test
	public void testPExpireAtBytes() {
		doReturn(Arrays.asList(new Object[] { true })).when(nativeConnection).exec();
		super.testPExpireAtBytes();
	}

	@Test
	public void testPExpireAt() {
		doReturn(Arrays.asList(new Object[] { true })).when(nativeConnection).exec();
		super.testPExpireAt();
	}

	@Test
	public void testPTtlBytes() {
		doReturn(Arrays.asList(new Object[] { 5l })).when(nativeConnection).exec();
		super.testPTtlBytes();
	}

	@Test
	public void testPTtl() {
		doReturn(Arrays.asList(new Object[] { 5l })).when(nativeConnection).exec();
		super.testPTtl();
	}

	@Test
	public void testDump() {
		doReturn(Arrays.asList(new Object[] { barBytes })).when(nativeConnection).exec();
		super.testDump();
	}

	@Test
	public void testScriptLoadBytes() {
		doReturn(Arrays.asList(new Object[] { "foo" })).when(nativeConnection).exec();
		super.testScriptLoadBytes();
	}

	@Test
	public void testScriptLoad() {
		doReturn(Arrays.asList(new Object[] { "foo" })).when(nativeConnection).exec();
		super.testScriptLoad();
	}

	@Test
	public void testScriptExists() {
		List<Boolean> results = Collections.singletonList(true);
		doReturn(Arrays.asList(new Object[] { results })).when(nativeConnection).exec();
		super.testScriptExists();
	}

	@Test
	public void testEvalBytes() {
		doReturn(Arrays.asList(new Object[] { "foo" })).when(nativeConnection).exec();
		super.testEvalBytes();
	}

	@Test
	public void testEval() {
		doReturn(Arrays.asList(new Object[] { "foo" })).when(nativeConnection).exec();
		super.testEval();
	}

	@Test
	public void testEvalShaBytes() {
		doReturn(Arrays.asList(new Object[] { "foo" })).when(nativeConnection).exec();
		super.testEvalShaBytes();
	}

	@Test
	public void testEvalSha() {
		doReturn(Arrays.asList(new Object[] { "foo" })).when(nativeConnection).exec();
		super.testEvalSha();
	}

	@Test
	public void testExecute() {
		doReturn(Arrays.asList(new Object[] { "foo" })).when(nativeConnection).exec();
		super.testExecute();
	}

	@Test
	public void testExecuteByteArgs() {
		doReturn(Arrays.asList(new Object[] { "foo" })).when(nativeConnection).exec();
		super.testExecuteByteArgs();
	}

	@Test
	public void testExecuteStringArgs() {
		doReturn(Arrays.asList(new Object[] { "foo" })).when(nativeConnection).exec();
		super.testExecuteStringArgs();
	}

	@Test
	public void testDisablePipelineAndTxDeserialize() {
		connection.setDeserializePipelineAndTxResults(false);
		doReturn(Arrays.asList(new Object[] { barBytes })).when(nativeConnection).exec();
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { barBytes }) })).when(nativeConnection)
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
		doReturn(Arrays.asList(new Object[] { barBytes, 3l })).when(nativeConnection).exec();
		doReturn(barBytes).when(nativeConnection).get(fooBytes);
		connection.get(foo);
		verifyResults(Arrays.asList(new Object[] { barBytes, 3l }));
	}

	@Test
	public void testDiscard() {
		doReturn(Arrays.asList(new Object[] { fooBytes })).when(nativeConnection).exec();
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { fooBytes }) })).when(nativeConnection)
				.closePipeline();
		doReturn(barBytes).when(nativeConnection).get(fooBytes);
		doReturn(fooBytes).when(nativeConnection).get(barBytes);
		connection.get(foo);
		connection.discard();
		connection.get(bar);
		// Converted results of get(bar) should be included
		verifyResults(Arrays.asList(new Object[] { foo }));
	}

	@Test // DATAREDIS-206
	@Override
	public void testTimeIsDelegatedCorrectlyToNativeConnection() {

		doReturn(Arrays.asList(new Object[] { 1L })).when(nativeConnection).exec();
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 1L }) })).when(nativeConnection).closePipeline();
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

	// DATAREDIS-438
	@Override
	public void testGeoAddWithGeoLocationBytes() {

		doReturn(Collections.singletonList(1L)).when(nativeConnection).exec();
		super.testGeoAddWithGeoLocationBytes();
	}

	// DATAREDIS-438
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

	// DATAREDIS-438
	@Override
	public void testGeoAddWithIterableOfGeoLocationBytes() {

		doReturn(Collections.singletonList(1L)).when(nativeConnection).exec();
		super.testGeoAddWithIterableOfGeoLocationBytes();
	}

	// DATAREDIS-438
	@Override
	public void testGeoAddWithIterableOfGeoLocation() {

		doReturn(Collections.singletonList(1L)).when(nativeConnection).exec();
		super.testGeoAddWithIterableOfGeoLocation();
	}

	@Test // DATAREDIS-438
	public void testGeoDistBytes() {

		doReturn(Arrays.asList(new Distance(102121.12d, DistanceUnit.METERS))).when(nativeConnection).exec();
		super.testGeoDistBytes();
	}

	@Test // DATAREDIS-438
	public void testGeoDist() {

		doReturn(Arrays.asList(new Distance(102121.12d, DistanceUnit.METERS))).when(nativeConnection).exec();
		super.testGeoDist();
	}

	@Test // DATAREDIS-438
	public void testGeoHashBytes() {

		doReturn(Arrays.asList(Collections.singletonList(bar))).when(nativeConnection).exec();
		super.testGeoHashBytes();
	}

	@Test // DATAREDIS-438
	public void testGeoHash() {

		doReturn(Arrays.asList(Collections.singletonList(bar))).when(nativeConnection).exec();
		super.testGeoHash();
	}

	@Test // DATAREDIS-438
	public void testGeoPosBytes() {

		doReturn(Arrays.asList(points)).when(nativeConnection).exec();
		super.testGeoPosBytes();
	}

	@Test // DATAREDIS-438
	public void testGeoPos() {

		doReturn(Arrays.asList(points)).when(nativeConnection).exec();
		super.testGeoPos();
	}

	@Test // DATAREDIS-438
	public void testGeoRadiusWithoutParamBytes() {

		doReturn(Arrays.asList(geoResults)).when(nativeConnection).exec();
		super.testGeoRadiusWithoutParamBytes();
	}

	@Test // DATAREDIS-438
	public void testGeoRadiusWithoutParam() {

		doReturn(Arrays.asList(geoResults)).when(nativeConnection).exec();
		super.testGeoRadiusWithoutParam();
	}

	@Test // DATAREDIS-438
	public void testGeoRadiusWithDistBytes() {

		doReturn(Arrays.asList(geoResults)).when(nativeConnection).exec();
		super.testGeoRadiusWithDistBytes();
	}

	@Test // DATAREDIS-438
	public void testGeoRadiusWithDist() {

		doReturn(Arrays.asList(geoResults)).when(nativeConnection).exec();
		super.testGeoRadiusWithDist();
	}

	@Test
	public void testGeoRadiusWithCoordAndDescBytes() {

		doReturn(Arrays.asList(geoResults)).when(nativeConnection).exec();
		super.testGeoRadiusWithCoordAndDescBytes();
	}

	@Test // DATAREDIS-438
	public void testGeoRadiusWithCoordAndDesc() {

		doReturn(Arrays.asList(geoResults)).when(nativeConnection).exec();
		super.testGeoRadiusWithCoordAndDesc();
	}

	@Test // DATAREDIS-438
	public void testGeoRadiusByMemberWithoutParamBytes() {

		doReturn(Arrays.asList(geoResults)).when(nativeConnection).exec();
		super.testGeoRadiusByMemberWithoutParamBytes();
	}

	@Test // DATAREDIS-438
	public void testGeoRadiusByMemberWithoutParam() {

		doReturn(Arrays.asList(geoResults)).when(nativeConnection).exec();
		super.testGeoRadiusByMemberWithoutParam();
	}

	@Test // DATAREDIS-438
	public void testGeoRadiusByMemberWithDistAndAscBytes() {

		doReturn(Arrays.asList(geoResults)).when(nativeConnection).exec();
		super.testGeoRadiusByMemberWithDistAndAscBytes();
	}

	@Test // DATAREDIS-438
	public void testGeoRadiusByMemberWithDistAndAsc() {

		doReturn(Arrays.asList(geoResults)).when(nativeConnection).exec();
		super.testGeoRadiusByMemberWithDistAndAsc();
	}

	@Test // DATAREDIS-438
	public void testGeoRadiusByMemberWithCoordAndCountBytes() {

		doReturn(Arrays.asList(geoResults)).when(nativeConnection).exec();
		super.testGeoRadiusByMemberWithCoordAndCountBytes();
	}

	@Test // DATAREDIS-438
	public void testGeoRadiusByMemberWithCoordAndCount() {

		doReturn(Arrays.asList(geoResults)).when(nativeConnection).exec();
		super.testGeoRadiusByMemberWithCoordAndCount();
	}

	protected List<Object> getResults() {
		return connection.exec();
	}
}
