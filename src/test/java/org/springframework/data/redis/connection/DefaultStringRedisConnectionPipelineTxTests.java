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

import static org.junit.Assert.*;
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
 * @author Mark Paluch
 */
public class DefaultStringRedisConnectionPipelineTxTests extends DefaultStringRedisConnectionTxTests {

	@Before
	public void setUp() {
		super.setUp();
		when(nativeConnection.isPipelined()).thenReturn(true);
	}

	@Test
	public void testAppend() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 1l }) })).when(nativeConnection).closePipeline();
		super.testAppend();
	}

	@Test
	public void testAppendBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 1l }) })).when(nativeConnection).closePipeline();
		super.testAppendBytes();
	}

	@Test
	public void testBlPopBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { bytesList }) })).when(nativeConnection)
				.closePipeline();
		super.testBlPopBytes();
	}

	@Test
	public void testBlPop() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { bytesList }) })).when(nativeConnection)
				.closePipeline();
		super.testBlPop();
	}

	@Test
	public void testBrPopBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { bytesList }) })).when(nativeConnection)
				.closePipeline();
		super.testBrPopBytes();
	}

	@Test
	public void testBrPop() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { bytesList }) })).when(nativeConnection)
				.closePipeline();
		super.testBrPop();
	}

	@Test
	public void testBrPopLPushBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { barBytes }) })).when(nativeConnection)
				.closePipeline();
		super.testBrPopLPushBytes();
	}

	@Test
	public void testBrPopLPush() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { barBytes }) })).when(nativeConnection)
				.closePipeline();
		super.testBrPopLPush();
	}

	@Test
	public void testDbSize() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 3l }) })).when(nativeConnection).closePipeline();
		super.testDbSize();
	}

	@Test
	public void testDecrBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 3l }) })).when(nativeConnection).closePipeline();
		super.testDecrBytes();
	}

	@Test
	public void testDecr() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 3l }) })).when(nativeConnection).closePipeline();
		super.testDecr();
	}

	@Test
	public void testDecrByBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 3l }) })).when(nativeConnection).closePipeline();
		super.testDecrByBytes();
	}

	@Test
	public void testDecrBy() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 3l }) })).when(nativeConnection).closePipeline();
		super.testDecrBy();
	}

	@Test
	public void testDelBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 1l }) })).when(nativeConnection).closePipeline();
		super.testDelBytes();
	}

	@Test
	public void testDel() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 1l }) })).when(nativeConnection).closePipeline();
		super.testDel();
	}

	@Test
	public void testEchoBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { barBytes }) })).when(nativeConnection)
				.closePipeline();
		super.testEchoBytes();
	}

	@Test
	public void testEcho() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { barBytes }) })).when(nativeConnection)
				.closePipeline();
		super.testEcho();
	}

	@Test
	public void testExistsBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { true }) })).when(nativeConnection)
				.closePipeline();
		super.testExistsBytes();
	}

	@Test
	public void testExists() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { true }) })).when(nativeConnection)
				.closePipeline();
		super.testExists();
	}

	@Test
	public void testExpireBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { true }) })).when(nativeConnection)
				.closePipeline();
		super.testExpireBytes();
	}

	@Test
	public void testExpire() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { true }) })).when(nativeConnection)
				.closePipeline();
		super.testExpire();
	}

	@Test
	public void testExpireAtBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { true }) })).when(nativeConnection)
				.closePipeline();
		super.testExpireAtBytes();
	}

	@Test
	public void testExpireAt() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { true }) })).when(nativeConnection)
				.closePipeline();
		super.testExpireAt();
	}

	@Test
	public void testGetBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { barBytes }) })).when(nativeConnection)
				.closePipeline();
		super.testGetBytes();
	}

	@Test
	public void testGet() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { barBytes }) })).when(nativeConnection)
				.closePipeline();
		super.testGet();
	}

	@Test
	public void testGetBitBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { true }) })).when(nativeConnection)
				.closePipeline();
		super.testGetBitBytes();
	}

	@Test
	public void testGetBit() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { true }) })).when(nativeConnection)
				.closePipeline();
		super.testGetBit();
	}

	@Test // DATAREDIS-661
	public void testGetConfig() {

		Properties results = new Properties();
		results.put("foo", "bar");

		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { results }) })).when(nativeConnection)
				.closePipeline();
		super.testGetConfig();
	}

	@Test
	public void testGetNativeConnection() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { "foo" }) })).when(nativeConnection)
				.closePipeline();
		super.testGetNativeConnection();
	}

	@Test
	public void testGetRangeBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { barBytes }) })).when(nativeConnection)
				.closePipeline();
		super.testGetRangeBytes();
	}

	@Test
	public void testGetRange() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { barBytes }) })).when(nativeConnection)
				.closePipeline();
		super.testGetRange();
	}

	@Test
	public void testGetSetBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { barBytes }) })).when(nativeConnection)
				.closePipeline();
		super.testGetSetBytes();
	}

	@Test
	public void testGetSet() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { barBytes }) })).when(nativeConnection)
				.closePipeline();
		super.testGetSet();
	}

	@Test
	public void testHDelBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 1l }) })).when(nativeConnection).closePipeline();
		super.testHDelBytes();
	}

	@Test
	public void testHDel() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 1l }) })).when(nativeConnection).closePipeline();
		super.testHDel();
	}

	@Test
	public void testHExistsBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { true }) })).when(nativeConnection)
				.closePipeline();
		super.testHExistsBytes();
	}

	@Test
	public void testHExists() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { true }) })).when(nativeConnection)
				.closePipeline();
		super.testHExists();
	}

	@Test
	public void testHGetBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { barBytes }) })).when(nativeConnection)
				.closePipeline();
		super.testHGetBytes();
	}

	@Test
	public void testHGet() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { barBytes }) })).when(nativeConnection)
				.closePipeline();
		super.testHGet();
	}

	@Test
	public void testHGetAllBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { bytesMap }) })).when(nativeConnection)
				.closePipeline();
		super.testHGetAllBytes();
	}

	@Test
	public void testHGetAll() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { bytesMap }) })).when(nativeConnection)
				.closePipeline();
		super.testHGetAll();
	}

	@Test
	public void testHIncrByBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 3l }) })).when(nativeConnection).closePipeline();
		super.testHIncrByBytes();
	}

	@Test
	public void testHIncrBy() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 3l }) })).when(nativeConnection).closePipeline();
		super.testHIncrBy();
	}

	@Test
	public void testHIncrByDoubleBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 3d }) })).when(nativeConnection).closePipeline();
		super.testHIncrByDoubleBytes();
	}

	@Test
	public void testHIncrByDouble() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 3d }) })).when(nativeConnection).closePipeline();
		super.testHIncrByDouble();
	}

	@Test
	public void testHKeysBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { bytesSet }) })).when(nativeConnection)
				.closePipeline();
		super.testHKeysBytes();
	}

	@Test
	public void testHKeys() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { bytesSet }) })).when(nativeConnection)
				.closePipeline();
		super.testHKeys();
	}

	@Test
	public void testHLenBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 3l }) })).when(nativeConnection).closePipeline();
		super.testHLenBytes();
	}

	@Test
	public void testHLen() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 3l }) })).when(nativeConnection).closePipeline();
		super.testHLen();
	}

	@Test
	public void testHMGetBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { bytesList }) })).when(nativeConnection)
				.closePipeline();
		super.testHMGetBytes();
	}

	@Test
	public void testHMGet() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { bytesList }) })).when(nativeConnection)
				.closePipeline();
		super.testHMGet();
	}

	@Test
	public void testHSetBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { true }) })).when(nativeConnection)
				.closePipeline();
		super.testHSetBytes();
	}

	@Test
	public void testHSet() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { true }) })).when(nativeConnection)
				.closePipeline();
		super.testHSet();
	}

	@Test
	public void testHSetNXBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { true }) })).when(nativeConnection)
				.closePipeline();
		super.testHSetNXBytes();
	}

	@Test
	public void testHSetNX() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { true }) })).when(nativeConnection)
				.closePipeline();
		super.testHSetNX();
	}

	@Test
	public void testHValsBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { bytesList }) })).when(nativeConnection)
				.closePipeline();
		super.testHValsBytes();
	}

	@Test
	public void testHVals() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { bytesList }) })).when(nativeConnection)
				.closePipeline();
		super.testHVals();
	}

	@Test
	public void testIncrBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 2l }) })).when(nativeConnection).closePipeline();
		super.testIncrBytes();
	}

	@Test
	public void testIncr() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 2l }) })).when(nativeConnection).closePipeline();
		super.testIncr();
	}

	@Test
	public void testIncrByBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 2l }) })).when(nativeConnection).closePipeline();
		super.testIncrByBytes();
	}

	@Test
	public void testIncrBy() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 2l }) })).when(nativeConnection).closePipeline();
		super.testIncrBy();
	}

	@Test
	public void testIncrByDoubleBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 2d }) })).when(nativeConnection).closePipeline();
		super.testIncrByDoubleBytes();
	}

	@Test
	public void testIncrByDouble() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 2d }) })).when(nativeConnection).closePipeline();
		super.testIncrByDouble();
	}

	@Test
	public void testInfo() {
		Properties props = new Properties();
		props.put("foo", "bar");
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { props }) })).when(nativeConnection)
				.closePipeline();
		super.testInfo();
	}

	@Test
	public void testInfoBySection() {
		Properties props = new Properties();
		props.put("foo", "bar");
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { props }) })).when(nativeConnection)
				.closePipeline();
		super.testInfoBySection();
	}

	@Test
	public void testKeysBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { bytesSet }) })).when(nativeConnection)
				.closePipeline();
		super.testKeysBytes();
	}

	@Test
	public void testKeys() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { bytesSet }) })).when(nativeConnection)
				.closePipeline();
		super.testKeys();
	}

	@Test
	public void testLastSave() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 6l }) })).when(nativeConnection).closePipeline();
		super.testLastSave();
	}

	@Test
	public void testLIndexBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { barBytes }) })).when(nativeConnection)
				.closePipeline();
		super.testLIndexBytes();
	}

	@Test
	public void testLIndex() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { barBytes }) })).when(nativeConnection)
				.closePipeline();
		super.testLIndex();
	}

	@Test
	public void testLInsertBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 8l }) })).when(nativeConnection).closePipeline();
		super.testLInsertBytes();
	}

	@Test
	public void testLInsert() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 8l }) })).when(nativeConnection).closePipeline();
		super.testLInsert();
	}

	@Test
	public void testLLenBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 8l }) })).when(nativeConnection).closePipeline();
		super.testLLenBytes();
	}

	@Test
	public void testLLen() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 8l }) })).when(nativeConnection).closePipeline();
		super.testLLen();
	}

	@Test
	public void testLPopBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { barBytes }) })).when(nativeConnection)
				.closePipeline();
		actual.add(connection.lPop(fooBytes));
		verifyResults(Arrays.asList(new Object[] { barBytes }));
	}

	@Test
	public void testLPop() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { barBytes }) })).when(nativeConnection)
				.closePipeline();
		super.testLPop();
	}

	@Test
	public void testLPushBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 8l }) })).when(nativeConnection).closePipeline();
		super.testLPushBytes();
	}

	@Test
	public void testLPush() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 8l }) })).when(nativeConnection).closePipeline();
		super.testLPush();
	}

	@Test
	public void testLPushXBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 8l }) })).when(nativeConnection).closePipeline();
		super.testLPushXBytes();
	}

	@Test
	public void testLPushX() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 8l }) })).when(nativeConnection).closePipeline();
		super.testLPushX();
	}

	@Test
	public void testLRangeBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { bytesList }) })).when(nativeConnection)
				.closePipeline();
		super.testLRangeBytes();
	}

	@Test
	public void testLRange() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { bytesList }) })).when(nativeConnection)
				.closePipeline();
		super.testLRange();
	}

	@Test
	public void testLRemBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 8l }) })).when(nativeConnection).closePipeline();
		super.testLRemBytes();
	}

	@Test
	public void testLRem() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 8l }) })).when(nativeConnection).closePipeline();
		super.testLRem();
	}

	@Test
	public void testMGetBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { bytesList }) })).when(nativeConnection)
				.closePipeline();
		super.testMGetBytes();
	}

	@Test
	public void testMGet() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { bytesList }) })).when(nativeConnection)
				.closePipeline();
		super.testMGet();
	}

	@Test
	public void testMSetNXBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { true }) })).when(nativeConnection)
				.closePipeline();
		super.testMSetNXBytes();
	}

	@Test
	public void testMSetNXString() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { true }) })).when(nativeConnection)
				.closePipeline();
		super.testMSetNXString();
	}

	@Test
	public void testPersistBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { true }) })).when(nativeConnection)
				.closePipeline();
		super.testPersistBytes();
	}

	@Test
	public void testPersist() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { true }) })).when(nativeConnection)
				.closePipeline();
		super.testPersist();
	}

	@Test
	public void testMoveBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { true }) })).when(nativeConnection)
				.closePipeline();
		super.testMoveBytes();
	}

	@Test
	public void testMove() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { true }) })).when(nativeConnection)
				.closePipeline();
		super.testMove();
	}

	@Test
	public void testPing() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { "pong" }) })).when(nativeConnection)
				.closePipeline();
		super.testPing();
	}

	@Test
	public void testPublishBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 2l }) })).when(nativeConnection).closePipeline();
		super.testPublishBytes();
	}

	@Test
	public void testPublish() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 2l }) })).when(nativeConnection).closePipeline();
		super.testPublish();
	}

	@Test
	public void testRandomKey() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { fooBytes }) })).when(nativeConnection)
				.closePipeline();
		super.testRandomKey();
	}

	@Test
	public void testRenameNXBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { true }) })).when(nativeConnection)
				.closePipeline();
		super.testRenameNXBytes();
	}

	@Test
	public void testRenameNX() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { true }) })).when(nativeConnection)
				.closePipeline();
		super.testRenameNX();
	}

	@Test
	public void testRPopBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { barBytes }) })).when(nativeConnection)
				.closePipeline();
		super.testRPopBytes();
	}

	@Test
	public void testRPop() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { barBytes }) })).when(nativeConnection)
				.closePipeline();
		super.testRPop();
	}

	@Test
	public void testRPopLPushBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { barBytes }) })).when(nativeConnection)
				.closePipeline();
		super.testRPopLPushBytes();
	}

	@Test
	public void testRPopLPush() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { barBytes }) })).when(nativeConnection)
				.closePipeline();
		super.testRPopLPush();
	}

	@Test
	public void testRPushBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 4l }) })).when(nativeConnection).closePipeline();
		super.testRPushBytes();
	}

	@Test
	public void testRPush() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 4l }) })).when(nativeConnection).closePipeline();
		super.testRPush();
	}

	@Test
	public void testRPushXBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 4l }) })).when(nativeConnection).closePipeline();
		super.testRPushXBytes();
	}

	@Test
	public void testRPushX() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 4l }) })).when(nativeConnection).closePipeline();
		super.testRPushX();
	}

	@Test
	public void testSAddBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 1l }) })).when(nativeConnection).closePipeline();
		super.testSAddBytes();
	}

	@Test
	public void testSAdd() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 1l }) })).when(nativeConnection).closePipeline();
		super.testSAdd();
	}

	@Test
	public void testSCardBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 4l }) })).when(nativeConnection).closePipeline();
		super.testSCardBytes();
	}

	@Test
	public void testSCard() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 4l }) })).when(nativeConnection).closePipeline();
		super.testSCard();
	}

	@Test
	public void testSDiffBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { bytesSet }) })).when(nativeConnection)
				.closePipeline();
		super.testSDiffBytes();
	}

	@Test
	public void testSDiff() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { bytesSet }) })).when(nativeConnection)
				.closePipeline();
		super.testSDiff();
	}

	@Test
	public void testSDiffStoreBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 3l }) })).when(nativeConnection).closePipeline();
		super.testSDiffStoreBytes();
	}

	@Test
	public void testSDiffStore() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 3l }) })).when(nativeConnection).closePipeline();
		super.testSDiffStore();
	}

	@Test
	public void testSetNXBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { true }) })).when(nativeConnection)
				.closePipeline();
		super.testSetNXBytes();
	}

	@Test
	public void testSetNX() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { true }) })).when(nativeConnection)
				.closePipeline();
		super.testSetNX();
	}

	@Test
	public void testSInterBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { bytesSet }) })).when(nativeConnection)
				.closePipeline();
		super.testSInterBytes();
	}

	@Test
	public void testSInter() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { bytesSet }) })).when(nativeConnection)
				.closePipeline();
		super.testSInter();
	}

	@Test
	public void testSInterStoreBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 3l }) })).when(nativeConnection).closePipeline();
		super.testSInterStoreBytes();
	}

	@Test
	public void testSInterStore() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 3l }) })).when(nativeConnection).closePipeline();
		super.testSInterStore();
	}

	@Test
	public void testSIsMemberBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { true }) })).when(nativeConnection)
				.closePipeline();
		super.testSIsMemberBytes();
	}

	@Test
	public void testSIsMember() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { true }) })).when(nativeConnection)
				.closePipeline();
		super.testSIsMember();
	}

	@Test
	public void testSMembersBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { bytesSet }) })).when(nativeConnection)
				.closePipeline();
		super.testSMembersBytes();
	}

	@Test
	public void testSMembers() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { bytesSet }) })).when(nativeConnection)
				.closePipeline();
		super.testSMembers();
	}

	@Test
	public void testSMoveBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { true }) })).when(nativeConnection)
				.closePipeline();
		super.testSMoveBytes();
	}

	@Test
	public void testSMove() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { true }) })).when(nativeConnection)
				.closePipeline();
		super.testSMove();
	}

	@Test
	public void testSortStoreBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 3l }) })).when(nativeConnection).closePipeline();
		super.testSortStoreBytes();
	}

	@Test
	public void testSortStore() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 3l }) })).when(nativeConnection).closePipeline();
		super.testSortStore();
	}

	@Test
	public void testSortBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { bytesList }) })).when(nativeConnection)
				.closePipeline();
		super.testSortBytes();
	}

	@Test
	public void testSort() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { bytesList }) })).when(nativeConnection)
				.closePipeline();
		super.testSort();
	}

	@Test
	public void testSPopBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { barBytes }) })).when(nativeConnection)
				.closePipeline();
		super.testSPopBytes();
	}

	@Test
	public void testSPop() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { barBytes }) })).when(nativeConnection)
				.closePipeline();
		super.testSPop();
	}

	@Test
	public void testSRandMemberBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { barBytes }) })).when(nativeConnection)
				.closePipeline();
		super.testSRandMemberBytes();
	}

	@Test
	public void testSRandMember() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { barBytes }) })).when(nativeConnection)
				.closePipeline();
		super.testSRandMember();
	}

	@Test
	public void testSRandMemberCountBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { bytesList }) })).when(nativeConnection)
				.closePipeline();
		super.testSRandMemberCountBytes();
	}

	@Test
	public void testSRandMemberCount() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { bytesList }) })).when(nativeConnection)
				.closePipeline();
		super.testSRandMemberCount();
	}

	@Test
	public void testSRemBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 1l }) })).when(nativeConnection).closePipeline();
		super.testSRemBytes();
	}

	@Test
	public void testSRem() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 1l }) })).when(nativeConnection).closePipeline();
		super.testSRem();
	}

	@Test
	public void testStrLenBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 5l }) })).when(nativeConnection).closePipeline();
		super.testStrLenBytes();
	}

	@Test
	public void testStrLen() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 5l }) })).when(nativeConnection).closePipeline();
		super.testStrLen();
	}

	@Test
	public void testBitCountBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 5l }) })).when(nativeConnection).closePipeline();
		super.testBitCountBytes();
	}

	@Test
	public void testBitCount() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 5l }) })).when(nativeConnection).closePipeline();
		super.testBitCount();
	}

	@Test
	public void testBitCountRangeBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 5l }) })).when(nativeConnection).closePipeline();
		super.testBitCountRangeBytes();
	}

	@Test
	public void testBitCountRange() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 5l }) })).when(nativeConnection).closePipeline();
		super.testBitCountRange();
	}

	@Test
	public void testBitOpBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 5l }) })).when(nativeConnection).closePipeline();
		super.testBitOpBytes();
	}

	@Test
	public void testBitOp() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 5l }) })).when(nativeConnection).closePipeline();
		super.testBitOp();
	}

	@Test
	public void testSUnionBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { bytesSet }) })).when(nativeConnection)
				.closePipeline();
		super.testSUnionBytes();
	}

	@Test
	public void testSUnion() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { bytesSet }) })).when(nativeConnection)
				.closePipeline();
		super.testSUnion();
	}

	@Test
	public void testSUnionStoreBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 5l }) })).when(nativeConnection).closePipeline();
		super.testSUnionStoreBytes();
	}

	@Test
	public void testSUnionStore() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 5l }) })).when(nativeConnection).closePipeline();
		super.testSUnionStore();
	}

	@Test
	public void testTtlBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 5l }) })).when(nativeConnection).closePipeline();
		super.testTtlBytes();
	}

	@Test
	public void testTtl() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 5l }) })).when(nativeConnection).closePipeline();
		super.testTtl();
	}

	// DATAREDIS-526
	@Override
	public void testTtlWithTimeUnit() {

		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 5L }) })).when(nativeConnection).closePipeline();
		super.testTtlWithTimeUnit();
	}

	@Test
	public void testTypeBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { DataType.HASH }) })).when(nativeConnection)
				.closePipeline();
		super.testTypeBytes();
	}

	@Test
	public void testType() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { DataType.HASH }) })).when(nativeConnection)
				.closePipeline();
		super.testType();
	}

	@Test
	public void testZAddBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { true }) })).when(nativeConnection)
				.closePipeline();
		super.testZAddBytes();
	}

	@Test
	public void testZAdd() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { true }) })).when(nativeConnection)
				.closePipeline();
		super.testZAdd();
	}

	@Test
	public void testZAddMultipleBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 1l }) })).when(nativeConnection).closePipeline();
		super.testZAddMultipleBytes();
	}

	@Test
	public void testZAddMultiple() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 1l }) })).when(nativeConnection).closePipeline();
		super.testZAddMultiple();
	}

	@Test
	public void testZCardBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 5l }) })).when(nativeConnection).closePipeline();
		super.testZCardBytes();
	}

	@Test
	public void testZCard() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 5l }) })).when(nativeConnection).closePipeline();
		super.testZCard();
	}

	@Test
	public void testZCountBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 5l }) })).when(nativeConnection).closePipeline();
		super.testZCountBytes();
	}

	@Test
	public void testZCount() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 5l }) })).when(nativeConnection).closePipeline();
		super.testZCount();
	}

	@Test
	public void testZIncrByBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 3d }) })).when(nativeConnection).closePipeline();
		super.testZIncrByBytes();
	}

	@Test
	public void testZIncrBy() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 3d }) })).when(nativeConnection).closePipeline();
		super.testZIncrBy();
	}

	@Test
	public void testZInterStoreAggWeightsBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 5l }) })).when(nativeConnection).closePipeline();
		super.testZInterStoreAggWeightsBytes();
	}

	@Test
	public void testZInterStoreAggWeights() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 5l }) })).when(nativeConnection).closePipeline();
		super.testZInterStoreAggWeights();
	}

	@Test
	public void testZInterStoreBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 5l }) })).when(nativeConnection).closePipeline();
		super.testZInterStoreBytes();
	}

	@Test
	public void testZInterStore() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 5l }) })).when(nativeConnection).closePipeline();
		super.testZInterStore();
	}

	@Test
	public void testZRangeBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { bytesSet }) })).when(nativeConnection)
				.closePipeline();
		super.testZRangeBytes();
	}

	@Test
	public void testZRange() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { bytesSet }) })).when(nativeConnection)
				.closePipeline();
		super.testZRange();
	}

	@Test
	public void testZRangeByScoreOffsetCountBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { bytesSet }) })).when(nativeConnection)
				.closePipeline();
		super.testZRangeByScoreOffsetCountBytes();
	}

	@Test
	public void testZRangeByScoreOffsetCount() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { bytesSet }) })).when(nativeConnection)
				.closePipeline();
		super.testZRangeByScoreOffsetCount();
	}

	@Test
	public void testZRangeByScoreBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { bytesSet }) })).when(nativeConnection)
				.closePipeline();
		super.testZRangeByScoreBytes();
	}

	@Test
	public void testZRangeByScore() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { bytesSet }) })).when(nativeConnection)
				.closePipeline();
		super.testZRangeByScore();
	}

	@Test
	public void testZRangeByScoreWithScoresOffsetCountBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { tupleSet }) })).when(nativeConnection)
				.closePipeline();
		super.testZRangeByScoreWithScoresOffsetCountBytes();
	}

	@Test
	public void testZRangeByScoreWithScoresOffsetCount() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { tupleSet }) })).when(nativeConnection)
				.closePipeline();
		super.testZRangeByScoreWithScoresOffsetCount();
	}

	@Test
	public void testZRangeByScoreWithScoresBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { tupleSet }) })).when(nativeConnection)
				.closePipeline();
		super.testZRangeByScoreWithScoresBytes();
	}

	@Test
	public void testZRangeByScoreWithScores() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { tupleSet }) })).when(nativeConnection)
				.closePipeline();
		super.testZRangeByScoreWithScores();
	}

	@Test
	public void testZRangeWithScoresBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { tupleSet }) })).when(nativeConnection)
				.closePipeline();
		super.testZRangeWithScoresBytes();
	}

	@Test
	public void testZRangeWithScores() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { tupleSet }) })).when(nativeConnection)
				.closePipeline();
		super.testZRangeWithScores();
	}

	@Test
	public void testZRevRangeByScoreOffsetCountBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { bytesSet }) })).when(nativeConnection)
				.closePipeline();
		super.testZRevRangeByScoreOffsetCountBytes();
	}

	@Test
	public void testZRevRangeByScoreOffsetCount() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { bytesSet }) })).when(nativeConnection)
				.closePipeline();
		super.testZRevRangeByScoreOffsetCount();
	}

	@Test
	public void testZRevRangeByScoreBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { bytesSet }) })).when(nativeConnection)
				.closePipeline();
		super.testZRevRangeByScoreBytes();
	}

	@Test
	public void testZRevRangeByScore() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { bytesSet }) })).when(nativeConnection)
				.closePipeline();
		super.testZRevRangeByScore();
	}

	@Test
	public void testZRevRangeByScoreWithScoresOffsetCountBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { tupleSet }) })).when(nativeConnection)
				.closePipeline();
		super.testZRevRangeByScoreWithScoresOffsetCountBytes();
	}

	@Test
	public void testZRevRangeByScoreWithScoresOffsetCount() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { tupleSet }) })).when(nativeConnection)
				.closePipeline();
		super.testZRevRangeByScoreWithScoresOffsetCount();
	}

	@Test
	public void testZRevRangeByScoreWithScoresBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { tupleSet }) })).when(nativeConnection)
				.closePipeline();
		super.testZRevRangeByScoreWithScoresBytes();
	}

	@Test
	public void testZRevRangeByScoreWithScores() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { tupleSet }) })).when(nativeConnection)
				.closePipeline();
		super.testZRevRangeByScoreWithScores();
	}

	@Test
	public void testZRankBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 5l }) })).when(nativeConnection).closePipeline();
		super.testZRankBytes();
	}

	@Test
	public void testZRank() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 5l }) })).when(nativeConnection).closePipeline();
		super.testZRank();
	}

	@Test
	public void testZRemBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 1l }) })).when(nativeConnection).closePipeline();
		super.testZRemBytes();
	}

	@Test
	public void testZRem() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 1l }) })).when(nativeConnection).closePipeline();
		super.testZRem();
	}

	@Test
	public void testZRemRangeBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 5l }) })).when(nativeConnection).closePipeline();
		super.testZRemRangeBytes();
	}

	@Test
	public void testZRemRange() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 5l }) })).when(nativeConnection).closePipeline();
		super.testZRemRange();
	}

	@Test
	public void testZRemRangeByScoreBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 5l }) })).when(nativeConnection).closePipeline();
		super.testZRemRangeByScoreBytes();
	}

	@Test
	public void testZRemRangeByScore() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 5l }) })).when(nativeConnection).closePipeline();
		super.testZRemRangeByScore();
	}

	@Test
	public void testZRevRangeBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { bytesSet }) })).when(nativeConnection)
				.closePipeline();
		super.testZRevRangeBytes();
	}

	@Test
	public void testZRevRange() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { bytesSet }) })).when(nativeConnection)
				.closePipeline();
		super.testZRevRange();
	}

	@Test
	public void testZRevRangeWithScoresBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { tupleSet }) })).when(nativeConnection)
				.closePipeline();
		super.testZRevRangeWithScoresBytes();
	}

	@Test
	public void testZRevRangeWithScores() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { tupleSet }) })).when(nativeConnection)
				.closePipeline();
		super.testZRevRangeWithScores();
	}

	@Test
	public void testZRevRankBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 5l }) })).when(nativeConnection).closePipeline();
		super.testZRevRankBytes();
	}

	@Test
	public void testZRevRank() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 5l }) })).when(nativeConnection).closePipeline();
		super.testZRevRank();
	}

	@Test
	public void testZScoreBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 3d }) })).when(nativeConnection).closePipeline();
		super.testZScoreBytes();
	}

	@Test
	public void testZScore() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 3d }) })).when(nativeConnection).closePipeline();
		super.testZScore();
	}

	@Test
	public void testZUnionStoreAggWeightsBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 5l }) })).when(nativeConnection).closePipeline();
		super.testZUnionStoreAggWeightsBytes();
	}

	@Test
	public void testZUnionStoreAggWeights() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 5l }) })).when(nativeConnection).closePipeline();
		super.testZUnionStoreAggWeights();
	}

	@Test
	public void testZUnionStoreBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 5l }) })).when(nativeConnection).closePipeline();
		super.testZUnionStoreBytes();
	}

	@Test
	public void testZUnionStore() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 5l }) })).when(nativeConnection).closePipeline();
		super.testZUnionStore();
	}

	@Test
	public void testPExpireBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { true }) })).when(nativeConnection)
				.closePipeline();
		super.testPExpireBytes();
	}

	@Test
	public void testPExpire() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { true }) })).when(nativeConnection)
				.closePipeline();
		super.testPExpire();
	}

	@Test
	public void testPExpireAtBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { true }) })).when(nativeConnection)
				.closePipeline();
		super.testPExpireAtBytes();
	}

	@Test
	public void testPExpireAt() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { true }) })).when(nativeConnection)
				.closePipeline();
		super.testPExpireAt();
	}

	@Test
	public void testPTtlBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 5l }) })).when(nativeConnection).closePipeline();
		super.testPTtlBytes();
	}

	@Test
	public void testPTtl() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { 5l }) })).when(nativeConnection).closePipeline();
		super.testPTtl();
	}

	@Test
	public void testDump() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { barBytes }) })).when(nativeConnection)
				.closePipeline();
		super.testDump();
	}

	@Test
	public void testScriptLoadBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { "foo" }) })).when(nativeConnection)
				.closePipeline();
		super.testScriptLoadBytes();
	}

	@Test
	public void testScriptLoad() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { "foo" }) })).when(nativeConnection)
				.closePipeline();
		super.testScriptLoad();
	}

	@Test
	public void testScriptExists() {
		List<Boolean> results = Collections.singletonList(true);
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { results }) })).when(nativeConnection)
				.closePipeline();
		super.testScriptExists();
	}

	@Test
	public void testEvalBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { "foo" }) })).when(nativeConnection)
				.closePipeline();
		super.testEvalBytes();
	}

	@Test
	public void testEval() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { "foo" }) })).when(nativeConnection)
				.closePipeline();
		super.testEval();
	}

	@Test
	public void testEvalShaBytes() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { "foo" }) })).when(nativeConnection)
				.closePipeline();
		super.testEvalShaBytes();
	}

	@Test
	public void testEvalSha() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { "foo" }) })).when(nativeConnection)
				.closePipeline();
		super.testEvalSha();
	}

	@Test
	public void testExecute() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { "foo" }) })).when(nativeConnection)
				.closePipeline();
		super.testExecute();
	}

	@Test
	public void testExecuteByteArgs() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { "foo" }) })).when(nativeConnection)
				.closePipeline();
		super.testExecuteByteArgs();
	}

	@Test
	public void testExecuteStringArgs() {
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { "foo" }) })).when(nativeConnection)
				.closePipeline();
		super.testExecuteStringArgs();
	}

	@Test
	public void testTxResultsNotSameSizeAsResults() {
		// Only call one method, but return 2 results from nativeConnection.exec()
		// Emulates scenario where user has called some methods directly on the native connection
		// while tx is open
		doReturn(Arrays.asList(new Object[] { Arrays.asList(new Object[] { barBytes, 3l }) })).when(nativeConnection)
				.closePipeline();
		doReturn(barBytes).when(nativeConnection).get(fooBytes);
		connection.get(foo);
		verifyResults(Arrays.asList(new Object[] { barBytes, 3l }));
	}

	@Test
	public void testTwoTxs() {
		doReturn(Arrays
				.asList(new Object[] { Arrays.asList(new Object[] { barBytes }), Arrays.asList(new Object[] { fooBytes }) }))
						.when(nativeConnection).closePipeline();
		connection.get(foo);
		connection.exec();
		connection.get(bar);
		connection.exec();
		List<Object> results = connection.closePipeline();
		assertEquals(
				Arrays.asList(new Object[] { Arrays.asList(new Object[] { bar }), Arrays.asList(new Object[] { foo }) }),
				results);
	}

	@Test // DATAREDIS-438
	public void testGeoAddBytes() {

		doReturn(Arrays.asList(Collections.singletonList(1L))).when(nativeConnection).closePipeline();
		super.testGeoAddBytes();
	}

	@Test // DATAREDIS-438
	public void testGeoAdd() {

		doReturn(Arrays.asList(Collections.singletonList(1L))).when(nativeConnection).closePipeline();
		super.testGeoAddBytes();
	}

	@Test // DATAREDIS-438
	public void testGeoAddWithGeoLocationBytes() {

		doReturn(Arrays.asList(Collections.singletonList(1L))).when(nativeConnection).closePipeline();
		super.testGeoAddWithGeoLocationBytes();
	}

	@Test // DATAREDIS-438
	public void testGeoAddWithGeoLocation() {

		doReturn(Arrays.asList(Collections.singletonList(1L))).when(nativeConnection).closePipeline();
		super.testGeoAddWithGeoLocation();
	}

	@Test // DATAREDIS-438
	public void testGeoAddCoordinateMapBytes() {

		doReturn(Arrays.asList(Collections.singletonList(1L))).when(nativeConnection).closePipeline();
		super.testGeoAddCoordinateMapBytes();
	}

	@Test // DATAREDIS-438
	public void testGeoAddCoordinateMap() {

		doReturn(Arrays.asList(Collections.singletonList(1L))).when(nativeConnection).closePipeline();
		super.testGeoAddCoordinateMap();
	}

	@Test // DATAREDIS-438
	public void testGeoAddWithIterableOfGeoLocationBytes() {

		doReturn(Arrays.asList(Collections.singletonList(1L))).when(nativeConnection).closePipeline();
		super.testGeoAddWithIterableOfGeoLocationBytes();
	}

	@Test // DATAREDIS-438
	public void testGeoAddWithIterableOfGeoLocation() {

		doReturn(Arrays.asList(Collections.singletonList(1L))).when(nativeConnection).closePipeline();
		super.testGeoAddWithIterableOfGeoLocation();
	}

	@Test // DATAREDIS-438
	public void testGeoDistBytes() {

		doReturn(Arrays.asList(Arrays.asList(new Distance(102121.12d, DistanceUnit.METERS)))).when(nativeConnection)
				.closePipeline();
		super.testGeoDistBytes();
	}

	@Test // DATAREDIS-438
	public void testGeoDist() {

		doReturn(Arrays.asList(Arrays.asList(new Distance(102121.12d, DistanceUnit.METERS)))).when(nativeConnection)
				.closePipeline();
		super.testGeoDist();
	}

	@Test // DATAREDIS-438
	public void testGeoHashBytes() {

		doReturn(Arrays.asList(Arrays.asList(Collections.singletonList(bar)))).when(nativeConnection).closePipeline();
		super.testGeoHashBytes();
	}

	@Test // DATAREDIS-438
	public void testGeoHash() {

		doReturn(Arrays.asList(Arrays.asList(Collections.singletonList(bar)))).when(nativeConnection).closePipeline();
		super.testGeoHash();
	}

	@Test // DATAREDIS-438
	public void testGeoPosBytes() {

		doReturn(Arrays.asList(Arrays.asList(points))).when(nativeConnection).closePipeline();
		super.testGeoPosBytes();
	}

	@Test // DATAREDIS-438
	public void testGeoPos() {

		doReturn(Arrays.asList(Arrays.asList(points))).when(nativeConnection).closePipeline();
		super.testGeoPos();
	}

	@Test // DATAREDIS-438
	public void testGeoRadiusWithoutParamBytes() {

		doReturn(Arrays.asList(Arrays.asList(geoResults))).when(nativeConnection).closePipeline();
		super.testGeoRadiusWithoutParamBytes();
	}

	@Test // DATAREDIS-438
	public void testGeoRadiusWithoutParam() {

		doReturn(Arrays.asList(Arrays.asList(geoResults))).when(nativeConnection).closePipeline();
		super.testGeoRadiusWithoutParam();
	}

	@Test // DATAREDIS-438
	public void testGeoRadiusWithDistBytes() {

		doReturn(Arrays.asList(Arrays.asList(geoResults))).when(nativeConnection).closePipeline();
		super.testGeoRadiusWithDistBytes();
	}

	@Test // DATAREDIS-438
	public void testGeoRadiusWithDist() {

		doReturn(Arrays.asList(Arrays.asList(geoResults))).when(nativeConnection).closePipeline();
		super.testGeoRadiusWithDist();
	}

	@Test // DATAREDIS-438
	public void testGeoRadiusWithCoordAndDescBytes() {

		doReturn(Arrays.asList(Arrays.asList(geoResults))).when(nativeConnection).closePipeline();
		super.testGeoRadiusWithCoordAndDescBytes();
	}

	@Test // DATAREDIS-438
	public void testGeoRadiusWithCoordAndDesc() {

		doReturn(Arrays.asList(Arrays.asList(geoResults))).when(nativeConnection).closePipeline();
		super.testGeoRadiusWithCoordAndDesc();
	}

	@Test // DATAREDIS-438
	public void testGeoRadiusByMemberWithoutParamBytes() {

		doReturn(Arrays.asList(Arrays.asList(geoResults))).when(nativeConnection).closePipeline();
		super.testGeoRadiusByMemberWithoutParamBytes();
	}

	@Test // DATAREDIS-438
	public void testGeoRadiusByMemberWithoutParam() {

		doReturn(Arrays.asList(Arrays.asList(geoResults))).when(nativeConnection).closePipeline();
		super.testGeoRadiusByMemberWithoutParam();
	}

	@Test // DATAREDIS-438
	public void testGeoRadiusByMemberWithDistAndAscBytes() {

		doReturn(Arrays.asList(Arrays.asList(geoResults))).when(nativeConnection).closePipeline();
		super.testGeoRadiusByMemberWithDistAndAscBytes();
	}

	@Test // DATAREDIS-438
	public void testGeoRadiusByMemberWithDistAndAsc() {

		doReturn(Arrays.asList(Arrays.asList(geoResults))).when(nativeConnection).closePipeline();
		super.testGeoRadiusByMemberWithDistAndAsc();
	}

	@Test // DATAREDIS-438
	public void testGeoRadiusByMemberWithCoordAndCountBytes() {

		doReturn(Arrays.asList(Arrays.asList(geoResults))).when(nativeConnection).closePipeline();
		super.testGeoRadiusByMemberWithCoordAndCountBytes();
	}

	@Test // DATAREDIS-438
	public void testGeoRadiusByMemberWithCoordAndCount() {

		doReturn(Arrays.asList(Arrays.asList(geoResults))).when(nativeConnection).closePipeline();
		super.testGeoRadiusByMemberWithCoordAndCount();
	}

	@SuppressWarnings("unchecked")
	protected List<Object> getResults() {
		connection.exec();
		List<Object> txResults = (List<Object>) connection.closePipeline().get(0);
		return txResults;
	}
}
