/*
 * Copyright 2011-2013 the original author or authors.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.springframework.data.redis.SpinBarrier.waitFor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.data.redis.TestCondition;
import org.springframework.data.redis.connection.RedisStringCommands.BitOperation;
import org.springframework.data.redis.connection.RedisZSetCommands.Aggregate;
import org.springframework.data.redis.connection.RedisZSetCommands.Tuple;
import org.springframework.data.redis.connection.StringRedisConnection.StringTuple;
import org.springframework.test.annotation.IfProfileValue;

/**
 * Base test class for integration tests that execute each operation of a
 * Connection while a pipeline is open, verifying that the operations return
 * null and the proper values are returned when closing the pipeline.
 * <p>
 * Pipelined results are generally native to the provider and not transformed by
 * our {@link RedisConnection}, so this test overrides
 * {@link AbstractConnectionIntegrationTests} when result types are different
 *
 * @author Jennifer Hickey
 *
 */
abstract public class AbstractConnectionPipelineIntegrationTests extends
		AbstractConnectionIntegrationTests {

	/**
	 * Individual results from closePipeline should be converted from List to
	 * LinkedHashSet
	 **/
	protected boolean convertResultToSet = false;

	/**
	 * Individual results from closePipeline should be converted to
	 * {@link Tuple}s
	 **/
	protected boolean convertResultToTuples = false;

	@Before
	public void setUp() {
		super.setUp();
		initConnection();
	}

	@Ignore
	public void testByteValue() {
	}

	@Ignore
	public void testNullKey() throws Exception {
	}

	@Ignore
	public void testNullValue() throws Exception {
	}

	@Ignore
	public void testHashNullKey() throws Exception {
	}

	@Ignore
	public void testHashNullValue() throws Exception {
	}

	@Ignore("Pub/Sub not supported while pipelining")
	public void testPubSubWithNamedChannels() throws Exception {
	}

	@Ignore("Pub/Sub not supported while pipelining")
	public void testPubSubWithPatterns() throws Exception {
	}

	@Test(expected = RedisPipelineException.class)
	public void exceptionExecuteNative() throws Exception {
		connection.execute("ZadD", getClass() + "#foo\t0.90\titem");
		getResults();
	}

	@Test
	public void testExecute() {
		connection.set("foo", "bar");
		actual.add(connection.execute("GET", "foo"));
		verifyResults(Arrays.asList(new Object[] { "bar" }), actual);
	}

	@IfProfileValue(name = "redisVersion", value = "2.6")
	@Test
	public void testPExpire() {
		connection.set("exp", "true");
		actual.add(connection.pExpire("exp", 100));
		verifyResults(Arrays.asList(new Object[] { 1l }), actual);
		assertTrue(waitFor(new KeyExpired("exp"), 1000l));
	}

	@IfProfileValue(name = "redisVersion", value = "2.6")
	@Test
	public void testPExpireKeyNotExists() {
		actual.add(connection.pExpire("nonexistent", 100));
		verifyResults(Arrays.asList(new Object[] { 0l }), actual);
	}

	@IfProfileValue(name = "redisVersion", value = "2.6")
	@Test
	public void testPExpireAt() {
		connection.set("exp2", "true");
		actual.add(connection.pExpireAt("exp2", System.currentTimeMillis() + 200));
		verifyResults(Arrays.asList(new Object[] { 1l }), actual);
		assertTrue(waitFor(new KeyExpired("exp2"), 1000l));
	}

	@IfProfileValue(name = "redisVersion", value = "2.6")
	@Test
	public void testPExpireAtKeyNotExists() {
		actual.add(connection.pExpireAt("nonexistent", System.currentTimeMillis() + 200));
		verifyResults(Arrays.asList(new Object[] { 0l }), actual);
	}

	@IfProfileValue(name = "redisVersion", value = "2.6")
	@Test
	public void testPTtl() {
		connection.set("whatup", "yo");
		actual.add(connection.pExpire("whatup", 9000l));
		verifyResults(Arrays.asList(new Object[] { 1l }), actual);
		assertTrue(waitFor(new TestCondition() {
			public boolean passes() {
				return (connection.pTtl("whatup") > -1);
			}
		}, 1000l));
	}

	@Test(expected=RedisPipelineException.class)
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testRestoreBadData() {
		// Use something other than dump-specific serialization
		connection.restore("testing".getBytes(), 0, "foo".getBytes());
		getResults();
	}

	@Test
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testRestoreExistingKey() {
		connection.set("testing", "12");
		connection.dump("testing".getBytes());
		List<Object> results = getResults();
		initConnection();
		connection.restore("testing".getBytes(), 0, (byte[]) results.get(1));
		try {
			getResults();
			fail("Expected pipeline exception restoring an existing key");
		}catch(RedisPipelineException e) {
		}
	}

	@Test
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testRestoreTtl() {
		connection.set("testing", "12");
		connection.dump("testing".getBytes());
		List<Object> results = getResults();
		initConnection();
		actual.add(connection.del("testing"));
		actual.add(connection.get("testing"));
		connection.restore("testing".getBytes(), 100l,  (byte[]) results.get(results.size() - 1));
		verifyResults(Arrays.asList(new Object[] { 1l, null }), actual);
		assertTrue(waitFor(new KeyExpired("testing"), 300l));
	}

	@Test
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testDumpAndRestore() {
		connection.set("testing", "12");
		connection.dump("testing".getBytes());
		List<Object> results = getResults();
		initConnection();
		actual.add(connection.del("testing"));
		actual.add((connection.get("testing")));
		connection.restore("testing".getBytes(), 0,  (byte[]) results.get(results.size() - 1));
		actual.add(connection.get("testing"));
		verifyResults(Arrays.asList(new Object[] { 1l, null, "12" }), actual);
	}

	@Test
	@IfProfileValue(name = "runLongTests", value = "true")
	public void testExpire() throws Exception {
		connection.set("exp", "true");
		actual.add(connection.expire("exp", 1));
		verifyResults(Arrays.asList(new Object[] { 1l }), actual);
		assertTrue(waitFor(new KeyExpired("exp"), 2500l));
	}

	@Test
	@IfProfileValue(name = "runLongTests", value = "true")
	public void testExpireAt() throws Exception {
		connection.set("exp2", "true");
		actual.add(connection.expireAt("exp2", System.currentTimeMillis() / 1000 + 1));
		verifyResults(Arrays.asList(new Object[] { 1l }), actual);
		assertTrue(waitFor(new KeyExpired("exp2"), 2500l));
	}

	@Test
	@IfProfileValue(name = "runLongTests", value = "true")
	public void testPersist() throws Exception {
		connection.set("exp3", "true");
		actual.add(connection.expire("exp3", 1));
		actual.add(connection.persist("exp3"));
		verifyResults(Arrays.asList(new Object[] { 1l, 1l }), actual);
		Thread.sleep(1500);
		assertTrue(connection.exists("exp3"));
	}

	@Test
	@IfProfileValue(name = "runLongTests", value = "true")
	public void testSetEx() throws Exception {
		connection.setEx("expy", 1l, "yep");
		actual.add(connection.get("expy"));
		verifyResults(Arrays.asList(new Object[] { "yep" }), actual);
		assertTrue(waitFor(new KeyExpired("expy"), 2500l));
	}

	@Test
	public void testBitSet() throws Exception {
		String key = "bitset-test";
		connection.setBit(key, 0, false);
		connection.setBit(key, 1, true);
		actual.add(connection.getBit(key, 0));
		actual.add(connection.getBit(key, 1));
		verifyResults(Arrays.asList(new Object[] { 0l, 0l, 0l, 1l }), actual);
	}

	@Test
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testBitCount() {
		String key = "bitset-test";
		connection.setBit(key, 0, false);
		connection.setBit(key, 1, true);
		connection.setBit(key, 2, true);
		actual.add(connection.bitCount(key));
		verifyResults(Arrays.asList(new Object[] { 0l, 0l, 0l, 2l }), actual);
	}

	@Test(expected=RedisPipelineException.class)
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testBitOpNotMultipleSources() {
		connection.set("key1", "abcd");
		connection.set("key2", "efgh");
		actual.add(connection.bitOp(BitOperation.NOT, "key3", "key1", "key2"));
		getResults();
	}

	@Test
	public void testDbSize() {
		connection.set("dbparam", "foo");
		assertNull(connection.dbSize());
		List<Object> results = getResults();
		assertEquals(2, results.size());
		assertTrue((Long) results.get(1) > 0);
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void testGetConfig() {
		assertNull(connection.getConfig("*"));
		List<Object> results = convertResults();
		assertEquals(1, results.size());
		assertTrue(!((List) results.get(0)).isEmpty());
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void testKeys() throws Exception {
		connection.set("keytest", "true");
		connection.set("keytest2", "true");
		connection.keys("key*");
		List<Object> results = convertResults();
		assertEquals(1, results.size());
		assertTrue(((List) results.get(0)).contains("keytest"));
	}

	@Test
	public void testRandomKey() {
		connection.set("some", "thing");
		assertNull(connection.randomKey());
		List<Object> results = convertResults();
		assertEquals(1, results.size());
		assertNotNull(results.get(0));
	}

	@Test
	public void testType() {
		connection.set("something", "yo");
		assertNull(connection.type("something"));
		List<Object> results = convertResults();
		assertEquals(1, results.size());
		assertEquals("string", results.get(0));
	}

	@Test
	public void testMSetNx() {
		Map<String, String> vals = new HashMap<String, String>();
		vals.put("height", "5");
		vals.put("width", "1");
		connection.mSetNXString(vals);
		assertNull(connection.mGet("height", "width"));
		verifyResults(Arrays.asList(new Object[] { 1l, Arrays.asList(new String[] { "5", "1" }) }),
				actual);
	}

	@Test
	public void testMSetNxFailure() {
		connection.set("height", "2");
		Map<String, String> vals = new HashMap<String, String>();
		vals.put("height", "5");
		vals.put("width", "1");
		actual.add(connection.mSetNXString(vals));
		actual.add(connection.mGet("height", "width"));
		verifyResults(Arrays.asList(new Object[] { 0l, Arrays.asList(new String[] { "2", null }) }),
				actual);
	}

	@Test
	public void testSetNx() {
		actual.add(connection.setNX("notaround", "54"));
		actual.add(connection.get("notaround"));
		actual.add(connection.setNX("notaround", "55"));
		actual.add(connection.get("notaround"));
		verifyResults(Arrays.asList(new Object[] { 1l, "54", 0l, "54" }), actual);
	}

	@Test
	public void testRenameNx() {
		connection.set("nxtest", "testit");
		actual.add(connection.renameNX("nxtest", "newnxtest"));
		actual.add(connection.get("newnxtest"));
		actual.add(connection.exists("nxtest"));
		verifyResults(Arrays.asList(new Object[] { 1l, "testit", 0l }), actual);
	}

	@Test
	public void testGetRangeSetRange() {
		connection.set("rangekey", "supercalifrag");
		actual.add(connection.getRange("rangekey", 0l, 2l));
		connection.setRange("rangekey", "ck", 2);
		actual.add(connection.get("rangekey"));
		verifyResults(Arrays.asList(new Object[] { "sup", 13l, "suckrcalifrag" }), actual);
	}

	@Test
	public void testMultiDiscard() throws Exception {
		DefaultStringRedisConnection conn2 = new DefaultStringRedisConnection(
				connectionFactory.getConnection());
		conn2.set("testitnow", "willdo");
		connection.multi();
		connection.set("testitnow2", "notok");
		connection.discard();
		connection.get("testitnow");
		List<Object> convertedResults = convertResults();
		assertEquals(Arrays.asList(new String[] { "willdo" }), convertedResults);
		connection.openPipeline();
		// Ensure we can run a new tx after discarding previous one
		testMultiExec();
	}

	@Test
	public void testMultiExec() throws Exception {
		connection.multi();
		connection.set("key", "value");
		assertNull(connection.get("key"));
		assertNull(connection.exec());
		List<Object> convertedResults = convertResults();
		assertEquals(Arrays.asList(new Object[] { "value" }), convertedResults);
	}

	@Test
	public void testUnwatch() throws Exception {
		connection.set("testitnow", "willdo");
		connection.watch("testitnow".getBytes());
		connection.unwatch();
		connection.multi();
		DefaultStringRedisConnection conn2 = new DefaultStringRedisConnection(
				connectionFactory.getConnection());
		conn2.set("testitnow", "something");
		connection.set("testitnow", "somethingelse");
		connection.get("testitnow");
		connection.exec();
		List<Object> convertedResults = convertResults();
		assertEquals(Arrays.asList(new Object[] {"somethingelse"}), convertedResults);
	}

	@Test
	public void testSAdd() {
		convertResultToSet = true;
		actual.add(connection.sAdd("myset", "foo"));
		actual.add(connection.sAdd("myset", "bar"));
		actual.add(connection.sMembers("myset"));
		verifyResults(
				Arrays.asList(new Object[] { 1l, 1l,
						new HashSet<String>(Arrays.asList(new String[] { "foo", "bar" })) }),
				actual);
	}

	@Test
	public void testSCard() {
		actual.add(connection.sAdd("myset", "foo"));
		actual.add(connection.sAdd("myset", "bar"));
		actual.add(connection.sCard("myset"));
		verifyResults(Arrays.asList(new Object[] { 1l, 1l, 2l }), actual);
	}

	@Test
	public void testSDiff() {
		convertResultToSet = true;
		actual.add(connection.sAdd("myset", "foo"));
		actual.add(connection.sAdd("myset", "bar"));
		actual.add(connection.sAdd("otherset", "bar"));
		actual.add(connection.sDiff("myset", "otherset"));
		verifyResults(
				Arrays.asList(new Object[] { 1l, 1l, 1l,
						new HashSet<String>(Collections.singletonList("foo")) }), actual);
	}

	@Test
	public void testSDiffStore() {
		convertResultToSet = true;
		actual.add(connection.sAdd("myset", "foo"));
		actual.add(connection.sAdd("myset", "bar"));
		actual.add(connection.sAdd("otherset", "bar"));
		connection.sDiffStore("thirdset", "myset", "otherset");
		actual.add(connection.sMembers("thirdset"));
		verifyResults(
				Arrays.asList(new Object[] { 1l, 1l, 1l, 1l,
						new HashSet<String>(Collections.singletonList("foo")) }), actual);
	}

	@Test
	public void testSInter() {
		convertResultToSet = true;
		actual.add(connection.sAdd("myset", "foo"));
		actual.add(connection.sAdd("myset", "bar"));
		actual.add(connection.sAdd("otherset", "bar"));
		actual.add(connection.sInter("myset", "otherset"));
		verifyResults(
				Arrays.asList(new Object[] { 1l, 1l, 1l,
						new HashSet<String>(Collections.singletonList("bar")) }), actual);
	}

	@Test
	public void testSInterStore() {
		convertResultToSet = true;
		actual.add(connection.sAdd("myset", "foo"));
		actual.add(connection.sAdd("myset", "bar"));
		actual.add(connection.sAdd("otherset", "bar"));
		connection.sInterStore("thirdset", "myset", "otherset");
		actual.add(connection.sMembers("thirdset"));
		verifyResults(
				Arrays.asList(new Object[] { 1l, 1l, 1l, 1l,
						new HashSet<String>(Collections.singletonList("bar")) }), actual);
	}

	@Test
	public void testSIsMember() {
		convertResultToSet = true;
		actual.add(connection.sAdd("myset", "foo"));
		actual.add(connection.sAdd("myset", "bar"));
		actual.add(connection.sIsMember("myset", "foo"));
		actual.add(connection.sIsMember("myset", "baz"));
		verifyResults(Arrays.asList(new Object[] { 1l, 1l, true, false }), actual);
	}

	@Test
	public void testSMove() {
		convertResultToSet = true;
		actual.add(connection.sAdd("myset", "foo"));
		actual.add(connection.sAdd("myset", "bar"));
		actual.add(connection.sAdd("otherset", "bar"));
		actual.add(connection.sMove("myset", "otherset", "foo"));
		actual.add(connection.sMembers("otherset"));
		verifyResults(
				Arrays.asList(new Object[] { 1l, 1l, 1l, 1l,
						new HashSet<String>(Arrays.asList(new String[] { "foo", "bar" })) }),
				actual);
	}

	@Test
	public void testSPop() {
		convertResultToSet = true;
		connection.sAdd("myset", "foo");
		connection.sAdd("myset", "bar");
		assertNull(connection.sPop("myset"));
		List<Object> results = convertResults();
		assertEquals(3, results.size());
		assertTrue(new HashSet<String>(Arrays.asList(new String[] { "foo", "bar" }))
				.contains(results.get(2)));
	}

	@Test
	public void testSRandMember() {
		convertResultToSet = true;
		connection.sAdd("myset", "foo");
		connection.sAdd("myset", "bar");
		assertNull(connection.sRandMember("myset"));
		List<Object> results = convertResults();
		assertEquals(3, results.size());
		assertTrue(new HashSet<String>(Arrays.asList(new String[] { "foo", "bar" }))
				.contains(results.get(2)));
	}

	@Test
	public void testSRem() {
		convertResultToSet = true;
		actual.add(connection.sAdd("myset", "foo"));
		actual.add(connection.sAdd("myset", "bar"));
		actual.add(connection.sRem("myset", "foo"));
		actual.add(connection.sRem("myset", "baz"));
		actual.add(connection.sMembers("myset"));
		verifyResults(
				Arrays.asList(new Object[] { 1l, 1l, 1l, 0l,
						new HashSet<String>(Arrays.asList(new String[] { "bar" })) }), actual);
	}

	@Test
	public void testSUnion() {
		convertResultToSet = true;
		actual.add(connection.sAdd("myset", "foo"));
		actual.add(connection.sAdd("myset", "bar"));
		actual.add(connection.sAdd("otherset", "bar"));
		actual.add(connection.sAdd("otherset", "baz"));
		actual.add(connection.sUnion("myset", "otherset"));
		verifyResults(
				Arrays.asList(new Object[] { 1l, 1l, 1l, 1l,
						new HashSet<String>(Arrays.asList(new String[] { "foo", "bar", "baz" })) }),
				actual);
	}

	@Test
	public void testSUnionStore() {
		convertResultToSet = true;
		actual.add(connection.sAdd("myset", "foo"));
		actual.add(connection.sAdd("myset", "bar"));
		actual.add(connection.sAdd("otherset", "bar"));
		actual.add(connection.sAdd("otherset", "baz"));
		connection.sUnionStore("thirdset", "myset", "otherset");
		actual.add(connection.sMembers("thirdset"));
		verifyResults(
				Arrays.asList(new Object[] { 1l, 1l, 1l, 1l, 3l,
						new HashSet<String>(Arrays.asList(new String[] { "foo", "bar", "baz" })) }),
				actual);
	}

	@Test
	public void testZAddAndZRange() {
		actual.add(connection.zAdd("myset", 2, "Bob"));
		actual.add(connection.zAdd("myset", 1, "James"));
		actual.add(connection.zRange("myset", 0, -1));
		verifyResults(Arrays.asList(new Object[] { 1l, 1l,
				Arrays.asList(new String[] { "James", "Bob" }) }), actual);
	}

	@Test
	public void testZCard() {
		actual.add(connection.zAdd("myset", 2, "Bob"));
		actual.add(connection.zAdd("myset", 1, "James"));
		actual.add(connection.zCard("myset"));
		verifyResults(Arrays.asList(new Object[] { 1l, 1l, 2l }), actual);
	}

	@Test
	public void testZCount() {
		actual.add(connection.zAdd("myset", 2, "Bob"));
		actual.add(connection.zAdd("myset", 1, "James"));
		actual.add(connection.zAdd("myset", 4, "Joe"));
		actual.add(connection.zCount("myset", 1, 2));
		verifyResults(Arrays.asList(new Object[] { 1l, 1l, 1l, 2l }), actual);
	}

	@Test
	public void testZIncrBy() {
		actual.add(connection.zAdd("myset", 2, "Bob"));
		actual.add(connection.zAdd("myset", 1, "James"));
		actual.add(connection.zAdd("myset", 4, "Joe"));
		actual.add(connection.zIncrBy("myset", 2, "Joe"));
		actual.add(connection.zRangeByScore("myset", 6, 6));
		verifyResults(
				Arrays.asList(new Object[] { 1l, 1l, 1l, 6d, Collections.singletonList("Joe") }),
				actual);
	}

	@Test
	public void testZInterStore() {
		actual.add(connection.zAdd("myset", 2, "Bob"));
		actual.add(connection.zAdd("myset", 1, "James"));
		actual.add(connection.zAdd("myset", 4, "Joe"));
		actual.add(connection.zAdd("otherset", 1, "Bob"));
		actual.add(connection.zAdd("otherset", 4, "James"));
		actual.add(connection.zInterStore("thirdset", "myset", "otherset"));
		actual.add(connection.zRange("thirdset", 0, -1));
		verifyResults(
				Arrays.asList(new Object[] { 1l, 1l, 1l, 1l, 1l, 2l,
						Arrays.asList(new String[] { "Bob", "James" }) }), actual);
	}

	@Test
	public void testZInterStoreAggWeights() {
		convertResultToTuples = true;
		actual.add(connection.zAdd("myset", 2, "Bob"));
		actual.add(connection.zAdd("myset", 1, "James"));
		actual.add(connection.zAdd("myset", 4, "Joe"));
		actual.add(connection.zAdd("otherset", 1, "Bob"));
		actual.add(connection.zAdd("otherset", 4, "James"));
		actual.add(connection.zInterStore("thirdset", Aggregate.MAX, new int[] { 2, 3 }, "myset",
				"otherset"));
		actual.add(connection.zRangeWithScores("thirdset", 0, -1));
		verifyResults(
				Arrays.asList(new Object[] {
						1l,
						1l,
						1l,
						1l,
						1l,
						2l,
						Arrays.asList(new StringTuple[] {
								new DefaultStringTuple("Bob".getBytes(), "Bob", 4d),
								new DefaultStringTuple("James".getBytes(), "James", 12d) }) }),
				actual);
	}

	@Test
	public void testZRangeWithScores() {
		convertResultToTuples = true;
		actual.add(connection.zAdd("myset", 2, "Bob"));
		actual.add(connection.zAdd("myset", 1, "James"));
		actual.add(connection.zRangeWithScores("myset", 0, -1));
		verifyResults(
				Arrays.asList(new Object[] {
						1l,
						1l,
						Arrays.asList(new StringTuple[] {
								new DefaultStringTuple("James".getBytes(), "James", 1d),
								new DefaultStringTuple("Bob".getBytes(), "Bob", 2d) }) }), actual);
	}

	@Test
	public void testZRangeByScore() {
		actual.add(connection.zAdd("myset", 2, "Bob"));
		actual.add(connection.zAdd("myset", 1, "James"));
		actual.add(connection.zRangeByScore("myset", 1, 1));
		verifyResults(
				Arrays.asList(new Object[] { 1l, 1l, Arrays.asList(new String[] { "James" }) }),
				actual);
	}

	@Test
	public void testZRangeByScoreOffsetCount() {
		actual.add(connection.zAdd("myset", 2, "Bob"));
		actual.add(connection.zAdd("myset", 1, "James"));
		actual.add(connection.zRangeByScore("myset", 1d, 3d, 1, -1));
		verifyResults(
				Arrays.asList(new Object[] { 1l, 1l, Arrays.asList(new String[] { "Bob" }) }),
				actual);
	}

	@Test
	public void testZRangeByScoreWithScores() {
		convertResultToTuples = true;
		actual.add(connection.zAdd("myset", 2, "Bob"));
		actual.add(connection.zAdd("myset", 1, "James"));
		actual.add(connection.zRangeByScoreWithScores("myset", 2d, 5d));
		verifyResults(
				Arrays.asList(new Object[] {
						1l,
						1l,
						Arrays.asList(new StringTuple[] { new DefaultStringTuple("Bob".getBytes(),
								"Bob", 2d) }) }), actual);
	}

	@Test
	public void testZRangeByScoreWithScoresOffsetCount() {
		convertResultToTuples = true;
		actual.add(connection.zAdd("myset", 2, "Bob"));
		actual.add(connection.zAdd("myset", 1, "James"));
		actual.add(connection.zRangeByScoreWithScores("myset", 1d, 5d, 0, 1));
		verifyResults(
				Arrays.asList(new Object[] {
						1l,
						1l,
						Arrays.asList(new StringTuple[] { new DefaultStringTuple(
								"James".getBytes(), "James", 1d) }) }), actual);
	}

	@Test
	public void testZRevRange() {
		actual.add(connection.zAdd("myset", 2, "Bob"));
		actual.add(connection.zAdd("myset", 1, "James"));
		actual.add(connection.zRevRange("myset", 0, -1));
		verifyResults(Arrays.asList(new Object[] { 1l, 1l,
				Arrays.asList(new String[] { "Bob", "James" }) }), actual);
	}

	@Test
	public void testZRevRangeWithScores() {
		convertResultToTuples = true;
		actual.add(connection.zAdd("myset", 2, "Bob"));
		actual.add(connection.zAdd("myset", 1, "James"));
		actual.add(connection.zRevRangeWithScores("myset", 0, -1));
		verifyResults(
				Arrays.asList(new Object[] {
						1l,
						1l,
						Arrays.asList(new StringTuple[] {
								new DefaultStringTuple("Bob".getBytes(), "Bob", 2d),
								new DefaultStringTuple("James".getBytes(), "James", 1d) }) }),
				actual);
	}

	@Test
	public void testZRevRangeByScoreOffsetCount() {
		actual.add(connection.zAdd("myset".getBytes(), 2, "Bob".getBytes()));
		actual.add(connection.zAdd("myset".getBytes(), 1, "James".getBytes()));
		actual.add(byteConnection.zRevRangeByScore("myset".getBytes(), 0d, 3d,
				0, 5));
		verifyResults(
				Arrays.asList(new Object[] { 1l, 1l,
						Arrays.asList(new String[] { "Bob", "James" }) }),
				actual);
	}

	@Test
	public void testZRevRangeByScore() {
		actual.add(connection.zAdd("myset".getBytes(), 2, "Bob".getBytes()));
		actual.add(connection.zAdd("myset".getBytes(), 1, "James".getBytes()));
		actual.add(byteConnection.zRevRangeByScore("myset".getBytes(), 0d, 3d));
		verifyResults(
				Arrays.asList(new Object[] { 1l, 1l,
						Arrays.asList(new String[] { "Bob", "James" }) }),
				actual);
	}

	@Test
	public void testZRevRangeByScoreWithScoresOffsetCount() {
		convertResultToTuples = true;
		actual.add(connection.zAdd("myset".getBytes(), 2, "Bob".getBytes()));
		actual.add(connection.zAdd("myset".getBytes(), 1, "James".getBytes()));
		actual.add(byteConnection.zRevRangeByScoreWithScores(
				"myset".getBytes(), 0d, 3d, 0, 1));
		verifyResults(Arrays.asList(new Object[] {
				1l,
				1l,
				Arrays.asList(new StringTuple[] { new DefaultStringTuple("Bob"
						.getBytes(), "Bob", 2d) }) }), actual);
	}

	@Test
	public void testZRevRangeByScoreWithScores() {
		convertResultToTuples = true;
		actual.add(connection.zAdd("myset".getBytes(), 2, "Bob".getBytes()));
		actual.add(connection.zAdd("myset".getBytes(), 1, "James".getBytes()));
		actual.add(connection.zAdd("myset".getBytes(), 3, "Joe".getBytes()));
		actual.add(byteConnection.zRevRangeByScoreWithScores(
				"myset".getBytes(), 0d, 2d));
		verifyResults(
				Arrays.asList(new Object[] {
						1l,
						1l,
						1l,
						Arrays.asList(new StringTuple[] {
								new DefaultStringTuple("Bob".getBytes(), "Bob",
										2d),
								new DefaultStringTuple("James".getBytes(),
										"James", 1d) }) }), actual);
	}

	@Test
	public void testZRank() {
		actual.add(connection.zAdd("myset", 2, "Bob"));
		actual.add(connection.zAdd("myset", 1, "James"));
		actual.add(connection.zRank("myset", "James"));
		actual.add(connection.zRank("myset", "Bob"));
		verifyResults(Arrays.asList(new Object[] { 1l, 1l, 0l, 1l }), actual);
	}

	@Test
	public void testZRem() {
		actual.add(connection.zAdd("myset", 2, "Bob"));
		actual.add(connection.zAdd("myset", 1, "James"));
		actual.add(connection.zRem("myset", "James"));
		actual.add(connection.zRange("myset", 0l, -1l));
		verifyResults(
				Arrays.asList(new Object[] { 1l, 1l, 1l, Arrays.asList(new String[] { "Bob" }) }),
				actual);
	}

	@Test
	public void testZRemRangeByRank() {
		actual.add(connection.zAdd("myset", 2, "Bob"));
		actual.add(connection.zAdd("myset", 1, "James"));
		actual.add(connection.zRemRange("myset", 0l, 3l));
		actual.add(connection.zRange("myset", 0l, -1l));
		verifyResults(Arrays.asList(new Object[] { 1l, 1l, 2l, new ArrayList<String>() }), actual);
	}

	@Test
	public void testZRemRangeByScore() {
		actual.add(connection.zAdd("myset", 2, "Bob"));
		actual.add(connection.zAdd("myset", 1, "James"));
		actual.add(connection.zRemRangeByScore("myset", 0d, 1d));
		actual.add(connection.zRange("myset", 0l, -1l));
		verifyResults(
				Arrays.asList(new Object[] { 1l, 1l, 1l, Arrays.asList(new String[] { "Bob" }) }),
				actual);
	}

	@Test
	public void testZRevRank() {
		actual.add(connection.zAdd("myset", 2, "Bob"));
		actual.add(connection.zAdd("myset", 1, "James"));
		actual.add(connection.zAdd("myset", 3, "Joe"));
		actual.add(connection.zRevRank("myset", "Joe"));
		verifyResults(Arrays.asList(new Object[] { 1l, 1l, 1l, 0l }), actual);
	}

	@Test
	public void testZScore() {
		actual.add(connection.zAdd("myset", 2, "Bob"));
		actual.add(connection.zAdd("myset", 1, "James"));
		actual.add(connection.zAdd("myset", 3, "Joe"));
		actual.add(connection.zScore("myset", "Joe"));
		verifyResults(Arrays.asList(new Object[] { 1l, 1l, 1l, 3d }), actual);
	}

	@Test
	public void testZUnionStore() {
		actual.add(connection.zAdd("myset", 2, "Bob"));
		actual.add(connection.zAdd("myset", 1, "James"));
		actual.add(connection.zAdd("myset", 6, "Joe"));
		actual.add(connection.zAdd("otherset", 1, "Bob"));
		actual.add(connection.zAdd("otherset", 4, "James"));
		actual.add(connection.zUnionStore("thirdset", "myset", "otherset"));
		actual.add(connection.zRange("thirdset", 0, -1));

		verifyResults(
				Arrays.asList(new Object[] { 1l, 1l, 1l, 1l, 1l, 3l,
						Arrays.asList(new String[] { "Bob", "James", "Joe" }) }), actual);
	}

	@Test
	public void testZUnionStoreAggWeights() {
		actual.add(connection.zAdd("myset", 2, "Bob"));
		actual.add(connection.zAdd("myset", 1, "James"));
		actual.add(connection.zAdd("myset", 4, "Joe"));
		actual.add(connection.zAdd("otherset", 1, "Bob"));
		actual.add(connection.zAdd("otherset", 4, "James"));
		actual.add(connection.zUnionStore("thirdset", Aggregate.MAX, new int[] { 2, 3 }, "myset",
				"otherset"));
		actual.add(connection.zRangeWithScores("thirdset", 0, -1));

		verifyResults(
				Arrays.asList(new Object[] {
						1l,
						1l,
						1l,
						1l,
						1l,
						3l,
						Arrays.asList(new StringTuple[] {
								new DefaultStringTuple("Bob".getBytes(), "Bob", 4d),
								new DefaultStringTuple("Joe".getBytes(), "Joe", 8d),
								new DefaultStringTuple("James".getBytes(), "James", 12d) }) }),
				actual);
	}

	@Test
	public void testHDel() throws Exception {
		actual.add(connection.hSet("test", "key", "val"));
		actual.add(connection.hDel("test", "key"));
		actual.add(connection.hDel("test", "foo"));
		actual.add(connection.hExists("test", "key"));
		verifyResults(Arrays.asList(new Object[] { 1l, 1l, 0l, 0l }), actual);
	}

	@Test
	public void testHKeys() {
		convertResultToSet = true;
		actual.add(connection.hSet("test", "key", "2"));
		actual.add(connection.hSet("test", "key2", "2"));
		actual.add(connection.hKeys("test"));
		verifyResults(
				Arrays.asList(new Object[] { 1l, 1l,
						new LinkedHashSet<String>(Arrays.asList(new String[] { "key", "key2" })) }),
				actual);
	}

	@Test
	public void testHSetGet() throws Exception {
		String hash = getClass() + ":hashtest";
		String key1 = UUID.randomUUID().toString();
		String key2 = UUID.randomUUID().toString();
		String value1 = "foo";
		String value2 = "bar";
		actual.add(connection.hSet(hash, key1, value1));
		actual.add(connection.hSet(hash, key2, value2));
		actual.add(connection.hGet(hash, key1));
		actual.add(connection.hGetAll(hash));
		List<String> expected = Arrays.asList(new String[] { key1, value1, key2, value2 });
		verifyResults(Arrays.asList(new Object[] { 1l, 1l, value1, expected }), actual);
	}

	@Test
	public void testHSetNX() throws Exception {
		actual.add(connection.hSetNX("myhash", "key1", "foo"));
		actual.add(connection.hSetNX("myhash", "key1", "bar"));
		actual.add(connection.hGet("myhash", "key1"));
		verifyResults(Arrays.asList(new Object[] { 1l, 0l, "foo" }), actual);
	}

	@Test
	public void testHIncrBy() {
		actual.add(connection.hSet("test", "key", "2"));
		actual.add(connection.hIncrBy("test", "key", 3l));
		actual.add(connection.hGet("test", "key"));
		verifyResults(Arrays.asList(new Object[] { 1l, 5l, "5" }), actual);
	}

	@Test
	public void testHLen() {
		actual.add(connection.hSet("test", "key", "2"));
		actual.add(connection.hSet("test", "key2", "2"));
		actual.add(connection.hLen("test"));
		verifyResults(Arrays.asList(new Object[] { 1l, 1l, 2l }), actual);
	}

	@Test
	public void testHVals() {
		actual.add(connection.hSet("test", "key", "foo"));
		actual.add(connection.hSet("test", "key2", "bar"));
		actual.add(connection.hVals("test"));
		verifyResults(Arrays.asList(new Object[] { 1l, 1l,
				Arrays.asList(new String[] { "foo", "bar" }) }), actual);
	}

	@Test
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testScriptLoadEvalSha() {
		// close the pipeline to get the return value of script load
		getResults();
		String sha1 = connection.scriptLoad("return KEYS[1]");
		initConnection();
		actual.add(connection.evalSha(sha1, ReturnType.VALUE, 2, "key1", "key2"));
		verifyResults(Arrays.asList(new Object[] {"key1"}), actual);
	}

	@Test
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testEvalShaArrayStrings() {
		// close the pipeline to get the return value of script load
		getResults();
		String sha1 = connection.scriptLoad("return {KEYS[1],ARGV[1]}");
		initConnection();
		actual.add(connection.evalSha(sha1, ReturnType.MULTI, 1, "key1", "arg1"));
		verifyResults(Arrays.asList(new Object[] { Arrays.asList(new Object[] {"key1", "arg1"})}), actual);
	}

	@Test(expected=RedisPipelineException.class)
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testEvalShaNotFound() {
		connection.evalSha("somefakesha", ReturnType.VALUE, 2, "key1", "key2");
		getResults();
	}

	@Test(expected=RedisPipelineException.class)
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testEvalReturnSingleError() {
		connection.eval("return redis.call('expire','foo')", ReturnType.BOOLEAN, 0);
		getResults();
	}

	@Test
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testEvalReturnFalse() {
		// pipelined results don't get converted to Booleans
		actual.add(connection.eval("return false", ReturnType.BOOLEAN, 0));
		verifyResults(Arrays.asList(new Object[] { null }), actual);
	}

	@Test
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testEvalReturnTrue() {
		// pipelined results don't get converted to Booleans
		actual.add(connection.eval("return true", ReturnType.BOOLEAN, 0));
		verifyResults(Arrays.asList(new Object[] { 1l }), actual);
	}

	@Test
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testScriptExists() {
		getResults();
		String sha1 = connection.scriptLoad("return 'foo'");
		initConnection();
		actual.add(connection.scriptExists(sha1, "98777234"));
		verifyResults(Arrays.asList(new Object[] {Arrays.asList(new Object[] {true, false})}), actual);
	}

	@Test
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testScriptFlush() {
		getResults();
		String sha1 = connection.scriptLoad("return KEYS[1]");
		connection.scriptFlush();
		initConnection();
		actual.add(connection.scriptExists(sha1));
		verifyResults(Arrays.asList(new Object[] {Arrays.asList(new Object[] { 0l })}), actual);
	}

	protected void initConnection() {
		connection.openPipeline();
	}

	protected void verifyResults(List<Object> expected, List<Object> actual) {
		List<Object> expectedPipeline = new ArrayList<Object>();
		for (int i = 0; i < actual.size(); i++) {
			expectedPipeline.add(null);
		}
		assertEquals(expectedPipeline, actual);
		assertEquals(expected, convertResults());
	}

	protected List<Object> getResults() {
		return connection.closePipeline();
	}
}
