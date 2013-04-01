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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.data.redis.connection.RedisZSetCommands.Aggregate;
import org.springframework.data.redis.connection.RedisZSetCommands.Tuple;
import org.springframework.data.redis.connection.StringRedisConnection.StringTuple;
import org.springframework.data.redis.serializer.SerializationUtils;
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
		connection.openPipeline();
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
		connection.closePipeline();
	}

	@Test
	public void testExecute() {
		connection.set("foo", "bar");
		actual.add(connection.execute("GET", "foo"));
		verifyResults(Arrays.asList(new Object[] { "bar" }), actual);
	}

	@Test
	@IfProfileValue(name = "runLongTests", value = "true")
	public void testExpire() throws Exception {
		connection.set("exp", "true");
		actual.add(connection.expire("exp", 1));
		Thread.sleep(2000);
		actual.add(connection.exists("exp"));
		verifyResults(Arrays.asList(new Object[] { 1l, 0l }), actual);
	}

	@Test
	@IfProfileValue(name = "runLongTests", value = "true")
	public void testExpireAt() throws Exception {
		connection.set("exp2", "true");
		actual.add(connection.expireAt("exp2", System.currentTimeMillis() / 1000 + 1));
		Thread.sleep(2000);
		actual.add(connection.exists("exp2"));
		verifyResults(Arrays.asList(new Object[] { 1l, 0l }), actual);
	}

	@Test
	@IfProfileValue(name = "runLongTests", value = "true")
	public void testPersist() throws Exception {
		connection.set("exp3", "true");
		actual.add(connection.expire("exp3", 1));
		actual.add(connection.persist("exp3"));
		Thread.sleep(1500);
		actual.add(connection.exists("exp3"));
		verifyResults(Arrays.asList(new Object[] { 1l, 1l, 1l }), actual);
	}

	@Test
	@IfProfileValue(name = "runLongTests", value = "true")
	public void testSetEx() throws Exception {
		connection.setEx("expy", 1l, "yep");
		actual.add(connection.get("expy"));
		Thread.sleep(2000);
		actual.add(connection.exists("expy"));
		verifyResults(Arrays.asList(new Object[] { "yep", 0l }), actual);
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
	public void testDbSize() {
		connection.set("dbparam", "foo");
		assertNull(connection.dbSize());
		List<Object> results = connection.closePipeline();
		assertEquals(2, results.size());
		assertTrue((Long) results.get(1) > 0);
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void testGetConfig() {
		assertNull(connection.getConfig("*"));
		List<Object> results = convertResults(connection.closePipeline());
		assertEquals(1, results.size());
		assertTrue(!((List) results.get(0)).isEmpty());
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void testKeys() throws Exception {
		connection.set("keytest", "true");
		connection.set("keytest2", "true");
		connection.keys("key*");
		List<Object> results = convertResults(connection.closePipeline());
		assertEquals(1, results.size());
		assertTrue(((List) results.get(0)).contains("keytest"));
	}

	@Test
	public void testRandomKey() {
		connection.set("some", "thing");
		assertNull(connection.randomKey());
		List<Object> results = convertResults(connection.closePipeline());
		assertEquals(1, results.size());
		assertNotNull(results.get(0));
	}

	@Test
	public void testType() {
		connection.set("something", "yo");
		assertNull(connection.type("something"));
		List<Object> results = convertResults(connection.closePipeline());
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
		List<Object> convertedResults = convertResults(connection.closePipeline());
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
		List<Object> convertedResults = convertResults(connection.closePipeline());
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
		List<Object> convertedResults = convertResults(connection.closePipeline());
		assertEquals(Arrays.asList(new Object[] { Arrays.asList(new String[] { "OK",
				"somethingelse" }) }), convertedResults);
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
		List<Object> results = convertResults(connection.closePipeline());
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
		List<Object> results = convertResults(connection.closePipeline());
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

	protected void verifyResults(List<Object> expected, List<Object> actual) {
		List<Object> expectedPipeline = new ArrayList<Object>();
		for (int i = 0; i < actual.size(); i++) {
			expectedPipeline.add(null);
		}
		assertEquals(expectedPipeline, actual);
		List<Object> pipelinedResults = connection.closePipeline();
		assertEquals(expected, convertResults(pipelinedResults));
	}

	protected List<Object> convertResults(List<Object> pipelinedResults) {
		List<Object> serializedResults = new ArrayList<Object>();
		for (Object result : pipelinedResults) {
			Object convertedResult = convertResult(result);
			if (!"OK".equals(convertedResult) && !"QUEUED".equals(convertedResult)) {
				serializedResults.add(convertedResult);
			}
		}
		return serializedResults;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	protected Object convertResult(Object result) {
		if (result instanceof List && !(((List) result).isEmpty())
				&& ((List) result).get(0) instanceof byte[]) {
			return (SerializationUtils.deserialize((List<byte[]>) result, stringSerializer));
		} else if (result instanceof byte[]) {
			return (stringSerializer.deserialize((byte[]) result));
		} else if (result instanceof Map
				&& ((Map) result).keySet().iterator().next() instanceof byte[]) {
			return (SerializationUtils.deserialize((Map) result, stringSerializer));
		} else if (result instanceof Set && !(((Set) result).isEmpty())
				&& ((Set) result).iterator().next() instanceof byte[]) {
			return (SerializationUtils.deserialize((Set) result, stringSerializer));
		}
		return result;
	}
}
