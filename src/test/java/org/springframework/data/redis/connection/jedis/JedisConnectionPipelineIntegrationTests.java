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
package org.springframework.data.redis.connection.jedis;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.AbstractConnectionPipelineIntegrationTests;
import org.springframework.data.redis.connection.DefaultStringTuple;
import org.springframework.data.redis.connection.StringRedisConnection.StringTuple;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import redis.clients.jedis.Tuple;

/**
 * Integration test of {@link JedisConnection} pipeline functionality
 *
 * @author Jennifer Hickey
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("JedisConnectionIntegrationTests-context.xml")
public class JedisConnectionPipelineIntegrationTests extends
		AbstractConnectionPipelineIntegrationTests {

	/**
	 * Individual results from closePipeline should be converted from
	 * LinkedHashSet to List
	 **/
	private boolean convertResultToList;

	@After
	public void tearDown() {
		try {
			connection.flushDb();
			connection.close();
		} catch (Exception e) {
			// Jedis leaves some incomplete data in OutputStream on NPE caused
			// by null key/value tests
			// Attempting to close the connection will result in error on
			// sending QUIT to Redis
		}
		connection = null;
	}

	@Ignore("DATAREDIS-143 Jedis ClassCastExceptions closing pipeline on certain ops")
	public void testGetConfig() {
	}

	@Ignore("DATAREDIS-143 Jedis ClassCastExceptions closing pipeline on certain ops")
	public void testWatch() {
	}

	@Ignore("DATAREDIS-143 Jedis ClassCastExceptions closing pipeline on certain ops")
	public void testUnwatch() {
	}

	@Ignore("DATAREDIS-143 Jedis ClassCastExceptions closing pipeline on certain ops")
	public void testSortStore() {
	}

	@Ignore("DATAREDIS-143 Jedis ClassCastExceptions closing pipeline on certain ops")
	public void testMultiExec() {
	}

	@Ignore("DATAREDIS-143 Jedis NPE closing pipeline on certain ops")
	public void testMultiDiscard() {
	}

	// Unsupported Ops
	@Test(expected=UnsupportedOperationException.class)
	public void testPExpire() {
		super.testPExpire();
	}

	@Test(expected=UnsupportedOperationException.class)
	public void testPExpireKeyNotExists() {
		super.testPExpireKeyNotExists();
	}

	@Test(expected=UnsupportedOperationException.class)
	public void testPExpireAt() {
		super.testPExpireAt();
	}

	@Test(expected=UnsupportedOperationException.class)
	public void testPExpireAtKeyNotExists() {
		super.testPExpireAtKeyNotExists();
	}

	@Test(expected=UnsupportedOperationException.class)
	public void testPTtl() {
		super.testPTtl();
	}

	@Test(expected=UnsupportedOperationException.class)
	public void testPTtlNoExpire() {
		super.testPTtlNoExpire();
	}

	@Test(expected=UnsupportedOperationException.class)
	public void testDumpAndRestore() {
		super.testDumpAndRestore();
	}

	@Test(expected=UnsupportedOperationException.class)
	public void testDumpNonExistentKey() {
		super.testDumpNonExistentKey();
	}

	@Test(expected=UnsupportedOperationException.class)
	public void testRestoreBadData() {
		super.testRestoreBadData();
	}

	@Test(expected=UnsupportedOperationException.class)
	public void testRestoreExistingKey() {
		super.testRestoreExistingKey();
	}

	@Test(expected=UnsupportedOperationException.class)
	public void testRestoreTtl() {
		super.testRestoreTtl();
	}

	@Test(expected = RedisSystemException.class)
	public void testBitSet() throws Exception {
		super.testBitSet();
	}

	@Test(expected=UnsupportedOperationException.class)
	public void testBitCount() {
		connection.bitCount("foo");
	}

	@Test(expected=UnsupportedOperationException.class)
	public void testBitCountInterval() {
		super.testBitCountInterval();
	}

	@Test(expected=UnsupportedOperationException.class)
	public void testBitCountNonExistentKey() {
		super.testBitCountNonExistentKey();
	}

	@Test(expected=UnsupportedOperationException.class)
	public void testBitOpAnd() {
		super.testBitOpAnd();
	}

	@Test(expected=UnsupportedOperationException.class)
	public void testBitOpOr() {
		super.testBitOpOr();
	}

	@Test(expected=UnsupportedOperationException.class)
	public void testBitOpXOr() {
		super.testBitOpXOr();
	}

	@Test(expected=UnsupportedOperationException.class)
	public void testBitOpNot() {
		super.testBitOpNot();
	}

	@Test(expected=UnsupportedOperationException.class)
	public void testBitOpNotMultipleSources() {
		super.testBitOpNotMultipleSources();
	}

	@Test(expected = RedisSystemException.class)
	public void testRandomKey() {
		super.testRandomKey();
	}

	@Test(expected = RedisSystemException.class)
	public void testGetRangeSetRange() {
		super.testGetRangeSetRange();
	}

	@Test(expected = RedisSystemException.class)
	public void testPingPong() throws Exception {
		super.testPingPong();
	}

	@Test(expected = RedisSystemException.class)
	public void testInfo() throws Exception {
		super.testInfo();
	}

	@Test(expected = RedisSystemException.class)
	public void testZRevRangeByScore() {
		super.testZRevRangeByScore();
	}

	@Test(expected = RedisSystemException.class)
	public void testZRevRangeByScoreOffsetCount() {
		super.testZRevRangeByScoreOffsetCount();
	}

	@Test(expected = RedisSystemException.class)
	public void testZRevRangeByScoreWithScores() {
		super.testZRevRangeByScoreWithScores();
	}

	@Test(expected = RedisSystemException.class)
	public void testZRevRangeByScoreWithScoresOffsetCount() {
		super.testZRevRangeByScoreWithScoresOffsetCount();
	}

	@Test(expected=UnsupportedOperationException.class)
	public void testHIncrByDouble() {
		super.testHIncrByDouble();
	}

	@Test(expected=UnsupportedOperationException.class)
	public void testIncrByDouble() {
		super.testIncrByDouble();
	}

	// Overrides, usually due to return values being Long vs Boolean or Set vs
	// List

	@Test
	public void testRenameNx() {
		connection.set("nxtest", "testit");
		actual.add(connection.renameNX("nxtest", "newnxtest"));
		actual.add(connection.get("newnxtest"));
		actual.add(connection.exists("nxtest"));
		verifyResults(Arrays.asList(new Object[] { 1l, "testit", false }), actual);
	}

	@Test
	public void testKeys() throws Exception {
		convertResultToList = true;
		super.testKeys();
	}

	@Test
	public void testZAddAndZRange() {
		convertResultToList = true;
		super.testZAddAndZRange();
	}

	@Test
	public void testZIncrBy() {
		convertResultToList = true;
		super.testZIncrBy();
	}

	@Test
	public void testZInterStore() {
		convertResultToList = true;
		super.testZInterStore();
	}

	@Test
	public void testZRangeByScore() {
		convertResultToList = true;
		super.testZRangeByScore();
	}

	@Test
	public void testZRangeByScoreOffsetCount() {
		convertResultToList = true;
		super.testZRangeByScoreOffsetCount();
	}

	@Test
	public void testZRevRange() {
		convertResultToList = true;
		super.testZRevRange();
	}

	@Test
	public void testZRem() {
		convertResultToList = true;
		super.testZRem();
	}

	@Test
	public void testZRemRangeByScore() {
		convertResultToList = true;
		super.testZRemRangeByScore();
	}

	@Test
	public void testZUnionStore() {
		convertResultToList = true;
		super.testZUnionStore();
	}

	@Test
	public void testZRemRangeByRank() {
		convertResultToList = true;
		super.testZRemRangeByRank();
	}

	@Test
	public void testHDel() throws Exception {
		actual.add(connection.hSet("test", "key", "val"));
		actual.add(connection.hDel("test", "key"));
		actual.add(connection.hDel("test", "foo"));
		actual.add(connection.hExists("test", "key"));
		verifyResults(Arrays.asList(new Object[] { 1l, 1l, 0l, false }), actual);
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
		Map<String, String> expected = new HashMap<String, String>();
		expected.put(key1, value1);
		expected.put(key2, value2);
		verifyResults(Arrays.asList(new Object[] { 1l, 1l, value1, expected }), actual);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	protected Object convertResult(Object result) {
		Object convertedResult = super.convertResult(result);
		if (convertedResult instanceof Set) {
			if (convertResultToList) {
				// Other providers represent zSets as Lists, so transform here
				return new ArrayList((Set) convertedResult);
			} else if (!(((Set) convertedResult).isEmpty())
					&& ((Set) convertedResult).iterator().next() instanceof Tuple) {
				List<StringTuple> tuples = new ArrayList<StringTuple>();
				for (Tuple value : ((Set<Tuple>) convertedResult)) {
					DefaultStringTuple tuple = new DefaultStringTuple(
							(byte[]) value.getBinaryElement(), value.getElement(), value.getScore());
					tuples.add(tuple);
				}
				return tuples;
			}
		}
		return convertedResult;
	}
}
