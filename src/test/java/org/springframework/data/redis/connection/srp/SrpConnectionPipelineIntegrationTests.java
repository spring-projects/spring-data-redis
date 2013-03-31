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
package org.springframework.data.redis.connection.srp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.data.redis.connection.AbstractConnectionPipelineIntegrationTests;
import org.springframework.data.redis.connection.DefaultStringTuple;
import org.springframework.data.redis.connection.RedisZSetCommands.Tuple;
import org.springframework.data.redis.connection.StringRedisConnection.StringTuple;
import org.springframework.data.redis.serializer.SerializationUtils;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import redis.client.RedisClientBase;
import redis.reply.BulkReply;
import redis.reply.Reply;

/**
 * Integration test of {@link SrpConnection} pipeline functionality
 *
 * @author Jennifer Hickey
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("SrpConnectionIntegrationTests-context.xml")
public class SrpConnectionPipelineIntegrationTests extends
		AbstractConnectionPipelineIntegrationTests {

	@Ignore("DATAREDIS-123, exec does not return command results")
	public void testMultiExec() throws Exception {
	}

	@Ignore("DATAREDIS-123, exec does not return command results")
	public void testMultiDiscard() {
	}

	@Ignore("DATAREDIS-123, exec does not return command results")
	public void testWatch() {
	}

	@Ignore("DATAREDIS-123, exec does not return command results")
	public void testUnwatch() {
	}

	@Ignore("DATAREDIS-130, sort not working")
	public void testSort() {
	}

	@Ignore("DATAREDIS-130, sort not working")
	public void testSortStore() {
	}

	@Ignore("DATAREDIS-132 config get broken in SRP 0.2")
	public void testGetConfig() {
	}

	@Ignore("DATAREDIS-142 SRP zCount/zInterStore methods execute synchronously when pipelining")
	public void testZCount() {
	}

	@Ignore("DATAREDIS-142 SRP zCount/zInterStore methods execute synchronously when pipelining")
	public void testZInterStore() {
	}

	@Ignore("DATAREDIS-152 Syntax error on zRangeByScore and and zRangeByScoreWithScores when using offset and count")
	public void testZRangeByScoreOffsetCount() {
	}

	@Ignore("DATAREDIS-152 Syntax error on zRangeByScore and and zRangeByScoreWithScores when using offset and count")
	public void testZRangeByScoreWithScoresOffsetCount() {
	}

	@Ignore("DATAREDIS-138 NPE in DefaultStringRedisConnection deserializing a null Map")
	public void testHSetGet() throws Exception {
		// String hash = getClass() + ":hashtest";
		// String key1 = UUID.randomUUID().toString();
		// String key2 = UUID.randomUUID().toString();
		// String value1 = "foo";
		// String value2 = "bar";
		// actual.add(connection.hSet(hash, key1, value1));
		// actual.add(connection.hSet(hash, key2, value2));
		// actual.add(connection.hGet(hash, key1));
		// actual.add(connection.hGetAll(hash));
		// List<String> expected = Arrays.asList(new String[] {key1, value1,
		// key2, value2});
		// verifyResults(Arrays.asList(new Object[] { 1l, 1l, value1, expected
		// }), actual);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testZInterStoreAggWeights() {
		super.testZInterStoreAggWeights();
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testZUnionStoreAggWeights() {
		super.testZUnionStoreAggWeights();
	}

	// Overrides, usually due to return values being Long vs Boolean or Set vs
	// List

	@Test
	public void testInfo() throws Exception {
		assertNull(connection.info());
		List<Object> results = connection.closePipeline();
		assertEquals(1, results.size());
		Properties info = SrpUtils.info(new BulkReply((byte[]) results.get(0)));
		assertTrue("at least 5 settings should be present", info.size() >= 5);
		String version = info.getProperty("redis_version");
		assertNotNull(version);
	}

	@Test
	public void testExists() {
		connection.set("existent", "true");
		actual.add(connection.exists("existent"));
		actual.add(connection.exists("nonexistent"));
		verifyResults(Arrays.asList(new Object[] { 1l, 0l }), actual);
	}

	@Test
	public void testRename() {
		connection.set("renametest", "testit");
		connection.rename("renametest", "newrenametest");
		actual.add(connection.get("newrenametest"));
		actual.add(connection.exists("renametest"));
		verifyResults(Arrays.asList(new Object[] { "testit", 0l }), actual);
	}

	@Test
	public void testSIsMember() {
		convertResultToSet = true;
		actual.add(connection.sAdd("myset", "foo"));
		actual.add(connection.sAdd("myset", "bar"));
		actual.add(connection.sIsMember("myset", "foo"));
		actual.add(connection.sIsMember("myset", "baz"));
		verifyResults(Arrays.asList(new Object[] { 1l, 1l, 1l, 0l }), actual);
	}

	@Test
	public void testZIncrBy() {
		actual.add(connection.zAdd("myset", 2, "Bob"));
		actual.add(connection.zAdd("myset", 1, "James"));
		actual.add(connection.zAdd("myset", 4, "Joe"));
		actual.add(connection.zIncrBy("myset", 2, "Joe"));
		actual.add(connection.zRangeByScore("myset", 6, 6));
		verifyResults(
				Arrays.asList(new Object[] { 1l, 1l, 1l, "6", Collections.singletonList("Joe") }),
				actual);
	}

	@Test
	public void testZScore() {
		actual.add(connection.zAdd("myset", 2, "Bob"));
		actual.add(connection.zAdd("myset", 1, "James"));
		actual.add(connection.zAdd("myset", 3, "Joe"));
		actual.add(connection.zScore("myset", "Joe"));
		verifyResults(Arrays.asList(new Object[] { 1l, 1l, 1l, "3" }), actual);
	}

	@Test
	public void testGetRangeSetRange() {
		assumeTrue(getRedisVersion() >= RedisClientBase.parseVersion("2.4.0"));
		super.testGetRangeSetRange();
	}

	private int getRedisVersion() {
		connection.closePipeline();
		int version = RedisClientBase.parseVersion((String)connection.info().get("redis_version"));
		connection.openPipeline();
		return version;
	}

	protected Object convertResult(Object result) {
		Object convertedResult = super.convertResult(result);
		if (convertedResult instanceof Reply[]) {
			if (convertResultToSet) {
				return SerializationUtils.deserialize(SrpUtils.toSet((Reply[]) convertedResult),
						stringSerializer);
			} else if (convertResultToTuples) {
				Set<Tuple> tuples = SrpUtils.convertTuple((Reply[]) convertedResult);
				List<StringTuple> stringTuples = new ArrayList<StringTuple>();
				for (Tuple tuple : tuples) {
					stringTuples.add(new DefaultStringTuple(tuple, new String(tuple.getValue())));
				}
				return stringTuples;
			} else {
				return SerializationUtils.deserialize(
						SrpUtils.toBytesList((Reply[]) convertedResult), stringSerializer);
			}
		}
		return convertedResult;
	}
}
