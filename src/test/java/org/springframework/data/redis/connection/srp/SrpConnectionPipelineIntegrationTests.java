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
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.data.redis.connection.AbstractConnectionPipelineIntegrationTests;
import org.springframework.data.redis.connection.DefaultStringRedisConnection;
import org.springframework.data.redis.connection.DefaultStringTuple;
import org.springframework.data.redis.connection.RedisPipelineException;
import org.springframework.data.redis.connection.RedisZSetCommands.Tuple;
import org.springframework.data.redis.connection.StringRedisConnection.StringTuple;
import org.springframework.data.redis.serializer.SerializationUtils;
import org.springframework.test.annotation.IfProfileValue;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import redis.reply.BulkReply;
import redis.reply.IntegerReply;
import redis.reply.Reply;
import redis.reply.StatusReply;

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

	@Test
	public void testMultiDiscard() throws Exception {
		DefaultStringRedisConnection conn2 = new DefaultStringRedisConnection(
				connectionFactory.getConnection());
		conn2.set("testitnow", "willdo");
		connection.multi();
		connection.set("testitnow2", "notok");
		connection.discard();
		// SRP throws Exception on transaction discard, so we expect pipeline exception here
		try {
			getResults();
			fail("Closing the pipeline on a discarded tx should throw a RedisPipelineException");
		}catch(RedisPipelineException e) {
		}
		assertEquals("willdo", connection.get("testitnow"));
		connection.openPipeline();
		// Ensure we can run a new tx after discarding previous one
		testMultiExec();
	}

	// SRP sets results of all commands in the pipeline to RedisException if exec returns a
	// null multi-bulk reply
	@Test(expected=RedisPipelineException.class)
	public void testWatch() throws Exception {
		connection.set("testitnow", "willdo");
		connection.watch("testitnow".getBytes());
		DefaultStringRedisConnection conn2 = new DefaultStringRedisConnection(
				connectionFactory.getConnection());
		conn2.set("testitnow", "something");
		conn2.close();
		connection.multi();
		connection.set("testitnow", "somethingelse");
		actual.add(connection.exec());
		actual.add(connection.get("testitnow"));
		verifyResults(Arrays.asList(new Object[] { null, "something" }), actual);
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
		List<Object> results = getResults();
		assertEquals(1, results.size());
		Properties info = SrpUtils.info(new BulkReply((byte[]) results.get(0)));
		assertTrue("at least 5 settings should be present", info.size() >= 5);
		String version = info.getProperty("redis_version");
		assertNotNull(version);
	}

	@Test
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testInfoBySection() throws Exception {
		assertNull(connection.info("server"));
		List<Object> results = getResults();
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
	@IfProfileValue(name = "redisVersion", values = {"2.4", "2.6"})
	public void testGetRangeSetRange() {
		super.testGetRangeSetRange();
	}

	@Test
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testIncrByDouble() {
		connection.set("tdb", "4.5");
		actual.add(connection.incrBy("tdb", 7.2));
		actual.add(connection.get("tdb"));
		// pipelined incrBy returns value as a byte[] instead of Double
		verifyResults(Arrays.asList(new Object[] { "11.7" , "11.7" }), actual);
	}

	@Test
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testHIncrByDouble() {
		actual.add(connection.hSet("test", "key", "2.9"));
		actual.add(connection.hIncrBy("test", "key", 3.5));
		actual.add(connection.hGet("test", "key"));
		// pipelined hIncrBy returns value as a byte[] instead of Double
		verifyResults(Arrays.asList(new Object[] { 1l, "6.4", "6.4" }), actual);
	}

	@Test
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testScriptExists() {
		getResults();
		String sha1 = connection.scriptLoad("return 'foo'");
		initConnection();
		actual.add(connection.scriptExists(sha1, "98777234"));
		verifyResults(Arrays.asList(new Object[] {Arrays.asList(new Object[] {1l, 0l})}), actual);
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
			} else if(((Reply[]) convertedResult).length > 0 && ((Reply[])convertedResult)[0] instanceof IntegerReply) {
				return SrpUtils.asIntegerList((Reply[])convertedResult);
			} else if(((Reply[]) convertedResult).length > 0 && ((Reply[])convertedResult)[0] instanceof StatusReply) {
				return SrpUtils.asStatusList((Reply[])convertedResult);
			}  else {
				return SerializationUtils.deserialize(
						SrpUtils.toBytesList((Reply[]) convertedResult), stringSerializer);
			}
		}
		return convertedResult;
	}
}
