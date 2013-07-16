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
package org.springframework.data.redis.connection.rjc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.data.redis.connection.AbstractConnectionPipelineIntegrationTests;
import org.springframework.data.redis.connection.DefaultStringRedisConnection;
import org.springframework.data.redis.connection.DefaultStringTuple;
import org.springframework.data.redis.connection.StringRedisConnection.StringTuple;
import org.springframework.data.redis.serializer.SerializationUtils;
import org.springframework.test.annotation.IfProfileValue;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Integration test of {@link RjcConnection} pipeline functionality
 *
 * @author Jennifer Hickey
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class RjcConnectionPipelineIntegrationTests extends
		AbstractConnectionPipelineIntegrationTests {

	@Ignore("DATAREDIS-134 string ops do not work with encoded values")
	public void testGetRangeSetRange() {
	}

	@Ignore("DATAREDIS-133 Key search does not work with regex")
	public void testKeys() throws Exception {
	}

	@Ignore("DATAREDIS-121 incr/decr does not work with encoded values")
	public void testDecrByIncrBy() {
	}

	@Ignore("DATAREDIS-121 incr/decr does not work with encoded values")
	public void testIncDecr() {
	}

	@Ignore("DATAREDIS-121 incr/decr does not work with encoded values")
	public void testHIncrBy() {
	}

	@Ignore("DATAREDIS-134 string ops do not work with encoded values")
	public void testSort() {
	}

	@Ignore("DATAREDIS-134 string ops do not work with encoded values")
	public void testSortStore() {
	}

	@Ignore("DATAREDIS-134 string ops do not work with encoded values")
	public void testSortNullParams() {
	}

	@Ignore("DATAREDIS-134 string ops do not work with encoded values")
	public void testStrLen() {
	}

	@Ignore("DATAREDIS-148 Syntax error on RJC zUnionStore")
	public void testZUnionStoreAggWeights() {
	}

	@Ignore("DATAREDIS-149 Wrong number of args on RJC brPop when pipelining")
	public void testBRPop() {
	}

	@Ignore("DATAREDIS-149 Wrong number of args on RJC brPop when pipelining")
	public void testBLPop() {
	}

	@Ignore("DATAREDIS-149 Wrong number of args on RJC brPop when pipelining")
	public void testBRPopTimeout() {
	}

	@Ignore("DATAREDIS-149 Wrong number of args on RJC brPop when pipelining")
	public void testBLPopTimeout() {
	}

	@Ignore("DATAREDIS-150 the 6 from zIncrBy improperly decoded")
	public void testZIncrBy() {
	}

	@Ignore("DATAREDIS-150 the 3 from zScore improperly decoded")
	public void testZScore() {
	}

	@Ignore("DATAREDIS-150 the results of info improperly decoded")
	public void testInfo() throws Exception {
	}

	@Ignore("DATAREDIS-150 the results of ping improperly decoded")
	public void testPingPong() throws Exception {
	}

	@Ignore("DATAREDIS-150 the results of type improperly decoded")
	public void testType() {
	}

	@Test
	@Ignore("DATAREDIS-161 Syntax error in pipelined RJC op causes SocketTimeouts on subsequent calls")
	public void exceptionExecuteNative() throws Exception {
	}

	@Ignore("DATAREDIS-221 database not reset on pooled connections")
	public void testSelect() {
	}

	// Overrides, usually due to return values being Long vs Boolean or Set vs
	// List

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
	public void testRename() {
		connection.set("renametest", "testit");
		connection.rename("renametest", "newrenametest");
		actual.add(connection.get("newrenametest"));
		actual.add(connection.exists("renametest"));
		verifyResults(Arrays.asList(new Object[] { "testit", 0l }), actual);
	}

	@Test
	public void testExists() {
		connection.set("existent", "true");
		actual.add(connection.exists("existent"));
		actual.add(connection.exists("nonexistent"));
		verifyResults(Arrays.asList(new Object[] { 1l, 0l }), actual);
	}

	@Test
	public void testMultiExec() throws Exception {
		connection.multi();
		connection.set("key", "value");
		assertNull(connection.get("key"));
		assertNull(connection.exec());
		List<Object> convertedResults = convertResults();
		// "OK" will be decoded to null
		assertEquals(Arrays.asList(new Object[] { Arrays.asList(new String[] { null, "value" }) }),
				convertedResults);
	}

	@Test
	public void testWatch() throws Exception {
		connection.set("testitnow", "willdo");
		connection.watch("testitnow".getBytes());
		//Give some time for watch to be asynch executed
		Thread.sleep(500);
		DefaultStringRedisConnection conn2 = new DefaultStringRedisConnection(
				connectionFactory.getConnection());
		conn2.set("testitnow", "something");
		conn2.close();
		connection.multi();
		connection.set("testitnow", "somethingelse");
		actual.add(connection.exec());
		actual.add(connection.get("testitnow"));
		List<Object> convertedResults = convertResults();
		// The null returned from exec will be filtered out
		assertEquals(Arrays.asList(new String[] { "something" }), convertedResults);
	}

	@Test
	public void testUnwatch() throws Exception {
		connection.set("testitnow", "willdo");
		connection.watch("testitnow".getBytes());
		connection.unwatch();
		//Give some time for unwatch to be asynch executed
		Thread.sleep(500);
		connection.multi();
		DefaultStringRedisConnection conn2 = new DefaultStringRedisConnection(
				connectionFactory.getConnection());
		conn2.set("testitnow", "something");
		connection.set("testitnow", "somethingelse");
		connection.get("testitnow");
		connection.exec();
		List<Object> convertedResults = convertResults();
		// "OK" will be decoded to null
		assertEquals(Arrays.asList(new Object[] { Arrays.asList(new String[] { null,
				"somethingelse" }) }), convertedResults);
	}

	@Test
	public void testExecute() {
		connection.set("foo", "bar");
		actual.add(connection.execute("GET", RjcUtils.decode("foo".getBytes())));
		verifyResults(Arrays.asList(new Object[] { "bar" }), actual);
	}

	@Test
	@IfProfileValue(name = "runLongTests", value = "true")
	public void testBRPopLPushTimeout() throws Exception {
		connection.bRPopLPush(1, "alist", "foo");
		Thread.sleep(1500l);
		List<Object> results = connection.closePipeline();
		assertEquals(Arrays.asList(new Object[] { null }), results);
	}

	protected List<Object> convertResults() {
		List<Object> serializedResults = new ArrayList<Object>();
		List<Object> pipelinedResults = getResults();
		for (Object result : pipelinedResults) {
			Object convertedResult = convertResult(result);
			// closePipeline attempts to decode "OK" and "QUEUED" which turn
			// them into null
			// Filter them out here
			if (convertedResult != null && !"OK".equals(convertedResult)
					&& !"QUEUED".equals(convertedResult)) {
				serializedResults.add(convertedResult);
			}
		}
		return serializedResults;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	protected Object convertResult(Object result) {
		if (result instanceof List && !(((List) result).isEmpty())
				&& ((List) result).get(0) instanceof String) {
			if (convertResultToSet) {
				return SerializationUtils.deserialize(RjcUtils.convertToSet((List) result),
						stringSerializer);
			} else if (convertResultToTuples) {
				List resultList = (List) result;
				List<StringTuple> stringTuples = new ArrayList<StringTuple>();
				for (int i = 0; i < resultList.size(); i += 2) {
					String value = stringSerializer.deserialize(RjcUtils.encode((String) resultList
							.get(i)));
					stringTuples.add(new DefaultStringTuple(value.getBytes(), value, Double
							.valueOf((String) resultList.get(i + 1))));
				}
				return stringTuples;
			} else {
				return SerializationUtils.deserialize(RjcUtils.convertToList((List) result),
						stringSerializer);
			}
		} else if (result instanceof byte[]) {
			return stringSerializer.deserialize((byte[]) result);
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
