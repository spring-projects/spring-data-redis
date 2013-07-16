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
package org.springframework.data.redis.connection.lettuce;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.data.redis.connection.AbstractConnectionPipelineIntegrationTests;
import org.springframework.data.redis.connection.DefaultStringRedisConnection;
import org.springframework.data.redis.connection.DefaultStringTuple;
import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.data.redis.connection.StringRedisConnection.StringTuple;
import org.springframework.test.annotation.IfProfileValue;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.lambdaworks.redis.KeyValue;
import com.lambdaworks.redis.ScoredValue;

/**
 * Integration test of {@link LettuceConnection} pipeline functionality
 *
 * @author Jennifer Hickey
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("LettuceConnectionIntegrationTests-context.xml")
public class LettuceConnectionPipelineIntegrationTests extends
		AbstractConnectionPipelineIntegrationTests {

	@Ignore("DATAREDIS-144 Lettuce closePipeline hangs with discarded transaction")
	public void testMultiDiscard() {
	}

	@Test(expected=UnsupportedOperationException.class)
	public void testSelect() {
		super.testSelect();
	}

	// Overrides, usually due to return values being Long vs Boolean or Set vs
	// List

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
		verifyResults(Arrays.asList(new Object[] { null, "something" }), actual);
	}

	@Test
	public void testUnwatch() throws Exception {
		connection.set("testitnow", "willdo");
		connection.watch("testitnow".getBytes());
		connection.unwatch();
		connection.multi();
		//Give some time for unwatch to be asynch executed
		Thread.sleep(500);
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
	public void testBLPop() {
		// Use a different synchronous connection to add items to the list, else blpop may
		// execute before items in the list (LettuceConnection uses different underlying conns for blocking ops)
		DefaultStringRedisConnection conn2 = new DefaultStringRedisConnection(
				connectionFactory.getConnection());
		conn2.lPush("poplist", "foo");
		conn2.lPush("poplist", "bar");
		actual.add(connection.bLPop(1, "poplist", "otherlist"));
		verifyResults(Arrays.asList(new Object[] {Arrays.asList(new String[] {"poplist", "bar"})}), actual);
	}

	@Test
	public void testBRPop() {
		// Use a different synchronous connection to add items to the list, else blpop may
		// execute before items in the list (LettuceConnection uses different underlying conns for blocking ops)
		DefaultStringRedisConnection conn2 = new DefaultStringRedisConnection(
				connectionFactory.getConnection());
		conn2.rPush("rpoplist", "bar");
		conn2.rPush("rpoplist", "foo");
		actual.add(connection.bRPop(1, "rpoplist"));
		verifyResults(Arrays.asList(new Object[] {Arrays.asList(new String[] {"rpoplist", "foo"})}), actual);
	}

	@Test
	public void testBRPopLPush() {
		// Use a different synchronous connection to add items to the list, else blpop may
		// execute before items in the list (LettuceConnection uses different underlying conns for blocking ops)
		DefaultStringRedisConnection conn2 = new DefaultStringRedisConnection(
				connectionFactory.getConnection());
		conn2.rPush("PopList", "hello");
		conn2.rPush("PopList", "world");
		conn2.rPush("pop2", "hey");
		assertNull(connection.bRPopLPush(1, "PopList", "pop2"));
		List<Object> results = convertResults();
		assertEquals(Arrays.asList(new String[] { "world" }), results);
		assertEquals(Arrays.asList(new String[] { "hello" }), connection.lRange("PopList", 0, -1));
		assertEquals(Arrays.asList(new String[] { "world", "hey" }),
				connection.lRange("pop2", 0, -1));
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
		verifyResults(Arrays.asList(new Object[] { true, true, value1, expected }), actual);
	}

	@Test
	public void testInfo() throws Exception {
		assertNull(connection.info());
		List<Object> results = getResults();
		assertEquals(1, results.size());
		Properties info = LettuceUtils.info((String) results.get(0));
		assertTrue("at least 5 settings should be present", info.size() >= 5);
		String version = info.getProperty("redis_version");
		assertNotNull(version);
	}

	@Test
	public void testMSetNx() {
		Map<String, String> vals = new HashMap<String, String>();
		vals.put("height", "5");
		vals.put("width", "1");
		connection.mSetNXString(vals);
		assertNull(connection.mGet("height", "width"));
		verifyResults(
				Arrays.asList(new Object[] { true, Arrays.asList(new String[] { "5", "1" }) }),
				actual);
	}

	@Test
	public void testSetNx() {
		actual.add(connection.setNX("notaround", "54"));
		actual.add(connection.get("notaround"));
		actual.add(connection.setNX("notaround", "55"));
		actual.add(connection.get("notaround"));
		verifyResults(Arrays.asList(new Object[] { true, "54", false, "54" }), actual);
	}

	@Test
	public void testRenameNx() {
		connection.set("nxtest", "testit");
		actual.add(connection.renameNX("nxtest", "newnxtest"));
		actual.add(connection.get("newnxtest"));
		actual.add(connection.exists("nxtest"));
		verifyResults(Arrays.asList(new Object[] { true, "testit", false }), actual);
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
				Arrays.asList(new Object[] { 1l, 1l, 1l, true,
						new HashSet<String>(Arrays.asList(new String[] { "foo", "bar" })) }),
				actual);
	}

	@Test
	public void testHKeys() {
		actual.add(connection.hSet("test", "key", "2"));
		actual.add(connection.hSet("test", "key2", "2"));
		actual.add(connection.hKeys("test"));
		verifyResults(
				Arrays.asList(new Object[] { true, true,
						Arrays.asList(new String[] { "key", "key2" }) }), actual);
	}

	@Test
	public void testHSetNX() throws Exception {
		actual.add(connection.hSetNX("myhash", "key1", "foo"));
		actual.add(connection.hSetNX("myhash", "key1", "bar"));
		actual.add(connection.hGet("myhash", "key1"));
		verifyResults(Arrays.asList(new Object[] { true, false, "foo" }), actual);
	}

	@Test
	public void testHIncrBy() {
		actual.add(connection.hSet("test", "key", "2"));
		actual.add(connection.hIncrBy("test", "key", 3l));
		actual.add(connection.hGet("test", "key"));
		verifyResults(Arrays.asList(new Object[] { true, 5l, "5" }), actual);
	}

	@Test
	public void testHLen() {
		actual.add(connection.hSet("test", "key", "2"));
		actual.add(connection.hSet("test", "key2", "2"));
		actual.add(connection.hLen("test"));
		verifyResults(Arrays.asList(new Object[] { true, true, 2l }), actual);
	}

	@Test
	public void testHVals() {
		actual.add(connection.hSet("test", "key", "foo"));
		actual.add(connection.hSet("test", "key2", "bar"));
		actual.add(connection.hVals("test"));
		verifyResults(
				Arrays.asList(new Object[] { true, true,
						Arrays.asList(new String[] { "foo", "bar" }) }), actual);
	}

	@Test
	public void testHDel() throws Exception {
		actual.add(connection.hSet("test", "key", "val"));
		actual.add(connection.hDel("test", "key"));
		actual.add(connection.hDel("test", "foo"));
		actual.add(connection.hExists("test", "key"));
		verifyResults(Arrays.asList(new Object[] { true, 1l, 0l, false }), actual);
	}

	@Test
	@IfProfileValue(name = "runLongTests", value = "true")
	public void testPersist() throws Exception {
		connection.set("exp3", "true");
		actual.add(connection.expire("exp3", 1));
		actual.add(connection.persist("exp3"));
		verifyResults(Arrays.asList(new Object[] { true, true }), actual);
		Thread.sleep(1500);
		assertTrue(connection.exists("exp3"));
	}

	@Test
	@IfProfileValue(name = "runLongTests", value = "true")
	public void testExpireAt() throws Exception {
		connection.set("exp2", "true");
		actual.add(connection.expireAt("exp2", System.currentTimeMillis() / 1000 + 1));
		verifyResults(Arrays.asList(new Object[] { true}), actual);
		assertFalse(exists("exp2", 2500l));
	}

	@Test
	@IfProfileValue(name = "runLongTests", value = "true")
	public void testExpire() throws Exception {
		connection.set("exp", "true");
		actual.add(connection.expire("exp", 1));
		verifyResults(Arrays.asList(new Object[] { true }), actual);
		assertFalse(exists("exp", 2500));
	}

	@Test
	public void testMove() {
		connection.set("foo", "bar");
		actual.add(connection.move("foo", 1));
		verifyResults(
				Arrays.asList(new Object[] { true}), actual);
		// Lettuce does not support select when using shared conn, use a new conn factory
		LettuceConnectionFactory factory2 = new LettuceConnectionFactory();
		factory2.setDatabase(1);
		factory2.afterPropertiesSet();
		StringRedisConnection conn2 = new DefaultStringRedisConnection(factory2.getConnection());
		try {
			assertEquals("bar",conn2.get("foo"));
		} finally {
			if(conn2.exists("foo")) {
				conn2.del("foo");
			}
			conn2.close();
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	protected Object convertResult(Object result) {
		Object convertedResult = super.convertResult(result);
		if (convertedResult instanceof KeyValue) {
			List<String> keyValue = new ArrayList<String>();
			keyValue.add((String) super.convertResult(((KeyValue) convertedResult).key));
			keyValue.add((String) super.convertResult(((KeyValue) convertedResult).value));
			return keyValue;
		} else if (convertedResult instanceof List && !(((List) result).isEmpty())
				&& ((List) convertedResult).get(0) instanceof ScoredValue) {
			List<StringTuple> tuples = new ArrayList<StringTuple>();
			for (ScoredValue value : ((List<ScoredValue>) convertedResult)) {
				DefaultStringTuple tuple = new DefaultStringTuple((byte[]) value.value, new String(
						(byte[]) value.value), value.score);
				tuples.add(tuple);
			}
			return tuples;
		}
		return convertedResult;
	}
}
