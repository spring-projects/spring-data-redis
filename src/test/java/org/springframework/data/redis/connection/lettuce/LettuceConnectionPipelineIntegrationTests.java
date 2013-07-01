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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;
import static org.springframework.data.redis.SpinBarrier.waitFor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.RedisVersionUtils;
import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.TestCondition;
import org.springframework.data.redis.connection.AbstractConnectionPipelineIntegrationTests;
import org.springframework.data.redis.connection.DefaultStringRedisConnection;
import org.springframework.data.redis.connection.DefaultStringTuple;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.connection.RedisStringCommands.BitOperation;
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
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testInfoBySection() throws Exception {
		assertNull(connection.info("server"));
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
	public void testMSetNxFailure() {
		connection.set("height", "2");
		Map<String, String> vals = new HashMap<String, String>();
		vals.put("height", "5");
		vals.put("width", "1");
		actual.add(connection.mSetNXString(vals));
		actual.add(connection.mGet("height", "width"));
		verifyResults(Arrays.asList(new Object[] { false, Arrays.asList(new String[] { "2", null }) }),
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
		assertTrue(waitFor(new KeyExpired("exp2"), 2500l));
	}

	@Test
	@IfProfileValue(name = "runLongTests", value = "true")
	public void testExpire() throws Exception {
		connection.set("exp", "true");
		actual.add(connection.expire("exp", 1));
		verifyResults(Arrays.asList(new Object[] { true }), actual);
		assertTrue(waitFor(new KeyExpired("exp"), 2500));
	}

	@Test
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testPExpire() {
		connection.set("exp", "true");
		actual.add(connection.pExpire("exp", 100));
		verifyResults(Arrays.asList(new Object[] { true }), actual);
		assertTrue(waitFor(new KeyExpired("exp"), 1000l));
	}

	@Test
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testPExpireKeyNotExists() {
		actual.add(connection.pExpire("nonexistent", 100));
		verifyResults(Arrays.asList(new Object[] { false }), actual);
	}

	@Test
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testPExpireAt() {
		connection.set("exp2", "true");
		actual.add(connection.pExpireAt("exp2", System.currentTimeMillis() + 200));
		verifyResults(Arrays.asList(new Object[] { true }), actual);
		assertTrue(waitFor(new KeyExpired("exp2"), 1000l));
	}

	@Test
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testPExpireAtKeyNotExists() {
		actual.add(connection.pExpireAt("nonexistent", System.currentTimeMillis() + 200));
		verifyResults(Arrays.asList(new Object[] { false }), actual);
	}

	@Test
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testPTtl() {
		connection.set("whatup", "yo");
		actual.add(connection.pExpire("whatup", 9000l));
		verifyResults(Arrays.asList(new Object[] { true }), actual);
		assertTrue(waitFor(new TestCondition() {
			public boolean passes() {
				return (connection.pTtl("whatup") > -1);
			}
		}, 1000l));
	}

	// LettuceConnection throws an UnsupportedOpException before we close the pipeline
	@Test(expected=UnsupportedOperationException.class)
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testBitOpNotMultipleSources() {
		connection.set("key1", "abcd");
		connection.set("key2", "efgh");
		try {
			actual.add(connection.bitOp(BitOperation.NOT, "key3", "key1", "key2"));
		}finally {
			getResults();
		}
	}

	@Test(expected=UnsupportedOperationException.class)
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testSRandMemberCountNegative() {
		super.testSRandMemberCountNegative();
	}

	@Test
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testEvalReturnFalse() {
		// Lettuce actually returns booleans, it's not an SDR conversion
		actual.add(connection.eval("return false", ReturnType.BOOLEAN, 0));
		verifyResults(Arrays.asList(new Object[] { false }), actual);
	}

	@Test
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testEvalReturnTrue() {
		// Lettuce actually returns booleans, it's not an SDR conversion
		actual.add(connection.eval("return true", ReturnType.BOOLEAN, 0));
		verifyResults(Arrays.asList(new Object[] { true }), actual);
	}

	@Test
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testScriptFlush() {
		getResults();
		String sha1 = connection.scriptLoad("return KEYS[1]");
		connection.scriptFlush();
		initConnection();
		actual.add(connection.scriptExists(sha1));
		// Lettuce actually returns booleans, it's not an SDR conversion
		verifyResults(Arrays.asList(new Object[] {Arrays.asList(new Object[] { false })}), actual);
	}

	@Test
	@IfProfileValue(name = "runLongTests", value = "true")
	public void testScriptKill() throws Exception{
		getResults();
		assumeTrue(RedisVersionUtils.atLeast("2.6", byteConnection));
		initConnection();
		final AtomicBoolean scriptDead = new AtomicBoolean(false);
		Thread th = new Thread(new Runnable() {
			public void run() {
				final LettuceConnectionFactory factory2 = new LettuceConnectionFactory(SettingsUtils.getHost(),
						SettingsUtils.getPort());
				factory2.afterPropertiesSet();
				DefaultStringRedisConnection conn2 = new DefaultStringRedisConnection(
						factory2.getConnection());
				try {
					conn2.eval("local time=1 while time < 10000000000 do time=time+1 end", ReturnType.BOOLEAN, 0);
				}catch(DataAccessException e) {
					scriptDead.set(true);
				}
				conn2.close();
			}
		});
		th.start();
		Thread.sleep(1000);
		getResults();
		connection.scriptKill();
		assertTrue(waitFor(new TestCondition() {
			public boolean passes() {
				return scriptDead.get();
			}
		}, 3000l));
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
