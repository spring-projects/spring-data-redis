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
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;
import static org.springframework.data.redis.SpinBarrier.waitFor;

import java.util.Arrays;
import java.util.List;
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
import org.springframework.data.redis.connection.RedisStringCommands.BitOperation;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.test.annotation.IfProfileValue;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

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
		// Exec results lost on flatten in closePipeline and also won't get converted
		// by DefaultStringRedisConnection yet
		List<Object> results = getResults();
		assertEquals("something", new String((byte[])results.get(0)));
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
		// Exec results lost on flatten in closePipeline and also won't get converted
				// by DefaultStringRedisConnection yet
		List<Object> results = getResults();
		assertEquals(1,results.size());
		assertEquals("somethingelse",new String((byte[])results.get(0)));;
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
				// Use separate conn factory to avoid using the underlying shared native conn on blocking script
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
		connection.scriptKill();
		getResults();
		assertTrue(waitFor(new TestCondition() {
			public boolean passes() {
				return scriptDead.get();
			}
		}, 3000l));
	}

	@Test
	public void testMove() {
		connection.set("foo", "bar");
		actual.add(connection.move("foo", 1));
		verifyResults(Arrays.asList(new Object[] { true}));
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
}
