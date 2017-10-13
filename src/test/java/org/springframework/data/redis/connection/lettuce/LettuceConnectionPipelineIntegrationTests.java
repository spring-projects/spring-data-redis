/*
 * Copyright 2011-2017 the original author or authors.
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

import static org.junit.Assert.*;
import static org.junit.Assume.*;
import static org.springframework.data.redis.SpinBarrier.*;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.RedisVersionUtils;
import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.connection.AbstractConnectionPipelineIntegrationTests;
import org.springframework.data.redis.connection.DefaultStringRedisConnection;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.data.redis.test.util.RelaxedJUnit4ClassRunner;
import org.springframework.test.annotation.IfProfileValue;
import org.springframework.test.context.ContextConfiguration;

/**
 * Integration test of {@link LettuceConnection} pipeline functionality
 *
 * @author Jennifer Hickey
 * @author Thomas Darimont
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@RunWith(RelaxedJUnit4ClassRunner.class)
@ContextConfiguration("LettuceConnectionIntegrationTests-context.xml")
public class LettuceConnectionPipelineIntegrationTests extends AbstractConnectionPipelineIntegrationTests {

	@Test(expected = UnsupportedOperationException.class)
	public void testSelect() {
		super.testSelect();
	}

	@Test
	@IfProfileValue(name = "runLongTests", value = "true")
	public void testScriptKill() throws Exception {
		getResults();
		assumeTrue(RedisVersionUtils.atLeast("2.6", byteConnection));
		initConnection();
		final AtomicBoolean scriptDead = new AtomicBoolean(false);
		Thread th = new Thread(() -> {
			// Use separate conn factory to avoid using the underlying shared native conn on blocking script
			final LettuceConnectionFactory factory2 = new LettuceConnectionFactory(SettingsUtils.getHost(),
					SettingsUtils.getPort());
			factory2.setClientResources(LettuceTestClientResources.getSharedClientResources());
			factory2.afterPropertiesSet();
			DefaultStringRedisConnection conn2 = new DefaultStringRedisConnection(factory2.getConnection());
			try {
				conn2.eval("local time=1 while time < 10000000000 do time=time+1 end", ReturnType.BOOLEAN, 0);
			} catch (DataAccessException e) {
				scriptDead.set(true);
			}
			conn2.close();
			factory2.destroy();
		});
		th.start();
		Thread.sleep(1000);
		connection.scriptKill();
		getResults();
		assertTrue(waitFor(scriptDead::get, 3000l));
	}

	@Test
	public void testMove() {

		actual.add(connection.set("foo", "bar"));
		actual.add(connection.move("foo", 1));
		verifyResults(Arrays.asList(new Object[] { true, true }));
		// Lettuce does not support select when using shared conn, use a new conn factory
		LettuceConnectionFactory factory2 = new LettuceConnectionFactory();
		factory2.setClientResources(LettuceTestClientResources.getSharedClientResources());
		factory2.setDatabase(1);
		factory2.afterPropertiesSet();
		StringRedisConnection conn2 = new DefaultStringRedisConnection(factory2.getConnection());
		try {
			assertEquals("bar", conn2.get("foo"));
		} finally {
			if (conn2.exists("foo")) {
				conn2.del("foo");
			}
			conn2.close();
			factory2.destroy();
		}
	}

	@Override
	@Test(expected = UnsupportedOperationException.class) // DATAREDIS-268
	public void testListClientsContainsAtLeastOneElement() {
		super.testListClientsContainsAtLeastOneElement();
	}

}
