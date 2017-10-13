/*
 * Copyright 2011-2016 the original author or authors.
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

import java.util.Arrays;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.data.redis.connection.AbstractConnectionTransactionIntegrationTests;
import org.springframework.data.redis.connection.DefaultStringRedisConnection;
import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.data.redis.test.util.RelaxedJUnit4ClassRunner;
import org.springframework.test.context.ContextConfiguration;

/**
 * Integration test of {@link LettuceConnection} functionality within a transaction
 * 
 * @author Jennifer Hickey
 * @author Thomas Darimont
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@RunWith(RelaxedJUnit4ClassRunner.class)
@ContextConfiguration("LettuceConnectionIntegrationTests-context.xml")
public class LettuceConnectionTransactionIntegrationTests extends AbstractConnectionTransactionIntegrationTests {

	@Test
	public void testMove() {

		actual.add(connection.set("foo", "bar"));
		actual.add(connection.move("foo", 1));
		verifyResults(Arrays.asList(true, true ));

		// Lettuce does not support select when using shared conn, use a new conn factory
		LettuceConnectionFactory factory2 = new LettuceConnectionFactory();
		factory2.setClientResources(LettuceTestClientResources.getSharedClientResources());
		factory2.setShutdownTimeout(0);
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

	@Test(expected = UnsupportedOperationException.class)
	public void testSelect() {
		super.testSelect();
	}
}
