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

import static org.junit.Assert.assertEquals;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.AbstractConnectionIntegrationTests;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class JedisConnectionIntegrationTests extends AbstractConnectionIntegrationTests {
	
	@After
	public void tearDown() {
		try {
			connection.close();
		}catch(DataAccessException e) {
			// Jedis leaves some incomplete data in OutputStream on NPE caused by null key/value tests
			// Attempting to close the connection will result in error on sending QUIT to Redis
			System.out.println("Connection already closed");
		}
		connection = null;
	}

	@Test
	public void testIncrDecrBy() {
		String key = "test.count";
		long largeNumber = 0x123456789L; // > 32bits
		connection.set(key.getBytes(), "0".getBytes());
		connection.incrBy(key.getBytes(), largeNumber);
		assertEquals(largeNumber, Long.valueOf(new String(connection.get(key.getBytes()))).longValue());
		connection.decrBy(key.getBytes(), largeNumber);
		assertEquals(0, Long.valueOf(new String(connection.get(key.getBytes()))).longValue());
		connection.decrBy(key.getBytes(), 2*largeNumber);
		assertEquals(-2*largeNumber, Long.valueOf(new String(connection.get(key.getBytes()))).longValue());
	}

	@Test
	public void testHashIncrDecrBy() {
		byte[] key = "test.hcount".getBytes();
		byte[] hkey = "hashkey".getBytes();

		long largeNumber = 0x123456789L; // > 32bits
		connection.hSet(key, hkey, "0".getBytes());
		connection.hIncrBy(key, hkey, largeNumber);
		assertEquals(largeNumber, Long.valueOf(new String(connection.hGet(key, hkey))).longValue());
		connection.hIncrBy(key, hkey, -2*largeNumber);
		assertEquals(-largeNumber, Long.valueOf(new String(connection.hGet(key, hkey))).longValue());
	}
}
