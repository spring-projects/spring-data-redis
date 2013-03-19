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
import org.junit.Test;
import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.connection.AbstractConnectionIntegrationTests;
import org.springframework.data.redis.connection.RedisConnectionFactory;

public class JedisConnectionIntegrationTests extends AbstractConnectionIntegrationTests {

	JedisConnectionFactory factory;

	public JedisConnectionIntegrationTests() {
		factory = new JedisConnectionFactory();
		factory.setUsePool(true);

		factory.setPort(SettingsUtils.getPort());
		factory.setHostName(SettingsUtils.getHost());

		factory.afterPropertiesSet();
	}

	
	protected RedisConnectionFactory getConnectionFactory() {
		return factory;
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
