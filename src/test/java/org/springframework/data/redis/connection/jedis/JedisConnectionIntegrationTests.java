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
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.connection.AbstractConnectionIntegrationTests;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import redis.clients.jedis.JedisPoolConfig;

/**
 * Integration test of {@link JedisConnection}
 *
 * @author Costin Leau
 * @author Jennifer Hickey
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class JedisConnectionIntegrationTests extends AbstractConnectionIntegrationTests {

	@After
	public void tearDown() {
		try {
			connection.flushDb();
			connection.close();
		} catch (Exception e) {
			// Jedis leaves some incomplete data in OutputStream on NPE caused
			// by null key/value tests
			// Attempting to flush the DB or close the connection will result in
			// error on sending QUIT to Redis
		}
		connection = null;
	}

	@Test
	public void testIncrDecrByLong() {
		String key = "test.count";
		long largeNumber = 0x123456789L; // > 32bits
		connection.set(key, "0");
		connection.incrBy(key, largeNumber);
		assertEquals(largeNumber, Long.valueOf(connection.get(key)).longValue());
		connection.decrBy(key, largeNumber);
		assertEquals(0, Long.valueOf(connection.get(key)).longValue());
		connection.decrBy(key, 2 * largeNumber);
		assertEquals(-2 * largeNumber, Long.valueOf(connection.get(key)).longValue());
	}

	@Test
	public void testHashIncrDecrByLong() {
		String key = "test.hcount";
		String hkey = "hashkey";

		long largeNumber = 0x123456789L; // > 32bits
		connection.hSet(key, hkey, "0");
		connection.hIncrBy(key, hkey, largeNumber);
		assertEquals(largeNumber, Long.valueOf(connection.hGet(key, hkey)).longValue());
		connection.hIncrBy(key, hkey, -2 * largeNumber);
		assertEquals(-largeNumber, Long.valueOf(connection.hGet(key, hkey)).longValue());
	}

	@Test
	public void testCreateConnectionWithDb() {
		JedisConnectionFactory factory2 = new JedisConnectionFactory();
		factory2.setDatabase(1);
		factory2.afterPropertiesSet();
		// No way to really verify we are in the selected DB
		factory2.getConnection().ping();
		factory2.destroy();
	}

	@Test(expected=InvalidDataAccessApiUsageException.class)
	public void testCreateConnectionWithDbFailure() {
		JedisConnectionFactory factory2 = new JedisConnectionFactory();
		factory2.setDatabase(77);
		factory2.afterPropertiesSet();
		factory2.getConnection();
		factory2.destroy();
	}

	@Test
	public void testClosePool() {
		JedisPoolConfig config = new JedisPoolConfig();
		config.setMaxActive(1);
		config.setMaxWait(1l);
		JedisConnectionFactory factory2 = new JedisConnectionFactory(config);
		factory2.setHostName(SettingsUtils.getHost());
		factory2.setPort(SettingsUtils.getPort());
		factory2.afterPropertiesSet();
		RedisConnection conn2 = factory2.getConnection();
		conn2.close();
		factory2.getConnection();
		factory2.destroy();
	}
}
