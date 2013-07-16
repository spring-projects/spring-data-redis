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
import org.springframework.data.redis.connection.AbstractConnectionIntegrationTests;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.test.annotation.IfProfileValue;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

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

	@Test(expected=UnsupportedOperationException.class)
	public void testPExpire() {
		super.testPExpire();
	}

	@Test(expected=UnsupportedOperationException.class)
	public void testPExpireKeyNotExists() {
		super.testPExpireKeyNotExists();
	}

	@Test(expected=UnsupportedOperationException.class)
	public void testPExpireAt() {
		super.testPExpireAt();
	}

	@Test(expected=UnsupportedOperationException.class)
	public void testPExpireAtKeyNotExists() {
		super.testPExpireAtKeyNotExists();
	}

	@Test(expected=UnsupportedOperationException.class)
	public void testPTtl() {
		super.testPTtl();
	}

	@Test(expected=UnsupportedOperationException.class)
	public void testPTtlNoExpire() {
		super.testPTtlNoExpire();
	}

	@Test(expected=UnsupportedOperationException.class)
	public void testDumpAndRestore() {
		super.testDumpAndRestore();
	}

	@Test(expected=UnsupportedOperationException.class)
	public void testDumpNonExistentKey() {
		super.testDumpNonExistentKey();
	}

	@Test(expected=UnsupportedOperationException.class)
	public void testRestoreBadData() {
		super.testRestoreBadData();
	}

	@Test(expected=UnsupportedOperationException.class)
	public void testRestoreExistingKey() {
		super.testRestoreExistingKey();
	}

	@Test(expected=UnsupportedOperationException.class)
	public void testRestoreTtl() {
		super.testRestoreTtl();
	}

	@Test(expected=UnsupportedOperationException.class)
	public void testBitCount() {
		super.testBitCount();
	}

	@Test(expected=UnsupportedOperationException.class)
	public void testBitCountInterval() {
		super.testBitCountInterval();
	}

	@Test(expected=UnsupportedOperationException.class)
	public void testBitCountNonExistentKey() {
		super.testBitCountNonExistentKey();
	}

	@Test(expected=UnsupportedOperationException.class)
	public void testBitOpAnd() {
		super.testBitOpAnd();
	}

	@Test(expected=UnsupportedOperationException.class)
	public void testBitOpOr() {
		super.testBitOpOr();
	}

	@Test(expected=UnsupportedOperationException.class)
	public void testBitOpXOr() {
		super.testBitOpXOr();
	}

	@Test(expected=UnsupportedOperationException.class)
	public void testBitOpNot() {
		super.testBitOpNot();
	}

	@Test(expected=UnsupportedOperationException.class)
	public void testBitOpNotMultipleSources() {
		super.testBitOpNotMultipleSources();
	}

	@Test(expected=UnsupportedOperationException.class)
	public void testHIncrByDouble() {
		super.testHIncrByDouble();
	}

	@Test(expected=UnsupportedOperationException.class)
	public void testIncrByDouble() {
		super.testIncrByDouble();
	}

	@Test(expected=UnsupportedOperationException.class)
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testSRandMemberCount() {
		super.testSRandMemberCount();
	}

	@Test(expected=UnsupportedOperationException.class)
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testSRandMemberCountKeyNotExists() {
		super.testSRandMemberCountKeyNotExists();
	}

	@Test(expected=UnsupportedOperationException.class)
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testSRandMemberCountNegative() {
		super.testSRandMemberCountNegative();
	}

	@Test(expected=UnsupportedOperationException.class)
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testInfoBySection() throws Exception {
		super.testInfoBySection();
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
	}

	@Test(expected=InvalidDataAccessApiUsageException.class)
	public void testCreateConnectionWithDbFailure() {
		JedisConnectionFactory factory2 = new JedisConnectionFactory();
		factory2.setDatabase(77);
		factory2.afterPropertiesSet();
		factory2.getConnection();
	}

	@Test(expected=InvalidDataAccessApiUsageException.class)
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testEvalReturnSingleError() {
		connection.eval("return redis.call('expire','foo')", ReturnType.BOOLEAN, 0);
	}

	@Test(expected=InvalidDataAccessApiUsageException.class)
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testEvalShaNotFound() {
		connection.evalSha("somefakesha", ReturnType.VALUE, 2, "key1", "key2");
	}
}
