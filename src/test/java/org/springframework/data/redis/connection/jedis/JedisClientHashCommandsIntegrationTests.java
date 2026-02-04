/*
 * Copyright 2026-present the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.redis.connection.jedis;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.connection.ExpirationOptions;
import org.springframework.data.redis.connection.RedisHashCommands;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.data.redis.test.condition.EnabledOnRedisAvailable;
import org.springframework.data.redis.test.extension.JedisExtension;

import static org.assertj.core.api.Assertions.*;

/**
 * Integration tests for {@link JedisClientHashCommands}. Tests all methods in direct, transaction, and pipelined modes.
 *
 * @author Tihomir Mateev
 * @since 4.1
 */
@EnabledOnRedisAvailable
@ExtendWith(JedisExtension.class)
class JedisClientHashCommandsIntegrationTests {

	private JedisClientConnectionFactory factory;
	private JedisClientConnection connection;

	@BeforeEach
	void setUp() {
		RedisStandaloneConfiguration config = new RedisStandaloneConfiguration(SettingsUtils.getHost(),
				SettingsUtils.getPort());
		factory = new JedisClientConnectionFactory(config);
		factory.afterPropertiesSet();
		connection = (JedisClientConnection) factory.getConnection();
	}

	@AfterEach
	void tearDown() {
		if (connection != null) {
			connection.serverCommands().flushDb();
			connection.close();
		}
		if (factory != null) {
			factory.destroy();
		}
	}

	// ============ Basic Hash Operations ============
	@Test
	void basicHashOperationsShouldWork() {
		// Test hSet - set field in hash
		Boolean setResult = connection.hashCommands().hSet("hash1".getBytes(), "field1".getBytes(), "value1".getBytes());
		assertThat(setResult).isTrue();

		// Test hGet - get field value
		byte[] getValue = connection.hashCommands().hGet("hash1".getBytes(), "field1".getBytes());
		assertThat(getValue).isEqualTo("value1".getBytes());

		// Test hExists - check field existence
		Boolean exists = connection.hashCommands().hExists("hash1".getBytes(), "field1".getBytes());
		assertThat(exists).isTrue();

		// Test hSetNX - set only if field doesn't exist
		Boolean setNXResult = connection.hashCommands().hSetNX("hash1".getBytes(), "field1".getBytes(),
				"newvalue".getBytes());
		assertThat(setNXResult).isFalse(); // Should fail as field exists
		Boolean setNXNew = connection.hashCommands().hSetNX("hash1".getBytes(), "field2".getBytes(), "value2".getBytes());
		assertThat(setNXNew).isTrue();

		// Test hDel - delete field
		Long delResult = connection.hashCommands().hDel("hash1".getBytes(), "field2".getBytes());
		assertThat(delResult).isEqualTo(1L);
		assertThat(connection.hashCommands().hExists("hash1".getBytes(), "field2".getBytes())).isFalse();
	}

	@Test
	void multipleFieldOperationsShouldWork() {
		// Test hMSet - set multiple fields at once
		Map<byte[], byte[]> fields = new HashMap<>();
		fields.put("f1".getBytes(), "v1".getBytes());
		fields.put("f2".getBytes(), "v2".getBytes());
		fields.put("f3".getBytes(), "v3".getBytes());
		connection.hashCommands().hMSet("hash2".getBytes(), fields);

		// Test hLen - get number of fields
		Long len = connection.hashCommands().hLen("hash2".getBytes());
		assertThat(len).isEqualTo(3L);

		// Test hMGet - get multiple field values
		List<byte[]> values = connection.hashCommands().hMGet("hash2".getBytes(), "f1".getBytes(), "f3".getBytes());
		assertThat(values).hasSize(2);
		assertThat(values.get(0)).isEqualTo("v1".getBytes());
		assertThat(values.get(1)).isEqualTo("v3".getBytes());

		// Test hKeys - get all field names
		Set<byte[]> keys = connection.hashCommands().hKeys("hash2".getBytes());
		assertThat(keys).hasSize(3);

		// Test hVals - get all values
		List<byte[]> vals = connection.hashCommands().hVals("hash2".getBytes());
		assertThat(vals).hasSize(3);

		// Test hGetAll - get all fields and values
		Map<byte[], byte[]> all = connection.hashCommands().hGetAll("hash2".getBytes());
		assertThat(all).hasSize(3);
	}

	@Test
	void hashCounterOperationsShouldWork() {
		// Test hIncrBy with Long
		connection.hashCommands().hSet("counters".getBytes(), "count1".getBytes(), "10".getBytes());
		Long incrResult = connection.hashCommands().hIncrBy("counters".getBytes(), "count1".getBytes(), 5);
		assertThat(incrResult).isEqualTo(15L);

		// Test hIncrBy with Double
		connection.hashCommands().hSet("counters".getBytes(), "count2".getBytes(), "10.5".getBytes());
		Double incrDoubleResult = connection.hashCommands().hIncrBy("counters".getBytes(), "count2".getBytes(), 2.5);
		assertThat(incrDoubleResult).isEqualTo(13.0);
	}

	@Test
	void hashFieldExpirationShouldWork() {
		// Set up hash with fields
		connection.hashCommands().hSet("expHash".getBytes(), "field1".getBytes(), "value1".getBytes());
		connection.hashCommands().hSet("expHash".getBytes(), "field2".getBytes(), "value2".getBytes());

		// Test hExpire - set expiration in seconds
		List<Long> expireResult = connection.hashCommands().hExpire("expHash".getBytes(), 10,
				ExpirationOptions.Condition.ALWAYS, "field1".getBytes());
		assertThat(expireResult).hasSize(1);

		// Test hTtl - get TTL in seconds
		List<Long> ttlResult = connection.hashCommands().hTtl("expHash".getBytes(), "field1".getBytes());
		assertThat(ttlResult).hasSize(1);
		assertThat(ttlResult.get(0)).isGreaterThan(0L);

		// Test hpExpire - set expiration in milliseconds
		List<Long> pExpireResult = connection.hashCommands().hpExpire("expHash".getBytes(), 10000,
				ExpirationOptions.Condition.ALWAYS, "field2".getBytes());
		assertThat(pExpireResult).hasSize(1);

		// Test hpTtl - get TTL in milliseconds
		List<Long> pTtlResult = connection.hashCommands().hpTtl("expHash".getBytes(), "field2".getBytes());
		assertThat(pTtlResult).hasSize(1);
		assertThat(pTtlResult.get(0)).isGreaterThan(0L);

		// Test hPersist - remove expiration
		List<Long> persistResult = connection.hashCommands().hPersist("expHash".getBytes(), "field1".getBytes());
		assertThat(persistResult).hasSize(1);
	}

	@Test
	void hashAdvancedOperationsShouldWork() {
		// Set up hash
		connection.hashCommands().hSet("advHash".getBytes(), "field1".getBytes(), "value1".getBytes());
		connection.hashCommands().hSet("advHash".getBytes(), "field2".getBytes(), "value2".getBytes());
		connection.hashCommands().hSet("advHash".getBytes(), "field3".getBytes(), "value3".getBytes());

		// Test hRandField - get random field
		byte[] randField = connection.hashCommands().hRandField("advHash".getBytes());
		assertThat(randField).isNotNull();

		// Test hRandField with count
		List<byte[]> randFields = connection.hashCommands().hRandField("advHash".getBytes(), 2);
		assertThat(randFields).hasSize(2);

		// Test hRandFieldWithValues - get random field with value
		Map.Entry<byte[], byte[]> randWithVal = connection.hashCommands().hRandFieldWithValues("advHash".getBytes());
		assertThat(randWithVal).isNotNull();

		// Test hRandFieldWithValues with count
		List<Map.Entry<byte[], byte[]>> randWithVals = connection.hashCommands().hRandFieldWithValues("advHash".getBytes(),
				2);
		assertThat(randWithVals).hasSize(2);

		// Test hGetDel - get and delete field
		List<byte[]> getDelResult = connection.hashCommands().hGetDel("advHash".getBytes(), "field1".getBytes());
		assertThat(getDelResult).hasSize(1);
		assertThat(getDelResult.get(0)).isEqualTo("value1".getBytes());
		assertThat(connection.hashCommands().hExists("advHash".getBytes(), "field1".getBytes())).isFalse();

		// Test hGetEx - get with expiration update
		List<byte[]> getExResult = connection.hashCommands().hGetEx("advHash".getBytes(), Expiration.seconds(10),
				"field2".getBytes());
		assertThat(getExResult).hasSize(1);
		assertThat(getExResult.get(0)).isEqualTo("value2".getBytes());

		// Test hSetEx - set with expiration
		Map<byte[], byte[]> setExFields = Map.of("field4".getBytes(), "value4".getBytes());
		Boolean setExResult = connection.hashCommands().hSetEx("advHash".getBytes(), setExFields,
				RedisHashCommands.HashFieldSetOption.UPSERT, Expiration.seconds(10));
		assertThat(setExResult).isTrue();

		// Test hStrLen - get field value length
		Long strLen = connection.hashCommands().hStrLen("advHash".getBytes(), "field2".getBytes());
		assertThat(strLen).isEqualTo(6L); // "value2" length
	}

	@Test
	void transactionShouldExecuteAtomically() {
		// Set up initial state
		connection.hashCommands().hSet("txHash".getBytes(), "counter".getBytes(), "10".getBytes());

		// Execute multiple hash operations in a transaction
		connection.multi();
		connection.hashCommands().hIncrBy("txHash".getBytes(), "counter".getBytes(), 5);
		connection.hashCommands().hSet("txHash".getBytes(), "field1".getBytes(), "value1".getBytes());
		connection.hashCommands().hSet("txHash".getBytes(), "field2".getBytes(), "value2".getBytes());
		connection.hashCommands().hLen("txHash".getBytes());
		connection.hashCommands().hGet("txHash".getBytes(), "counter".getBytes());
		List<Object> results = connection.exec();

		// Verify all commands executed
		assertThat(results).hasSize(5);
		assertThat(results.get(0)).isEqualTo(15L); // hIncrBy result
		assertThat(results.get(1)).isEqualTo(true); // hSet field1
		assertThat(results.get(2)).isEqualTo(true); // hSet field2
		assertThat(results.get(3)).isEqualTo(3L); // hLen
		assertThat(results.get(4)).isEqualTo("15".getBytes()); // hGet counter

		// Verify final state
		assertThat(connection.hashCommands().hLen("txHash".getBytes())).isEqualTo(3L);
		assertThat(connection.hashCommands().hGet("txHash".getBytes(), "counter".getBytes())).isEqualTo("15".getBytes());
	}

	@Test
	void pipelineShouldExecuteMultipleCommands() {
		// Set up initial state
		connection.hashCommands().hSet("pipeHash".getBytes(), "counter".getBytes(), "10".getBytes());

		// Execute multiple hash operations in pipeline
		connection.openPipeline();
		connection.hashCommands().hIncrBy("pipeHash".getBytes(), "counter".getBytes(), 5);
		connection.hashCommands().hSet("pipeHash".getBytes(), "field1".getBytes(), "value1".getBytes());
		connection.hashCommands().hMSet("pipeHash".getBytes(),
				Map.of("field2".getBytes(), "value2".getBytes(), "field3".getBytes(), "value3".getBytes()));
		connection.hashCommands().hLen("pipeHash".getBytes());
		connection.hashCommands().hKeys("pipeHash".getBytes());
		connection.hashCommands().hGet("pipeHash".getBytes(), "counter".getBytes());
		List<Object> results = connection.closePipeline();

		// Verify all command results (hMSet returns void, so only 5 results)
		assertThat(results).hasSize(5);
		assertThat(results.get(0)).isEqualTo(15L); // hIncrBy result
		assertThat(results.get(1)).isEqualTo(true); // hSet field1
		// hMSet returns void - no result in list
		assertThat(results.get(2)).isEqualTo(4L); // hLen (counter, field1, field2, field3)
		@SuppressWarnings("unchecked")
		Set<byte[]> keys = (Set<byte[]>) results.get(3);
		assertThat(keys).hasSize(4); // hKeys
		assertThat(results.get(4)).isEqualTo("15".getBytes()); // hGet counter
	}

}
