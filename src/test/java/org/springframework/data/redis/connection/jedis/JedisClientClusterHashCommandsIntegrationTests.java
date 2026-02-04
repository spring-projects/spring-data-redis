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

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.connection.RedisClusterConfiguration;
import org.springframework.data.redis.connection.RedisClusterConnection;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.data.redis.test.condition.EnabledOnRedisClusterAvailable;
import org.springframework.data.redis.test.extension.JedisExtension;

import static org.assertj.core.api.Assertions.*;

/**
 * Integration tests for {@link JedisClientHashCommands} in cluster mode. Tests all methods in direct and pipelined
 * modes (transactions not supported in cluster).
 *
 * @author Tihomir Mateev
 * @since 4.1
 */
@EnabledOnRedisClusterAvailable
@ExtendWith(JedisExtension.class)
class JedisClientClusterHashCommandsIntegrationTests {

	private JedisClientConnectionFactory factory;
	private RedisClusterConnection connection;

	@BeforeEach
	void setUp() {
		RedisClusterConfiguration config = new RedisClusterConfiguration().clusterNode(SettingsUtils.getHost(),
				SettingsUtils.getClusterPort());
		factory = new JedisClientConnectionFactory(config);
		factory.afterPropertiesSet();
		connection = factory.getClusterConnection();
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
		// Test hSet - set field
		Boolean hSetResult = connection.hashCommands().hSet("hash1".getBytes(), "field1".getBytes(), "value1".getBytes());
		assertThat(hSetResult).isTrue();

		// Test hGet - get field value
		byte[] hGetResult = connection.hashCommands().hGet("hash1".getBytes(), "field1".getBytes());
		assertThat(hGetResult).isEqualTo("value1".getBytes());

		// Test hExists - check if field exists
		Boolean hExistsResult = connection.hashCommands().hExists("hash1".getBytes(), "field1".getBytes());
		assertThat(hExistsResult).isTrue();

		// Test hSetNX - set if field not exists
		Boolean hSetNXResult = connection.hashCommands().hSetNX("hash1".getBytes(), "field2".getBytes(),
				"value2".getBytes());
		assertThat(hSetNXResult).isTrue();
		Boolean hSetNXResult2 = connection.hashCommands().hSetNX("hash1".getBytes(), "field2".getBytes(),
				"value3".getBytes());
		assertThat(hSetNXResult2).isFalse();

		// Test hDel - delete field
		Long hDelResult = connection.hashCommands().hDel("hash1".getBytes(), "field1".getBytes());
		assertThat(hDelResult).isEqualTo(1L);
		assertThat(connection.hashCommands().hExists("hash1".getBytes(), "field1".getBytes())).isFalse();
	}

	@Test
	void multipleFieldOperationsShouldWork() {
		// Test hMSet - set multiple fields
		Map<byte[], byte[]> fields = Map.of("field1".getBytes(), "value1".getBytes(), "field2".getBytes(),
				"value2".getBytes(), "field3".getBytes(), "value3".getBytes());
		connection.hashCommands().hMSet("hash2".getBytes(), fields);

		// Test hLen - get number of fields
		Long hLenResult = connection.hashCommands().hLen("hash2".getBytes());
		assertThat(hLenResult).isEqualTo(3L);

		// Test hMGet - get multiple fields
		List<byte[]> hMGetResult = connection.hashCommands().hMGet("hash2".getBytes(), "field1".getBytes(),
				"field2".getBytes());
		assertThat(hMGetResult).hasSize(2);
		assertThat(hMGetResult.get(0)).isEqualTo("value1".getBytes());

		// Test hKeys - get all field names
		Set<byte[]> hKeysResult = connection.hashCommands().hKeys("hash2".getBytes());
		assertThat(hKeysResult).hasSize(3);

		// Test hVals - get all values
		List<byte[]> hValsResult = connection.hashCommands().hVals("hash2".getBytes());
		assertThat(hValsResult).hasSize(3);

		// Test hGetAll - get all fields and values
		Map<byte[], byte[]> hGetAllResult = connection.hashCommands().hGetAll("hash2".getBytes());
		assertThat(hGetAllResult).hasSize(3);
	}

	@Test
	void hashCounterOperationsShouldWork() {
		// Test hIncrBy - increment field by long
		Long hIncrByResult = connection.hashCommands().hIncrBy("hash3".getBytes(), "counter".getBytes(), 5);
		assertThat(hIncrByResult).isEqualTo(5L);
		Long hIncrByResult2 = connection.hashCommands().hIncrBy("hash3".getBytes(), "counter".getBytes(), 3);
		assertThat(hIncrByResult2).isEqualTo(8L);

		// Test hIncrBy - increment field by double
		Double hIncrByFloatResult = connection.hashCommands().hIncrBy("hash3".getBytes(), "floatCounter".getBytes(), 1.5);
		assertThat(hIncrByFloatResult).isEqualTo(1.5);
		Double hIncrByFloatResult2 = connection.hashCommands().hIncrBy("hash3".getBytes(), "floatCounter".getBytes(), 2.3);
		assertThat(hIncrByFloatResult2).isCloseTo(3.8, within(0.01));
	}

	@Test
	void hashFieldExpirationShouldWork() {
		// Set up hash with fields
		connection.hashCommands().hSet("hash4".getBytes(), "field1".getBytes(), "value1".getBytes());
		connection.hashCommands().hSet("hash4".getBytes(), "field2".getBytes(), "value2".getBytes());

		// Test hExpire - set field expiration in seconds
		List<Long> hExpireResult = connection.hashCommands().hExpire("hash4".getBytes(), 100, "field1".getBytes());
		assertThat(hExpireResult).hasSize(1);
		assertThat(hExpireResult.get(0)).isEqualTo(1L);

		// Test hTtl - get field TTL in seconds
		List<Long> hTtlResult = connection.hashCommands().hTtl("hash4".getBytes(), "field1".getBytes());
		assertThat(hTtlResult).hasSize(1);
		assertThat(hTtlResult.get(0)).isGreaterThan(0L);

		// Test hpExpire - set field expiration in milliseconds
		List<Long> hpExpireResult = connection.hashCommands().hpExpire("hash4".getBytes(), 100000, "field2".getBytes());
		assertThat(hpExpireResult).hasSize(1);
		assertThat(hpExpireResult.get(0)).isEqualTo(1L);

		// Test hpTtl - get field TTL in milliseconds
		List<Long> hpTtlResult = connection.hashCommands().hpTtl("hash4".getBytes(), "field2".getBytes());
		assertThat(hpTtlResult).hasSize(1);
		assertThat(hpTtlResult.get(0)).isGreaterThan(0L);

		// Test hPersist - remove field expiration
		List<Long> hPersistResult = connection.hashCommands().hPersist("hash4".getBytes(), "field1".getBytes());
		assertThat(hPersistResult).hasSize(1);
		assertThat(hPersistResult.get(0)).isEqualTo(1L);
		List<Long> ttlAfterPersist = connection.hashCommands().hTtl("hash4".getBytes(), "field1".getBytes());
		assertThat(ttlAfterPersist.get(0)).isEqualTo(-1L);
	}

	@Test
	void hashAdvancedOperationsShouldWork() {
		// Set up hash
		Map<byte[], byte[]> fields = Map.of("field1".getBytes(), "value1".getBytes(), "field2".getBytes(),
				"value2".getBytes(), "field3".getBytes(), "value3".getBytes());
		connection.hashCommands().hMSet("hash5".getBytes(), fields);

		// Test hRandField - get random field
		byte[] hRandFieldResult = connection.hashCommands().hRandField("hash5".getBytes());
		assertThat(hRandFieldResult).isNotNull();

		// Test hRandField with count
		List<byte[]> hRandFieldCountResult = connection.hashCommands().hRandField("hash5".getBytes(), 2);
		assertThat(hRandFieldCountResult).hasSize(2);

		// Test hRandFieldWithValues - get random field with values
		List<Map.Entry<byte[], byte[]>> hRandFieldWithValuesResult = connection.hashCommands()
				.hRandFieldWithValues("hash5".getBytes(), 2);
		assertThat(hRandFieldWithValuesResult).hasSize(2);

		// Test hGetDel - get and delete field
		List<byte[]> hGetDelResult = connection.hashCommands().hGetDel("hash5".getBytes(), "field1".getBytes());
		assertThat(hGetDelResult).hasSize(1);
		assertThat(hGetDelResult.get(0)).isEqualTo("value1".getBytes());
		assertThat(connection.hashCommands().hExists("hash5".getBytes(), "field1".getBytes())).isFalse();

		// Test hGetEx - get field with expiration
		List<byte[]> hGetExResult = connection.hashCommands().hGetEx("hash5".getBytes(), Expiration.seconds(100),
				"field2".getBytes());
		assertThat(hGetExResult).hasSize(1);
		assertThat(hGetExResult.get(0)).isEqualTo("value2".getBytes());

		// Test hSetEx - set field with expiration
		Boolean hSetExResult = connection.hashCommands().hSetEx("hash5".getBytes(),
				Map.of("field4".getBytes(), "value4".getBytes()),
				org.springframework.data.redis.connection.RedisHashCommands.HashFieldSetOption.UPSERT, Expiration.seconds(100));
		assertThat(hSetExResult).isTrue();

		// Test hStrLen - get field value length
		Long hStrLenResult = connection.hashCommands().hStrLen("hash5".getBytes(), "field2".getBytes());
		assertThat(hStrLenResult).isEqualTo(6L);
	}
}
