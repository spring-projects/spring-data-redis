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

import java.time.Duration;
import java.util.Set;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.connection.RedisClusterConfiguration;
import org.springframework.data.redis.connection.RedisClusterConnection;
import org.springframework.data.redis.connection.ValueEncoding;
import org.springframework.data.redis.test.condition.EnabledOnRedisClusterAvailable;
import org.springframework.data.redis.test.extension.JedisExtension;

import static org.assertj.core.api.Assertions.*;

/**
 * Integration tests for {@link JedisClientKeyCommands} in cluster mode. Tests all methods in direct and pipelined modes
 * (transactions not supported in cluster).
 *
 * @author Tihomir Mateev
 * @since 4.1
 */
@EnabledOnRedisClusterAvailable
@ExtendWith(JedisExtension.class)
class JedisClientClusterKeyCommandsIntegrationTests {

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

	// ============ Basic Key Operations ============
	@Test
	void basicKeyOperationsShouldWork() {
		// Set up keys
		connection.stringCommands().set("key1".getBytes(), "value1".getBytes());
		connection.stringCommands().set("key2".getBytes(), "value2".getBytes());

		// Test exists - check if key exists
		Boolean existsResult = connection.keyCommands().exists("key1".getBytes());
		assertThat(existsResult).isTrue();

		// Test del - delete key
		Long delResult = connection.keyCommands().del("key1".getBytes());
		assertThat(delResult).isEqualTo(1L);
		assertThat(connection.keyCommands().exists("key1".getBytes())).isFalse();

		// Test unlink - unlink key (async delete)
		Long unlinkResult = connection.keyCommands().unlink("key2".getBytes());
		assertThat(unlinkResult).isEqualTo(1L);

		// Test type - get key type
		connection.stringCommands().set("stringKey".getBytes(), "value".getBytes());
		DataType typeResult = connection.keyCommands().type("stringKey".getBytes());
		assertThat(typeResult).isEqualTo(DataType.STRING);

		// Test touch - update last access time
		Long touchResult = connection.keyCommands().touch("stringKey".getBytes());
		assertThat(touchResult).isEqualTo(1L);
	}

	@Test
	void keyCopyAndRenameOperationsShouldWork() {
		// Set up key
		connection.stringCommands().set("{tag}key1".getBytes(), "value1".getBytes());

		// Test copy - copy key
		Boolean copyResult = connection.keyCommands().copy("{tag}key1".getBytes(), "{tag}key2".getBytes(), false);
		assertThat(copyResult).isTrue();
		assertThat(connection.stringCommands().get("{tag}key2".getBytes())).isEqualTo("value1".getBytes());

		// Test rename - rename key
		connection.keyCommands().rename("{tag}key1".getBytes(), "{tag}key3".getBytes());
		assertThat(connection.keyCommands().exists("{tag}key1".getBytes())).isFalse();
		assertThat(connection.keyCommands().exists("{tag}key3".getBytes())).isTrue();

		// Test renameNX - rename only if new key doesn't exist
		connection.stringCommands().set("{tag}key4".getBytes(), "value4".getBytes());
		Boolean renameNXResult = connection.keyCommands().renameNX("{tag}key3".getBytes(), "{tag}key5".getBytes());
		assertThat(renameNXResult).isTrue();
		Boolean renameNXResult2 = connection.keyCommands().renameNX("{tag}key4".getBytes(), "{tag}key5".getBytes());
		assertThat(renameNXResult2).isFalse();
	}

	@Test
	void keyExpirationOperationsShouldWork() {
		// Set up key
		connection.stringCommands().set("key1".getBytes(), "value1".getBytes());

		// Test expire - set expiration in seconds
		Boolean expireResult = connection.keyCommands().expire("key1".getBytes(), 100);
		assertThat(expireResult).isTrue();

		// Test pExpire - set expiration in milliseconds
		connection.stringCommands().set("key2".getBytes(), "value2".getBytes());
		Boolean pExpireResult = connection.keyCommands().pExpire("key2".getBytes(), 100000);
		assertThat(pExpireResult).isTrue();

		// Test expireAt - set expiration at timestamp
		connection.stringCommands().set("key3".getBytes(), "value3".getBytes());
		long futureTimestamp = System.currentTimeMillis() / 1000 + 100;
		Boolean expireAtResult = connection.keyCommands().expireAt("key3".getBytes(), futureTimestamp);
		assertThat(expireAtResult).isTrue();

		// Test pExpireAt - set expiration at timestamp in milliseconds
		connection.stringCommands().set("key4".getBytes(), "value4".getBytes());
		long futureTimestampMs = System.currentTimeMillis() + 100000;
		Boolean pExpireAtResult = connection.keyCommands().pExpireAt("key4".getBytes(), futureTimestampMs);
		assertThat(pExpireAtResult).isTrue();

		// Test ttl - get time to live in seconds
		Long ttlResult = connection.keyCommands().ttl("key1".getBytes());
		assertThat(ttlResult).isGreaterThan(0L);

		// Test pTtl - get time to live in milliseconds
		Long pTtlResult = connection.keyCommands().pTtl("key2".getBytes());
		assertThat(pTtlResult).isGreaterThan(0L);

		// Test persist - remove expiration
		Boolean persistResult = connection.keyCommands().persist("key1".getBytes());
		assertThat(persistResult).isTrue();
		assertThat(connection.keyCommands().ttl("key1".getBytes())).isEqualTo(-1L);
	}

	@Test
	void keyDiscoveryOperationsShouldWork() {
		// Set up keys
		connection.stringCommands().set("test:key1".getBytes(), "value1".getBytes());
		connection.stringCommands().set("test:key2".getBytes(), "value2".getBytes());
		connection.stringCommands().set("other:key".getBytes(), "value3".getBytes());

		// Test keys - get keys matching pattern
		Set<byte[]> keysResult = connection.keyCommands().keys("test:*".getBytes());
		assertThat(keysResult).hasSizeGreaterThanOrEqualTo(2);

		// Test randomKey - get random key
		byte[] randomKeyResult = connection.keyCommands().randomKey();
		assertThat(randomKeyResult).isNotNull();
	}

	@Test
	void keyInspectionOperationsShouldWork() {
		// Set up key
		connection.stringCommands().set("key1".getBytes(), "value1".getBytes());

		// Test dump - dump key
		byte[] dumpResult = connection.keyCommands().dump("key1".getBytes());
		assertThat(dumpResult).isNotNull();

		// Test restore - restore key
		connection.keyCommands().restore("key2".getBytes(), 0, dumpResult);
		assertThat(connection.stringCommands().get("key2".getBytes())).isEqualTo("value1".getBytes());

		// Test encodingOf - get encoding
		ValueEncoding encodingResult = connection.keyCommands().encodingOf("key1".getBytes());
		assertThat(encodingResult).isNotNull();

		// Test idletime - get idle time
		Duration idletimeResult = connection.keyCommands().idletime("key1".getBytes());
		assertThat(idletimeResult).isNotNull();

		// Test refcount - get reference count
		Long refcountResult = connection.keyCommands().refcount("key1".getBytes());
		assertThat(refcountResult).isNotNull();
	}
}
