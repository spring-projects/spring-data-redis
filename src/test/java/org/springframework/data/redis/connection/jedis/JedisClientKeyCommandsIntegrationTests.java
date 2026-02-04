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
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.ValueEncoding;
import org.springframework.data.redis.test.condition.EnabledOnRedisAvailable;
import org.springframework.data.redis.test.extension.JedisExtension;

import static org.assertj.core.api.Assertions.*;

/**
 * Integration tests for {@link JedisClientKeyCommands}. Tests all methods in direct, transaction, and pipelined modes.
 *
 * @author Tihomir Mateev
 * @since 4.1
 */
@EnabledOnRedisAvailable
@ExtendWith(JedisExtension.class)
class JedisClientKeyCommandsIntegrationTests {

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

	// ============ Basic Key Operations ============
	@Test
	void basicKeyOperationsShouldWork() {
		// Test exists - single key
		connection.stringCommands().set("key1".getBytes(), "value1".getBytes());
		Boolean existsResult = connection.keyCommands().exists("key1".getBytes());
		assertThat(existsResult).isTrue();

		// Test exists - multiple keys
		connection.stringCommands().set("key2".getBytes(), "value2".getBytes());
		Long existsMultiResult = connection.keyCommands().exists("key1".getBytes(), "key2".getBytes(), "key3".getBytes());
		assertThat(existsMultiResult).isEqualTo(2L);

		// Test type
		DataType typeResult = connection.keyCommands().type("key1".getBytes());
		assertThat(typeResult).isEqualTo(DataType.STRING);

		// Test touch
		Long touchResult = connection.keyCommands().touch("key1".getBytes(), "key2".getBytes());
		assertThat(touchResult).isEqualTo(2L);

		// Test del
		Long delResult = connection.keyCommands().del("key1".getBytes());
		assertThat(delResult).isEqualTo(1L);
		assertThat(connection.keyCommands().exists("key1".getBytes())).isFalse();

		// Test unlink
		Long unlinkResult = connection.keyCommands().unlink("key2".getBytes());
		assertThat(unlinkResult).isEqualTo(1L);
	}

	@Test
	void keyCopyAndRenameOperationsShouldWork() {
		// Set up test data
		connection.stringCommands().set("source".getBytes(), "value".getBytes());

		// Test copy
		Boolean copyResult = connection.keyCommands().copy("source".getBytes(), "dest".getBytes(), false);
		assertThat(copyResult).isTrue();
		assertThat(connection.keyCommands().exists("dest".getBytes())).isTrue();

		// Test rename
		connection.keyCommands().rename("source".getBytes(), "newName".getBytes());
		assertThat(connection.keyCommands().exists("source".getBytes())).isFalse();
		assertThat(connection.keyCommands().exists("newName".getBytes())).isTrue();

		// Test renameNX - should fail if destination exists
		connection.stringCommands().set("existing".getBytes(), "val".getBytes());
		Boolean renameNXResult = connection.keyCommands().renameNX("newName".getBytes(), "existing".getBytes());
		assertThat(renameNXResult).isFalse();

		// Test renameNX - should succeed if destination doesn't exist
		Boolean renameNXSuccess = connection.keyCommands().renameNX("newName".getBytes(), "unique".getBytes());
		assertThat(renameNXSuccess).isTrue();
	}

	@Test
	void keyExpirationOperationsShouldWork() {
		// Set up test data
		connection.stringCommands().set("key1".getBytes(), "value1".getBytes());
		connection.stringCommands().set("key2".getBytes(), "value2".getBytes());

		// Test expire - set expiration in seconds
		Boolean expireResult = connection.keyCommands().expire("key1".getBytes(), 100);
		assertThat(expireResult).isTrue();

		// Test pExpire - set expiration in milliseconds
		Boolean pExpireResult = connection.keyCommands().pExpire("key2".getBytes(), 100000);
		assertThat(pExpireResult).isTrue();

		// Test ttl - get time to live in seconds
		Long ttlResult = connection.keyCommands().ttl("key1".getBytes());
		assertThat(ttlResult).isGreaterThan(0L).isLessThanOrEqualTo(100L);

		// Test pTtl - get time to live in milliseconds
		Long pTtlResult = connection.keyCommands().pTtl("key2".getBytes());
		assertThat(pTtlResult).isGreaterThan(0L).isLessThanOrEqualTo(100000L);

		// Test persist - remove expiration
		Boolean persistResult = connection.keyCommands().persist("key1".getBytes());
		assertThat(persistResult).isTrue();
		Long ttlAfterPersist = connection.keyCommands().ttl("key1".getBytes());
		assertThat(ttlAfterPersist).isEqualTo(-1L); // -1 means no expiration
	}

	@Test
	void keyDiscoveryOperationsShouldWork() {
		// Set up test data
		connection.stringCommands().set("user:1".getBytes(), "alice".getBytes());
		connection.stringCommands().set("user:2".getBytes(), "bob".getBytes());
		connection.stringCommands().set("product:1".getBytes(), "laptop".getBytes());

		// Test keys - find keys matching pattern
		Set<byte[]> keysResult = connection.keyCommands().keys("user:*".getBytes());
		assertThat(keysResult).hasSize(2);

		// Test randomKey - get random key
		byte[] randomKeyResult = connection.keyCommands().randomKey();
		assertThat(randomKeyResult).isNotNull();
	}

	@Test
	void keyInspectionOperationsShouldWork() {
		// Set up test data
		connection.stringCommands().set("key1".getBytes(), "value1".getBytes());
		connection.stringCommands().get("key1".getBytes()); // Access to update idletime

		// Test dump - serialize key value
		byte[] dumpResult = connection.keyCommands().dump("key1".getBytes());
		assertThat(dumpResult).isNotNull();

		// Test encodingOf - get encoding
		ValueEncoding encodingResult = connection.keyCommands().encodingOf("key1".getBytes());
		assertThat(encodingResult).isNotNull();

		// Test idletime - get idle time
		Duration idletimeResult = connection.keyCommands().idletime("key1".getBytes());
		assertThat(idletimeResult).isNotNull();

		// Test refcount - get reference count
		Long refcountResult = connection.keyCommands().refcount("key1".getBytes());
		assertThat(refcountResult).isNotNull().isGreaterThanOrEqualTo(0L);
	}

	@Test
	void transactionShouldExecuteAtomically() {
		// Set up initial state
		connection.stringCommands().set("key1".getBytes(), "value1".getBytes());
		connection.stringCommands().set("key2".getBytes(), "value2".getBytes());

		// Execute multiple key operations in a transaction
		connection.multi();
		connection.keyCommands().exists("key1".getBytes());
		connection.keyCommands().type("key1".getBytes());
		connection.keyCommands().expire("key1".getBytes(), 100);
		connection.keyCommands().ttl("key1".getBytes());
		connection.keyCommands().del("key2".getBytes());
		List<Object> results = connection.exec();

		// Verify all commands executed
		assertThat(results).hasSize(5);
		assertThat(results.get(0)).isEqualTo(true); // exists result
		assertThat(results.get(1)).isEqualTo(DataType.STRING); // type result
		assertThat(results.get(2)).isEqualTo(true); // expire result
		assertThat(results.get(3)).isInstanceOf(Long.class); // ttl result
		assertThat(results.get(4)).isEqualTo(1L); // del result
	}

	@Test
	void pipelineShouldExecuteMultipleCommands() {
		// Set up initial state
		connection.stringCommands().set("key1".getBytes(), "value1".getBytes());
		connection.stringCommands().set("key2".getBytes(), "value2".getBytes());
		connection.stringCommands().set("key3".getBytes(), "value3".getBytes());

		// Execute multiple key operations in pipeline
		connection.openPipeline();
		connection.keyCommands().exists("key1".getBytes(), "key2".getBytes());
		connection.keyCommands().type("key1".getBytes());
		connection.keyCommands().touch("key1".getBytes(), "key2".getBytes());
		connection.keyCommands().copy("key1".getBytes(), "key4".getBytes(), false);
		connection.keyCommands().del("key3".getBytes());
		List<Object> results = connection.closePipeline();

		// Verify all command results
		assertThat(results).hasSize(5);
		assertThat(results.get(0)).isEqualTo(2L); // exists result
		assertThat(results.get(1)).isEqualTo(DataType.STRING); // type result
		assertThat(results.get(2)).isEqualTo(2L); // touch result
		assertThat(results.get(3)).isEqualTo(true); // copy result
		assertThat(results.get(4)).isEqualTo(1L); // del result
	}
}
