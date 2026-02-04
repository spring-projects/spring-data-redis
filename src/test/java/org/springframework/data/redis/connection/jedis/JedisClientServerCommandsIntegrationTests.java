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
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.connection.RedisServerCommands.FlushOption;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.core.types.RedisClientInfo;
import org.springframework.data.redis.test.condition.EnabledOnRedisAvailable;
import org.springframework.data.redis.test.extension.JedisExtension;

import static org.assertj.core.api.Assertions.*;

/**
 * Integration tests for {@link JedisClientServerCommands}. Tests all methods in direct, transaction, and pipelined
 * modes.
 *
 * @author Tihomir Mateev
 * @since 4.1
 */
@EnabledOnRedisAvailable
@ExtendWith(JedisExtension.class)
class JedisClientServerCommandsIntegrationTests {

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

	// ============ Database Operations ============
	@Test
	void databaseOperationsShouldWork() {
		// Add some data
		connection.stringCommands().set("key1".getBytes(), "value1".getBytes());
		connection.stringCommands().set("key2".getBytes(), "value2".getBytes());

		// Test dbSize - get database size
		Long dbSize = connection.serverCommands().dbSize();
		assertThat(dbSize).isGreaterThanOrEqualTo(2L);

		// Test flushDb - flush current database
		connection.serverCommands().flushDb();
		assertThat(connection.serverCommands().dbSize()).isEqualTo(0L);

		// Add data again
		connection.stringCommands().set("key3".getBytes(), "value3".getBytes());

		// Test flushDb with FlushOption
		connection.serverCommands().flushDb(FlushOption.SYNC);
		assertThat(connection.serverCommands().dbSize()).isEqualTo(0L);

		// Test flushAll - flush all databases
		connection.serverCommands().flushAll();
		assertThat(connection.serverCommands().dbSize()).isEqualTo(0L);

		// Test flushAll with FlushOption
		connection.serverCommands().flushAll(FlushOption.SYNC);
		assertThat(connection.serverCommands().dbSize()).isEqualTo(0L);
	}

	@Test
	void persistenceOperationsShouldWork() {
		// Test bgReWriteAof - background rewrite AOF
		connection.serverCommands().bgReWriteAof();
		// Should not throw exception

		// Test lastSave - get last save time
		Long lastSave = connection.serverCommands().lastSave();
		assertThat(lastSave).isGreaterThan(0L);

		// Test bgSave - should fail because AOF rewrite is in progress
		// Only one background operation (BGSAVE or BGREWRITEAOF) can run at a time
		assertThatExceptionOfType(Exception.class).isThrownBy(() -> {
			connection.serverCommands().bgSave();
		}).withMessageContaining("child process");
	}

	@Test
	void infoOperationsShouldWork() {
		// Test info - get all server info
		Properties info = connection.serverCommands().info();
		assertThat(info).isNotNull().isNotEmpty();

		// Test info with section - get specific section
		Properties serverInfo = connection.serverCommands().info("server");
		assertThat(serverInfo).isNotNull();

		// Test time - get server time
		Long time = connection.serverCommands().time(TimeUnit.MILLISECONDS);
		assertThat(time).isGreaterThan(0L);
	}

	@Test
	void configOperationsShouldWork() {
		// Test getConfig - get configuration
		Properties config = connection.serverCommands().getConfig("maxmemory");
		assertThat(config).isNotNull();

		// Test setConfig - set configuration
		connection.serverCommands().setConfig("maxmemory-policy", "noeviction");
		// Should not throw exception

		// Test resetConfigStats - reset config stats
		connection.serverCommands().resetConfigStats();
		// Should not throw exception

		// Test rewriteConfig - rewrite config file (may fail if no config file)
		try {
			connection.serverCommands().rewriteConfig();
		} catch (Exception e) {
			// Expected if no config file
		}
	}

	@Test
	void clientOperationsShouldWork() {
		// Test setClientName - set client name
		connection.serverCommands().setClientName("testClient".getBytes());
		// Should not throw exception

		// Test getClientName - get client name
		String clientName = connection.serverCommands().getClientName();
		assertThat(clientName).isNotNull();
		assertThat(clientName).isEqualTo("testClient");

		// Test getClientList - get list of clients
		List<RedisClientInfo> clientList = connection.serverCommands().getClientList();
		assertThat(clientList).isNotNull().isNotEmpty();
	}

	@Test
	void replicationOperationsShouldWork() {
		// Test replicaOfNoOne - make server a master
		connection.serverCommands().replicaOfNoOne();
		// Should not throw exception
	}

	@Test
	void transactionShouldExecuteAtomically() {
		// Set up initial state
		connection.stringCommands().set("key1".getBytes(), "value1".getBytes());

		// Execute multiple server operations in a transaction
		connection.multi();
		connection.serverCommands().dbSize();
		connection.serverCommands().time(TimeUnit.MILLISECONDS);
		connection.serverCommands().info("server");
		List<Object> results = connection.exec();

		// Verify all commands executed
		assertThat(results).hasSize(3);
		assertThat(results.get(0)).isInstanceOf(Long.class); // dbSize result
		assertThat(results.get(1)).isInstanceOf(Long.class); // time result
		assertThat(results.get(2)).isInstanceOf(Properties.class); // info result
	}

	@Test
	void pipelineShouldExecuteMultipleCommands() {
		// Set up initial state
		connection.stringCommands().set("key1".getBytes(), "value1".getBytes());
		connection.stringCommands().set("key2".getBytes(), "value2".getBytes());

		// Execute multiple server operations in pipeline
		connection.openPipeline();
		connection.serverCommands().dbSize();
		connection.serverCommands().time(TimeUnit.MILLISECONDS);
		connection.serverCommands().info();
		connection.serverCommands().getConfig("maxmemory");
		List<Object> results = connection.closePipeline();

		// Verify all command results
		assertThat(results).hasSize(4);
		assertThat(results.get(0)).isInstanceOf(Long.class); // dbSize result
		assertThat(results.get(1)).isInstanceOf(Long.class); // time result
		assertThat(results.get(2)).isInstanceOf(Properties.class); // info result
		assertThat(results.get(3)).isInstanceOf(Properties.class); // getConfig result
	}
}
