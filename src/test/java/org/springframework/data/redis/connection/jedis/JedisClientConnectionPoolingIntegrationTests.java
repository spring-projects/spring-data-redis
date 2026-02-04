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

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.util.ConnectionVerifier;

import redis.clients.jedis.Connection;

import static org.assertj.core.api.Assertions.*;

/**
 * Integration tests for {@link JedisClientConnectionFactory} connection pooling behavior.
 *
 * @author Tihomir Mateev
 * @since 4.1
 */
class JedisClientConnectionPoolingIntegrationTests {

	private JedisClientConnectionFactory factory;

	@AfterEach
	void tearDown() {
		if (factory != null) {
			factory.destroy();
		}
	}

	@Test // GH-XXXX
	void shouldNotUsePoolingByDefault() {

		factory = new JedisClientConnectionFactory(
				new RedisStandaloneConfiguration(SettingsUtils.getHost(), SettingsUtils.getPort()),
				JedisClientConfiguration.defaultConfiguration());
		factory.afterPropertiesSet();
		factory.start();

		assertThat(factory.getUsePool()).isFalse();
	}

	@Test // GH-XXXX
	void shouldRespectPoolConfiguration() {

		GenericObjectPoolConfig<Connection> poolConfig = new GenericObjectPoolConfig<>();
		poolConfig.setMaxTotal(5);
		poolConfig.setMaxIdle(3);
		poolConfig.setMinIdle(1);

		JedisClientConfiguration clientConfig = JedisClientConfiguration.builder().usePooling().poolConfig(poolConfig)
				.build();

		factory = new JedisClientConnectionFactory(
				new RedisStandaloneConfiguration(SettingsUtils.getHost(), SettingsUtils.getPort()), clientConfig);
		factory.afterPropertiesSet();
		factory.start();

		assertThat(factory.getClientConfiguration().getPoolConfig()).hasValue(poolConfig);
		assertThat(factory.getClientConfiguration().getPoolConfig().get().getMaxTotal()).isEqualTo(5);
		assertThat(factory.getClientConfiguration().getPoolConfig().get().getMaxIdle()).isEqualTo(3);
		assertThat(factory.getClientConfiguration().getPoolConfig().get().getMinIdle()).isEqualTo(1);
	}

	@Test // GH-XXXX
	void shouldReuseConnectionsFromPool() {

		GenericObjectPoolConfig<Connection> poolConfig = new GenericObjectPoolConfig<>();
		poolConfig.setMaxTotal(1);
		poolConfig.setMaxIdle(1);

		JedisClientConfiguration clientConfig = JedisClientConfiguration.builder().usePooling().poolConfig(poolConfig)
				.build();

		factory = new JedisClientConnectionFactory(
				new RedisStandaloneConfiguration(SettingsUtils.getHost(), SettingsUtils.getPort()), clientConfig);
		factory.afterPropertiesSet();
		factory.start();

		// Get connection, use it, close it
		try (RedisConnection conn1 = factory.getConnection()) {
			assertThat(conn1.ping()).isEqualTo("PONG");
		}

		// Get another connection - should reuse from pool
		try (RedisConnection conn2 = factory.getConnection()) {
			assertThat(conn2.ping()).isEqualTo("PONG");
		}
	}

	@Test // GH-XXXX
	void shouldEnforceMaxTotalConnections() {

		GenericObjectPoolConfig<Connection> poolConfig = new GenericObjectPoolConfig<>();
		poolConfig.setMaxTotal(2);
		poolConfig.setMaxIdle(2);

		JedisClientConfiguration clientConfig = JedisClientConfiguration.builder().usePooling().poolConfig(poolConfig)
				.build();

		factory = new JedisClientConnectionFactory(
				new RedisStandaloneConfiguration(SettingsUtils.getHost(), SettingsUtils.getPort()), clientConfig);
		factory.afterPropertiesSet();
		factory.start();

		// Get max connections
		try (RedisConnection conn1 = factory.getConnection(); RedisConnection conn2 = factory.getConnection()) {
			assertThat(conn1.ping()).isEqualTo("PONG");
			assertThat(conn2.ping()).isEqualTo("PONG");
		}
	}

	@Test // GH-XXXX
	void shouldReleaseConnectionOnException() {

		GenericObjectPoolConfig<Connection> poolConfig = new GenericObjectPoolConfig<>();
		poolConfig.setMaxTotal(1);

		JedisClientConfiguration clientConfig = JedisClientConfiguration.builder().usePooling().poolConfig(poolConfig)
				.build();

		factory = new JedisClientConnectionFactory(
				new RedisStandaloneConfiguration(SettingsUtils.getHost(), SettingsUtils.getPort()), clientConfig);
		factory.afterPropertiesSet();
		factory.start();

		try (RedisConnection conn = factory.getConnection()) {
			try {
				conn.stringCommands().get(null); // Should throw exception
			} catch (Exception ignore) {
				// Expected
			}
		}

		// Connection should be released back to pool despite exception
		try (RedisConnection conn2 = factory.getConnection()) {
			assertThat(conn2.serverCommands().dbSize()).isNotNull();
		}
	}

	@Test // GH-XXXX
	void shouldHandleDatabaseSelection() {

		GenericObjectPoolConfig<Connection> poolConfig = new GenericObjectPoolConfig<>();
		poolConfig.setMaxTotal(1);
		poolConfig.setMaxIdle(1);

		JedisClientConfiguration clientConfig = JedisClientConfiguration.builder().usePooling().poolConfig(poolConfig)
				.build();

		RedisStandaloneConfiguration standaloneConfig = new RedisStandaloneConfiguration(SettingsUtils.getHost(),
				SettingsUtils.getPort());
		standaloneConfig.setDatabase(1);

		factory = new JedisClientConnectionFactory(standaloneConfig, clientConfig);
		factory.afterPropertiesSet();
		factory.start();

		ConnectionVerifier.create(factory).execute(RedisConnection::ping).verifyAndClose();
	}

	@Test // GH-XXXX
	void shouldFailWithInvalidDatabase() {

		RedisStandaloneConfiguration standaloneConfig = new RedisStandaloneConfiguration(SettingsUtils.getHost(),
				SettingsUtils.getPort());
		standaloneConfig.setDatabase(77); // Invalid database

		factory = new JedisClientConnectionFactory(standaloneConfig, JedisClientConfiguration.defaultConfiguration());
		factory.afterPropertiesSet();
		factory.start();

		// Exception is thrown when actually using the connection, not when getting it
		assertThatExceptionOfType(InvalidDataAccessApiUsageException.class).isThrownBy(() -> {
			try (RedisConnection conn = factory.getConnection()) {
				conn.ping();
			}
		}).withMessageContaining("DB index is out of range");
	}

	@Test // GH-XXXX
	void shouldReturnConnectionToPoolAfterPipelineSelect() {

		GenericObjectPoolConfig<Connection> poolConfig = new GenericObjectPoolConfig<>();
		poolConfig.setMaxTotal(1);
		poolConfig.setMaxIdle(1);

		JedisClientConfiguration clientConfig = JedisClientConfiguration.builder().usePooling().poolConfig(poolConfig)
				.build();

		RedisStandaloneConfiguration standaloneConfig = new RedisStandaloneConfiguration(SettingsUtils.getHost(),
				SettingsUtils.getPort());
		standaloneConfig.setDatabase(1);

		factory = new JedisClientConnectionFactory(standaloneConfig, clientConfig);
		factory.afterPropertiesSet();
		factory.start();

		ConnectionVerifier.create(factory).execute(RedisConnection::openPipeline).verifyAndRun(connectionFactory -> {
			connectionFactory.getConnection();
			connectionFactory.destroy();
		});
	}

	@Test // GH-XXXX
	void shouldDisablePoolingWhenConfigured() {

		JedisClientConfiguration clientConfig = JedisClientConfiguration.builder().build(); // No pooling

		factory = new JedisClientConnectionFactory(
				new RedisStandaloneConfiguration(SettingsUtils.getHost(), SettingsUtils.getPort()), clientConfig);
		factory.afterPropertiesSet();
		factory.start();

		assertThat(factory.getUsePool()).isFalse();

		try (RedisConnection conn = factory.getConnection()) {
			assertThat(conn.ping()).isEqualTo("PONG");
		}
	}
}
