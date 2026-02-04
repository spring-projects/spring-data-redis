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
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.util.ConnectionVerifier;
import org.springframework.test.util.ReflectionTestUtils;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Error handling and recovery tests for {@link JedisClientConnectionFactory}.
 *
 * @author Tihomir Mateev
 * @since 4.1
 */
class JedisClientConnectionErrorHandlingTests {

	private JedisClientConnectionFactory factory;

	@AfterEach
	void tearDown() {
		if (factory != null) {
			factory.destroy();
		}
	}

	@Test // GH-XXXX
	void shouldFailWithInvalidHost() {

		factory = new JedisClientConnectionFactory(
				new RedisStandaloneConfiguration("invalid-host-that-does-not-exist", 6379));
		factory.afterPropertiesSet();
		factory.start();

		assertThatExceptionOfType(RedisConnectionFailureException.class).isThrownBy(() -> factory.getConnection().ping());
	}

	@Test // GH-XXXX
	void shouldFailWithInvalidPort() {

		factory = new JedisClientConnectionFactory(new RedisStandaloneConfiguration(SettingsUtils.getHost(), 9999));
		factory.afterPropertiesSet();
		factory.start();

		assertThatExceptionOfType(RedisConnectionFailureException.class).isThrownBy(() -> factory.getConnection().ping());
	}

	@Test // GH-XXXX - DATAREDIS-714
	void shouldFailWithInvalidDatabase() {

		RedisStandaloneConfiguration config = new RedisStandaloneConfiguration(SettingsUtils.getHost(),
				SettingsUtils.getPort());
		config.setDatabase(77);
		factory = new JedisClientConnectionFactory(config);
		factory.afterPropertiesSet();
		factory.start();

		assertThatExceptionOfType(InvalidDataAccessApiUsageException.class).isThrownBy(() -> {
			try (RedisConnection conn = factory.getConnection()) {
				conn.ping(); // Trigger actual connection
			}
		}).withMessageContaining("DB index is out of range");
	}

	@Test // GH-XXXX
	void shouldReleaseConnectionOnException() {

		GenericObjectPoolConfig<Object> poolConfig = new GenericObjectPoolConfig<>();
		poolConfig.setMaxTotal(1);

		factory = new JedisClientConnectionFactory(
				new RedisStandaloneConfiguration(SettingsUtils.getHost(), SettingsUtils.getPort()),
				JedisClientConfiguration.builder().usePooling().poolConfig(poolConfig).build());
		factory.afterPropertiesSet();
		factory.start();

		try (RedisConnection conn = factory.getConnection()) {
			try {
				conn.get(null); // Should throw exception
			} catch (Exception ignore) {
				// Expected
			}
		}

		// Should be able to get another connection (pool not exhausted)
		try (RedisConnection conn = factory.getConnection()) {
			assertThat(conn.ping()).isEqualTo("PONG");
		}
	}

	@Test // GH-XXXX - GH-2356
	void closeWithFailureShouldReleaseConnection() {

		GenericObjectPoolConfig<Object> poolConfig = new GenericObjectPoolConfig<>();
		poolConfig.setMaxTotal(1);

		factory = new JedisClientConnectionFactory(
				new RedisStandaloneConfiguration(SettingsUtils.getHost(), SettingsUtils.getPort()),
				JedisClientConfiguration.builder().usePooling().poolConfig(poolConfig).build());

		ConnectionVerifier.create(factory) //
				.execute(connection -> {
					JedisSubscription subscriptionMock = mock(JedisSubscription.class);
					doThrow(new IllegalStateException()).when(subscriptionMock).close();
					ReflectionTestUtils.setField(connection, "subscription", subscriptionMock);
				}) //
				.verifyAndRun(connectionFactory -> {
					connectionFactory.getConnection().dbSize();
					connectionFactory.destroy();
				});
	}

	@Test // GH-XXXX - GH-2057
	void getConnectionShouldFailIfNotInitialized() {

		factory = new JedisClientConnectionFactory();

		assertThatIllegalStateException().isThrownBy(() -> factory.getConnection());
		assertThatIllegalStateException().isThrownBy(() -> factory.getClusterConnection());
		assertThatIllegalStateException().isThrownBy(() -> factory.getSentinelConnection());
	}
}
