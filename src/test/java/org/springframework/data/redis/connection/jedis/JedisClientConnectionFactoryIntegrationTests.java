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

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.connection.ClusterCommandExecutor;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.core.types.RedisClientInfo;
import org.springframework.data.redis.test.condition.EnabledOnRedisClusterAvailable;
import org.springframework.data.redis.util.ConnectionVerifier;
import org.springframework.data.redis.util.RedisClientLibraryInfo;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Integration tests for {@link JedisClientConnectionFactory}.
 * <p>
 * These tests require Redis 7.2+ to be available.
 *
 * @author Tihomir Mateev
 * @since 4.1
 */
class JedisClientConnectionFactoryIntegrationTests {

	private @Nullable JedisClientConnectionFactory factory;

	@AfterEach
	void tearDown() {

		if (factory != null) {
			factory.destroy();
		}
	}

	@Test
	void shouldInitializeWithStandaloneConfiguration() {

		factory = new JedisClientConnectionFactory(
				new RedisStandaloneConfiguration(SettingsUtils.getHost(), SettingsUtils.getPort()),
				JedisClientConfiguration.defaultConfiguration());
		factory.afterPropertiesSet();
		factory.start();

		try (RedisConnection connection = factory.getConnection()) {
			assertThat(connection.ping()).isEqualTo("PONG");
		}
	}

	@Test
	void connectionAppliesClientName() {

		factory = new JedisClientConnectionFactory(
				new RedisStandaloneConfiguration(SettingsUtils.getHost(), SettingsUtils.getPort()),
				JedisClientConfiguration.builder().clientName("jedis-client-test").build());
		factory.afterPropertiesSet();
		factory.start();

		try (RedisConnection connection = factory.getConnection()) {
			assertThat(connection.serverCommands().getClientName()).isEqualTo("jedis-client-test");
		}
	}

	@Test
	void clientListReportsJedisLibNameWithSpringDataSuffix() {

		factory = new JedisClientConnectionFactory(
				new RedisStandaloneConfiguration(SettingsUtils.getHost(), SettingsUtils.getPort()),
				JedisClientConfiguration.builder().clientName("jedisClientLibName").build());
		factory.afterPropertiesSet();
		factory.start();

		try (RedisConnection connection = factory.getConnection()) {

			RedisClientInfo self = connection.serverCommands().getClientList().stream()
					.filter(info -> "jedisClientLibName".equals(info.getName())).findFirst().orElseThrow();

			String expectedUpstreamDriver = "%s_v%s".formatted(RedisClientLibraryInfo.FRAMEWORK_NAME,
					RedisClientLibraryInfo.getVersion());
			assertThat(self.get("lib-name")).startsWith("jedis(" + expectedUpstreamDriver);
		} finally {
			factory.destroy();
		}
	}

	@Test
	void startStopStartConnectionFactory() {

		factory = new JedisClientConnectionFactory(
				new RedisStandaloneConfiguration(SettingsUtils.getHost(), SettingsUtils.getPort()),
				JedisClientConfiguration.defaultConfiguration());
		factory.afterPropertiesSet();

		factory.start();
		assertThat(factory.isRunning()).isTrue();

		factory.stop();
		assertThat(factory.isRunning()).isFalse();
		assertThatIllegalStateException().isThrownBy(() -> factory.getConnection());

		factory.start();
		assertThat(factory.isRunning()).isTrue();
		try (RedisConnection connection = factory.getConnection()) {
			assertThat(connection.ping()).isEqualTo("PONG");
		}

		factory.destroy();
	}

	@Test
	void shouldReturnStandaloneConfiguration() {

		RedisStandaloneConfiguration configuration = new RedisStandaloneConfiguration();
		factory = new JedisClientConnectionFactory(configuration, JedisClientConfiguration.defaultConfiguration());

		assertThat(factory.getStandaloneConfiguration()).isSameAs(configuration);
		assertThat(factory.getSentinelConfiguration()).isNull();
		assertThat(factory.getClusterConfiguration()).isNull();
	}

	@Test
	void shouldConnectWithPassword() {

		RedisStandaloneConfiguration standaloneConfiguration = new RedisStandaloneConfiguration(SettingsUtils.getHost(),
				SettingsUtils.getPort());

		ConnectionVerifier
				.create(
						new JedisClientConnectionFactory(standaloneConfiguration, JedisClientConfiguration.defaultConfiguration())) //
				.execute(connection -> assertThat(connection.ping()).isEqualTo("PONG")).verifyAndClose();
	}

	@Test // GH-XXXX
	@EnabledOnRedisClusterAvailable
	void configuresExecutorCorrectlyForCluster() {

		AsyncTaskExecutor mockTaskExecutor = mock(AsyncTaskExecutor.class);

		factory = new JedisClientConnectionFactory(SettingsUtils.clusterConfiguration());
		factory.setExecutor(mockTaskExecutor);
		factory.start();

		ClusterCommandExecutor clusterCommandExecutor = factory.getRequiredClusterCommandExecutor();
		assertThat(clusterCommandExecutor).extracting("executor").isEqualTo(mockTaskExecutor);

		factory.destroy();
	}
}
