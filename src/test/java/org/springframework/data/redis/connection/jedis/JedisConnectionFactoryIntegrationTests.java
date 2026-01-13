/*
 * Copyright 2017-present the original author or authors.
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

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.List;
import org.springframework.data.redis.core.types.RedisClientInfo;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.connection.ClusterCommandExecutor;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.test.condition.EnabledOnRedisClusterAvailable;

/**
 * Integration tests for {@link JedisConnectionFactory}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 */
class JedisConnectionFactoryIntegrationTests {

	private @Nullable JedisConnectionFactory factory;

	@AfterEach
	void tearDown() {

		if (factory != null) {
			factory.destroy();
		}
	}

	@Test // DATAREDIS-574
	void shouldInitializeWithStandaloneConfiguration() {

		factory = new JedisConnectionFactory(
				new RedisStandaloneConfiguration(SettingsUtils.getHost(), SettingsUtils.getPort()),
				JedisClientConfiguration.defaultConfiguration());
		factory.afterPropertiesSet();
		factory.start();

		try (RedisConnection connection = factory.getConnection()) {
			assertThat(connection.ping()).isEqualTo("PONG");
		}
	}

	@Test // DATAREDIS-575
	void connectionAppliesClientName() {

		factory = new JedisConnectionFactory(
				new RedisStandaloneConfiguration(SettingsUtils.getHost(), SettingsUtils.getPort()),
				JedisClientConfiguration.builder().clientName("clientName").build());
		factory.afterPropertiesSet();
		factory.start();

		RedisConnection connection = factory.getConnection();

		assertThat(connection.getClientName()).isEqualTo("clientName");
	}

	@Test // CLIENT SETINFO
	void clientListReportsJedisLibNameWithSpringDataSuffix() {

		factory = new JedisConnectionFactory(
				new RedisStandaloneConfiguration(SettingsUtils.getHost(), SettingsUtils.getPort()),
				JedisClientConfiguration.builder().clientName("clientName").build());
		factory.afterPropertiesSet();
		factory.start();

		try (RedisConnection connection = factory.getConnection()) {

			List<RedisClientInfo> clients = connection.serverCommands().getClientList();

			RedisClientInfo self = clients.stream()
					.filter(info -> "clientName".equals(info.getName()))
					.findFirst()
					.orElseThrow();

			String libName = self.get("lib-name");

			assertThat(libName).isNotNull();
			assertThat(libName).contains("jedis(");
			assertThat(libName).contains("sdr_v");
		}
	}

	@Test // CLIENT SETINFO
	void clientListReportsJedisLibNameWithUpstreamSuffix() {

		factory = new JedisConnectionFactory(
				new RedisStandaloneConfiguration(SettingsUtils.getHost(), SettingsUtils.getPort()),
				JedisClientConfiguration.builder().clientName("clientName").build());
		factory.afterPropertiesSet();
		factory.start();

		try (RedisConnection connection = factory.getConnection()) {

			List<RedisClientInfo> clients = connection.serverCommands().getClientList();

			RedisClientInfo self = clients.stream()
					.filter(info -> "clientName".equals(info.getName()))
					.findFirst()
					.orElseThrow();

			String libName = self.get("lib-name");

			assertThat(libName).isNotNull();
			assertThat(libName).contains("jedis(");
			assertThat(libName).contains("sdr_v");
		}
	}



	@Test // GH-2503
	void startStopStartConnectionFactory() {

		factory = new JedisConnectionFactory(
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

	@Test // GH-2594
	@EnabledOnRedisClusterAvailable
	void configuresExecutorCorrectly() {

		AsyncTaskExecutor mockTaskExecutor = mock(AsyncTaskExecutor.class);

		JedisConnectionFactory factory = new JedisConnectionFactory(SettingsUtils.clusterConfiguration());
		factory.setExecutor(mockTaskExecutor);
		factory.start();

		ClusterCommandExecutor clusterCommandExecutor = factory.getRequiredClusterCommandExecutor();
		assertThat(clusterCommandExecutor).extracting("executor").isEqualTo(mockTaskExecutor);

		factory.destroy();
	}
}
