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

import org.junit.jupiter.api.Test;
import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.connection.RedisConnectionCommands;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.test.condition.EnabledOnRedisAvailable;
import org.springframework.data.redis.test.condition.EnabledOnRedisVersion;
import org.springframework.data.redis.util.ConnectionVerifier;

import static org.assertj.core.api.Assertions.*;

/**
 * Integration tests for Redis 6+ ACL authentication using {@link JedisClientConnectionFactory}.
 *
 * @author Tihomir Mateev
 * @since 4.1
 */
@EnabledOnRedisVersion("6.0")
@EnabledOnRedisAvailable(6382)
class JedisClientAclIntegrationTests {

	@Test
	void shouldConnectWithDefaultAuthentication() {

		RedisStandaloneConfiguration standaloneConfiguration = new RedisStandaloneConfiguration("localhost", 6382);
		standaloneConfiguration.setPassword("foobared");

		ConnectionVerifier.create(new JedisClientConnectionFactory(standaloneConfiguration)) //
				.execute(connection -> {
					assertThat(connection.ping()).isEqualTo("PONG");
				}) //
				.verifyAndClose();
	}

	@Test // DATAREDIS-1046
	void shouldConnectStandaloneWithAclAuthentication() {

		RedisStandaloneConfiguration standaloneConfiguration = new RedisStandaloneConfiguration("localhost", 6382);
		standaloneConfiguration.setUsername("spring");
		standaloneConfiguration.setPassword("data");

		ConnectionVerifier.create(new JedisClientConnectionFactory(standaloneConfiguration)) //
				.execute(connection -> {
					assertThat(connection.ping()).isEqualTo("PONG");
				}) //
				.verifyAndClose();
	}

	@Test // DATAREDIS-1046
	void shouldConnectStandaloneWithAclAuthenticationAndPooling() {

		RedisStandaloneConfiguration standaloneConfiguration = new RedisStandaloneConfiguration("localhost", 6382);
		standaloneConfiguration.setUsername("spring");
		standaloneConfiguration.setPassword("data");

		JedisClientConnectionFactory connectionFactory = new JedisClientConnectionFactory(standaloneConfiguration,
				JedisClientConfiguration.builder().usePooling().build());

		ConnectionVerifier.create(connectionFactory) //
				.execute(connection -> {
					assertThat(connection.ping()).isEqualTo("PONG");
				}) //
				.verifyAndClose();
	}

	@Test
	void shouldFailWithWrongPassword() {

		RedisStandaloneConfiguration standaloneConfiguration = new RedisStandaloneConfiguration("localhost", 6382);
		standaloneConfiguration.setPassword("wrong-password");

		JedisClientConnectionFactory connectionFactory = new JedisClientConnectionFactory(standaloneConfiguration);

		assertThatThrownBy(() -> {
			ConnectionVerifier.create(connectionFactory) //
					.execute(RedisConnectionCommands::ping) //
					.verifyAndClose();
		}).hasMessageContaining("WRONGPASS");
	}

	@Test
	void shouldFailWithWrongUsername() {

		RedisStandaloneConfiguration standaloneConfiguration = new RedisStandaloneConfiguration("localhost", 6382);
		standaloneConfiguration.setUsername("wrong-user");
		standaloneConfiguration.setPassword("data");

		JedisClientConnectionFactory connectionFactory = new JedisClientConnectionFactory(standaloneConfiguration);

		assertThatThrownBy(() -> {
			ConnectionVerifier.create(connectionFactory) //
					.execute(RedisConnectionCommands::ping) //
					.verifyAndClose();
		}).hasMessageContaining("WRONGPASS");
	}

	@Test
	void shouldConnectWithPasswordOnly() {

		RedisStandaloneConfiguration standaloneConfiguration = new RedisStandaloneConfiguration(SettingsUtils.getHost(),
				SettingsUtils.getPort());

		// No password set for default Redis instance
		ConnectionVerifier
				.create(
						new JedisClientConnectionFactory(standaloneConfiguration, JedisClientConfiguration.defaultConfiguration())) //
				.execute(connection -> {
					assertThat(connection.ping()).isEqualTo("PONG");
				}) //
				.verifyAndClose();
	}
}
