/*
 * Copyright 2020-2025 the original author or authors.
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

import java.io.IOException;
import java.util.Collections;

import org.junit.jupiter.api.Test;
import org.springframework.data.redis.ConnectionFactoryTracker;
import org.springframework.data.redis.connection.RedisSentinelConfiguration;
import org.springframework.data.redis.connection.RedisSentinelConnection;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.test.condition.EnabledOnRedisAvailable;
import org.springframework.data.redis.test.condition.EnabledOnRedisSentinelAvailable;
import org.springframework.data.redis.test.condition.EnabledOnRedisVersion;
import org.springframework.data.redis.util.ConnectionVerifier;

/**
 * Integration tests for Redis 6 ACL.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 */
@EnabledOnRedisVersion("6.0")
@EnabledOnRedisAvailable(6382)
class JedisAclIntegrationTests {

	@Test
	void shouldConnectWithDefaultAuthentication() {

		RedisStandaloneConfiguration standaloneConfiguration = new RedisStandaloneConfiguration("localhost", 6382);
		standaloneConfiguration.setPassword("foobared");

		ConnectionVerifier.create(new JedisConnectionFactory(standaloneConfiguration)) //
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

		ConnectionVerifier.create(new JedisConnectionFactory(standaloneConfiguration)) //
				.execute(connection -> {
					assertThat(connection.ping()).isEqualTo("PONG");
				}) //
				.verifyAndClose();
	}

	@Test // DATAREDIS-1145
	@EnabledOnRedisSentinelAvailable(26382)
	void shouldConnectSentinelWithAclAuthentication() throws IOException {

		// Note: As per https://github.com/redis/redis/issues/7708, Sentinel does not support ACL authentication yet.

		RedisSentinelConfiguration sentinelConfiguration = new RedisSentinelConfiguration("mymaster",
				Collections.singleton("localhost:26382"));
		sentinelConfiguration.setSentinelPassword("foobared");

		JedisConnectionFactory connectionFactory = new JedisConnectionFactory(sentinelConfiguration);
		connectionFactory.afterPropertiesSet();
		ConnectionFactoryTracker.add(connectionFactory);

		try (RedisSentinelConnection connection = connectionFactory.getSentinelConnection()) {
			assertThat(connection.masters()).isNotEmpty();
		}

		connectionFactory.destroy();
	}

	@Test // DATAREDIS-1046
	void shouldConnectStandaloneWithAclAuthenticationAndPooling() {

		RedisStandaloneConfiguration standaloneConfiguration = new RedisStandaloneConfiguration("localhost", 6382);
		standaloneConfiguration.setUsername("spring");
		standaloneConfiguration.setPassword("data");

		JedisConnectionFactory connectionFactory = new JedisConnectionFactory(standaloneConfiguration,
				JedisClientConfiguration.builder().usePooling().build());

		ConnectionVerifier.create(connectionFactory) //
				.execute(connection -> {
					assertThat(connection.ping()).isEqualTo("PONG");
				}) //
				.verifyAndClose();
	}
}
