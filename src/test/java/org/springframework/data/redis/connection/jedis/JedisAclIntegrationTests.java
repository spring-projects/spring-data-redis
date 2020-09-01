/*
 * Copyright 2020 the original author or authors.
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
import static org.junit.Assume.*;

import org.junit.Before;
import org.junit.Test;

import org.springframework.data.redis.RedisTestProfileValueSource;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;

/**
 * Integration tests for Redis 6 ACL.
 *
 * @author Mark Paluch
 */
public class JedisAclIntegrationTests {

	@Before
	public void before() {
		assumeTrue(RedisTestProfileValueSource.atLeast("redisVersion", "6.0"));
	}

	@Test
	public void shouldConnectWithDefaultAuthentication() {

		RedisStandaloneConfiguration standaloneConfiguration = new RedisStandaloneConfiguration("localhost", 6382);
		standaloneConfiguration.setPassword("foobared");

		JedisConnectionFactory connectionFactory = new JedisConnectionFactory(standaloneConfiguration);
		connectionFactory.afterPropertiesSet();

		RedisConnection connection = connectionFactory.getConnection();

		assertThat(connection.ping()).isEqualTo("PONG");
		connection.close();

		connectionFactory.destroy();
	}

	@Test // DATAREDIS-1046
	public void shouldConnectStandaloneWithAclAuthentication() {

		RedisStandaloneConfiguration standaloneConfiguration = new RedisStandaloneConfiguration("localhost", 6382);
		standaloneConfiguration.setUsername("spring");
		standaloneConfiguration.setPassword("data");

		JedisConnectionFactory connectionFactory = new JedisConnectionFactory(standaloneConfiguration);
		connectionFactory.afterPropertiesSet();

		RedisConnection connection = connectionFactory.getConnection();

		assertThat(connection.ping()).isEqualTo("PONG");
		connection.close();

		connectionFactory.destroy();
	}

	@Test // DATAREDIS-1046
	public void shouldConnectStandaloneWithAclAuthenticationAndPooling() {

		RedisStandaloneConfiguration standaloneConfiguration = new RedisStandaloneConfiguration("localhost", 6382);
		standaloneConfiguration.setUsername("spring");
		standaloneConfiguration.setPassword("data");

		JedisConnectionFactory connectionFactory = new JedisConnectionFactory(standaloneConfiguration);
		connectionFactory.setUsePool(true);
		connectionFactory.afterPropertiesSet();

		RedisConnection connection = connectionFactory.getConnection();

		assertThat(connection.ping()).isEqualTo("PONG");
		connection.close();

		connectionFactory.destroy();
	}
}
