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
package org.springframework.data.redis.connection.lettuce;

import static org.assertj.core.api.Assertions.*;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.protocol.ProtocolVersion;

import java.io.IOException;
import java.util.Collections;

import org.junit.Assume;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisSentinelConfiguration;
import org.springframework.data.redis.connection.RedisSentinelConnection;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.RedisStaticMasterReplicaConfiguration;
import org.springframework.data.redis.test.extension.LettuceTestClientResources;
import org.springframework.data.redis.test.util.MinimumRedisVersionRule;
import org.springframework.data.redis.test.util.ServerAvailable;

/**
 * Integration tests for Redis 6 ACL.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 */
public class LettuceAclIntegrationTests {

	@ClassRule public static RuleChain requirements = RuleChain.outerRule(ServerAvailable.runningAtLocalhost(6382))
			.around(new MinimumRedisVersionRule());

	@Test // DATAREDIS-1046
	public void shouldConnectWithDefaultAuthentication() {

		RedisStandaloneConfiguration standaloneConfiguration = new RedisStandaloneConfiguration("localhost", 6382);
		standaloneConfiguration.setPassword("foobared");

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(standaloneConfiguration);
		connectionFactory.setClientResources(LettuceTestClientResources.getSharedClientResources());
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

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(standaloneConfiguration);
		connectionFactory.setClientResources(LettuceTestClientResources.getSharedClientResources());
		connectionFactory.afterPropertiesSet();

		RedisConnection connection = connectionFactory.getConnection();

		assertThat(connection.ping()).isEqualTo("PONG");
		connection.close();

		connectionFactory.destroy();
	}

	@Test // DATAREDIS-1145
	public void shouldConnectSentinelWithAuthentication() throws IOException {

		Assume.assumeTrue("Redis Sentinel at localhost:26382 did not answer.",
				ServerAvailable.runningAtLocalhost(26382).isAvailable());

		// Note: As per https://github.com/redis/redis/issues/7708, Sentinel does not support ACL authentication yet.

		LettuceClientConfiguration configuration = LettuceClientConfiguration.builder()
				.clientResources(LettuceTestClientResources.getSharedClientResources())
				.clientOptions(ClientOptions.builder().protocolVersion(ProtocolVersion.RESP2).build()).build();

		RedisSentinelConfiguration sentinelConfiguration = new RedisSentinelConfiguration("mymaster",
				Collections.singleton("localhost:26382"));
		sentinelConfiguration.setSentinelPassword("foobared");

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(sentinelConfiguration, configuration);
		connectionFactory.afterPropertiesSet();

		RedisSentinelConnection connection = connectionFactory.getSentinelConnection();

		assertThat(connection.masters()).isNotEmpty();
		connection.close();

		connectionFactory.destroy();
	}

	@Test // DATAREDIS-1046
	@Ignore("https://github.com/lettuce-io/lettuce-core/issues/1406")
	public void shouldConnectMasterReplicaWithAclAuthentication() {

		RedisStaticMasterReplicaConfiguration masterReplicaConfiguration = new RedisStaticMasterReplicaConfiguration(
				"localhost", 6382);
		masterReplicaConfiguration.setUsername("spring");
		masterReplicaConfiguration.setPassword("data");

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(masterReplicaConfiguration);
		connectionFactory.setClientResources(LettuceTestClientResources.getSharedClientResources());
		connectionFactory.afterPropertiesSet();

		RedisConnection connection = connectionFactory.getConnection();

		assertThat(connection.ping()).isEqualTo("PONG");
		connection.close();

		connectionFactory.destroy();
	}
}
