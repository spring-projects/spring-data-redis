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

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import redis.clients.jedis.RedisProtocol;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.csc.Cache;

import org.junit.jupiter.api.Test;

import org.springframework.data.redis.connection.RedisSentinelConfiguration;

/**
 * Integration tests for {@link JedisConnectionFactory} using {@link redis.clients.jedis.UnifiedJedis}.
 *
 * @author Mark Paluch
 */
class UnifiedJedisConnectionFactorySentinelIntegrationTests
		extends JedisConnectionFactorySentinelIntegrationTests {

	/**
	 * Creates a {@link JedisConnectionFactory} that forces legacy mode for testing the legacy sentinel code path.
	 */
	JedisConnectionFactory createConnectionFactory(RedisSentinelConfiguration configuration) {
		return new JedisConnectionFactory(configuration);
	}

	/**
	 * Creates a {@link JedisConnectionFactory} that forces legacy mode for testing the legacy sentinel code path.
	 */
	@Override
	JedisConnectionFactory createConnectionFactory(RedisSentinelConfiguration configuration,
			JedisClientConfiguration clientConfiguration) {
		return new JedisConnectionFactory(configuration, clientConfiguration);
	}

	@Test // GH-3315
	void shouldCustomizeStandaloneClient() {

		Cache c = mock(Cache.class);
		factory = new JedisConnectionFactory(SENTINEL_CONFIG,
				JedisClientConfiguration.builder().customizeClientConfig(it -> it.protocol(RedisProtocol.RESP3))
						.customizeClient(builder -> builder.cache(c)).build());
		factory.afterPropertiesSet();
		factory.start();

		UnifiedJedis client = factory.getRequiredRedisClient();

		assertThat(client.getCache()).isEqualTo(c);
	}
}
