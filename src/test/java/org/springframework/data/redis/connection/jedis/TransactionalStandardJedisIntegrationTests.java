/*
 * Copyright 2014-present the original author or authors.
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

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.connection.AbstractTransactionalTestBase;
import org.springframework.test.context.ContextConfiguration;

/**
 * Integration tests for Spring {@code @Transactional} support with {@link UnifiedJedisConnection}.
 * <p>
 * Tests rollback/commit behavior and transaction synchronization when using
 * the modern Jedis 7.x API with {@link UnifiedJedisConnection}.
 *
 * @author Tihomir Mateev
 * @since 4.1
 * @see TransactionalJedisIntegrationTests
 * @see UnifiedJedisConnection
 */
@ContextConfiguration
public class TransactionalStandardJedisIntegrationTests extends AbstractTransactionalTestBase {

	@Configuration
	public static class StandardJedisContextConfiguration extends RedisContextConfiguration {

		@Override
		@Bean
		public JedisConnectionFactory redisConnectionFactory() {
			JedisClientConfiguration clientConfig = JedisClientConfiguration.builder().build();
			return new JedisConnectionFactory(SettingsUtils.standaloneConfiguration(), clientConfig);
		}
	}
}

