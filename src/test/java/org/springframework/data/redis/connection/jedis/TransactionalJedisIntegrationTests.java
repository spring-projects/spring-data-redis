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
 * Integration tests for Spring {@code @Transactional} support with legacy {@link JedisConnection}.
 * <p>
 * Tests rollback/commit behavior and transaction synchronization when using
 * the legacy Jedis API with {@link JedisConnection}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @see TransactionalStandardJedisIntegrationTests
 * @see JedisConnection
 */
@ContextConfiguration
public class TransactionalJedisIntegrationTests extends AbstractTransactionalTestBase {

	@Configuration
	public static class JedisContextConfiguration extends RedisContextConfiguration {

		@Override
		@Bean
		public JedisConnectionFactory redisConnectionFactory() {
			// Use anonymous subclass to force legacy JedisConnection mode
			return new JedisConnectionFactory(SettingsUtils.standaloneConfiguration()) {
				@Override
				public boolean isUsingUnifiedJedisConnection() {
					return false; // Force legacy JedisConnection
				}
			};
		}
	}
}
