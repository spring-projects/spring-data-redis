/*
 * Copyright 2015-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.redis.connection.jedis;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.connection.AbstractTransactionalTestBase;
import org.springframework.data.redis.connection.jedis.JedisTransactionalConnectionStarvationTest.PooledJedisContextConfiguration;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.test.annotation.Rollback;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.transaction.annotation.Transactional;

import redis.clients.jedis.JedisPoolConfig;

/**
 * @author Thomas Darimont
 * @author Christoph Strobl
 */
@ContextConfiguration(classes = { PooledJedisContextConfiguration.class })
public class JedisTransactionalConnectionStarvationTest extends AbstractTransactionalTestBase {

	protected static final int MAX_CONNECTIONS = 5;

	@Autowired StringRedisTemplate template;

	protected void tryOperations(int numOperationsToTry) {

		ValueOperations<String, String> ops = template.opsForValue();

		for (int i = 0; i < numOperationsToTry; i++) {
			ops.set("test-key-" + i, "test-value-" + i);
		}
	}

	@Test // DATAREDIS-332
	@Rollback
	public void testNumberOfOperationsIsOne() {
		tryOperations(1);
	}

	@Test // DATAREDIS-332
	@Rollback
	public void testNumberOfOperationsEqualToNumberOfConnections() {
		tryOperations(MAX_CONNECTIONS);
	}

	@Test // DATAREDIS-332
	@Rollback
	public void testNumberOfOperationsGreaterThanNumberOfConnections() {
		tryOperations(MAX_CONNECTIONS + 1);
	}

	@Test // DATAREDIS-548
	@Transactional(readOnly = true)
	public void readonlyTransactionSyncShouldNotExcceedMaxConnections() {
		tryOperations(MAX_CONNECTIONS + 1);
	}

	@Configuration
	public static class PooledJedisContextConfiguration extends RedisContextConfiguration {

		@Bean
		public JedisConnectionFactory redisConnectionFactory() {

			JedisConnectionFactory connectionFactory = new JedisConnectionFactory();
			connectionFactory.setHostName(SettingsUtils.getHost());
			connectionFactory.setPort(SettingsUtils.getPort());

			JedisPoolConfig poolConfig = new JedisPoolConfig();
			poolConfig.setMaxTotal(MAX_CONNECTIONS);
			poolConfig.setMaxIdle(MAX_CONNECTIONS);
			poolConfig.setMaxWaitMillis(2000L);
			connectionFactory.setPoolConfig(poolConfig);
			connectionFactory.afterPropertiesSet();

			return connectionFactory;
		}
	}
}
