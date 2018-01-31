/*
 * Copyright 2014-2018 the original author or authors.
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

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisSentinelConfiguration;
import org.springframework.data.redis.test.util.RedisSentinelRule;

/**
 * Sentinel integration tests for {@link JedisConnectionFactory}.
 *
 * @author Christoph Strobl
 * @author Fu Jian
 * @author Mark Paluch
 */
public class JedisConnectionFactorySentinelIntegrationTests {

	private static final RedisSentinelConfiguration SENTINEL_CONFIG = new RedisSentinelConfiguration().master("mymaster")
			.sentinel("127.0.0.1", 26379).sentinel("127.0.0.1", 26380);
	private JedisConnectionFactory factory;

	public @Rule RedisSentinelRule sentinelRule = RedisSentinelRule.forConfig(SENTINEL_CONFIG).oneActive();

	@After
	public void tearDown() {

		if (factory != null) {
			factory.destroy();
		}
	}

	@Test // DATAREDIS-574, DATAREDIS-765
	public void shouldInitializeWithSentinelConfiguration() {

		JedisClientConfiguration clientConfiguration = JedisClientConfiguration.builder() //
				.clientName("clientName") //
				.build();

		factory = new JedisConnectionFactory(SENTINEL_CONFIG, clientConfiguration);
		factory.afterPropertiesSet();

		RedisConnection connection = factory.getConnection();

		assertThat(factory.getUsePool(), is(true));
		assertThat(connection.getClientName(), equalTo("clientName"));
	}

	@Test // DATAREDIS-324
	public void shouldSendCommandCorrectlyViaConnectionFactoryUsingSentinel() {

		factory = new JedisConnectionFactory(SENTINEL_CONFIG);
		factory.afterPropertiesSet();

		assertThat(factory.getConnection().ping(), equalTo("PONG"));
	}

	@Test // DATAREDIS-552
	public void getClientNameShouldEqualWithFactorySetting() {

		factory = new JedisConnectionFactory(SENTINEL_CONFIG);
		factory.setClientName("clientName");
		factory.afterPropertiesSet();

		assertThat(factory.getConnection().getClientName(), equalTo("clientName"));
	}
}
