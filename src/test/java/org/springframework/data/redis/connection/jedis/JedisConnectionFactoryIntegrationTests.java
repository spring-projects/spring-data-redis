/*
 * Copyright 2017-2018 the original author or authors.
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

import static org.hamcrest.core.IsEqual.*;
import static org.junit.Assert.*;

import redis.clients.jedis.JedisShardInfo;

import org.junit.After;
import org.junit.Test;
import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;

/**
 * Integration tests for {@link JedisConnectionFactory}.
 *
 * @author Mark Paluch
 */
public class JedisConnectionFactoryIntegrationTests {

	private JedisConnectionFactory factory;

	@After
	public void tearDown() {

		if (factory != null) {
			factory.destroy();
		}
	}

	@Test // DATAREDIS-574
	public void shardInfoShouldOverrideFactorySettings() {

		factory = new JedisConnectionFactory(new JedisShardInfo(SettingsUtils.getHost(), SettingsUtils.getPort()));
		factory.setUsePool(false);
		factory.setPassword("foo");
		factory.setHostName("bar");
		factory.setPort(1234);
		factory.afterPropertiesSet();

		assertThat(factory.getConnection().ping(), equalTo("PONG"));
	}

	@Test // DATAREDIS-574
	public void shouldInitiaizeWithStandaloneConfiguration() {

		factory = new JedisConnectionFactory(
				new RedisStandaloneConfiguration(SettingsUtils.getHost(), SettingsUtils.getPort()),
				JedisClientConfiguration.defaultConfiguration());
		factory.afterPropertiesSet();

		assertThat(factory.getConnection().ping(), equalTo("PONG"));
	}
}
