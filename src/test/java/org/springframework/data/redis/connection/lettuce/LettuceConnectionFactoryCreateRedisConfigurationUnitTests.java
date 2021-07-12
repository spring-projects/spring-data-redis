/*
 * Copyright 2015-2021 the original author or authors.
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

import io.lettuce.core.RedisURI;

import org.assertj.core.api.recursive.comparison.RecursiveComparisonConfiguration;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import org.springframework.data.redis.connection.RedisConfiguration;
import org.springframework.data.redis.connection.RedisNode;
import org.springframework.data.redis.connection.RedisSentinelConfiguration;
import org.springframework.data.redis.connection.RedisSocketConfiguration;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;

/**
 * Unit tests for the {@link LettuceConnectionFactory#createRedisConfiguration(RedisURI)} factory method.
 *
 * @author Chris Bono
 */
class LettuceConnectionFactoryCreateRedisConfigurationUnitTests {

	@Test
	void requiresRedisURI() {
		assertThatThrownBy(() -> LettuceConnectionFactory.createRedisConfiguration(null))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessage("RedisURI must not be null");
	}

	@Nested // GH-2117
	class CreateRedisSentinelConfiguration {

		@Test
		void minimalFieldsSetOnRedisURI() {
			RedisURI redisURI = RedisURI.create("redis-sentinel://myserver?sentinelMasterId=5150");
			RedisSentinelConfiguration expectedSentinelConfiguration = new RedisSentinelConfiguration();
			expectedSentinelConfiguration.setMaster("5150");
			expectedSentinelConfiguration.addSentinel(new RedisNode("myserver", 26379));

			RedisConfiguration sentinelConfiguration = LettuceConnectionFactory.createRedisConfiguration(redisURI);

			assertThat(sentinelConfiguration).usingRecursiveComparison(sentinelCompareConfig())
					.isInstanceOf(RedisSentinelConfiguration.class)
					.isEqualTo(expectedSentinelConfiguration);
		}

		@Test
		void allFieldsSetOnRedisURI() {
			RedisURI redisURI = RedisURI.create("redis-sentinel://fooUser:fooPass@myserver1:111,myserver2:222/7?sentinelMasterId=5150");
			// Set the passwords directly on the sentinels so that it gets picked up by converter
			char[] sentinelPass = "changeme".toCharArray();
			redisURI.getSentinels().forEach(sentinelRedisUri -> sentinelRedisUri.setPassword(sentinelPass));
			RedisSentinelConfiguration expectedSentinelConfiguration = new RedisSentinelConfiguration();
			expectedSentinelConfiguration.setMaster("5150");
			expectedSentinelConfiguration.setDatabase(7);
			expectedSentinelConfiguration.setUsername("fooUser");
			expectedSentinelConfiguration.setPassword("fooPass");
			expectedSentinelConfiguration.setSentinelPassword(sentinelPass);
			expectedSentinelConfiguration.addSentinel(new RedisNode("myserver1", 111));
			expectedSentinelConfiguration.addSentinel(new RedisNode("myserver2", 222));

			RedisConfiguration sentinelConfiguration = LettuceConnectionFactory.createRedisConfiguration(redisURI);

			assertThat(sentinelConfiguration).usingRecursiveComparison(sentinelCompareConfig())
					.isInstanceOf(RedisSentinelConfiguration.class)
					.isEqualTo(expectedSentinelConfiguration);
		}

		// RedisSentinelConfiguration does not provide equals impl
		private RecursiveComparisonConfiguration sentinelCompareConfig() {
			return RecursiveComparisonConfiguration.builder().withComparedFields(
					"master",
					"username",
					"password",
					"sentinelPassword",
					"database",
					"sentinels")
					.build();
		}
	}

	@Nested // GH-2117
	class CreateRedisSocketConfiguration {

		@Test
		void minimalFieldsSetOnRedisURI() {
			RedisURI redisURI = RedisURI.builder()
					.socket("mysocket")
					.build();
			RedisSocketConfiguration expectedSocketConfiguration = new RedisSocketConfiguration();
			expectedSocketConfiguration.setSocket("mysocket");

			RedisConfiguration socketConfiguration = LettuceConnectionFactory.createRedisConfiguration(redisURI);

			assertThat(socketConfiguration).usingRecursiveComparison(socketCompareConfig())
					.isInstanceOf(RedisSocketConfiguration.class)
					.isEqualTo(expectedSocketConfiguration);
		}

		@Test
		void allFieldsSetOnRedisURI() {
			RedisURI redisURI = RedisURI.builder()
					.socket("mysocket")
					.withAuthentication("fooUser", "fooPass".toCharArray())
					.withDatabase(7)
					.build();
			RedisSocketConfiguration expectedSocketConfiguration = new RedisSocketConfiguration();
			expectedSocketConfiguration.setSocket("mysocket");
			expectedSocketConfiguration.setUsername("fooUser");
			expectedSocketConfiguration.setPassword("fooPass");
			expectedSocketConfiguration.setDatabase(7);

			RedisConfiguration socketConfiguration = LettuceConnectionFactory.createRedisConfiguration(redisURI);

			assertThat(socketConfiguration).usingRecursiveComparison(socketCompareConfig())
					.isInstanceOf(RedisSocketConfiguration.class)
					.isEqualTo(expectedSocketConfiguration);
		}

		// RedisSocketConfiguration does not provide equals impl
		private RecursiveComparisonConfiguration socketCompareConfig() {
			return RecursiveComparisonConfiguration.builder().withComparedFields(
					"socket",
					"username",
					"password",
					"database")
					.build();
		}
	}

	@Nested // GH-2117
	class CreateRedisStandaloneConfiguration {

		@Test
		void minimalFieldsSetOnRedisURI() {
			RedisURI redisURI = RedisURI.create("redis://myserver");
			RedisStandaloneConfiguration expectedStandaloneConfiguration = new RedisStandaloneConfiguration();
			expectedStandaloneConfiguration.setHostName("myserver");

			RedisConfiguration StandaloneConfiguration = LettuceConnectionFactory.createRedisConfiguration(redisURI);

			assertThat(StandaloneConfiguration).usingRecursiveComparison(standaloneCompareConfig())
					.isInstanceOf(RedisStandaloneConfiguration.class)
					.isEqualTo(expectedStandaloneConfiguration);
		}

		@Test
		void allFieldsSetOnRedisURI() {
			RedisURI redisURI = RedisURI.create("redis://fooUser:fooPass@myserver1:111/7");
			RedisStandaloneConfiguration expectedStandaloneConfiguration = new RedisStandaloneConfiguration();
			expectedStandaloneConfiguration.setHostName("myserver1");
			expectedStandaloneConfiguration.setPort(111);
			expectedStandaloneConfiguration.setDatabase(7);
			expectedStandaloneConfiguration.setUsername("fooUser");
			expectedStandaloneConfiguration.setPassword("fooPass");

			RedisConfiguration StandaloneConfiguration = LettuceConnectionFactory.createRedisConfiguration(redisURI);

			assertThat(StandaloneConfiguration).usingRecursiveComparison(standaloneCompareConfig())
					.isInstanceOf(RedisStandaloneConfiguration.class)
					.isEqualTo(expectedStandaloneConfiguration);

		}

		// RedisStandaloneConfiguration does not provide equals impl
		private RecursiveComparisonConfiguration standaloneCompareConfig() {
			return RecursiveComparisonConfiguration.builder().withComparedFields(
					"host",
					"port",
					"username",
					"password",
					"database")
					.build();
		}
	}

}
