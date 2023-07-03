/*
 * Copyright 2016-2023 the original author or authors.
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
package org.springframework.data.redis.repository;

import static org.springframework.data.redis.connection.ClusterTestVariables.*;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.extension.ExtendWith;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.data.redis.connection.RedisClusterConfiguration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.repository.configuration.EnableRedisRepositories;
import org.springframework.data.redis.test.condition.EnabledOnRedisClusterAvailable;
import org.springframework.lang.NonNullApi;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@ExtendWith(SpringExtension.class)
@ContextConfiguration
@EnabledOnRedisClusterAvailable
class RedisRepositoryClusterIntegrationTests extends RedisRepositoryIntegrationTestBase {

	static final List<String> CLUSTER_NODES = Arrays.asList(CLUSTER_NODE_1.asString(), CLUSTER_NODE_2.asString(),
			CLUSTER_NODE_3.asString());

	@Configuration
	@EnableRedisRepositories(considerNestedRepositories = true, indexConfiguration = MyIndexConfiguration.class,
			keyspaceConfiguration = MyKeyspaceConfiguration.class,
			includeFilters = { @ComponentScan.Filter(type = FilterType.ASSIGNABLE_TYPE,
					classes = { PersonRepository.class, CityRepository.class, ImmutableObjectRepository.class }) })
	static class Config {

		@Bean
		RedisTemplate<?, ?> redisTemplate(RedisConnectionFactory connectionFactory) {

			RedisTemplate<byte[], byte[]> template = new RedisTemplate<>();
			template.setConnectionFactory(connectionFactory);

			return template;
		}

		@Bean
		RedisConnectionFactory connectionFactory() {
			RedisClusterConfiguration clusterConfig = new RedisClusterConfiguration(CLUSTER_NODES);
			JedisConnectionFactory connectionFactory = new JedisConnectionFactory(clusterConfig);
			return connectionFactory;
		}
	}
}
