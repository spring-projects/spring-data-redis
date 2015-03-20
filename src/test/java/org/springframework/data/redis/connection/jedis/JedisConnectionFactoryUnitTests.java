/*
 * Copyright 2014-2015 the original author or authors.
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

import static org.mockito.Mockito.*;

import java.io.IOException;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.Test;
import org.mockito.Matchers;
import org.springframework.data.redis.connection.RedisClusterConfiguration;
import org.springframework.data.redis.connection.RedisSentinelConfiguration;
import org.springframework.test.util.ReflectionTestUtils;

import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;

/**
 * @author Christoph Strobl
 */
public class JedisConnectionFactoryUnitTests {

	private JedisConnectionFactory connectionFactory;

	private static final RedisSentinelConfiguration SINGLE_SENTINEL_CONFIG = new RedisSentinelConfiguration().master(
			"mymaster").sentinel("127.0.0.1", 26379);

	private static final RedisClusterConfiguration CLUSTER_CONFIG = new RedisClusterConfiguration().clusterNode(
			"127.0.0.1", 6379).clusterNode("127.0.0.1", 6380);

	/**
	 * @see DATAREDIS-324
	 */
	@Test
	public void shouldInitSentinelPoolWhenSentinelConfigPresent() {

		connectionFactory = initSpyedConnectionFactory(SINGLE_SENTINEL_CONFIG, new JedisPoolConfig());
		connectionFactory.afterPropertiesSet();

		verify(connectionFactory, times(1)).createRedisSentinelPool(Matchers.eq(SINGLE_SENTINEL_CONFIG));
		verify(connectionFactory, never()).createRedisPool();
	}

	/**
	 * @see DATAREDIS-324
	 */
	@Test
	public void shouldInitJedisPoolWhenNoSentinelConfigPresent() {

		connectionFactory = initSpyedConnectionFactory((RedisSentinelConfiguration) null, new JedisPoolConfig());
		connectionFactory.afterPropertiesSet();

		verify(connectionFactory, times(1)).createRedisPool();
		verify(connectionFactory, never()).createRedisSentinelPool(Matchers.any(RedisSentinelConfiguration.class));
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void shouldInitConnectionCorrectlyWhenClusterConfigPresent() {

		connectionFactory = initSpyedConnectionFactory(CLUSTER_CONFIG, new JedisPoolConfig());
		connectionFactory.afterPropertiesSet();

		verify(connectionFactory, times(1)).createCluster(Matchers.eq(CLUSTER_CONFIG),
				Matchers.any(GenericObjectPoolConfig.class));
		verify(connectionFactory, never()).createRedisPool();
	}

	/**
	 * @throws IOException
	 * @see DATAREDIS-315
	 */
	@Test
	public void shouldClostClusterCorrectlyOnFactoryDestruction() throws IOException {

		JedisCluster clusterMock = mock(JedisCluster.class);
		JedisConnectionFactory factory = new JedisConnectionFactory();
		ReflectionTestUtils.setField(factory, "cluster", clusterMock);

		factory.destroy();

		verify(clusterMock, times(1)).close();
	}

	private JedisConnectionFactory initSpyedConnectionFactory(RedisSentinelConfiguration sentinelConfig,
			JedisPoolConfig poolConfig) {

		// we have to use a spy here as jedis would start connecting to redis sentinels when the pool is created.
		JedisConnectionFactory factorySpy = spy(new JedisConnectionFactory(sentinelConfig, poolConfig));
		doReturn(null).when(factorySpy).createRedisSentinelPool(Matchers.any(RedisSentinelConfiguration.class));
		doReturn(null).when(factorySpy).createRedisPool();
		return factorySpy;
	}

	private JedisConnectionFactory initSpyedConnectionFactory(RedisClusterConfiguration clusterConfig,
			JedisPoolConfig poolConfig) {

		JedisConnectionFactory factorySpy = spy(new JedisConnectionFactory(clusterConfig));
		doReturn(null).when(factorySpy).createCluster(Matchers.any(RedisClusterConfiguration.class),
				Matchers.any(GenericObjectPoolConfig.class));
		doReturn(null).when(factorySpy).createRedisPool();
		return factorySpy;
	}
}
