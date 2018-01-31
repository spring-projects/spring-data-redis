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

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;
import sun.net.www.protocol.https.DefaultHostnameVerifier;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocketFactory;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.Test;
import org.springframework.data.redis.connection.RedisClusterConfiguration;
import org.springframework.data.redis.connection.RedisPassword;
import org.springframework.data.redis.connection.RedisSentinelConfiguration;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.test.util.ReflectionTestUtils;

/**
 * Unit tests for {@link JedisConnectionFactory}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public class JedisConnectionFactoryUnitTests {

	private JedisConnectionFactory connectionFactory;

	private static final RedisSentinelConfiguration SINGLE_SENTINEL_CONFIG = new RedisSentinelConfiguration()
			.master("mymaster").sentinel("127.0.0.1", 26379);

	private static final RedisClusterConfiguration CLUSTER_CONFIG = new RedisClusterConfiguration()
			.clusterNode("127.0.0.1", 6379).clusterNode("127.0.0.1", 6380);

	@Test // DATAREDIS-324
	public void shouldInitSentinelPoolWhenSentinelConfigPresent() {

		connectionFactory = initSpyedConnectionFactory(SINGLE_SENTINEL_CONFIG, new JedisPoolConfig());
		connectionFactory.afterPropertiesSet();

		verify(connectionFactory, times(1)).createRedisSentinelPool(eq(SINGLE_SENTINEL_CONFIG));
		verify(connectionFactory, never()).createRedisPool();
	}

	@Test // DATAREDIS-324
	public void shouldInitJedisPoolWhenNoSentinelConfigPresent() {

		connectionFactory = initSpyedConnectionFactory((RedisSentinelConfiguration) null, new JedisPoolConfig());
		connectionFactory.afterPropertiesSet();

		verify(connectionFactory, times(1)).createRedisPool();
		verify(connectionFactory, never()).createRedisSentinelPool(any(RedisSentinelConfiguration.class));
	}

	@Test(expected = IllegalStateException.class) // DATAREDIS-765
	public void shouldRejectPoolDisablingWhenSentinelConfigPresent() {

		connectionFactory = new JedisConnectionFactory(new RedisSentinelConfiguration());

		connectionFactory.setUsePool(false);
	}

	@Test // DATAREDIS-315
	public void shouldInitConnectionCorrectlyWhenClusterConfigPresent() {

		connectionFactory = initSpyedConnectionFactory(CLUSTER_CONFIG, new JedisPoolConfig());
		connectionFactory.afterPropertiesSet();

		verify(connectionFactory, times(1)).createCluster(eq(CLUSTER_CONFIG),
				any(GenericObjectPoolConfig.class));
		verify(connectionFactory, never()).createRedisPool();
	}

	@Test // DATAREDIS-315
	public void shouldCloseClusterCorrectlyOnFactoryDestruction() throws IOException {

		JedisCluster clusterMock = mock(JedisCluster.class);
		JedisConnectionFactory factory = new JedisConnectionFactory();
		ReflectionTestUtils.setField(factory, "cluster", clusterMock);

		factory.destroy();

		verify(clusterMock, times(1)).close();
	}

	@Test // DATAREDIS-574
	public void shouldReadStandalonePassword() {

		RedisStandaloneConfiguration envConfig = new RedisStandaloneConfiguration();
		envConfig.setPassword(RedisPassword.of("foo"));

		connectionFactory = new JedisConnectionFactory(envConfig, JedisClientConfiguration.defaultConfiguration());

		assertThat(connectionFactory.getPassword()).isEqualTo("foo");
	}

	@Test // DATAREDIS-574
	public void shouldWriteStandalonePassword() {

		RedisStandaloneConfiguration envConfig = new RedisStandaloneConfiguration();
		envConfig.setPassword(RedisPassword.of("foo"));

		connectionFactory = new JedisConnectionFactory(envConfig, JedisClientConfiguration.defaultConfiguration());
		connectionFactory.setPassword("bar");

		assertThat(connectionFactory.getPassword()).isEqualTo("bar");
		assertThat(envConfig.getPassword()).isEqualTo(RedisPassword.of("bar"));
	}

	@Test // DATAREDIS-574
	public void shouldReadSentinelPassword() {

		RedisSentinelConfiguration envConfig = new RedisSentinelConfiguration();
		envConfig.setPassword(RedisPassword.of("foo"));

		connectionFactory = new JedisConnectionFactory(envConfig, JedisClientConfiguration.defaultConfiguration());

		assertThat(connectionFactory.getPassword()).isEqualTo("foo");
	}

	@Test // DATAREDIS-574
	public void shouldWriteSentinelPassword() {

		RedisSentinelConfiguration envConfig = new RedisSentinelConfiguration();
		envConfig.setPassword(RedisPassword.of("foo"));

		connectionFactory = new JedisConnectionFactory(envConfig, JedisClientConfiguration.defaultConfiguration());
		connectionFactory.setPassword("bar");

		assertThat(connectionFactory.getPassword()).isEqualTo("bar");
		assertThat(envConfig.getPassword()).isEqualTo(RedisPassword.of("bar"));
	}

	@Test // DATAREDIS-574
	public void shouldReadClusterPassword() {

		RedisClusterConfiguration envConfig = new RedisClusterConfiguration();
		envConfig.setPassword(RedisPassword.of("foo"));

		connectionFactory = new JedisConnectionFactory(envConfig, JedisClientConfiguration.defaultConfiguration());

		assertThat(connectionFactory.getPassword()).isEqualTo("foo");
	}

	@Test // DATAREDIS-574
	public void shouldWriteClusterPassword() {

		RedisClusterConfiguration envConfig = new RedisClusterConfiguration();
		envConfig.setPassword(RedisPassword.of("foo"));

		connectionFactory = new JedisConnectionFactory(envConfig, JedisClientConfiguration.defaultConfiguration());
		connectionFactory.setPassword("bar");

		assertThat(connectionFactory.getPassword()).isEqualTo("bar");
		assertThat(envConfig.getPassword()).isEqualTo(RedisPassword.of("bar"));
	}

	@Test // DATAREDIS-574
	public void shouldReadStandaloneDatabaseIndex() {

		RedisStandaloneConfiguration envConfig = new RedisStandaloneConfiguration();
		envConfig.setDatabase(2);

		connectionFactory = new JedisConnectionFactory(envConfig, JedisClientConfiguration.defaultConfiguration());

		assertThat(connectionFactory.getDatabase()).isEqualTo(2);
	}

	@Test // DATAREDIS-574
	public void shouldWriteStandaloneDatabaseIndex() {

		RedisStandaloneConfiguration envConfig = new RedisStandaloneConfiguration();
		envConfig.setDatabase(2);

		connectionFactory = new JedisConnectionFactory(envConfig, JedisClientConfiguration.defaultConfiguration());
		connectionFactory.setDatabase(3);

		assertThat(connectionFactory.getDatabase()).isEqualTo(3);
		assertThat(envConfig.getDatabase()).isEqualTo(3);
	}

	@Test // DATAREDIS-574
	public void shouldReadSentinelDatabaseIndex() {

		RedisSentinelConfiguration envConfig = new RedisSentinelConfiguration();
		envConfig.setDatabase(2);

		connectionFactory = new JedisConnectionFactory(envConfig, JedisClientConfiguration.defaultConfiguration());

		assertThat(connectionFactory.getDatabase()).isEqualTo(2);
	}

	@Test // DATAREDIS-574
	public void shouldWriteSentinelDatabaseIndex() {

		RedisSentinelConfiguration envConfig = new RedisSentinelConfiguration();
		envConfig.setDatabase(2);

		connectionFactory = new JedisConnectionFactory(envConfig, JedisClientConfiguration.defaultConfiguration());
		connectionFactory.setDatabase(3);

		assertThat(connectionFactory.getDatabase()).isEqualTo(3);
		assertThat(envConfig.getDatabase()).isEqualTo(3);
	}

	@Test // DATAREDIS-574
	public void shouldApplyClientConfiguration() throws NoSuchAlgorithmException {

		SSLParameters sslParameters = new SSLParameters();
		SSLContext context = SSLContext.getDefault();
		SSLSocketFactory socketFactory = context.getSocketFactory();
		JedisPoolConfig poolConfig = new JedisPoolConfig();

		JedisClientConfiguration configuration = JedisClientConfiguration.builder().useSsl() //
				.hostnameVerifier(new DefaultHostnameVerifier()) //
				.sslParameters(sslParameters) //
				.sslSocketFactory(socketFactory).and() //
				.clientName("my-client") //
				.connectTimeout(Duration.of(10, ChronoUnit.MINUTES)) //
				.readTimeout(Duration.of(5, ChronoUnit.DAYS)) //
				.usePooling().poolConfig(poolConfig) //
				.build();

		connectionFactory = new JedisConnectionFactory(new RedisStandaloneConfiguration(), configuration);

		assertThat(connectionFactory.getClientConfiguration()).isSameAs(configuration);
		assertThat(connectionFactory.isUseSsl()).isTrue();
		assertThat(connectionFactory.getClientName()).isEqualTo("my-client");
		assertThat(connectionFactory.getTimeout()).isEqualTo((int) Duration.of(5, ChronoUnit.DAYS).toMillis());
		assertThat(connectionFactory.getUsePool()).isTrue();
		assertThat(connectionFactory.getPoolConfig()).isSameAs(poolConfig);
	}

	@Test // DATAREDIS-574
	public void shouldReturnStandaloneConfiguration() {

		RedisStandaloneConfiguration configuration = new RedisStandaloneConfiguration();
		connectionFactory = new JedisConnectionFactory(configuration, JedisClientConfiguration.defaultConfiguration());

		assertThat(connectionFactory.getStandaloneConfiguration()).isSameAs(configuration);
		assertThat(connectionFactory.getSentinelConfiguration()).isNull();
		assertThat(connectionFactory.getClusterConfiguration()).isNull();
	}

	@Test // DATAREDIS-574
	public void shouldReturnSentinelConfiguration() {

		RedisSentinelConfiguration configuration = new RedisSentinelConfiguration();
		connectionFactory = new JedisConnectionFactory(configuration, JedisClientConfiguration.defaultConfiguration());

		assertThat(connectionFactory.getStandaloneConfiguration()).isNotNull();
		assertThat(connectionFactory.getSentinelConfiguration()).isSameAs(configuration);
		assertThat(connectionFactory.getClusterConfiguration()).isNull();
	}

	@Test // DATAREDIS-574
	public void shouldReturnClusterConfiguration() {

		RedisClusterConfiguration configuration = new RedisClusterConfiguration();
		connectionFactory = new JedisConnectionFactory(configuration, JedisClientConfiguration.defaultConfiguration());

		assertThat(connectionFactory.getStandaloneConfiguration()).isNotNull();
		assertThat(connectionFactory.getSentinelConfiguration()).isNull();
		assertThat(connectionFactory.getClusterConfiguration()).isSameAs(configuration);
	}

	@Test(expected = IllegalStateException.class) // DATAREDIS-574
	public void shouldDenyChangesToImmutableClientConfiguration() throws NoSuchAlgorithmException {

		connectionFactory = new JedisConnectionFactory(new RedisStandaloneConfiguration(),
				JedisClientConfiguration.defaultConfiguration());

		connectionFactory.setClientName("foo");
	}

	private JedisConnectionFactory initSpyedConnectionFactory(RedisSentinelConfiguration sentinelConfig,
			JedisPoolConfig poolConfig) {

		// we have to use a spy here as jedis would start connecting to redis sentinels when the pool is created.
		JedisConnectionFactory factorySpy = spy(new JedisConnectionFactory(sentinelConfig, poolConfig));
		doReturn(null).when(factorySpy).createRedisSentinelPool(any(RedisSentinelConfiguration.class));
		doReturn(null).when(factorySpy).createRedisPool();
		return factorySpy;
	}

	private JedisConnectionFactory initSpyedConnectionFactory(RedisClusterConfiguration clusterConfig,
			JedisPoolConfig poolConfig) {

		JedisConnectionFactory factorySpy = spy(new JedisConnectionFactory(clusterConfig));
		doReturn(null).when(factorySpy).createCluster(any(RedisClusterConfiguration.class),
				any(GenericObjectPoolConfig.class));
		doReturn(null).when(factorySpy).createRedisPool();
		return factorySpy;
	}
}
