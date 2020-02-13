/*
 * Copyright 2015-2020 the original author or authors.
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
import static org.mockito.Mockito.*;
import static org.springframework.data.redis.connection.ClusterTestVariables.*;
import static org.springframework.data.redis.connection.RedisConfiguration.*;
import static org.springframework.data.redis.connection.lettuce.LettuceTestClientResources.*;
import static org.springframework.test.util.ReflectionTestUtils.*;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.resource.ClientResources;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.springframework.beans.DirectFieldAccessor;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.data.redis.ConnectionFactoryTracker;
import org.springframework.data.redis.connection.RedisClusterConfiguration;
import org.springframework.data.redis.connection.RedisClusterConnection;
import org.springframework.data.redis.connection.RedisConfiguration;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisPassword;
import org.springframework.data.redis.connection.RedisSentinelConfiguration;
import org.springframework.data.redis.connection.RedisSocketConfiguration;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.test.util.ReflectionTestUtils;

/**
 * Unit tests for {@link LettuceConnectionFactory}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Balázs Németh
 * @author Ruben Cervilla
 * @author Luis De Bello
 */
public class LettuceConnectionFactoryUnitTests {

	RedisClusterConfiguration clusterConfig;

	@Before
	public void setUp() {
		clusterConfig = new RedisClusterConfiguration().clusterNode("127.0.0.1", 6379).clusterNode("127.0.0.1", 6380);
	}

	@After
	public void tearDown() {
		ConnectionFactoryTracker.cleanUp();
	}

	@Test // DATAREDIS-315
	public void shouldInitClientCorrectlyWhenClusterConfigPresent() {

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(clusterConfig);
		connectionFactory.setClientResources(getSharedClientResources());
		connectionFactory.afterPropertiesSet();
		ConnectionFactoryTracker.add(connectionFactory);

		assertThat(getField(connectionFactory, "client")).isInstanceOf(RedisClusterClient.class);
	}

	@Test // DATAREDIS-315
	@SuppressWarnings("unchecked")
	public void timeoutShouldBeSetCorrectlyOnClusterClient() {

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(clusterConfig);
		connectionFactory.setClientResources(getSharedClientResources());
		connectionFactory.setTimeout(1000);
		connectionFactory.afterPropertiesSet();
		ConnectionFactoryTracker.add(connectionFactory);

		AbstractRedisClient client = (AbstractRedisClient) getField(connectionFactory, "client");
		assertThat(client).isInstanceOf(RedisClusterClient.class);

		Iterable<RedisURI> initialUris = (Iterable<RedisURI>) getField(client, "initialUris");

		for (RedisURI uri : initialUris) {
			assertThat(uri.getTimeout()).isEqualTo(Duration.ofMillis(connectionFactory.getTimeout()));
		}
	}

	@Test // DATAREDIS-930
	public void portShouldBeReturnedProperlyBasedOnConfiguration() {

		RedisConfiguration redisConfiguration = new RedisStandaloneConfiguration("localhost", 16379);

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(redisConfiguration,
				LettuceClientConfiguration.defaultConfiguration());

		assertThat(connectionFactory.getPort()).isEqualTo(16379);
	}

	@Test // DATAREDIS-930
	public void portShouldBeReturnedProperlyBasedOnCustomRedisConfiguration() {

		RedisConfiguration redisConfiguration = new CustomRedisConfiguration("localhost", 16379);

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(redisConfiguration,
				LettuceClientConfiguration.defaultConfiguration());

		assertThat(connectionFactory.getPort()).isEqualTo(16379);
		assertThat(connectionFactory.getHostName()).isEqualTo("localhost");
	}

	@Test // DATAREDIS-930
	public void hostNameShouldBeReturnedProperlyBasedOnConfiguration() {

		RedisConfiguration redisConfiguration = new RedisStandaloneConfiguration("external");

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(redisConfiguration,
				LettuceClientConfiguration.defaultConfiguration());

		assertThat(connectionFactory.getHostName()).isEqualTo("external");
	}

	@Test // DATAREDIS-930
	public void hostNameShouldBeReturnedProperlyBasedOnCustomRedisConfiguration() {

		RedisConfiguration redisConfiguration = new CustomRedisConfiguration("external");

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(redisConfiguration,
				LettuceClientConfiguration.defaultConfiguration());

		assertThat(connectionFactory.getPort()).isEqualTo(6379);
		assertThat(connectionFactory.getHostName()).isEqualTo("external");
	}

	@Test // DATAREDIS-315
	@SuppressWarnings("unchecked")
	public void passwordShouldBeSetCorrectlyOnClusterClient() {

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(clusterConfig);
		connectionFactory.setClientResources(getSharedClientResources());
		connectionFactory.setPassword("o_O");
		connectionFactory.afterPropertiesSet();
		ConnectionFactoryTracker.add(connectionFactory);

		AbstractRedisClient client = (AbstractRedisClient) getField(connectionFactory, "client");
		assertThat(client).isInstanceOf(RedisClusterClient.class);

		Iterable<RedisURI> initialUris = (Iterable<RedisURI>) getField(client, "initialUris");

		for (RedisURI uri : initialUris) {
			assertThat(uri.getPassword()).isEqualTo(connectionFactory.getPassword().toCharArray());
		}
	}

	@Test // DATAREDIS-524, DATAREDIS-1045, DATAREDIS-1060
	public void passwordShouldNotBeSetOnSentinelClient() {

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(
				new RedisSentinelConfiguration("mymaster", Collections.singleton("host:1234")));
		connectionFactory.setClientResources(getSharedClientResources());
		connectionFactory.setPassword("o_O");
		connectionFactory.afterPropertiesSet();
		ConnectionFactoryTracker.add(connectionFactory);

		AbstractRedisClient client = (AbstractRedisClient) getField(connectionFactory, "client");
		assertThat(client).isInstanceOf(RedisClient.class);

		RedisURI redisUri = (RedisURI) getField(client, "redisURI");

		assertThat(redisUri.getPassword()).isEqualTo(connectionFactory.getPassword().toCharArray());

		for (RedisURI sentinel : redisUri.getSentinels()) {
			assertThat(sentinel.getPassword()).isNull();
		}
	}

	@Test // DATAREDIS-1060
	public void sentinelPasswordShouldBeSetOnSentinelClient() {

		RedisSentinelConfiguration config = new RedisSentinelConfiguration("mymaster", Collections.singleton("host:1234"));
		config.setSentinelPassword("sentinel-pwd");

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(config);
		connectionFactory.setClientResources(getSharedClientResources());
		connectionFactory.setPassword("o_O");
		connectionFactory.afterPropertiesSet();
		ConnectionFactoryTracker.add(connectionFactory);

		AbstractRedisClient client = (AbstractRedisClient) getField(connectionFactory, "client");
		assertThat(client).isInstanceOf(RedisClient.class);

		RedisURI redisUri = (RedisURI) getField(client, "redisURI");

		assertThat(redisUri.getPassword()).isEqualTo(connectionFactory.getPassword().toCharArray());

		for (RedisURI sentinel : redisUri.getSentinels()) {
			assertThat(sentinel.getPassword()).isEqualTo("sentinel-pwd".toCharArray());
		}
	}

	@Test // DATAREDIS-1060
	public void sentinelPasswordShouldNotLeakIntoDataNodeClient() {

		RedisSentinelConfiguration config = new RedisSentinelConfiguration("mymaster", Collections.singleton("host:1234"));
		config.setSentinelPassword("sentinel-pwd");

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(config);
		connectionFactory.setClientResources(getSharedClientResources());
		connectionFactory.afterPropertiesSet();
		ConnectionFactoryTracker.add(connectionFactory);

		AbstractRedisClient client = (AbstractRedisClient) getField(connectionFactory, "client");
		assertThat(client).isInstanceOf(RedisClient.class);

		RedisURI redisUri = (RedisURI) getField(client, "redisURI");

		assertThat(redisUri.getPassword()).isNull();

		for (RedisURI sentinel : redisUri.getSentinels()) {
			assertThat(sentinel.getPassword()).isEqualTo("sentinel-pwd".toCharArray());
		}
	}

	@Test // DATAREDIS-462
	public void clusterClientShouldInitializeWithoutClientResources() {

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(clusterConfig);
		connectionFactory.setShutdownTimeout(0);
		connectionFactory.afterPropertiesSet();
		ConnectionFactoryTracker.add(connectionFactory);

		AbstractRedisClient client = (AbstractRedisClient) getField(connectionFactory, "client");
		assertThat(client).isInstanceOf(RedisClusterClient.class);
	}

	@Test // DATAREDIS-480
	public void sslOptionsShouldBeDisabledByDefaultOnClient() {

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory();
		connectionFactory.setClientResources(getSharedClientResources());
		connectionFactory.afterPropertiesSet();
		ConnectionFactoryTracker.add(connectionFactory);

		AbstractRedisClient client = (AbstractRedisClient) getField(connectionFactory, "client");
		assertThat(client).isInstanceOf(RedisClient.class);

		RedisURI redisUri = (RedisURI) getField(client, "redisURI");

		assertThat(redisUri.isSsl()).isFalse();
		assertThat(connectionFactory.isUseSsl()).isFalse();
		assertThat(redisUri.isStartTls()).isFalse();
		assertThat(connectionFactory.isStartTls()).isFalse();
		assertThat(redisUri.isVerifyPeer()).isTrue();
		assertThat(connectionFactory.isVerifyPeer()).isTrue();
	}

	@Test // DATAREDIS-476
	public void sslShouldBeSetCorrectlyOnClient() {

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory();
		connectionFactory.setClientResources(getSharedClientResources());
		connectionFactory.setUseSsl(true);
		connectionFactory.afterPropertiesSet();
		ConnectionFactoryTracker.add(connectionFactory);

		AbstractRedisClient client = (AbstractRedisClient) getField(connectionFactory, "client");
		assertThat(client).isInstanceOf(RedisClient.class);

		RedisURI redisUri = (RedisURI) getField(client, "redisURI");

		assertThat(redisUri.isSsl()).isTrue();
		assertThat(connectionFactory.isUseSsl()).isTrue();
		assertThat(redisUri.isVerifyPeer()).isTrue();
		assertThat(connectionFactory.isVerifyPeer()).isTrue();
	}

	@Test // DATAREDIS-480
	public void verifyPeerOptionShouldBeSetCorrectlyOnClient() {

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory();
		connectionFactory.setClientResources(getSharedClientResources());
		connectionFactory.setVerifyPeer(false);
		connectionFactory.afterPropertiesSet();
		ConnectionFactoryTracker.add(connectionFactory);

		AbstractRedisClient client = (AbstractRedisClient) getField(connectionFactory, "client");
		assertThat(client).isInstanceOf(RedisClient.class);

		RedisURI redisUri = (RedisURI) getField(client, "redisURI");

		assertThat(redisUri.isVerifyPeer()).isFalse();
		assertThat(connectionFactory.isVerifyPeer()).isFalse();
	}

	@Test // DATAREDIS-480
	public void startTLSOptionShouldBeSetCorrectlyOnClient() {

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory();
		connectionFactory.setClientResources(getSharedClientResources());
		connectionFactory.setStartTls(true);
		connectionFactory.afterPropertiesSet();
		ConnectionFactoryTracker.add(connectionFactory);

		AbstractRedisClient client = (AbstractRedisClient) getField(connectionFactory, "client");
		assertThat(client).isInstanceOf(RedisClient.class);

		RedisURI redisUri = (RedisURI) getField(client, "redisURI");

		assertThat(redisUri.isStartTls()).isTrue();
		assertThat(connectionFactory.isStartTls()).isTrue();
	}

	@Test // DATAREDIS-990
	public void sslShouldBeSetCorrectlyOnSentinelClient() {

		RedisSentinelConfiguration sentinelConfiguration = new RedisSentinelConfiguration("myMaster",
				Collections.singleton("localhost:1234"));
		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(sentinelConfiguration);
		connectionFactory.setClientResources(getSharedClientResources());
		connectionFactory.setUseSsl(true);
		connectionFactory.afterPropertiesSet();
		ConnectionFactoryTracker.add(connectionFactory);

		AbstractRedisClient client = (AbstractRedisClient) getField(connectionFactory, "client");
		assertThat(client).isInstanceOf(RedisClient.class);

		RedisURI redisUri = (RedisURI) getField(client, "redisURI");

		assertThat(redisUri.isSsl()).isTrue();
		assertThat(connectionFactory.isUseSsl()).isTrue();
		assertThat(redisUri.isVerifyPeer()).isTrue();
		assertThat(connectionFactory.isVerifyPeer()).isTrue();
	}

	@Test // DATAREDIS-990
	public void verifyPeerOptionShouldBeSetCorrectlyOnSentinelClient() {

		RedisSentinelConfiguration sentinelConfiguration = new RedisSentinelConfiguration("myMaster",
				Collections.singleton("localhost:1234"));
		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(sentinelConfiguration);
		connectionFactory.setClientResources(getSharedClientResources());
		connectionFactory.setVerifyPeer(false);
		connectionFactory.afterPropertiesSet();
		ConnectionFactoryTracker.add(connectionFactory);

		AbstractRedisClient client = (AbstractRedisClient) getField(connectionFactory, "client");
		assertThat(client).isInstanceOf(RedisClient.class);

		RedisURI redisUri = (RedisURI) getField(client, "redisURI");

		assertThat(redisUri.isVerifyPeer()).isFalse();
		assertThat(connectionFactory.isVerifyPeer()).isFalse();
	}

	@Test // DATAREDIS-990
	public void startTLSOptionShouldBeSetCorrectlyOnSentinelClient() {

		RedisSentinelConfiguration sentinelConfiguration = new RedisSentinelConfiguration("myMaster",
				Collections.singleton("localhost:1234"));
		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(sentinelConfiguration);
		connectionFactory.setClientResources(getSharedClientResources());
		connectionFactory.setStartTls(true);
		connectionFactory.afterPropertiesSet();
		ConnectionFactoryTracker.add(connectionFactory);

		AbstractRedisClient client = (AbstractRedisClient) getField(connectionFactory, "client");
		assertThat(client).isInstanceOf(RedisClient.class);

		RedisURI redisUri = (RedisURI) getField(client, "redisURI");

		assertThat(redisUri.isStartTls()).isTrue();
		assertThat(connectionFactory.isStartTls()).isTrue();
	}

	@Test // DATAREDIS-537
	public void sslShouldBeSetCorrectlyOnClusterClient() {

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(
				new RedisClusterConfiguration().clusterNode(CLUSTER_NODE_1));
		connectionFactory.setClientResources(getSharedClientResources());
		connectionFactory.setUseSsl(true);
		connectionFactory.afterPropertiesSet();
		ConnectionFactoryTracker.add(connectionFactory);

		AbstractRedisClient client = (AbstractRedisClient) getField(connectionFactory, "client");
		assertThat(client).isInstanceOf(RedisClusterClient.class);

		Iterable<RedisURI> initialUris = (Iterable<RedisURI>) getField(client, "initialUris");

		for (RedisURI uri : initialUris) {
			assertThat(uri.isSsl()).isTrue();
		}
	}

	@Test // DATAREDIS-537
	public void startTLSOptionShouldBeSetCorrectlyOnClusterClient() {

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(
				new RedisClusterConfiguration().clusterNode(CLUSTER_NODE_1));
		connectionFactory.setClientResources(getSharedClientResources());
		connectionFactory.setStartTls(true);
		connectionFactory.afterPropertiesSet();
		ConnectionFactoryTracker.add(connectionFactory);

		AbstractRedisClient client = (AbstractRedisClient) getField(connectionFactory, "client");
		assertThat(client).isInstanceOf(RedisClusterClient.class);

		Iterable<RedisURI> initialUris = (Iterable<RedisURI>) getField(client, "initialUris");

		for (RedisURI uri : initialUris) {
			assertThat(uri.isStartTls()).isTrue();
		}
	}

	@Test // DATAREDIS-537
	public void verifyPeerTLSOptionShouldBeSetCorrectlyOnClusterClient() {

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(
				new RedisClusterConfiguration().clusterNode(CLUSTER_NODE_1));
		connectionFactory.setClientResources(getSharedClientResources());
		connectionFactory.setVerifyPeer(true);
		connectionFactory.afterPropertiesSet();
		ConnectionFactoryTracker.add(connectionFactory);

		AbstractRedisClient client = (AbstractRedisClient) getField(connectionFactory, "client");
		assertThat(client).isInstanceOf(RedisClusterClient.class);

		Iterable<RedisURI> initialUris = (Iterable<RedisURI>) getField(client, "initialUris");

		for (RedisURI uri : initialUris) {
			assertThat(uri.isVerifyPeer()).isTrue();
		}
	}

	@Test // DATAREDIS-682
	public void socketShouldBeSetOnStandaloneClient() {

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(new RedisSocketConfiguration());
		connectionFactory.afterPropertiesSet();
		ConnectionFactoryTracker.add(connectionFactory);

		AbstractRedisClient client = (AbstractRedisClient) getField(connectionFactory, "client");
		assertThat(client).isInstanceOf(RedisClient.class);

		RedisURI redisUri = (RedisURI) getField(client, "redisURI");

		assertThat(redisUri.getSocket()).isEqualTo("/tmp/redis.sock");
	}

	@Test // DATAREDIS-574
	public void shouldReadStandalonePassword() {

		RedisStandaloneConfiguration envConfig = new RedisStandaloneConfiguration();
		envConfig.setPassword(RedisPassword.of("foo"));

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(envConfig,
				LettuceClientConfiguration.defaultConfiguration());

		assertThat(connectionFactory.getPassword()).isEqualTo("foo");
	}

	@Test // DATAREDIS-574
	public void shouldWriteStandalonePassword() {

		RedisStandaloneConfiguration envConfig = new RedisStandaloneConfiguration();
		envConfig.setPassword(RedisPassword.of("foo"));

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(envConfig,
				LettuceClientConfiguration.defaultConfiguration());
		connectionFactory.setPassword("bar");

		assertThat(connectionFactory.getPassword()).isEqualTo("bar");
		assertThat(envConfig.getPassword()).isEqualTo(RedisPassword.of("bar"));
	}

	@Test // DATAREDIS-574
	public void shouldReadSentinelPassword() {

		RedisSentinelConfiguration envConfig = new RedisSentinelConfiguration();
		envConfig.setPassword(RedisPassword.of("foo"));

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(envConfig,
				LettuceClientConfiguration.defaultConfiguration());

		assertThat(connectionFactory.getPassword()).isEqualTo("foo");
	}

	@Test // DATAREDIS-574
	public void shouldWriteSentinelPassword() {

		RedisSentinelConfiguration envConfig = new RedisSentinelConfiguration();
		envConfig.setPassword(RedisPassword.of("foo"));

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(envConfig,
				LettuceClientConfiguration.defaultConfiguration());
		connectionFactory.setPassword("bar");

		assertThat(connectionFactory.getPassword()).isEqualTo("bar");
		assertThat(envConfig.getPassword()).isEqualTo(RedisPassword.of("bar"));
	}

	@Test // DATAREDIS-682
	public void shouldWriteSocketPassword() {

		RedisSocketConfiguration envConfig = new RedisSocketConfiguration();
		envConfig.setPassword(RedisPassword.of("foo"));

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(envConfig,
				LettuceClientConfiguration.defaultConfiguration());
		connectionFactory.setPassword("bar");

		assertThat(connectionFactory.getPassword()).isEqualTo("bar");
		assertThat(envConfig.getPassword()).isEqualTo(RedisPassword.of("bar"));
	}

	@Test // DATAREDIS-574
	public void shouldReadClusterPassword() {

		RedisClusterConfiguration envConfig = new RedisClusterConfiguration();
		envConfig.setPassword(RedisPassword.of("foo"));

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(envConfig,
				LettuceClientConfiguration.defaultConfiguration());

		assertThat(connectionFactory.getPassword()).isEqualTo("foo");
	}

	@Test // DATAREDIS-574
	public void shouldWriteClusterPassword() {

		RedisClusterConfiguration envConfig = new RedisClusterConfiguration();
		envConfig.setPassword(RedisPassword.of("foo"));

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(envConfig,
				LettuceClientConfiguration.defaultConfiguration());
		connectionFactory.setPassword("bar");

		assertThat(connectionFactory.getPassword()).isEqualTo("bar");
		assertThat(envConfig.getPassword()).isEqualTo(RedisPassword.of("bar"));
	}

	@Test // DATAREDIS-574
	public void shouldReadStandaloneDatabaseIndex() {

		RedisStandaloneConfiguration envConfig = new RedisStandaloneConfiguration();
		envConfig.setDatabase(2);

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(envConfig,
				LettuceClientConfiguration.defaultConfiguration());

		assertThat(connectionFactory.getDatabase()).isEqualTo(2);
	}

	@Test // DATAREDIS-574
	public void shouldWriteStandaloneDatabaseIndex() {

		RedisStandaloneConfiguration envConfig = new RedisStandaloneConfiguration();
		envConfig.setDatabase(2);

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(envConfig,
				LettuceClientConfiguration.defaultConfiguration());
		connectionFactory.setDatabase(3);

		assertThat(connectionFactory.getDatabase()).isEqualTo(3);
		assertThat(envConfig.getDatabase()).isEqualTo(3);
	}

	@Test // DATAREDIS-574
	public void shouldReadSentinelDatabaseIndex() {

		RedisSentinelConfiguration envConfig = new RedisSentinelConfiguration();
		envConfig.setDatabase(2);

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(envConfig,
				LettuceClientConfiguration.defaultConfiguration());

		assertThat(connectionFactory.getDatabase()).isEqualTo(2);
	}

	@Test // DATAREDIS-574
	public void shouldWriteSentinelDatabaseIndex() {

		RedisSentinelConfiguration envConfig = new RedisSentinelConfiguration();
		envConfig.setDatabase(2);

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(envConfig,
				LettuceClientConfiguration.defaultConfiguration());
		connectionFactory.setDatabase(3);

		assertThat(connectionFactory.getDatabase()).isEqualTo(3);
		assertThat(envConfig.getDatabase()).isEqualTo(3);
	}

	@Test // DATAREDIS-682
	public void shouldWriteSocketDatabaseIndex() {

		RedisSocketConfiguration envConfig = new RedisSocketConfiguration();
		envConfig.setDatabase(2);

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(envConfig,
				LettuceClientConfiguration.defaultConfiguration());
		connectionFactory.setDatabase(3);

		assertThat(connectionFactory.getDatabase()).isEqualTo(3);
		assertThat(envConfig.getDatabase()).isEqualTo(3);
	}

	@Test // DATAREDIS-574
	public void shouldApplyClientConfiguration() {

		ClientOptions clientOptions = ClientOptions.create();
		ClientResources sharedClientResources = LettuceTestClientResources.getSharedClientResources();

		LettuceClientConfiguration configuration = LettuceClientConfiguration.builder() //
				.useSsl() //
				.disablePeerVerification() //
				.startTls().and() //
				.clientOptions(clientOptions) //
				.clientResources(sharedClientResources) //
				.commandTimeout(Duration.ofMinutes(5)) //
				.shutdownTimeout(Duration.ofHours(2)) //
				.build();

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(new RedisStandaloneConfiguration(),
				configuration);

		assertThat(connectionFactory.getClientConfiguration()).isEqualTo(configuration);

		assertThat(connectionFactory.isUseSsl()).isTrue();
		assertThat(connectionFactory.isVerifyPeer()).isFalse();
		assertThat(connectionFactory.isStartTls()).isTrue();
		assertThat(connectionFactory.getClientResources()).isEqualTo(sharedClientResources);
		assertThat(connectionFactory.getTimeout()).isEqualTo(Duration.ofMinutes(5).toMillis());
		assertThat(connectionFactory.getShutdownTimeout()).isEqualTo(Duration.ofHours(2).toMillis());
	}

	@Test // DATAREDIS-574
	public void shouldReturnStandaloneConfiguration() {

		RedisStandaloneConfiguration configuration = new RedisStandaloneConfiguration();
		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(configuration,
				LettuceClientConfiguration.defaultConfiguration());

		assertThat(connectionFactory.getStandaloneConfiguration()).isEqualTo(configuration);
		assertThat(connectionFactory.getSentinelConfiguration()).isNull();
		assertThat(connectionFactory.getClusterConfiguration()).isNull();
	}

	@Test // DATAREDIS-682
	public void shouldReturnSocketConfiguration() {

		RedisSocketConfiguration configuration = new RedisSocketConfiguration("/var/redis/socket");
		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(configuration,
				LettuceClientConfiguration.defaultConfiguration());

		assertThat(connectionFactory.getStandaloneConfiguration()).isNotNull();
		assertThat(connectionFactory.getSocketConfiguration()).isEqualTo(configuration);
		assertThat(connectionFactory.getSentinelConfiguration()).isNull();
		assertThat(connectionFactory.getClusterConfiguration()).isNull();
	}

	@Test // DATAREDIS-574
	public void shouldReturnSentinelConfiguration() {

		RedisSentinelConfiguration configuration = new RedisSentinelConfiguration();
		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(configuration,
				LettuceClientConfiguration.defaultConfiguration());

		assertThat(connectionFactory.getStandaloneConfiguration()).isNotNull();
		assertThat(connectionFactory.getSentinelConfiguration()).isEqualTo(configuration);
		assertThat(connectionFactory.getClusterConfiguration()).isNull();
	}

	@Test // DATAREDIS-574
	public void shouldReturnClusterConfiguration() {

		RedisClusterConfiguration configuration = new RedisClusterConfiguration();
		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(configuration,
				LettuceClientConfiguration.defaultConfiguration());

		assertThat(connectionFactory.getStandaloneConfiguration()).isNotNull();
		assertThat(connectionFactory.getSentinelConfiguration()).isNull();
		assertThat(connectionFactory.getClusterConfiguration()).isEqualTo(configuration);
	}

	@Test // DATAREDIS-574
	public void shouldDenyChangesToImmutableClientConfiguration() {

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(new RedisStandaloneConfiguration(),
				LettuceClientConfiguration.defaultConfiguration());

		assertThatIllegalStateException().isThrownBy(() -> connectionFactory.setUseSsl(false));
	}

	@Test // DATAREDIS-676
	public void timeoutShouldBePassedOnToClusterConnection() {

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(clusterConfig);
		connectionFactory.setShutdownTimeout(0);
		connectionFactory.setTimeout(2000);
		connectionFactory.setShareNativeConnection(false);
		connectionFactory.afterPropertiesSet();
		ConnectionFactoryTracker.add(connectionFactory);

		RedisClusterConnection clusterConnection = connectionFactory.getClusterConnection();
		assertThat(ReflectionTestUtils.getField(clusterConnection, "timeout")).isEqualTo(2000L);

		clusterConnection.close();
	}

	@Test // DATAREDIS-676
	public void timeoutSetOnClientConfigShouldBePassedOnToClusterConnection() {

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(clusterConfig, LettuceClientConfiguration
				.builder().commandTimeout(Duration.ofSeconds(2)).shutdownTimeout(Duration.ZERO).build());
		connectionFactory.setShareNativeConnection(false);

		connectionFactory.afterPropertiesSet();
		ConnectionFactoryTracker.add(connectionFactory);

		RedisClusterConnection clusterConnection = connectionFactory.getClusterConnection();
		assertThat(ReflectionTestUtils.getField(clusterConnection, "timeout")).isEqualTo(2000L);

		clusterConnection.close();
	}

	@Test // DATAREDIS-731
	public void shouldShareNativeConnectionWithCluster() {

		RedisClusterClient clientMock = mock(RedisClusterClient.class);
		StatefulRedisClusterConnection<byte[], byte[]> connectionMock = mock(StatefulRedisClusterConnection.class);
		when(clientMock.connectAsync(ByteArrayCodec.INSTANCE))
				.thenReturn(CompletableFuture.completedFuture(connectionMock));

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(clusterConfig,
				LettuceClientConfiguration.defaultConfiguration()) {

			@Override
			protected AbstractRedisClient createClient() {
				return clientMock;
			}
		};

		connectionFactory.afterPropertiesSet();

		new DirectFieldAccessor(connectionFactory).setPropertyValue("client", clientMock);

		connectionFactory.getClusterConnection().close();
		connectionFactory.getClusterConnection().close();

		verify(clientMock).connectAsync(ArgumentMatchers.any(RedisCodec.class));
	}

	@Test // DATAREDIS-950
	public void shouldValidateSharedClusterConnection() {

		RedisClusterClient clientMock = mock(RedisClusterClient.class);
		StatefulRedisClusterConnection<byte[], byte[]> connectionMock = mock(StatefulRedisClusterConnection.class);
		RedisAdvancedClusterCommands<byte[], byte[]> syncMock = mock(RedisAdvancedClusterCommands.class);
		when(clientMock.connectAsync(ByteArrayCodec.INSTANCE))
				.thenReturn(CompletableFuture.completedFuture(connectionMock));
		when(connectionMock.isOpen()).thenReturn(true);
		when(connectionMock.sync()).thenReturn(syncMock);

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(clusterConfig,
				LettuceClientConfiguration.defaultConfiguration()) {

			@Override
			protected AbstractRedisClient createClient() {
				return clientMock;
			}
		};

		connectionFactory.setValidateConnection(true);
		connectionFactory.afterPropertiesSet();

		try (RedisConnection connection = connectionFactory.getConnection()) {
			connection.ping();
		}

		verify(syncMock).ping();
	}

	@Test // DATAREDIS-953
	public void shouldReleaseSharedConnectionOnlyOnce() {

		RedisClusterClient clientMock = mock(RedisClusterClient.class);
		StatefulRedisClusterConnection<byte[], byte[]> connectionMock = mock(StatefulRedisClusterConnection.class);
		when(clientMock.connectAsync(ByteArrayCodec.INSTANCE))
				.thenReturn(CompletableFuture.completedFuture(connectionMock));
		when(connectionMock.isOpen()).thenReturn(false);
		when(connectionMock.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(clusterConfig,
				LettuceClientConfiguration.defaultConfiguration()) {

			@Override
			protected AbstractRedisClient createClient() {
				return clientMock;
			}
		};

		connectionFactory.setValidateConnection(true);
		connectionFactory.afterPropertiesSet();

		connectionFactory.getConnection().close();

		verify(connectionMock).closeAsync();
	}

	@Test // DATAREDIS-721
	@SuppressWarnings("unchecked")
	public void shouldEagerlyInitializeSharedConnection() {

		LettuceConnectionProvider connectionProviderMock = mock(LettuceConnectionProvider.class);
		StatefulRedisConnection connectionMock = mock(StatefulRedisConnection.class);

		when(connectionProviderMock.getConnectionAsync(any()))
				.thenReturn(CompletableFuture.completedFuture(connectionMock));

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory() {
			@Override
			protected LettuceConnectionProvider doCreateConnectionProvider(AbstractRedisClient client,
					RedisCodec<?, ?> codec) {
				return connectionProviderMock;
			}
		};
		connectionFactory.setClientResources(LettuceTestClientResources.getSharedClientResources());
		connectionFactory.setEagerInitialization(true);

		connectionFactory.afterPropertiesSet();

		verify(connectionProviderMock, times(2)).getConnection(StatefulConnection.class);
	}

	@Test // DATAREDIS-1027
	public void shouldDisposeConnectionProviders() throws Exception {

		LettuceConnectionProvider connectionProviderMock = mock(LettuceConnectionProvider.class,
				withSettings().extraInterfaces(DisposableBean.class));
		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory() {
			@Override
			protected LettuceConnectionProvider doCreateConnectionProvider(AbstractRedisClient client,
					RedisCodec<?, ?> codec) {
				return connectionProviderMock;
			}
		};

		connectionFactory.afterPropertiesSet();
		connectionFactory.destroy();

		verify((DisposableBean) connectionProviderMock, times(2)).destroy();
	}

	@Test // DATAREDIS-842
	public void databaseShouldBeSetCorrectlyOnSentinelClient() {

		RedisSentinelConfiguration redisSentinelConfiguration = new RedisSentinelConfiguration("mymaster",
				Collections.singleton("host:1234"));
		redisSentinelConfiguration.setDatabase(1);
		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(redisSentinelConfiguration);
		connectionFactory.setClientResources(getSharedClientResources());
		connectionFactory.setPassword("o_O");
		connectionFactory.afterPropertiesSet();
		ConnectionFactoryTracker.add(connectionFactory);

		AbstractRedisClient client = (AbstractRedisClient) getField(connectionFactory, "client");
		assertThat(client).isInstanceOf(RedisClient.class);

		RedisURI redisUri = (RedisURI) getField(client, "redisURI");

		assertThat(redisUri.getDatabase()).isEqualTo(1);
	}

	@Test // DATAREDIS-949
	public void maxRedirectsShouldBeSetOnClientOptions() {

		RedisClusterConfiguration clusterConfiguration = new RedisClusterConfiguration();
		clusterConfiguration.clusterNode("localhost", 1234).setMaxRedirects(42);

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(clusterConfiguration,
				LettuceClientConfiguration.defaultConfiguration());
		connectionFactory.afterPropertiesSet();
		ConnectionFactoryTracker.add(connectionFactory);

		RedisClusterClient client = (RedisClusterClient) getField(connectionFactory, "client");

		ClusterClientOptions options = (ClusterClientOptions) client.getOptions();

		assertThat(options.getMaxRedirects()).isEqualTo(42);
		assertThat(options.isValidateClusterNodeMembership()).isTrue();
		assertThat(options.getTimeoutOptions().isApplyConnectionTimeout()).isTrue();
	}

	@Test // DATAREDIS-949
	public void maxRedirectsShouldBeSetOnClusterClientOptions() {

		RedisClusterConfiguration clusterConfiguration = new RedisClusterConfiguration();
		clusterConfiguration.clusterNode("localhost", 1234).setMaxRedirects(42);

		LettuceClientConfiguration clientConfiguration = LettuceClientConfiguration.builder()
				.clientOptions(ClusterClientOptions.builder().validateClusterNodeMembership(false).build()).build();

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(clusterConfiguration,
				clientConfiguration);
		connectionFactory.afterPropertiesSet();
		ConnectionFactoryTracker.add(connectionFactory);

		RedisClusterClient client = (RedisClusterClient) getField(connectionFactory, "client");

		ClusterClientOptions options = (ClusterClientOptions) client.getOptions();

		assertThat(options.getMaxRedirects()).isEqualTo(42);
		assertThat(options.isValidateClusterNodeMembership()).isFalse();
		assertThat(options.getTimeoutOptions().isApplyConnectionTimeout()).isFalse();
	}

	@Data
	@AllArgsConstructor
	static class CustomRedisConfiguration implements RedisConfiguration, WithHostAndPort {

		private String hostName;
		private int port;

		CustomRedisConfiguration(String hostName) {
			this(hostName, 6379);
		}
	}
}
