/*
 * Copyright 2015-2024 the original author or authors.
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
import static org.springframework.data.redis.test.extension.LettuceTestClientResources.*;
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
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.springframework.beans.DirectFieldAccessor;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.data.redis.ConnectionFactoryTracker;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.connection.PoolException;
import org.springframework.data.redis.connection.RedisClusterConfiguration;
import org.springframework.data.redis.connection.RedisClusterConnection;
import org.springframework.data.redis.connection.RedisConfiguration;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisNode;
import org.springframework.data.redis.connection.RedisPassword;
import org.springframework.data.redis.connection.RedisSentinelConfiguration;
import org.springframework.data.redis.connection.RedisSocketConfiguration;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.test.extension.LettuceTestClientResources;

/**
 * Unit tests for {@link LettuceConnectionFactory}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Balázs Németh
 * @author Ruben Cervilla
 * @author Luis De Bello
 * @author Andrea Como
 * @author Chris Bono
 * @author John Blum
 */
class LettuceConnectionFactoryUnitTests {

	private RedisClusterConfiguration clusterConfig;

	@BeforeEach
	void setUp() {
		clusterConfig = new RedisClusterConfiguration().clusterNode("127.0.0.1", 6379).clusterNode("127.0.0.1", 6380);
	}

	@AfterEach
	void tearDown() {
		ConnectionFactoryTracker.cleanUp();
	}

	@Test // DATAREDIS-315
	void shouldInitClientCorrectlyWhenClusterConfigPresent() {

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(clusterConfig,
				LettuceTestClientConfiguration.defaultConfiguration());
		connectionFactory.afterPropertiesSet();
		ConnectionFactoryTracker.add(connectionFactory);

		assertThat(getField(connectionFactory, "client")).isInstanceOf(RedisClusterClient.class);
	}

	@Test // DATAREDIS-315
	@SuppressWarnings("unchecked")
	void timeoutShouldBeSetCorrectlyOnClusterClient() {

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
	void portShouldBeReturnedProperlyBasedOnConfiguration() {

		RedisConfiguration redisConfiguration = new RedisStandaloneConfiguration("localhost", 16379);

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(redisConfiguration,
				LettuceTestClientConfiguration.defaultConfiguration());

		assertThat(connectionFactory.getPort()).isEqualTo(16379);
	}

	@Test // DATAREDIS-930
	void portShouldBeReturnedProperlyBasedOnCustomRedisConfiguration() {

		RedisConfiguration redisConfiguration = new CustomRedisConfiguration("localhost", 16379);

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(redisConfiguration,
				LettuceTestClientConfiguration.defaultConfiguration());

		assertThat(connectionFactory.getPort()).isEqualTo(16379);
		assertThat(connectionFactory.getHostName()).isEqualTo("localhost");
	}

	@Test // DATAREDIS-930
	void hostNameShouldBeReturnedProperlyBasedOnConfiguration() {

		RedisConfiguration redisConfiguration = new RedisStandaloneConfiguration("external");

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(redisConfiguration,
				LettuceTestClientConfiguration.defaultConfiguration());

		assertThat(connectionFactory.getHostName()).isEqualTo("external");
	}

	@Test // DATAREDIS-930
	void hostNameShouldBeReturnedProperlyBasedOnCustomRedisConfiguration() {

		RedisConfiguration redisConfiguration = new CustomRedisConfiguration("external");

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(redisConfiguration,
				LettuceTestClientConfiguration.defaultConfiguration());

		assertThat(connectionFactory.getPort()).isEqualTo(6379);
		assertThat(connectionFactory.getHostName()).isEqualTo("external");
	}

	@Test // DATAREDIS-315
	@SuppressWarnings("unchecked")
	void passwordShouldBeSetCorrectlyOnClusterClient() {

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(clusterConfig,
				LettuceTestClientConfiguration.defaultConfiguration());
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

	@Test // GH-2376
	@SuppressWarnings("unchecked")
	void credentialsProviderShouldBeSetCorrectlyOnClusterClient() {

		clusterConfig.setUsername("foo");
		clusterConfig.setPassword("bar");

		LettuceClientConfiguration clientConfiguration = LettuceTestClientConfiguration.builder()
				.redisCredentialsProviderFactory(new RedisCredentialsProviderFactory() {}).build();
		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(clusterConfig, clientConfiguration);
		connectionFactory.afterPropertiesSet();
		ConnectionFactoryTracker.add(connectionFactory);

		AbstractRedisClient client = (AbstractRedisClient) getField(connectionFactory, "client");
		assertThat(client).isInstanceOf(RedisClusterClient.class);

		Iterable<RedisURI> initialUris = (Iterable<RedisURI>) getField(client, "initialUris");

		for (RedisURI uri : initialUris) {

			uri.getCredentialsProvider().resolveCredentials().as(StepVerifier::create).consumeNextWith(actual -> {
				assertThat(actual.getUsername()).isEqualTo("foo");
				assertThat(new String(actual.getPassword())).isEqualTo("bar");
			}).verifyComplete();
		}
	}

	@Test // GH-2376
	void credentialsProviderShouldBeSetCorrectlyOnStandaloneClient() {

		RedisStandaloneConfiguration config = new RedisStandaloneConfiguration("localhost");
		config.setUsername("foo");
		config.setPassword("bar");

		LettuceClientConfiguration clientConfiguration = LettuceTestClientConfiguration.builder()
				.clientResources(getSharedClientResources())
				.redisCredentialsProviderFactory(new RedisCredentialsProviderFactory() {}).build();
		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(config, clientConfiguration);
		connectionFactory.afterPropertiesSet();
		ConnectionFactoryTracker.add(connectionFactory);

		AbstractRedisClient client = (AbstractRedisClient) getField(connectionFactory, "client");
		assertThat(client).isInstanceOf(RedisClient.class);

		RedisURI uri = (RedisURI) getField(client, "redisURI");

		uri.getCredentialsProvider().resolveCredentials().as(StepVerifier::create).consumeNextWith(actual -> {
			assertThat(actual.getUsername()).isEqualTo("foo");
			assertThat(new String(actual.getPassword())).isEqualTo("bar");
		}).verifyComplete();
	}

	@Test // DATAREDIS-524, DATAREDIS-1045, DATAREDIS-1060
	void passwordShouldNotBeSetOnSentinelClient() {

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(
				new RedisSentinelConfiguration("mymaster", Collections.singleton("host:1234")),
				LettuceTestClientConfiguration.defaultConfiguration());
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
	void sentinelPasswordShouldBeSetOnSentinelClient() {

		RedisSentinelConfiguration config = new RedisSentinelConfiguration("mymaster", Collections.singleton("host:1234"));
		config.setSentinelPassword("sentinel-pwd");

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(config,
				LettuceTestClientConfiguration.defaultConfiguration());
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

	@Test // GH-2376
	void sentinelCredentialsProviderShouldBeSetOnSentinelClient() {

		RedisSentinelConfiguration config = new RedisSentinelConfiguration("mymaster", Collections.singleton("host:1234"));
		config.setUsername("data-user");
		config.setPassword("data-pwd");
		config.setSentinelPassword("sentinel-pwd");

		LettuceClientConfiguration clientConfiguration = LettuceTestClientConfiguration.builder()
				.redisCredentialsProviderFactory(new RedisCredentialsProviderFactory() {}).build();

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(config, clientConfiguration);
		connectionFactory.afterPropertiesSet();
		ConnectionFactoryTracker.add(connectionFactory);

		AbstractRedisClient client = (AbstractRedisClient) getField(connectionFactory, "client");
		assertThat(client).isInstanceOf(RedisClient.class);

		RedisURI redisUri = (RedisURI) getField(client, "redisURI");

		redisUri.getCredentialsProvider().resolveCredentials().as(StepVerifier::create).consumeNextWith(actual -> {
			assertThat(actual.getUsername()).isEqualTo("data-user");
			assertThat(new String(actual.getPassword())).isEqualTo("data-pwd");
		}).verifyComplete();

		for (RedisURI sentinelUri : redisUri.getSentinels()) {

			sentinelUri.getCredentialsProvider().resolveCredentials().as(StepVerifier::create).consumeNextWith(actual -> {
				assertThat(actual.getUsername()).isNull();
				assertThat(new String(actual.getPassword())).isEqualTo("sentinel-pwd");
			}).verifyComplete();
		}
	}

	@Test // DATAREDIS-1060
	void sentinelPasswordShouldNotLeakIntoDataNodeClient() {

		RedisSentinelConfiguration config = new RedisSentinelConfiguration("mymaster", Collections.singleton("host:1234"));
		config.setSentinelPassword("sentinel-pwd");

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(config,
				LettuceTestClientConfiguration.defaultConfiguration());
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
	@Disabled("Until Lettuce supports Sinks")
	void clusterClientShouldInitializeWithoutClientResources() {

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(clusterConfig);
		connectionFactory.setShutdownTimeout(0);
		connectionFactory.afterPropertiesSet();
		ConnectionFactoryTracker.add(connectionFactory);

		AbstractRedisClient client = (AbstractRedisClient) getField(connectionFactory, "client");
		assertThat(client).isInstanceOf(RedisClusterClient.class);
	}

	@Test // DATAREDIS-480
	void sslOptionsShouldBeDisabledByDefaultOnClient() {

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(new RedisStandaloneConfiguration(),
				LettuceTestClientConfiguration.defaultConfiguration());
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
	void sslShouldBeSetCorrectlyOnClient() {

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(new RedisStandaloneConfiguration(),
				LettuceTestClientConfiguration.builder().useSsl().build());
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
	void verifyPeerOptionShouldBeSetCorrectlyOnClient() {

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(new RedisStandaloneConfiguration(),
				LettuceTestClientConfiguration.builder().useSsl().disablePeerVerification().build());
		connectionFactory.afterPropertiesSet();
		ConnectionFactoryTracker.add(connectionFactory);
		connectionFactory.start();

		AbstractRedisClient client = (AbstractRedisClient) getField(connectionFactory, "client");
		assertThat(client).isInstanceOf(RedisClient.class);

		RedisURI redisUri = (RedisURI) getField(client, "redisURI");

		assertThat(redisUri.isVerifyPeer()).isFalse();
		assertThat(connectionFactory.isVerifyPeer()).isFalse();
	}

	@Test // DATAREDIS-480
	void startTLSOptionShouldBeSetCorrectlyOnClient() {

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(new RedisStandaloneConfiguration(),
				LettuceTestClientConfiguration.builder().useSsl().startTls().build());
		connectionFactory.afterPropertiesSet();
		ConnectionFactoryTracker.add(connectionFactory);
		connectionFactory.start();

		AbstractRedisClient client = (AbstractRedisClient) getField(connectionFactory, "client");
		assertThat(client).isInstanceOf(RedisClient.class);

		RedisURI redisUri = (RedisURI) getField(client, "redisURI");

		assertThat(redisUri.isStartTls()).isTrue();
		assertThat(connectionFactory.isStartTls()).isTrue();
	}

	@Test // DATAREDIS-990
	void sslShouldBeSetCorrectlyOnSentinelClient() {

		RedisSentinelConfiguration sentinelConfiguration = new RedisSentinelConfiguration("myMaster",
				Collections.singleton("localhost:1234"));
		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(sentinelConfiguration,
				LettuceTestClientConfiguration.builder().useSsl().startTls().build());
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
	void verifyPeerOptionShouldBeSetCorrectlyOnSentinelClient() {

		RedisSentinelConfiguration sentinelConfiguration = new RedisSentinelConfiguration("myMaster",
				Collections.singleton("localhost:1234"));
		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(sentinelConfiguration,
				LettuceTestClientConfiguration.builder().useSsl().disablePeerVerification().build());
		connectionFactory.afterPropertiesSet();
		ConnectionFactoryTracker.add(connectionFactory);

		AbstractRedisClient client = (AbstractRedisClient) getField(connectionFactory, "client");
		assertThat(client).isInstanceOf(RedisClient.class);

		RedisURI redisUri = (RedisURI) getField(client, "redisURI");

		assertThat(redisUri.isVerifyPeer()).isFalse();
		assertThat(connectionFactory.isVerifyPeer()).isFalse();
	}

	@Test // DATAREDIS-990
	void startTLSOptionShouldBeSetCorrectlyOnSentinelClient() {

		RedisSentinelConfiguration sentinelConfiguration = new RedisSentinelConfiguration("myMaster",
				Collections.singleton("localhost:1234"));
		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(sentinelConfiguration,
				LettuceTestClientConfiguration.builder().useSsl().startTls().build());
		connectionFactory.afterPropertiesSet();
		ConnectionFactoryTracker.add(connectionFactory);

		AbstractRedisClient client = (AbstractRedisClient) getField(connectionFactory, "client");
		assertThat(client).isInstanceOf(RedisClient.class);

		RedisURI redisUri = (RedisURI) getField(client, "redisURI");

		assertThat(redisUri.isStartTls()).isTrue();
		assertThat(connectionFactory.isStartTls()).isTrue();
	}

	@Test // DATAREDIS-537
	void sslShouldBeSetCorrectlyOnClusterClient() {

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(
				new RedisClusterConfiguration().clusterNode(CLUSTER_NODE_1),
				LettuceTestClientConfiguration.builder().useSsl().build());
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
	void startTLSOptionShouldBeSetCorrectlyOnClusterClient() {

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(
				new RedisClusterConfiguration().clusterNode(CLUSTER_NODE_1),
				LettuceTestClientConfiguration.builder().useSsl().startTls().build());
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
	void verifyPeerTLSOptionShouldBeSetCorrectlyOnClusterClient() {

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(
				new RedisClusterConfiguration().clusterNode(CLUSTER_NODE_1),
				LettuceTestClientConfiguration.builder().useSsl().startTls().build());
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
	void socketShouldBeSetOnStandaloneClient() {

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(new RedisSocketConfiguration(),
				LettuceTestClientConfiguration.defaultConfiguration());
		connectionFactory.afterPropertiesSet();
		ConnectionFactoryTracker.add(connectionFactory);

		AbstractRedisClient client = (AbstractRedisClient) getField(connectionFactory, "client");
		assertThat(client).isInstanceOf(RedisClient.class);

		RedisURI redisUri = (RedisURI) getField(client, "redisURI");

		assertThat(redisUri.getSocket()).isEqualTo("/tmp/redis.sock");
	}

	@Test // DATAREDIS-574
	void shouldReadStandalonePassword() {

		RedisStandaloneConfiguration envConfig = new RedisStandaloneConfiguration();
		envConfig.setPassword(RedisPassword.of("foo"));

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(envConfig,
				LettuceTestClientConfiguration.defaultConfiguration());

		assertThat(connectionFactory.getPassword()).isEqualTo("foo");
	}

	@Test // DATAREDIS-574
	void shouldWriteStandalonePassword() {

		RedisStandaloneConfiguration envConfig = new RedisStandaloneConfiguration();
		envConfig.setPassword(RedisPassword.of("foo"));

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(envConfig,
				LettuceTestClientConfiguration.defaultConfiguration());
		connectionFactory.setPassword("bar");

		assertThat(connectionFactory.getPassword()).isEqualTo("bar");
		assertThat(envConfig.getPassword()).isEqualTo(RedisPassword.of("bar"));
	}

	@Test // DATAREDIS-574
	void shouldReadSentinelPassword() {

		RedisSentinelConfiguration envConfig = new RedisSentinelConfiguration();
		envConfig.setPassword(RedisPassword.of("foo"));

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(envConfig,
				LettuceTestClientConfiguration.defaultConfiguration());

		assertThat(connectionFactory.getPassword()).isEqualTo("foo");
	}

	@Test // DATAREDIS-574
	void shouldWriteSentinelPassword() {

		RedisSentinelConfiguration envConfig = new RedisSentinelConfiguration();
		envConfig.setPassword(RedisPassword.of("foo"));

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(envConfig,
				LettuceTestClientConfiguration.defaultConfiguration());
		connectionFactory.setPassword("bar");

		assertThat(connectionFactory.getPassword()).isEqualTo("bar");
		assertThat(envConfig.getPassword()).isEqualTo(RedisPassword.of("bar"));
	}

	@Test // DATAREDIS-682
	void shouldWriteSocketPassword() {

		RedisSocketConfiguration envConfig = new RedisSocketConfiguration();
		envConfig.setPassword(RedisPassword.of("foo"));

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(envConfig,
				LettuceTestClientConfiguration.defaultConfiguration());
		connectionFactory.setPassword("bar");

		assertThat(connectionFactory.getPassword()).isEqualTo("bar");
		assertThat(envConfig.getPassword()).isEqualTo(RedisPassword.of("bar"));
	}

	@Test // DATAREDIS-574
	void shouldReadClusterPassword() {

		RedisClusterConfiguration envConfig = new RedisClusterConfiguration();
		envConfig.setPassword(RedisPassword.of("foo"));

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(envConfig,
				LettuceTestClientConfiguration.defaultConfiguration());

		assertThat(connectionFactory.getPassword()).isEqualTo("foo");
	}

	@Test // DATAREDIS-574
	void shouldWriteClusterPassword() {

		RedisClusterConfiguration envConfig = new RedisClusterConfiguration();
		envConfig.setPassword(RedisPassword.of("foo"));

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(envConfig,
				LettuceTestClientConfiguration.defaultConfiguration());
		connectionFactory.setPassword("bar");

		assertThat(connectionFactory.getPassword()).isEqualTo("bar");
		assertThat(envConfig.getPassword()).isEqualTo(RedisPassword.of("bar"));
	}

	@Test // DATAREDIS-574
	void shouldReadStandaloneDatabaseIndex() {

		RedisStandaloneConfiguration envConfig = new RedisStandaloneConfiguration();
		envConfig.setDatabase(2);

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(envConfig,
				LettuceTestClientConfiguration.defaultConfiguration());

		assertThat(connectionFactory.getDatabase()).isEqualTo(2);
	}

	@Test // DATAREDIS-574
	void shouldWriteStandaloneDatabaseIndex() {

		RedisStandaloneConfiguration envConfig = new RedisStandaloneConfiguration();
		envConfig.setDatabase(2);

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(envConfig,
				LettuceTestClientConfiguration.defaultConfiguration());
		connectionFactory.setDatabase(3);

		assertThat(connectionFactory.getDatabase()).isEqualTo(3);
		assertThat(envConfig.getDatabase()).isEqualTo(3);
	}

	@Test // DATAREDIS-574
	void shouldReadSentinelDatabaseIndex() {

		RedisSentinelConfiguration envConfig = new RedisSentinelConfiguration();
		envConfig.setDatabase(2);

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(envConfig,
				LettuceTestClientConfiguration.defaultConfiguration());

		assertThat(connectionFactory.getDatabase()).isEqualTo(2);
	}

	@Test // DATAREDIS-574
	void shouldWriteSentinelDatabaseIndex() {

		RedisSentinelConfiguration envConfig = new RedisSentinelConfiguration();
		envConfig.setDatabase(2);

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(envConfig,
				LettuceTestClientConfiguration.defaultConfiguration());
		connectionFactory.setDatabase(3);

		assertThat(connectionFactory.getDatabase()).isEqualTo(3);
		assertThat(envConfig.getDatabase()).isEqualTo(3);
	}

	@Test // DATAREDIS-682
	void shouldWriteSocketDatabaseIndex() {

		RedisSocketConfiguration envConfig = new RedisSocketConfiguration();
		envConfig.setDatabase(2);

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(envConfig,
				LettuceTestClientConfiguration.defaultConfiguration());
		connectionFactory.setDatabase(3);

		assertThat(connectionFactory.getDatabase()).isEqualTo(3);
		assertThat(envConfig.getDatabase()).isEqualTo(3);
	}

	@Test // DATAREDIS-574
	void shouldApplyClientConfiguration() {

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
	void shouldReturnStandaloneConfiguration() {

		RedisStandaloneConfiguration configuration = new RedisStandaloneConfiguration();
		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(configuration,
				LettuceTestClientConfiguration.defaultConfiguration());

		assertThat(connectionFactory.getStandaloneConfiguration()).isEqualTo(configuration);
		assertThat(connectionFactory.getSentinelConfiguration()).isNull();
		assertThat(connectionFactory.getClusterConfiguration()).isNull();
	}

	@Test // DATAREDIS-682
	void shouldReturnSocketConfiguration() {

		RedisSocketConfiguration configuration = new RedisSocketConfiguration("/var/redis/socket");
		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(configuration,
				LettuceTestClientConfiguration.defaultConfiguration());

		assertThat(connectionFactory.getStandaloneConfiguration()).isNotNull();
		assertThat(connectionFactory.getSocketConfiguration()).isEqualTo(configuration);
		assertThat(connectionFactory.getSentinelConfiguration()).isNull();
		assertThat(connectionFactory.getClusterConfiguration()).isNull();
	}

	@Test // DATAREDIS-574
	void shouldReturnSentinelConfiguration() {

		RedisSentinelConfiguration configuration = new RedisSentinelConfiguration();
		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(configuration,
				LettuceTestClientConfiguration.defaultConfiguration());

		assertThat(connectionFactory.getStandaloneConfiguration()).isNotNull();
		assertThat(connectionFactory.getSentinelConfiguration()).isEqualTo(configuration);
		assertThat(connectionFactory.getClusterConfiguration()).isNull();
	}

	@Test // DATAREDIS-574
	void shouldReturnClusterConfiguration() {

		RedisClusterConfiguration configuration = new RedisClusterConfiguration();
		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(configuration,
				LettuceTestClientConfiguration.defaultConfiguration());

		assertThat(connectionFactory.getStandaloneConfiguration()).isNotNull();
		assertThat(connectionFactory.getSentinelConfiguration()).isNull();
		assertThat(connectionFactory.getClusterConfiguration()).isEqualTo(configuration);
	}

	@Test // DATAREDIS-574
	void shouldDenyChangesToImmutableClientConfiguration() {

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(new RedisStandaloneConfiguration(),
				LettuceTestClientConfiguration.defaultConfiguration());

		assertThatIllegalStateException().isThrownBy(() -> connectionFactory.setUseSsl(false));
	}

	@Test // DATAREDIS-676
	void timeoutShouldBePassedOnToClusterConnection() {

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(clusterConfig);
		connectionFactory.setShutdownTimeout(0);
		connectionFactory.setTimeout(2000);
		connectionFactory.setShareNativeConnection(false);
		connectionFactory.setClientResources(getSharedClientResources());
		connectionFactory.afterPropertiesSet();
		ConnectionFactoryTracker.add(connectionFactory);

		RedisClusterConnection clusterConnection = connectionFactory.getClusterConnection();
		assertThat(getField(clusterConnection, "timeout")).isEqualTo(2000L);

		clusterConnection.close();
	}

	@Test // DATAREDIS-676
	void timeoutSetOnClientConfigShouldBePassedOnToClusterConnection() {

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(clusterConfig,
				LettuceTestClientConfiguration.builder().commandTimeout(Duration.ofSeconds(2)).build());
		connectionFactory.setShareNativeConnection(false);

		connectionFactory.afterPropertiesSet();
		ConnectionFactoryTracker.add(connectionFactory);

		RedisClusterConnection clusterConnection = connectionFactory.getClusterConnection();
		assertThat(getField(clusterConnection, "timeout")).isEqualTo(2000L);

		clusterConnection.close();
	}

	@Test // DATAREDIS-731
	void shouldShareNativeConnectionWithCluster() {

		RedisClusterClient clientMock = mock(RedisClusterClient.class);
		StatefulRedisClusterConnection<byte[], byte[]> connectionMock = mock(StatefulRedisClusterConnection.class);
		when(clientMock.connectAsync(ByteArrayCodec.INSTANCE))
				.thenReturn(CompletableFuture.completedFuture(connectionMock));

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(clusterConfig,
				LettuceTestClientConfiguration.defaultConfiguration()) {

			@Override
			protected AbstractRedisClient createClient() {
				return clientMock;
			}
		};

		connectionFactory.afterPropertiesSet();
		connectionFactory.start();

		new DirectFieldAccessor(connectionFactory).setPropertyValue("client", clientMock);

		connectionFactory.getClusterConnection().close();
		connectionFactory.getClusterConnection().close();

		verify(clientMock).connectAsync(ArgumentMatchers.any(RedisCodec.class));
	}

	@Test // DATAREDIS-950
	void shouldValidateSharedClusterConnection() {

		RedisClusterClient clientMock = mock(RedisClusterClient.class);
		StatefulRedisClusterConnection<byte[], byte[]> connectionMock = mock(StatefulRedisClusterConnection.class);
		RedisAdvancedClusterCommands<byte[], byte[]> syncMock = mock(RedisAdvancedClusterCommands.class);
		when(clientMock.connectAsync(ByteArrayCodec.INSTANCE))
				.thenReturn(CompletableFuture.completedFuture(connectionMock));
		when(connectionMock.isOpen()).thenReturn(true);
		when(connectionMock.sync()).thenReturn(syncMock);

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(clusterConfig,
				LettuceTestClientConfiguration.defaultConfiguration()) {

			@Override
			protected AbstractRedisClient createClient() {
				return clientMock;
			}
		};

		connectionFactory.setValidateConnection(true);
		connectionFactory.afterPropertiesSet();
		connectionFactory.start();

		try (RedisConnection connection = connectionFactory.getConnection()) {
			connection.ping();
		}

		verify(syncMock).ping();
	}

	@Test // DATAREDIS-953
	void shouldReleaseSharedConnectionOnlyOnce() {

		RedisClusterClient clientMock = mock(RedisClusterClient.class);
		StatefulRedisClusterConnection<byte[], byte[]> connectionMock = mock(StatefulRedisClusterConnection.class);
		when(clientMock.connectAsync(ByteArrayCodec.INSTANCE))
				.thenReturn(CompletableFuture.completedFuture(connectionMock));
		when(connectionMock.isOpen()).thenReturn(false);
		when(connectionMock.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(clusterConfig,
				LettuceTestClientConfiguration.defaultConfiguration()) {

			@Override
			protected AbstractRedisClient createClient() {
				return clientMock;
			}
		};

		connectionFactory.setValidateConnection(true);
		connectionFactory.afterPropertiesSet();
		connectionFactory.start();

		connectionFactory.getConnection().close();

		verify(connectionMock).closeAsync();
	}

	@Test // DATAREDIS-721
	@SuppressWarnings("unchecked")
	void shouldEagerlyInitializeSharedConnection() {

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
		connectionFactory.start();

		verify(connectionProviderMock, times(2)).getConnection(StatefulConnection.class);
	}

	@Test // DATAREDIS-1189
	void shouldTranslateConnectionException() {

		LettuceConnectionProvider connectionProviderMock = mock(LettuceConnectionProvider.class);

		when(connectionProviderMock.getConnection(any())).thenThrow(new PoolException("error"));

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory() {
			@Override
			protected LettuceConnectionProvider doCreateConnectionProvider(AbstractRedisClient client,
					RedisCodec<?, ?> codec) {
				return connectionProviderMock;
			}
		};
		connectionFactory.setClientResources(LettuceTestClientResources.getSharedClientResources());
		connectionFactory.afterPropertiesSet();
		connectionFactory.start();

		assertThatExceptionOfType(RedisConnectionFailureException.class)
				.isThrownBy(() -> connectionFactory.getConnection().ping()).withCauseInstanceOf(PoolException.class);
	}

	@Test // DATAREDIS-1027
	void shouldDisposeConnectionProviders() throws Exception {

		LettuceConnectionProvider connectionProviderMock = mock(LettuceConnectionProvider.class,
				withSettings().extraInterfaces(DisposableBean.class));
		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory() {
			@Override
			protected LettuceConnectionProvider doCreateConnectionProvider(AbstractRedisClient client,
					RedisCodec<?, ?> codec) {
				return connectionProviderMock;
			}
		};

		connectionFactory.setClientResources(getSharedClientResources());
		connectionFactory.afterPropertiesSet();
		connectionFactory.start();
		connectionFactory.destroy();

		verify((DisposableBean) connectionProviderMock, times(2)).destroy();
	}

	@Test // DATAREDIS-842
	void databaseShouldBeSetCorrectlyOnSentinelClient() {

		RedisSentinelConfiguration redisSentinelConfiguration = new RedisSentinelConfiguration("mymaster",
				Collections.singleton("host:1234"));
		redisSentinelConfiguration.setDatabase(1);
		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(redisSentinelConfiguration,
				LettuceTestClientConfiguration.defaultConfiguration());
		connectionFactory.setPassword("o_O");
		connectionFactory.afterPropertiesSet();
		ConnectionFactoryTracker.add(connectionFactory);

		AbstractRedisClient client = (AbstractRedisClient) getField(connectionFactory, "client");
		assertThat(client).isInstanceOf(RedisClient.class);

		RedisURI redisUri = (RedisURI) getField(client, "redisURI");

		assertThat(redisUri.getDatabase()).isEqualTo(1);
	}

	@Test // DATAREDIS-949
	void maxRedirectsShouldBeSetOnClientOptions() {

		RedisClusterConfiguration clusterConfiguration = new RedisClusterConfiguration();
		clusterConfiguration.clusterNode("localhost", 1234).setMaxRedirects(42);

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(clusterConfiguration,
				LettuceTestClientConfiguration.builder().build());
		connectionFactory.afterPropertiesSet();
		ConnectionFactoryTracker.add(connectionFactory);

		RedisClusterClient client = (RedisClusterClient) getField(connectionFactory, "client");

		ClusterClientOptions options = (ClusterClientOptions) client.getOptions();

		assertThat(options.getMaxRedirects()).isEqualTo(42);
		assertThat(options.isValidateClusterNodeMembership()).isTrue();
		assertThat(options.getTimeoutOptions().isApplyConnectionTimeout()).isTrue();
	}

	@Test // DATAREDIS-949
	void maxRedirectsShouldBeSetOnClusterClientOptions() {

		RedisClusterConfiguration clusterConfiguration = new RedisClusterConfiguration();
		clusterConfiguration.clusterNode("localhost", 1234).setMaxRedirects(42);

		LettuceClientConfiguration clientConfiguration = LettuceTestClientConfiguration.builder()
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

	@Test // DATAREDIS-1142
	void shouldFallbackToReactiveRedisClusterConnectionWhenGetReactiveConnectionWithClusterConfig() {

		LettuceConnectionProvider connectionProviderMock = mock(LettuceConnectionProvider.class);
		StatefulConnection<?, ?> statefulConnection = mock(StatefulConnection.class);
		when(connectionProviderMock.getConnection(any())).thenReturn(statefulConnection);
		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(clusterConfig) {

			@Override
			protected LettuceConnectionProvider doCreateConnectionProvider(AbstractRedisClient client,
					RedisCodec<?, ?> codec) {
				return connectionProviderMock;
			}
		};
		connectionFactory.setClientResources(getSharedClientResources());
		connectionFactory.afterPropertiesSet();
		connectionFactory.start();

		LettuceReactiveRedisConnection reactiveConnection = connectionFactory.getReactiveConnection();

		assertThat(reactiveConnection).isInstanceOf(LettuceReactiveRedisClusterConnection.class);
	}

	@Test // GH-1745
	void getNativeClientShouldReturnClient() {

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory();
		connectionFactory.setClientResources(getSharedClientResources());
		connectionFactory.afterPropertiesSet();
		connectionFactory.start();

		assertThat(connectionFactory.getNativeClient()).isInstanceOf(RedisClient.class);

		connectionFactory = new LettuceConnectionFactory(clusterConfig);
		connectionFactory.setClientResources(getSharedClientResources());
		connectionFactory.afterPropertiesSet();
		connectionFactory.start();

		assertThat(connectionFactory.getRequiredNativeClient()).isInstanceOf(RedisClusterClient.class);
	}

	@Test // GH-1745
	void getNativeClientShouldFailIfNotInitialized() {

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory();

		assertThatIllegalStateException().isThrownBy(connectionFactory::getRequiredNativeClient)
				.withMessageContaining("Use start() to initialize");
	}

	@Test // GH-2057
	void getConnectionShouldFailIfNotInitialized() {

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory();

		assertThatIllegalStateException().isThrownBy(connectionFactory::getConnection);
		assertThatIllegalStateException().isThrownBy(connectionFactory::getClusterConnection);
		assertThatIllegalStateException().isThrownBy(connectionFactory::getSentinelConnection);
		assertThatIllegalStateException().isThrownBy(connectionFactory::getReactiveConnection);
		assertThatIllegalStateException().isThrownBy(connectionFactory::getReactiveClusterConnection);
	}

	@Test // GH-2116
	void createRedisConfigurationRequiresRedisUri() {

		assertThatIllegalArgumentException()
				.isThrownBy(() -> LettuceConnectionFactory.createRedisConfiguration((RedisURI) null))
				.withMessage("RedisURI must not be null");
	}

	@Test // GH-2116
	void createMinimalRedisStandaloneConfiguration() {

		RedisURI redisURI = RedisURI.create("redis://myserver");

		RedisStandaloneConfiguration expected = new RedisStandaloneConfiguration();
		expected.setHostName("myserver");

		RedisConfiguration configuration = LettuceConnectionFactory.createRedisConfiguration(redisURI);

		assertThat(configuration).isEqualTo(expected);
	}

	@Test // GH-2116
	void createFullRedisStanaloneConfiguration() {

		RedisURI redisURI = RedisURI.create("redis://fooUser:fooPass@myserver1:111/7");

		RedisStandaloneConfiguration expected = new RedisStandaloneConfiguration();
		expected.setHostName("myserver1");
		expected.setPort(111);
		expected.setDatabase(7);
		expected.setUsername("fooUser");
		expected.setPassword("fooPass");

		RedisConfiguration configuration = LettuceConnectionFactory.createRedisConfiguration(redisURI);

		assertThat(configuration).isEqualTo(expected);
	}

	@Test // GH-2116
	void createMinimalRedisSocketConfiguration() {

		RedisURI redisURI = RedisURI.Builder.socket("mysocket").build();

		RedisSocketConfiguration expected = new RedisSocketConfiguration();
		expected.setSocket("mysocket");

		RedisConfiguration socketConfiguration = LettuceConnectionFactory.createRedisConfiguration(redisURI);

		assertThat(socketConfiguration).isEqualTo(expected);
	}

	@Test // GH-2116
	void createFullRedisSocketConfiguration() {

		RedisURI redisURI = RedisURI.Builder.socket("mysocket").withAuthentication("fooUser", "fooPass".toCharArray())
				.withDatabase(7).build();

		RedisSocketConfiguration expected = new RedisSocketConfiguration();
		expected.setSocket("mysocket");
		expected.setUsername("fooUser");
		expected.setPassword("fooPass");
		expected.setDatabase(7);

		RedisConfiguration socketConfiguration = LettuceConnectionFactory.createRedisConfiguration(redisURI);

		assertThat(socketConfiguration).isEqualTo(expected);
	}

	@Test // GH-2116
	void createMinimalRedisSentinelConfiguration() {

		RedisURI redisURI = RedisURI.create("redis-sentinel://myserver?sentinelMasterId=5150");

		RedisSentinelConfiguration expected = new RedisSentinelConfiguration();
		expected.setMaster("5150");
		expected.addSentinel(new RedisNode("myserver", 26379));

		RedisConfiguration sentinelConfiguration = LettuceConnectionFactory.createRedisConfiguration(redisURI);

		assertThat(sentinelConfiguration).isEqualTo(expected);
	}

	@Test // GH-2116
	void createFullRedisSentinelConfiguration() {

		RedisURI redisURI = RedisURI
				.create("redis-sentinel://fooUser:fooPass@myserver1:111,myserver2:222/7?sentinelMasterId=5150");
		// Set the passwords directly on the sentinels so that it gets picked up by converter
		char[] sentinelPass = "changeme".toCharArray();
		redisURI.getSentinels().forEach(sentinelRedisUri -> sentinelRedisUri.setPassword(sentinelPass));

		RedisSentinelConfiguration expected = new RedisSentinelConfiguration();
		expected.setMaster("5150");
		expected.setDatabase(7);
		expected.setUsername("fooUser");
		expected.setPassword("fooPass");
		expected.setSentinelPassword(sentinelPass);
		expected.addSentinel(new RedisNode("myserver1", 111));
		expected.addSentinel(new RedisNode("myserver2", 222));

		RedisConfiguration configuration = LettuceConnectionFactory.createRedisConfiguration(redisURI);

		assertThat(configuration).isEqualTo(expected);
	}

	@Test // GH-2503, GH-2635
	void afterPropertiesSetTriggersConnectionInitialization() {

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory();
		connectionFactory.afterPropertiesSet();

		assertThat(connectionFactory.isRunning()).isTrue();
	}

	@Test // GH-2594
	void createRedisConfigurationWithNullInvalidRedisUriString() {

		Arrays.asList("  ", "", null)
				.forEach(redisUri -> assertThatIllegalArgumentException()
						.isThrownBy(() -> LettuceConnectionFactory.createRedisConfiguration(redisUri))
						.withMessage("RedisURI must not be null or empty").withNoCause());
	}

	@Test // GH-2594
	void createRedisConfigurationWithValidRedisUriString() {

		RedisConfiguration redisConfiguration = LettuceConnectionFactory.createRedisConfiguration("redis://skullbox:6789");

		assertThat(redisConfiguration).isInstanceOf(RedisStandaloneConfiguration.class);

		assertThat(redisConfiguration).asInstanceOf(InstanceOfAssertFactories.type(RedisStandaloneConfiguration.class))
				.extracting(RedisStandaloneConfiguration::getHostName).isEqualTo("skullbox");

		assertThat(redisConfiguration).asInstanceOf(InstanceOfAssertFactories.type(RedisStandaloneConfiguration.class))
				.extracting(RedisStandaloneConfiguration::getPort).isEqualTo(6789);
	}

	@Test // GH-2866
	void earlyStartupDoesNotStartConnectionFactory() {

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(new RedisStandaloneConfiguration(),
				LettuceTestClientConfiguration.defaultConfiguration());
		connectionFactory.setEarlyStartup(false);
		connectionFactory.afterPropertiesSet();

		assertThat(connectionFactory.isEarlyStartup()).isFalse();
		assertThat(connectionFactory.isAutoStartup()).isTrue();
		assertThat(connectionFactory.isRunning()).isFalse();

		AbstractRedisClient client = (AbstractRedisClient) getField(connectionFactory, "client");
		assertThat(client).isNull();
	}

	static class CustomRedisConfiguration implements RedisConfiguration, WithHostAndPort {

		private String hostName;
		private int port;

		CustomRedisConfiguration(String hostName) {
			this(hostName, 6379);
		}

		CustomRedisConfiguration(String hostName, int port) {
			this.hostName = hostName;
			this.port = port;
		}

		@Override
		public String getHostName() {
			return this.hostName;
		}

		@Override
		public void setHostName(String hostName) {
			this.hostName = hostName;
		}

		@Override
		public int getPort() {
			return this.port;
		}

		@Override
		public void setPort(int port) {
			this.port = port;
		}

		@Override
		public boolean equals(Object obj) {

			if (this == obj) {
				return true;
			}

			if (!(obj instanceof CustomRedisConfiguration that)) {
				return false;
			}

			return Objects.equals(this.getHostName(), that.getHostName()) && Objects.equals(this.getPort(), that.getPort());
		}

		@Override
		public int hashCode() {
			return Objects.hash(getHostName(), getPort());
		}

		@Override
		public String toString() {

			return "CustomRedisConfiguration{" + "hostName='" + hostName + '\'' + ", port=" + port + '}';
		}
	}
}
