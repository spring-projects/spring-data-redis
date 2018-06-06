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
package org.springframework.data.redis.connection.lettuce;

import static org.hamcrest.core.Is.*;
import static org.hamcrest.core.IsEqual.*;
import static org.hamcrest.core.IsInstanceOf.*;
import static org.hamcrest.core.IsNull.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static org.springframework.data.redis.connection.ClusterTestVariables.*;
import static org.springframework.data.redis.connection.lettuce.LettuceTestClientResources.*;
import static org.springframework.test.util.ReflectionTestUtils.*;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.resource.ClientResources;

import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.Collections;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.springframework.beans.DirectFieldAccessor;
import org.springframework.data.redis.ConnectionFactoryTracker;
import org.springframework.data.redis.connection.RedisClusterConfiguration;
import org.springframework.data.redis.connection.RedisClusterConnection;
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

		assertThat(getField(connectionFactory, "client"), instanceOf(RedisClusterClient.class));
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
		assertThat(client, instanceOf(RedisClusterClient.class));

		Iterable<RedisURI> initialUris = (Iterable<RedisURI>) getField(client, "initialUris");

		for (RedisURI uri : initialUris) {
			assertThat(uri.getTimeout(), is(equalTo(Duration.ofMillis(connectionFactory.getTimeout()))));
		}
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
		assertThat(client, instanceOf(RedisClusterClient.class));

		Iterable<RedisURI> initialUris = (Iterable<RedisURI>) getField(client, "initialUris");

		for (RedisURI uri : initialUris) {
			assertThat(uri.getPassword(), is(equalTo(connectionFactory.getPassword().toCharArray())));
		}
	}

	@Test // DATAREDIS-524
	public void passwordShouldBeSetCorrectlyOnSentinelClient() {

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(
				new RedisSentinelConfiguration("mymaster", Collections.singleton("host:1234")));
		connectionFactory.setClientResources(getSharedClientResources());
		connectionFactory.setPassword("o_O");
		connectionFactory.afterPropertiesSet();
		ConnectionFactoryTracker.add(connectionFactory);

		AbstractRedisClient client = (AbstractRedisClient) getField(connectionFactory, "client");
		assertThat(client, instanceOf(RedisClient.class));

		RedisURI redisUri = (RedisURI) getField(client, "redisURI");

		assertThat(redisUri.getPassword(), is(equalTo(connectionFactory.getPassword().toCharArray())));
	}

	@Test // DATAREDIS-462
	public void clusterClientShouldInitializeWithoutClientResources() {

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(clusterConfig);
		connectionFactory.setShutdownTimeout(0);
		connectionFactory.afterPropertiesSet();
		ConnectionFactoryTracker.add(connectionFactory);

		AbstractRedisClient client = (AbstractRedisClient) getField(connectionFactory, "client");
		assertThat(client, instanceOf(RedisClusterClient.class));
	}

	@Test // DATAREDIS-480
	public void sslOptionsShouldBeDisabledByDefaultOnClient() {

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory();
		connectionFactory.setClientResources(getSharedClientResources());
		connectionFactory.afterPropertiesSet();
		ConnectionFactoryTracker.add(connectionFactory);

		AbstractRedisClient client = (AbstractRedisClient) getField(connectionFactory, "client");
		assertThat(client, instanceOf(RedisClient.class));

		RedisURI redisUri = (RedisURI) getField(client, "redisURI");

		assertThat(redisUri.isSsl(), is(false));
		assertThat(connectionFactory.isUseSsl(), is(false));
		assertThat(redisUri.isStartTls(), is(false));
		assertThat(connectionFactory.isStartTls(), is(false));
		assertThat(redisUri.isVerifyPeer(), is(true));
		assertThat(connectionFactory.isVerifyPeer(), is(true));
	}

	@Test // DATAREDIS-476
	public void sslShouldBeSetCorrectlyOnClient() {

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory();
		connectionFactory.setClientResources(getSharedClientResources());
		connectionFactory.setUseSsl(true);
		connectionFactory.afterPropertiesSet();
		ConnectionFactoryTracker.add(connectionFactory);

		AbstractRedisClient client = (AbstractRedisClient) getField(connectionFactory, "client");
		assertThat(client, instanceOf(RedisClient.class));

		RedisURI redisUri = (RedisURI) getField(client, "redisURI");

		assertThat(redisUri.isSsl(), is(true));
		assertThat(connectionFactory.isUseSsl(), is(true));
		assertThat(redisUri.isVerifyPeer(), is(true));
		assertThat(connectionFactory.isVerifyPeer(), is(true));
	}

	@Test // DATAREDIS-480
	public void verifyPeerOptionShouldBeSetCorrectlyOnClient() {

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory();
		connectionFactory.setClientResources(getSharedClientResources());
		connectionFactory.setVerifyPeer(false);
		connectionFactory.afterPropertiesSet();
		ConnectionFactoryTracker.add(connectionFactory);

		AbstractRedisClient client = (AbstractRedisClient) getField(connectionFactory, "client");
		assertThat(client, instanceOf(RedisClient.class));

		RedisURI redisUri = (RedisURI) getField(client, "redisURI");

		assertThat(redisUri.isVerifyPeer(), is(false));
		assertThat(connectionFactory.isVerifyPeer(), is(false));
	}

	@Test // DATAREDIS-480
	public void startTLSOptionShouldBeSetCorrectlyOnClient() {

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory();
		connectionFactory.setClientResources(getSharedClientResources());
		connectionFactory.setStartTls(true);
		connectionFactory.afterPropertiesSet();
		ConnectionFactoryTracker.add(connectionFactory);

		AbstractRedisClient client = (AbstractRedisClient) getField(connectionFactory, "client");
		assertThat(client, instanceOf(RedisClient.class));

		RedisURI redisUri = (RedisURI) getField(client, "redisURI");

		assertThat(redisUri.isStartTls(), is(true));
		assertThat(connectionFactory.isStartTls(), is(true));
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
		assertThat(client, instanceOf(RedisClusterClient.class));

		Iterable<RedisURI> initialUris = (Iterable<RedisURI>) getField(client, "initialUris");

		for (RedisURI uri : initialUris) {
			assertThat(uri.isSsl(), is(true));
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
		assertThat(client, instanceOf(RedisClusterClient.class));

		Iterable<RedisURI> initialUris = (Iterable<RedisURI>) getField(client, "initialUris");

		for (RedisURI uri : initialUris) {
			assertThat(uri.isStartTls(), is(true));
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
		assertThat(client, instanceOf(RedisClusterClient.class));

		Iterable<RedisURI> initialUris = (Iterable<RedisURI>) getField(client, "initialUris");

		for (RedisURI uri : initialUris) {
			assertThat(uri.isVerifyPeer(), is(true));
		}
	}

	@Test // DATAREDIS-682
	public void socketShouldBeSetOnStandaloneClient() {

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(new RedisSocketConfiguration());
		connectionFactory.afterPropertiesSet();
		ConnectionFactoryTracker.add(connectionFactory);

		AbstractRedisClient client = (AbstractRedisClient) getField(connectionFactory, "client");
		assertThat(client, instanceOf(RedisClient.class));

		RedisURI redisUri = (RedisURI) getField(client, "redisURI");

		assertThat(redisUri.getSocket(), is("/tmp/redis.sock"));
	}

	@Test // DATAREDIS-574
	public void shouldReadStandalonePassword() {

		RedisStandaloneConfiguration envConfig = new RedisStandaloneConfiguration();
		envConfig.setPassword(RedisPassword.of("foo"));

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(envConfig,
				LettuceClientConfiguration.defaultConfiguration());

		assertThat(connectionFactory.getPassword(), is(equalTo("foo")));
	}

	@Test // DATAREDIS-574
	public void shouldWriteStandalonePassword() {

		RedisStandaloneConfiguration envConfig = new RedisStandaloneConfiguration();
		envConfig.setPassword(RedisPassword.of("foo"));

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(envConfig,
				LettuceClientConfiguration.defaultConfiguration());
		connectionFactory.setPassword("bar");

		assertThat(connectionFactory.getPassword(), is(equalTo("bar")));
		assertThat(envConfig.getPassword(), is(equalTo(RedisPassword.of("bar"))));
	}

	@Test // DATAREDIS-574
	public void shouldReadSentinelPassword() {

		RedisSentinelConfiguration envConfig = new RedisSentinelConfiguration();
		envConfig.setPassword(RedisPassword.of("foo"));

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(envConfig,
				LettuceClientConfiguration.defaultConfiguration());

		assertThat(connectionFactory.getPassword(), is(equalTo("foo")));
	}

	@Test // DATAREDIS-574
	public void shouldWriteSentinelPassword() {

		RedisSentinelConfiguration envConfig = new RedisSentinelConfiguration();
		envConfig.setPassword(RedisPassword.of("foo"));

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(envConfig,
				LettuceClientConfiguration.defaultConfiguration());
		connectionFactory.setPassword("bar");

		assertThat(connectionFactory.getPassword(), is(equalTo("bar")));
		assertThat(envConfig.getPassword(), is(equalTo(RedisPassword.of("bar"))));
	}

	@Test // DATAREDIS-682
	public void shouldWriteSocketPassword() {

		RedisSocketConfiguration envConfig = new RedisSocketConfiguration();
		envConfig.setPassword(RedisPassword.of("foo"));

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(envConfig,
				LettuceClientConfiguration.defaultConfiguration());
		connectionFactory.setPassword("bar");

		assertThat(connectionFactory.getPassword(), is(equalTo("bar")));
		assertThat(envConfig.getPassword(), is(equalTo(RedisPassword.of("bar"))));
	}

	@Test // DATAREDIS-574
	public void shouldReadClusterPassword() {

		RedisClusterConfiguration envConfig = new RedisClusterConfiguration();
		envConfig.setPassword(RedisPassword.of("foo"));

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(envConfig,
				LettuceClientConfiguration.defaultConfiguration());

		assertThat(connectionFactory.getPassword(), is(equalTo("foo")));
	}

	@Test // DATAREDIS-574
	public void shouldWriteClusterPassword() {

		RedisClusterConfiguration envConfig = new RedisClusterConfiguration();
		envConfig.setPassword(RedisPassword.of("foo"));

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(envConfig,
				LettuceClientConfiguration.defaultConfiguration());
		connectionFactory.setPassword("bar");

		assertThat(connectionFactory.getPassword(), is(equalTo("bar")));
		assertThat(envConfig.getPassword(), is(equalTo(RedisPassword.of("bar"))));
	}

	@Test // DATAREDIS-574
	public void shouldReadStandaloneDatabaseIndex() {

		RedisStandaloneConfiguration envConfig = new RedisStandaloneConfiguration();
		envConfig.setDatabase(2);

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(envConfig,
				LettuceClientConfiguration.defaultConfiguration());

		assertThat(connectionFactory.getDatabase(), is(2));
	}

	@Test // DATAREDIS-574
	public void shouldWriteStandaloneDatabaseIndex() {

		RedisStandaloneConfiguration envConfig = new RedisStandaloneConfiguration();
		envConfig.setDatabase(2);

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(envConfig,
				LettuceClientConfiguration.defaultConfiguration());
		connectionFactory.setDatabase(3);

		assertThat(connectionFactory.getDatabase(), is(3));
		assertThat(envConfig.getDatabase(), is(3));
	}

	@Test // DATAREDIS-574
	public void shouldReadSentinelDatabaseIndex() {

		RedisSentinelConfiguration envConfig = new RedisSentinelConfiguration();
		envConfig.setDatabase(2);

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(envConfig,
				LettuceClientConfiguration.defaultConfiguration());

		assertThat(connectionFactory.getDatabase(), is(2));
	}

	@Test // DATAREDIS-574
	public void shouldWriteSentinelDatabaseIndex() {

		RedisSentinelConfiguration envConfig = new RedisSentinelConfiguration();
		envConfig.setDatabase(2);

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(envConfig,
				LettuceClientConfiguration.defaultConfiguration());
		connectionFactory.setDatabase(3);

		assertThat(connectionFactory.getDatabase(), is(3));
		assertThat(envConfig.getDatabase(), is(3));
	}

	@Test // DATAREDIS-682
	public void shouldWriteSocketDatabaseIndex() {

		RedisSocketConfiguration envConfig = new RedisSocketConfiguration();
		envConfig.setDatabase(2);

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(envConfig,
				LettuceClientConfiguration.defaultConfiguration());
		connectionFactory.setDatabase(3);

		assertThat(connectionFactory.getDatabase(), is(3));
		assertThat(envConfig.getDatabase(), is(3));
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

		assertThat(connectionFactory.getClientConfiguration(), is(configuration));

		assertThat(connectionFactory.isUseSsl(), is(true));
		assertThat(connectionFactory.isVerifyPeer(), is(false));
		assertThat(connectionFactory.isStartTls(), is(true));
		assertThat(connectionFactory.getClientResources(), is(sharedClientResources));
		assertThat(connectionFactory.getTimeout(), is(Duration.ofMinutes(5).toMillis()));
		assertThat(connectionFactory.getShutdownTimeout(), is(Duration.ofHours(2).toMillis()));
	}

	@Test // DATAREDIS-574
	public void shouldReturnStandaloneConfiguration() {

		RedisStandaloneConfiguration configuration = new RedisStandaloneConfiguration();
		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(configuration,
				LettuceClientConfiguration.defaultConfiguration());

		assertThat(connectionFactory.getStandaloneConfiguration(), is(configuration));
		assertThat(connectionFactory.getSentinelConfiguration(), is(nullValue()));
		assertThat(connectionFactory.getClusterConfiguration(), is(nullValue()));
	}

	@Test // DATAREDIS-682
	public void shouldReturnSocketConfiguration() {

		RedisSocketConfiguration configuration = new RedisSocketConfiguration("/var/redis/socket");
		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(configuration,
				LettuceClientConfiguration.defaultConfiguration());

		assertThat(connectionFactory.getStandaloneConfiguration(), is(notNullValue()));
		assertThat(connectionFactory.getSocketConfiguration(), is(configuration));
		assertThat(connectionFactory.getSentinelConfiguration(), is(nullValue()));
		assertThat(connectionFactory.getClusterConfiguration(), is(nullValue()));
	}

	@Test // DATAREDIS-574
	public void shouldReturnSentinelConfiguration() {

		RedisSentinelConfiguration configuration = new RedisSentinelConfiguration();
		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(configuration,
				LettuceClientConfiguration.defaultConfiguration());

		assertThat(connectionFactory.getStandaloneConfiguration(), is(notNullValue()));
		assertThat(connectionFactory.getSentinelConfiguration(), is(configuration));
		assertThat(connectionFactory.getClusterConfiguration(), is(nullValue()));
	}

	@Test // DATAREDIS-574
	public void shouldReturnClusterConfiguration() {

		RedisClusterConfiguration configuration = new RedisClusterConfiguration();
		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(configuration,
				LettuceClientConfiguration.defaultConfiguration());

		assertThat(connectionFactory.getStandaloneConfiguration(), is(notNullValue()));
		assertThat(connectionFactory.getSentinelConfiguration(), is(nullValue()));
		assertThat(connectionFactory.getClusterConfiguration(), is(configuration));
	}

	@Test(expected = IllegalStateException.class) // DATAREDIS-574
	public void shouldDenyChangesToImmutableClientConfiguration() throws NoSuchAlgorithmException {

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(new RedisStandaloneConfiguration(),
				LettuceClientConfiguration.defaultConfiguration());

		connectionFactory.setUseSsl(false);
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
		assertThat(ReflectionTestUtils.getField(clusterConnection, "timeout"), is(equalTo(2000L)));

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
		assertThat(ReflectionTestUtils.getField(clusterConnection, "timeout"), is(equalTo(2000L)));

		clusterConnection.close();
	}

	@Test // DATAREDIS-731
	public void shouldShareNativeConnectionWithCluster() {

		RedisClusterClient clientMock = mock(RedisClusterClient.class);
		StatefulRedisClusterConnection<byte[], byte[]> connectionMock = mock(StatefulRedisClusterConnection.class);
		when(clientMock.connect(ByteArrayCodec.INSTANCE)).thenReturn(connectionMock);

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

		verify(clientMock).connect(ArgumentMatchers.any(RedisCodec.class));
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
		assertThat(client, instanceOf(RedisClient.class));

		RedisURI redisUri = (RedisURI) getField(client, "redisURI");

		assertThat(redisUri.getDatabase(), is(equalTo(1)));
	}
}
