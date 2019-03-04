/*
 * Copyright 2015-2019 the original author or authors.
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
import static org.springframework.data.redis.connection.ClusterTestVariables.*;
import static org.springframework.data.redis.connection.lettuce.LettuceTestClientResources.*;
import static org.springframework.test.util.ReflectionTestUtils.*;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.springframework.data.redis.ConnectionFactoryTracker;
import org.springframework.data.redis.connection.RedisClusterConfiguration;
import org.springframework.data.redis.connection.RedisClusterConnection;
import org.springframework.data.redis.connection.RedisSentinelConfiguration;
import org.springframework.test.util.ReflectionTestUtils;

import com.lambdaworks.redis.AbstractRedisClient;
import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisURI;
import com.lambdaworks.redis.cluster.RedisClusterClient;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Balázs Németh
 * @author Dmytro Liash
 */
public class LettuceConnectionFactoryUnitTests {

	RedisClusterConfiguration clusterConfig;

	@Before
	public void setUp() {
		clusterConfig = new RedisClusterConfiguration().clusterNode("127.0.0.1", 6379).clusterNode("127.0.0.1", 6380);
	}

	@After
	public void tearDown() throws Exception {
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
			assertThat(uri.getTimeout(), is(equalTo(connectionFactory.getTimeout())));
			assertThat(uri.getUnit(), is(equalTo(TimeUnit.MILLISECONDS)));
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

	@Test // DATAREDIS-676
	public void timeoutShouldBePassedOnToClusterConnection() {

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(clusterConfig);
		connectionFactory.setShutdownTimeout(0);
		connectionFactory.setTimeout(2000);
		connectionFactory.afterPropertiesSet();
		ConnectionFactoryTracker.add(connectionFactory);

		RedisClusterConnection clusterConnection = connectionFactory.getClusterConnection();
		assertThat((Long) ReflectionTestUtils.getField(clusterConnection, "timeout"), is(equalTo(2000L)));

		clusterConnection.close();
	}

	@Test // DATAREDIS-939
	public void timeoutShouldBePassedOnToSentinelURI() {

		RedisSentinelConfiguration sentinelConfiguration = new RedisSentinelConfiguration().master("test")
				.sentinel("sentinel", 26379);
		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(sentinelConfiguration);
		connectionFactory.setTimeout(5);
		connectionFactory.afterPropertiesSet();
		ConnectionFactoryTracker.add(connectionFactory);

		AbstractRedisClient client = (AbstractRedisClient) getField(connectionFactory, "client");
		assertThat(client, instanceOf(RedisClient.class));
		RedisURI redisUri = (RedisURI) getField(client, "redisURI");
		assertThat(redisUri, is(notNullValue()));
		assertThat(redisUri.getTimeout(), is(equalTo(5L)));
		assertThat(redisUri.getUnit(), is(equalTo(TimeUnit.MILLISECONDS)));
	}
}
