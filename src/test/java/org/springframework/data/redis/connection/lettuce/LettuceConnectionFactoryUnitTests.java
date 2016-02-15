/*
 * Copyright 2015-2016 the original author or authors.
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
import static org.junit.Assert.*;
import static org.springframework.test.util.ReflectionTestUtils.*;

import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.data.redis.ConnectionFactoryTracker;
import org.springframework.data.redis.connection.RedisClusterConfiguration;

import com.lambdaworks.redis.AbstractRedisClient;
import com.lambdaworks.redis.RedisURI;
import com.lambdaworks.redis.cluster.RedisClusterClient;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
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

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void shouldInitClientCorrectlyWhenClusterConfigPresent() {

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(clusterConfig);
		connectionFactory.setClientResources(TestClientResources.get());
		connectionFactory.afterPropertiesSet();
		ConnectionFactoryTracker.add(connectionFactory);

		assertThat(getField(connectionFactory, "client"), instanceOf(RedisClusterClient.class));
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	@SuppressWarnings("unchecked")
	public void timeoutShouldBeSetCorrectlyOnClusterClient() {

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(clusterConfig);
		connectionFactory.setClientResources(TestClientResources.get());
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

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	@SuppressWarnings("unchecked")
	public void passwordShouldBeSetCorrectlyOnClusterClient() {

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(clusterConfig);
		connectionFactory.setClientResources(TestClientResources.get());
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

	/**
	 * @see DATAREDIS-462
	 */
	@Test
	@SuppressWarnings("unchecked")
	public void clusterClientShouldInitializeWithoutClientResources() {

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(clusterConfig);
		connectionFactory.setShutdownTimeout(0);
		connectionFactory.afterPropertiesSet();
		ConnectionFactoryTracker.add(connectionFactory);

		AbstractRedisClient client = (AbstractRedisClient) getField(connectionFactory, "client");
		assertThat(client, instanceOf(RedisClusterClient.class));
	}
}
