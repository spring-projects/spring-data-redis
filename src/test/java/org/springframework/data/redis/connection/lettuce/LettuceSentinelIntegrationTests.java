/*
 * Copyright 2015-2023 the original author or authors.
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

import io.lettuce.core.ReadFrom;
import reactor.test.StepVerifier;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.springframework.data.redis.ConnectionFactoryTracker;
import org.springframework.data.redis.connection.AbstractConnectionIntegrationTests;
import org.springframework.data.redis.connection.DefaultStringRedisConnection;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisSentinelConfiguration;
import org.springframework.data.redis.connection.RedisSentinelConnection;
import org.springframework.data.redis.connection.RedisServer;
import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.data.redis.connection.lettuce.extension.LettuceConnectionFactoryExtension;
import org.springframework.data.redis.test.condition.EnabledOnRedisSentinelAvailable;
import org.springframework.data.redis.test.extension.LettuceTestClientResources;
import org.springframework.data.redis.test.extension.RedisSentinel;

/**
 * Integration tests for Lettuce and Redis Sentinel interaction.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 */
@ExtendWith(LettuceConnectionFactoryExtension.class)
@EnabledOnRedisSentinelAvailable
public class LettuceSentinelIntegrationTests extends AbstractConnectionIntegrationTests {

	private static final String MASTER_NAME = "mymaster";
	private static final RedisServer SENTINEL_0 = new RedisServer("127.0.0.1", 26379);
	private static final RedisServer SENTINEL_1 = new RedisServer("127.0.0.1", 26380);

	private static final RedisServer REPLICA_0 = new RedisServer("127.0.0.1", 6380);
	private static final RedisServer REPLICA_1 = new RedisServer("127.0.0.1", 6381);

	private static final RedisSentinelConfiguration SENTINEL_CONFIG;
	static {

		SENTINEL_CONFIG = new RedisSentinelConfiguration() //
				.master(MASTER_NAME).sentinel(SENTINEL_0).sentinel(SENTINEL_1);

		SENTINEL_CONFIG.setDatabase(5);
	}

	public LettuceSentinelIntegrationTests(@RedisSentinel LettuceConnectionFactory connectionFactory) {
		this.connectionFactory = connectionFactory;
	}

	@AfterEach
	public void tearDown() {

		try {

			// since we use more than one db we're required to flush them all
			connection.flushAll();
		} catch (Exception e) {
			// Connection may be closed in certain cases, like after pub/sub
			// tests
		}
		connection.close();
	}

	@Test
	@Disabled("SELECT not allowed on shared connections")
	@Override
	public void testMove() {}

	@Test
	@Disabled("SELECT not allowed on shared connections")
	@Override
	public void testSelect() {
		super.testSelect();
	}

	@Test // DATAREDIS-348
	void shouldReadMastersCorrectly() {

		List<RedisServer> servers = (List<RedisServer>) connectionFactory.getSentinelConnection().masters();
		assertThat(servers.size()).isEqualTo(1);
		assertThat(servers.get(0).getName()).isEqualTo(MASTER_NAME);
	}

	@Test // DATAREDIS-842, DATAREDIS-973
	void shouldUseSpecifiedDatabase() {

		RedisConnection connection = connectionFactory.getConnection();

		connection.flushAll();
		connection.set("foo".getBytes(), "bar".getBytes());
		connection.close();

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory();
		connectionFactory.setClientResources(LettuceTestClientResources.getSharedClientResources());
		connectionFactory.setShutdownTimeout(0);
		connectionFactory.setShareNativeConnection(false);
		connectionFactory.setDatabase(5);
		connectionFactory.afterPropertiesSet();

		RedisConnection directConnection = connectionFactory.getConnection();
		assertThat(directConnection.exists("foo".getBytes())).isFalse();
		directConnection.select(0);

		assertThat(directConnection.exists("foo".getBytes())).isTrue();
		directConnection.close();
		connectionFactory.destroy();
	}

	@Test // DATAREDIS-973
	void reactiveShouldUseSpecifiedDatabase() {

		RedisConnection connection = connectionFactory.getConnection();

		connection.flushAll();
		connection.set("foo".getBytes(), "bar".getBytes());

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory();
		connectionFactory.setClientResources(LettuceTestClientResources.getSharedClientResources());
		connectionFactory.setShutdownTimeout(0);
		connectionFactory.setShareNativeConnection(false);
		connectionFactory.setDatabase(5);
		connectionFactory.afterPropertiesSet();

		LettuceReactiveRedisConnection reactiveConnection = connectionFactory.getReactiveConnection();

		reactiveConnection.keyCommands().exists(ByteBuffer.wrap("foo".getBytes())) //
				.as(StepVerifier::create) //
				.expectNext(false) //
				.verifyComplete();

		reactiveConnection.close();
		connectionFactory.destroy();
	}

	@Test
	// DATAREDIS-348
	void shouldReadReplicasOfMastersCorrectly() {

		RedisSentinelConnection sentinelConnection = connectionFactory.getSentinelConnection();

		List<RedisServer> servers = (List<RedisServer>) sentinelConnection.masters();
		assertThat(servers.size()).isEqualTo(1);

		Collection<RedisServer> replicas = sentinelConnection.replicas(servers.get(0));
		assertThat(replicas).containsAnyOf(REPLICA_0, REPLICA_1);
	}

	@Test // DATAREDIS-462
	@Disabled("Until Lettuce has moved to Sinks API")
	void factoryWorksWithoutClientResources() {

		LettuceConnectionFactory factory = new LettuceConnectionFactory(SENTINEL_CONFIG);
		factory.setShutdownTimeout(0);
		factory.afterPropertiesSet();

		ConnectionFactoryTracker.add(factory);

		StringRedisConnection connection = new DefaultStringRedisConnection(factory.getConnection());

		try {
			assertThat(connection.ping()).isEqualTo("PONG");
		} finally {
			connection.close();
		}
	}

	@Test // DATAREDIS-576
	void connectionAppliesClientName() {

		LettuceClientConfiguration clientName = LettuceClientConfiguration.builder()
				.clientResources(LettuceTestClientResources.getSharedClientResources()).clientName("clientName").build();

		LettuceConnectionFactory factory = new LettuceConnectionFactory(SENTINEL_CONFIG, clientName);
		factory.afterPropertiesSet();

		ConnectionFactoryTracker.add(factory);

		StringRedisConnection connection = new DefaultStringRedisConnection(factory.getConnection());

		try {
			assertThat(connection.getClientName()).isEqualTo("clientName");
		} finally {
			connection.close();
		}
	}

	@Test // DATAREDIS-580
	void factoryWithReadFromMasterSettings() {

		LettuceConnectionFactory factory = new LettuceConnectionFactory(SENTINEL_CONFIG,
				LettuceTestClientConfiguration.builder().readFrom(ReadFrom.MASTER).build());
		factory.afterPropertiesSet();

		ConnectionFactoryTracker.add(factory);

		StringRedisConnection connection = new DefaultStringRedisConnection(factory.getConnection());

		try {
			assertThat(connection.ping()).isEqualTo("PONG");
			assertThat(connection.info().getProperty("role")).isEqualTo("master");
		} finally {
			connection.close();
		}
	}

	@Test // DATAREDIS-580
	void factoryWithReadFromReplicaSettings() {

		LettuceConnectionFactory factory = new LettuceConnectionFactory(SENTINEL_CONFIG,
				LettuceTestClientConfiguration.builder().readFrom(ReadFrom.REPLICA).build());
		factory.afterPropertiesSet();

		ConnectionFactoryTracker.add(factory);

		StringRedisConnection connection = new DefaultStringRedisConnection(factory.getConnection());

		try {
			assertThat(connection.ping()).isEqualTo("PONG");
			assertThat(connection.info().getProperty("role")).isEqualTo("slave");
		} finally {
			connection.close();
		}
	}

	@Test // DATAREDIS-580
	void factoryUsesMasterReplicaConnections() {

		LettuceClientConfiguration configuration = LettuceTestClientConfiguration.builder().readFrom(ReadFrom.SLAVE)
				.build();

		LettuceConnectionFactory factory = new LettuceConnectionFactory(SENTINEL_CONFIG, configuration);
		factory.afterPropertiesSet();

		RedisConnection connection = factory.getConnection();

		try {
			assertThat(connection.ping()).isEqualTo("PONG");
			assertThat(connection.info().getProperty("role")).isEqualTo("slave");
		} finally {
			connection.close();
		}

		factory.destroy();
	}
}
