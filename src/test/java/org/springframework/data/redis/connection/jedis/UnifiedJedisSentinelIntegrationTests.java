/*
 * Copyright 2025-present the original author or authors.
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
package org.springframework.data.redis.connection.jedis;

import static org.assertj.core.api.Assertions.*;

import redis.clients.jedis.RedisSentinelClient;

import java.util.Collection;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.connection.AbstractConnectionIntegrationTests;
import org.springframework.data.redis.connection.RedisSentinelConnection;
import org.springframework.data.redis.connection.RedisServer;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.test.condition.EnabledOnRedisSentinelAvailable;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

/**
 * Integration tests for {@link UnifiedJedisConnection} with Redis Sentinel using
 * the modern {@link RedisSentinelClient} API.
 *
 * @author Tihomir Mateev
 * @since 4.1
 * @see JedisSentinelIntegrationTests
 */
@ExtendWith(SpringExtension.class)
@ContextConfiguration
@EnabledOnRedisSentinelAvailable
public class UnifiedJedisSentinelIntegrationTests extends AbstractConnectionIntegrationTests {

	private static final RedisServer REPLICA_0 = new RedisServer("127.0.0.1", 6380);
	private static final RedisServer REPLICA_1 = new RedisServer("127.0.0.1", 6381);

	@AfterEach
	public void tearDown() {
		try {
			connection.serverCommands().flushAll();
		} catch (Exception ignore) {
			// Jedis leaves some incomplete data in OutputStream on NPE caused by null key/value tests
		}

		try {
			connection.close();
		} catch (Exception ignore) {}

		connection = null;
	}

	@Test
	void testConnectionIsUnifiedJedisConnection() {
		assertThat(byteConnection).isInstanceOf(UnifiedJedisConnection.class);
	}

	@Test
	void testNativeConnectionIsRedisSentinelClient() {
		assertThat(byteConnection.getNativeConnection()).isInstanceOf(RedisSentinelClient.class);
	}

	@Test
	void shouldReadMastersCorrectly() {
		List<RedisServer> servers = (List<RedisServer>) connectionFactory.getSentinelConnection().masters();
		assertThat(servers).hasSize(1);
		assertThat(servers.get(0).getName()).isEqualTo(SettingsUtils.getSentinelMaster());
	}

	@Test
	void shouldReadReplicaOfMastersCorrectly() {
		RedisSentinelConnection sentinelConnection = connectionFactory.getSentinelConnection();

		List<RedisServer> servers = (List<RedisServer>) sentinelConnection.masters();
		assertThat(servers).hasSize(1);

		Collection<RedisServer> replicas = sentinelConnection.replicas(servers.get(0));
		assertThat(replicas).hasSize(2).contains(REPLICA_0, REPLICA_1);
	}

	@Test
	void shouldSetClientName() {
		assertThat(connection.getClientName()).isEqualTo("unified-jedis-sentinel-client");
	}

	@Test
	public void testEvalReturnSingleError() {
		assertThatExceptionOfType(InvalidDataAccessApiUsageException.class)
				.isThrownBy(() -> connection.eval("return redis.call('expire','foo')", ReturnType.BOOLEAN, 0));
	}

	@Test
	public void testEvalArrayScriptError() {
		assertThatExceptionOfType(InvalidDataAccessApiUsageException.class)
				.isThrownBy(() -> connection.eval("return {1,2", ReturnType.MULTI, 1, "foo", "bar"));
	}

	@Test
	public void testEvalShaNotFound() {
		assertThatExceptionOfType(InvalidDataAccessApiUsageException.class)
				.isThrownBy(() -> connection.evalSha("somefakesha", ReturnType.VALUE, 2, "key1", "key2"));
	}

	@Test
	public void testEvalShaArrayError() {
		assertThatExceptionOfType(InvalidDataAccessApiUsageException.class)
				.isThrownBy(() -> connection.evalSha("notasha", ReturnType.MULTI, 1, "key1", "arg1"));
	}

	@Test
	public void testRestoreBadData() {
		assertThatExceptionOfType(InvalidDataAccessApiUsageException.class)
				.isThrownBy(() -> connection.restore("testing".getBytes(), 0, "foo".getBytes()));
	}

	@Test
	@Disabled
	@Override
	public void testRestoreExistingKey() {}

	/**
	 * SELECT is not supported with pooled connections because it contaminates the pool.
	 */
	@Test
	@Disabled("SELECT is not supported with pooled connections")
	@Override
	public void testSelect() {}

	/**
	 * MOVE uses SELECT internally and is not supported with pooled connections.
	 */
	@Test
	@Disabled("MOVE is not supported with pooled connections")
	@Override
	public void testMove() {}

	/**
	 * setClientName is not supported with pooled connections - configure via JedisConnectionFactory.
	 */
	@Test
	@Disabled("setClientName is not supported with pooled connections")
	@Override
	public void clientSetNameWorksCorrectly() {}

	@Test
	public void testExecWithoutMulti() {
		assertThatExceptionOfType(InvalidDataAccessApiUsageException.class).isThrownBy(() -> connection.exec());
	}

	@Test
	public void testErrorInTx() {
		assertThatExceptionOfType(InvalidDataAccessApiUsageException.class).isThrownBy(() -> {
			connection.multi();
			connection.set("foo", "bar");
			// Try to do a list op on a value
			connection.lPop("foo");
			connection.exec();
			getResults();
		});
	}
}

