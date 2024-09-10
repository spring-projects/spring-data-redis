/*
 * Copyright 2011-2024 the original author or authors.
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

import redis.clients.jedis.JedisPoolConfig;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.connection.AbstractConnectionPipelineIntegrationTests;
import org.springframework.data.redis.util.ConnectionVerifier;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

/**
 * Integration test of {@link JedisConnection} pipeline functionality
 *
 * @author Jennifer Hickey
 * @author Christoph Strobl
 * @author Thomas Darimont
 * @author Mark Paluch
 * @author Ivan Kripakov
 */
@ExtendWith(SpringExtension.class)
@ContextConfiguration("JedisConnectionIntegrationTests-context.xml")
public class JedisConnectionPipelineIntegrationTests extends AbstractConnectionPipelineIntegrationTests {

	@AfterEach
	public void tearDown() {
		try {
			connection.flushAll();
			connection.close();
		} catch (Exception ignore) {
			// Jedis leaves some incomplete data in OutputStream on NPE caused by null key/value tests
			// Attempting to close the connection will result in error on sending QUIT to Redis
		}
		connection = null;
	}

	@Test
	// DATAREDIS-213 - Verify connection returns to pool after select
	public void testClosePoolPipelinedDbSelect() {

		JedisPoolConfig config = new JedisPoolConfig();
		config.setMaxTotal(1);
		config.setMaxIdle(1);
		JedisConnectionFactory factory2 = new JedisConnectionFactory(config);
		factory2.setHostName(SettingsUtils.getHost());
		factory2.setPort(SettingsUtils.getPort());
		factory2.setDatabase(1);

		ConnectionVerifier.create(factory2) //
				.execute(conn2 -> conn2.openPipeline()) //
				.verifyAndRun(connectionFactory -> {
					connectionFactory.getConnection();
					connectionFactory.destroy();
				});
	}

	// Unsupported Ops
	@Test
	public void testScriptExists() {
		assertThatExceptionOfType(InvalidDataAccessApiUsageException.class).isThrownBy(super::testScriptExists);
	}

	@Test
	public void testScriptKill() {
		assertThatExceptionOfType(InvalidDataAccessApiUsageException.class).isThrownBy(() -> connection.scriptKill());
	}

	@Test
	@Disabled
	public void testScriptFlush() {}

	@Test // DATAREDIS-269
	public void clientSetNameWorksCorrectly() {
		assertThatExceptionOfType(InvalidDataAccessApiUsageException.class).isThrownBy(super::clientSetNameWorksCorrectly);
	}

	@Test
	@Override
	// DATAREDIS-268
	public void testListClientsContainsAtLeastOneElement() {
		assertThatExceptionOfType(InvalidDataAccessApiUsageException.class)
				.isThrownBy(super::testListClientsContainsAtLeastOneElement);
	}

	@Test // DATAREDIS-296
	@Disabled
	public void testExecWithoutMulti() {}

	@Test
	@Override
	@Disabled
	public void testMultiExec() {}

	@Test
	@Override
	@Disabled
	public void testMultiDiscard() {}

	@Test
	@Override
	@Disabled
	public void testErrorInTx() {}

	@Test
	@Override
	@Disabled
	public void testWatch() {}

	@Test
	@Override
	@Disabled
	public void testUnwatch() {}

	@Test
	@Override
	@Disabled
	public void testMultiAlreadyInTx() {}

	@Test
	@Override
	@Disabled
	public void testPingPong() {}

	@Test
	@Override
	@Disabled
	public void testFlushDb() {}

	@Override
	@Disabled
	public void testEcho() {}

	@Override
	@Disabled
	public void testInfo() {}

	@Override
	@Disabled
	public void testInfoBySection() {}

	@Override
	@Disabled
	public void testMove() {}

	@Test
	@Override
	@Disabled
	public void testGetConfig() {}

	@Test
	@Override
	@Disabled
	public void testLastSave() {}

	@Test
	@Override
	@Disabled
	public void testGetTimeShouldRequestServerTime() {}

	@Test
	@Override
	@Disabled
	public void testGetTimeShouldRequestServerTimeAsMicros() {}

	@Test
	@Override
	@Disabled
	public void testDbSize() {}

}
