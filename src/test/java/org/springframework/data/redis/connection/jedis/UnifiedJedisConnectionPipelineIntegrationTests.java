/*
 * Copyright 2011-present the original author or authors.
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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.connection.AbstractConnectionPipelineIntegrationTests;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

/**
 * Integration test of {@link UnifiedJedisConnection} pipeline functionality.
 * <p>
 *
 * @author Tihomir Mateev
 * @since 4.1
 * @see UnifiedJedisConnection
 * @see JedisConnectionPipelineIntegrationTests
 */
@ExtendWith(SpringExtension.class)
@ContextConfiguration("UnifiedJedisConnectionIntegrationTests-context.xml")
public class UnifiedJedisConnectionPipelineIntegrationTests extends AbstractConnectionPipelineIntegrationTests {

	@AfterEach
	public void tearDown() {
		try {
			// Close pipeline first to ensure any queued commands are executed/cleared
			// and the pipeline's connection is returned to the pool
			if (connection.isPipelined()) {
				try {
					connection.closePipeline();
				} catch (Exception ignore) {
					// Ignore errors from incomplete pipeline commands
				}
			}
		} catch (Exception ignore) {
			// Ignore pipeline errors
		}

		try {
			connection.serverCommands().flushAll();
		} catch (Exception ignore) {
			// Jedis leaves some incomplete data in OutputStream on NPE caused by null key/value tests
		}

		try {
			connection.close();
		} catch (Exception ignore) {
			// Attempting to close the connection will result in error on sending QUIT to Redis
		}
		connection = null;
	}

	@Test
	void testConnectionIsUnifiedJedisConnection() {
		assertThat(byteConnection).isInstanceOf(UnifiedJedisConnection.class);
	}

	// Unsupported Ops
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

	@Test
    @Override
	@Disabled
	public void testEcho() {}

	@Test
    @Override
	@Disabled
	public void testInfo() {}

	@Test
	@Override
	@Disabled
	public void testInfoBySection() {}

	@Test
	@Override
	@Disabled("SELECT is not supported with pooled connections")
	public void testSelect() {}

	@Test
	@Override
	@Disabled("MOVE uses SELECT internally and is not supported with pooled connections")
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

