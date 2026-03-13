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
import org.springframework.data.redis.connection.AbstractConnectionTransactionIntegrationTests;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

/**
 * Integration test of {@link UnifiedJedisConnection} transaction functionality.
 * <p>
 *
 * @author Tihomir Mateev
 * @since 4.1
 * @see UnifiedJedisConnection
 * @see JedisConnectionTransactionIntegrationTests
 */
@ExtendWith(SpringExtension.class)
@ContextConfiguration("StandardJedisConnectionIntegrationTests-context.xml")
public class UnifiedJedisConnectionTransactionIntegrationTests extends AbstractConnectionTransactionIntegrationTests {

	@AfterEach
	public void tearDown() {
		try {
			// Make sure any open transaction is properly closed
			if (connection.isQueueing()) {
				try {
					connection.discard();
				} catch (Exception ignore) {
					// Ignore errors from transaction cleanup
				}
			}
		} catch (Exception ignore) {
			// Ignore transaction errors
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
	void testConnectionIsStandardJedisConnection() {
		assertThat(byteConnection).isInstanceOf(UnifiedJedisConnection.class);
	}

	@Test
    @Disabled("Jedis issue: Transaction tries to return String instead of List<String>")
	public void testGetConfig() {}

	@Test
	public void testEvalShaNotFound() {
		assertThatExceptionOfType(InvalidDataAccessApiUsageException.class).isThrownBy(() -> {
			connection.evalSha("somefakesha", ReturnType.VALUE, 2, "key1", "key2");
			getResults();
		});
	}

	@Test
	public void testEvalShaArrayError() {
		assertThatExceptionOfType(InvalidDataAccessApiUsageException.class).isThrownBy(() -> {
			connection.evalSha("notasha", ReturnType.MULTI, 1, "key1", "arg1");
			getResults();
		});
	}

	@Test
	public void testEvalArrayScriptError() {
		assertThatExceptionOfType(InvalidDataAccessApiUsageException.class).isThrownBy(() -> {
			connection.eval("return {1,2", ReturnType.MULTI, 1, "foo", "bar");
			getResults();
		});
	}

	@Test
	public void testEvalReturnSingleError() {
		assertThatExceptionOfType(InvalidDataAccessApiUsageException.class).isThrownBy(() -> {
			connection.eval("return redis.call('expire','foo')", ReturnType.BOOLEAN, 0);
			getResults();
		});
	}

	// Unsupported Ops
	@Test
	@Disabled
	public void testInfoBySection() {}

	@Test
	@Disabled
	public void testRestoreBadData() {}

	@Test
	@Disabled
	public void testRestoreExistingKey() {}

	@Test // DATAREDIS-269
	@Disabled
	public void clientSetNameWorksCorrectly() {}

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
	public void testMove() {}

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

	@Test
	@Override
	@Disabled
	public void testSelect() {}

	@Test
	@Override
	@Disabled("Parameter ordering in zrevrangeByLex(byte[] key, byte[] max, byte[] min) is swapped so transactions use inverse parameter order")
	public void zRevRangeByLexTest() {}
}

