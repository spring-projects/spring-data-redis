/*
 * Copyright 2011-2022 the original author or authors.
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

import org.springframework.data.redis.connection.AbstractConnectionTransactionIntegrationTests;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

/**
 * Integration test of {@link JedisConnection} transaction functionality.
 * <p>
 * Each method of {@link JedisConnection} behaves differently if executed with a transaction (i.e. between multi and
 * exec or discard calls), so this test covers those branching points
 *
 * @author Jennifer Hickey
 * @author Mark Paluch
 */
@ExtendWith(SpringExtension.class)
@ContextConfiguration("JedisConnectionIntegrationTests-context.xml")
public class JedisConnectionTransactionIntegrationTests extends AbstractConnectionTransactionIntegrationTests {

	@AfterEach
	public void tearDown() {
		try {
			connection.flushAll();
			connection.close();
		} catch (Exception e) {
			// Jedis leaves some incomplete data in OutputStream on NPE caused
			// by null key/value tests
			// Attempting to close the connection will result in error on
			// sending QUIT to Redis
		}
		connection = null;
	}

	@Disabled("Jedis issue: Transaction tries to return String instead of List<String>")
	public void testGetConfig() {}

	// Unsupported Ops
	@Test
	@Disabled
	public void testScriptLoadEvalSha() {
	}

	@Test
	@Disabled
	public void testEvalShaArrayStrings() {
	}

	@Test
	@Disabled
	public void testEvalShaArrayBytes() {
	}

	@Test
	@Disabled
	public void testEvalShaNotFound() {
	}

	@Test
	public void testEvalShaArrayError() {
		assertThatExceptionOfType(UnsupportedOperationException.class)
				.isThrownBy(() -> connection.evalSha("notasha", ReturnType.MULTI, 1, "key1", "arg1"));
	}

	@Test
	public void testEvalArrayScriptError() {
		assertThatExceptionOfType(UnsupportedOperationException.class)
				.isThrownBy(() -> connection.eval("return {1,2", ReturnType.MULTI, 1, "foo", "bar"));
	}

	@Test
	@Disabled
	public void testEvalReturnString() {
	}

	@Test
	@Disabled
	public void testEvalReturnNumber() {
	}

	@Test
	@Disabled
	public void testEvalReturnSingleOK() {
	}

	@Test
	@Disabled
	public void testEvalReturnSingleError() {
	}

	@Test
	@Disabled
	public void testEvalReturnFalse() {
	}

	@Test
	@Disabled
	public void testEvalReturnTrue() {
	}

	@Test
	@Disabled
	public void testEvalReturnArrayStrings() {
	}

	@Test
	@Disabled
	public void testEvalReturnArrayNumbers() {
	}

	@Test
	@Disabled
	public void testEvalReturnArrayOKs() {
	}

	@Test
	@Disabled
	public void testEvalReturnArrayFalses() {
	}

	@Test
	@Disabled
	public void testEvalReturnArrayTrues() {
	}

	@Test
	@Disabled
	public void testScriptExists() {
	}

	@Test
	@Disabled
	public void testScriptKill() {
	}

	@Test
	@Disabled
	public void testScriptFlush() {
	}

	@Test
	@Disabled
	public void testInfoBySection() {
	}

	@Test
	@Disabled
	public void testRestoreBadData() {
	}

	@Test
	@Disabled
	public void testRestoreExistingKey() {
	}

	@Test // DATAREDIS-269
	@Disabled
	public void clientSetNameWorksCorrectly() {
	}

	@Test
	@Override
	// DATAREDIS-268
	public void testListClientsContainsAtLeastOneElement() {
		assertThatExceptionOfType(UnsupportedOperationException.class)
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
