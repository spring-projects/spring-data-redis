/*
 * Copyright 2011-2020 the original author or authors.
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

import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.connection.AbstractConnectionTransactionIntegrationTests;
import org.springframework.data.redis.test.util.RelaxedJUnit4ClassRunner;
import org.springframework.test.annotation.IfProfileValue;
import org.springframework.test.context.ContextConfiguration;

/**
 * Integration test of {@link JedisConnection} transaction functionality.
 * <p>
 * Each method of {@link JedisConnection} behaves differently if executed with a transaction (i.e. between multi and
 * exec or discard calls), so this test covers those branching points
 *
 * @author Jennifer Hickey
 * @author Mark Paluch
 */
@RunWith(RelaxedJUnit4ClassRunner.class)
@ContextConfiguration("JedisConnectionIntegrationTests-context.xml")
public class JedisConnectionTransactionIntegrationTests extends AbstractConnectionTransactionIntegrationTests {

	@After
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

	@Ignore("Jedis issue: Transaction tries to return String instead of List<String>")
	public void testGetConfig() {}

	// Unsupported Ops
	@Test
	@IfProfileValue(name = "redisVersion", value = "2.6+")
	public void testScriptLoadEvalSha() {
		assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(super::testScriptLoadEvalSha);
	}

	@Test
	@IfProfileValue(name = "redisVersion", value = "2.6+")
	public void testEvalShaArrayStrings() {
		assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(super::testEvalShaArrayStrings);
	}

	@Test
	@IfProfileValue(name = "redisVersion", value = "2.6+")
	public void testEvalShaArrayBytes() {
		assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(super::testEvalShaArrayBytes);
	}

	@Test
	@IfProfileValue(name = "redisVersion", value = "2.6+")
	public void testEvalShaNotFound() {
		assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(super::testEvalShaNotFound);
	}

	@Test
	@IfProfileValue(name = "redisVersion", value = "2.6+")
	public void testEvalShaArrayError() {
		assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(super::testEvalShaArrayError);
	}

	@Test
	@IfProfileValue(name = "redisVersion", value = "2.6+")
	public void testEvalArrayScriptError() {
		assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(super::testEvalArrayScriptError);
	}

	@Test
	@IfProfileValue(name = "redisVersion", value = "2.6+")
	public void testEvalReturnString() {
		assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(super::testEvalReturnString);
	}

	@Test
	@IfProfileValue(name = "redisVersion", value = "2.6+")
	public void testEvalReturnNumber() {
		assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(super::testEvalReturnNumber);
	}

	@Test
	@IfProfileValue(name = "redisVersion", value = "2.6+")
	public void testEvalReturnSingleOK() {
		assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(super::testEvalReturnSingleOK);
	}

	@Test
	@IfProfileValue(name = "redisVersion", value = "2.6+")
	public void testEvalReturnSingleError() {
		assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(super::testEvalReturnSingleError);
	}

	@Test
	@IfProfileValue(name = "redisVersion", value = "2.6+")
	public void testEvalReturnFalse() {
		assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(super::testEvalReturnFalse);
	}

	@Test
	@IfProfileValue(name = "redisVersion", value = "2.6+")
	public void testEvalReturnTrue() {
		assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(super::testEvalReturnTrue);
	}

	@Test
	@IfProfileValue(name = "redisVersion", value = "2.6+")
	public void testEvalReturnArrayStrings() {
		assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(super::testEvalReturnArrayStrings);
	}

	@Test
	@IfProfileValue(name = "redisVersion", value = "2.6+")
	public void testEvalReturnArrayNumbers() {
		assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(super::testEvalReturnArrayNumbers);
	}

	@Test
	@IfProfileValue(name = "redisVersion", value = "2.6+")
	public void testEvalReturnArrayOKs() {
		assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(super::testEvalReturnArrayOKs);
	}

	@Test
	@IfProfileValue(name = "redisVersion", value = "2.6+")
	public void testEvalReturnArrayFalses() {
		assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(super::testEvalReturnArrayFalses);
	}

	@Test
	@IfProfileValue(name = "redisVersion", value = "2.6+")
	public void testEvalReturnArrayTrues() {
		assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(super::testEvalReturnArrayTrues);
	}

	@Test
	@IfProfileValue(name = "redisVersion", value = "2.6+")
	public void testScriptExists() {
		assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(super::testScriptExists);
	}

	@Test
	@IfProfileValue(name = "redisVersion", value = "2.6+")
	public void testScriptKill() {
		assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(() -> connection.scriptKill());
	}

	@Test
	@IfProfileValue(name = "redisVersion", value = "2.6+")
	public void testScriptFlush() {
		assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(() -> connection.scriptFlush());
	}

	@Test
	@IfProfileValue(name = "redisVersion", value = "2.6+")
	public void testInfoBySection() {
		assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(super::testInfoBySection);
	}

	@Test
	@IfProfileValue(name = "redisVersion", value = "2.6+")
	public void testRestoreBadData() {
		assertThatExceptionOfType(InvalidDataAccessApiUsageException.class).isThrownBy(super::testRestoreBadData);
	}

	@Test
	@IfProfileValue(name = "redisVersion", value = "2.6+")
	public void testRestoreExistingKey() {
		assertThatExceptionOfType(InvalidDataAccessApiUsageException.class).isThrownBy(super::testRestoreExistingKey);
	}

	@Test // DATAREDIS-269
	public void clientSetNameWorksCorrectly() {
		assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(super::clientSetNameWorksCorrectly);
	}

	@Test
	@Override
	// DATAREDIS-268
	public void testListClientsContainsAtLeastOneElement() {
		assertThatExceptionOfType(UnsupportedOperationException.class)
				.isThrownBy(super::testListClientsContainsAtLeastOneElement);
	}
}
