/*
 * Copyright 2026-present the original author or authors.
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

import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.connection.AbstractConnectionIntegrationTests;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import static org.assertj.core.api.Assertions.*;

/**
 * Brief integration tests for all JedisClient*Commands classes. Tests basic command execution and response parsing.
 * <p>
 * Note: Jedis throws {@link InvalidDataAccessApiUsageException} for script errors and command errors, while Lettuce
 * throws {@code RedisSystemException}. This is expected behavior based on {@link JedisExceptionConverter} which
 * converts all {@code JedisException} to {@link InvalidDataAccessApiUsageException}. Tests that verify exception types
 * are overridden to expect the correct Jedis exceptions.
 *
 * @author Tihomir Mateev
 * @since 4.1
 */
@ExtendWith(SpringExtension.class)
@ContextConfiguration
class JedisClientCommandsIntegrationTests extends AbstractConnectionIntegrationTests {

	@AfterEach
	@Override
	public void tearDown() {
		// Ensure any open transaction is discarded before cleanup
		if (connection != null && connection.isQueueing()) {
			try {
				connection.discard();
			} catch (Exception e) {
				// Ignore - connection might be in an invalid state
			}
		}
		super.tearDown();
	}

	// ========================================================================
	// Pipeline Tests
	// ========================================================================

	@Test // GH-XXXX - Pipeline basic operations
	void pipelineShouldWork() {
		connection.openPipeline();
		connection.set("pkey1", "pvalue1");
		connection.set("pkey2", "pvalue2");
		connection.get("pkey1");
		connection.get("pkey2");
		List<Object> results = connection.closePipeline();

		assertThat(results).hasSize(4);
		assertThat(results.get(0)).isEqualTo(true); // set result
		assertThat(results.get(1)).isEqualTo(true); // set result
		assertThat(results.get(2)).isEqualTo("pvalue1"); // get result
		assertThat(results.get(3)).isEqualTo("pvalue2"); // get result
	}

	@Test // GH-XXXX - Pipeline with multiple data types
	void pipelineWithMultipleDataTypesShouldWork() {
		connection.openPipeline();
		connection.set("str", "value");
		connection.hSet("hash", "field", "hvalue");
		connection.lPush("list", "lvalue");
		connection.sAdd("set", "svalue");
		connection.zAdd("zset", 1.0, "zvalue");
		connection.get("str");
		connection.hGet("hash", "field");
		connection.lPop("list");
		connection.sIsMember("set", "svalue");
		connection.zScore("zset", "zvalue");
		List<Object> results = connection.closePipeline();

		assertThat(results).hasSize(10);
		assertThat(results.get(5)).isEqualTo("value");
		assertThat(results.get(6)).isEqualTo("hvalue");
		assertThat(results.get(7)).isEqualTo("lvalue");
		assertThat(results.get(8)).isEqualTo(true);
		assertThat(results.get(9)).isEqualTo(1.0);
	}

	// ========================================================================
	// Transaction Tests
	// ========================================================================

	@Test // GH-XXXX - Transaction basic operations
	void transactionShouldWork() {
		connection.multi();
		connection.set("txkey1", "txvalue1");
		connection.set("txkey2", "txvalue2");
		connection.get("txkey1");
		connection.get("txkey2");
		List<Object> results = connection.exec();

		assertThat(results).hasSize(4);
		assertThat(results.get(0)).isEqualTo(true); // set result
		assertThat(results.get(1)).isEqualTo(true); // set result
		assertThat(results.get(2)).isEqualTo("txvalue1"); // get result
		assertThat(results.get(3)).isEqualTo("txvalue2"); // get result

		// Verify values were actually set
		assertThat(connection.get("txkey1")).isEqualTo("txvalue1");
		assertThat(connection.get("txkey2")).isEqualTo("txvalue2");
	}

	@Test // GH-XXXX - Transaction with multiple data types
	void transactionWithMultipleDataTypesShouldWork() {
		connection.multi();
		connection.set("txstr", "value");
		connection.hSet("txhash", "field", "hvalue");
		connection.lPush("txlist", "lvalue");
		connection.sAdd("txset", "svalue");
		connection.zAdd("txzset", 1.0, "zvalue");
		connection.get("txstr");
		connection.hGet("txhash", "field");
		connection.lPop("txlist");
		connection.sIsMember("txset", "svalue");
		connection.zScore("txzset", "zvalue");
		List<Object> results = connection.exec();

		assertThat(results).hasSize(10);
		assertThat(results.get(5)).isEqualTo("value");
		assertThat(results.get(6)).isEqualTo("hvalue");
		assertThat(results.get(7)).isEqualTo("lvalue");
		assertThat(results.get(8)).isEqualTo(true);
		assertThat(results.get(9)).isEqualTo(1.0);
	}

	@Test // GH-XXXX - Transaction discard
	void transactionDiscardShouldWork() {
		connection.set("discardkey", "original");
		connection.multi();
		connection.set("discardkey", "modified");
		connection.discard();

		// Value should remain unchanged
		assertThat(connection.get("discardkey")).isEqualTo("original");
	}

	// ========================================================================
	// Exception Type Overrides for Jedis
	// ========================================================================
	// Jedis throws InvalidDataAccessApiUsageException for script/command errors
	// while Lettuce throws RedisSystemException. Override parent tests to expect
	// the correct exception type for Jedis.

	@Override
	@Test
	public void testEvalShaArrayError() {
		assertThatExceptionOfType(InvalidDataAccessApiUsageException.class).isThrownBy(() -> {
			connection.evalSha("notasha", ReturnType.MULTI, 1, "key1", "arg1");
			getResults();
		});
	}

	@Override
	@Test
	public void testEvalShaNotFound() {
		assertThatExceptionOfType(InvalidDataAccessApiUsageException.class).isThrownBy(() -> {
			connection.evalSha("somefakesha", ReturnType.VALUE, 2, "key1", "key2");
			getResults();
		});
	}

	@Override
	@Test
	public void testEvalReturnSingleError() {
		assertThatExceptionOfType(InvalidDataAccessApiUsageException.class).isThrownBy(() -> {
			connection.eval("return redis.call('expire','foo')", ReturnType.BOOLEAN, 0);
			getResults();
		});
	}

	@Override
	@Test
	public void testEvalArrayScriptError() {
		assertThatExceptionOfType(InvalidDataAccessApiUsageException.class).isThrownBy(() -> {
			// Syntax error
			connection.eval("return {1,2", ReturnType.MULTI, 1, "foo", "bar");
			getResults();
		});
	}

	@Override
	@Test
	public void testExecWithoutMulti() {
		assertThatExceptionOfType(InvalidDataAccessApiUsageException.class).isThrownBy(() -> {
			connection.exec();
		});
	}

	@Override
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

	@Override
	@Test
	public void testRestoreBadData() {
		assertThatExceptionOfType(InvalidDataAccessApiUsageException.class).isThrownBy(() -> {
			// Use something other than dump-specific serialization
			connection.restore("testing".getBytes(), 0, "foo".getBytes());
			getResults();
		});
	}

	@Override
	@Test
	public void testRestoreExistingKey() {

		actual.add(connection.set("testing", "12"));
		actual.add(connection.dump("testing".getBytes()));
		List<Object> results = getResults();
		initConnection();
		assertThatExceptionOfType(InvalidDataAccessApiUsageException.class).isThrownBy(() -> {
			connection.restore("testing".getBytes(), 0, (byte[]) results.get(1));
			getResults();
		});
	}
}
