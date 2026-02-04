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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.connection.AbstractConnectionPipelineIntegrationTests;
import org.springframework.data.redis.connection.RedisPipelineException;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import static org.assertj.core.api.Assertions.*;

/**
 * Integration tests for {@link JedisClientConnection} pipeline functionality.
 * <p>
 * Note: Jedis throws {@link InvalidDataAccessApiUsageException} for script errors and command errors, while Lettuce
 * throws {@code RedisSystemException}. This is expected behavior based on {@link JedisExceptionConverter}.
 *
 * @author Tihomir Mateev
 * @since 4.1
 */
@ExtendWith(SpringExtension.class)
@ContextConfiguration("JedisClientConnectionIntegrationTests-context.xml")
public class JedisClientConnectionPipelineIntegrationTests extends AbstractConnectionPipelineIntegrationTests {

	@AfterEach
	public void tearDown() {
		try {
			connection.serverCommands().flushAll();
			connection.close();
		} catch (Exception e) {
			// Ignore
		}
		connection = null;
	}

	@Override
	@Test
	public void testEvalShaArrayError() {
		connection.evalSha("notasha", ReturnType.MULTI, 1, "key1", "arg1");
		assertThatExceptionOfType(RedisPipelineException.class).isThrownBy(this::getResults)
				.withCauseInstanceOf(InvalidDataAccessApiUsageException.class);
	}

	@Override
	@Test
	public void testEvalShaNotFound() {
		connection.evalSha("somefakesha", ReturnType.VALUE, 2, "key1", "key2");
		assertThatExceptionOfType(RedisPipelineException.class).isThrownBy(this::getResults)
				.withCauseInstanceOf(InvalidDataAccessApiUsageException.class);
	}

	@Override
	@Test
	public void testEvalReturnSingleError() {
		connection.eval("return redis.call('expire','foo')", ReturnType.BOOLEAN, 0);
		assertThatExceptionOfType(RedisPipelineException.class).isThrownBy(this::getResults)
				.withCauseInstanceOf(InvalidDataAccessApiUsageException.class);
	}

	@Override
	@Test
	public void testEvalArrayScriptError() {
		// Syntax error
		connection.eval("return {1,2", ReturnType.MULTI, 1, "foo", "bar");
		assertThatExceptionOfType(RedisPipelineException.class).isThrownBy(this::getResults)
				.withCauseInstanceOf(InvalidDataAccessApiUsageException.class);
	}

	@Override
	@Test
	public void testRestoreBadData() {
		// Use something other than dump-specific serialization
		connection.restore("testing".getBytes(), 0, "foo".getBytes());
		assertThatExceptionOfType(RedisPipelineException.class).isThrownBy(this::getResults)
				.withCauseInstanceOf(InvalidDataAccessApiUsageException.class);
	}

	// These tests expect RedisPipelineException but Jedis throws earlier during multi()/exec() calls
	@Override
	@Test
	public void testExecWithoutMulti() {
		assertThatExceptionOfType(InvalidDataAccessApiUsageException.class).isThrownBy(connection::exec)
				.withMessage("No ongoing transaction; Did you forget to call multi");
	}

	@Override
	@Test
	public void testErrorInTx() {
		assertThatExceptionOfType(InvalidDataAccessApiUsageException.class).isThrownBy(connection::multi)
				.withMessage("Cannot use Transaction while a pipeline is open");
	}

	@Override
	@Test
	public void testMultiExec() {
		assertThatExceptionOfType(InvalidDataAccessApiUsageException.class).isThrownBy(connection::multi)
				.withMessage("Cannot use Transaction while a pipeline is open");
	}

	@Override
	@Test
	public void testMultiAlreadyInTx() {
		assertThatExceptionOfType(InvalidDataAccessApiUsageException.class).isThrownBy(connection::multi)
				.withMessage("Cannot use Transaction while a pipeline is open");
	}

	@Override
	@Test
	public void testMultiDiscard() {
		assertThatExceptionOfType(InvalidDataAccessApiUsageException.class).isThrownBy(connection::multi)
				.withMessage("Cannot use Transaction while a pipeline is open");
	}

	@Override
	@Test
	public void testWatch() {
		assertThatExceptionOfType(InvalidDataAccessApiUsageException.class).isThrownBy(connection::multi)
				.withMessage("Cannot use Transaction while a pipeline is open");
	}

	@Override
	@Test
	public void testUnwatch() {
		assertThatExceptionOfType(InvalidDataAccessApiUsageException.class).isThrownBy(connection::multi)
				.withMessage("Cannot use Transaction while a pipeline is open");
	}
}
