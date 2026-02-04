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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.test.condition.EnabledOnRedisAvailable;
import org.springframework.data.redis.test.extension.JedisExtension;

import static org.assertj.core.api.Assertions.*;

/**
 * Integration tests for {@link JedisClientScriptingCommands}. Tests all methods in direct, transaction, and pipelined
 * modes.
 *
 * @author Tihomir Mateev
 * @since 4.1
 */
@EnabledOnRedisAvailable
@ExtendWith(JedisExtension.class)
class JedisClientScriptingCommandsIntegrationTests {

	private JedisClientConnectionFactory factory;
	private JedisClientConnection connection;

	@BeforeEach
	void setUp() {
		RedisStandaloneConfiguration config = new RedisStandaloneConfiguration(SettingsUtils.getHost(),
				SettingsUtils.getPort());
		factory = new JedisClientConnectionFactory(config);
		factory.afterPropertiesSet();
		connection = (JedisClientConnection) factory.getConnection();
	}

	@AfterEach
	void tearDown() {
		if (connection != null) {
			connection.close();
		}
		if (factory != null) {
			factory.destroy();
		}
	}

	// ============ Script Execution Operations ============
	@Test
	void scriptExecutionOperationsShouldWork() {
		// Simple Lua script that returns a value
		String script = "return 'Hello, Redis!'";

		// Test eval - execute script
		Object evalResult = connection.scriptingCommands().eval(script.getBytes(), ReturnType.VALUE, 0);
		assertThat(evalResult).isEqualTo("Hello, Redis!".getBytes());

		// Script with keys and args
		String scriptWithArgs = "return {KEYS[1], ARGV[1]}";
		Object evalWithArgsResult = connection.scriptingCommands().eval(scriptWithArgs.getBytes(), ReturnType.MULTI, 1,
				"key1".getBytes(), "arg1".getBytes());
		assertThat(evalWithArgsResult).isInstanceOf(List.class);

		// Test scriptLoad - load script and get SHA
		String sha = connection.scriptingCommands().scriptLoad(script.getBytes());
		assertThat(sha).isNotNull().hasSize(40); // SHA-1 hash is 40 characters

		// Test scriptExists - check if script exists
		List<Boolean> existsResult = connection.scriptingCommands().scriptExists(sha);
		assertThat(existsResult).containsExactly(true);

		// Test evalSha with String SHA
		Object evalShaResult = connection.scriptingCommands().evalSha(sha, ReturnType.VALUE, 0);
		assertThat(evalShaResult).isEqualTo("Hello, Redis!".getBytes());

		// Test evalSha with byte[] SHA
		Object evalShaByteResult = connection.scriptingCommands().evalSha(sha.getBytes(), ReturnType.VALUE, 0);
		assertThat(evalShaByteResult).isEqualTo("Hello, Redis!".getBytes());

		// Test scriptFlush - remove all scripts
		connection.scriptingCommands().scriptFlush();
		List<Boolean> existsAfterFlush = connection.scriptingCommands().scriptExists(sha);
		assertThat(existsAfterFlush).containsExactly(false);
	}

	@Test
	void transactionShouldExecuteAtomically() {
		// Set up initial state
		String script = "return 42";
		String sha = connection.scriptingCommands().scriptLoad(script.getBytes());

		// Execute multiple scripting operations in a transaction
		connection.multi();
		connection.scriptingCommands().eval(script.getBytes(), ReturnType.INTEGER, 0);
		connection.scriptingCommands().evalSha(sha, ReturnType.INTEGER, 0);
		connection.scriptingCommands().scriptExists(sha);
		List<Object> results = connection.exec();

		// Verify all commands executed
		assertThat(results).hasSize(3);
		assertThat(results.get(0)).isEqualTo(42L); // eval result
		assertThat(results.get(1)).isEqualTo(42L); // evalSha result
		@SuppressWarnings("unchecked")
		List<Boolean> existsResult = (List<Boolean>) results.get(2);
		assertThat(existsResult).containsExactly(true); // scriptExists result
	}

	@Test
	void pipelineShouldExecuteMultipleCommands() {
		// Set up initial state
		String script1 = "return 'first'";
		String script2 = "return 'second'";
		String sha1 = connection.scriptingCommands().scriptLoad(script1.getBytes());
		String sha2 = connection.scriptingCommands().scriptLoad(script2.getBytes());

		// Execute multiple scripting operations in pipeline
		connection.openPipeline();
		connection.scriptingCommands().eval(script1.getBytes(), ReturnType.VALUE, 0);
		connection.scriptingCommands().evalSha(sha2, ReturnType.VALUE, 0);
		connection.scriptingCommands().scriptExists(sha1, sha2);
		List<Object> results = connection.closePipeline();

		// Verify all command results
		assertThat(results).hasSize(3);
		assertThat(results.get(0)).isEqualTo("first".getBytes()); // eval result
		assertThat(results.get(1)).isEqualTo("second".getBytes()); // evalSha result
		@SuppressWarnings("unchecked")
		List<Boolean> existsResult = (List<Boolean>) results.get(2);
		assertThat(existsResult).containsExactly(true, true); // scriptExists result
	}
}
