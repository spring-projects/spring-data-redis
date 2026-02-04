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
import org.springframework.data.redis.connection.RedisClusterConfiguration;
import org.springframework.data.redis.connection.RedisClusterConnection;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.test.condition.EnabledOnRedisClusterAvailable;
import org.springframework.data.redis.test.extension.JedisExtension;

import static org.assertj.core.api.Assertions.*;

/**
 * Integration tests for {@link JedisClientScriptingCommands} in cluster mode. Tests all methods in direct and pipelined
 * modes (transactions not supported in cluster).
 *
 * @author Tihomir Mateev
 * @since 4.1
 */
@EnabledOnRedisClusterAvailable
@ExtendWith(JedisExtension.class)
class JedisClientClusterScriptingCommandsIntegrationTests {

	private JedisClientConnectionFactory factory;
	private RedisClusterConnection connection;

	@BeforeEach
	void setUp() {
		RedisClusterConfiguration config = new RedisClusterConfiguration().clusterNode(SettingsUtils.getHost(),
				SettingsUtils.getClusterPort());
		factory = new JedisClientConnectionFactory(config);
		factory.afterPropertiesSet();
		connection = factory.getClusterConnection();
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

		// Test evalSha with String SHA
		Object evalShaResult = connection.scriptingCommands().evalSha(sha, ReturnType.VALUE, 0);
		assertThat(evalShaResult).isEqualTo("Hello, Redis!".getBytes());

		// Test evalSha with byte[] SHA
		Object evalShaByteResult = connection.scriptingCommands().evalSha(sha.getBytes(), ReturnType.VALUE, 0);
		assertThat(evalShaByteResult).isEqualTo("Hello, Redis!".getBytes());

		// Test scriptFlush - remove all scripts
		connection.scriptingCommands().scriptFlush();
	}
}
