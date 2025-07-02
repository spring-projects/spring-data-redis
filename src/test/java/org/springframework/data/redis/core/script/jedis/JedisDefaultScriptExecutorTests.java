/*
 * Copyright 2014-2025 the original author or authors.
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
package org.springframework.data.redis.core.script.jedis;

import redis.clients.jedis.Jedis;

import org.junit.jupiter.api.Disabled;

import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.jedis.extension.JedisConnectionFactoryExtension;
import org.springframework.data.redis.core.script.AbstractDefaultScriptExecutorTests;
import org.springframework.data.redis.core.script.DefaultScriptExecutor;
import org.springframework.data.redis.test.extension.RedisStandalone;

/**
 * Integration test of {@link DefaultScriptExecutor} with {@link Jedis}.
 *
 * @author Thomas Darimont
 */
public class JedisDefaultScriptExecutorTests extends AbstractDefaultScriptExecutorTests {

	@Override
	protected RedisConnectionFactory getConnectionFactory() {
		return JedisConnectionFactoryExtension.getConnectionFactory(RedisStandalone.class);
	}

	@Disabled("transactional execution is currently not supported with Jedis")
	@Override
	public void testExecuteTx() {
		// super.testExecuteTx();
	}

	@Disabled("pipelined execution is currently not supported with Jedis")
	@Override
	public void testExecutePipelined() {
		// super.testExecutePipelined();
	}
}
