/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.redis.connection.jedis;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.connection.AbstractConnectionIntegrationTests;
import org.springframework.data.redis.connection.RedisSentinelConfiguration;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.test.util.RedisSentinelRule;
import org.springframework.test.annotation.IfProfileValue;

/**
 * @author Christoph Strobl
 */
public class JedisSentinelIntegrationTests extends AbstractConnectionIntegrationTests {

	private static final RedisSentinelConfiguration SENTINEL_CONFIG = new RedisSentinelConfiguration().master("mymaster")
			.sentinel("127.0.0.1", 26379).sentinel("127.0.0.1", 26380).sentinel("127.0.0.1", 26381);

	public @Rule RedisSentinelRule sentinelRule = RedisSentinelRule.forConfig(SENTINEL_CONFIG).oneActive();

	@Before
	public void setUp() {
		JedisConnectionFactory jedisConnectionFactory = new JedisConnectionFactory(SENTINEL_CONFIG);
		jedisConnectionFactory.afterPropertiesSet();
		connectionFactory = jedisConnectionFactory;
		super.setUp();
	}

	@After
	public void tearDown() {
		super.tearDown();
		((JedisConnectionFactory) connectionFactory).destroy();
	}

	@Test
	@Ignore
	public void testScriptKill() throws Exception {
		super.testScriptKill();
	}

	@Test(expected = InvalidDataAccessApiUsageException.class)
	@IfProfileValue(name = "redisVersion", value = "2.6+")
	public void testEvalReturnSingleError() {
		connection.eval("return redis.call('expire','foo')", ReturnType.BOOLEAN, 0);
	}

	@Test(expected = InvalidDataAccessApiUsageException.class)
	@IfProfileValue(name = "redisVersion", value = "2.6+")
	public void testEvalArrayScriptError() {
		super.testEvalArrayScriptError();
	}

	@Test(expected = InvalidDataAccessApiUsageException.class)
	@IfProfileValue(name = "redisVersion", value = "2.6+")
	public void testEvalShaNotFound() {
		connection.evalSha("somefakesha", ReturnType.VALUE, 2, "key1", "key2");
	}

	@Test(expected = InvalidDataAccessApiUsageException.class)
	@IfProfileValue(name = "redisVersion", value = "2.6+")
	public void testEvalShaArrayError() {
		super.testEvalShaArrayError();
	}

	@Test(expected = InvalidDataAccessApiUsageException.class)
	@IfProfileValue(name = "redisVersion", value = "2.6+")
	public void testRestoreBadData() {
		super.testRestoreBadData();
	}

	@Test(expected = InvalidDataAccessApiUsageException.class)
	@IfProfileValue(name = "redisVersion", value = "2.6+")
	public void testRestoreExistingKey() {
		super.testRestoreExistingKey();
	}

	@Test(expected = InvalidDataAccessApiUsageException.class)
	public void testExecWithoutMulti() {
		super.testExecWithoutMulti();
	}

	@Test(expected = InvalidDataAccessApiUsageException.class)
	public void testErrorInTx() {
		super.testErrorInTx();
	}
}
