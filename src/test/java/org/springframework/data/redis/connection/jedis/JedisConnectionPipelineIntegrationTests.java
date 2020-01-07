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

import redis.clients.jedis.JedisPoolConfig;

import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.connection.AbstractConnectionPipelineIntegrationTests;
import org.springframework.data.redis.connection.DefaultStringRedisConnection;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.test.util.RelaxedJUnit4ClassRunner;
import org.springframework.test.annotation.IfProfileValue;
import org.springframework.test.context.ContextConfiguration;

/**
 * Integration test of {@link JedisConnection} pipeline functionality
 *
 * @author Jennifer Hickey
 * @author Christoph Strobl
 * @author Thomas Darimont
 * @author Mark Paluch
 */
@RunWith(RelaxedJUnit4ClassRunner.class)
@ContextConfiguration("JedisConnectionIntegrationTests-context.xml")
public class JedisConnectionPipelineIntegrationTests extends AbstractConnectionPipelineIntegrationTests {

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

	@Ignore("Jedis issue: Pipeline tries to return String instead of List<String>")
	public void testGetConfig() {}

	@Test
	public void testWatch() {
		connection.set("testitnow", "willdo");
		connection.watch("testitnow".getBytes());
		// Jedis doesn't actually send commands until you close the pipeline
		getResults();
		DefaultStringRedisConnection conn2 = new DefaultStringRedisConnection(connectionFactory.getConnection());
		conn2.set("testitnow", "something");
		conn2.close();
		// Reopen the pipeline
		initConnection();
		connection.multi();
		connection.set("testitnow", "somethingelse");
		actual.add(connection.exec());
		actual.add(connection.get("testitnow"));
		verifyResults(Arrays.asList(new Object[] { null, "something" }));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testUnwatch() throws Exception {
		connection.set("testitnow", "willdo");
		connection.watch("testitnow".getBytes());
		// Jedis doesn't actually send commands until you close the pipeline
		getResults();
		initConnection();
		connection.unwatch();
		// Jedis doesn't actually send commands until you close the pipeline
		getResults();
		initConnection();
		connection.multi();
		DefaultStringRedisConnection conn2 = new DefaultStringRedisConnection(connectionFactory.getConnection());
		conn2.set("testitnow", "something");
		connection.set("testitnow", "somethingelse");
		connection.get("testitnow");
		actual.add(connection.exec());
		List<Object> results = getResults();
		List<Object> execResults = (List<Object>) results.get(0);
		assertThat(execResults).isEqualTo(Arrays.asList(new Object[] { true, "somethingelse" }));
	}

	@Test
	// DATAREDIS-213 - Verify connection returns to pool after select
	public void testClosePoolPipelinedDbSelect() {

		JedisPoolConfig config = new JedisPoolConfig();
		config.setMaxTotal(1);
		config.setMaxIdle(1);
		JedisConnectionFactory factory2 = new JedisConnectionFactory(config);
		factory2.setHostName(SettingsUtils.getHost());
		factory2.setPort(SettingsUtils.getPort());
		factory2.setDatabase(1);
		factory2.afterPropertiesSet();
		RedisConnection conn2 = factory2.getConnection();
		conn2.openPipeline();
		conn2.close();
		factory2.getConnection();
		factory2.destroy();
	}

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
	@Ignore
	public void testEvalShaNotFound() {}

	@Test
	@Ignore
	public void testEvalShaArrayError() {}

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
	@Ignore
	public void testEvalReturnSingleError() {
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
	@Ignore
	public void testScriptFlush() {}

	@Test
	@IfProfileValue(name = "redisVersion", value = "2.6+")
	public void testInfoBySection() {
		assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(super::testInfoBySection);
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

	@Test // DATAREDIS-296
	@Ignore
	public void testExecWithoutMulti() {}
}
