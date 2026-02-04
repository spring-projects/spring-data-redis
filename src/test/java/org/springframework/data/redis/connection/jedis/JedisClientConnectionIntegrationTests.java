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
 * Integration test of {@link JedisClientConnection}
 * <p>
 * These tests require Redis 7.2+ to be available.
 *
 * @author Tihomir Mateev
 * @since 4.1
 */
@ExtendWith(SpringExtension.class)
@ContextConfiguration
public class JedisClientConnectionIntegrationTests extends AbstractConnectionIntegrationTests {

	@AfterEach
	public void tearDown() {
		try {
			connection.flushAll();
		} catch (Exception ignore) {
			// Jedis leaves some incomplete data in OutputStream on NPE caused by null key/value tests
			// Attempting to flush the DB or close the connection will result in error on sending QUIT to Redis
		}

		try {
			connection.close();
		} catch (Exception ignore) {}

		connection = null;
	}

	@Test
	void shouldSetAndGetValue() {
		connection.set("key", "value");
		assertThat(connection.get("key")).isEqualTo("value");
	}

	@Test
	void shouldHandlePipeline() {
		connection.openPipeline();
		connection.set("key1", "value1");
		connection.set("key2", "value2");
		connection.get("key1");
		connection.get("key2");

		var results = connection.closePipeline();

		assertThat(results).hasSize(4);
		assertThat(results.get(2)).isEqualTo("value1");
		assertThat(results.get(3)).isEqualTo("value2");
	}

	@Test
	void shouldHandleTransaction() {
		connection.multi();
		connection.set("txKey1", "txValue1");
		connection.set("txKey2", "txValue2");
		connection.get("txKey1");

		var results = connection.exec();

		assertThat(results).isNotNull();
		assertThat(results).hasSize(3);
		assertThat(results.get(2)).isEqualTo("txValue1");
	}

	@Test
	void shouldGetClientName() {
		// Reset client name first in case another test changed it
		connection.setClientName("jedis-client-test".getBytes());
		assertThat(connection.getClientName()).isEqualTo("jedis-client-test");
	}

	@Override
	@Test
	public void testMove() {
		// Ensure we're on database 0
		connection.select(0);
		connection.set("foo", "bar");
		assertThat(connection.move("foo", 1)).isTrue();

		connection.select(1);
		try {
			assertThat(connection.get("foo")).isEqualTo("bar");
		} finally {
			if (connection.exists("foo")) {
				connection.del("foo");
			}
			// Reset to database 0
			connection.select(0);
		}
	}

	@Test
	void shouldSelectDatabase() {
		connection.select(1);
		connection.set("dbKey", "dbValue");

		connection.select(0);
		assertThat(connection.get("dbKey")).isNull();

		connection.select(1);
		assertThat(connection.get("dbKey")).isEqualTo("dbValue");

		// Clean up
		connection.del("dbKey");
		connection.select(0);
	}

	@Test
	void shouldHandleWatchUnwatch() {
		connection.set("watchKey", "initialValue");

		connection.watch("watchKey".getBytes());
		connection.multi();
		connection.set("watchKey", "newValue");

		var results = connection.exec();

		assertThat(results).isNotNull();
		assertThat(connection.get("watchKey")).isEqualTo("newValue");

		connection.unwatch();
	}

	@Test
	void shouldHandleHashOperations() {
		connection.hSet("hash", "field1", "value1");
		connection.hSet("hash", "field2", "value2");

		assertThat(connection.hGet("hash", "field1")).isEqualTo("value1");
		assertThat(connection.hGet("hash", "field2")).isEqualTo("value2");
		assertThat(connection.hLen("hash")).isEqualTo(2L);
	}

	@Test
	void shouldHandleListOperations() {
		connection.lPush("list", "value1");
		connection.lPush("list", "value2");
		connection.rPush("list", "value3");

		assertThat(connection.lLen("list")).isEqualTo(3L);
		assertThat(connection.lPop("list")).isEqualTo("value2");
		assertThat(connection.rPop("list")).isEqualTo("value3");
	}

	@Test
	void shouldHandleSetOperations() {
		connection.sAdd("set", "member1");
		connection.sAdd("set", "member2");
		connection.sAdd("set", "member3");

		assertThat(connection.sCard("set")).isEqualTo(3L);
		assertThat(connection.sIsMember("set", "member1")).isTrue();
		assertThat(connection.sIsMember("set", "member4")).isFalse();
	}

	// Jedis throws InvalidDataAccessApiUsageException for script errors, not RedisSystemException
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
