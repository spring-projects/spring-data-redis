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
import org.springframework.data.redis.connection.RedisListCommands;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.test.condition.EnabledOnRedisAvailable;
import org.springframework.data.redis.test.extension.JedisExtension;

import static org.assertj.core.api.Assertions.*;

/**
 * Integration tests for {@link JedisClientListCommands}. Tests all methods in direct, transaction, and pipelined modes.
 *
 * @author Tihomir Mateev
 * @since 4.1
 */
@EnabledOnRedisAvailable
@ExtendWith(JedisExtension.class)
class JedisClientListCommandsIntegrationTests {

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
			connection.flushDb();
			connection.close();
		}
		if (factory != null) {
			factory.destroy();
		}
	}

	// ============ Basic Push/Pop Operations ============
	@Test
	void basicPushPopOperationsShouldWork() {
		// Test rPush - push to right (tail)
		Long rPushResult = connection.listCommands().rPush("list1".getBytes(), "v1".getBytes(), "v2".getBytes(),
				"v3".getBytes());
		assertThat(rPushResult).isEqualTo(3L);

		// Test lPush - push to left (head)
		Long lPushResult = connection.listCommands().lPush("list1".getBytes(), "v0".getBytes());
		assertThat(lPushResult).isEqualTo(4L);
		// List is now: [v0, v1, v2, v3]

		// Test rPushX - push to right only if key exists
		Long rPushXResult = connection.listCommands().rPushX("list1".getBytes(), "v4".getBytes());
		assertThat(rPushXResult).isEqualTo(5L);
		Long rPushXNonExist = connection.listCommands().rPushX("nonexist".getBytes(), "v1".getBytes());
		assertThat(rPushXNonExist).isEqualTo(0L);

		// Test lPushX - push to left only if key exists
		Long lPushXResult = connection.listCommands().lPushX("list1".getBytes(), "v-1".getBytes());
		assertThat(lPushXResult).isEqualTo(6L);
		// List is now: [v-1, v0, v1, v2, v3, v4]

		// Test rPop - pop from right
		byte[] rPopResult = connection.listCommands().rPop("list1".getBytes());
		assertThat(rPopResult).isEqualTo("v4".getBytes());

		// Test lPop - pop from left
		byte[] lPopResult = connection.listCommands().lPop("list1".getBytes());
		assertThat(lPopResult).isEqualTo("v-1".getBytes());
		// List is now: [v0, v1, v2, v3]

		// Test lPop with count
		List<byte[]> lPopCountResult = connection.listCommands().lPop("list1".getBytes(), 2);
		assertThat(lPopCountResult).hasSize(2);
		assertThat(lPopCountResult.get(0)).isEqualTo("v0".getBytes());
		assertThat(lPopCountResult.get(1)).isEqualTo("v1".getBytes());

		// Test rPop with count
		List<byte[]> rPopCountResult = connection.listCommands().rPop("list1".getBytes(), 2);
		assertThat(rPopCountResult).hasSize(2);
		assertThat(rPopCountResult.get(0)).isEqualTo("v3".getBytes());
		assertThat(rPopCountResult.get(1)).isEqualTo("v2".getBytes());
	}

	@Test
	void listInspectionOperationsShouldWork() {
		// Set up list
		connection.listCommands().rPush("list2".getBytes(), "a".getBytes(), "b".getBytes(), "c".getBytes(), "a".getBytes());

		// Test lLen - get list length
		Long len = connection.listCommands().lLen("list2".getBytes());
		assertThat(len).isEqualTo(4L);

		// Test lRange - get range of elements
		List<byte[]> range = connection.listCommands().lRange("list2".getBytes(), 0, 2);
		assertThat(range).hasSize(3);
		assertThat(range.get(0)).isEqualTo("a".getBytes());
		assertThat(range.get(1)).isEqualTo("b".getBytes());
		assertThat(range.get(2)).isEqualTo("c".getBytes());

		// Test lIndex - get element at index
		byte[] indexResult = connection.listCommands().lIndex("list2".getBytes(), 1);
		assertThat(indexResult).isEqualTo("b".getBytes());

		// Test lPos - find position of element
		List<Long> posResult = connection.listCommands().lPos("list2".getBytes(), "a".getBytes(), null, null);
		assertThat(posResult).isNotEmpty();
		assertThat(posResult.get(0)).isEqualTo(0L); // First occurrence at index 0
	}

	@Test
	void listModificationOperationsShouldWork() {
		// Set up list
		connection.listCommands().rPush("list3".getBytes(), "v1".getBytes(), "v2".getBytes(), "v3".getBytes(),
				"v4".getBytes());

		// Test lSet - set element at index
		connection.listCommands().lSet("list3".getBytes(), 1, "v2-modified".getBytes());
		byte[] modified = connection.listCommands().lIndex("list3".getBytes(), 1);
		assertThat(modified).isEqualTo("v2-modified".getBytes());

		// Test lInsert - insert before/after element
		Long insertResult = connection.listCommands().lInsert("list3".getBytes(), RedisListCommands.Position.BEFORE,
				"v3".getBytes(), "v2.5".getBytes());
		assertThat(insertResult).isEqualTo(5L);

		// Test lRem - remove elements
		connection.listCommands().rPush("list3".getBytes(), "v2-modified".getBytes()); // Add duplicate
		Long remResult = connection.listCommands().lRem("list3".getBytes(), 2, "v2-modified".getBytes());
		assertThat(remResult).isEqualTo(2L); // Removed 2 occurrences

		// Test lTrim - trim list to range
		connection.listCommands().lTrim("list3".getBytes(), 0, 2);
		Long lenAfterTrim = connection.listCommands().lLen("list3".getBytes());
		assertThat(lenAfterTrim).isEqualTo(3L);
	}

	@Test
	void listMovementOperationsShouldWork() {
		// Set up source list
		connection.listCommands().rPush("src".getBytes(), "v1".getBytes(), "v2".getBytes(), "v3".getBytes());

		// Test lMove - move element from one list to another
		byte[] movedElement = connection.listCommands().lMove("src".getBytes(), "dst".getBytes(),
				RedisListCommands.Direction.LEFT, RedisListCommands.Direction.RIGHT);
		assertThat(movedElement).isEqualTo("v1".getBytes());
		assertThat(connection.listCommands().lLen("src".getBytes())).isEqualTo(2L);
		assertThat(connection.listCommands().lLen("dst".getBytes())).isEqualTo(1L);

		// Test rPopLPush - pop from right of source, push to left of destination
		byte[] rPopLPushResult = connection.listCommands().rPopLPush("src".getBytes(), "dst".getBytes());
		assertThat(rPopLPushResult).isEqualTo("v3".getBytes());
		assertThat(connection.listCommands().lLen("src".getBytes())).isEqualTo(1L);
		assertThat(connection.listCommands().lLen("dst".getBytes())).isEqualTo(2L);
	}

	@Test
	void blockingOperationsShouldWork() {
		// Set up lists
		connection.listCommands().rPush("blist1".getBytes(), "v1".getBytes(), "v2".getBytes());
		connection.listCommands().rPush("blist2".getBytes(), "v3".getBytes());

		// Test bLPop - blocking pop from left
		List<byte[]> bLPopResult = connection.listCommands().bLPop(1, "blist1".getBytes());
		assertThat(bLPopResult).hasSize(2); // [key, value]
		assertThat(bLPopResult.get(1)).isEqualTo("v1".getBytes());

		// Test bRPop - blocking pop from right
		List<byte[]> bRPopResult = connection.listCommands().bRPop(1, "blist1".getBytes());
		assertThat(bRPopResult).hasSize(2);
		assertThat(bRPopResult.get(1)).isEqualTo("v2".getBytes());

		// Test bLMove - blocking move
		byte[] bLMoveResult = connection.listCommands().bLMove("blist2".getBytes(), "blist1".getBytes(),
				RedisListCommands.Direction.LEFT, RedisListCommands.Direction.RIGHT, 1.0);
		assertThat(bLMoveResult).isEqualTo("v3".getBytes());

		// Test bRPopLPush - blocking pop from right and push to left
		connection.listCommands().rPush("blist2".getBytes(), "v4".getBytes());
		byte[] bRPopLPushResult = connection.listCommands().bRPopLPush(1, "blist2".getBytes(), "blist1".getBytes());
		assertThat(bRPopLPushResult).isEqualTo("v4".getBytes());
	}

	@Test
	void transactionShouldExecuteAtomically() {
		// Set up initial state
		connection.listCommands().rPush("txList".getBytes(), "v1".getBytes(), "v2".getBytes());

		// Execute multiple list operations in a transaction
		connection.multi();
		connection.listCommands().rPush("txList".getBytes(), "v3".getBytes());
		connection.listCommands().lPush("txList".getBytes(), "v0".getBytes());
		connection.listCommands().lLen("txList".getBytes());
		connection.listCommands().lRange("txList".getBytes(), 0, -1);
		connection.listCommands().lIndex("txList".getBytes(), 1);
		List<Object> results = connection.exec();

		// Verify all commands executed
		assertThat(results).hasSize(5);
		assertThat(results.get(0)).isEqualTo(3L); // rPush result
		assertThat(results.get(1)).isEqualTo(4L); // lPush result
		assertThat(results.get(2)).isEqualTo(4L); // lLen result
		@SuppressWarnings("unchecked")
		List<byte[]> range = (List<byte[]>) results.get(3);
		assertThat(range).hasSize(4); // lRange result
		assertThat(results.get(4)).isEqualTo("v1".getBytes()); // lIndex result

		// Verify final state
		assertThat(connection.listCommands().lLen("txList".getBytes())).isEqualTo(4L);
	}

	@Test
	void pipelineShouldExecuteMultipleCommands() {
		// Set up initial state
		connection.listCommands().rPush("pipeList".getBytes(), "v1".getBytes(), "v2".getBytes());

		// Execute multiple list operations in pipeline
		connection.openPipeline();
		connection.listCommands().rPush("pipeList".getBytes(), "v3".getBytes(), "v4".getBytes());
		connection.listCommands().lPush("pipeList".getBytes(), "v0".getBytes());
		connection.listCommands().lLen("pipeList".getBytes());
		connection.listCommands().lRange("pipeList".getBytes(), 0, -1);
		connection.listCommands().lPop("pipeList".getBytes());
		connection.listCommands().rPop("pipeList".getBytes());
		List<Object> results = connection.closePipeline();

		// Verify all command results
		assertThat(results).hasSize(6);
		assertThat(results.get(0)).isEqualTo(4L); // rPush result
		assertThat(results.get(1)).isEqualTo(5L); // lPush result
		assertThat(results.get(2)).isEqualTo(5L); // lLen result
		@SuppressWarnings("unchecked")
		List<byte[]> range = (List<byte[]>) results.get(3);
		assertThat(range).hasSize(5); // lRange result
		assertThat(results.get(4)).isEqualTo("v0".getBytes()); // lPop result
		assertThat(results.get(5)).isEqualTo("v4".getBytes()); // rPop result
	}
}
