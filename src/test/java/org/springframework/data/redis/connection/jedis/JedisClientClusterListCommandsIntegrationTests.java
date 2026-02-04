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
import org.springframework.data.redis.connection.RedisListCommands.Direction;
import org.springframework.data.redis.connection.RedisListCommands.Position;
import org.springframework.data.redis.test.condition.EnabledOnRedisClusterAvailable;
import org.springframework.data.redis.test.extension.JedisExtension;

import static org.assertj.core.api.Assertions.*;

/**
 * Integration tests for {@link JedisClientListCommands} in cluster mode. Tests all methods in direct and pipelined
 * modes (transactions not supported in cluster).
 *
 * @author Tihomir Mateev
 * @since 4.1
 */
@EnabledOnRedisClusterAvailable
@ExtendWith(JedisExtension.class)
class JedisClientClusterListCommandsIntegrationTests {

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
			connection.serverCommands().flushDb();
			connection.close();
		}
		if (factory != null) {
			factory.destroy();
		}
	}

	// ============ Basic Push/Pop Operations ============
	@Test
	void basicPushPopOperationsShouldWork() {
		// Test rPush - push to right
		Long rPushResult = connection.listCommands().rPush("list1".getBytes(), "value1".getBytes());
		assertThat(rPushResult).isEqualTo(1L);

		// Test lPush - push to left
		Long lPushResult = connection.listCommands().lPush("list1".getBytes(), "value0".getBytes());
		assertThat(lPushResult).isEqualTo(2L);

		// Test rPushX - push to right only if exists
		Long rPushXResult = connection.listCommands().rPushX("list1".getBytes(), "value2".getBytes());
		assertThat(rPushXResult).isEqualTo(3L);
		Long rPushXResult2 = connection.listCommands().rPushX("nonexistent".getBytes(), "value".getBytes());
		assertThat(rPushXResult2).isEqualTo(0L);

		// Test lPushX - push to left only if exists
		Long lPushXResult = connection.listCommands().lPushX("list1".getBytes(), "value-1".getBytes());
		assertThat(lPushXResult).isEqualTo(4L);

		// Test rPop - pop from right
		byte[] rPopResult = connection.listCommands().rPop("list1".getBytes());
		assertThat(rPopResult).isEqualTo("value2".getBytes());

		// Test lPop - pop from left
		byte[] lPopResult = connection.listCommands().lPop("list1".getBytes());
		assertThat(lPopResult).isEqualTo("value-1".getBytes());

		// Test lPop with count
		connection.listCommands().rPush("list2".getBytes(), "a".getBytes(), "b".getBytes(), "c".getBytes());
		List<byte[]> lPopCountResult = connection.listCommands().lPop("list2".getBytes(), 2);
		assertThat(lPopCountResult).hasSize(2);

		// Test rPop with count
		List<byte[]> rPopCountResult = connection.listCommands().rPop("list2".getBytes(), 1);
		assertThat(rPopCountResult).hasSize(1);
	}

	@Test
	void listInspectionOperationsShouldWork() {
		// Set up list
		connection.listCommands().rPush("list1".getBytes(), "value1".getBytes(), "value2".getBytes(), "value3".getBytes());

		// Test lLen - get list length
		Long lLenResult = connection.listCommands().lLen("list1".getBytes());
		assertThat(lLenResult).isEqualTo(3L);

		// Test lRange - get range of elements
		List<byte[]> lRangeResult = connection.listCommands().lRange("list1".getBytes(), 0, -1);
		assertThat(lRangeResult).hasSize(3);
		assertThat(lRangeResult.get(0)).isEqualTo("value1".getBytes());

		// Test lIndex - get element at index
		byte[] lIndexResult = connection.listCommands().lIndex("list1".getBytes(), 1);
		assertThat(lIndexResult).isEqualTo("value2".getBytes());

		// Test lPos - get position of element
		Long lPosResult = connection.listCommands().lPos("list1".getBytes(), "value2".getBytes());
		assertThat(lPosResult).isEqualTo(1L);
	}

	@Test
	void listModificationOperationsShouldWork() {
		// Set up list
		connection.listCommands().rPush("list1".getBytes(), "value1".getBytes(), "value2".getBytes(), "value3".getBytes());

		// Test lSet - set element at index
		connection.listCommands().lSet("list1".getBytes(), 1, "newValue".getBytes());
		assertThat(connection.listCommands().lIndex("list1".getBytes(), 1)).isEqualTo("newValue".getBytes());

		// Test lInsert - insert before/after element
		Long lInsertResult = connection.listCommands().lInsert("list1".getBytes(), Position.BEFORE, "newValue".getBytes(),
				"inserted".getBytes());
		assertThat(lInsertResult).isGreaterThan(0L);

		// Test lRem - remove elements
		connection.listCommands().rPush("list2".getBytes(), "a".getBytes(), "b".getBytes(), "a".getBytes(), "c".getBytes());
		Long lRemResult = connection.listCommands().lRem("list2".getBytes(), 2, "a".getBytes());
		assertThat(lRemResult).isEqualTo(2L);

		// Test lTrim - trim list to range
		connection.listCommands().lTrim("list2".getBytes(), 0, 1);
		assertThat(connection.listCommands().lLen("list2".getBytes())).isLessThanOrEqualTo(2L);
	}

	@Test
	void listMovementOperationsShouldWork() {
		// Set up lists
		connection.listCommands().rPush("{tag}list1".getBytes(), "a".getBytes(), "b".getBytes(), "c".getBytes());
		connection.listCommands().rPush("{tag}list2".getBytes(), "x".getBytes());

		// Test lMove - move element between lists
		byte[] lMoveResult = connection.listCommands().lMove("{tag}list1".getBytes(), "{tag}list2".getBytes(),
				Direction.RIGHT, Direction.LEFT);
		assertThat(lMoveResult).isEqualTo("c".getBytes());
		assertThat(connection.listCommands().lLen("{tag}list1".getBytes())).isEqualTo(2L);
		assertThat(connection.listCommands().lLen("{tag}list2".getBytes())).isEqualTo(2L);

		// Test rPopLPush - pop from right and push to left
		byte[] rPopLPushResult = connection.listCommands().rPopLPush("{tag}list1".getBytes(), "{tag}list2".getBytes());
		assertThat(rPopLPushResult).isEqualTo("b".getBytes());
	}

	@Test
	void blockingOperationsShouldWork() {
		// Set up list
		connection.listCommands().rPush("list1".getBytes(), "value1".getBytes(), "value2".getBytes());

		// Test bLPop - blocking left pop
		List<byte[]> bLPopResult = connection.listCommands().bLPop(1, "list1".getBytes());
		assertThat(bLPopResult).hasSize(2); // [key, value]
		assertThat(bLPopResult.get(1)).isEqualTo("value1".getBytes());

		// Test bRPop - blocking right pop
		List<byte[]> bRPopResult = connection.listCommands().bRPop(1, "list1".getBytes());
		assertThat(bRPopResult).hasSize(2);
		assertThat(bRPopResult.get(1)).isEqualTo("value2".getBytes());

		// Test bLMove - blocking move
		connection.listCommands().rPush("{tag}list2".getBytes(), "a".getBytes());
		connection.listCommands().rPush("{tag}list3".getBytes(), "x".getBytes());
		byte[] bLMoveResult = connection.listCommands().bLMove("{tag}list2".getBytes(), "{tag}list3".getBytes(),
				Direction.RIGHT, Direction.LEFT, 1);
		assertThat(bLMoveResult).isEqualTo("a".getBytes());

		// Test bRPopLPush - blocking right pop left push
		connection.listCommands().rPush("{tag}list4".getBytes(), "b".getBytes());
		connection.listCommands().rPush("{tag}list5".getBytes(), "y".getBytes());
		byte[] bRPopLPushResult = connection.listCommands().bRPopLPush(1, "{tag}list4".getBytes(), "{tag}list5".getBytes());
		assertThat(bRPopLPushResult).isEqualTo("b".getBytes());
	}
}
