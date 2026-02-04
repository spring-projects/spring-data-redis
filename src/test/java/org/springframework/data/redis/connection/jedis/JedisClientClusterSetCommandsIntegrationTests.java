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
import java.util.Set;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.connection.RedisClusterConfiguration;
import org.springframework.data.redis.connection.RedisClusterConnection;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.test.condition.EnabledOnRedisClusterAvailable;
import org.springframework.data.redis.test.extension.JedisExtension;

import static org.assertj.core.api.Assertions.*;

/**
 * Integration tests for {@link JedisClientSetCommands} in cluster mode. Tests all methods in direct and pipelined modes
 * (transactions not supported in cluster).
 *
 * @author Tihomir Mateev
 * @since 4.1
 */
@EnabledOnRedisClusterAvailable
@ExtendWith(JedisExtension.class)
class JedisClientClusterSetCommandsIntegrationTests {

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

	// ============ Basic Set Operations ============
	@Test
	void basicSetOperationsShouldWork() {
		// Test sAdd - add members
		Long sAddResult = connection.setCommands().sAdd("set1".getBytes(), "member1".getBytes(), "member2".getBytes(),
				"member3".getBytes());
		assertThat(sAddResult).isEqualTo(3L);

		// Test sMembers - get all members
		Set<byte[]> sMembersResult = connection.setCommands().sMembers("set1".getBytes());
		assertThat(sMembersResult).hasSize(3);

		// Test sIsMember - check membership
		Boolean sIsMemberResult = connection.setCommands().sIsMember("set1".getBytes(), "member1".getBytes());
		assertThat(sIsMemberResult).isTrue();
		Boolean sIsMemberResult2 = connection.setCommands().sIsMember("set1".getBytes(), "nonexistent".getBytes());
		assertThat(sIsMemberResult2).isFalse();

		// Test sMIsMember - check multiple memberships
		List<Boolean> sMIsMemberResult = connection.setCommands().sMIsMember("set1".getBytes(), "member1".getBytes(),
				"nonexistent".getBytes());
		assertThat(sMIsMemberResult).containsExactly(true, false);

		// Test sCard - get cardinality
		Long sCardResult = connection.setCommands().sCard("set1".getBytes());
		assertThat(sCardResult).isEqualTo(3L);

		// Test sRem - remove members
		Long sRemResult = connection.setCommands().sRem("set1".getBytes(), "member1".getBytes());
		assertThat(sRemResult).isEqualTo(1L);
		assertThat(connection.setCommands().sCard("set1".getBytes())).isEqualTo(2L);
	}

	@Test
	void setOperationsWithMultipleSetsShouldWork() {
		// Set up sets
		connection.setCommands().sAdd("{tag}set1".getBytes(), "a".getBytes(), "b".getBytes(), "c".getBytes());
		connection.setCommands().sAdd("{tag}set2".getBytes(), "b".getBytes(), "c".getBytes(), "d".getBytes());
		connection.setCommands().sAdd("{tag}set3".getBytes(), "c".getBytes(), "d".getBytes(), "e".getBytes());

		// Test sInter - intersection
		Set<byte[]> sInterResult = connection.setCommands().sInter("{tag}set1".getBytes(), "{tag}set2".getBytes());
		assertThat(sInterResult).hasSize(2); // b, c

		// Test sInterStore - intersection and store
		Long sInterStoreResult = connection.setCommands().sInterStore("{tag}dest1".getBytes(), "{tag}set1".getBytes(),
				"{tag}set2".getBytes());
		assertThat(sInterStoreResult).isEqualTo(2L);

		// Test sUnion - union
		Set<byte[]> sUnionResult = connection.setCommands().sUnion("{tag}set1".getBytes(), "{tag}set2".getBytes());
		assertThat(sUnionResult).hasSize(4); // a, b, c, d

		// Test sUnionStore - union and store
		Long sUnionStoreResult = connection.setCommands().sUnionStore("{tag}dest2".getBytes(), "{tag}set1".getBytes(),
				"{tag}set2".getBytes());
		assertThat(sUnionStoreResult).isEqualTo(4L);

		// Test sDiff - difference
		Set<byte[]> sDiffResult = connection.setCommands().sDiff("{tag}set1".getBytes(), "{tag}set2".getBytes());
		assertThat(sDiffResult).hasSize(1); // a

		// Test sDiffStore - difference and store
		Long sDiffStoreResult = connection.setCommands().sDiffStore("{tag}dest3".getBytes(), "{tag}set1".getBytes(),
				"{tag}set2".getBytes());
		assertThat(sDiffStoreResult).isEqualTo(1L);
	}

	@Test
	void setMovementOperationsShouldWork() {
		// Set up sets
		connection.setCommands().sAdd("{tag}set1".getBytes(), "a".getBytes(), "b".getBytes(), "c".getBytes());
		connection.setCommands().sAdd("{tag}set2".getBytes(), "x".getBytes());

		// Test sMove - move member between sets
		Boolean sMoveResult = connection.setCommands().sMove("{tag}set1".getBytes(), "{tag}set2".getBytes(),
				"a".getBytes());
		assertThat(sMoveResult).isTrue();
		assertThat(connection.setCommands().sCard("{tag}set1".getBytes())).isEqualTo(2L);
		assertThat(connection.setCommands().sCard("{tag}set2".getBytes())).isEqualTo(2L);

		// Test sPop - pop random member
		byte[] sPopResult = connection.setCommands().sPop("{tag}set1".getBytes());
		assertThat(sPopResult).isNotNull();

		// Test sPop with count
		connection.setCommands().sAdd("{tag}set3".getBytes(), "a".getBytes(), "b".getBytes(), "c".getBytes(),
				"d".getBytes());
		List<byte[]> sPopCountResult = connection.setCommands().sPop("{tag}set3".getBytes(), 2);
		assertThat(sPopCountResult).hasSize(2);

		// Test sRandMember - get random member
		connection.setCommands().sAdd("set4".getBytes(), "a".getBytes(), "b".getBytes(), "c".getBytes());
		byte[] sRandMemberResult = connection.setCommands().sRandMember("set4".getBytes());
		assertThat(sRandMemberResult).isNotNull();

		// Test sRandMember with count
		List<byte[]> sRandMemberCountResult = connection.setCommands().sRandMember("set4".getBytes(), 2);
		assertThat(sRandMemberCountResult).hasSize(2);
	}

	@Test
	void setScanOperationsShouldWork() {
		// Set up set with many members
		for (int i = 0; i < 20; i++) {
			connection.setCommands().sAdd("set1".getBytes(), ("member" + i).getBytes());
		}

		// Test sScan - scan set members
		Cursor<byte[]> cursor = connection.setCommands().sScan("set1".getBytes(),
				ScanOptions.scanOptions().count(5).build());
		assertThat(cursor).isNotNull();
		int count = 0;
		while (cursor.hasNext()) {
			cursor.next();
			count++;
		}
		assertThat(count).isEqualTo(20);
	}
}
