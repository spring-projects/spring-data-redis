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
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.test.condition.EnabledOnRedisAvailable;
import org.springframework.data.redis.test.extension.JedisExtension;

import static org.assertj.core.api.Assertions.*;

/**
 * Integration tests for {@link JedisClientSetCommands}. Tests all methods in direct, transaction, and pipelined modes.
 *
 * @author Tihomir Mateev
 * @since 4.1
 */
@EnabledOnRedisAvailable
@ExtendWith(JedisExtension.class)
class JedisClientSetCommandsIntegrationTests {

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

	// ============ Basic Set Operations ============
	@Test
	void basicSetOperationsShouldWork() {
		// Test sAdd - add members to set
		Long addResult = connection.setCommands().sAdd("set1".getBytes(), "m1".getBytes(), "m2".getBytes(),
				"m3".getBytes());
		assertThat(addResult).isEqualTo(3L);

		// Test sCard - get set cardinality
		Long cardResult = connection.setCommands().sCard("set1".getBytes());
		assertThat(cardResult).isEqualTo(3L);

		// Test sIsMember - check membership
		Boolean isMember = connection.setCommands().sIsMember("set1".getBytes(), "m1".getBytes());
		assertThat(isMember).isTrue();
		Boolean notMember = connection.setCommands().sIsMember("set1".getBytes(), "m99".getBytes());
		assertThat(notMember).isFalse();

		// Test sMIsMember - check multiple memberships
		List<Boolean> mIsMember = connection.setCommands().sMIsMember("set1".getBytes(), "m1".getBytes(), "m99".getBytes(),
				"m2".getBytes());
		assertThat(mIsMember).containsExactly(true, false, true);

		// Test sMembers - get all members
		Set<byte[]> members = connection.setCommands().sMembers("set1".getBytes());
		assertThat(members).hasSize(3);

		// Test sRem - remove members
		Long remResult = connection.setCommands().sRem("set1".getBytes(), "m2".getBytes());
		assertThat(remResult).isEqualTo(1L);
		assertThat(connection.setCommands().sCard("set1".getBytes())).isEqualTo(2L);
	}

	@Test
	void setOperationsShouldWork() {
		// Set up sets
		connection.setCommands().sAdd("set1".getBytes(), "a".getBytes(), "b".getBytes(), "c".getBytes());
		connection.setCommands().sAdd("set2".getBytes(), "b".getBytes(), "c".getBytes(), "d".getBytes());
		connection.setCommands().sAdd("set3".getBytes(), "c".getBytes(), "d".getBytes(), "e".getBytes());

		// Test sDiff - difference
		Set<byte[]> diffResult = connection.setCommands().sDiff("set1".getBytes(), "set2".getBytes());
		assertThat(diffResult).hasSize(1); // Only "a"

		// Test sDiffStore - store difference
		Long diffStoreResult = connection.setCommands().sDiffStore("diffDst".getBytes(), "set1".getBytes(),
				"set2".getBytes());
		assertThat(diffStoreResult).isEqualTo(1L);

		// Test sInter - intersection
		Set<byte[]> interResult = connection.setCommands().sInter("set1".getBytes(), "set2".getBytes());
		assertThat(interResult).hasSize(2); // "b" and "c"

		// Test sInterStore - store intersection
		Long interStoreResult = connection.setCommands().sInterStore("interDst".getBytes(), "set1".getBytes(),
				"set2".getBytes());
		assertThat(interStoreResult).isEqualTo(2L);

		// Test sInterCard - intersection cardinality
		Long interCard = connection.setCommands().sInterCard("set1".getBytes(), "set2".getBytes());
		assertThat(interCard).isEqualTo(2L);

		// Test sUnion - union
		Set<byte[]> unionResult = connection.setCommands().sUnion("set1".getBytes(), "set2".getBytes());
		assertThat(unionResult).hasSize(4); // "a", "b", "c", "d"

		// Test sUnionStore - store union
		Long unionStoreResult = connection.setCommands().sUnionStore("unionDst".getBytes(), "set1".getBytes(),
				"set2".getBytes());
		assertThat(unionStoreResult).isEqualTo(4L);
	}

	@Test
	void setRandomAndPopOperationsShouldWork() {
		// Set up set
		connection.setCommands().sAdd("set4".getBytes(), "m1".getBytes(), "m2".getBytes(), "m3".getBytes(),
				"m4".getBytes());

		// Test sRandMember - get random member without removing
		byte[] randMember = connection.setCommands().sRandMember("set4".getBytes());
		assertThat(randMember).isNotNull();
		assertThat(connection.setCommands().sCard("set4".getBytes())).isEqualTo(4L); // Still 4

		// Test sRandMember with count
		List<byte[]> randMembers = connection.setCommands().sRandMember("set4".getBytes(), 2);
		assertThat(randMembers).hasSize(2);

		// Test sPop - pop random member
		byte[] poppedMember = connection.setCommands().sPop("set4".getBytes());
		assertThat(poppedMember).isNotNull();
		assertThat(connection.setCommands().sCard("set4".getBytes())).isEqualTo(3L); // Now 3

		// Test sPop with count
		List<byte[]> poppedMembers = connection.setCommands().sPop("set4".getBytes(), 2);
		assertThat(poppedMembers).hasSize(2);
		assertThat(connection.setCommands().sCard("set4".getBytes())).isEqualTo(1L); // Now 1
	}

	@Test
	void setMoveOperationShouldWork() {
		// Set up sets
		connection.setCommands().sAdd("src".getBytes(), "m1".getBytes(), "m2".getBytes());
		connection.setCommands().sAdd("dst".getBytes(), "m3".getBytes());

		// Test sMove - move member from one set to another
		Boolean moveResult = connection.setCommands().sMove("src".getBytes(), "dst".getBytes(), "m1".getBytes());
		assertThat(moveResult).isTrue();
		assertThat(connection.setCommands().sCard("src".getBytes())).isEqualTo(1L);
		assertThat(connection.setCommands().sCard("dst".getBytes())).isEqualTo(2L);
		assertThat(connection.setCommands().sIsMember("dst".getBytes(), "m1".getBytes())).isTrue();
	}

	@Test
	void transactionShouldExecuteAtomically() {
		// Set up initial state
		connection.setCommands().sAdd("txSet1".getBytes(), "a".getBytes(), "b".getBytes());
		connection.setCommands().sAdd("txSet2".getBytes(), "b".getBytes(), "c".getBytes());

		// Execute multiple set operations in a transaction
		connection.multi();
		connection.setCommands().sAdd("txSet1".getBytes(), "d".getBytes());
		connection.setCommands().sCard("txSet1".getBytes());
		connection.setCommands().sInter("txSet1".getBytes(), "txSet2".getBytes());
		connection.setCommands().sUnion("txSet1".getBytes(), "txSet2".getBytes());
		connection.setCommands().sIsMember("txSet1".getBytes(), "a".getBytes());
		List<Object> results = connection.exec();

		// Verify all commands executed
		assertThat(results).hasSize(5);
		assertThat(results.get(0)).isEqualTo(1L); // sAdd result
		assertThat(results.get(1)).isEqualTo(3L); // sCard result
		@SuppressWarnings("unchecked")
		Set<byte[]> interResult = (Set<byte[]>) results.get(2);
		assertThat(interResult).hasSize(1); // sInter result
		@SuppressWarnings("unchecked")
		Set<byte[]> unionResult = (Set<byte[]>) results.get(3);
		assertThat(unionResult).hasSize(4); // sUnion result
		assertThat(results.get(4)).isEqualTo(true); // sIsMember result
	}

	@Test
	void pipelineShouldExecuteMultipleCommands() {
		// Set up initial state
		connection.setCommands().sAdd("pipeSet1".getBytes(), "a".getBytes(), "b".getBytes());
		connection.setCommands().sAdd("pipeSet2".getBytes(), "b".getBytes(), "c".getBytes());

		// Execute multiple set operations in pipeline
		connection.openPipeline();
		connection.setCommands().sAdd("pipeSet1".getBytes(), "d".getBytes());
		connection.setCommands().sCard("pipeSet1".getBytes());
		connection.setCommands().sMembers("pipeSet1".getBytes());
		connection.setCommands().sInter("pipeSet1".getBytes(), "pipeSet2".getBytes());
		connection.setCommands().sRem("pipeSet1".getBytes(), "a".getBytes());
		List<Object> results = connection.closePipeline();

		// Verify all command results
		assertThat(results).hasSize(5);
		assertThat(results.get(0)).isEqualTo(1L); // sAdd result
		assertThat(results.get(1)).isEqualTo(3L); // sCard result
		@SuppressWarnings("unchecked")
		Set<byte[]> membersResult = (Set<byte[]>) results.get(2);
		assertThat(membersResult).hasSize(3); // sMembers result
		@SuppressWarnings("unchecked")
		Set<byte[]> interResult = (Set<byte[]>) results.get(3);
		assertThat(interResult).hasSize(1); // sInter result
		assertThat(results.get(4)).isEqualTo(1L); // sRem result
	}
}
