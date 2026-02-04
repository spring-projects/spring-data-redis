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

import java.util.Collections;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisClusterConfiguration;
import org.springframework.data.redis.connection.RedisClusterNode;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.test.condition.EnabledOnRedisClusterAvailable;
import org.springframework.data.redis.test.extension.JedisExtension;

import redis.clients.jedis.RedisClusterClient;

import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.redis.connection.ClusterTestVariables.*;

/**
 * Integration tests for {@link JedisClientClusterConnection}.
 * <p>
 * These tests verify that the cluster implementation works correctly with RedisClusterClient (Jedis 7.2+). Tests cover
 * basic operations, cluster-specific commands, and multi-key operations.
 *
 * @author Tihomir Mateev
 * @since 4.1
 */
@EnabledOnRedisClusterAvailable
@ExtendWith(JedisExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class JedisClientClusterConnectionIntegrationTests {

	private JedisClientConnectionFactory factory;
	private JedisClientClusterConnection connection;

	private static final byte[] KEY_1 = "key1".getBytes();
	private static final byte[] KEY_2 = "key2".getBytes();
	private static final byte[] VALUE_1 = "value1".getBytes();
	private static final byte[] VALUE_2 = "value2".getBytes();

	@BeforeEach
	void setUp() {
		RedisClusterConfiguration clusterConfig = new RedisClusterConfiguration();
		clusterConfig.addClusterNode(new RedisClusterNode(CLUSTER_HOST, MASTER_NODE_1_PORT));
		clusterConfig.addClusterNode(new RedisClusterNode(CLUSTER_HOST, MASTER_NODE_2_PORT));
		clusterConfig.addClusterNode(new RedisClusterNode(CLUSTER_HOST, MASTER_NODE_3_PORT));

		factory = new JedisClientConnectionFactory(clusterConfig);
		factory.afterPropertiesSet();
		factory.start();

		connection = (JedisClientClusterConnection) factory.getClusterConnection();
	}

	@AfterEach
	void tearDown() {
		try {
			// Clean up test keys
			if (connection != null && !connection.isClosed()) {
				connection.serverCommands().flushDb();
			}
		} catch (Exception e) {
			// Ignore cleanup errors
		}

		if (connection != null && !connection.isClosed()) {
			connection.close();
		}
		if (factory != null) {
			factory.destroy();
		}
	}

	// ========================================================================
	// Basic Connection Tests
	// ========================================================================

	@Test // GH-XXXX
	void connectionShouldBeCreated() {
		assertThat(connection).isNotNull();
		assertThat(connection.getNativeConnection()).isNotNull();
		assertThat(connection.getNativeConnection()).isInstanceOf(RedisClusterClient.class);
	}

	@Test // GH-XXXX
	void isClosedShouldReturnFalseInitially() {
		assertThat(connection.isClosed()).isFalse();
	}

	@Test // GH-XXXX
	void closeShouldMarkConnectionAsClosed() {
		connection.close();
		assertThat(connection.isClosed()).isTrue();
	}

	@Test // GH-XXXX
	void pingShouldWork() {
		String result = connection.ping();
		assertThat(result).isEqualTo("PONG");
	}

	@Test // GH-XXXX
	void pingNodeShouldWork() {
		RedisClusterNode node = new RedisClusterNode(CLUSTER_HOST, MASTER_NODE_1_PORT);
		String result = connection.ping(node);
		assertThat(result).isEqualTo("PONG");
	}

	// ========================================================================
	// String Commands Tests
	// ========================================================================

	@Test // GH-XXXX
	void stringCommandsShouldWork() {
		assertThat(connection.stringCommands()).isNotNull();

		// Test basic set/get
		Boolean setResult = connection.stringCommands().set(KEY_1, VALUE_1);
		assertThat(setResult).isTrue();

		byte[] getValue = connection.stringCommands().get(KEY_1);
		assertThat(getValue).isEqualTo(VALUE_1);
	}

	@Test // GH-XXXX
	void stringCommandsMultipleKeysShouldWork() {
		connection.stringCommands().set(KEY_1, VALUE_1);
		connection.stringCommands().set(KEY_2, VALUE_2);

		assertThat(connection.stringCommands().get(KEY_1)).isEqualTo(VALUE_1);
		assertThat(connection.stringCommands().get(KEY_2)).isEqualTo(VALUE_2);
	}

	// ========================================================================
	// Hash Commands Tests
	// ========================================================================

	@Test // GH-XXXX
	void hashCommandsShouldWork() {
		assertThat(connection.hashCommands()).isNotNull();

		byte[] hashKey = "hash1".getBytes();
		byte[] field = "field1".getBytes();
		byte[] value = "hvalue1".getBytes();

		Boolean hsetResult = connection.hashCommands().hSet(hashKey, field, value);
		assertThat(hsetResult).isTrue();

		byte[] hgetResult = connection.hashCommands().hGet(hashKey, field);
		assertThat(hgetResult).isEqualTo(value);
	}

	// ========================================================================
	// List Commands Tests
	// ========================================================================

	@Test // GH-XXXX
	void listCommandsShouldWork() {
		assertThat(connection.listCommands()).isNotNull();

		byte[] listKey = "list1".getBytes();
		byte[] value = "lvalue1".getBytes();

		Long lpushResult = connection.listCommands().lPush(listKey, value);
		assertThat(lpushResult).isEqualTo(1L);

		byte[] lpopResult = connection.listCommands().lPop(listKey);
		assertThat(lpopResult).isEqualTo(value);
	}

	// ========================================================================
	// Set Commands Tests
	// ========================================================================

	@Test // GH-XXXX
	void setCommandsShouldWork() {
		assertThat(connection.setCommands()).isNotNull();

		byte[] setKey = "set1".getBytes();
		byte[] member = "member1".getBytes();

		Long saddResult = connection.setCommands().sAdd(setKey, member);
		assertThat(saddResult).isEqualTo(1L);

		Boolean sismemberResult = connection.setCommands().sIsMember(setKey, member);
		assertThat(sismemberResult).isTrue();
	}

	// ========================================================================
	// ZSet Commands Tests
	// ========================================================================

	@Test // GH-XXXX
	void zsetCommandsShouldWork() {
		assertThat(connection.zSetCommands()).isNotNull();

		byte[] zsetKey = "zset1".getBytes();
		byte[] member = "zmember1".getBytes();
		double score = 1.5;

		Boolean zaddResult = connection.zSetCommands().zAdd(zsetKey, score, member);
		assertThat(zaddResult).isTrue();

		Double zscoreResult = connection.zSetCommands().zScore(zsetKey, member);
		assertThat(zscoreResult).isEqualTo(score);
	}

	// ========================================================================
	// Key Commands Tests
	// ========================================================================

	@Test // GH-XXXX
	void keyCommandsShouldWork() {
		assertThat(connection.keyCommands()).isNotNull();

		connection.stringCommands().set(KEY_1, VALUE_1);

		Boolean existsResult = connection.keyCommands().exists(KEY_1);
		assertThat(existsResult).isTrue();

		Long delResult = connection.keyCommands().del(KEY_1);
		assertThat(delResult).isEqualTo(1L);

		existsResult = connection.keyCommands().exists(KEY_1);
		assertThat(existsResult).isFalse();
	}

	@Test // GH-XXXX
	void keyCommandsExpireShouldWork() {
		connection.stringCommands().set(KEY_1, VALUE_1);

		Boolean expireResult = connection.keyCommands().expire(KEY_1, 10);
		assertThat(expireResult).isTrue();

		Long ttl = connection.keyCommands().ttl(KEY_1);
		assertThat(ttl).isGreaterThan(0L).isLessThanOrEqualTo(10L);
	}

	// ========================================================================
	// Server Commands Tests
	// ========================================================================

	@Test // GH-XXXX
	void serverCommandsShouldWork() {
		assertThat(connection.serverCommands()).isNotNull();

		// Test dbSize - should aggregate across all nodes
		Long dbSize = connection.serverCommands().dbSize();
		assertThat(dbSize).isNotNull().isGreaterThanOrEqualTo(0L);
	}

	@Test // GH-XXXX
	void serverCommandsInfoShouldWork() {
		java.util.Properties info = connection.serverCommands().info();
		assertThat(info).isNotNull().isNotEmpty();
	}

	@Test // GH-XXXX
	void serverCommandsFlushDbShouldWork() {
		connection.stringCommands().set(KEY_1, VALUE_1);
		assertThat(connection.keyCommands().exists(KEY_1)).isTrue();

		connection.serverCommands().flushDb();

		assertThat(connection.keyCommands().exists(KEY_1)).isFalse();
	}

	// ========================================================================
	// Geo Commands Tests
	// ========================================================================

	@Test // GH-XXXX
	void geoCommandsShouldWork() {
		assertThat(connection.geoCommands()).isNotNull();

		byte[] geoKey = "geo1".getBytes();
		byte[] member = "location1".getBytes();

		Long geoaddResult = connection.geoCommands().geoAdd(geoKey,
				new RedisGeoCommands.GeoLocation<>(member, new Point(13.361389, 38.115556)));
		assertThat(geoaddResult).isEqualTo(1L);

		Distance distance = connection.geoCommands().geoDist(geoKey, member, member);
		assertThat(distance).isNotNull();
		assertThat(distance.getValue()).isEqualTo(0.0);
	}

	// ========================================================================
	// HyperLogLog Commands Tests
	// ========================================================================

	@Test // GH-XXXX
	void hyperLogLogCommandsShouldWork() {
		assertThat(connection.hyperLogLogCommands()).isNotNull();

		byte[] hllKey = "hll1".getBytes();
		byte[] value = "element1".getBytes();

		Long pfaddResult = connection.hyperLogLogCommands().pfAdd(hllKey, value);
		assertThat(pfaddResult).isEqualTo(1L);

		Long pfcountResult = connection.hyperLogLogCommands().pfCount(hllKey);
		assertThat(pfcountResult).isEqualTo(1L);
	}

	// ========================================================================
	// Stream Commands Tests
	// ========================================================================

	@Test // GH-XXXX
	void streamCommandsShouldWork() {
		assertThat(connection.streamCommands()).isNotNull();

		byte[] streamKey = "stream1".getBytes();
		byte[] field = "field1".getBytes();
		byte[] value = "svalue1".getBytes();

		RecordId recordId = connection.streamCommands().xAdd(streamKey, Collections.singletonMap(field, value));
		assertThat(recordId).isNotNull();

		Long xlenResult = connection.streamCommands().xLen(streamKey);
		assertThat(xlenResult).isEqualTo(1L);
	}

	// ========================================================================
	// Scripting Commands Tests
	// ========================================================================

	@Test // GH-XXXX
	void scriptingCommandsShouldWork() {
		assertThat(connection.scriptingCommands()).isNotNull();

		byte[] script = "return 'hello'".getBytes();

		byte[] result = connection.scriptingCommands().eval(script,
				org.springframework.data.redis.connection.ReturnType.VALUE, 0);
		assertThat(new String(result)).isEqualTo("hello");
	}

	// ========================================================================
	// Cluster-Specific Tests
	// ========================================================================

	@Test // GH-XXXX
	void clusterGetNodesShouldWork() {
		Iterable<RedisClusterNode> nodes = connection.clusterGetNodes();
		assertThat(nodes).isNotNull();
		assertThat(nodes).isNotEmpty();
		assertThat(nodes).hasSizeGreaterThanOrEqualTo(3); // At least 3 master nodes
	}

	@Test // GH-XXXX
	void clusterGetSlotForKeyShouldWork() {
		Integer slot = connection.clusterGetSlotForKey(KEY_1);
		assertThat(slot).isNotNull();
		assertThat(slot).isBetween(0, 16383);
	}

	@Test // GH-XXXX
	void clusterGetNodeForSlotShouldWork() {
		Integer slot = connection.clusterGetSlotForKey(KEY_1);
		RedisClusterNode node = connection.clusterGetNodeForSlot(slot);
		assertThat(node).isNotNull();
		assertThat(node.isMaster()).isTrue();
	}

	@Test // GH-XXXX
	void clusterGetNodeForKeyShouldWork() {
		RedisClusterNode node = connection.clusterGetNodeForKey(KEY_1);
		assertThat(node).isNotNull();
		assertThat(node.isMaster()).isTrue();
	}
}
