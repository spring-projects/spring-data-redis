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
import org.springframework.data.domain.Range;
import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.connection.RedisClusterConfiguration;
import org.springframework.data.redis.connection.RedisClusterConnection;
import org.springframework.data.redis.connection.zset.Aggregate;
import org.springframework.data.redis.connection.zset.Tuple;
import org.springframework.data.redis.connection.zset.Weights;
import org.springframework.data.redis.test.condition.EnabledOnRedisClusterAvailable;
import org.springframework.data.redis.test.extension.JedisExtension;

import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.redis.connection.RedisZSetCommands.*;

/**
 * Integration tests for {@link JedisClientZSetCommands} in cluster mode. Tests all methods in direct and pipelined
 * modes (transactions not supported in cluster).
 *
 * @author Tihomir Mateev
 * @since 4.1
 */
@EnabledOnRedisClusterAvailable
@ExtendWith(JedisExtension.class)
class JedisClientClusterZSetCommandsIntegrationTests {

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

	// ============ Basic ZSet Operations ============
	@Test
	void basicZSetOperationsShouldWork() {
		// Test zAdd - add members with scores
		Boolean zAddResult = connection.zSetCommands().zAdd("zset1".getBytes(), 1.0, "member1".getBytes(),
				ZAddArgs.empty());
		assertThat(zAddResult).isTrue();
		Long zAddMultiResult = connection.zSetCommands().zAdd("zset1".getBytes(),
				Set.of(Tuple.of("member2".getBytes(), 2.0), Tuple.of("member3".getBytes(), 3.0)), ZAddArgs.empty());
		assertThat(zAddMultiResult).isEqualTo(2L);

		// Test zCard - get cardinality
		Long zCardResult = connection.zSetCommands().zCard("zset1".getBytes());
		assertThat(zCardResult).isEqualTo(3L);

		// Test zScore - get member score
		Double zScoreResult = connection.zSetCommands().zScore("zset1".getBytes(), "member2".getBytes());
		assertThat(zScoreResult).isEqualTo(2.0);

		// Test zMScore - get multiple scores
		List<Double> zMScoreResult = connection.zSetCommands().zMScore("zset1".getBytes(), "member1".getBytes(),
				"member3".getBytes());
		assertThat(zMScoreResult).containsExactly(1.0, 3.0);

		// Test zRank - get rank (ascending)
		Long zRankResult = connection.zSetCommands().zRank("zset1".getBytes(), "member2".getBytes());
		assertThat(zRankResult).isEqualTo(1L);

		// Test zRevRank - get rank (descending)
		Long zRevRankResult = connection.zSetCommands().zRevRank("zset1".getBytes(), "member2".getBytes());
		assertThat(zRevRankResult).isEqualTo(1L);

		// Test zRem - remove members
		Long zRemResult = connection.zSetCommands().zRem("zset1".getBytes(), "member1".getBytes());
		assertThat(zRemResult).isEqualTo(1L);
		assertThat(connection.zSetCommands().zCard("zset1".getBytes())).isEqualTo(2L);
	}

	@Test
	void zSetRangeOperationsShouldWork() {
		// Set up zset
		connection.zSetCommands().zAdd("zset1".getBytes(), Set.of(Tuple.of("a".getBytes(), 1.0),
				Tuple.of("b".getBytes(), 2.0), Tuple.of("c".getBytes(), 3.0), Tuple.of("d".getBytes(), 4.0)), ZAddArgs.empty());

		// Test zRange - get range by index
		Set<byte[]> zRangeResult = connection.zSetCommands().zRange("zset1".getBytes(), 0, 2);
		assertThat(zRangeResult).hasSize(3);

		// Test zRangeWithScores - get range with scores
		Set<Tuple> zRangeWithScoresResult = connection.zSetCommands().zRangeWithScores("zset1".getBytes(), 0, 2);
		assertThat(zRangeWithScoresResult).hasSize(3);

		// Test zRevRange - get reverse range
		Set<byte[]> zRevRangeResult = connection.zSetCommands().zRevRange("zset1".getBytes(), 0, 2);
		assertThat(zRevRangeResult).hasSize(3);

		// Test zRevRangeWithScores - get reverse range with scores
		Set<Tuple> zRevRangeWithScoresResult = connection.zSetCommands().zRevRangeWithScores("zset1".getBytes(), 0, 2);
		assertThat(zRevRangeWithScoresResult).hasSize(3);

		// Test zRangeByScore - get range by score
		Set<byte[]> zRangeByScoreResult = connection.zSetCommands().zRangeByScore("zset1".getBytes(),
				Range.closed(1.0, 3.0));
		assertThat(zRangeByScoreResult).hasSize(3);

		// Test zRangeByScoreWithScores
		Set<Tuple> zRangeByScoreWithScoresResult = connection.zSetCommands().zRangeByScoreWithScores("zset1".getBytes(),
				Range.closed(1.0, 3.0));
		assertThat(zRangeByScoreWithScoresResult).hasSize(3);

		// Test zRevRangeByScore
		Set<byte[]> zRevRangeByScoreResult = connection.zSetCommands().zRevRangeByScore("zset1".getBytes(),
				Range.closed(1.0, 3.0));
		assertThat(zRevRangeByScoreResult).hasSize(3);
	}

	@Test
	void zSetCountAndIncrementOperationsShouldWork() {
		// Set up zset
		connection.zSetCommands().zAdd("zset1".getBytes(),
				Set.of(Tuple.of("a".getBytes(), 1.0), Tuple.of("b".getBytes(), 2.0), Tuple.of("c".getBytes(), 3.0)),
				ZAddArgs.empty());

		// Test zCount - count members in score range
		Long zCountResult = connection.zSetCommands().zCount("zset1".getBytes(), Range.closed(1.0, 2.0));
		assertThat(zCountResult).isEqualTo(2L);

		// Test zLexCount - count members in lex range
		Long zLexCountResult = connection.zSetCommands().zLexCount("zset1".getBytes(), Range.unbounded());
		assertThat(zLexCountResult).isEqualTo(3L);

		// Test zIncrBy - increment member score
		Double zIncrByResult = connection.zSetCommands().zIncrBy("zset1".getBytes(), 5.0, "a".getBytes());
		assertThat(zIncrByResult).isEqualTo(6.0);
		assertThat(connection.zSetCommands().zScore("zset1".getBytes(), "a".getBytes())).isEqualTo(6.0);
	}

	@Test
	void zSetRemovalOperationsShouldWork() {
		// Set up zset
		connection.zSetCommands().zAdd(
				"zset1".getBytes(), Set.of(Tuple.of("a".getBytes(), 1.0), Tuple.of("b".getBytes(), 2.0),
						Tuple.of("c".getBytes(), 3.0), Tuple.of("d".getBytes(), 4.0), Tuple.of("e".getBytes(), 5.0)),
				ZAddArgs.empty());

		// Test zRemRange - remove by rank range
		Long zRemRangeResult = connection.zSetCommands().zRemRange("zset1".getBytes(), 0, 1);
		assertThat(zRemRangeResult).isEqualTo(2L);

		// Test zRemRangeByScore - remove by score range
		connection.zSetCommands().zAdd("zset2".getBytes(),
				Set.of(Tuple.of("a".getBytes(), 1.0), Tuple.of("b".getBytes(), 2.0), Tuple.of("c".getBytes(), 3.0)),
				ZAddArgs.empty());
		Long zRemRangeByScoreResult = connection.zSetCommands().zRemRangeByScore("zset2".getBytes(),
				Range.closed(1.0, 2.0));
		assertThat(zRemRangeByScoreResult).isEqualTo(2L);

		// Test zRemRangeByLex - remove by lex range
		connection.zSetCommands().zAdd("zset3".getBytes(),
				Set.of(Tuple.of("a".getBytes(), 0.0), Tuple.of("b".getBytes(), 0.0), Tuple.of("c".getBytes(), 0.0)),
				ZAddArgs.empty());
		Long zRemRangeByLexResult = connection.zSetCommands().zRemRangeByLex("zset3".getBytes(),
				Range.closed("a".getBytes(), "b".getBytes()));
		assertThat(zRemRangeByLexResult).isGreaterThanOrEqualTo(1L);
	}

	@Test
	void zSetPopOperationsShouldWork() {
		// Set up zset
		connection.zSetCommands().zAdd("zset1".getBytes(),
				Set.of(Tuple.of("a".getBytes(), 1.0), Tuple.of("b".getBytes(), 2.0), Tuple.of("c".getBytes(), 3.0)),
				ZAddArgs.empty());

		// Test zPopMin - pop minimum
		Tuple zPopMinResult = connection.zSetCommands().zPopMin("zset1".getBytes());
		assertThat(zPopMinResult).isNotNull();
		assertThat(zPopMinResult.getScore()).isEqualTo(1.0);

		// Test zPopMin with count
		connection.zSetCommands().zAdd("zset2".getBytes(),
				Set.of(Tuple.of("a".getBytes(), 1.0), Tuple.of("b".getBytes(), 2.0), Tuple.of("c".getBytes(), 3.0)),
				ZAddArgs.empty());
		Set<Tuple> zPopMinCountResult = connection.zSetCommands().zPopMin("zset2".getBytes(), 2);
		assertThat(zPopMinCountResult).hasSize(2);

		// Test zPopMax - pop maximum
		Tuple zPopMaxResult = connection.zSetCommands().zPopMax("zset1".getBytes());
		assertThat(zPopMaxResult).isNotNull();
		assertThat(zPopMaxResult.getScore()).isEqualTo(3.0);

		// Test zPopMax with count
		connection.zSetCommands().zAdd("zset3".getBytes(),
				Set.of(Tuple.of("a".getBytes(), 1.0), Tuple.of("b".getBytes(), 2.0), Tuple.of("c".getBytes(), 3.0)),
				ZAddArgs.empty());
		Set<Tuple> zPopMaxCountResult = connection.zSetCommands().zPopMax("zset3".getBytes(), 2);
		assertThat(zPopMaxCountResult).hasSize(2);
	}

	@Test
	void zSetSetOperationsShouldWork() {
		// Set up zsets
		connection.zSetCommands().zAdd("{tag}zset1".getBytes(),
				Set.of(Tuple.of("a".getBytes(), 1.0), Tuple.of("b".getBytes(), 2.0)), ZAddArgs.empty());
		connection.zSetCommands().zAdd("{tag}zset2".getBytes(),
				Set.of(Tuple.of("b".getBytes(), 3.0), Tuple.of("c".getBytes(), 4.0)), ZAddArgs.empty());

		// Test zUnionStore - union and store
		Long zUnionStoreResult = connection.zSetCommands().zUnionStore("{tag}dest1".getBytes(), "{tag}zset1".getBytes(),
				"{tag}zset2".getBytes());
		assertThat(zUnionStoreResult).isEqualTo(3L);

		// Test zUnionStore with weights
		Long zUnionStoreWeightsResult = connection.zSetCommands().zUnionStore("{tag}dest2".getBytes(), Aggregate.SUM,
				Weights.of(2, 3), "{tag}zset1".getBytes(), "{tag}zset2".getBytes());
		assertThat(zUnionStoreWeightsResult).isEqualTo(3L);

		// Test zInterStore - intersection and store
		Long zInterStoreResult = connection.zSetCommands().zInterStore("{tag}dest3".getBytes(), "{tag}zset1".getBytes(),
				"{tag}zset2".getBytes());
		assertThat(zInterStoreResult).isEqualTo(1L); // only 'b' is common

		// Test zDiffStore - difference and store
		Long zDiffStoreResult = connection.zSetCommands().zDiffStore("{tag}dest4".getBytes(), "{tag}zset1".getBytes(),
				"{tag}zset2".getBytes());
		assertThat(zDiffStoreResult).isEqualTo(1L); // only 'a' is in zset1 but not zset2
	}

	@Test
	void zSetRandomOperationsShouldWork() {
		// Set up zset
		connection.zSetCommands().zAdd("zset1".getBytes(),
				Set.of(Tuple.of("a".getBytes(), 1.0), Tuple.of("b".getBytes(), 2.0), Tuple.of("c".getBytes(), 3.0)),
				ZAddArgs.empty());

		// Test zRandMember - get random member
		byte[] zRandMemberResult = connection.zSetCommands().zRandMember("zset1".getBytes());
		assertThat(zRandMemberResult).isNotNull();

		// Test zRandMember with count
		List<byte[]> zRandMemberCountResult = connection.zSetCommands().zRandMember("zset1".getBytes(), 2);
		assertThat(zRandMemberCountResult).hasSize(2);

		// Test zRandMemberWithScore - get random member with score
		Tuple zRandMemberWithScoreResult = connection.zSetCommands().zRandMemberWithScore("zset1".getBytes());
		assertThat(zRandMemberWithScoreResult).isNotNull();

		// Test zRandMemberWithScore with count
		List<Tuple> zRandMemberWithScoreCountResult = connection.zSetCommands().zRandMemberWithScore("zset1".getBytes(), 2);
		assertThat(zRandMemberWithScoreCountResult).hasSize(2);
	}
}
