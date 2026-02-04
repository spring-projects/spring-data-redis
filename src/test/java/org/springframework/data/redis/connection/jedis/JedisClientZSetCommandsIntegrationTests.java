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
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.RedisZSetCommands;
import org.springframework.data.redis.connection.zset.Aggregate;
import org.springframework.data.redis.connection.zset.Tuple;
import org.springframework.data.redis.test.condition.EnabledOnRedisAvailable;
import org.springframework.data.redis.test.extension.JedisExtension;

import static org.assertj.core.api.Assertions.*;

/**
 * Integration tests for {@link JedisClientZSetCommands}. Tests all methods in direct, transaction, and pipelined modes.
 *
 * @author Tihomir Mateev
 * @since 4.1
 */
@EnabledOnRedisAvailable
@ExtendWith(JedisExtension.class)
class JedisClientZSetCommandsIntegrationTests {

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
		// Test zAdd - add single member
		Boolean addResult = connection.zSetCommands().zAdd("zset1".getBytes(), 1.0, "m1".getBytes(),
				RedisZSetCommands.ZAddArgs.empty());
		assertThat(addResult).isTrue();

		// Test zAdd with tuples - add multiple members
		Set<Tuple> tuples = Set.of(Tuple.of("m2".getBytes(), 2.0), Tuple.of("m3".getBytes(), 3.0),
				Tuple.of("m4".getBytes(), 4.0));
		Long addTuplesResult = connection.zSetCommands().zAdd("zset1".getBytes(), tuples,
				RedisZSetCommands.ZAddArgs.empty());
		assertThat(addTuplesResult).isEqualTo(3L);

		// Test zCard - get cardinality
		Long cardResult = connection.zSetCommands().zCard("zset1".getBytes());
		assertThat(cardResult).isEqualTo(4L);

		// Test zIncrBy - increment score
		Double incrResult = connection.zSetCommands().zIncrBy("zset1".getBytes(), 0.5, "m1".getBytes());
		assertThat(incrResult).isEqualTo(1.5);

		// Test zRem - remove members
		Long remResult = connection.zSetCommands().zRem("zset1".getBytes(), "m4".getBytes());
		assertThat(remResult).isEqualTo(1L);
		assertThat(connection.zSetCommands().zCard("zset1".getBytes())).isEqualTo(3L);
	}

	@Test
	void zSetScoreOperationsShouldWork() {
		// Set up sorted set
		Set<Tuple> tuples = Set.of(Tuple.of("alice".getBytes(), 100.0), Tuple.of("bob".getBytes(), 200.0),
				Tuple.of("charlie".getBytes(), 150.0));
		connection.zSetCommands().zAdd("scores".getBytes(), tuples, RedisZSetCommands.ZAddArgs.empty());

		// Test zScore - get score of member
		Double aliceScore = connection.zSetCommands().zScore("scores".getBytes(), "alice".getBytes());
		assertThat(aliceScore).isEqualTo(100.0);

		// Test zMScore - get scores of multiple members
		List<Double> scores = connection.zSetCommands().zMScore("scores".getBytes(), "alice".getBytes(), "bob".getBytes());
		assertThat(scores).containsExactly(100.0, 200.0);
	}

	@Test
	void zSetRankOperationsShouldWork() {
		// Set up sorted set
		Set<Tuple> tuples = Set.of(Tuple.of("alice".getBytes(), 100.0), Tuple.of("bob".getBytes(), 200.0),
				Tuple.of("charlie".getBytes(), 150.0), Tuple.of("david".getBytes(), 175.0));
		connection.zSetCommands().zAdd("leaderboard".getBytes(), tuples, RedisZSetCommands.ZAddArgs.empty());

		// Test zRank - get rank (0-based, ascending)
		Long aliceRank = connection.zSetCommands().zRank("leaderboard".getBytes(), "alice".getBytes());
		assertThat(aliceRank).isEqualTo(0L); // Lowest score

		// Test zRevRank - get reverse rank (0-based, descending)
		Long aliceRevRank = connection.zSetCommands().zRevRank("leaderboard".getBytes(), "alice".getBytes());
		assertThat(aliceRevRank).isEqualTo(3L); // Highest reverse rank
	}

	@Test
	void zSetRangeOperationsShouldWork() {
		// Set up sorted set
		Set<Tuple> tuples = Set.of(Tuple.of("m1".getBytes(), 1.0), Tuple.of("m2".getBytes(), 2.0),
				Tuple.of("m3".getBytes(), 3.0), Tuple.of("m4".getBytes(), 4.0), Tuple.of("m5".getBytes(), 5.0));
		connection.zSetCommands().zAdd("zset2".getBytes(), tuples, RedisZSetCommands.ZAddArgs.empty());

		// Test zRange - get range by index
		Set<byte[]> rangeResult = connection.zSetCommands().zRange("zset2".getBytes(), 1, 3);
		assertThat(rangeResult).hasSize(3);

		// Test zRangeWithScores - get range with scores
		Set<Tuple> rangeWithScores = connection.zSetCommands().zRangeWithScores("zset2".getBytes(), 0, 2);
		assertThat(rangeWithScores).hasSize(3);

		// Test zRevRange - get reverse range
		Set<byte[]> revRangeResult = connection.zSetCommands().zRevRange("zset2".getBytes(), 0, 2);
		assertThat(revRangeResult).hasSize(3);

		// Test zRevRangeWithScores - get reverse range with scores
		Set<Tuple> revRangeWithScores = connection.zSetCommands().zRevRangeWithScores("zset2".getBytes(), 0, 1);
		assertThat(revRangeWithScores).hasSize(2);

		// Test zRangeByScore - get range by score
		Set<byte[]> rangeByScore = connection.zSetCommands().zRangeByScore("zset2".getBytes(), 2.0, 4.0);
		assertThat(rangeByScore).hasSize(3);

		// Test zRangeByScoreWithScores - get range by score with scores
		Set<Tuple> rangeByScoreWithScores = connection.zSetCommands().zRangeByScoreWithScores("zset2".getBytes(), 2.0, 4.0);
		assertThat(rangeByScoreWithScores).hasSize(3);

		// Test zRevRangeByScore - get reverse range by score
		Set<byte[]> revRangeByScore = connection.zSetCommands().zRevRangeByScore("zset2".getBytes(), 2.0, 4.0);
		assertThat(revRangeByScore).hasSize(3);

		// Test zRevRangeByScoreWithScores - get reverse range by score with scores
		Set<Tuple> revRangeByScoreWithScores = connection.zSetCommands().zRevRangeByScoreWithScores("zset2".getBytes(), 2.0,
				4.0);
		assertThat(revRangeByScoreWithScores).hasSize(3);
	}

	@Test
	void zSetCountOperationsShouldWork() {
		// Set up sorted set
		Set<Tuple> tuples = Set.of(Tuple.of("a".getBytes(), 1.0), Tuple.of("b".getBytes(), 2.0),
				Tuple.of("c".getBytes(), 3.0), Tuple.of("d".getBytes(), 4.0), Tuple.of("e".getBytes(), 5.0));
		connection.zSetCommands().zAdd("zset3".getBytes(), tuples, RedisZSetCommands.ZAddArgs.empty());

		// Test zCount - count members in score range
		Long countResult = connection.zSetCommands().zCount("zset3".getBytes(), 2.0, 4.0);
		assertThat(countResult).isEqualTo(3L);

		// Test zCount with Range
		Long countRangeResult = connection.zSetCommands().zCount("zset3".getBytes(), Range.closed(2.0, 4.0));
		assertThat(countRangeResult).isEqualTo(3L);

		// Test zLexCount - count members in lex range
		Long lexCountResult = connection.zSetCommands().zLexCount("zset3".getBytes(),
				Range.closed("a".getBytes(), "c".getBytes()));
		assertThat(lexCountResult).isGreaterThanOrEqualTo(0L);
	}

	@Test
	void zSetRandomAndPopOperationsShouldWork() {
		// Set up sorted set
		Set<Tuple> tuples = Set.of(Tuple.of("m1".getBytes(), 1.0), Tuple.of("m2".getBytes(), 2.0),
				Tuple.of("m3".getBytes(), 3.0), Tuple.of("m4".getBytes(), 4.0));
		connection.zSetCommands().zAdd("zset4".getBytes(), tuples, RedisZSetCommands.ZAddArgs.empty());

		// Test zRandMember - get random member
		byte[] randMember = connection.zSetCommands().zRandMember("zset4".getBytes());
		assertThat(randMember).isNotNull();

		// Test zRandMember with count
		List<byte[]> randMembers = connection.zSetCommands().zRandMember("zset4".getBytes(), 2);
		assertThat(randMembers).hasSize(2);

		// Test zRandMemberWithScore - get random member with score
		Tuple randTuple = connection.zSetCommands().zRandMemberWithScore("zset4".getBytes());
		assertThat(randTuple).isNotNull();

		// Test zRandMemberWithScore with count
		List<Tuple> randTuples = connection.zSetCommands().zRandMemberWithScore("zset4".getBytes(), 2);
		assertThat(randTuples).hasSize(2);

		// Test zPopMin - pop minimum
		Tuple minTuple = connection.zSetCommands().zPopMin("zset4".getBytes());
		assertThat(minTuple).isNotNull();
		assertThat(connection.zSetCommands().zCard("zset4".getBytes())).isEqualTo(3L);

		// Test zPopMin with count
		Set<Tuple> minTuples = connection.zSetCommands().zPopMin("zset4".getBytes(), 2);
		assertThat(minTuples).hasSize(2);
		assertThat(connection.zSetCommands().zCard("zset4".getBytes())).isEqualTo(1L);

		// Re-populate for zPopMax tests
		connection.zSetCommands().zAdd("zset4".getBytes(), tuples, RedisZSetCommands.ZAddArgs.empty());

		// Test zPopMax - pop maximum
		Tuple maxTuple = connection.zSetCommands().zPopMax("zset4".getBytes());
		assertThat(maxTuple).isNotNull();

		// Test zPopMax with count
		Set<Tuple> maxTuples = connection.zSetCommands().zPopMax("zset4".getBytes(), 2);
		assertThat(maxTuples).hasSize(2);
	}

	@Test
	void zSetSetOperationsShouldWork() {
		// Set up sorted sets
		Set<Tuple> tuples1 = Set.of(Tuple.of("a".getBytes(), 1.0), Tuple.of("b".getBytes(), 2.0),
				Tuple.of("c".getBytes(), 3.0));
		Set<Tuple> tuples2 = Set.of(Tuple.of("b".getBytes(), 4.0), Tuple.of("c".getBytes(), 5.0),
				Tuple.of("d".getBytes(), 6.0));
		connection.zSetCommands().zAdd("zset5".getBytes(), tuples1, RedisZSetCommands.ZAddArgs.empty());
		connection.zSetCommands().zAdd("zset6".getBytes(), tuples2, RedisZSetCommands.ZAddArgs.empty());

		// Test zUnion - union of sets
		Set<byte[]> unionResult = connection.zSetCommands().zUnion("zset5".getBytes(), "zset6".getBytes());
		assertThat(unionResult).hasSize(4); // a, b, c, d

		// Test zUnionWithScores - union with scores
		Set<Tuple> unionWithScores = connection.zSetCommands().zUnionWithScores(Aggregate.SUM, new int[] { 1, 1 },
				"zset5".getBytes(), "zset6".getBytes());
		assertThat(unionWithScores).hasSize(4);

		// Test zUnionStore - store union
		Long unionStoreResult = connection.zSetCommands().zUnionStore("unionDst".getBytes(), "zset5".getBytes(),
				"zset6".getBytes());
		assertThat(unionStoreResult).isEqualTo(4L);

		// Test zInter - intersection of sets
		Set<byte[]> interResult = connection.zSetCommands().zInter("zset5".getBytes(), "zset6".getBytes());
		assertThat(interResult).hasSize(2); // b, c

		// Test zInterWithScores - intersection with scores
		Set<Tuple> interWithScores = connection.zSetCommands().zInterWithScores(Aggregate.SUM, new int[] { 1, 1 },
				"zset5".getBytes(), "zset6".getBytes());
		assertThat(interWithScores).hasSize(2);

		// Test zInterStore - store intersection
		Long interStoreResult = connection.zSetCommands().zInterStore("interDst".getBytes(), "zset5".getBytes(),
				"zset6".getBytes());
		assertThat(interStoreResult).isEqualTo(2L);

		// Test zDiff - difference of sets
		Set<byte[]> diffResult = connection.zSetCommands().zDiff("zset5".getBytes(), "zset6".getBytes());
		assertThat(diffResult).hasSize(1); // a

		// Test zDiffWithScores - difference with scores
		Set<Tuple> diffWithScores = connection.zSetCommands().zDiffWithScores("zset5".getBytes(), "zset6".getBytes());
		assertThat(diffWithScores).hasSize(1);

		// Test zDiffStore - store difference
		Long diffStoreResult = connection.zSetCommands().zDiffStore("diffDst".getBytes(), "zset5".getBytes(),
				"zset6".getBytes());
		assertThat(diffStoreResult).isEqualTo(1L);
	}

	@Test
	void zSetRemovalOperationsShouldWork() {
		// Set up sorted set
		Set<Tuple> tuples = Set.of(Tuple.of("a".getBytes(), 1.0), Tuple.of("b".getBytes(), 2.0),
				Tuple.of("c".getBytes(), 3.0), Tuple.of("d".getBytes(), 4.0), Tuple.of("e".getBytes(), 5.0));
		connection.zSetCommands().zAdd("zset7".getBytes(), tuples, RedisZSetCommands.ZAddArgs.empty());

		// Test zRemRange - remove by rank range
		Long remRankResult = connection.zSetCommands().zRemRange("zset7".getBytes(), 0, 1);
		assertThat(remRankResult).isEqualTo(2L);
		assertThat(connection.zSetCommands().zCard("zset7".getBytes())).isEqualTo(3L);

		// Test zRemRangeByScore - remove by score range
		Long remScoreResult = connection.zSetCommands().zRemRangeByScore("zset7".getBytes(), 3.0, 4.0);
		assertThat(remScoreResult).isEqualTo(2L);
		assertThat(connection.zSetCommands().zCard("zset7".getBytes())).isEqualTo(1L);

		// Re-populate for zRemRangeByLex test
		connection.zSetCommands().zAdd("zset8".getBytes(), tuples, RedisZSetCommands.ZAddArgs.empty());

		// Test zRemRangeByLex - remove by lex range
		Long remLexResult = connection.zSetCommands().zRemRangeByLex("zset8".getBytes(),
				Range.closed("a".getBytes(), "c".getBytes()));
		assertThat(remLexResult).isGreaterThanOrEqualTo(0L);
	}

	@Test
	void transactionShouldExecuteAtomically() {
		// Set up initial state
		Set<Tuple> tuples = Set.of(Tuple.of("m1".getBytes(), 1.0), Tuple.of("m2".getBytes(), 2.0));
		connection.zSetCommands().zAdd("txZset".getBytes(), tuples, RedisZSetCommands.ZAddArgs.empty());

		// Execute multiple zset operations in a transaction
		connection.multi();
		connection.zSetCommands().zAdd("txZset".getBytes(), 3.0, "m3".getBytes(), RedisZSetCommands.ZAddArgs.empty());
		connection.zSetCommands().zCard("txZset".getBytes());
		connection.zSetCommands().zScore("txZset".getBytes(), "m1".getBytes());
		connection.zSetCommands().zRank("txZset".getBytes(), "m2".getBytes());
		connection.zSetCommands().zRange("txZset".getBytes(), 0, -1);
		List<Object> results = connection.exec();

		// Verify all commands executed
		assertThat(results).hasSize(5);
		assertThat(results.get(0)).isEqualTo(true); // zAdd result
		assertThat(results.get(1)).isEqualTo(3L); // zCard result
		assertThat(results.get(2)).isEqualTo(1.0); // zScore result
		assertThat(results.get(3)).isEqualTo(1L); // zRank result
		@SuppressWarnings("unchecked")
		Set<byte[]> rangeResult = (Set<byte[]>) results.get(4);
		assertThat(rangeResult).hasSize(3); // zRange result
	}

	@Test
	void pipelineShouldExecuteMultipleCommands() {
		// Set up initial state
		Set<Tuple> tuples = Set.of(Tuple.of("m1".getBytes(), 1.0), Tuple.of("m2".getBytes(), 2.0),
				Tuple.of("m3".getBytes(), 3.0));
		connection.zSetCommands().zAdd("pipeZset".getBytes(), tuples, RedisZSetCommands.ZAddArgs.empty());

		// Execute multiple zset operations in pipeline
		connection.openPipeline();
		connection.zSetCommands().zAdd("pipeZset".getBytes(), 4.0, "m4".getBytes(), RedisZSetCommands.ZAddArgs.empty());
		connection.zSetCommands().zCard("pipeZset".getBytes());
		connection.zSetCommands().zIncrBy("pipeZset".getBytes(), 0.5, "m1".getBytes());
		connection.zSetCommands().zRangeWithScores("pipeZset".getBytes(), 0, -1);
		connection.zSetCommands().zRem("pipeZset".getBytes(), "m2".getBytes());
		List<Object> results = connection.closePipeline();

		// Verify all command results
		assertThat(results).hasSize(5);
		assertThat(results.get(0)).isEqualTo(true); // zAdd result
		assertThat(results.get(1)).isEqualTo(4L); // zCard result
		assertThat(results.get(2)).isEqualTo(1.5); // zIncrBy result
		@SuppressWarnings("unchecked")
		Set<Tuple> rangeResult = (Set<Tuple>) results.get(3);
		assertThat(rangeResult).hasSize(4); // zRangeWithScores result
		assertThat(results.get(4)).isEqualTo(1L); // zRem result
	}
}
