/*
 * Copyright 2016-2023 the original author or authors.
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
package org.springframework.data.redis.connection.lettuce;

import static org.assertj.core.api.Assertions.*;
import static org.assertj.core.api.Assumptions.*;
import static org.springframework.data.domain.Range.Bound.*;

import reactor.test.StepVerifier;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;

import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.zset.DefaultTuple;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.test.condition.EnabledOnCommand;
import org.springframework.data.redis.test.extension.parametrized.ParameterizedRedisTest;

/**
 * Integration tests for {@link LettuceReactiveZSetCommands}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Michele Mancioppi
 */
public class LettuceReactiveZSetCommandsIntegrationTests extends LettuceReactiveCommandsTestSupport {

	private static final Range<Long> ONE_TO_TWO = Range.closed(1L, 2L);

	private static final Range<Double> TWO_TO_THREE_ALL_INCLUSIVE = Range.closed(2D, 3D);
	private static final Range<Double> TWO_INCLUSIVE_TO_THREE_EXCLUSIVE = Range.rightOpen(2D, 3D);
	private static final Range<Double> TWO_EXCLUSIVE_TO_THREE_INCLUSIVE = Range.leftOpen(2D, 3D);

	public LettuceReactiveZSetCommandsIntegrationTests(Fixture fixture) {
		super(fixture);
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void zAddShouldAddValuesWithScores() {
		assertThat(connection.zSetCommands().zAdd(KEY_1_BBUFFER, 3.5D, VALUE_1_BBUFFER).block()).isEqualTo(1L);
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void zRemShouldRemoveValuesFromSet() {

		nativeCommands.zadd(KEY_1, 1D, VALUE_1);
		nativeCommands.zadd(KEY_1, 2D, VALUE_2);
		nativeCommands.zadd(KEY_1, 3D, VALUE_3);

		assertThat(connection.zSetCommands().zRem(KEY_1_BBUFFER, Arrays.asList(VALUE_1_BBUFFER, VALUE_3_BBUFFER)).block())
				.isEqualTo(2L);
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void zIncrByShouldInreaseAndReturnScore() {

		nativeCommands.zadd(KEY_1, 1D, VALUE_1);

		assertThat(connection.zSetCommands().zIncrBy(KEY_1_BBUFFER, 3.5D, VALUE_1_BBUFFER).block()).isEqualTo(4.5D);
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void zRankShouldReturnIndexCorrectly() {

		nativeCommands.zadd(KEY_1, 1D, VALUE_1);
		nativeCommands.zadd(KEY_1, 2D, VALUE_2);
		nativeCommands.zadd(KEY_1, 3D, VALUE_3);

		assertThat(connection.zSetCommands().zRank(KEY_1_BBUFFER, VALUE_3_BBUFFER).block()).isEqualTo(2L);
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void zRevRankShouldReturnIndexCorrectly() {

		nativeCommands.zadd(KEY_1, 1D, VALUE_1);
		nativeCommands.zadd(KEY_1, 2D, VALUE_2);
		nativeCommands.zadd(KEY_1, 3D, VALUE_3);

		assertThat(connection.zSetCommands().zRevRank(KEY_1_BBUFFER, VALUE_3_BBUFFER).block()).isEqualTo(0L);
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void zRangeShouldReturnValuesCorrectly() {

		nativeCommands.zadd(KEY_1, 1D, VALUE_1);
		nativeCommands.zadd(KEY_1, 2D, VALUE_2);
		nativeCommands.zadd(KEY_1, 3D, VALUE_3);

		connection.zSetCommands().zRange(KEY_1_BBUFFER, ONE_TO_TWO).as(StepVerifier::create) //
				.expectNext(VALUE_2_BBUFFER, VALUE_3_BBUFFER) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void zRangeWithScoreShouldReturnTuplesCorrectly() {

		nativeCommands.zadd(KEY_1, 1D, VALUE_1);
		nativeCommands.zadd(KEY_1, 2D, VALUE_2);
		nativeCommands.zadd(KEY_1, 3D, VALUE_3);

		connection.zSetCommands().zRangeWithScores(KEY_1_BBUFFER, ONE_TO_TWO).as(StepVerifier::create) //
				.expectNext(new DefaultTuple(VALUE_2_BBUFFER.array(), 2D), new DefaultTuple(VALUE_3_BBUFFER.array(), 3D)) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void zRevRangeShouldReturnValuesCorrectly() {

		nativeCommands.zadd(KEY_1, 1D, VALUE_1);
		nativeCommands.zadd(KEY_1, 2D, VALUE_2);
		nativeCommands.zadd(KEY_1, 3D, VALUE_3);

		connection.zSetCommands().zRevRange(KEY_1_BBUFFER, ONE_TO_TWO).as(StepVerifier::create) //
				.expectNext(VALUE_2_BBUFFER, VALUE_1_BBUFFER) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void zRevRangeWithScoreShouldReturnTuplesCorrectly() {

		nativeCommands.zadd(KEY_1, 1D, VALUE_1);
		nativeCommands.zadd(KEY_1, 2D, VALUE_2);
		nativeCommands.zadd(KEY_1, 3D, VALUE_3);

		connection.zSetCommands().zRevRangeWithScores(KEY_1_BBUFFER, ONE_TO_TWO).as(StepVerifier::create) //
				.expectNext(new DefaultTuple(VALUE_2_BBUFFER.array(), 2D), new DefaultTuple(VALUE_1_BBUFFER.array(), 1D)) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void zRangeByScoreShouldReturnValuesCorrectly() {

		nativeCommands.zadd(KEY_1, 1D, VALUE_1);
		nativeCommands.zadd(KEY_1, 2D, VALUE_2);
		nativeCommands.zadd(KEY_1, 3D, VALUE_3);

		connection.zSetCommands().zRangeByScore(KEY_1_BBUFFER, Range.closed(2D, 3D)).as(StepVerifier::create) //
				.expectNext(VALUE_2_BBUFFER, VALUE_3_BBUFFER) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-852
	void zRangeByScoreShouldReturnValuesCorrectlyWithMinUnbounded() {

		nativeCommands.zadd(KEY_1, 1D, VALUE_1);
		nativeCommands.zadd(KEY_1, 2D, VALUE_2);
		nativeCommands.zadd(KEY_1, 3D, VALUE_3);

		connection.zSetCommands().zRangeByScore(KEY_1_BBUFFER, Range.of(Range.Bound.unbounded(), Range.Bound.inclusive(3D)))
				.as(StepVerifier::create) //
				.expectNext(VALUE_1_BBUFFER, VALUE_2_BBUFFER, VALUE_3_BBUFFER) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-852
	void zRangeByScoreShouldReturnValuesCorrectlyWithMaxUnbounded() {

		nativeCommands.zadd(KEY_1, 1D, VALUE_1);
		nativeCommands.zadd(KEY_1, 2D, VALUE_2);
		nativeCommands.zadd(KEY_1, 3D, VALUE_3);

		connection.zSetCommands().zRangeByScore(KEY_1_BBUFFER, Range.of(Range.Bound.inclusive(0D), Range.Bound.unbounded()))
				.as(StepVerifier::create) //
				.expectNext(VALUE_1_BBUFFER, VALUE_2_BBUFFER, VALUE_3_BBUFFER) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void zRangeByScoreShouldReturnValuesCorrectlyWithMinExclusion() {

		nativeCommands.zadd(KEY_1, 1D, VALUE_1);
		nativeCommands.zadd(KEY_1, 2D, VALUE_2);
		nativeCommands.zadd(KEY_1, 3D, VALUE_3);

		connection.zSetCommands().zRangeByScore(KEY_1_BBUFFER, TWO_EXCLUSIVE_TO_THREE_INCLUSIVE).as(StepVerifier::create) //
				.expectNext(VALUE_3_BBUFFER) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void zRangeByScoreShouldReturnValuesCorrectlyWithMaxExclusion() {

		nativeCommands.zadd(KEY_1, 1D, VALUE_1);
		nativeCommands.zadd(KEY_1, 2D, VALUE_2);
		nativeCommands.zadd(KEY_1, 3D, VALUE_3);

		connection.zSetCommands().zRangeByScore(KEY_1_BBUFFER, TWO_INCLUSIVE_TO_THREE_EXCLUSIVE).as(StepVerifier::create) //
				.expectNext(VALUE_2_BBUFFER) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void zRangeByScoreWithScoreShouldReturnTuplesCorrectly() {

		nativeCommands.zadd(KEY_1, 1D, VALUE_1);
		nativeCommands.zadd(KEY_1, 2D, VALUE_2);
		nativeCommands.zadd(KEY_1, 3D, VALUE_3);

		connection.zSetCommands().zRangeByScoreWithScores(KEY_1_BBUFFER, TWO_TO_THREE_ALL_INCLUSIVE)
				.as(StepVerifier::create) //
				.expectNext(new DefaultTuple(VALUE_2_BBUFFER.array(), 2D), new DefaultTuple(VALUE_3_BBUFFER.array(), 3D)) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void zRangeByScoreWithScoreShouldReturnTuplesCorrectlyWithMinExclusion() {

		nativeCommands.zadd(KEY_1, 1D, VALUE_1);
		nativeCommands.zadd(KEY_1, 2D, VALUE_2);
		nativeCommands.zadd(KEY_1, 3D, VALUE_3);

		connection.zSetCommands().zRangeByScoreWithScores(KEY_1_BBUFFER, TWO_EXCLUSIVE_TO_THREE_INCLUSIVE)
				.as(StepVerifier::create) //
				.expectNext(new DefaultTuple(VALUE_3_BBUFFER.array(), 3D)) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void zRangeByScoreWithScoreShouldReturnTuplesCorrectlyWithMaxExclusion() {

		nativeCommands.zadd(KEY_1, 1D, VALUE_1);
		nativeCommands.zadd(KEY_1, 2D, VALUE_2);
		nativeCommands.zadd(KEY_1, 3D, VALUE_3);

		connection.zSetCommands().zRangeByScoreWithScores(KEY_1_BBUFFER, TWO_INCLUSIVE_TO_THREE_EXCLUSIVE)
				.as(StepVerifier::create) //
				.expectNext(new DefaultTuple(VALUE_2_BBUFFER.array(), 2D)) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void zRevRangeByScoreShouldReturnValuesCorrectly() {

		nativeCommands.zadd(KEY_1, 1D, VALUE_1);
		nativeCommands.zadd(KEY_1, 2D, VALUE_2);
		nativeCommands.zadd(KEY_1, 3D, VALUE_3);

		connection.zSetCommands().zRevRangeByScore(KEY_1_BBUFFER, TWO_TO_THREE_ALL_INCLUSIVE).as(StepVerifier::create) //
				.expectNext(VALUE_3_BBUFFER, VALUE_2_BBUFFER) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void zRevRangeByScoreShouldReturnValuesCorrectlyWithMinExclusion() {

		nativeCommands.zadd(KEY_1, 1D, VALUE_1);
		nativeCommands.zadd(KEY_1, 2D, VALUE_2);
		nativeCommands.zadd(KEY_1, 3D, VALUE_3);

		connection.zSetCommands().zRevRangeByScore(KEY_1_BBUFFER, TWO_EXCLUSIVE_TO_THREE_INCLUSIVE).as(StepVerifier::create) //
				.expectNext(VALUE_3_BBUFFER) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void zRevRangeByScoreShouldReturnValuesCorrectlyWithMaxExclusion() {

		nativeCommands.zadd(KEY_1, 1D, VALUE_1);
		nativeCommands.zadd(KEY_1, 2D, VALUE_2);
		nativeCommands.zadd(KEY_1, 3D, VALUE_3);

		connection.zSetCommands().zRevRangeByScore(KEY_1_BBUFFER, TWO_INCLUSIVE_TO_THREE_EXCLUSIVE).as(StepVerifier::create) //
				.expectNext(VALUE_2_BBUFFER) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void zRevRangeByScoreWithScoreShouldReturnTuplesCorrectly() {

		nativeCommands.zadd(KEY_1, 1D, VALUE_1);
		nativeCommands.zadd(KEY_1, 2D, VALUE_2);
		nativeCommands.zadd(KEY_1, 3D, VALUE_3);

		connection.zSetCommands().zRevRangeByScoreWithScores(KEY_1_BBUFFER, TWO_TO_THREE_ALL_INCLUSIVE)
				.as(StepVerifier::create) //
				.expectNext(new DefaultTuple(VALUE_3_BBUFFER.array(), 3D), new DefaultTuple(VALUE_2_BBUFFER.array(), 2D)) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void zRevRangeByScoreWithScoreShouldReturnTuplesCorrectlyWithMinExclusion() {

		nativeCommands.zadd(KEY_1, 1D, VALUE_1);
		nativeCommands.zadd(KEY_1, 2D, VALUE_2);
		nativeCommands.zadd(KEY_1, 3D, VALUE_3);

		connection.zSetCommands().zRevRangeByScoreWithScores(KEY_1_BBUFFER, TWO_EXCLUSIVE_TO_THREE_INCLUSIVE)
				.as(StepVerifier::create) //
				.expectNext(new DefaultTuple(VALUE_3_BBUFFER.array(), 3D)) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void zRevRangeByScoreWithScoreShouldReturnTuplesCorrectlyWithMaxExclusion() {

		nativeCommands.zadd(KEY_1, 1D, VALUE_1);
		nativeCommands.zadd(KEY_1, 2D, VALUE_2);
		nativeCommands.zadd(KEY_1, 3D, VALUE_3);

		connection.zSetCommands().zRevRangeByScoreWithScores(KEY_1_BBUFFER, TWO_INCLUSIVE_TO_THREE_EXCLUSIVE)
				.as(StepVerifier::create) //
				.expectNext(new DefaultTuple(VALUE_2_BBUFFER.array(), 2D)) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-743
	void zScanShouldIterateOverSortedSet() {

		nativeCommands.zadd(KEY_1, 1D, VALUE_1);
		nativeCommands.zadd(KEY_1, 2D, VALUE_2);
		nativeCommands.zadd(KEY_1, 3D, VALUE_3);

		connection.zSetCommands().zScan(KEY_1_BBUFFER, ScanOptions.scanOptions().count(1).build()).as(StepVerifier::create) //
				.expectNextCount(3).verifyComplete();

		connection.zSetCommands().zScan(KEY_1_BBUFFER, ScanOptions.scanOptions().match(VALUE_2).build())
				.as(StepVerifier::create) //
				.expectNext(new DefaultTuple(VALUE_2_BBUFFER.array(), 2D)) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void zCountShouldCountValuesInRange() {

		nativeCommands.zadd(KEY_1, 1D, VALUE_1);
		nativeCommands.zadd(KEY_1, 2D, VALUE_2);
		nativeCommands.zadd(KEY_1, 3D, VALUE_3);

		assertThat(connection.zSetCommands().zCount(KEY_1_BBUFFER, TWO_TO_THREE_ALL_INCLUSIVE).block()).isEqualTo(2L);
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void zCountShouldCountValuesInRangeWithMinExlusion() {

		nativeCommands.zadd(KEY_1, 1D, VALUE_1);
		nativeCommands.zadd(KEY_1, 2D, VALUE_2);
		nativeCommands.zadd(KEY_1, 3D, VALUE_3);

		assertThat(connection.zSetCommands().zCount(KEY_1_BBUFFER, TWO_EXCLUSIVE_TO_THREE_INCLUSIVE).block()).isEqualTo(1L);
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void zCountShouldCountValuesInRangeWithMaxExlusion() {

		nativeCommands.zadd(KEY_1, 1D, VALUE_1);
		nativeCommands.zadd(KEY_1, 2D, VALUE_2);
		nativeCommands.zadd(KEY_1, 3D, VALUE_3);

		assertThat(connection.zSetCommands().zCount(KEY_1_BBUFFER, TWO_INCLUSIVE_TO_THREE_EXCLUSIVE).block()).isEqualTo(1L);
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void zCountShouldCountValuesInRangeWithNegativeInfinity() {

		nativeCommands.zadd(KEY_1, 1D, VALUE_1);
		nativeCommands.zadd(KEY_1, 2D, VALUE_2);
		nativeCommands.zadd(KEY_1, 3D, VALUE_3);

		assertThat(connection.zSetCommands().zCount(KEY_1_BBUFFER, Range.leftUnbounded(inclusive(2D))).block())
				.isEqualTo(2L);
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void zCountShouldCountValuesInRangeWithPositiveInfinity() {

		nativeCommands.zadd(KEY_1, 1D, VALUE_1);
		nativeCommands.zadd(KEY_1, 2D, VALUE_2);
		nativeCommands.zadd(KEY_1, 3D, VALUE_3);

		assertThat(connection.zSetCommands().zCount(KEY_1_BBUFFER, Range.rightUnbounded(inclusive(2D))).block())
				.isEqualTo(2L);
	}

	@ParameterizedRedisTest // GH-2007
	@EnabledOnCommand("ZPOPMIN")
	void zPopMinShouldReturnCorrectly() {

		nativeCommands.zadd(KEY_1, 1D, VALUE_1);
		nativeCommands.zadd(KEY_1, 2D, VALUE_2);
		nativeCommands.zadd(KEY_1, 3D, VALUE_3);

		connection.zSetCommands().zPopMin(KEY_1_BBUFFER).as(StepVerifier::create)
				.expectNext(new DefaultTuple(VALUE_1_BYTES, 1D)).verifyComplete();

		connection.zSetCommands().zPopMin(KEY_1_BBUFFER, 2).as(StepVerifier::create)
				.expectNext(new DefaultTuple(VALUE_2_BYTES, 2D)).expectNext(new DefaultTuple(VALUE_3_BYTES, 3D))
				.verifyComplete();
	}

	@ParameterizedRedisTest // GH-2007
	@EnabledOnCommand("BZPOPMIN")
	void bzPopMinShouldReturnCorrectly() {

		nativeCommands.zadd(KEY_1, 1D, VALUE_1);
		nativeCommands.zadd(KEY_1, 2D, VALUE_2);
		nativeCommands.zadd(KEY_1, 3D, VALUE_3);

		connection.zSetCommands().bZPopMin(KEY_1_BBUFFER, Duration.ofSeconds(1)).as(StepVerifier::create)
				.expectNext(new DefaultTuple(VALUE_1_BYTES, 1D)).verifyComplete();
	}

	@ParameterizedRedisTest // GH-2007
	@EnabledOnCommand("ZPOPMAX")
	void zPopMaxShouldReturnCorrectly() {

		nativeCommands.zadd(KEY_1, 1D, VALUE_1);
		nativeCommands.zadd(KEY_1, 2D, VALUE_2);
		nativeCommands.zadd(KEY_1, 3D, VALUE_3);

		connection.zSetCommands().zPopMax(KEY_1_BBUFFER).as(StepVerifier::create)
				.expectNext(new DefaultTuple(VALUE_3_BYTES, 3D)).verifyComplete();

		connection.zSetCommands().zPopMax(KEY_1_BBUFFER, 2).as(StepVerifier::create)
				.expectNext(new DefaultTuple(VALUE_2_BYTES, 2D)).expectNext(new DefaultTuple(VALUE_1_BYTES, 1D))
				.verifyComplete();
	}

	@ParameterizedRedisTest // GH-2007
	@EnabledOnCommand("BZPOPMAX")
	void bzPopMaxShouldReturnCorrectly() {

		nativeCommands.zadd(KEY_1, 1D, VALUE_1);
		nativeCommands.zadd(KEY_1, 2D, VALUE_2);
		nativeCommands.zadd(KEY_1, 3D, VALUE_3);

		connection.zSetCommands().bZPopMax(KEY_1_BBUFFER, Duration.ofSeconds(1)).as(StepVerifier::create)
				.expectNext(new DefaultTuple(VALUE_3_BYTES, 3D)).verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void zCardShouldReturnSizeCorrectly() {

		nativeCommands.zadd(KEY_1, 1D, VALUE_1);
		nativeCommands.zadd(KEY_1, 2D, VALUE_2);
		nativeCommands.zadd(KEY_1, 3D, VALUE_3);

		assertThat(connection.zSetCommands().zCard(KEY_1_BBUFFER).block()).isEqualTo(3L);
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void zScoreShouldReturnScoreCorrectly() {

		nativeCommands.zadd(KEY_1, 2D, VALUE_2);

		assertThat(connection.zSetCommands().zScore(KEY_1_BBUFFER, VALUE_2_BBUFFER).block()).isEqualTo(2D);
	}

	@ParameterizedRedisTest // GH-2038
	@EnabledOnCommand("ZMSCORE")
	void zMScoreShouldReturnScoreCorrectly() {

		nativeCommands.zadd(KEY_1, 1D, VALUE_1);
		nativeCommands.zadd(KEY_1, 2D, VALUE_2);

		connection.zSetCommands().zMScore(KEY_1_BBUFFER, Arrays.asList(VALUE_1_BBUFFER, VALUE_2_BBUFFER))
				.as(StepVerifier::create).expectNext(Arrays.asList(1D, 2D)).verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void zRemRangeByRankShouldRemoveValuesCorrectly() {

		nativeCommands.zadd(KEY_1, 1D, VALUE_1);
		nativeCommands.zadd(KEY_1, 2D, VALUE_2);
		nativeCommands.zadd(KEY_1, 3D, VALUE_3);

		assertThat(connection.zSetCommands().zRemRangeByRank(KEY_1_BBUFFER, ONE_TO_TWO).block()).isEqualTo(2L);
	}

	@ParameterizedRedisTest // GH-1816
	void zRemRangeByLexRemovesValuesCorrectly() {

		nativeCommands.zadd(KEY_1, 0D, "aaaa");
		nativeCommands.zadd(KEY_1, 0D, "b");
		nativeCommands.zadd(KEY_1, 0D, "c");
		nativeCommands.zadd(KEY_1, 0D, "d");
		nativeCommands.zadd(KEY_1, 0D, "e");
		nativeCommands.zadd(KEY_1, 0D, "foo");
		nativeCommands.zadd(KEY_1, 0D, "zap");
		nativeCommands.zadd(KEY_1, 0D, "zip");
		nativeCommands.zadd(KEY_1, 0D, "ALPHA");
		nativeCommands.zadd(KEY_1, 0D, "alpha");

		connection.zSetCommands().zRemRangeByLex(KEY_1_BBUFFER, Range.closed("alpha", "omega")) //
				.as(StepVerifier::create) //
				.expectNext(6L) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void zRemRangeByScoreShouldRemoveValuesCorrectly() {

		nativeCommands.zadd(KEY_1, 1D, VALUE_1);
		nativeCommands.zadd(KEY_1, 2D, VALUE_2);
		nativeCommands.zadd(KEY_1, 3D, VALUE_3);

		assertThat(connection.zSetCommands().zRemRangeByScore(KEY_1_BBUFFER, Range.closed(1D, 2D)).block()).isEqualTo(2L);
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void zRemRangeByScoreShouldRemoveValuesCorrectlyWithNegativeInfinity() {

		nativeCommands.zadd(KEY_1, 1D, VALUE_1);
		nativeCommands.zadd(KEY_1, 2D, VALUE_2);
		nativeCommands.zadd(KEY_1, 3D, VALUE_3);

		assertThat(connection.zSetCommands().zRemRangeByScore(KEY_1_BBUFFER, Range.leftUnbounded(inclusive(2D))).block())
				.isEqualTo(2L);
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void zRemRangeByScoreShouldRemoveValuesCorrectlyWithPositiveInfinity() {

		nativeCommands.zadd(KEY_1, 1D, VALUE_1);
		nativeCommands.zadd(KEY_1, 2D, VALUE_2);
		nativeCommands.zadd(KEY_1, 3D, VALUE_3);

		assertThat(connection.zSetCommands().zRemRangeByScore(KEY_1_BBUFFER, Range.rightUnbounded(inclusive(2D))).block())
				.isEqualTo(2L);
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void zRemRangeByScoreShouldRemoveValuesCorrectlyWithExcludingMinRange() {

		nativeCommands.zadd(KEY_1, 1D, VALUE_1);
		nativeCommands.zadd(KEY_1, 2D, VALUE_2);
		nativeCommands.zadd(KEY_1, 3D, VALUE_3);

		assertThat(connection.zSetCommands().zRemRangeByScore(KEY_1_BBUFFER, TWO_EXCLUSIVE_TO_THREE_INCLUSIVE).block())
				.isEqualTo(1L);
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void zRemRangeByScoreShouldRemoveValuesCorrectlyWithExcludingMaxRange() {

		nativeCommands.zadd(KEY_1, 1D, VALUE_1);
		nativeCommands.zadd(KEY_1, 2D, VALUE_2);
		nativeCommands.zadd(KEY_1, 3D, VALUE_3);

		assertThat(connection.zSetCommands().zRemRangeByScore(KEY_1_BBUFFER, TWO_INCLUSIVE_TO_THREE_EXCLUSIVE).block())
				.isEqualTo(1L);
	}

	@ParameterizedRedisTest // GH-2041
	void zDiffShouldWorkCorrectly() {

		assumeThat(nativeCommands).isInstanceOf(io.lettuce.core.api.sync.RedisCommands.class);

		nativeCommands.zadd(KEY_1, 1D, VALUE_1);
		nativeCommands.zadd(KEY_1, 2D, VALUE_2);
		nativeCommands.zadd(KEY_1, 3D, VALUE_3);
		nativeCommands.zadd(KEY_2, 1D, VALUE_1);
		nativeCommands.zadd(KEY_2, 2D, VALUE_2);

		connection.zSetCommands().zDiff(Arrays.asList(KEY_1_BBUFFER, KEY_2_BBUFFER)) //
				.collectList() //
				.as(StepVerifier::create) //
				.assertNext(actual -> {
					assertThat(actual).containsOnly(VALUE_3_BBUFFER);
				}).verifyComplete();

		connection.zSetCommands().zDiffWithScores(Arrays.asList(KEY_1_BBUFFER, KEY_2_BBUFFER)) //
				.collectList() //
				.as(StepVerifier::create) //
				.assertNext(actual -> {
					assertThat(actual).containsOnly(new DefaultTuple(VALUE_3_BYTES, 3D));
				}).verifyComplete();
	}

	@ParameterizedRedisTest // GH-2041
	void zDiffStoreShouldWorkCorrectly() {

		assumeThat(nativeCommands).isInstanceOf(io.lettuce.core.api.sync.RedisCommands.class);

		nativeCommands.zadd(KEY_1, 1D, VALUE_1);
		nativeCommands.zadd(KEY_1, 2D, VALUE_2);
		nativeCommands.zadd(KEY_1, 3D, VALUE_3);
		nativeCommands.zadd(KEY_2, 1D, VALUE_1);
		nativeCommands.zadd(KEY_2, 2D, VALUE_2);

		connection.zSetCommands().zDiffStore(KEY_3_BBUFFER, Arrays.asList(KEY_1_BBUFFER, KEY_2_BBUFFER)) //
				.as(StepVerifier::create) //
				.expectNext(1L).verifyComplete();
	}

	@ParameterizedRedisTest // GH-2042
	void zInterShouldWorkCorrectly() {

		assumeThat(nativeCommands).isInstanceOf(io.lettuce.core.api.sync.RedisCommands.class);

		nativeCommands.zadd(KEY_1, 1D, VALUE_1);
		nativeCommands.zadd(KEY_1, 2D, VALUE_2);
		nativeCommands.zadd(KEY_2, 1D, VALUE_1);
		nativeCommands.zadd(KEY_2, 2D, VALUE_2);
		nativeCommands.zadd(KEY_2, 3D, VALUE_3);

		connection.zSetCommands().zInter(Arrays.asList(KEY_1_BBUFFER, KEY_2_BBUFFER)) //
				.collectList() //
				.as(StepVerifier::create) //
				.assertNext(actual -> {
					assertThat(actual).contains(VALUE_1_BBUFFER, VALUE_2_BBUFFER);
				}).verifyComplete();

		connection.zSetCommands().zInterWithScores(Arrays.asList(KEY_1_BBUFFER, KEY_2_BBUFFER), Arrays.asList(2D, 3D)) //
				.collectList() //
				.as(StepVerifier::create) //
				.assertNext(actual -> {
					assertThat(actual).contains(new DefaultTuple(VALUE_1_BYTES, 5D), new DefaultTuple(VALUE_2_BYTES, 10D));
				}).verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void zInterStoreShouldWorkCorrectly() {

		assumeThat(nativeCommands).isInstanceOf(io.lettuce.core.api.sync.RedisCommands.class);

		nativeCommands.zadd(KEY_1, 1D, VALUE_1);
		nativeCommands.zadd(KEY_1, 2D, VALUE_2);
		nativeCommands.zadd(KEY_2, 1D, VALUE_1);
		nativeCommands.zadd(KEY_2, 2D, VALUE_2);
		nativeCommands.zadd(KEY_2, 3D, VALUE_3);

		assertThat(connection.zSetCommands()
				.zInterStore(KEY_3_BBUFFER, Arrays.asList(KEY_1_BBUFFER, KEY_2_BBUFFER), Arrays.asList(2D, 3D)).block())
						.isEqualTo(2L);
	}

	@ParameterizedRedisTest // GH-2042
	void zUnionShouldWorkCorrectly() {

		assumeThat(nativeCommands).isInstanceOf(io.lettuce.core.api.sync.RedisCommands.class);

		nativeCommands.zadd(KEY_1, 1D, VALUE_1);
		nativeCommands.zadd(KEY_1, 2D, VALUE_2);
		nativeCommands.zadd(KEY_2, 1D, VALUE_1);
		nativeCommands.zadd(KEY_2, 2D, VALUE_2);
		nativeCommands.zadd(KEY_2, 3D, VALUE_3);

		connection.zSetCommands().zUnion(Arrays.asList(KEY_1_BBUFFER, KEY_2_BBUFFER)) //
				.collectList() //
				.as(StepVerifier::create) //
				.assertNext(actual -> {
					assertThat(actual).contains(VALUE_1_BBUFFER, VALUE_2_BBUFFER, VALUE_3_BBUFFER);
				}).verifyComplete();

		connection.zSetCommands().zUnionWithScores(Arrays.asList(KEY_1_BBUFFER, KEY_2_BBUFFER), Arrays.asList(2D, 3D)) //
				.collectList() //
				.as(StepVerifier::create) //
				.assertNext(actual -> {
					assertThat(actual).contains(new DefaultTuple(VALUE_1_BYTES, 5D), new DefaultTuple(VALUE_2_BYTES, 10D),
							new DefaultTuple(VALUE_3_BYTES, 9D));
				}).verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void zUnionStoreShouldWorkCorrectly() {

		assumeThat(nativeCommands).isInstanceOf(io.lettuce.core.api.sync.RedisCommands.class);

		nativeCommands.zadd(KEY_1, 1D, VALUE_1);
		nativeCommands.zadd(KEY_1, 2D, VALUE_2);
		nativeCommands.zadd(KEY_2, 1D, VALUE_1);
		nativeCommands.zadd(KEY_2, 2D, VALUE_2);
		nativeCommands.zadd(KEY_2, 3D, VALUE_3);

		assertThat(connection.zSetCommands()
				.zUnionStore(KEY_3_BBUFFER, Arrays.asList(KEY_1_BBUFFER, KEY_2_BBUFFER), Arrays.asList(2D, 3D)).block())
						.isEqualTo(3L);
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void zRangeByLex() {

		nativeCommands.zadd(KEY_1, 0D, "a");
		nativeCommands.zadd(KEY_1, 0D, "b");
		nativeCommands.zadd(KEY_1, 0D, "c");
		nativeCommands.zadd(KEY_1, 0D, "d");
		nativeCommands.zadd(KEY_1, 0D, "e");
		nativeCommands.zadd(KEY_1, 0D, "f");
		nativeCommands.zadd(KEY_1, 0D, "g");

		Range<String> emptyToC = Range.closed("", "c");

		assertThat(connection.zSetCommands().zRangeByLex(KEY_1_BBUFFER, emptyToC).collectList().block()).containsExactly(
				ByteBuffer.wrap("a".getBytes()), ByteBuffer.wrap("b".getBytes()), ByteBuffer.wrap("c".getBytes()));

		assertThat(connection.zSetCommands().zRangeByLex(KEY_1_BBUFFER, Range.rightOpen("", "c")).collectList().block())
				.containsExactly(ByteBuffer.wrap("a".getBytes()), ByteBuffer.wrap("b".getBytes()));

		assertThat(connection.zSetCommands().zRangeByLex(KEY_1_BBUFFER, Range.rightOpen("aaa", "g")).collectList().block())
				.containsExactly(ByteBuffer.wrap("b".getBytes()), ByteBuffer.wrap("c".getBytes()),
						ByteBuffer.wrap("d".getBytes()), ByteBuffer.wrap("e".getBytes()), ByteBuffer.wrap("f".getBytes()));
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void zRevRangeByLex() {

		nativeCommands.zadd(KEY_1, 0D, "a");
		nativeCommands.zadd(KEY_1, 0D, "b");
		nativeCommands.zadd(KEY_1, 0D, "c");
		nativeCommands.zadd(KEY_1, 0D, "d");
		nativeCommands.zadd(KEY_1, 0D, "e");
		nativeCommands.zadd(KEY_1, 0D, "f");
		nativeCommands.zadd(KEY_1, 0D, "g");

		assertThat(connection.zSetCommands().zRevRangeByLex(KEY_1_BBUFFER, Range.closed("", "c")).collectList().block())
				.containsExactly(ByteBuffer.wrap("c".getBytes()), ByteBuffer.wrap("b".getBytes()),
						ByteBuffer.wrap("a".getBytes()));

		assertThat(connection.zSetCommands().zRevRangeByLex(KEY_1_BBUFFER, Range.rightOpen("", "c")).collectList().block())
				.containsExactly(ByteBuffer.wrap("b".getBytes()), ByteBuffer.wrap("a".getBytes()));

		assertThat(
				connection.zSetCommands().zRevRangeByLex(KEY_1_BBUFFER, Range.rightOpen("aaa", "g")).collectList().block())
						.containsExactly(ByteBuffer.wrap("f".getBytes()), ByteBuffer.wrap("e".getBytes()),
								ByteBuffer.wrap("d".getBytes()), ByteBuffer.wrap("c".getBytes()), ByteBuffer.wrap("b".getBytes()));
	}

}
