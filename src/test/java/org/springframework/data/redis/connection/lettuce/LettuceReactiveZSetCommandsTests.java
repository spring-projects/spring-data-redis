/*
 * Copyright 2016-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.redis.connection.lettuce;

import static org.hamcrest.core.Is.*;
import static org.junit.Assert.*;
import static org.junit.Assume.*;

import reactor.test.StepVerifier;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.hamcrest.collection.IsIterableContainingInOrder;
import org.junit.Test;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.DefaultTuple;
import org.springframework.data.redis.core.ScanOptions;

/**
 * Integration tests for {@link LettuceReactiveZSetCommands}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Michele Mancioppi
 */
public class LettuceReactiveZSetCommandsTests extends LettuceReactiveCommandsTestsBase {

	@Test // DATAREDIS-525
	public void zAddShouldAddValuesWithScores() {
		assertThat(connection.zSetCommands().zAdd(KEY_1_BBUFFER, 3.5D, VALUE_1_BBUFFER).block(), is(1L));
	}

	@Test // DATAREDIS-525
	public void zRemShouldRemoveValuesFromSet() {

		nativeCommands.zadd(KEY_1, 1D, VALUE_1);
		nativeCommands.zadd(KEY_1, 2D, VALUE_2);
		nativeCommands.zadd(KEY_1, 3D, VALUE_3);

		assertThat(connection.zSetCommands().zRem(KEY_1_BBUFFER, Arrays.asList(VALUE_1_BBUFFER, VALUE_3_BBUFFER)).block(),
				is(2L));
	}

	@Test // DATAREDIS-525
	public void zIncrByShouldInreaseAndReturnScore() {

		nativeCommands.zadd(KEY_1, 1D, VALUE_1);

		assertThat(connection.zSetCommands().zIncrBy(KEY_1_BBUFFER, 3.5D, VALUE_1_BBUFFER).block(), is(4.5D));
	}

	@Test // DATAREDIS-525
	public void zRankShouldReturnIndexCorrectly() {

		nativeCommands.zadd(KEY_1, 1D, VALUE_1);
		nativeCommands.zadd(KEY_1, 2D, VALUE_2);
		nativeCommands.zadd(KEY_1, 3D, VALUE_3);

		assertThat(connection.zSetCommands().zRank(KEY_1_BBUFFER, VALUE_3_BBUFFER).block(), is(2L));
	}

	@Test // DATAREDIS-525
	public void zRevRankShouldReturnIndexCorrectly() {

		nativeCommands.zadd(KEY_1, 1D, VALUE_1);
		nativeCommands.zadd(KEY_1, 2D, VALUE_2);
		nativeCommands.zadd(KEY_1, 3D, VALUE_3);

		assertThat(connection.zSetCommands().zRevRank(KEY_1_BBUFFER, VALUE_3_BBUFFER).block(), is(0L));
	}

	@Test // DATAREDIS-525
	public void zRangeShouldReturnValuesCorrectly() {

		nativeCommands.zadd(KEY_1, 1D, VALUE_1);
		nativeCommands.zadd(KEY_1, 2D, VALUE_2);
		nativeCommands.zadd(KEY_1, 3D, VALUE_3);

		StepVerifier.create(connection.zSetCommands().zRange(KEY_1_BBUFFER, new Range<>(1L, 2L))) //
				.expectNext(VALUE_2_BBUFFER, VALUE_3_BBUFFER) //
				.verifyComplete();
	}

	@Test // DATAREDIS-525
	public void zRangeWithScoreShouldReturnTuplesCorrectly() {

		nativeCommands.zadd(KEY_1, 1D, VALUE_1);
		nativeCommands.zadd(KEY_1, 2D, VALUE_2);
		nativeCommands.zadd(KEY_1, 3D, VALUE_3);

		StepVerifier.create(connection.zSetCommands().zRangeWithScores(KEY_1_BBUFFER, new Range<>(1L, 2L))) //
				.expectNext(new DefaultTuple(VALUE_2_BBUFFER.array(), 2D), new DefaultTuple(VALUE_3_BBUFFER.array(), 3D)) //
				.verifyComplete();
	}

	@Test // DATAREDIS-525
	public void zRevRangeShouldReturnValuesCorrectly() {

		nativeCommands.zadd(KEY_1, 1D, VALUE_1);
		nativeCommands.zadd(KEY_1, 2D, VALUE_2);
		nativeCommands.zadd(KEY_1, 3D, VALUE_3);

		StepVerifier.create(connection.zSetCommands().zRevRange(KEY_1_BBUFFER, new Range<>(1L, 2L))) //
				.expectNext(VALUE_2_BBUFFER, VALUE_1_BBUFFER) //
				.verifyComplete();
	}

	@Test // DATAREDIS-525
	public void zRevRangeWithScoreShouldReturnTuplesCorrectly() {

		nativeCommands.zadd(KEY_1, 1D, VALUE_1);
		nativeCommands.zadd(KEY_1, 2D, VALUE_2);
		nativeCommands.zadd(KEY_1, 3D, VALUE_3);

		StepVerifier.create(connection.zSetCommands().zRevRangeWithScores(KEY_1_BBUFFER, new Range<>(1L, 2L))) //
				.expectNext(new DefaultTuple(VALUE_2_BBUFFER.array(), 2D), new DefaultTuple(VALUE_1_BBUFFER.array(), 1D)) //
				.verifyComplete();
	}

	@Test // DATAREDIS-525
	public void zRangeByScoreShouldReturnValuesCorrectly() {

		nativeCommands.zadd(KEY_1, 1D, VALUE_1);
		nativeCommands.zadd(KEY_1, 2D, VALUE_2);
		nativeCommands.zadd(KEY_1, 3D, VALUE_3);

		StepVerifier.create(connection.zSetCommands().zRangeByScore(KEY_1_BBUFFER, new Range<>(2D, 3D))) //
				.expectNext(VALUE_2_BBUFFER, VALUE_3_BBUFFER) //
				.verifyComplete();
	}

	@Test // DATAREDIS-852
	public void zRangeByScoreShouldReturnValuesCorrectlyWithMinUnbounded() {

		nativeCommands.zadd(KEY_1, 1D, VALUE_1);
		nativeCommands.zadd(KEY_1, 2D, VALUE_2);
		nativeCommands.zadd(KEY_1, 3D, VALUE_3);

		StepVerifier
				.create(connection.zSetCommands().zRangeByScore(KEY_1_BBUFFER,
						Range.of(Range.Bound.unbounded(), Range.Bound.inclusive(3D)))) //
				.expectNext(VALUE_1_BBUFFER, VALUE_2_BBUFFER, VALUE_3_BBUFFER) //
				.verifyComplete();
	}

	@Test // DATAREDIS-852
	public void zRangeByScoreShouldReturnValuesCorrectlyWithMaxUnbounded() {

		nativeCommands.zadd(KEY_1, 1D, VALUE_1);
		nativeCommands.zadd(KEY_1, 2D, VALUE_2);
		nativeCommands.zadd(KEY_1, 3D, VALUE_3);

		StepVerifier
				.create(connection.zSetCommands().zRangeByScore(KEY_1_BBUFFER,
						Range.of(Range.Bound.inclusive(0D), Range.Bound.unbounded()))) //
				.expectNext(VALUE_1_BBUFFER, VALUE_2_BBUFFER, VALUE_3_BBUFFER) //
				.verifyComplete();
	}

	@Test // DATAREDIS-525
	public void zRangeByScoreShouldReturnValuesCorrectlyWithMinExclusion() {

		nativeCommands.zadd(KEY_1, 1D, VALUE_1);
		nativeCommands.zadd(KEY_1, 2D, VALUE_2);
		nativeCommands.zadd(KEY_1, 3D, VALUE_3);

		StepVerifier.create(connection.zSetCommands().zRangeByScore(KEY_1_BBUFFER, new Range<>(2D, 3D, false, true))) //
				.expectNext(VALUE_3_BBUFFER) //
				.verifyComplete();
	}

	@Test // DATAREDIS-525
	public void zRangeByScoreShouldReturnValuesCorrectlyWithMaxExclusion() {

		nativeCommands.zadd(KEY_1, 1D, VALUE_1);
		nativeCommands.zadd(KEY_1, 2D, VALUE_2);
		nativeCommands.zadd(KEY_1, 3D, VALUE_3);

		StepVerifier.create(connection.zSetCommands().zRangeByScore(KEY_1_BBUFFER, new Range<>(2D, 3D, true, false))) //
				.expectNext(VALUE_2_BBUFFER) //
				.verifyComplete();
	}

	@Test // DATAREDIS-525
	public void zRangeByScoreWithScoreShouldReturnTuplesCorrectly() {

		nativeCommands.zadd(KEY_1, 1D, VALUE_1);
		nativeCommands.zadd(KEY_1, 2D, VALUE_2);
		nativeCommands.zadd(KEY_1, 3D, VALUE_3);

		StepVerifier.create(connection.zSetCommands().zRangeByScoreWithScores(KEY_1_BBUFFER, new Range<>(2D, 3D))) //
				.expectNext(new DefaultTuple(VALUE_2_BBUFFER.array(), 2D), new DefaultTuple(VALUE_3_BBUFFER.array(), 3D)) //
				.verifyComplete();
	}

	@Test // DATAREDIS-525
	public void zRangeByScoreWithScoreShouldReturnTuplesCorrectlyWithMinExclusion() {

		nativeCommands.zadd(KEY_1, 1D, VALUE_1);
		nativeCommands.zadd(KEY_1, 2D, VALUE_2);
		nativeCommands.zadd(KEY_1, 3D, VALUE_3);

		StepVerifier
				.create(connection.zSetCommands().zRangeByScoreWithScores(KEY_1_BBUFFER, new Range<>(2D, 3D, false, true))) //
				.expectNext(new DefaultTuple(VALUE_3_BBUFFER.array(), 3D)) //
				.verifyComplete();
	}

	@Test // DATAREDIS-525
	public void zRangeByScoreWithScoreShouldReturnTuplesCorrectlyWithMaxExclusion() {

		nativeCommands.zadd(KEY_1, 1D, VALUE_1);
		nativeCommands.zadd(KEY_1, 2D, VALUE_2);
		nativeCommands.zadd(KEY_1, 3D, VALUE_3);

		StepVerifier
				.create(connection.zSetCommands().zRangeByScoreWithScores(KEY_1_BBUFFER, new Range<>(2D, 3D, true, false))) //
				.expectNext(new DefaultTuple(VALUE_2_BBUFFER.array(), 2D)) //
				.verifyComplete();
	}

	@Test // DATAREDIS-525
	public void zRevRangeByScoreShouldReturnValuesCorrectly() {

		nativeCommands.zadd(KEY_1, 1D, VALUE_1);
		nativeCommands.zadd(KEY_1, 2D, VALUE_2);
		nativeCommands.zadd(KEY_1, 3D, VALUE_3);

		StepVerifier.create(connection.zSetCommands().zRevRangeByScore(KEY_1_BBUFFER, new Range<>(2D, 3D))) //
				.expectNext(VALUE_3_BBUFFER, VALUE_2_BBUFFER) //
				.verifyComplete();
	}

	@Test // DATAREDIS-525
	public void zRevRangeByScoreShouldReturnValuesCorrectlyWithMinExclusion() {

		nativeCommands.zadd(KEY_1, 1D, VALUE_1);
		nativeCommands.zadd(KEY_1, 2D, VALUE_2);
		nativeCommands.zadd(KEY_1, 3D, VALUE_3);

		StepVerifier.create(connection.zSetCommands().zRevRangeByScore(KEY_1_BBUFFER, new Range<>(2D, 3D, false, true))) //
				.expectNext(VALUE_3_BBUFFER) //
				.verifyComplete();
	}

	@Test // DATAREDIS-525
	public void zRevRangeByScoreShouldReturnValuesCorrectlyWithMaxExclusion() {

		nativeCommands.zadd(KEY_1, 1D, VALUE_1);
		nativeCommands.zadd(KEY_1, 2D, VALUE_2);
		nativeCommands.zadd(KEY_1, 3D, VALUE_3);

		StepVerifier.create(connection.zSetCommands().zRevRangeByScore(KEY_1_BBUFFER, new Range<>(2D, 3D, true, false))) //
				.expectNext(VALUE_2_BBUFFER) //
				.verifyComplete();
	}

	@Test // DATAREDIS-525
	public void zRevRangeByScoreWithScoreShouldReturnTuplesCorrectly() {

		nativeCommands.zadd(KEY_1, 1D, VALUE_1);
		nativeCommands.zadd(KEY_1, 2D, VALUE_2);
		nativeCommands.zadd(KEY_1, 3D, VALUE_3);

		StepVerifier.create(connection.zSetCommands().zRevRangeByScoreWithScores(KEY_1_BBUFFER, new Range<>(2D, 3D))) //
				.expectNext(new DefaultTuple(VALUE_3_BBUFFER.array(), 3D), new DefaultTuple(VALUE_2_BBUFFER.array(), 2D)) //
				.verifyComplete();
	}

	@Test // DATAREDIS-525
	public void zRevRangeByScoreWithScoreShouldReturnTuplesCorrectlyWithMinExclusion() {

		nativeCommands.zadd(KEY_1, 1D, VALUE_1);
		nativeCommands.zadd(KEY_1, 2D, VALUE_2);
		nativeCommands.zadd(KEY_1, 3D, VALUE_3);

		StepVerifier
				.create(connection.zSetCommands().zRevRangeByScoreWithScores(KEY_1_BBUFFER, new Range<>(2D, 3D, false, true))) //
				.expectNext(new DefaultTuple(VALUE_3_BBUFFER.array(), 3D)) //
				.verifyComplete();
	}

	@Test // DATAREDIS-525
	public void zRevRangeByScoreWithScoreShouldReturnTuplesCorrectlyWithMaxExclusion() {

		nativeCommands.zadd(KEY_1, 1D, VALUE_1);
		nativeCommands.zadd(KEY_1, 2D, VALUE_2);
		nativeCommands.zadd(KEY_1, 3D, VALUE_3);

		StepVerifier
				.create(connection.zSetCommands().zRevRangeByScoreWithScores(KEY_1_BBUFFER, new Range<>(2D, 3D, true, false))) //
				.expectNext(new DefaultTuple(VALUE_2_BBUFFER.array(), 2D)) //
				.verifyComplete();
	}

	@Test // DATAREDIS-743
	public void zScanShouldIterateOverSortedSet() {

		nativeCommands.zadd(KEY_1, 1D, VALUE_1);
		nativeCommands.zadd(KEY_1, 2D, VALUE_2);
		nativeCommands.zadd(KEY_1, 3D, VALUE_3);

		StepVerifier.create(connection.zSetCommands().zScan(KEY_1_BBUFFER, ScanOptions.scanOptions().count(1).build())) //
				.expectNextCount(3).verifyComplete();

		StepVerifier
				.create(connection.zSetCommands().zScan(KEY_1_BBUFFER, ScanOptions.scanOptions().match(VALUE_2).build())) //
				.expectNext(new DefaultTuple(VALUE_2_BBUFFER.array(), 2D)) //
				.verifyComplete();
	}

	@Test // DATAREDIS-525
	public void zCountShouldCountValuesInRange() {

		nativeCommands.zadd(KEY_1, 1D, VALUE_1);
		nativeCommands.zadd(KEY_1, 2D, VALUE_2);
		nativeCommands.zadd(KEY_1, 3D, VALUE_3);

		assertThat(connection.zSetCommands().zCount(KEY_1_BBUFFER, new Range<>(2D, 3D)).block(), is(2L));
	}

	@Test // DATAREDIS-525
	public void zCountShouldCountValuesInRangeWithMinExlusion() {

		nativeCommands.zadd(KEY_1, 1D, VALUE_1);
		nativeCommands.zadd(KEY_1, 2D, VALUE_2);
		nativeCommands.zadd(KEY_1, 3D, VALUE_3);

		assertThat(connection.zSetCommands().zCount(KEY_1_BBUFFER, new Range<>(2D, 3D, false, true)).block(), is(1L));
	}

	@Test // DATAREDIS-525
	public void zCountShouldCountValuesInRangeWithMaxExlusion() {

		nativeCommands.zadd(KEY_1, 1D, VALUE_1);
		nativeCommands.zadd(KEY_1, 2D, VALUE_2);
		nativeCommands.zadd(KEY_1, 3D, VALUE_3);

		assertThat(connection.zSetCommands().zCount(KEY_1_BBUFFER, new Range<>(2D, 3D, true, false)).block(), is(1L));
	}

	@Test // DATAREDIS-525
	public void zCountShouldCountValuesInRangeWithNegativeInfinity() {

		nativeCommands.zadd(KEY_1, 1D, VALUE_1);
		nativeCommands.zadd(KEY_1, 2D, VALUE_2);
		nativeCommands.zadd(KEY_1, 3D, VALUE_3);

		assertThat(connection.zSetCommands().zCount(KEY_1_BBUFFER, new Range<>(Double.NEGATIVE_INFINITY, 2D)).block(),
				is(2L));
	}

	@Test // DATAREDIS-525
	public void zCountShouldCountValuesInRangeWithPositiveInfinity() {

		nativeCommands.zadd(KEY_1, 1D, VALUE_1);
		nativeCommands.zadd(KEY_1, 2D, VALUE_2);
		nativeCommands.zadd(KEY_1, 3D, VALUE_3);

		assertThat(connection.zSetCommands().zCount(KEY_1_BBUFFER, new Range<>(2D, Double.POSITIVE_INFINITY)).block(),
				is(2L));
	}

	@Test // DATAREDIS-525
	public void zCardShouldReturnSizeCorrectly() {

		nativeCommands.zadd(KEY_1, 1D, VALUE_1);
		nativeCommands.zadd(KEY_1, 2D, VALUE_2);
		nativeCommands.zadd(KEY_1, 3D, VALUE_3);

		assertThat(connection.zSetCommands().zCard(KEY_1_BBUFFER).block(), is(3L));
	}

	@Test // DATAREDIS-525
	public void zScoreShouldReturnScoreCorrectly() {

		nativeCommands.zadd(KEY_1, 2D, VALUE_2);

		assertThat(connection.zSetCommands().zScore(KEY_1_BBUFFER, VALUE_2_BBUFFER).block(), is(2D));
	}

	@Test // DATAREDIS-525
	public void zRemRangeByRankShouldRemoveValuesCorrectly() {

		nativeCommands.zadd(KEY_1, 1D, VALUE_1);
		nativeCommands.zadd(KEY_1, 2D, VALUE_2);
		nativeCommands.zadd(KEY_1, 3D, VALUE_3);

		assertThat(connection.zSetCommands().zRemRangeByRank(KEY_1_BBUFFER, new Range<>(1L, 2L)).block(), is(2L));
	}

	@Test // DATAREDIS-525
	public void zRemRangeByScoreShouldRemoveValuesCorrectly() {

		nativeCommands.zadd(KEY_1, 1D, VALUE_1);
		nativeCommands.zadd(KEY_1, 2D, VALUE_2);
		nativeCommands.zadd(KEY_1, 3D, VALUE_3);

		assertThat(connection.zSetCommands().zRemRangeByScore(KEY_1_BBUFFER, new Range<>(1D, 2D)).block(), is(2L));
	}

	@Test // DATAREDIS-525
	public void zRemRangeByScoreShouldRemoveValuesCorrectlyWithNegativeInfinity() {

		nativeCommands.zadd(KEY_1, 1D, VALUE_1);
		nativeCommands.zadd(KEY_1, 2D, VALUE_2);
		nativeCommands.zadd(KEY_1, 3D, VALUE_3);

		assertThat(
				connection.zSetCommands().zRemRangeByScore(KEY_1_BBUFFER, new Range<>(Double.NEGATIVE_INFINITY, 2D)).block(),
				is(2L));
	}

	@Test // DATAREDIS-525
	public void zRemRangeByScoreShouldRemoveValuesCorrectlyWithPositiveInfinity() {

		nativeCommands.zadd(KEY_1, 1D, VALUE_1);
		nativeCommands.zadd(KEY_1, 2D, VALUE_2);
		nativeCommands.zadd(KEY_1, 3D, VALUE_3);

		assertThat(
				connection.zSetCommands().zRemRangeByScore(KEY_1_BBUFFER, new Range<>(2D, Double.POSITIVE_INFINITY)).block(),
				is(2L));
	}

	@Test // DATAREDIS-525
	public void zRemRangeByScoreShouldRemoveValuesCorrectlyWithExcludingMinRange() {

		nativeCommands.zadd(KEY_1, 1D, VALUE_1);
		nativeCommands.zadd(KEY_1, 2D, VALUE_2);
		nativeCommands.zadd(KEY_1, 3D, VALUE_3);

		assertThat(connection.zSetCommands().zRemRangeByScore(KEY_1_BBUFFER, new Range<>(2D, 3D, false, true)).block(),
				is(1L));
	}

	@Test // DATAREDIS-525
	public void zRemRangeByScoreShouldRemoveValuesCorrectlyWithExcludingMaxRange() {

		nativeCommands.zadd(KEY_1, 1D, VALUE_1);
		nativeCommands.zadd(KEY_1, 2D, VALUE_2);
		nativeCommands.zadd(KEY_1, 3D, VALUE_3);

		assertThat(connection.zSetCommands().zRemRangeByScore(KEY_1_BBUFFER, new Range<>(2D, 3D, true, false)).block(),
				is(1L));
	}

	@Test // DATAREDIS-525
	public void zUnionStoreShouldWorkCorrectly() {

		assumeThat(connectionProvider instanceof StandaloneConnectionProvider, is(true));

		nativeCommands.zadd(KEY_1, 1D, VALUE_1);
		nativeCommands.zadd(KEY_1, 2D, VALUE_2);
		nativeCommands.zadd(KEY_2, 1D, VALUE_1);
		nativeCommands.zadd(KEY_2, 2D, VALUE_2);
		nativeCommands.zadd(KEY_2, 3D, VALUE_3);

		assertThat(
				connection.zSetCommands()
						.zUnionStore(KEY_3_BBUFFER, Arrays.asList(KEY_1_BBUFFER, KEY_2_BBUFFER), Arrays.asList(2D, 3D)).block(),
				is(3L));
	}

	@Test // DATAREDIS-525
	public void zInterStoreShouldWorkCorrectly() {

		assumeThat(connectionProvider instanceof StandaloneConnectionProvider, is(true));

		nativeCommands.zadd(KEY_1, 1D, VALUE_1);
		nativeCommands.zadd(KEY_1, 2D, VALUE_2);
		nativeCommands.zadd(KEY_2, 1D, VALUE_1);
		nativeCommands.zadd(KEY_2, 2D, VALUE_2);
		nativeCommands.zadd(KEY_2, 3D, VALUE_3);

		assertThat(
				connection.zSetCommands()
						.zInterStore(KEY_3_BBUFFER, Arrays.asList(KEY_1_BBUFFER, KEY_2_BBUFFER), Arrays.asList(2D, 3D)).block(),
				is(2L));
	}

	@Test // DATAREDIS-525
	public void zRangeByLex() {

		nativeCommands.zadd(KEY_1, 0D, "a");
		nativeCommands.zadd(KEY_1, 0D, "b");
		nativeCommands.zadd(KEY_1, 0D, "c");
		nativeCommands.zadd(KEY_1, 0D, "d");
		nativeCommands.zadd(KEY_1, 0D, "e");
		nativeCommands.zadd(KEY_1, 0D, "f");
		nativeCommands.zadd(KEY_1, 0D, "g");

		assertThat(connection.zSetCommands().zRangeByLex(KEY_1_BBUFFER, new Range<>("", "c")).collectList().block(),
				IsIterableContainingInOrder.contains(ByteBuffer.wrap("a".getBytes()), ByteBuffer.wrap("b".getBytes()),
						ByteBuffer.wrap("c".getBytes())));

		assertThat(
				connection.zSetCommands().zRangeByLex(KEY_1_BBUFFER, new Range<>("", "c", true, false)).collectList().block(),
				IsIterableContainingInOrder.contains(ByteBuffer.wrap("a".getBytes()), ByteBuffer.wrap("b".getBytes())));

		assertThat(
				connection.zSetCommands().zRangeByLex(KEY_1_BBUFFER, new Range<>("aaa", "g", true, false)).collectList()
						.block(),
				IsIterableContainingInOrder.contains(ByteBuffer.wrap("b".getBytes()), ByteBuffer.wrap("c".getBytes()),
						ByteBuffer.wrap("d".getBytes()), ByteBuffer.wrap("e".getBytes()), ByteBuffer.wrap("f".getBytes())));
	}

	@Test // DATAREDIS-525
	public void zRevRangeByLex() {

		nativeCommands.zadd(KEY_1, 0D, "a");
		nativeCommands.zadd(KEY_1, 0D, "b");
		nativeCommands.zadd(KEY_1, 0D, "c");
		nativeCommands.zadd(KEY_1, 0D, "d");
		nativeCommands.zadd(KEY_1, 0D, "e");
		nativeCommands.zadd(KEY_1, 0D, "f");
		nativeCommands.zadd(KEY_1, 0D, "g");

		assertThat(connection.zSetCommands().zRevRangeByLex(KEY_1_BBUFFER, new Range<>("", "c")).collectList().block(),
				IsIterableContainingInOrder.contains(ByteBuffer.wrap("c".getBytes()), ByteBuffer.wrap("b".getBytes()),
						ByteBuffer.wrap("a".getBytes())));

		assertThat(
				connection.zSetCommands().zRevRangeByLex(KEY_1_BBUFFER, new Range<>("", "c", true, false)).collectList()
						.block(),
				IsIterableContainingInOrder.contains(ByteBuffer.wrap("b".getBytes()), ByteBuffer.wrap("a".getBytes())));

		assertThat(
				connection.zSetCommands().zRevRangeByLex(KEY_1_BBUFFER, new Range<>("aaa", "g", true, false)).collectList()
						.block(),
				IsIterableContainingInOrder.contains(ByteBuffer.wrap("f".getBytes()), ByteBuffer.wrap("e".getBytes()),
						ByteBuffer.wrap("d".getBytes()), ByteBuffer.wrap("c".getBytes()), ByteBuffer.wrap("b".getBytes())));
	}

}
