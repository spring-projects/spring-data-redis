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

import static org.hamcrest.collection.IsIterableContainingInOrder.*;
import static org.hamcrest.core.Is.*;
import static org.hamcrest.core.IsEqual.*;
import static org.hamcrest.core.IsNot.*;
import static org.junit.Assert.*;
import static org.junit.Assume.*;
import static org.springframework.data.domain.Range.Bound.*;

import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;

import org.junit.Test;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.ReactiveListCommands.PopResult;
import org.springframework.data.redis.connection.ReactiveListCommands.PushCommand;
import org.springframework.data.redis.connection.ReactiveRedisConnection;
import org.springframework.data.redis.connection.ReactiveRedisConnection.CommandResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.RangeCommand;
import org.springframework.data.redis.connection.RedisListCommands.Position;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Michele Mancioppi
 */
public class LettuceReactiveListCommandTests extends LettuceReactiveCommandsTestsBase {

	@Test // DATAREDIS-525
	public void rPushShouldAppendValuesCorrectly() {

		nativeCommands.lpush(KEY_1, VALUE_1);

		assertThat(connection.listCommands().rPush(KEY_1_BBUFFER, Arrays.asList(VALUE_2_BBUFFER, VALUE_3_BBUFFER)).block(),
				is(3L));
		assertThat(nativeCommands.lrange(KEY_1, 0, -1), contains(VALUE_1, VALUE_2, VALUE_3));
	}

	@Test // DATAREDIS-525
	public void lPushShouldPrependValuesCorrectly() {

		nativeCommands.lpush(KEY_1, VALUE_1);

		assertThat(connection.listCommands().lPush(KEY_1_BBUFFER, Arrays.asList(VALUE_2_BBUFFER, VALUE_3_BBUFFER)).block(),
				is(3L));
		assertThat(nativeCommands.lrange(KEY_1, 0, -1), contains(VALUE_3, VALUE_2, VALUE_1));
	}

	@Test // DATAREDIS-525
	public void rPushXShouldAppendValuesCorrectly() {

		nativeCommands.lpush(KEY_1, VALUE_1);

		assertThat(connection.listCommands().rPushX(KEY_1_BBUFFER, VALUE_2_BBUFFER).block(), is(2L));
		assertThat(nativeCommands.lrange(KEY_1, 0, -1), contains(VALUE_1, VALUE_2));
	}

	@Test // DATAREDIS-525
	public void lPushXShouldPrependValuesCorrectly() {

		nativeCommands.lpush(KEY_1, VALUE_1);

		assertThat(connection.listCommands().lPushX(KEY_1_BBUFFER, VALUE_2_BBUFFER).block(), is(2L));
		assertThat(nativeCommands.lrange(KEY_1, 0, -1), contains(VALUE_2, VALUE_1));
	}

	@Test(expected = InvalidDataAccessApiUsageException.class) // DATAREDIS-525
	public void pushShouldThrowErrorForMoreThanOneValueWhenUsingExistsOption() {

		connection.listCommands()
				.push(Mono.just(
						PushCommand.right().values(Arrays.asList(VALUE_1_BBUFFER, VALUE_2_BBUFFER)).to(KEY_1_BBUFFER).ifExists()))
				.blockFirst();
	}

	@Test // DATAREDIS-525
	public void lLenShouldReturnSizeCorrectly() {

		nativeCommands.lpush(KEY_1, VALUE_1, VALUE_2);

		assertThat(connection.listCommands().lLen(KEY_1_BBUFFER).block(), is(2L));
	}

	@Test // DATAREDIS-525
	public void lRangeShouldReturnValuesCorrectly() {

		nativeCommands.rpush(KEY_1, VALUE_1, VALUE_2, VALUE_3);

		assertThat(connection.listCommands().lRange(KEY_1_BBUFFER, 1, 2).toIterable(),
				contains(VALUE_2_BBUFFER, VALUE_3_BBUFFER));
	}

	@Test // DATAREDIS-852
	public void lRangeShouldReturnValuesCorrectlyWithMinUnbounded() {

		nativeCommands.rpush(KEY_1, VALUE_1, VALUE_2, VALUE_3);

		RangeCommand rangeCommand = RangeCommand.key(KEY_1_BBUFFER).within(Range.of(unbounded(), inclusive(1L)));

		StepVerifier.create(connection.listCommands().lRange(Mono.just(rangeCommand)).flatMap(CommandResponse::getOutput)) //
				.expectNext(VALUE_1_BBUFFER).expectNext(VALUE_2_BBUFFER).verifyComplete();
	}

	@Test // DATAREDIS-852
	public void lRangeShouldReturnValuesCorrectlyWithMaxUnbounded() {

		nativeCommands.rpush(KEY_1, VALUE_1, VALUE_2, VALUE_3);

		RangeCommand rangeCommand = RangeCommand.key(KEY_1_BBUFFER).within(Range.of(inclusive(1L), unbounded()));

		StepVerifier.create(connection.listCommands().lRange(Mono.just(rangeCommand)).flatMap(CommandResponse::getOutput)) //
				.expectNext(VALUE_2_BBUFFER).expectNext(VALUE_3_BBUFFER).verifyComplete();
	}

	@Test // DATAREDIS-525
	public void lTrimShouldReturnValuesCorrectly() {

		nativeCommands.rpush(KEY_1, VALUE_1, VALUE_2, VALUE_3);

		assertThat(connection.listCommands().lTrim(KEY_1_BBUFFER, 1, 2).block(), is(true));
		assertThat(nativeCommands.lrange(KEY_1, 0, -1), not(contains(VALUE_1_BBUFFER)));
	}

	@Test // DATAREDIS-852
	public void lTrimShouldReturnValuesCorrectlyWithMinUnbounded() {

		nativeCommands.rpush(KEY_1, VALUE_1, VALUE_2, VALUE_3);

		RangeCommand rangeCommand = RangeCommand.key(KEY_1_BBUFFER).within(Range.of(unbounded(), inclusive(1L)));

		StepVerifier.create(connection.listCommands().lTrim(Mono.just(rangeCommand))) //
				.expectNext(new ReactiveRedisConnection.BooleanResponse<>(rangeCommand, true)) //
				.verifyComplete();
	}

	@Test // DATAREDIS-852
	public void lTrimShouldReturnValuesCorrectlyWithMaxUnbounded() {

		nativeCommands.rpush(KEY_1, VALUE_1, VALUE_2, VALUE_3);

		RangeCommand rangeCommand = RangeCommand.key(KEY_1_BBUFFER).within(Range.of(inclusive(1L), unbounded()));

		StepVerifier.create(connection.listCommands().lTrim(Mono.just(rangeCommand))) //
				.expectNext(new ReactiveRedisConnection.BooleanResponse<>(rangeCommand, true)) //
				.verifyComplete();
	}

	@Test // DATAREDIS-525
	public void lIndexShouldReturnValueCorrectly() {

		nativeCommands.rpush(KEY_1, VALUE_1, VALUE_2, VALUE_3);

		assertThat(connection.listCommands().lIndex(KEY_1_BBUFFER, 1).block(), is(equalTo(VALUE_2_BBUFFER)));
	}

	@Test // DATAREDIS-525
	public void lInsertShouldAddValueCorrectlyBeforeExisting() {

		nativeCommands.rpush(KEY_1, VALUE_1, VALUE_2);

		assertThat(
				connection.listCommands().lInsert(KEY_1_BBUFFER, Position.BEFORE, VALUE_2_BBUFFER, VALUE_3_BBUFFER).block(),
				is(3L));
		assertThat(nativeCommands.lrange(KEY_1, 0, -1), contains(VALUE_1, VALUE_3, VALUE_2));
	}

	@Test // DATAREDIS-525
	public void lInsertShouldAddValueCorrectlyAfterExisting() {

		nativeCommands.rpush(KEY_1, VALUE_1, VALUE_2);

		assertThat(
				connection.listCommands().lInsert(KEY_1_BBUFFER, Position.AFTER, VALUE_2_BBUFFER, VALUE_3_BBUFFER).block(),
				is(3L));
		assertThat(nativeCommands.lrange(KEY_1, 0, -1), contains(VALUE_1, VALUE_2, VALUE_3));
	}

	@Test // DATAREDIS-525
	public void lSetSouldSetValueCorrectly() {

		nativeCommands.rpush(KEY_1, VALUE_1, VALUE_2);

		assertThat(connection.listCommands().lSet(KEY_1_BBUFFER, 1L, VALUE_3_BBUFFER).block(), is(true));
		assertThat(nativeCommands.lrange(KEY_1, 0, -1), contains(VALUE_1, VALUE_3));
		assertThat(nativeCommands.lrange(KEY_1, 0, -1), not(contains(VALUE_2)));
	}

	@Test // DATAREDIS-525
	public void lRemSouldRemoveAllValuesCorrectly() {

		nativeCommands.rpush(KEY_1, VALUE_1, VALUE_2, VALUE_1, VALUE_3);

		assertThat(connection.listCommands().lRem(KEY_1_BBUFFER, VALUE_1_BBUFFER).block(), is(2L));
		assertThat(nativeCommands.lrange(KEY_1, 0, -1), contains(VALUE_2, VALUE_3));
		assertThat(nativeCommands.lrange(KEY_1, 0, -1), not(contains(VALUE_1)));
	}

	@Test // DATAREDIS-525
	public void lRemSouldRemoveFirstValuesCorrectly() {

		nativeCommands.rpush(KEY_1, VALUE_1, VALUE_2, VALUE_1, VALUE_3);

		assertThat(connection.listCommands().lRem(KEY_1_BBUFFER, 1L, VALUE_1_BBUFFER).block(), is(1L));
		assertThat(nativeCommands.lrange(KEY_1, 0, -1), contains(VALUE_2, VALUE_1, VALUE_3));
	}

	@Test // DATAREDIS-525
	public void lRemSouldRemoveLastValuesCorrectly() {

		nativeCommands.rpush(KEY_1, VALUE_1, VALUE_2, VALUE_1, VALUE_3);

		assertThat(connection.listCommands().lRem(KEY_1_BBUFFER, -1L, VALUE_1_BBUFFER).block(), is(1L));
		assertThat(nativeCommands.lrange(KEY_1, 0, -1), contains(VALUE_1, VALUE_2, VALUE_3));
	}

	@Test // DATAREDIS-525
	public void lPopSouldRemoveFirstValueCorrectly() {

		nativeCommands.rpush(KEY_1, VALUE_1, VALUE_2, VALUE_3);

		assertThat(connection.listCommands().lPop(KEY_1_BBUFFER).block(), is(equalTo(VALUE_1_BBUFFER)));
		assertThat(nativeCommands.lrange(KEY_1, 0, -1), contains(VALUE_2, VALUE_3));
	}

	@Test // DATAREDIS-525
	public void rPopSouldRemoveFirstValueCorrectly() {

		nativeCommands.rpush(KEY_1, VALUE_1, VALUE_2, VALUE_3);

		assertThat(connection.listCommands().rPop(KEY_1_BBUFFER).block(), is(equalTo(VALUE_3_BBUFFER)));
		assertThat(nativeCommands.lrange(KEY_1, 0, -1), contains(VALUE_1, VALUE_2));
	}

	@Test // DATAREDIS-525
	public void blPopShouldReturnFirstAvailable() {

		assumeThat(connectionProvider instanceof StandaloneConnectionProvider, is(true));

		nativeCommands.rpush(KEY_1, VALUE_1, VALUE_2, VALUE_3);

		PopResult result = connection.listCommands()
				.blPop(Arrays.asList(KEY_1_BBUFFER, KEY_2_BBUFFER), Duration.ofSeconds(1L)).block();
		assertThat(result.getKey(), is(equalTo(KEY_1_BBUFFER)));
		assertThat(result.getValue(), is(equalTo(VALUE_1_BBUFFER)));
	}

	@Test // DATAREDIS-525
	public void brPopShouldReturnLastAvailable() {

		assumeThat(connectionProvider instanceof StandaloneConnectionProvider, is(true));

		nativeCommands.rpush(KEY_1, VALUE_1, VALUE_2, VALUE_3);

		PopResult result = connection.listCommands()
				.brPop(Arrays.asList(KEY_1_BBUFFER, KEY_2_BBUFFER), Duration.ofSeconds(1L)).block();
		assertThat(result.getKey(), is(equalTo(KEY_1_BBUFFER)));
		assertThat(result.getValue(), is(equalTo(VALUE_3_BBUFFER)));
	}

	@Test // DATAREDIS-525
	public void rPopLPushShouldWorkCorrectly() {

		nativeCommands.rpush(KEY_1, VALUE_1, VALUE_2, VALUE_3);
		nativeCommands.rpush(KEY_2, VALUE_1);

		ByteBuffer result = connection.listCommands().rPopLPush(KEY_1_BBUFFER, KEY_2_BBUFFER).block();

		assertThat(result, is(equalTo(VALUE_3_BBUFFER)));
		assertThat(nativeCommands.llen(KEY_2), is(2L));
		assertThat(nativeCommands.lindex(KEY_2, 0), is(equalTo(VALUE_3)));
	}

	@Test // DATAREDIS-525
	public void brPopLPushShouldWorkCorrectly() {

		assumeThat(connectionProvider instanceof StandaloneConnectionProvider, is(true));

		nativeCommands.rpush(KEY_1, VALUE_1, VALUE_2, VALUE_3);
		nativeCommands.rpush(KEY_2, VALUE_1);

		ByteBuffer result = connection.listCommands().bRPopLPush(KEY_1_BBUFFER, KEY_2_BBUFFER, Duration.ofSeconds(1))
				.block();

		assertThat(result, is(equalTo(VALUE_3_BBUFFER)));
		assertThat(nativeCommands.llen(KEY_2), is(2L));
		assertThat(nativeCommands.lindex(KEY_2, 0), is(equalTo(VALUE_3)));
	}
}
