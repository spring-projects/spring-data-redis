/*
 * Copyright 2016-2022 the original author or authors.
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

import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;

import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.ReactiveListCommands.LPosCommand;
import org.springframework.data.redis.connection.ReactiveListCommands.PopResult;
import org.springframework.data.redis.connection.ReactiveListCommands.PushCommand;
import org.springframework.data.redis.connection.ReactiveRedisConnection;
import org.springframework.data.redis.connection.ReactiveRedisConnection.CommandResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.RangeCommand;
import org.springframework.data.redis.connection.RedisListCommands.Position;
import org.springframework.data.redis.test.condition.EnabledOnCommand;
import org.springframework.data.redis.test.extension.parametrized.ParameterizedRedisTest;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Michele Mancioppi
 */
public class LettuceReactiveListCommandIntegrationTests extends LettuceReactiveCommandsTestSupport {

	public LettuceReactiveListCommandIntegrationTests(Fixture fixture) {
		super(fixture);
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void rPushShouldAppendValuesCorrectly() {

		nativeCommands.lpush(KEY_1, VALUE_1);

		assertThat(connection.listCommands().rPush(KEY_1_BBUFFER, Arrays.asList(VALUE_2_BBUFFER, VALUE_3_BBUFFER)).block())
				.isEqualTo(3L);
		assertThat(nativeCommands.lrange(KEY_1, 0, -1)).containsExactly(VALUE_1, VALUE_2, VALUE_3);
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void lPushShouldPrependValuesCorrectly() {

		nativeCommands.lpush(KEY_1, VALUE_1);

		assertThat(connection.listCommands().lPush(KEY_1_BBUFFER, Arrays.asList(VALUE_2_BBUFFER, VALUE_3_BBUFFER)).block())
				.isEqualTo(3L);
		assertThat(nativeCommands.lrange(KEY_1, 0, -1)).containsExactly(VALUE_3, VALUE_2, VALUE_1);
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void rPushXShouldAppendValuesCorrectly() {

		nativeCommands.lpush(KEY_1, VALUE_1);

		assertThat(connection.listCommands().rPushX(KEY_1_BBUFFER, VALUE_2_BBUFFER).block()).isEqualTo(2L);
		assertThat(nativeCommands.lrange(KEY_1, 0, -1)).containsExactly(VALUE_1, VALUE_2);
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void lPushXShouldPrependValuesCorrectly() {

		nativeCommands.lpush(KEY_1, VALUE_1);

		assertThat(connection.listCommands().lPushX(KEY_1_BBUFFER, VALUE_2_BBUFFER).block()).isEqualTo(2L);
		assertThat(nativeCommands.lrange(KEY_1, 0, -1)).containsExactly(VALUE_2, VALUE_1);
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void pushShouldThrowErrorForMoreThanOneValueWhenUsingExistsOption() {
		assertThatExceptionOfType(InvalidDataAccessApiUsageException.class).isThrownBy(() -> connection.listCommands()
				.push(Mono.just(
						PushCommand.right().values(Arrays.asList(VALUE_1_BBUFFER, VALUE_2_BBUFFER)).to(KEY_1_BBUFFER).ifExists()))
				.blockFirst());
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void lLenShouldReturnSizeCorrectly() {

		nativeCommands.lpush(KEY_1, VALUE_1, VALUE_2);

		assertThat(connection.listCommands().lLen(KEY_1_BBUFFER).block()).isEqualTo(2L);
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void lRangeShouldReturnValuesCorrectly() {

		nativeCommands.rpush(KEY_1, VALUE_1, VALUE_2, VALUE_3);

		assertThat(connection.listCommands().lRange(KEY_1_BBUFFER, 1, 2).toIterable()).containsExactly(VALUE_2_BBUFFER,
				VALUE_3_BBUFFER);
	}

	@ParameterizedRedisTest // DATAREDIS-852
	void lRangeShouldReturnValuesCorrectlyWithMinUnbounded() {

		nativeCommands.rpush(KEY_1, VALUE_1, VALUE_2, VALUE_3);

		RangeCommand rangeCommand = RangeCommand.key(KEY_1_BBUFFER).within(Range.of(unbounded(), inclusive(1L)));

		connection.listCommands().lRange(Mono.just(rangeCommand)).flatMap(CommandResponse::getOutput)
				.as(StepVerifier::create) //
				.expectNext(VALUE_1_BBUFFER).expectNext(VALUE_2_BBUFFER).verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-852
	void lRangeShouldReturnValuesCorrectlyWithMaxUnbounded() {

		nativeCommands.rpush(KEY_1, VALUE_1, VALUE_2, VALUE_3);

		RangeCommand rangeCommand = RangeCommand.key(KEY_1_BBUFFER).within(Range.of(inclusive(1L), unbounded()));

		connection.listCommands().lRange(Mono.just(rangeCommand)).flatMap(CommandResponse::getOutput)
				.as(StepVerifier::create) //
				.expectNext(VALUE_2_BBUFFER).expectNext(VALUE_3_BBUFFER).verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void lTrimShouldReturnValuesCorrectly() {

		nativeCommands.rpush(KEY_1, VALUE_1, VALUE_2, VALUE_3);

		assertThat(connection.listCommands().lTrim(KEY_1_BBUFFER, 1, 2).block()).isTrue();
		assertThat(nativeCommands.lrange(KEY_1, 0, -1)).doesNotContain(VALUE_1);
	}

	@ParameterizedRedisTest // DATAREDIS-852
	void lTrimShouldReturnValuesCorrectlyWithMinUnbounded() {

		nativeCommands.rpush(KEY_1, VALUE_1, VALUE_2, VALUE_3);

		RangeCommand rangeCommand = RangeCommand.key(KEY_1_BBUFFER).within(Range.of(unbounded(), inclusive(1L)));

		connection.listCommands().lTrim(Mono.just(rangeCommand)).as(StepVerifier::create) //
				.expectNext(new ReactiveRedisConnection.BooleanResponse<>(rangeCommand, true)) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-852
	void lTrimShouldReturnValuesCorrectlyWithMaxUnbounded() {

		nativeCommands.rpush(KEY_1, VALUE_1, VALUE_2, VALUE_3);

		RangeCommand rangeCommand = RangeCommand.key(KEY_1_BBUFFER).within(Range.of(inclusive(1L), unbounded()));

		connection.listCommands().lTrim(Mono.just(rangeCommand)).as(StepVerifier::create) //
				.expectNext(new ReactiveRedisConnection.BooleanResponse<>(rangeCommand, true)) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void lIndexShouldReturnValueCorrectly() {

		nativeCommands.rpush(KEY_1, VALUE_1, VALUE_2, VALUE_3);

		assertThat(connection.listCommands().lIndex(KEY_1_BBUFFER, 1).block()).isEqualTo(VALUE_2_BBUFFER);
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void lInsertShouldAddValueCorrectlyBeforeExisting() {

		nativeCommands.rpush(KEY_1, VALUE_1, VALUE_2);

		assertThat(
				connection.listCommands().lInsert(KEY_1_BBUFFER, Position.BEFORE, VALUE_2_BBUFFER, VALUE_3_BBUFFER).block())
						.isEqualTo(3L);
		assertThat(nativeCommands.lrange(KEY_1, 0, -1)).containsExactly(VALUE_1, VALUE_3, VALUE_2);
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void lInsertShouldAddValueCorrectlyAfterExisting() {

		nativeCommands.rpush(KEY_1, VALUE_1, VALUE_2);

		assertThat(
				connection.listCommands().lInsert(KEY_1_BBUFFER, Position.AFTER, VALUE_2_BBUFFER, VALUE_3_BBUFFER).block())
						.isEqualTo(3L);
		assertThat(nativeCommands.lrange(KEY_1, 0, -1)).containsExactly(VALUE_1, VALUE_2, VALUE_3);
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void lSetSouldSetValueCorrectly() {

		nativeCommands.rpush(KEY_1, VALUE_1, VALUE_2);

		assertThat(connection.listCommands().lSet(KEY_1_BBUFFER, 1L, VALUE_3_BBUFFER).block()).isTrue();
		assertThat(nativeCommands.lrange(KEY_1, 0, -1)).containsExactly(VALUE_1, VALUE_3);
		assertThat(nativeCommands.lrange(KEY_1, 0, -1)).doesNotContain(VALUE_2);
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void lRemSouldRemoveAllValuesCorrectly() {

		nativeCommands.rpush(KEY_1, VALUE_1, VALUE_2, VALUE_1, VALUE_3);

		assertThat(connection.listCommands().lRem(KEY_1_BBUFFER, VALUE_1_BBUFFER).block()).isEqualTo(2L);
		assertThat(nativeCommands.lrange(KEY_1, 0, -1)).containsExactly(VALUE_2, VALUE_3);
		assertThat(nativeCommands.lrange(KEY_1, 0, -1)).doesNotContain(VALUE_1);
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void lRemSouldRemoveFirstValuesCorrectly() {

		nativeCommands.rpush(KEY_1, VALUE_1, VALUE_2, VALUE_1, VALUE_3);

		assertThat(connection.listCommands().lRem(KEY_1_BBUFFER, 1L, VALUE_1_BBUFFER).block()).isEqualTo(1L);
		assertThat(nativeCommands.lrange(KEY_1, 0, -1)).containsExactly(VALUE_2, VALUE_1, VALUE_3);
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void lRemSouldRemoveLastValuesCorrectly() {

		nativeCommands.rpush(KEY_1, VALUE_1, VALUE_2, VALUE_1, VALUE_3);

		assertThat(connection.listCommands().lRem(KEY_1_BBUFFER, -1L, VALUE_1_BBUFFER).block()).isEqualTo(1L);
		assertThat(nativeCommands.lrange(KEY_1, 0, -1)).containsExactly(VALUE_1, VALUE_2, VALUE_3);
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void lPopSouldRemoveFirstValueCorrectly() {

		nativeCommands.rpush(KEY_1, VALUE_1, VALUE_2, VALUE_3);

		assertThat(connection.listCommands().lPop(KEY_1_BBUFFER).block()).isEqualTo(VALUE_1_BBUFFER);
		assertThat(nativeCommands.lrange(KEY_1, 0, -1)).containsExactly(VALUE_2, VALUE_3);
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void rPopSouldRemoveFirstValueCorrectly() {

		nativeCommands.rpush(KEY_1, VALUE_1, VALUE_2, VALUE_3);

		assertThat(connection.listCommands().rPop(KEY_1_BBUFFER).block()).isEqualTo(VALUE_3_BBUFFER);
		assertThat(nativeCommands.lrange(KEY_1, 0, -1)).containsExactly(VALUE_1, VALUE_2);
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void blPopShouldReturnFirstAvailable() {

		assumeThat(connectionProvider).isInstanceOf(StandaloneConnectionProvider.class);

		nativeCommands.rpush(KEY_1, VALUE_1, VALUE_2, VALUE_3);

		PopResult result = connection.listCommands()
				.blPop(Arrays.asList(KEY_1_BBUFFER, KEY_2_BBUFFER), Duration.ofSeconds(1L)).block();
		assertThat(result.getKey()).isEqualTo(KEY_1_BBUFFER);
		assertThat(result.getValue()).isEqualTo(VALUE_1_BBUFFER);
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void brPopShouldReturnLastAvailable() {

		assumeThat(connectionProvider).isInstanceOf(StandaloneConnectionProvider.class);

		nativeCommands.rpush(KEY_1, VALUE_1, VALUE_2, VALUE_3);

		PopResult result = connection.listCommands()
				.brPop(Arrays.asList(KEY_1_BBUFFER, KEY_2_BBUFFER), Duration.ofSeconds(1L)).block();
		assertThat(result.getKey()).isEqualTo(KEY_1_BBUFFER);
		assertThat(result.getValue()).isEqualTo(VALUE_3_BBUFFER);
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void rPopLPushShouldWorkCorrectly() {

		nativeCommands.rpush(KEY_1, VALUE_1, VALUE_2, VALUE_3);
		nativeCommands.rpush(KEY_2, VALUE_1);

		ByteBuffer result = connection.listCommands().rPopLPush(KEY_1_BBUFFER, KEY_2_BBUFFER).block();

		assertThat(result).isEqualTo(VALUE_3_BBUFFER);
		assertThat(nativeCommands.llen(KEY_2)).isEqualTo(2L);
		assertThat(nativeCommands.lindex(KEY_2, 0)).isEqualTo(VALUE_3);
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void brPopLPushShouldWorkCorrectly() {

		assumeThat(connectionProvider).isInstanceOf(StandaloneConnectionProvider.class);

		nativeCommands.rpush(KEY_1, VALUE_1, VALUE_2, VALUE_3);
		nativeCommands.rpush(KEY_2, VALUE_1);

		ByteBuffer result = connection.listCommands().bRPopLPush(KEY_1_BBUFFER, KEY_2_BBUFFER, Duration.ofSeconds(1))
				.block();

		assertThat(result).isEqualTo(VALUE_3_BBUFFER);
		assertThat(nativeCommands.llen(KEY_2)).isEqualTo(2L);
		assertThat(nativeCommands.lindex(KEY_2, 0)).isEqualTo(VALUE_3);
	}

	@ParameterizedRedisTest // DATAREDIS-1196
	@EnabledOnCommand("LPOS")
	void lPos() {

		nativeCommands.rpush(KEY_1, "a", "b", "c", "1", "2", "3", "c", "c");

		connection.listCommands().lPos(KEY_1_BBUFFER, ByteBuffer.wrap("c".getBytes(StandardCharsets.UTF_8))) //
				.as(StepVerifier::create) //
				.expectNext(2L) //
				.verifyComplete();

	}

	@ParameterizedRedisTest // DATAREDIS-1196
	@EnabledOnCommand("LPOS")
	void lPosRank() {

		nativeCommands.rpush(KEY_1, "a", "b", "c", "1", "2", "3", "c", "c");

		connection.listCommands()
				.lPos(LPosCommand.lPosOf(ByteBuffer.wrap("c".getBytes(StandardCharsets.UTF_8))).from(KEY_1_BBUFFER).rank(2)) //
				.as(StepVerifier::create) //
				.expectNext(6L) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-1196
	@EnabledOnCommand("LPOS")
	void lPosNegativeRank() {

		nativeCommands.rpush(KEY_1, "a", "b", "c", "1", "2", "3", "c", "c");

		connection.listCommands()
				.lPos(LPosCommand.lPosOf(ByteBuffer.wrap("c".getBytes(StandardCharsets.UTF_8))).from(KEY_1_BBUFFER).rank(-1)) //
				.as(StepVerifier::create) //
				.expectNext(7L) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-1196
	@EnabledOnCommand("LPOS")
	void lPosCount() {

		nativeCommands.rpush(KEY_1, "a", "b", "c", "1", "2", "3", "c", "c");

		connection.listCommands()
				.lPos(LPosCommand.lPosOf(ByteBuffer.wrap("c".getBytes(StandardCharsets.UTF_8))).from(KEY_1_BBUFFER).count(2)) //
				.as(StepVerifier::create) //
				.expectNext(2L) //
				.expectNext(6L) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-1196
	@EnabledOnCommand("LPOS")
	void lPosRankCount() {

		nativeCommands.rpush(KEY_1, "a", "b", "c", "1", "2", "3", "c", "c");

		connection.listCommands()
				.lPos(LPosCommand.lPosOf(ByteBuffer.wrap("c".getBytes(StandardCharsets.UTF_8))).from(KEY_1_BBUFFER)
						.from(KEY_1_BBUFFER).rank(-1).count(2)) //
				.as(StepVerifier::create) //
				.expectNext(7L) //
				.expectNext(6L) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-1196
	@EnabledOnCommand("LPOS")
	void lPosCountZero() {

		nativeCommands.rpush(KEY_1, "a", "b", "c", "1", "2", "3", "c", "c");

		connection.listCommands()
				.lPos(LPosCommand.lPosOf(ByteBuffer.wrap("c".getBytes(StandardCharsets.UTF_8))).from(KEY_1_BBUFFER).count(0)) //
				.as(StepVerifier::create) //
				.expectNext(2L) //
				.expectNext(6L) //
				.expectNext(7L) //
				.verifyComplete();
	}

}
