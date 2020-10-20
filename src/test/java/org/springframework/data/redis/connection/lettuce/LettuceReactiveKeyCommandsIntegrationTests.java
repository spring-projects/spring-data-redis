/*
 * Copyright 2016-2020 the original author or authors.
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

import io.lettuce.core.SetArgs;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.connection.ReactiveRedisConnection.KeyCommand;
import org.springframework.data.redis.connection.ReactiveRedisConnection.NumericResponse;
import org.springframework.data.redis.connection.ValueEncoding.RedisValueEncoding;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.test.condition.EnabledOnCommand;
import org.springframework.data.redis.test.extension.parametrized.ParameterizedRedisTest;

/**
 * Integration tests for {@link LettuceReactiveKeyCommands}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public class LettuceReactiveKeyCommandsIntegrationTests extends LettuceReactiveCommandsTestSupport {

	public LettuceReactiveKeyCommandsIntegrationTests(Fixture fixture) {
		super(fixture);
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void existsShouldReturnTrueForExistingKeys() {

		nativeCommands.set(KEY_1, VALUE_1);

		connection.keyCommands().exists(KEY_1_BBUFFER).as(StepVerifier::create).expectNext(true).verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void existsShouldReturnFalseForNonExistingKeys() {
		connection.keyCommands().exists(KEY_1_BBUFFER).as(StepVerifier::create).expectNext(false).verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void typeShouldReturnTypeCorrectly() {

		nativeCommands.set(KEY_1, VALUE_2);
		nativeCommands.sadd(KEY_2, VALUE_2);
		nativeCommands.hset(KEY_3, KEY_1, VALUE_1);

		connection.keyCommands().type(KEY_1_BBUFFER).as(StepVerifier::create).expectNext(DataType.STRING).verifyComplete();
		connection.keyCommands().type(KEY_2_BBUFFER).as(StepVerifier::create).expectNext(DataType.SET).verifyComplete();
		connection.keyCommands().type(KEY_3_BBUFFER).as(StepVerifier::create).expectNext(DataType.HASH).verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void keysShouldReturnCorrectly() {

		nativeCommands.set(KEY_1, VALUE_2);
		nativeCommands.set(KEY_2, VALUE_2);
		nativeCommands.set(KEY_3, VALUE_3);

		nativeCommands.set(VALUE_1, KEY_1);
		nativeCommands.set(VALUE_2, KEY_2);
		nativeCommands.set(VALUE_3, KEY_3);

		connection.keyCommands().keys(ByteBuffer.wrap("*".getBytes())).flatMapIterable(it -> it).as(StepVerifier::create) //
				.expectNextCount(6) //
				.verifyComplete();

		connection.keyCommands().keys(ByteBuffer.wrap("key*".getBytes())).flatMapIterable(it -> it).as(StepVerifier::create) //
				.expectNextCount(3).verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-743
	void scanShouldShouldIterateOverKeyspace() {

		nativeCommands.set(KEY_1, VALUE_2);
		nativeCommands.set(KEY_2, VALUE_2);
		nativeCommands.set(KEY_3, VALUE_3);

		nativeCommands.set(VALUE_1, KEY_1);
		nativeCommands.set(VALUE_2, KEY_2);
		nativeCommands.set(VALUE_3, KEY_3);

		connection.keyCommands().scan(ScanOptions.scanOptions().count(2).build()).as(StepVerifier::create) //
				.expectNextCount(6) //
				.verifyComplete();

		connection.keyCommands().scan(ScanOptions.scanOptions().count(2).match("key*").build()).as(StepVerifier::create) //
				.expectNextCount(3) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void randomKeyShouldReturnAnyKey() {

		nativeCommands.set(KEY_1, VALUE_2);
		nativeCommands.set(KEY_2, VALUE_2);
		nativeCommands.set(KEY_3, VALUE_3);

		connection.keyCommands().randomKey().as(StepVerifier::create).expectNextCount(1).verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void randomKeyShouldReturnNullWhenNoKeyExists() {
		connection.keyCommands().randomKey().as(StepVerifier::create).verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void renameShouldAlterKeyNameCorrectly() {

		nativeCommands.set(KEY_1, VALUE_2);

		connection.keyCommands().rename(KEY_1_BBUFFER, KEY_2_BBUFFER).as(StepVerifier::create).expectNext(true)
				.verifyComplete();
		assertThat(nativeCommands.exists(KEY_2)).isEqualTo(1L);
		assertThat(nativeCommands.exists(KEY_1)).isEqualTo(0L);
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void renameShouldThrowErrorWhenKeyDoesNotExist() {

		connection.keyCommands().rename(KEY_1_BBUFFER, KEY_2_BBUFFER).as(StepVerifier::create)
				.expectError(RedisSystemException.class).verify();
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void renameNXShouldAlterKeyNameCorrectly() {

		nativeCommands.set(KEY_1, VALUE_2);

		connection.keyCommands().renameNX(KEY_1_BBUFFER, KEY_2_BBUFFER).as(StepVerifier::create).expectNext(true)
				.verifyComplete();

		assertThat(nativeCommands.exists(KEY_2)).isEqualTo(1L);
		assertThat(nativeCommands.exists(KEY_1)).isEqualTo(0L);
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void renameNXShouldNotAlterExistingKeyName() {

		nativeCommands.set(KEY_1, VALUE_2);
		nativeCommands.set(KEY_2, VALUE_2);

		connection.keyCommands().renameNX(KEY_1_BBUFFER, KEY_2_BBUFFER).as(StepVerifier::create).expectNext(false)
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void shouldDeleteKeyCorrectly() {

		nativeCommands.set(KEY_1, VALUE_1);

		connection.keyCommands().del(KEY_1_BBUFFER).as(StepVerifier::create).expectNext(1L).verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void shouldDeleteKeysCorrectly() {

		nativeCommands.set(KEY_1, VALUE_1);
		nativeCommands.set(KEY_2, VALUE_2);

		Flux<NumericResponse<KeyCommand, Long>> result = connection.keyCommands()
				.del(Flux.fromIterable(Arrays.asList(new KeyCommand(KEY_1_BBUFFER), new KeyCommand(KEY_2_BBUFFER))));

		result.as(StepVerifier::create).expectNextCount(2).verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void shouldDeleteKeysInBatchCorrectly() {

		nativeCommands.set(KEY_1, VALUE_1);
		nativeCommands.set(KEY_2, VALUE_2);

		Mono<Long> result = connection.keyCommands().mDel(Arrays.asList(KEY_1_BBUFFER, KEY_2_BBUFFER));

		result.as(StepVerifier::create).expectNext(2L).verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void shouldDeleteKeysInMultipleBatchesCorrectly() {

		nativeCommands.set(KEY_1, VALUE_1);
		nativeCommands.set(KEY_2, VALUE_2);

		Flux<List<ByteBuffer>> input = Flux.just(Arrays.asList(KEY_1_BBUFFER, KEY_2_BBUFFER),
				Collections.singletonList(KEY_1_BBUFFER));

		Flux<Long> result = connection.keyCommands().mDel(input).map(NumericResponse::getOutput);

		result.as(StepVerifier::create).expectNextCount(2).verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-693
	@EnabledOnCommand("UNLINK")
	void shouldUnlinkKeyCorrectly() {

		nativeCommands.set(KEY_1, VALUE_1);

		connection.keyCommands().unlink(KEY_1_BBUFFER).as(StepVerifier::create).expectNext(1L).verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-693
	@EnabledOnCommand("UNLINK")
	void shouldUnlinkKeysCorrectly() {

		nativeCommands.set(KEY_1, VALUE_1);
		nativeCommands.set(KEY_2, VALUE_2);

		Flux<NumericResponse<KeyCommand, Long>> result = connection.keyCommands()
				.unlink(Flux.fromIterable(Arrays.asList(new KeyCommand(KEY_1_BBUFFER), new KeyCommand(KEY_2_BBUFFER))));

		result.as(StepVerifier::create).expectNextCount(2).verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-693
	@EnabledOnCommand("UNLINK")
	void shouldUnlinkKeysInBatchCorrectly() {

		nativeCommands.set(KEY_1, VALUE_1);
		nativeCommands.set(KEY_2, VALUE_2);

		Mono<Long> result = connection.keyCommands().mUnlink(Arrays.asList(KEY_1_BBUFFER, KEY_2_BBUFFER));

		result.as(StepVerifier::create).expectNext(2L).verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-693
	@EnabledOnCommand("UNLINK")
	void shouldUnlinkKeysInMultipleBatchesCorrectly() {

		nativeCommands.set(KEY_1, VALUE_1);
		nativeCommands.set(KEY_2, VALUE_2);

		Flux<List<ByteBuffer>> input = Flux.just(Arrays.asList(KEY_1_BBUFFER, KEY_2_BBUFFER),
				Collections.singletonList(KEY_1_BBUFFER));

		Flux<Long> result = connection.keyCommands().mUnlink(input).map(NumericResponse::getOutput);

		result.as(StepVerifier::create).expectNextCount(2).verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-602
	void shouldExpireKeysCorrectly() {

		nativeCommands.set(KEY_1, VALUE_1);

		connection.keyCommands().expire(KEY_1_BBUFFER, Duration.ofSeconds(10)).as(StepVerifier::create) //
				.expectNext(true) //
				.expectComplete() //
				.verify();

		assertThat(nativeCommands.ttl(KEY_1)).isGreaterThan(8L);
	}

	@ParameterizedRedisTest // DATAREDIS-602, DATAREDIS-1031
	void shouldPreciseExpireKeysCorrectly() {

		nativeCommands.set(KEY_1, VALUE_1);

		connection.keyCommands().pExpire(KEY_1_BBUFFER, Duration.ofSeconds(10)).as(StepVerifier::create) //
				.expectNext(true) //
				.expectComplete() //
				.verify();

		assertThat(nativeCommands.ttl(KEY_1)).isGreaterThan(8).isLessThan(11);
	}

	@ParameterizedRedisTest // DATAREDIS-602, DATAREDIS-1031
	void shouldExpireAtKeysCorrectly() {

		nativeCommands.set(KEY_1, VALUE_1);
		Instant expireAt = Instant.now().plus(Duration.ofSeconds(10));

		connection.keyCommands().expireAt(KEY_1_BBUFFER, expireAt).as(StepVerifier::create) //
				.expectNext(true) //
				.expectComplete() //
				.verify();

		assertThat(nativeCommands.ttl(KEY_1)).isGreaterThan(8).isLessThan(11);
	}

	@ParameterizedRedisTest // DATAREDIS-602, DATAREDIS-1031
	void shouldPreciseExpireAtKeysCorrectly() {

		nativeCommands.set(KEY_1, VALUE_1);
		Instant expireAt = Instant.now().plus(Duration.ofSeconds(10));

		connection.keyCommands().pExpireAt(KEY_1_BBUFFER, expireAt).as(StepVerifier::create) //
				.expectNext(true) //
				.expectComplete() //
				.verify();

		assertThat(nativeCommands.ttl(KEY_1)).isGreaterThan(8).isLessThan(11);
	}

	@ParameterizedRedisTest // DATAREDIS-602
	void shouldReportTimeToLiveCorrectly() {

		nativeCommands.set(KEY_1, VALUE_1, SetArgs.Builder.ex(10));

		connection.keyCommands().ttl(KEY_1_BBUFFER).as(StepVerifier::create) //
				.expectNextCount(1) //
				.expectComplete() //
				.verify();

		assertThat(nativeCommands.ttl(KEY_1)).isGreaterThan(8L);
	}

	@ParameterizedRedisTest // DATAREDIS-602
	void shouldReportPreciseTimeToLiveCorrectly() {

		nativeCommands.set(KEY_1, VALUE_1, SetArgs.Builder.ex(10));

		connection.keyCommands().pTtl(KEY_1_BBUFFER).as(StepVerifier::create) //
				.expectNextMatches(actual -> {
					assertThat(actual).isGreaterThan(8000L);
					return true;
				}) //
				.expectComplete() //
				.verify();
	}

	@ParameterizedRedisTest // DATAREDIS-602
	void shouldPersist() {

		nativeCommands.set(KEY_1, VALUE_1, SetArgs.Builder.ex(10));

		connection.keyCommands().persist(KEY_1_BBUFFER).as(StepVerifier::create) //
				.expectNext(true) //
				.expectComplete() //
				.verify();

		assertThat(nativeCommands.ttl(KEY_1)).isEqualTo(-1L);
	}

	@ParameterizedRedisTest // DATAREDIS-602
	void shouldMoveToDatabase() {

		assumeThat(connection).isNotInstanceOf(LettuceReactiveRedisClusterConnection.class);

		nativeCommands.set(KEY_1, VALUE_1);

		connection.keyCommands().move(KEY_1_BBUFFER, 5).as(StepVerifier::create) //
				.expectNext(true) //
				.expectComplete() //
				.verify();
		assertThat(nativeCommands.exists(KEY_1)).isEqualTo(0L);
	}

	@ParameterizedRedisTest // DATAREDIS-694
	void touchReturnsNrOfKeysTouched() {

		nativeCommands.set(KEY_1, VALUE_1);
		nativeCommands.set(KEY_2, VALUE_2);

		connection.keyCommands().touch(Arrays.asList(KEY_1_BBUFFER, KEY_2_BBUFFER, KEY_3_BBUFFER)).as(StepVerifier::create)
				.expectNext(2L) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-694
	void touchReturnsZeroIfNoKeysTouched() {

		connection.keyCommands().touch(Collections.singletonList(KEY_1_BBUFFER)).as(StepVerifier::create) //
				.expectNext(0L) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-716
	void encodingReturnsCorrectly() {

		nativeCommands.set(KEY_1, "1000");

		connection.keyCommands().encodingOf(KEY_1_BBUFFER).as(StepVerifier::create).expectNext(RedisValueEncoding.INT)
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-716
	void encodingReturnsVacantWhenKeyDoesNotExist() {

		connection.keyCommands().encodingOf(KEY_1_BBUFFER).as(StepVerifier::create).expectNext(RedisValueEncoding.VACANT)
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-716
	void idletimeReturnsCorrectly() {

		nativeCommands.set(KEY_1, "1000");
		nativeCommands.get(KEY_1);

		connection.keyCommands().idletime(KEY_1_BBUFFER).as(StepVerifier::create).assertNext(actual -> {
			assertThat(actual).isLessThan(Duration.ofSeconds(5));
		}).verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-716
	void idldetimeReturnsNullWhenKeyDoesNotExist() {
		connection.keyCommands().idletime(KEY_1_BBUFFER).as(StepVerifier::create).verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-716
	void refcountReturnsCorrectly() {

		nativeCommands.lpush(KEY_1, "1000");

		connection.keyCommands().refcount(KEY_1_BBUFFER).as(StepVerifier::create).expectNext(1L).verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-716
	void refcountReturnsNullWhenKeyDoesNotExist() {
		connection.keyCommands().refcount(KEY_1_BBUFFER).as(StepVerifier::create).verifyComplete();
	}
}
