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

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static org.junit.Assume.*;

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

import org.junit.Rule;
import org.junit.Test;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.connection.ReactiveRedisConnection.KeyCommand;
import org.springframework.data.redis.connection.ReactiveRedisConnection.NumericResponse;
import org.springframework.data.redis.connection.ValueEncoding.RedisValueEncoding;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.test.util.MinimumRedisVersionRule;
import org.springframework.test.annotation.IfProfileValue;

/**
 * Integration tests for {@link LettuceReactiveKeyCommands}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public class LettuceReactiveKeyCommandsTests extends LettuceReactiveCommandsTestsBase {

	public @Rule MinimumRedisVersionRule versionRule = new MinimumRedisVersionRule();

	@Test // DATAREDIS-525
	public void existsShouldReturnTrueForExistingKeys() {

		nativeCommands.set(KEY_1, VALUE_1);

		StepVerifier.create(connection.keyCommands().exists(KEY_1_BBUFFER)).expectNext(true).verifyComplete();
	}

	@Test // DATAREDIS-525
	public void existsShouldReturnFalseForNonExistingKeys() {
		StepVerifier.create(connection.keyCommands().exists(KEY_1_BBUFFER)).expectNext(false).verifyComplete();
	}

	@Test // DATAREDIS-525
	public void typeShouldReturnTypeCorrectly() {

		nativeCommands.set(KEY_1, VALUE_2);
		nativeCommands.sadd(KEY_2, VALUE_2);
		nativeCommands.hset(KEY_3, KEY_1, VALUE_1);

		StepVerifier.create(connection.keyCommands().type(KEY_1_BBUFFER)).expectNext(DataType.STRING).verifyComplete();
		StepVerifier.create(connection.keyCommands().type(KEY_2_BBUFFER)).expectNext(DataType.SET).verifyComplete();
		StepVerifier.create(connection.keyCommands().type(KEY_3_BBUFFER)).expectNext(DataType.HASH).verifyComplete();
	}

	@Test // DATAREDIS-525
	public void keysShouldReturnCorrectly() {

		nativeCommands.set(KEY_1, VALUE_2);
		nativeCommands.set(KEY_2, VALUE_2);
		nativeCommands.set(KEY_3, VALUE_3);

		nativeCommands.set(VALUE_1, KEY_1);
		nativeCommands.set(VALUE_2, KEY_2);
		nativeCommands.set(VALUE_3, KEY_3);

		StepVerifier.create(connection.keyCommands().keys(ByteBuffer.wrap("*".getBytes())).flatMapIterable(it -> it)) //
				.expectNextCount(6) //
				.verifyComplete();

		StepVerifier.create(connection.keyCommands().keys(ByteBuffer.wrap("key*".getBytes())).flatMapIterable(it -> it)) //
				.expectNextCount(3).verifyComplete();
	}

	@Test // DATAREDIS-743
	public void scanShouldShouldIterateOverKeyspace() {

		nativeCommands.set(KEY_1, VALUE_2);
		nativeCommands.set(KEY_2, VALUE_2);
		nativeCommands.set(KEY_3, VALUE_3);

		nativeCommands.set(VALUE_1, KEY_1);
		nativeCommands.set(VALUE_2, KEY_2);
		nativeCommands.set(VALUE_3, KEY_3);

		StepVerifier.create(connection.keyCommands().scan(ScanOptions.scanOptions().count(2).build())) //
				.expectNextCount(6) //
				.verifyComplete();

		StepVerifier.create(connection.keyCommands().scan(ScanOptions.scanOptions().count(2).match("key*").build())) //
				.expectNextCount(3) //
				.verifyComplete();
	}

	@Test // DATAREDIS-525
	public void randomKeyShouldReturnAnyKey() {

		nativeCommands.set(KEY_1, VALUE_2);
		nativeCommands.set(KEY_2, VALUE_2);
		nativeCommands.set(KEY_3, VALUE_3);

		StepVerifier.create(connection.keyCommands().randomKey()).expectNextCount(1).verifyComplete();
	}

	@Test // DATAREDIS-525
	public void randomKeyShouldReturnNullWhenNoKeyExists() {
		StepVerifier.create(connection.keyCommands().randomKey()).verifyComplete();
	}

	@Test // DATAREDIS-525
	public void renameShouldAlterKeyNameCorrectly() {

		nativeCommands.set(KEY_1, VALUE_2);

		StepVerifier.create(connection.keyCommands().rename(KEY_1_BBUFFER, KEY_2_BBUFFER)).expectNext(true)
				.verifyComplete();
		assertThat(nativeCommands.exists(KEY_2), is(1L));
		assertThat(nativeCommands.exists(KEY_1), is(0L));
	}

	@Test // DATAREDIS-525
	public void renameShouldThrowErrorWhenKeyDoesNotExist() {

		StepVerifier.create(connection.keyCommands().rename(KEY_1_BBUFFER, KEY_2_BBUFFER))
				.expectError(RedisSystemException.class).verify();
	}

	@Test // DATAREDIS-525
	public void renameNXShouldAlterKeyNameCorrectly() {

		nativeCommands.set(KEY_1, VALUE_2);

		StepVerifier.create(connection.keyCommands().renameNX(KEY_1_BBUFFER, KEY_2_BBUFFER)).expectNext(true)
				.verifyComplete();

		assertThat(nativeCommands.exists(KEY_2), is(1L));
		assertThat(nativeCommands.exists(KEY_1), is(0L));
	}

	@Test // DATAREDIS-525
	public void renameNXShouldNotAlterExistingKeyName() {

		nativeCommands.set(KEY_1, VALUE_2);
		nativeCommands.set(KEY_2, VALUE_2);

		StepVerifier.create(connection.keyCommands().renameNX(KEY_1_BBUFFER, KEY_2_BBUFFER)).expectNext(false)
				.verifyComplete();
	}

	@Test // DATAREDIS-525
	public void shouldDeleteKeyCorrectly() {

		nativeCommands.set(KEY_1, VALUE_1);

		StepVerifier.create(connection.keyCommands().del(KEY_1_BBUFFER)).expectNext(1L).verifyComplete();
	}

	@Test // DATAREDIS-525
	public void shouldDeleteKeysCorrectly() {

		nativeCommands.set(KEY_1, VALUE_1);
		nativeCommands.set(KEY_2, VALUE_2);

		Flux<NumericResponse<KeyCommand, Long>> result = connection.keyCommands()
				.del(Flux.fromIterable(Arrays.asList(new KeyCommand(KEY_1_BBUFFER), new KeyCommand(KEY_2_BBUFFER))));

		StepVerifier.create(result).expectNextCount(2).verifyComplete();
	}

	@Test // DATAREDIS-525
	public void shouldDeleteKeysInBatchCorrectly() {

		nativeCommands.set(KEY_1, VALUE_1);
		nativeCommands.set(KEY_2, VALUE_2);

		Mono<Long> result = connection.keyCommands().mDel(Arrays.asList(KEY_1_BBUFFER, KEY_2_BBUFFER));

		StepVerifier.create(result).expectNext(2L).verifyComplete();
	}

	@Test // DATAREDIS-525
	public void shouldDeleteKeysInMultipleBatchesCorrectly() {

		nativeCommands.set(KEY_1, VALUE_1);
		nativeCommands.set(KEY_2, VALUE_2);

		Flux<List<ByteBuffer>> input = Flux.just(Arrays.asList(KEY_1_BBUFFER, KEY_2_BBUFFER),
				Collections.singletonList(KEY_1_BBUFFER));

		Flux<Long> result = connection.keyCommands().mDel(input).map(NumericResponse::getOutput);

		StepVerifier.create(result).expectNextCount(2).verifyComplete();
	}

	@Test // DATAREDIS-693
	@IfProfileValue(name = "redisVersion", value = "4.0.0+")
	public void shouldUnlinkKeyCorrectly() {

		nativeCommands.set(KEY_1, VALUE_1);

		StepVerifier.create(connection.keyCommands().unlink(KEY_1_BBUFFER)).expectNext(1L).verifyComplete();
	}

	@Test // DATAREDIS-693
	@IfProfileValue(name = "redisVersion", value = "4.0.0+")
	public void shouldUnlinkKeysCorrectly() {

		nativeCommands.set(KEY_1, VALUE_1);
		nativeCommands.set(KEY_2, VALUE_2);

		Flux<NumericResponse<KeyCommand, Long>> result = connection.keyCommands()
				.unlink(Flux.fromIterable(Arrays.asList(new KeyCommand(KEY_1_BBUFFER), new KeyCommand(KEY_2_BBUFFER))));

		StepVerifier.create(result).expectNextCount(2).verifyComplete();
	}

	@Test // DATAREDIS-693
	@IfProfileValue(name = "redisVersion", value = "4.0.0+")
	public void shouldUnlinkKeysInBatchCorrectly() {

		nativeCommands.set(KEY_1, VALUE_1);
		nativeCommands.set(KEY_2, VALUE_2);

		Mono<Long> result = connection.keyCommands().mUnlink(Arrays.asList(KEY_1_BBUFFER, KEY_2_BBUFFER));

		StepVerifier.create(result).expectNext(2L).verifyComplete();
	}

	@Test // DATAREDIS-693
	@IfProfileValue(name = "redisVersion", value = "4.0.0+")
	public void shouldUnlinkKeysInMultipleBatchesCorrectly() {

		nativeCommands.set(KEY_1, VALUE_1);
		nativeCommands.set(KEY_2, VALUE_2);

		Flux<List<ByteBuffer>> input = Flux.just(Arrays.asList(KEY_1_BBUFFER, KEY_2_BBUFFER),
				Collections.singletonList(KEY_1_BBUFFER));

		Flux<Long> result = connection.keyCommands().mUnlink(input).map(NumericResponse::getOutput);

		StepVerifier.create(result).expectNextCount(2).verifyComplete();
	}

	@Test // DATAREDIS-602
	public void shouldExpireKeysCorrectly() {

		nativeCommands.set(KEY_1, VALUE_1);

		StepVerifier.create(connection.keyCommands().expire(KEY_1_BBUFFER, Duration.ofSeconds(10))) //
				.expectNext(true) //
				.expectComplete() //
				.verify();

		assertThat(nativeCommands.ttl(KEY_1), is(greaterThan(8L)));
	}

	@Test // DATAREDIS-602
	public void shouldPreciseExpireKeysCorrectly() {

		nativeCommands.set(KEY_1, VALUE_1);

		StepVerifier.create(connection.keyCommands().pExpire(KEY_1_BBUFFER, Duration.ofSeconds(10))) //
				.expectNext(true) //
				.expectComplete() //
				.verify();

		assertThat(nativeCommands.ttl(KEY_1), is(greaterThan(8L)));
	}

	@Test // DATAREDIS-602
	public void shouldExpireAtKeysCorrectly() {

		nativeCommands.set(KEY_1, VALUE_1);
		Instant expireAt = Instant.now().plus(Duration.ofSeconds(10));

		StepVerifier.create(connection.keyCommands().expireAt(KEY_1_BBUFFER, expireAt)) //
				.expectNext(true) //
				.expectComplete() //
				.verify();

		assertThat(nativeCommands.ttl(KEY_1), is(greaterThan(8L)));
	}

	@Test // DATAREDIS-602
	public void shouldPreciseExpireAtKeysCorrectly() {

		nativeCommands.set(KEY_1, VALUE_1);
		Instant expireAt = Instant.now().plus(Duration.ofSeconds(10));

		StepVerifier.create(connection.keyCommands().pExpireAt(KEY_1_BBUFFER, expireAt)) //
				.expectNext(true) //
				.expectComplete() //
				.verify();

		assertThat(nativeCommands.ttl(KEY_1), is(greaterThan(8L)));
	}

	@Test // DATAREDIS-602
	public void shouldReportTimeToLiveCorrectly() {

		nativeCommands.set(KEY_1, VALUE_1, SetArgs.Builder.ex(10));

		StepVerifier.create(connection.keyCommands().ttl(KEY_1_BBUFFER)) //
				.expectNextMatches(actual -> {
					assertThat(nativeCommands.ttl(KEY_1), is(greaterThan(8L)));
					return true;
				}) //
				.expectComplete() //
				.verify();
	}

	@Test // DATAREDIS-602
	public void shouldReportPreciseTimeToLiveCorrectly() {

		nativeCommands.set(KEY_1, VALUE_1, SetArgs.Builder.ex(10));

		StepVerifier.create(connection.keyCommands().pTtl(KEY_1_BBUFFER)) //
				.expectNextMatches(actual -> {
					assertThat(actual, is(greaterThan(8000L)));
					return true;
				}) //
				.expectComplete() //
				.verify();
	}

	@Test // DATAREDIS-602
	public void shouldPersist() {

		nativeCommands.set(KEY_1, VALUE_1, SetArgs.Builder.ex(10));

		StepVerifier.create(connection.keyCommands().persist(KEY_1_BBUFFER)) //
				.expectNext(true) //
				.expectComplete() //
				.verify();

		assertThat(nativeCommands.ttl(KEY_1), is(-1L));
	}

	@Test // DATAREDIS-602
	public void shouldMoveToDatabase() {

		assumeThat(connection, is(not(instanceOf(LettuceReactiveRedisClusterConnection.class))));

		nativeCommands.set(KEY_1, VALUE_1);

		StepVerifier.create(connection.keyCommands().move(KEY_1_BBUFFER, 5)) //
				.expectNext(true) //
				.expectComplete() //
				.verify();
		assertThat(nativeCommands.exists(KEY_1), is(0L));
	}

	@Test // DATAREDIS-694
	public void touchReturnsNrOfKeysTouched() {

		nativeCommands.set(KEY_1, VALUE_1);
		nativeCommands.set(KEY_2, VALUE_2);

		StepVerifier.create(connection.keyCommands().touch(Arrays.asList(KEY_1_BBUFFER, KEY_2_BBUFFER, KEY_3_BBUFFER)))
				.expectNext(2L) //
				.verifyComplete();
	}

	@Test // DATAREDIS-694
	public void touchReturnsZeroIfNoKeysTouched() {

		StepVerifier.create(connection.keyCommands().touch(Collections.singletonList(KEY_1_BBUFFER))) //
				.expectNext(0L) //
				.verifyComplete();
	}

	@Test // DATAREDIS-716
	public void encodingReturnsCorrectly() {

		nativeCommands.set(KEY_1, "1000");

		connection.keyCommands().encodingOf(KEY_1_BBUFFER).as(StepVerifier::create).expectNext(RedisValueEncoding.INT)
				.verifyComplete();
	}

	@Test // DATAREDIS-716
	public void encodingReturnsVacantWhenKeyDoesNotExist() {

		connection.keyCommands().encodingOf(KEY_1_BBUFFER).as(StepVerifier::create).expectNext(RedisValueEncoding.VACANT)
				.verifyComplete();
	}

	@Test // DATAREDIS-716
	public void idletimeReturnsCorrectly() {

		nativeCommands.set(KEY_1, "1000");
		nativeCommands.get(KEY_1);

		connection.keyCommands().idletime(KEY_1_BBUFFER).as(StepVerifier::create).expectNext(Duration.ofSeconds(0))
				.verifyComplete();
	}

	@Test // DATAREDIS-716
	public void idldetimeReturnsNullWhenKeyDoesNotExist() {
		connection.keyCommands().idletime(KEY_1_BBUFFER).as(StepVerifier::create).verifyComplete();
	}

	@Test // DATAREDIS-716
	public void refcountReturnsCorrectly() {

		nativeCommands.lpush(KEY_1, "1000");

		connection.keyCommands().refcount(KEY_1_BBUFFER).as(StepVerifier::create).expectNext(1L).verifyComplete();
	}

	@Test // DATAREDIS-716
	public void refcountReturnsNullWhenKeyDoesNotExist() {
		connection.keyCommands().refcount(KEY_1_BBUFFER).as(StepVerifier::create).verifyComplete();
	}
}
