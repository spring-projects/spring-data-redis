/*
 * Copyright 2016-2021 the original author or authors.
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
import static org.junit.Assume.*;
import static org.springframework.data.redis.connection.BitFieldSubCommands.*;
import static org.springframework.data.redis.connection.BitFieldSubCommands.BitFieldIncrBy.Overflow.*;
import static org.springframework.data.redis.connection.BitFieldSubCommands.BitFieldType.*;
import static org.springframework.data.redis.connection.BitFieldSubCommands.Offset.offset;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.assertj.core.data.Offset;

import org.springframework.data.domain.Range;
import org.springframework.data.domain.Range.Bound;
import org.springframework.data.redis.connection.ReactiveRedisConnection;
import org.springframework.data.redis.connection.ReactiveRedisConnection.ByteBufferResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.CommandResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.KeyCommand;
import org.springframework.data.redis.connection.ReactiveRedisConnection.MultiValueResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.RangeCommand;
import org.springframework.data.redis.connection.ReactiveStringCommands.SetCommand;
import org.springframework.data.redis.connection.RedisStringCommands.BitOperation;
import org.springframework.data.redis.connection.RedisStringCommands.SetOption;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.data.redis.test.condition.EnabledOnCommand;
import org.springframework.data.redis.test.extension.parametrized.ParameterizedRedisTest;
import org.springframework.data.redis.test.util.HexStringUtils;
import org.springframework.data.redis.util.ByteUtils;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Michele Mancioppi
 */
public class LettuceReactiveStringCommandsIntegrationTests extends LettuceReactiveCommandsTestSupport {

	public LettuceReactiveStringCommandsIntegrationTests(Fixture fixture) {
		super(fixture);
	}

	@ParameterizedRedisTest // GH-2050
	@EnabledOnCommand("GETEX")
	void getExShouldWorkCorrectly() {

		nativeCommands.set(KEY_1, VALUE_1);

		connection.stringCommands().getEx(KEY_1_BBUFFER, Expiration.seconds(10)).as(StepVerifier::create) //
				.expectNext(VALUE_1_BBUFFER) //
				.verifyComplete();

		assertThat(nativeCommands.ttl(KEY_1)).isGreaterThan(1L);
	}

	@ParameterizedRedisTest // GH-2050
	@EnabledOnCommand("GETDEL")
	void getDelShouldWorkCorrectly() {

		nativeCommands.set(KEY_1, VALUE_1);

		connection.stringCommands().getDel(KEY_1_BBUFFER).as(StepVerifier::create) //
				.expectNext(VALUE_1_BBUFFER) //
				.verifyComplete();

		assertThat(nativeCommands.exists(KEY_1)).isZero();
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void getSetShouldReturnPreviousValueCorrectly() {

		nativeCommands.set(KEY_1, VALUE_1);

		connection.stringCommands().getSet(KEY_1_BBUFFER, VALUE_2_BBUFFER).as(StepVerifier::create) //
				.expectNext(VALUE_1_BBUFFER) //
				.verifyComplete();

		assertThat(nativeCommands.get(KEY_1)).isEqualTo(VALUE_2);
	}

	@ParameterizedRedisTest // DATAREDIS-525, DATAREDIS-645
	void getSetShouldNotEmitPreviousValueCorrectlyWhenNotExists() {

		connection.stringCommands().getSet(KEY_1_BBUFFER, VALUE_2_BBUFFER).as(StepVerifier::create).verifyComplete();

		assertThat(nativeCommands.get(KEY_1)).isEqualTo(VALUE_2);
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void setShouldAddValueCorrectly() {

		connection.stringCommands().set(KEY_1_BBUFFER, VALUE_1_BBUFFER).as(StepVerifier::create) //
				.expectNext(true) //
				.verifyComplete();

		assertThat(nativeCommands.get(KEY_1)).isEqualTo(VALUE_1);
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void setShouldAddValuesCorrectly() {

		List<SetCommand> setCommands = Arrays.asList(SetCommand.set(KEY_1_BBUFFER).value(VALUE_1_BBUFFER),
				SetCommand.set(KEY_2_BBUFFER).value(VALUE_2_BBUFFER));

		connection.stringCommands().set(Flux.fromIterable(setCommands)).as(StepVerifier::create) //
				.expectNextCount(2) //
				.verifyComplete();

		assertThat(nativeCommands.get(KEY_1)).isEqualTo(VALUE_1);
		assertThat(nativeCommands.get(KEY_2)).isEqualTo(VALUE_2);
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void getShouldRetrieveValueCorrectly() {

		nativeCommands.set(KEY_1, VALUE_1);

		connection.stringCommands().get(KEY_1_BBUFFER).as(StepVerifier::create).expectNext(VALUE_1_BBUFFER)
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-525, DATAREDIS-645
	void getShouldNotEmitValueValueIfAbsent() {
		connection.stringCommands().get(KEY_1_BBUFFER).as(StepVerifier::create).verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void getShouldRetrieveValuesCorrectly() {

		nativeCommands.set(KEY_1, VALUE_1);
		nativeCommands.set(KEY_2, VALUE_2);

		Stream<KeyCommand> stream = Stream.of(new KeyCommand(KEY_1_BBUFFER), new KeyCommand(KEY_2_BBUFFER));
		Flux<ByteBufferResponse<KeyCommand>> result = connection.stringCommands().get(Flux.fromStream(stream));

		result.map(CommandResponse::getOutput).map(ByteUtils::getBytes).map(String::new).as(StepVerifier::create) //
				.expectNext(new String(ByteUtils.getBytes(VALUE_1_BBUFFER)), new String(ByteUtils.getBytes(VALUE_2_BBUFFER))) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void getShouldRetrieveValuesWithNullCorrectly() {

		nativeCommands.set(KEY_1, VALUE_1);
		nativeCommands.set(KEY_3, VALUE_3);

		Stream<KeyCommand> stream = Stream.of(new KeyCommand(KEY_1_BBUFFER), new KeyCommand(KEY_2_BBUFFER),
				new KeyCommand(KEY_3_BBUFFER));
		Flux<ByteBufferResponse<KeyCommand>> result = connection.stringCommands().get(Flux.fromStream(stream));

		result.map(CommandResponse::isPresent).as(StepVerifier::create) //
				.expectNext(true, false, true) //
				.verifyComplete();

	}

	@ParameterizedRedisTest // DATAREDIS-525
	void mGetShouldRetrieveValueCorrectly() {

		nativeCommands.set(KEY_1, VALUE_1);
		nativeCommands.set(KEY_2, VALUE_2);

		connection.stringCommands().mGet(Arrays.asList(KEY_1_BBUFFER, KEY_2_BBUFFER)).as(StepVerifier::create) //
				.consumeNextWith(byteBuffers -> assertThat(byteBuffers).containsExactly(VALUE_1_BBUFFER, VALUE_2_BBUFFER))//
				.verifyComplete();

	}

	@ParameterizedRedisTest // DATAREDIS-525
	void mGetShouldRetrieveNullValueCorrectly() {

		nativeCommands.set(KEY_1, VALUE_1);
		nativeCommands.set(KEY_3, VALUE_3);

		Mono<List<ByteBuffer>> result = connection.stringCommands()
				.mGet(Arrays.asList(KEY_1_BBUFFER, KEY_2_BBUFFER, KEY_3_BBUFFER));

		assertThat(result.block()).containsExactly(VALUE_1_BBUFFER, ByteBuffer.allocate(0), VALUE_3_BBUFFER);
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void mGetShouldRetrieveValuesCorrectly() {

		nativeCommands.set(KEY_1, VALUE_1);
		nativeCommands.set(KEY_2, VALUE_2);

		List<List<ByteBuffer>> lists = Arrays.asList(Arrays.asList(KEY_1_BBUFFER, KEY_2_BBUFFER),
				Collections.singletonList(KEY_2_BBUFFER));
		Flux<List<ByteBuffer>> result = connection.stringCommands().mGet(Flux.fromIterable(lists))
				.map(MultiValueResponse::getOutput);

		Set<List<ByteBuffer>> expected = new HashSet<>();
		expected.add(Arrays.asList(VALUE_1_BBUFFER, VALUE_2_BBUFFER));
		expected.add(Collections.singletonList(VALUE_2_BBUFFER));

		result.collect(Collectors.toSet()).as(StepVerifier::create) //
				.expectNext(expected) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void setNXshouldOnlySetValueWhenNotPresent() {

		connection.stringCommands().setNX(KEY_1_BBUFFER, VALUE_1_BBUFFER).as(StepVerifier::create) //
				.expectNext(true) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void setNXshouldNotSetValueWhenAlreadyPresent() {

		nativeCommands.setnx(KEY_1, VALUE_1);

		connection.stringCommands().setNX(KEY_1_BBUFFER, VALUE_2_BBUFFER).as(StepVerifier::create) //
				.expectNext(false) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void setEXshouldSetKeyAndExpirationTime() {

		connection.stringCommands().setEX(KEY_1_BBUFFER, VALUE_1_BBUFFER, Expiration.seconds(3)).as(StepVerifier::create) //
				.expectNext(true) //
				.verifyComplete();

		assertThat(nativeCommands.ttl(KEY_1) > 1).isTrue();
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void pSetEXshouldSetKeyAndExpirationTime() {

		connection.stringCommands().pSetEX(KEY_1_BBUFFER, VALUE_1_BBUFFER, Expiration.milliseconds(600))
				.as(StepVerifier::create) //
				.expectNext(true) //
				.verifyComplete();

		assertThat(nativeCommands.pttl(KEY_1) > 1).isTrue();
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void mSetShouldAddMultipleKeyValuePairs() {

		Map<ByteBuffer, ByteBuffer> map = new LinkedHashMap<>();
		map.put(KEY_1_BBUFFER, VALUE_1_BBUFFER);
		map.put(KEY_2_BBUFFER, VALUE_2_BBUFFER);

		connection.stringCommands().mSet(map).as(StepVerifier::create).expectNext(true).verifyComplete();

		assertThat(nativeCommands.get(KEY_1)).isEqualTo(VALUE_1);
		assertThat(nativeCommands.get(KEY_2)).isEqualTo(VALUE_2);
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void mSetNXShouldAddMultipleKeyValuePairs() {

		assumeTrue(connectionProvider instanceof StandaloneConnectionProvider);

		Map<ByteBuffer, ByteBuffer> map = new LinkedHashMap<>();
		map.put(KEY_1_BBUFFER, VALUE_1_BBUFFER);
		map.put(KEY_2_BBUFFER, VALUE_2_BBUFFER);

		connection.stringCommands().mSetNX(map).as(StepVerifier::create).expectNext(true).verifyComplete();

		assertThat(nativeCommands.get(KEY_1)).isEqualTo(VALUE_1);
		assertThat(nativeCommands.get(KEY_2)).isEqualTo(VALUE_2);
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void mSetNXShouldNotAddMultipleKeyValuePairsWhenAlreadyExit() {

		assumeTrue(connectionProvider instanceof StandaloneConnectionProvider);

		nativeCommands.set(KEY_2, VALUE_2);

		Map<ByteBuffer, ByteBuffer> map = new LinkedHashMap<>();
		map.put(KEY_1_BBUFFER, VALUE_1_BBUFFER);
		map.put(KEY_2_BBUFFER, VALUE_2_BBUFFER);

		connection.stringCommands().mSetNX(map).as(StepVerifier::create).expectNext(false).verifyComplete();

		assertThat(nativeCommands.exists(KEY_1)).isEqualTo(0L);
		assertThat(nativeCommands.get(KEY_2)).isEqualTo(VALUE_2);
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void appendShouldDoItsThing() {

		connection.stringCommands().append(KEY_1_BBUFFER, VALUE_1_BBUFFER).as(StepVerifier::create) //
				.expectNext(7L) //
				.verifyComplete();

		connection.stringCommands().append(KEY_1_BBUFFER, VALUE_2_BBUFFER).as(StepVerifier::create) //
				.expectNext(14L) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void getRangeShouldReturnSubstringCorrectly() {

		nativeCommands.set(KEY_1, VALUE_1);

		connection.stringCommands().getRange(KEY_1_BBUFFER, 2, 3).as(StepVerifier::create) //
				.expectNext(ByteBuffer.wrap("lu".getBytes())) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void getRangeShouldReturnSubstringCorrectlyWithMinUnbound() {

		nativeCommands.set(KEY_1, VALUE_1);

		RangeCommand rangeCommand = RangeCommand.key(KEY_1_BBUFFER)
				.within(Range.of(Bound.unbounded(), Bound.inclusive(2L)));

		connection.stringCommands().getRange(Mono.just(rangeCommand)).as(StepVerifier::create) //
				.expectNext(new ReactiveRedisConnection.ByteBufferResponse<>(rangeCommand, ByteBuffer.wrap("val".getBytes())))
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void getRangeShouldReturnSubstringCorrectlyWithMaxUnbound() {

		nativeCommands.set(KEY_1, VALUE_1);

		RangeCommand rangeCommand = RangeCommand.key(KEY_1_BBUFFER)
				.within(Range.of(Bound.inclusive(0L), Bound.unbounded()));

		connection.stringCommands().getRange(Mono.just(rangeCommand)).as(StepVerifier::create) //
				.expectNext(new ReactiveRedisConnection.ByteBufferResponse<>(rangeCommand, ByteBuffer.wrap(VALUE_1.getBytes())))
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void setRangeShouldReturnNewStringLengthCorrectly() {

		nativeCommands.set(KEY_1, VALUE_1);

		connection.stringCommands().setRange(KEY_1_BBUFFER, VALUE_2_BBUFFER, 3).as(StepVerifier::create) //
				.expectNext(10L) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void getBitShouldReturnValueCorrectly() {

		nativeCommands.set(KEY_1, VALUE_1);

		connection.stringCommands().getBit(KEY_1_BBUFFER, 1).as(StepVerifier::create) //
				.expectNext(true) //
				.verifyComplete();

		connection.stringCommands().getBit(KEY_1_BBUFFER, 7).as(StepVerifier::create) //
				.expectNext(false) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void setBitShouldReturnValueCorrectly() {

		nativeCommands.set(KEY_1, VALUE_1);

		connection.stringCommands().setBit(KEY_1_BBUFFER, 1, false).as(StepVerifier::create) //
				.expectNext(true) //
				.verifyComplete();

		assertThat(nativeCommands.getbit(KEY_1, 1)).isEqualTo(0L);
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void bitCountShouldReturnValueCorrectly() {

		nativeCommands.set(KEY_1, VALUE_1);

		connection.stringCommands().bitCount(KEY_1_BBUFFER).as(StepVerifier::create) //
				.expectNext(28L) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void bitCountShouldCountInRangeCorrectly() {

		nativeCommands.set(KEY_1, VALUE_1);

		connection.stringCommands().bitCount(KEY_1_BBUFFER, 2, 4).as(StepVerifier::create) //
				.expectNext(13L) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-562
	void bitFieldSetShouldWorkCorrectly() {

		connection.stringCommands().bitField(KEY_1_BBUFFER, create().set(INT_8).valueAt(offset(0L)).to(10L))
				.as(StepVerifier::create)
				.expectNext(Collections.singletonList(0L)).verifyComplete();

		connection.stringCommands().bitField(KEY_1_BBUFFER, create().set(INT_8).valueAt(offset(0L)).to(20L))
				.as(StepVerifier::create)
				.expectNext(Collections.singletonList(10L)).verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-562
	void bitFieldGetShouldWorkCorrectly() {

		connection.stringCommands().bitField(KEY_1_BBUFFER, create().get(INT_8).valueAt(offset(0L)))
				.as(StepVerifier::create)
				.expectNext(Collections.singletonList(0L)).verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-562
	void bitFieldIncrByShouldWorkCorrectly() {

		connection.stringCommands().bitField(KEY_1_BBUFFER, create().incr(INT_8).valueAt(offset(100L)).by(1L))
				.as(StepVerifier::create)
				.expectNext(Collections.singletonList(1L)).verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-562
	void bitFieldIncrByWithOverflowShouldWorkCorrectly() {

		connection.stringCommands()
				.bitField(KEY_1_BBUFFER, create().incr(unsigned(2)).valueAt(offset(102L)).overflow(FAIL).by(1L))
				.as(StepVerifier::create)
				.expectNext(Collections.singletonList(1L)).verifyComplete();
		connection.stringCommands()
				.bitField(KEY_1_BBUFFER, create().incr(unsigned(2)).valueAt(offset(102L)).overflow(FAIL).by(1L))
				.as(StepVerifier::create)
				.expectNext(Collections.singletonList(2L)).verifyComplete();
		connection.stringCommands()
				.bitField(KEY_1_BBUFFER, create().incr(unsigned(2)).valueAt(offset(102L)).overflow(FAIL).by(1L))
				.as(StepVerifier::create)
				.expectNext(Collections.singletonList(3L)).verifyComplete();
		connection.stringCommands()
				.bitField(KEY_1_BBUFFER, create().incr(unsigned(2)).valueAt(offset(102L)).overflow(FAIL).by(1L))
				.as(StepVerifier::create)
				.expectNext(Collections.singletonList(null)).verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-562
	void bitfieldShouldAllowMultipleSubcommands() {

		connection.stringCommands()
				.bitField(KEY_1_BBUFFER, create().incr(signed(5)).valueAt(offset(100L)).by(1L).get(unsigned(4)).valueAt(0L))
				.as(StepVerifier::create)
				.expectNext(Arrays.asList(1L, 0L)).verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void bitOpAndShouldWorkAsExpected() {

		assumeTrue(connectionProvider instanceof StandaloneConnectionProvider);

		nativeCommands.set(KEY_1, VALUE_1);
		nativeCommands.set(KEY_2, VALUE_2);

		connection.stringCommands().bitOp(Arrays.asList(KEY_1_BBUFFER, KEY_2_BBUFFER), BitOperation.AND, KEY_3_BBUFFER)
				.as(StepVerifier::create) //
				.expectNext(7L) //
				.verifyComplete();

		assertThat(nativeCommands.get(KEY_3)).isEqualTo("value-0");
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void bitOpOrShouldWorkAsExpected() {

		assumeTrue(connectionProvider instanceof StandaloneConnectionProvider);

		nativeCommands.set(KEY_1, VALUE_1);
		nativeCommands.set(KEY_2, VALUE_2);

		connection.stringCommands().bitOp(Arrays.asList(KEY_1_BBUFFER, KEY_2_BBUFFER), BitOperation.OR, KEY_3_BBUFFER)
				.as(StepVerifier::create) //
				.expectNext(7L) //
				.verifyComplete();

		assertThat(nativeCommands.get(KEY_3)).isEqualTo(VALUE_3);
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void bitNotShouldThrowExceptionWhenMoreThanOnSourceKey() {

		assumeTrue(connectionProvider instanceof StandaloneConnectionProvider);

		connection.stringCommands().bitOp(Arrays.asList(KEY_1_BBUFFER, KEY_2_BBUFFER), BitOperation.NOT, KEY_3_BBUFFER)
				.as(StepVerifier::create) //
				.expectError(IllegalArgumentException.class) //
				.verify();
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void strLenShouldReturnValueCorrectly() {

		nativeCommands.set(KEY_1, VALUE_1);

		connection.stringCommands().strLen(KEY_1_BBUFFER).as(StepVerifier::create) //
				.expectNext(7L) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-697
	void bitPosShouldReturnPositionCorrectly() {

		nativeBinaryCommands.set(KEY_1_BBUFFER, ByteBuffer.wrap(HexStringUtils.hexToBytes("fff000")));

		connection.stringCommands().bitPos(KEY_1_BBUFFER, false).as(StepVerifier::create).expectNext(12L).verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-697
	void bitPosShouldReturnPositionInRangeCorrectly() {

		nativeBinaryCommands.set(KEY_1_BBUFFER, ByteBuffer.wrap(HexStringUtils.hexToBytes("fff0f0")));

		connection.stringCommands().bitPos(KEY_1_BBUFFER, true, Range.of(Bound.inclusive(2L), Bound.unbounded()))
				.as(StepVerifier::create)
				.expectNext(16L).verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-1103
	void setKeepTTL() {

		long expireSeconds = 10;
		nativeCommands.setex(KEY_1, expireSeconds, VALUE_1);

		connection.stringCommands().set(KEY_1_BBUFFER, VALUE_2_BBUFFER, Expiration.keepTtl(), SetOption.upsert())
				.as(StepVerifier::create) //
				.expectNext(true) //
				.verifyComplete();

		assertThat(nativeBinaryCommands.ttl(KEY_1_BBUFFER)).isCloseTo(expireSeconds, Offset.offset(5L));
		assertThat(nativeCommands.get(KEY_1)).isEqualTo(VALUE_2);
	}
}
