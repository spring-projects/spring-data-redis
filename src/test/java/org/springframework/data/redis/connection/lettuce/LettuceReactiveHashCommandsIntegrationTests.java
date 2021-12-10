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

import reactor.test.StepVerifier;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.test.extension.parametrized.ParameterizedRedisTest;

/**
 * Integration tests for {@link LettuceReactiveHashCommands}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public class LettuceReactiveHashCommandsIntegrationTests extends LettuceReactiveCommandsTestSupport {

	private static final String FIELD_1 = "field-1";
	private static final String FIELD_2 = "field-2";
	private static final String FIELD_3 = "field-3";

	private static final byte[] FIELD_1_BYTES = FIELD_1.getBytes(StandardCharsets.UTF_8);
	private static final byte[] FIELD_2_BYTES = FIELD_2.getBytes(StandardCharsets.UTF_8);
	private static final byte[] FIELD_3_BYTES = FIELD_3.getBytes(StandardCharsets.UTF_8);

	private static final ByteBuffer FIELD_1_BBUFFER = ByteBuffer.wrap(FIELD_1_BYTES);
	private static final ByteBuffer FIELD_2_BBUFFER = ByteBuffer.wrap(FIELD_2_BYTES);
	private static final ByteBuffer FIELD_3_BBUFFER = ByteBuffer.wrap(FIELD_3_BYTES);

	public LettuceReactiveHashCommandsIntegrationTests(Fixture fixture) {
		super(fixture);
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void hSetShouldOperateCorrectly() {

		connection.hashCommands().hSet(KEY_1_BBUFFER, FIELD_1_BBUFFER, VALUE_1_BBUFFER).as(StepVerifier::create)
				.expectNext(true).verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void hSetNxShouldOperateCorrectly() {
		connection.hashCommands().hSetNX(KEY_1_BBUFFER, FIELD_1_BBUFFER, VALUE_1_BBUFFER).as(StepVerifier::create)
				.expectNext(true).verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void hSetNxShouldReturnFalseIfFieldAlreadyExists() {

		nativeCommands.hset(KEY_1, FIELD_1, VALUE_1);

		connection.hashCommands().hSetNX(KEY_1_BBUFFER, FIELD_1_BBUFFER, VALUE_1_BBUFFER).as(StepVerifier::create)
				.expectNext(false).verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void hGetShouldReturnValueForExistingField() {

		nativeCommands.hset(KEY_1, FIELD_1, VALUE_1);
		nativeCommands.hset(KEY_1, FIELD_2, VALUE_2);
		nativeCommands.hset(KEY_1, FIELD_3, VALUE_3);

		connection.hashCommands().hGet(KEY_1_BBUFFER, FIELD_1_BBUFFER).as(StepVerifier::create).expectNext(VALUE_1_BBUFFER)
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void hGetShouldReturnNullForNotExistingField() {

		nativeCommands.hset(KEY_1, FIELD_1, VALUE_1);

		connection.hashCommands().hGet(KEY_1_BBUFFER, FIELD_2_BBUFFER).as(StepVerifier::create).verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void hMGetShouldReturnValueForFields() {

		nativeCommands.hset(KEY_1, FIELD_1, VALUE_1);
		nativeCommands.hset(KEY_1, FIELD_2, VALUE_2);
		nativeCommands.hset(KEY_1, FIELD_3, VALUE_3);

		connection.hashCommands().hMGet(KEY_1_BBUFFER, Arrays.asList(FIELD_1_BBUFFER, FIELD_3_BBUFFER))
				.as(StepVerifier::create)
				.consumeNextWith(actual -> {

					assertThat(actual).contains(VALUE_1_BBUFFER, VALUE_3_BBUFFER);

				}).verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-525, GH-2210
	void hMGetShouldReturnNullValueForFieldsThatHaveNoValue() {

		nativeCommands.hset(KEY_1, FIELD_1, VALUE_1);
		nativeCommands.hset(KEY_1, FIELD_3, VALUE_3);

		connection.hashCommands().hMGet(KEY_1_BBUFFER, Collections.singletonList(FIELD_1_BBUFFER)).as(StepVerifier::create)
				.expectNext(Collections.singletonList(VALUE_1_BBUFFER)).verifyComplete();

		connection.hashCommands().hMGet(KEY_1_BBUFFER, Collections.singletonList(FIELD_2_BBUFFER))
				.as(StepVerifier::create)
				.expectNext(Collections.singletonList(null)).verifyComplete();

		connection.hashCommands().hMGet(KEY_1_BBUFFER, Arrays.asList(FIELD_1_BBUFFER, FIELD_2_BBUFFER, FIELD_3_BBUFFER))
				.as(StepVerifier::create)
				.expectNext(Arrays.asList(VALUE_1_BBUFFER, null, VALUE_3_BBUFFER)).verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void hMSetSouldSetValuesCorrectly() {

		Map<ByteBuffer, ByteBuffer> fieldValues = new LinkedHashMap<>();
		fieldValues.put(FIELD_1_BBUFFER, VALUE_1_BBUFFER);
		fieldValues.put(FIELD_2_BBUFFER, VALUE_2_BBUFFER);

		connection.hashCommands().hMSet(KEY_1_BBUFFER, fieldValues).as(StepVerifier::create).expectNext(true)
				.verifyComplete();
		assertThat(nativeCommands.hget(KEY_1, FIELD_1)).isEqualTo(VALUE_1);
		assertThat(nativeCommands.hget(KEY_1, FIELD_2)).isEqualTo(VALUE_2);
	}

	@ParameterizedRedisTest // DATAREDIS-791
	void hMSetShouldOverwriteValuesCorrectly() {

		Map<ByteBuffer, ByteBuffer> fieldValues = new LinkedHashMap<>();
		fieldValues.put(FIELD_1_BBUFFER, VALUE_1_BBUFFER);

		connection.hashCommands().hMSet(KEY_1_BBUFFER, fieldValues).as(StepVerifier::create).expectNext(true)
				.verifyComplete();

		Map<ByteBuffer, ByteBuffer> overwriteFieldValues = new LinkedHashMap<>();
		overwriteFieldValues.put(FIELD_1_BBUFFER, VALUE_2_BBUFFER);

		connection.hashCommands().hMSet(KEY_1_BBUFFER, overwriteFieldValues).as(StepVerifier::create).expectNext(true)
				.verifyComplete();
		assertThat(nativeCommands.hget(KEY_1, FIELD_1)).isEqualTo(VALUE_2);
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void hExistsShouldReturnTrueForExistingField() {

		nativeCommands.hset(KEY_1, FIELD_1, VALUE_1);

		connection.hashCommands().hExists(KEY_1_BBUFFER, FIELD_1_BBUFFER).as(StepVerifier::create).expectNext(true)
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void hExistsShouldReturnFalseForNonExistingField() {
		connection.hashCommands().hExists(KEY_1_BBUFFER, FIELD_1_BBUFFER).as(StepVerifier::create).expectNext(false)
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void hDelShouldRemoveSingleFieldsCorrectly() {

		nativeCommands.hset(KEY_1, FIELD_1, VALUE_1);
		nativeCommands.hset(KEY_1, FIELD_2, VALUE_2);
		nativeCommands.hset(KEY_1, FIELD_3, VALUE_3);

		connection.hashCommands().hDel(KEY_1_BBUFFER, FIELD_2_BBUFFER).as(StepVerifier::create).expectNext(true)
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void hDelShouldRemoveMultipleFieldsCorrectly() {

		nativeCommands.hset(KEY_1, FIELD_1, VALUE_1);
		nativeCommands.hset(KEY_1, FIELD_2, VALUE_2);
		nativeCommands.hset(KEY_1, FIELD_3, VALUE_3);

		connection.hashCommands().hDel(KEY_1_BBUFFER, Arrays.asList(FIELD_1_BBUFFER, FIELD_3_BBUFFER))
				.as(StepVerifier::create)
				.expectNext(2L).verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void hLenShouldReturnSizeCorrectly() {

		nativeCommands.hset(KEY_1, FIELD_1, VALUE_1);
		nativeCommands.hset(KEY_1, FIELD_2, VALUE_2);
		nativeCommands.hset(KEY_1, FIELD_3, VALUE_3);

		connection.hashCommands().hLen(KEY_1_BBUFFER).as(StepVerifier::create).expectNext(3L).verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void hKeysShouldReturnFieldsCorrectly() {

		nativeCommands.hset(KEY_1, FIELD_1, VALUE_1);
		nativeCommands.hset(KEY_1, FIELD_2, VALUE_2);
		nativeCommands.hset(KEY_1, FIELD_3, VALUE_3);

		connection.hashCommands().hKeys(KEY_1_BBUFFER).as(StepVerifier::create) //
				.expectNext(FIELD_1_BBUFFER, FIELD_2_BBUFFER, FIELD_3_BBUFFER) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void hValsShouldReturnValuesCorrectly() {

		nativeCommands.hset(KEY_1, FIELD_1, VALUE_1);
		nativeCommands.hset(KEY_1, FIELD_2, VALUE_2);
		nativeCommands.hset(KEY_1, FIELD_3, VALUE_3);

		connection.hashCommands().hVals(KEY_1_BBUFFER).as(StepVerifier::create)
				.expectNext(VALUE_1_BBUFFER, VALUE_2_BBUFFER, VALUE_3_BBUFFER) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void hGetAllShouldReturnEntriesCorrectly() {

		nativeCommands.hset(KEY_1, FIELD_1, VALUE_1);
		nativeCommands.hset(KEY_1, FIELD_2, VALUE_2);
		nativeCommands.hset(KEY_1, FIELD_3, VALUE_3);

		Map<ByteBuffer, ByteBuffer> expected = new HashMap<>();
		expected.put(FIELD_1_BBUFFER, VALUE_1_BBUFFER);
		expected.put(FIELD_2_BBUFFER, VALUE_2_BBUFFER);
		expected.put(FIELD_3_BBUFFER, VALUE_3_BBUFFER);

		connection.hashCommands().hGetAll(KEY_1_BBUFFER).buffer(3).as(StepVerifier::create) //
				.consumeNextWith(list -> {
					assertThat(list.containsAll(expected.entrySet())).isTrue();
				}) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-743
	void hScanShouldIterateOverHash() {

		nativeCommands.hset(KEY_1, FIELD_1, VALUE_1);
		nativeCommands.hset(KEY_1, FIELD_2, VALUE_2);
		nativeCommands.hset(KEY_1, FIELD_3, VALUE_3);

		connection.hashCommands().hScan(KEY_1_BBUFFER, ScanOptions.scanOptions().count(1).build()).as(StepVerifier::create) //
				.expectNextCount(3) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-698
	void hStrLenReturnsFieldLength() {

		nativeCommands.hset(KEY_1, FIELD_1, VALUE_1);
		nativeCommands.hset(KEY_1, FIELD_2, VALUE_2);

		connection.hashCommands().hStrLen(KEY_1_BBUFFER, FIELD_1_BBUFFER).as(StepVerifier::create)
				.expectNext(Long.valueOf(VALUE_1.length())) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-698
	void hStrLenReturnsZeroWhenFieldDoesNotExist() {

		nativeCommands.hset(KEY_1, FIELD_2, VALUE_3);

		connection.hashCommands().hStrLen(KEY_1_BBUFFER, FIELD_1_BBUFFER).as(StepVerifier::create).expectNext(0L) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-698
	void hStrLenReturnsZeroWhenKeyDoesNotExist() {

		connection.hashCommands().hStrLen(KEY_1_BBUFFER, FIELD_1_BBUFFER).as(StepVerifier::create).expectNext(0L) //
				.verifyComplete();
	}
}
