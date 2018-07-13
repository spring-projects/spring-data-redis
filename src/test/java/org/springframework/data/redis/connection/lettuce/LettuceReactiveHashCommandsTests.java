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

import reactor.test.StepVerifier;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.Test;
import org.springframework.data.redis.core.ScanOptions;

/**
 * Integration tests for {@link LettuceReactiveHashCommands}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public class LettuceReactiveHashCommandsTests extends LettuceReactiveCommandsTestsBase {

	static final String FIELD_1 = "field-1";
	static final String FIELD_2 = "field-2";
	static final String FIELD_3 = "field-3";

	static final byte[] FIELD_1_BYTES = FIELD_1.getBytes(StandardCharsets.UTF_8);
	static final byte[] FIELD_2_BYTES = FIELD_2.getBytes(StandardCharsets.UTF_8);
	static final byte[] FIELD_3_BYTES = FIELD_3.getBytes(StandardCharsets.UTF_8);

	static final ByteBuffer FIELD_1_BBUFFER = ByteBuffer.wrap(FIELD_1_BYTES);
	static final ByteBuffer FIELD_2_BBUFFER = ByteBuffer.wrap(FIELD_2_BYTES);
	static final ByteBuffer FIELD_3_BBUFFER = ByteBuffer.wrap(FIELD_3_BYTES);

	@Test // DATAREDIS-525
	public void hSetShouldOperateCorrectly() {

		StepVerifier.create(connection.hashCommands().hSet(KEY_1_BBUFFER, FIELD_1_BBUFFER, VALUE_1_BBUFFER))
				.expectNext(true).verifyComplete();
	}

	@Test // DATAREDIS-525
	public void hSetNxShouldOperateCorrectly() {
		StepVerifier.create(connection.hashCommands().hSetNX(KEY_1_BBUFFER, FIELD_1_BBUFFER, VALUE_1_BBUFFER))
				.expectNext(true).verifyComplete();
	}

	@Test // DATAREDIS-525
	public void hSetNxShouldReturnFalseIfFieldAlreadyExists() {

		nativeCommands.hset(KEY_1, FIELD_1, VALUE_1);

		StepVerifier.create(connection.hashCommands().hSetNX(KEY_1_BBUFFER, FIELD_1_BBUFFER, VALUE_1_BBUFFER))
				.expectNext(false).verifyComplete();
	}

	@Test // DATAREDIS-525
	public void hGetShouldReturnValueForExistingField() {

		nativeCommands.hset(KEY_1, FIELD_1, VALUE_1);
		nativeCommands.hset(KEY_1, FIELD_2, VALUE_2);
		nativeCommands.hset(KEY_1, FIELD_3, VALUE_3);

		StepVerifier.create(connection.hashCommands().hGet(KEY_1_BBUFFER, FIELD_1_BBUFFER)).expectNext(VALUE_1_BBUFFER)
				.verifyComplete();
	}

	@Test // DATAREDIS-525
	public void hGetShouldReturnNullForNotExistingField() {

		nativeCommands.hset(KEY_1, FIELD_1, VALUE_1);

		StepVerifier.create(connection.hashCommands().hGet(KEY_1_BBUFFER, FIELD_2_BBUFFER)).verifyComplete();
	}

	@Test // DATAREDIS-525
	public void hMGetShouldReturnValueForFields() {

		nativeCommands.hset(KEY_1, FIELD_1, VALUE_1);
		nativeCommands.hset(KEY_1, FIELD_2, VALUE_2);
		nativeCommands.hset(KEY_1, FIELD_3, VALUE_3);

		StepVerifier.create(connection.hashCommands().hMGet(KEY_1_BBUFFER, Arrays.asList(FIELD_1_BBUFFER, FIELD_3_BBUFFER)))
				.consumeNextWith(actual -> {

					assertThat(actual, hasItems(VALUE_1_BBUFFER, VALUE_3_BBUFFER));

				}).verifyComplete();
	}

	@Test // DATAREDIS-525
	public void hMGetShouldReturnNullValueForFieldsThatHaveNoValue() {

		nativeCommands.hset(KEY_1, FIELD_1, VALUE_1);
		nativeCommands.hset(KEY_1, FIELD_3, VALUE_3);

		StepVerifier
				.create(connection.hashCommands().hMGet(KEY_1_BBUFFER,
						Arrays.asList(FIELD_1_BBUFFER, FIELD_2_BBUFFER, FIELD_3_BBUFFER)))
				.expectNext(Arrays.asList(VALUE_1_BBUFFER, null, VALUE_3_BBUFFER)).verifyComplete();
	}

	@Test // DATAREDIS-525
	public void hMSetSouldSetValuesCorrectly() {

		Map<ByteBuffer, ByteBuffer> fieldValues = new LinkedHashMap<>();
		fieldValues.put(FIELD_1_BBUFFER, VALUE_1_BBUFFER);
		fieldValues.put(FIELD_2_BBUFFER, VALUE_2_BBUFFER);

		StepVerifier.create(connection.hashCommands().hMSet(KEY_1_BBUFFER, fieldValues)).expectNext(true).verifyComplete();
		assertThat(nativeCommands.hget(KEY_1, FIELD_1), is(equalTo(VALUE_1)));
		assertThat(nativeCommands.hget(KEY_1, FIELD_2), is(equalTo(VALUE_2)));
	}

	@Test // DATAREDIS-791
	public void hMSetShouldOverwriteValuesCorrectly() {

		Map<ByteBuffer, ByteBuffer> fieldValues = new LinkedHashMap<>();
		fieldValues.put(FIELD_1_BBUFFER, VALUE_1_BBUFFER);

		StepVerifier.create(connection.hashCommands().hMSet(KEY_1_BBUFFER, fieldValues)).expectNext(true).verifyComplete();

		Map<ByteBuffer, ByteBuffer> overwriteFieldValues = new LinkedHashMap<>();
		overwriteFieldValues.put(FIELD_1_BBUFFER, VALUE_2_BBUFFER);

		StepVerifier.create(connection.hashCommands().hMSet(KEY_1_BBUFFER, overwriteFieldValues)).expectNext(true)
				.verifyComplete();
		assertThat(nativeCommands.hget(KEY_1, FIELD_1), is(equalTo(VALUE_2)));
	}

	@Test // DATAREDIS-525
	public void hExistsShouldReturnTrueForExistingField() {

		nativeCommands.hset(KEY_1, FIELD_1, VALUE_1);

		StepVerifier.create(connection.hashCommands().hExists(KEY_1_BBUFFER, FIELD_1_BBUFFER)).expectNext(true)
				.verifyComplete();
	}

	@Test // DATAREDIS-525
	public void hExistsShouldReturnFalseForNonExistingField() {
		StepVerifier.create(connection.hashCommands().hExists(KEY_1_BBUFFER, FIELD_1_BBUFFER)).expectNext(false)
				.verifyComplete();
	}

	@Test // DATAREDIS-525
	public void hDelShouldRemoveSingleFieldsCorrectly() {

		nativeCommands.hset(KEY_1, FIELD_1, VALUE_1);
		nativeCommands.hset(KEY_1, FIELD_2, VALUE_2);
		nativeCommands.hset(KEY_1, FIELD_3, VALUE_3);

		StepVerifier.create(connection.hashCommands().hDel(KEY_1_BBUFFER, FIELD_2_BBUFFER)).expectNext(true)
				.verifyComplete();
	}

	@Test // DATAREDIS-525
	public void hDelShouldRemoveMultipleFieldsCorrectly() {

		nativeCommands.hset(KEY_1, FIELD_1, VALUE_1);
		nativeCommands.hset(KEY_1, FIELD_2, VALUE_2);
		nativeCommands.hset(KEY_1, FIELD_3, VALUE_3);

		StepVerifier.create(connection.hashCommands().hDel(KEY_1_BBUFFER, Arrays.asList(FIELD_1_BBUFFER, FIELD_3_BBUFFER)))
				.expectNext(2L).verifyComplete();
	}

	@Test // DATAREDIS-525
	public void hLenShouldReturnSizeCorrectly() {

		nativeCommands.hset(KEY_1, FIELD_1, VALUE_1);
		nativeCommands.hset(KEY_1, FIELD_2, VALUE_2);
		nativeCommands.hset(KEY_1, FIELD_3, VALUE_3);

		StepVerifier.create(connection.hashCommands().hLen(KEY_1_BBUFFER)).expectNext(3L).verifyComplete();
	}

	@Test // DATAREDIS-525
	public void hKeysShouldReturnFieldsCorrectly() {

		nativeCommands.hset(KEY_1, FIELD_1, VALUE_1);
		nativeCommands.hset(KEY_1, FIELD_2, VALUE_2);
		nativeCommands.hset(KEY_1, FIELD_3, VALUE_3);

		StepVerifier.create(connection.hashCommands().hKeys(KEY_1_BBUFFER)) //
				.expectNext(FIELD_1_BBUFFER, FIELD_2_BBUFFER, FIELD_3_BBUFFER) //
				.verifyComplete();
	}

	@Test // DATAREDIS-525
	public void hValsShouldReturnValuesCorrectly() {

		nativeCommands.hset(KEY_1, FIELD_1, VALUE_1);
		nativeCommands.hset(KEY_1, FIELD_2, VALUE_2);
		nativeCommands.hset(KEY_1, FIELD_3, VALUE_3);

		StepVerifier.create(connection.hashCommands().hVals(KEY_1_BBUFFER))
				.expectNext(VALUE_1_BBUFFER, VALUE_2_BBUFFER, VALUE_3_BBUFFER) //
				.verifyComplete();
	}

	@Test // DATAREDIS-525
	public void hGetAllShouldReturnEntriesCorrectly() {

		nativeCommands.hset(KEY_1, FIELD_1, VALUE_1);
		nativeCommands.hset(KEY_1, FIELD_2, VALUE_2);
		nativeCommands.hset(KEY_1, FIELD_3, VALUE_3);

		Map<ByteBuffer, ByteBuffer> expected = new HashMap<>();
		expected.put(FIELD_1_BBUFFER, VALUE_1_BBUFFER);
		expected.put(FIELD_2_BBUFFER, VALUE_2_BBUFFER);
		expected.put(FIELD_3_BBUFFER, VALUE_3_BBUFFER);

		StepVerifier.create(connection.hashCommands().hGetAll(KEY_1_BBUFFER).buffer(3)) //
				.consumeNextWith(list -> {
					assertTrue(list.containsAll(expected.entrySet()));
				}) //
				.verifyComplete();
	}

	@Test // DATAREDIS-743
	public void hScanShouldIterateOverHash() {

		nativeCommands.hset(KEY_1, FIELD_1, VALUE_1);
		nativeCommands.hset(KEY_1, FIELD_2, VALUE_2);
		nativeCommands.hset(KEY_1, FIELD_3, VALUE_3);

		StepVerifier.create(connection.hashCommands().hScan(KEY_1_BBUFFER, ScanOptions.scanOptions().count(1).build())) //
				.expectNextCount(3) //
				.verifyComplete();
	}

	@Test // DATAREDIS-698
	public void hStrLenReturnsFieldLength() {

		nativeCommands.hset(KEY_1, FIELD_1, VALUE_1);
		nativeCommands.hset(KEY_1, FIELD_2, VALUE_2);

		StepVerifier.create(connection.hashCommands().hStrLen(KEY_1_BBUFFER, FIELD_1_BBUFFER))
				.expectNext(Long.valueOf(VALUE_1.length())) //
				.verifyComplete();
	}

	@Test // DATAREDIS-698
	public void hStrLenReturnsZeroWhenFieldDoesNotExist() {

		nativeCommands.hset(KEY_1, FIELD_2, VALUE_3);

		StepVerifier.create(connection.hashCommands().hStrLen(KEY_1_BBUFFER, FIELD_1_BBUFFER)).expectNext(0L) //
				.verifyComplete();
	}

	@Test // DATAREDIS-698
	public void hStrLenReturnsZeroWhenKeyDoesNotExist() {

		StepVerifier.create(connection.hashCommands().hStrLen(KEY_1_BBUFFER, FIELD_1_BBUFFER)).expectNext(0L) //
				.verifyComplete();
	}
}
