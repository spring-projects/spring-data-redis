/*
 * Copyright 2016 the original author or authors.
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

import static org.hamcrest.collection.IsIterableContainingInAnyOrder.*;
import static org.hamcrest.collection.IsIterableContainingInOrder.*;
import static org.hamcrest.core.Is.*;
import static org.hamcrest.core.IsEqual.*;
import static org.hamcrest.core.IsNull.*;
import static org.junit.Assert.*;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.Test;

/**
 * @author Christoph Strobl
 */
public class LettuceReactiveHashCommandsTests extends LettuceReactiveCommandsTestsBase {

	static final String FIELD_1 = "field-1";
	static final String FIELD_2 = "field-2";
	static final String FIELD_3 = "field-3";

	static final byte[] FIELD_1_BYTES = FIELD_1.getBytes(Charset.forName("UTF-8"));
	static final byte[] FIELD_2_BYTES = FIELD_2.getBytes(Charset.forName("UTF-8"));
	static final byte[] FIELD_3_BYTES = FIELD_3.getBytes(Charset.forName("UTF-8"));

	static final ByteBuffer FIELD_1_BBUFFER = ByteBuffer.wrap(FIELD_1_BYTES);
	static final ByteBuffer FIELD_2_BBUFFER = ByteBuffer.wrap(FIELD_2_BYTES);
	static final ByteBuffer FIELD_3_BBUFFER = ByteBuffer.wrap(FIELD_3_BYTES);

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void hSetShouldOperateCorrectly() {
		assertThat(connection.hashCommands().hSet(KEY_1_BBUFFER, FIELD_1_BBUFFER, VALUE_1_BBUFFER).block(), is(true));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void hSetNxShouldOperateCorrectly() {
		assertThat(connection.hashCommands().hSetNX(KEY_1_BBUFFER, FIELD_1_BBUFFER, VALUE_1_BBUFFER).block(), is(true));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void hSetNxShouldReturnFalseIfFieldAlreadyExists() {

		nativeCommands.hset(KEY_1, FIELD_1, VALUE_1);

		assertThat(connection.hashCommands().hSetNX(KEY_1_BBUFFER, FIELD_1_BBUFFER, VALUE_1_BBUFFER).block(), is(false));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void hGetShouldReturnValueForExistingField() {

		nativeCommands.hset(KEY_1, FIELD_1, VALUE_1);
		nativeCommands.hset(KEY_1, FIELD_2, VALUE_2);
		nativeCommands.hset(KEY_1, FIELD_3, VALUE_3);

		assertThat(connection.hashCommands().hGet(KEY_1_BBUFFER, FIELD_1_BBUFFER).block(), is(equalTo(VALUE_1_BBUFFER)));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void hGetShouldReturnNullForNotExistingField() {

		nativeCommands.hset(KEY_1, FIELD_1, VALUE_1);

		assertThat(connection.hashCommands().hGet(KEY_1_BBUFFER, FIELD_2_BBUFFER).block(), is(nullValue()));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void hMGetShouldReturnValueForFields() {

		nativeCommands.hset(KEY_1, FIELD_1, VALUE_1);
		nativeCommands.hset(KEY_1, FIELD_2, VALUE_2);
		nativeCommands.hset(KEY_1, FIELD_3, VALUE_3);

		assertThat(connection.hashCommands().hMGet(KEY_1_BBUFFER, Arrays.asList(FIELD_1_BBUFFER, FIELD_3_BBUFFER)).block(),
				contains(VALUE_1_BBUFFER, VALUE_3_BBUFFER));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void hMGetShouldReturnNullValueForFieldsThatHaveNoValue() {

		nativeCommands.hset(KEY_1, FIELD_1, VALUE_1);
		nativeCommands.hset(KEY_1, FIELD_3, VALUE_3);

		assertThat(connection.hashCommands()
				.hMGet(KEY_1_BBUFFER, Arrays.asList(FIELD_1_BBUFFER, FIELD_2_BBUFFER, FIELD_3_BBUFFER)).block(),
				contains(VALUE_1_BBUFFER, null, VALUE_3_BBUFFER));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void hMSetSouldSetValuesCorrectly() {

		Map<ByteBuffer, ByteBuffer> fieldValues = new LinkedHashMap<>();
		fieldValues.put(FIELD_1_BBUFFER, VALUE_1_BBUFFER);
		fieldValues.put(FIELD_2_BBUFFER, VALUE_2_BBUFFER);

		assertThat(connection.hashCommands().hMSet(KEY_1_BBUFFER, fieldValues).block(), is(true));
		assertThat(nativeCommands.hget(KEY_1, FIELD_1), is(equalTo(VALUE_1)));
		assertThat(nativeCommands.hget(KEY_1, FIELD_2), is(equalTo(VALUE_2)));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void hExistsShouldReturnTrueForExistingField() {

		nativeCommands.hset(KEY_1, FIELD_1, VALUE_1);

		assertThat(connection.hashCommands().hExists(KEY_1_BBUFFER, FIELD_1_BBUFFER).block(), is(true));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void hExistsShouldReturnFalseForNonExistingField() {
		assertThat(connection.hashCommands().hExists(KEY_1_BBUFFER, FIELD_1_BBUFFER).block(), is(false));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void hDelShouldRemoveSingleFieldsCorrectly() {

		nativeCommands.hset(KEY_1, FIELD_1, VALUE_1);
		nativeCommands.hset(KEY_1, FIELD_2, VALUE_2);
		nativeCommands.hset(KEY_1, FIELD_3, VALUE_3);

		assertThat(connection.hashCommands().hDel(KEY_1_BBUFFER, FIELD_2_BBUFFER).block(), is(true));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void hDelShouldRemoveMultipleFieldsCorrectly() {

		nativeCommands.hset(KEY_1, FIELD_1, VALUE_1);
		nativeCommands.hset(KEY_1, FIELD_2, VALUE_2);
		nativeCommands.hset(KEY_1, FIELD_3, VALUE_3);

		assertThat(connection.hashCommands().hDel(KEY_1_BBUFFER, Arrays.asList(FIELD_1_BBUFFER, FIELD_3_BBUFFER)).block(),
				is(2L));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void hLenShouldReturnSizeCorrectly() {

		nativeCommands.hset(KEY_1, FIELD_1, VALUE_1);
		nativeCommands.hset(KEY_1, FIELD_2, VALUE_2);
		nativeCommands.hset(KEY_1, FIELD_3, VALUE_3);

		assertThat(connection.hashCommands().hLen(KEY_1_BBUFFER).block(), is(3L));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void hKeysShouldReturnFieldsCorrectly() {

		nativeCommands.hset(KEY_1, FIELD_1, VALUE_1);
		nativeCommands.hset(KEY_1, FIELD_2, VALUE_2);
		nativeCommands.hset(KEY_1, FIELD_3, VALUE_3);

		assertThat(connection.hashCommands().hKeys(KEY_1_BBUFFER).block(),
				containsInAnyOrder(FIELD_1_BBUFFER, FIELD_2_BBUFFER, FIELD_3_BBUFFER));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void hValsShouldReturnValuesCorrectly() {

		nativeCommands.hset(KEY_1, FIELD_1, VALUE_1);
		nativeCommands.hset(KEY_1, FIELD_2, VALUE_2);
		nativeCommands.hset(KEY_1, FIELD_3, VALUE_3);

		assertThat(connection.hashCommands().hVals(KEY_1_BBUFFER).block(),
				containsInAnyOrder(VALUE_1_BBUFFER, VALUE_2_BBUFFER, VALUE_3_BBUFFER));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void hGetAlllShouldReturnEntriesCorrectly() {

		nativeCommands.hset(KEY_1, FIELD_1, VALUE_1);
		nativeCommands.hset(KEY_1, FIELD_2, VALUE_2);
		nativeCommands.hset(KEY_1, FIELD_3, VALUE_3);

		System.out.println(connection.hashCommands().hGetAll(KEY_1_BBUFFER).block());
	}

}
