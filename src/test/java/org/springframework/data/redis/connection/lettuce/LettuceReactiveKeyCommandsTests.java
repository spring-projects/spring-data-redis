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

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.collection.IsCollectionWithSize.*;
import static org.junit.Assert.*;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.junit.Test;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.connection.ReactiveRedisConnection.KeyCommand;
import org.springframework.data.redis.connection.ReactiveRedisConnection.NumericResponse;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.TestSubscriber;

/**
 * @author Christoph Strobl
 */
public class LettuceReactiveKeyCommandsTests extends LettuceReactiveCommandsTestsBase {

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void existsShouldReturnTrueForExistingKeys() {

		nativeCommands.set(KEY_1, VALUE_1);

		assertThat(connection.keyCommands().exists(KEY_1_BBUFFER).block(), is(true));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void existsShouldReturnFalseForNonExistingKeys() {
		assertThat(connection.keyCommands().exists(KEY_1_BBUFFER).block(), is(false));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void typeShouldReturnTypeCorrectly() {

		nativeCommands.set(KEY_1, VALUE_2);
		nativeCommands.sadd(KEY_2, VALUE_2);
		nativeCommands.hset(KEY_3, KEY_1, VALUE_1);

		assertThat(connection.keyCommands().type(KEY_1_BBUFFER).block(), is(DataType.STRING));
		assertThat(connection.keyCommands().type(KEY_2_BBUFFER).block(), is(DataType.SET));
		assertThat(connection.keyCommands().type(KEY_3_BBUFFER).block(), is(DataType.HASH));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void keysShouldReturnCorrectly() {

		nativeCommands.set(KEY_1, VALUE_2);
		nativeCommands.set(KEY_2, VALUE_2);
		nativeCommands.set(KEY_3, VALUE_3);

		nativeCommands.set(VALUE_1, KEY_1);
		nativeCommands.set(VALUE_2, KEY_2);
		nativeCommands.set(VALUE_3, KEY_3);

		assertThat(connection.keyCommands().keys(ByteBuffer.wrap("*".getBytes())).block(), hasSize(6));
		assertThat(connection.keyCommands().keys(ByteBuffer.wrap("key*".getBytes())).block(), hasSize(3));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void randomKeyShouldReturnAnyKey() {

		nativeCommands.set(KEY_1, VALUE_2);
		nativeCommands.set(KEY_2, VALUE_2);
		nativeCommands.set(KEY_3, VALUE_3);

		assertThat(connection.keyCommands().randomKey().block(), is(notNullValue()));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void randomKeyShouldReturnNullWhenNoKeyExists() {
		assertThat(connection.keyCommands().randomKey().block(), is(nullValue()));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void renameShouldAlterKeyNameCorrectly() {

		nativeCommands.set(KEY_1, VALUE_2);

		assertThat(connection.keyCommands().rename(KEY_1_BBUFFER, KEY_2_BBUFFER).block(), is(true));
		assertThat(nativeCommands.exists(KEY_2), is(true));
		assertThat(nativeCommands.exists(KEY_1), is(false));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test(expected = RedisSystemException.class)
	public void renameShouldThrowErrorWhenKeyDoesNotExit() {
		assertThat(connection.keyCommands().rename(KEY_1_BBUFFER, KEY_2_BBUFFER).block(), is(true));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void renameNXShouldAlterKeyNameCorrectly() {

		nativeCommands.set(KEY_1, VALUE_2);

		assertThat(connection.keyCommands().rename(KEY_1_BBUFFER, KEY_2_BBUFFER).block(), is(true));

		assertThat(nativeCommands.exists(KEY_2), is(true));
		assertThat(nativeCommands.exists(KEY_1), is(false));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void renameNXShouldNotAlterExistingKeyName() {

		nativeCommands.set(KEY_1, VALUE_2);
		nativeCommands.set(KEY_2, VALUE_2);

		assertThat(connection.keyCommands().renameNX(KEY_1_BBUFFER, KEY_2_BBUFFER).block(), is(false));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void shouldDeleteKeyCorrectly() {

		nativeCommands.set(KEY_1, VALUE_1);

		Mono<Long> result = connection.keyCommands().del(KEY_1_BBUFFER);
		assertThat(result.block(), is(1L));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void shouldDeleteKeysCorrectly() {

		nativeCommands.set(KEY_1, VALUE_1);
		nativeCommands.set(KEY_2, VALUE_2);

		Flux<NumericResponse<KeyCommand, Long>> result = connection.keyCommands()
				.del(Flux.fromIterable(Arrays.asList(new KeyCommand(KEY_1_BBUFFER), new KeyCommand(KEY_2_BBUFFER))));

		TestSubscriber<NumericResponse<KeyCommand, Long>> subscriber = TestSubscriber.create();
		result.subscribe(subscriber);
		subscriber.await();

		subscriber.assertValueCount(2);
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void shouldDeleteKeysInBatchCorrectly() {

		nativeCommands.set(KEY_1, VALUE_1);
		nativeCommands.set(KEY_2, VALUE_2);

		Mono<Long> result = connection.keyCommands().mDel(Arrays.asList(KEY_1_BBUFFER, KEY_2_BBUFFER));

		assertThat(result.block(), is(2L));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void shouldDeleteKeysInMultipleBatchesCorrectly() {

		nativeCommands.set(KEY_1, VALUE_1);
		nativeCommands.set(KEY_2, VALUE_2);

		Flux<Long> result = connection.keyCommands()
				.mDel(
						Flux.fromIterable(Arrays.asList(Arrays.asList(KEY_1_BBUFFER, KEY_2_BBUFFER), Arrays.asList(KEY_1_BBUFFER))))
				.map(NumericResponse::getOutput);

		TestSubscriber<Long> subscriber = TestSubscriber.create();
		result.subscribe(subscriber);
		subscriber.await();

		subscriber.assertValueCount(2);
	}

}
