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

import static org.hamcrest.collection.IsIterableContainingInOrder.*;
import static org.hamcrest.core.Is.*;
import static org.hamcrest.core.IsEqual.*;
import static org.hamcrest.core.IsNull.*;
import static org.junit.Assert.*;
import static org.junit.Assume.assumeThat;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.springframework.data.redis.connection.ReactiveRedisConnection.BooleanResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.ByteBufferResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.KeyCommand;
import org.springframework.data.redis.connection.ReactiveRedisConnection.MultiValueResponse;
import org.springframework.data.redis.connection.ReactiveStringCommands.SetCommand;
import org.springframework.data.redis.connection.RedisStringCommands.BitOperation;
import org.springframework.data.redis.core.types.Expiration;

import org.springframework.data.redis.test.util.LettuceRedisClientProvider;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.TestSubscriber;

/**
 * @author Christoph Strobl
 */
public class LettuceReactiveStringCommandsTests extends LettuceReactiveCommandsTestsBase {

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void getSetShouldReturnPreviousValueCorrectly() {

		nativeCommands.set(KEY_1, VALUE_1);

		Mono<ByteBuffer> result = connection.stringCommands().getSet(KEY_1_BBUFFER, VALUE_2_BBUFFER);

		assertThat(result.block(), is(equalTo(VALUE_1_BBUFFER)));
		assertThat(nativeCommands.get(KEY_1), is(equalTo(VALUE_2)));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void getSetShouldReturnPreviousValueCorrectlyWhenNoExists() {

		Mono<ByteBuffer> result = connection.stringCommands().getSet(KEY_1_BBUFFER, VALUE_2_BBUFFER);

		ByteBuffer value = result.block();
		assertThat(value, is(notNullValue()));
		assertThat(value, is(equalTo(ByteBuffer.allocate(0))));
		assertThat(nativeCommands.get(KEY_1), is(equalTo(VALUE_2)));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void setShouldAddValueCorrectly() {

		Mono<Boolean> result = connection.stringCommands().set(KEY_1_BBUFFER, VALUE_1_BBUFFER);

		assertThat(result.block(), is(true));
		assertThat(nativeCommands.get(KEY_1), is(equalTo(VALUE_1)));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void setShouldAddValuesCorrectly() {

		Flux<BooleanResponse<SetCommand>> result = connection.stringCommands()
				.set(Flux.fromIterable(Arrays.asList(SetCommand.set(KEY_1_BBUFFER).value(VALUE_1_BBUFFER),
						SetCommand.set(KEY_2_BBUFFER).value(VALUE_2_BBUFFER))));

		TestSubscriber<BooleanResponse<SetCommand>> subscriber = TestSubscriber.create();
		result.subscribe(subscriber);
		subscriber.await();

		subscriber.assertValueCount(2);
		assertThat(nativeCommands.get(KEY_1), is(equalTo(VALUE_1)));
		assertThat(nativeCommands.get(KEY_2), is(equalTo(VALUE_2)));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void getShouldRetriveValueCorrectly() {

		nativeCommands.set(KEY_1, VALUE_1);

		Mono<ByteBuffer> result = connection.stringCommands().get(KEY_1_BBUFFER);
		assertThat(result.block(), is(equalTo(VALUE_1_BBUFFER)));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void getShouldRetriveNullValueCorrectly() {

		Mono<ByteBuffer> result = connection.stringCommands().get(KEY_1_BBUFFER);
		assertThat(result.block(), is(equalTo(ByteBuffer.allocate(0))));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void getShouldRetriveValuesCorrectly() {

		nativeCommands.set(KEY_1, VALUE_1);
		nativeCommands.set(KEY_2, VALUE_2);

		Flux<ByteBufferResponse<KeyCommand>> result = connection.stringCommands()
				.get(Flux.fromStream(Arrays.asList(new KeyCommand(KEY_1_BBUFFER), new KeyCommand(KEY_2_BBUFFER)).stream()));

		TestSubscriber<ByteBufferResponse<KeyCommand>> subscriber = TestSubscriber.create();
		result.subscribe(subscriber);
		subscriber.await();

		subscriber.assertValueCount(2);
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void getShouldRetriveValuesWithNullCorrectly() {

		nativeCommands.set(KEY_1, VALUE_1);
		nativeCommands.set(KEY_3, VALUE_3);

		Flux<ByteBufferResponse<KeyCommand>> result = connection.stringCommands().get(Flux.fromStream(Arrays
				.asList(new KeyCommand(KEY_1_BBUFFER), new KeyCommand(KEY_2_BBUFFER), new KeyCommand(KEY_3_BBUFFER)).stream()));

		TestSubscriber<ByteBufferResponse<KeyCommand>> subscriber = TestSubscriber.create();
		result.subscribe(subscriber);
		subscriber.await();

		subscriber.assertValueCount(3);
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void mGetShouldRetriveValueCorrectly() {

		nativeCommands.set(KEY_1, VALUE_1);
		nativeCommands.set(KEY_2, VALUE_2);

		Mono<List<ByteBuffer>> result = connection.stringCommands().mGet(Arrays.asList(KEY_1_BBUFFER, KEY_2_BBUFFER));
		assertThat(result.block(), contains(VALUE_1_BBUFFER, VALUE_2_BBUFFER));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void mGetShouldRetriveNullValueCorrectly() {

		nativeCommands.set(KEY_1, VALUE_1);
		nativeCommands.set(KEY_3, VALUE_3);

		Mono<List<ByteBuffer>> result = connection.stringCommands()
				.mGet(Arrays.asList(KEY_1_BBUFFER, KEY_2_BBUFFER, KEY_3_BBUFFER));

		assertThat(result.block(), contains(VALUE_1_BBUFFER, ByteBuffer.allocate(0), VALUE_3_BBUFFER));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void mGetShouldRetriveValuesCorrectly() {

		nativeCommands.set(KEY_1, VALUE_1);
		nativeCommands.set(KEY_2, VALUE_2);

		Flux<List<ByteBuffer>> result = connection.stringCommands()
				.mGet(
						Flux.fromIterable(Arrays.asList(Arrays.asList(KEY_1_BBUFFER, KEY_2_BBUFFER), Arrays.asList(KEY_2_BBUFFER))))
				.map(MultiValueResponse::getOutput);

		TestSubscriber<List<ByteBuffer>> subscriber = TestSubscriber.create();
		result.subscribe(subscriber);
		subscriber.await();

		subscriber.assertValueCount(2);
		subscriber.assertContainValues(
				new HashSet<>(Arrays.asList(Arrays.asList(VALUE_1_BBUFFER, VALUE_2_BBUFFER), Arrays.asList(VALUE_2_BBUFFER))));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void setNXshouldOnlySetValueWhenNotPresent() {
		assertThat(connection.stringCommands().setNX(KEY_1_BBUFFER, VALUE_1_BBUFFER).block(), is(true));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void setNXshouldNotSetValueWhenAlreadyPresent() {

		nativeCommands.setnx(KEY_1, VALUE_1);

		assertThat(connection.stringCommands().setNX(KEY_1_BBUFFER, VALUE_2_BBUFFER).block(), is(false));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void setEXshouldSetKeyAndExpirationTime() {

		connection.stringCommands().setEX(KEY_1_BBUFFER, VALUE_1_BBUFFER, Expiration.seconds(3)).block();

		assertThat(nativeCommands.ttl(KEY_1) > 1, is(true));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void pSetEXshouldSetKeyAndExpirationTime() {

		connection.stringCommands().pSetEX(KEY_1_BBUFFER, VALUE_1_BBUFFER, Expiration.milliseconds(600)).block();

		assertThat(nativeCommands.pttl(KEY_1) > 1, is(true));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void mSetShouldAddMultipleKeyValueParis() {

		Map<ByteBuffer, ByteBuffer> map = new LinkedHashMap<>();
		map.put(KEY_1_BBUFFER, VALUE_1_BBUFFER);
		map.put(KEY_2_BBUFFER, VALUE_2_BBUFFER);

		connection.stringCommands().mSet(map).block();

		assertThat(nativeCommands.get(KEY_1), is(equalTo(VALUE_1)));
		assertThat(nativeCommands.get(KEY_2), is(equalTo(VALUE_2)));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void mSetNXShouldAddMultipleKeyValueParis() {

		assumeThat(clientProvider instanceof LettuceRedisClientProvider, is(true));

		Map<ByteBuffer, ByteBuffer> map = new LinkedHashMap<>();
		map.put(KEY_1_BBUFFER, VALUE_1_BBUFFER);
		map.put(KEY_2_BBUFFER, VALUE_2_BBUFFER);

		connection.stringCommands().mSetNX(map).block();

		assertThat(nativeCommands.get(KEY_1), is(equalTo(VALUE_1)));
		assertThat(nativeCommands.get(KEY_2), is(equalTo(VALUE_2)));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void mSetNXShouldNotAddMultipleKeyValueParisWhenAlreadyExit() {

		assumeThat(clientProvider instanceof LettuceRedisClientProvider, is(true));

		nativeCommands.set(KEY_2, VALUE_2);

		Map<ByteBuffer, ByteBuffer> map = new LinkedHashMap<>();
		map.put(KEY_1_BBUFFER, VALUE_1_BBUFFER);
		map.put(KEY_2_BBUFFER, VALUE_2_BBUFFER);

		assertThat(connection.stringCommands().mSetNX(map).block(), is(false));

		assertThat(nativeCommands.exists(KEY_1), is(false));
		assertThat(nativeCommands.get(KEY_2), is(equalTo(VALUE_2)));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void appendShouldDoItsThing() {

		assertThat(connection.stringCommands().append(KEY_1_BBUFFER, VALUE_1_BBUFFER).block(), is(7L));
		assertThat(connection.stringCommands().append(KEY_1_BBUFFER, VALUE_2_BBUFFER).block(), is(14L));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void getRangeShouldReturnSubstringCorrectly() {

		nativeCommands.set(KEY_1, VALUE_1);

		assertThat(connection.stringCommands().getRange(KEY_1_BBUFFER, 2, 3).block(),
				is(equalTo(ByteBuffer.wrap("lu".getBytes()))));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void setRangeShouldReturnNewStringLengthCorrectly() {

		nativeCommands.set(KEY_1, VALUE_1);

		assertThat(connection.stringCommands().setRange(KEY_1_BBUFFER, VALUE_2_BBUFFER, 3).block(), is(10L));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void getBitShouldReturnValueCorrectly() {

		nativeCommands.set(KEY_1, VALUE_1);

		assertThat(connection.stringCommands().getBit(KEY_1_BBUFFER, 1).block(), is(true));
		assertThat(connection.stringCommands().getBit(KEY_1_BBUFFER, 7).block(), is(false));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void setBitShouldReturnValueCorrectly() {

		nativeCommands.set(KEY_1, VALUE_1);

		assertThat(connection.stringCommands().setBit(KEY_1_BBUFFER, 1, false).block(), is(true));
		assertThat(nativeCommands.getbit(KEY_1, 1), is(0L));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void bitCountShouldReturnValueCorrectly() {

		nativeCommands.set(KEY_1, VALUE_1);

		assertThat(connection.stringCommands().bitCount(KEY_1_BBUFFER).block(), is(28L));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void bitCountShouldCountInRangeCorrectly() {

		nativeCommands.set(KEY_1, VALUE_1);

		assertThat(connection.stringCommands().bitCount(KEY_1_BBUFFER, 2, 4).block(), is(13L));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void bitOpAndShouldWorkAsExpected() {

		assumeThat(clientProvider instanceof LettuceRedisClientProvider, is(true));

		nativeCommands.set(KEY_1, VALUE_1);
		nativeCommands.set(KEY_2, VALUE_2);

		assertThat(connection.stringCommands()
				.bitOp(Arrays.asList(KEY_1_BBUFFER, KEY_2_BBUFFER), BitOperation.AND, KEY_3_BBUFFER).block(), is(7L));
		assertThat(nativeCommands.get(KEY_3), is(equalTo("value-0")));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void bitOpOrShouldWorkAsExpected() {

		assumeThat(clientProvider instanceof LettuceRedisClientProvider, is(true));

		nativeCommands.set(KEY_1, VALUE_1);
		nativeCommands.set(KEY_2, VALUE_2);

		assertThat(connection.stringCommands()
				.bitOp(Arrays.asList(KEY_1_BBUFFER, KEY_2_BBUFFER), BitOperation.OR, KEY_3_BBUFFER).block(), is(7L));
		assertThat(nativeCommands.get(KEY_3), is(equalTo(VALUE_3)));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test(expected = IllegalArgumentException.class)
	public void bitNotShouldThrowExceptionWhenMoreThanOnSourceKey() {

		assumeThat(clientProvider instanceof LettuceRedisClientProvider, is(true));

		connection.stringCommands().bitOp(Arrays.asList(KEY_1_BBUFFER, KEY_2_BBUFFER), BitOperation.NOT, KEY_3_BBUFFER)
				.block();
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void strLenShouldReturnValueCorrectly() {

		nativeCommands.set(KEY_1, VALUE_1);
		assertThat(connection.stringCommands().strLen(KEY_1_BBUFFER).block(), is(7L));
	}
}
