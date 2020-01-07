/*
 * Copyright 2018-2020 the original author or authors.
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

import io.lettuce.core.XReadArgs;
import org.junit.Ignore;
import org.springframework.data.redis.connection.stream.RecordId;
import reactor.test.StepVerifier;

import java.util.Collections;

import org.junit.Before;
import org.junit.Test;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.RedisTestProfileValueSource;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.RedisZSetCommands.Limit;

/**
 * Integration tests for {@link LettuceReactiveStreamCommands}.
 *
 * @author Mark Paluch
 */
public class LettuceReactiveStreamCommandsTests extends LettuceReactiveCommandsTestsBase {

	@Before
	public void before() {

		// TODO: Upgrade to 5.0
		assumeTrue(RedisTestProfileValueSource.matches("redisVersion", "4.9"));
	}

	@Test // DATAREDIS-864
	public void xAddShouldAddMessage() {

		connection.streamCommands().xAdd(KEY_1_BBUFFER, Collections.singletonMap(KEY_2_BBUFFER, VALUE_2_BBUFFER)) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		connection.streamCommands().xLen(KEY_1_BBUFFER) //
				.as(StepVerifier::create) //
				.expectNext(1L) //
				.verifyComplete();
	}

	@Test // DATAREDIS-864
	public void xDelShouldRemoveMessage() {

		RecordId messageId = connection.streamCommands()
				.xAdd(KEY_1_BBUFFER, Collections.singletonMap(KEY_2_BBUFFER, VALUE_2_BBUFFER)).block();

		connection.streamCommands().xDel(KEY_1_BBUFFER, messageId) //
				.as(StepVerifier::create) //
				.expectNext(1L) //
				.verifyComplete();

		connection.streamCommands().xLen(KEY_1_BBUFFER) //
				.as(StepVerifier::create) //
				.expectNext(0L) //
				.verifyComplete();
	}

	@Test // DATAREDIS-864
	public void xRangeShouldReportMessages() {

		connection.streamCommands().xAdd(KEY_1_BBUFFER, Collections.singletonMap(KEY_1_BBUFFER, VALUE_1_BBUFFER)) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		connection.streamCommands().xAdd(KEY_1_BBUFFER, Collections.singletonMap(KEY_2_BBUFFER, VALUE_2_BBUFFER)) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		connection.streamCommands().xRange(KEY_1_BBUFFER, Range.unbounded()) //
				.as(StepVerifier::create) //
				.assertNext(it -> {

					assertThat(it.getStream()).isEqualTo(KEY_1_BBUFFER);
					assertThat(it.getValue()).containsEntry(KEY_1_BBUFFER, VALUE_1_BBUFFER);

				}) //
				.expectNextCount(1).verifyComplete();

		connection.streamCommands().xRange(KEY_1_BBUFFER, Range.unbounded(), Limit.limit().count(1)) //
				.as(StepVerifier::create) //
				.assertNext(it -> {

					assertThat(it.getStream()).isEqualTo(KEY_1_BBUFFER);
					assertThat(it.getValue()).containsEntry(KEY_1_BBUFFER, VALUE_1_BBUFFER);
				}) //
				.verifyComplete();
	}

	@Test // DATAREDIS-864
	public void xReadShouldReadMessage() {

		connection.streamCommands().xAdd(KEY_1_BBUFFER, Collections.singletonMap(KEY_1_BBUFFER, VALUE_1_BBUFFER)) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		connection.streamCommands().xRead(StreamOffset.create(KEY_1_BBUFFER, ReadOffset.from("0-0"))) //
				.as(StepVerifier::create) //
				.assertNext(it -> {

					assertThat(it.getStream()).isEqualTo(KEY_1_BBUFFER);
					assertThat(it.getValue()).containsEntry(KEY_1_BBUFFER, VALUE_1_BBUFFER);
				}) //
				.verifyComplete();
	}

	@Test // DATAREDIS-864
	public void xReadGroupShouldReadMessage() {

		String initialMessage = nativeCommands.xadd(KEY_1, KEY_1, VALUE_1);
		nativeCommands.xgroupCreate(XReadArgs.StreamOffset.from(KEY_1, initialMessage), "my-group");

		nativeCommands.xadd(KEY_1, KEY_2, VALUE_2);

		connection.streamCommands()
				.xReadGroup(Consumer.from("my-group", "my-consumer"),
						StreamOffset.create(KEY_1_BBUFFER, ReadOffset.lastConsumed())) //
				.as(StepVerifier::create) //
				.assertNext(it -> {

					assertThat(it.getStream()).isEqualTo(KEY_1_BBUFFER);
					assertThat(it.getValue()).containsEntry(KEY_2_BBUFFER, VALUE_2_BBUFFER);
				}) //
				.verifyComplete();
	}

	@Test // DATAREDIS-864
	public void xRevRangeShouldReportMessages() {

		connection.streamCommands().xAdd(KEY_1_BBUFFER, Collections.singletonMap(KEY_1_BBUFFER, VALUE_1_BBUFFER)) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		connection.streamCommands().xAdd(KEY_1_BBUFFER, Collections.singletonMap(KEY_2_BBUFFER, VALUE_2_BBUFFER)) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		connection.streamCommands().xRevRange(KEY_1_BBUFFER, Range.unbounded(), Limit.limit().count(1)) //
				.as(StepVerifier::create) //
				.assertNext(it -> {

					assertThat(it.getStream()).isEqualTo(KEY_1_BBUFFER);
					assertThat(it.getValue()).containsEntry(KEY_2_BBUFFER, VALUE_2_BBUFFER);
				}) //
				.verifyComplete();
	}

	@Test // DATAREDIS-864
	public void xGroupCreateShouldCreateGroup() {

		nativeCommands.xadd(KEY_1, Collections.singletonMap(KEY_2, VALUE_2));

		connection.streamCommands().xGroupCreate(KEY_1_BBUFFER, "group-1", ReadOffset.latest()) //
		.as(StepVerifier::create) //
		.expectNext("OK") //
		.verifyComplete();
	}

	@Test // DATAREDIS-864
	@Ignore("commands sent correctly - however lettuce returns false")
	public void xGroupDelConsumerShouldRemoveConsumer() {

		String id = nativeCommands.xadd(KEY_1, Collections.singletonMap(KEY_2, VALUE_2));
		nativeCommands.xgroupCreate(XReadArgs.StreamOffset.from(KEY_1, id), "group-1");
		nativeCommands.xreadgroup(io.lettuce.core.Consumer.from("group-1", "consumer-1"), XReadArgs.StreamOffset.from(KEY_1, id));

		connection.streamCommands().xGroupDelConsumer(KEY_1_BBUFFER, Consumer.from("group-1", "consumer-1"))
				.as(StepVerifier::create) //
				.expectNext("OK") //
				.verifyComplete();
	}

	@Test // DATAREDIS-864
	public void xGroupDestroyShouldDestroyGroup() {

		String id = nativeCommands.xadd(KEY_1, Collections.singletonMap(KEY_2, VALUE_2));
		nativeCommands.xgroupCreate(XReadArgs.StreamOffset.from(KEY_1, id), "group-1");

		connection.streamCommands().xGroupDestroy(KEY_1_BBUFFER, "group-1")
				.as(StepVerifier::create) //
				.expectNext("OK") //
				.verifyComplete();
	}



}
