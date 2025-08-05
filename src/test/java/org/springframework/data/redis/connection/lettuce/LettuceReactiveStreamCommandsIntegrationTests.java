/*
 * Copyright 2018-2025 the original author or authors.
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

import io.lettuce.core.XReadArgs;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;

import org.assertj.core.data.Offset;
import org.junit.Ignore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedClass;

import org.springframework.data.domain.Range;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.Limit;
import org.springframework.data.redis.connection.RedisStreamCommands.XClaimOptions;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.test.condition.EnabledOnCommand;

/**
 * Integration tests for {@link LettuceReactiveStreamCommands}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @author Tugdual Grall
 * @author Dengliming
 * @author Jeonggyu Choi
 */
@ParameterizedClass
@EnabledOnCommand("XADD")
public class LettuceReactiveStreamCommandsIntegrationTests extends LettuceReactiveCommandsTestSupport {

	public LettuceReactiveStreamCommandsIntegrationTests(Fixture fixture) {
		super(fixture);
	}

	@Test // DATAREDIS-864
	void xAddShouldAddMessage() {

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
	void xDelShouldRemoveMessage() {

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
	void xRangeShouldReportMessages() {

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
	void xReadShouldReadMessage() {

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
	void xReadGroupShouldReadMessage() {

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
	void xRevRangeShouldReportMessages() {

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
	void xGroupCreateShouldCreateGroup() {

		nativeCommands.xadd(KEY_1, Collections.singletonMap(KEY_2, VALUE_2));

		connection.streamCommands().xGroupCreate(KEY_1_BBUFFER, "group-1", ReadOffset.latest()) //
				.as(StepVerifier::create) //
				.expectNext("OK") //
				.verifyComplete();
	}

	@Test // DATAREDIS-864
	void xGroupCreateShouldCreateGroupBeforeStream() {
		connection.streamCommands().xGroupCreate(KEY_1_BBUFFER, "group-1", ReadOffset.latest(), false)
				.as(StepVerifier::create) //
				.expectError(RedisSystemException.class) //
				.verify();

		connection.streamCommands().xGroupCreate(KEY_1_BBUFFER, "group-1", ReadOffset.latest(), true) //
				.as(StepVerifier::create) //
				.expectNext("OK") //
				.verifyComplete();
	}

	@Test // DATAREDIS-864
	@Ignore("commands sent correctly - however lettuce returns false")
	void xGroupDelConsumerShouldRemoveConsumer() {

		String id = nativeCommands.xadd(KEY_1, Collections.singletonMap(KEY_2, VALUE_2));
		nativeCommands.xgroupCreate(XReadArgs.StreamOffset.from(KEY_1, id), "group-1");
		nativeCommands.xreadgroup(io.lettuce.core.Consumer.from("group-1", "consumer-1"),
				XReadArgs.StreamOffset.from(KEY_1, id));

		connection.streamCommands().xGroupDelConsumer(KEY_1_BBUFFER, Consumer.from("group-1", "consumer-1"))
				.as(StepVerifier::create) //
				.expectNext("OK") //
				.verifyComplete();
	}

	@Test // DATAREDIS-864
	void xGroupDestroyShouldDestroyGroup() {

		String id = nativeCommands.xadd(KEY_1, Collections.singletonMap(KEY_2, VALUE_2));
		nativeCommands.xgroupCreate(XReadArgs.StreamOffset.from(KEY_1, id), "group-1");

		connection.streamCommands().xGroupDestroy(KEY_1_BBUFFER, "group-1").as(StepVerifier::create) //
				.expectNext("OK") //
				.verifyComplete();
	}

	@Test // DATAREDIS-1084
	void xPendingShouldLoadOverviewCorrectly() {

		String initialMessage = nativeCommands.xadd(KEY_1, KEY_1, VALUE_1);
		nativeCommands.xgroupCreate(XReadArgs.StreamOffset.from(KEY_1, initialMessage), "my-group");

		nativeCommands.xadd(KEY_1, KEY_2, VALUE_2);

		connection.streamCommands()
				.xReadGroup(Consumer.from("my-group", "my-consumer"),
						StreamOffset.create(KEY_1_BBUFFER, ReadOffset.lastConsumed())) //
				.then().as(StepVerifier::create) //
				.verifyComplete();

		nativeCommands.xadd(KEY_1, KEY_2, VALUE_2);

		connection.streamCommands().xPending(KEY_1_BBUFFER, "my-group").as(StepVerifier::create).assertNext(it -> {

			assertThat(it.getGroupName()).isEqualTo("my-group");
			assertThat(it.getTotalPendingMessages()).isEqualTo(1L);
			assertThat(it.getIdRange()).isNotNull();
			assertThat(it.getPendingMessagesPerConsumer()).hasSize(1).containsEntry("my-consumer", 1L);
		}).verifyComplete();
	}

	@Test // DATAREDIS-1084
	void xPendingShouldLoadEmptyOverviewCorrectly() {

		String initialMessage = nativeCommands.xadd(KEY_1, KEY_1, VALUE_1);
		nativeCommands.xgroupCreate(XReadArgs.StreamOffset.from(KEY_1, initialMessage), "my-group");

		nativeCommands.xadd(KEY_1, KEY_2, VALUE_2);

		connection.streamCommands().xPending(KEY_1_BBUFFER, "my-group").as(StepVerifier::create).assertNext(it -> {

			assertThat(it.getGroupName()).isEqualTo("my-group");
			assertThat(it.getTotalPendingMessages()).isEqualTo(0L);
			assertThat(it.getIdRange()).isNotNull();
			assertThat(it.getPendingMessagesPerConsumer()).isEmpty();
		}).verifyComplete();
	}

	@Test // DATAREDIS-1084
	void xPendingShouldLoadPendingMessages() {

		String initialMessage = nativeCommands.xadd(KEY_1, KEY_1, VALUE_1);
		nativeCommands.xgroupCreate(XReadArgs.StreamOffset.from(KEY_1, initialMessage), "my-group");

		nativeCommands.xadd(KEY_1, KEY_2, VALUE_2);

		connection.streamCommands()
				.xReadGroup(Consumer.from("my-group", "my-consumer"),
						StreamOffset.create(KEY_1_BBUFFER, ReadOffset.lastConsumed())) //
				.then().as(StepVerifier::create) //
				.verifyComplete();

		connection.streamCommands().xPending(KEY_1_BBUFFER, "my-group", Range.open("-", "+"), 10L).as(StepVerifier::create)
				.assertNext(it -> {

					assertThat(it.size()).isOne();
					assertThat(it.get(0).getConsumerName()).isEqualTo("my-consumer");
					assertThat(it.get(0).getGroupName()).isEqualTo("my-group");
					assertThat(it.get(0).getTotalDeliveryCount()).isOne();
					assertThat(it.get(0).getIdAsString()).isNotNull();
				}).verifyComplete();
	}

	@Test // DATAREDIS-1084
	void xPendingShouldLoadPendingMessagesForConsumer() {

		String initialMessage = nativeCommands.xadd(KEY_1, KEY_1, VALUE_1);
		nativeCommands.xgroupCreate(XReadArgs.StreamOffset.from(KEY_1, initialMessage), "my-group");

		nativeCommands.xadd(KEY_1, KEY_2, VALUE_2);

		connection.streamCommands()
				.xReadGroup(Consumer.from("my-group", "my-consumer"),
						StreamOffset.create(KEY_1_BBUFFER, ReadOffset.lastConsumed())) //
				.then().as(StepVerifier::create) //
				.verifyComplete();

		connection.streamCommands().xPending(KEY_1_BBUFFER, "my-group", "my-consumer", Range.open("-", "+"), 10L)
				.as(StepVerifier::create).assertNext(it -> {

					assertThat(it.size()).isOne();
					assertThat(it.get(0).getConsumerName()).isEqualTo("my-consumer");
					assertThat(it.get(0).getGroupName()).isEqualTo("my-group");
					assertThat(it.get(0).getTotalDeliveryCount()).isOne();
					assertThat(it.get(0).getIdAsString()).isNotNull();
				}).verifyComplete();
	}

	@Test // DATAREDIS-1084
	void xPendingShouldLoadPendingMessagesForNonExistingConsumer() {

		String initialMessage = nativeCommands.xadd(KEY_1, KEY_1, VALUE_1);
		nativeCommands.xgroupCreate(XReadArgs.StreamOffset.from(KEY_1, initialMessage), "my-group");

		nativeCommands.xadd(KEY_1, KEY_2, VALUE_2);

		connection.streamCommands()
				.xReadGroup(Consumer.from("my-group", "my-consumer"),
						StreamOffset.create(KEY_1_BBUFFER, ReadOffset.lastConsumed())) //
				.then().as(StepVerifier::create) //
				.verifyComplete();

		connection.streamCommands().xPending(KEY_1_BBUFFER, "my-group", "my-consumer-2", Range.open("-", "+"), 10L)
				.as(StepVerifier::create).assertNext(it -> {

					assertThat(it.size()).isZero();
				}).verifyComplete();
	}

	@ParameterizedRedisTest // GH-2046
	void xPendingShouldLoadPendingMessagesForGroupAndIdle() {

		String initialMessage = nativeCommands.xadd(KEY_1, KEY_1, VALUE_1);
		nativeCommands.xgroupCreate(XReadArgs.StreamOffset.from(KEY_1, initialMessage), "my-group");

		nativeCommands.xadd(KEY_1, KEY_2, VALUE_2);

		connection.streamCommands()
				.xReadGroup(Consumer.from("my-group", "my-consumer"),
						StreamOffset.create(KEY_1_BBUFFER, ReadOffset.lastConsumed()))
				.then().as(StepVerifier::create).verifyComplete();

		Duration exceededIdle = Duration.of(1, ChronoUnit.MILLIS);

		connection.streamCommands().xPending(KEY_1_BBUFFER, "my-group", Range.open("-", "+"), 10L, exceededIdle)
				.delaySubscription(Duration.ofMillis(100)).as(StepVerifier::create).assertNext(it -> {
					assertThat(it.size()).isOne();
					assertThat(it.get(0).getConsumerName()).isEqualTo("my-consumer");
					assertThat(it.get(0).getGroupName()).isEqualTo("my-group");
					assertThat(it.get(0).getTotalDeliveryCount()).isOne();
					assertThat(it.get(0).getIdAsString()).isNotNull();
				}).verifyComplete();
	}

	@ParameterizedRedisTest // GH-2046
	void xPendingShouldLoadEmptyPendingMessagesForGroupAndIdleWhenDurationNotExceeded() {

		String initialMessage = nativeCommands.xadd(KEY_1, KEY_1, VALUE_1);
		nativeCommands.xgroupCreate(XReadArgs.StreamOffset.from(KEY_1, initialMessage), "my-group");

		nativeCommands.xadd(KEY_1, KEY_2, VALUE_2);

		connection.streamCommands()
				.xReadGroup(Consumer.from("my-group", "my-consumer"),
						StreamOffset.create(KEY_1_BBUFFER, ReadOffset.lastConsumed()))
				.then().as(StepVerifier::create).verifyComplete();

		Duration notExceededIdle = Duration.ofMinutes(10);

		connection.streamCommands().xPending(KEY_1_BBUFFER, "my-group", Range.open("-", "+"), 10L, notExceededIdle)
				.delaySubscription(Duration.ofMillis(100))

				.as(StepVerifier::create).assertNext(it -> {
					assertThat(it.isEmpty()).isTrue();
				}).verifyComplete();
	}

	@ParameterizedRedisTest // GH-2046
	void xPendingShouldLoadPendingMessagesForGroupNameAndConsumerNameAndIdle() {

		String initialMessage = nativeCommands.xadd(KEY_1, KEY_1, VALUE_1);
		nativeCommands.xgroupCreate(XReadArgs.StreamOffset.from(KEY_1, initialMessage), "my-group");

		nativeCommands.xadd(KEY_1, KEY_2, VALUE_2);

		connection.streamCommands()
				.xReadGroup(Consumer.from("my-group", "my-consumer"),
						StreamOffset.create(KEY_1_BBUFFER, ReadOffset.lastConsumed()))
				.then().as(StepVerifier::create).verifyComplete();

		Duration exceededIdle = Duration.ofMillis(1);

		connection.streamCommands()
				.xPending(KEY_1_BBUFFER, "my-group", "my-consumer", Range.open("-", "+"), 10L, exceededIdle)
				.delaySubscription(Duration.ofMillis(100))

				.as(StepVerifier::create).assertNext(it -> {
					assertThat(it.size()).isOne();
					assertThat(it.get(0).getConsumerName()).isEqualTo("my-consumer");
					assertThat(it.get(0).getGroupName()).isEqualTo("my-group");
					assertThat(it.get(0).getTotalDeliveryCount()).isOne();
					assertThat(it.get(0).getIdAsString()).isNotNull();
				}).verifyComplete();
	}

	@ParameterizedRedisTest // GH-2046
	void xPendingShouldLoadEmptyPendingMessagesForGroupNameAndConsumerNameAndIdleWhenDurationNotExceeded() {

		String initialMessage = nativeCommands.xadd(KEY_1, KEY_1, VALUE_1);
		nativeCommands.xgroupCreate(XReadArgs.StreamOffset.from(KEY_1, initialMessage), "my-group");

		nativeCommands.xadd(KEY_1, KEY_2, VALUE_2);

		connection.streamCommands()
				.xReadGroup(Consumer.from("my-group", "my-consumer"),
						StreamOffset.create(KEY_1_BBUFFER, ReadOffset.lastConsumed()))
				.then().as(StepVerifier::create).verifyComplete();

		Duration notExceededIdle = Duration.ofMinutes(10);

		connection.streamCommands()
				.xPending(KEY_1_BBUFFER, "my-group", "my-consumer", Range.open("-", "+"), 10L, notExceededIdle)
				.delaySubscription(Duration.ofMillis(100))

				.as(StepVerifier::create).assertNext(it -> {
					assertThat(it.isEmpty()).isTrue();
				}).verifyComplete();
	}

	@ParameterizedRedisTest // GH-2046
	void xPendingShouldLoadPendingMessageesForConsumerAndIdle() {

		String initialMessage = nativeCommands.xadd(KEY_1, KEY_1, VALUE_1);
		nativeCommands.xgroupCreate(XReadArgs.StreamOffset.from(KEY_1, initialMessage), "my-group");

		nativeCommands.xadd(KEY_1, KEY_2, VALUE_2);

		connection.streamCommands()
				.xReadGroup(Consumer.from("my-group", "my-consumer"),
						StreamOffset.create(KEY_1_BBUFFER, ReadOffset.lastConsumed()))
				.then().as(StepVerifier::create).verifyComplete();

		Duration exceededIdle = Duration.ofMillis(1);

		connection.streamCommands()
				.xPending(KEY_1_BBUFFER, Consumer.from("my-group", "my-consumer"), Range.open("-", "+"), 10L, exceededIdle)
				.delaySubscription(Duration.ofMillis(100))

				.as(StepVerifier::create).assertNext(it -> {
					assertThat(it.size()).isOne();
					assertThat(it.get(0).getConsumerName()).isEqualTo("my-consumer");
					assertThat(it.get(0).getGroupName()).isEqualTo("my-group");
					assertThat(it.get(0).getTotalDeliveryCount()).isOne();
					assertThat(it.get(0).getIdAsString()).isNotNull();
				}).verifyComplete();
	}

	@ParameterizedRedisTest // GH-2046
	void xPendingShouldLoadEmptyPendingMessagesForConsumerAndIdleWhenDurationNotExceeded() {

		String initialMessage = nativeCommands.xadd(KEY_1, KEY_1, VALUE_1);
		nativeCommands.xgroupCreate(XReadArgs.StreamOffset.from(KEY_1, initialMessage), "my-group");

		nativeCommands.xadd(KEY_1, KEY_2, VALUE_2);

		connection.streamCommands()
				.xReadGroup(Consumer.from("my-group", "my-consumer"),
						StreamOffset.create(KEY_1_BBUFFER, ReadOffset.lastConsumed()))
				.then().as(StepVerifier::create).verifyComplete();

		Duration notExceededIdle = Duration.ofMinutes(10);

		connection.streamCommands()
				.xPending(KEY_1_BBUFFER, Consumer.from("my-group", "my-consumer"), Range.open("-", "+"), 10L, notExceededIdle)
				.delaySubscription(Duration.ofMillis(100)).as(StepVerifier::create).assertNext(it -> {
					assertThat(it.isEmpty()).isTrue();
				}).verifyComplete();
	}

	@Test // DATAREDIS-1084
	void xPendingShouldLoadEmptyPendingMessages() {

		String initialMessage = nativeCommands.xadd(KEY_1, KEY_1, VALUE_1);
		nativeCommands.xgroupCreate(XReadArgs.StreamOffset.from(KEY_1, initialMessage), "my-group");

		nativeCommands.xadd(KEY_1, KEY_2, VALUE_2);

		connection.streamCommands().xPending(KEY_1_BBUFFER, "my-group", Range.open("-", "+"), 10L).as(StepVerifier::create)
				.assertNext(it -> {
					assertThat(it.isEmpty()).isTrue();
				}).verifyComplete();
	}

	@Test // DATAREDIS-1084
	void xClaim() {

		String initialMessage = nativeCommands.xadd(KEY_1, KEY_1, VALUE_1);
		nativeCommands.xgroupCreate(XReadArgs.StreamOffset.from(KEY_1, initialMessage), "my-group");

		String expected = nativeCommands.xadd(KEY_1, KEY_2, VALUE_2);

		connection.streamCommands()
				.xReadGroup(Consumer.from("my-group", "my-consumer"),
						StreamOffset.create(KEY_1_BBUFFER, ReadOffset.lastConsumed())) //
				.delayElements(Duration.ofMillis(5)).next() //
				.flatMapMany(record -> {
					return connection.streamCommands().xClaim(KEY_1_BBUFFER, "my-group", "my-consumer",
							XClaimOptions.minIdle(Duration.ofMillis(1)).ids(record.getId()));
				}

				).as(StepVerifier::create) //
				.assertNext(it -> assertThat(it.getId().getValue()).isEqualTo(expected)) //
				.verifyComplete();
	}

	@Test // DATAREDIS-1119
	void xinfo() {

		String firstRecord = nativeCommands.xadd(KEY_1, KEY_2, VALUE_2);
		String lastRecord = nativeCommands.xadd(KEY_1, KEY_3, VALUE_3);
		nativeCommands.xgroupCreate(XReadArgs.StreamOffset.from(KEY_1, "0"), "my-group");
		nativeCommands.xreadgroup(io.lettuce.core.Consumer.from("my-group", "my-consumer"),
				XReadArgs.StreamOffset.from(KEY_1, ">"));

		connection.streamCommands().xInfo(KEY_1_BBUFFER).as(StepVerifier::create) //
				.consumeNextWith(info -> {
					assertThat(info.streamLength()).isEqualTo(2L);
					assertThat(info.radixTreeKeySize()).isOne();
					assertThat(info.radixTreeNodesSize()).isEqualTo(2L);
					assertThat(info.groupCount()).isOne();
					assertThat(info.lastGeneratedId()).isEqualTo(lastRecord);
					assertThat(info.firstEntryId()).isEqualTo(firstRecord);
					assertThat(info.lastEntryId()).isEqualTo(lastRecord);
				}).verifyComplete();
	}

	@Test // DATAREDIS-1119
	void xinfoNoGroup() {

		String firstRecord = nativeCommands.xadd(KEY_1, KEY_2, VALUE_2);
		String lastRecord = nativeCommands.xadd(KEY_1, KEY_3, VALUE_3);

		connection.streamCommands().xInfo(KEY_1_BBUFFER).as(StepVerifier::create) //
				.consumeNextWith(info -> {
					assertThat(info.streamLength()).isEqualTo(2L);
					assertThat(info.radixTreeKeySize()).isOne();
					assertThat(info.radixTreeNodesSize()).isEqualTo(2L);
					assertThat(info.groupCount()).isZero();
					assertThat(info.lastGeneratedId()).isEqualTo(lastRecord);
					assertThat(info.firstEntryId()).isEqualTo(firstRecord);
					assertThat(info.lastEntryId()).isEqualTo(lastRecord);
				}).verifyComplete();
	}

	@Test // DATAREDIS-1119
	void xinfoGroups() {

		nativeCommands.xadd(KEY_1, KEY_2, VALUE_2);
		String lastRecord = nativeCommands.xadd(KEY_1, KEY_3, VALUE_3);
		nativeCommands.xgroupCreate(XReadArgs.StreamOffset.from(KEY_1, "0"), "my-group");
		nativeCommands.xreadgroup(io.lettuce.core.Consumer.from("my-group", "my-consumer"),
				XReadArgs.StreamOffset.from(KEY_1, ">"));

		connection.streamCommands().xInfoGroups(KEY_1_BBUFFER).as(StepVerifier::create) //
				.consumeNextWith(info -> {
					assertThat(info.groupName()).isEqualTo("my-group");
					assertThat(info.consumerCount()).isEqualTo(1L);
					assertThat(info.pendingCount()).isEqualTo(2L);
					assertThat(info.lastDeliveredId()).isEqualTo(lastRecord);
				}).verifyComplete();
	}

	@Test // DATAREDIS-1119
	void xinfoGroupsNoGroup() {

		nativeCommands.xadd(KEY_1, KEY_2, VALUE_2);
		String lastRecord = nativeCommands.xadd(KEY_1, KEY_3, VALUE_3);

		connection.streamCommands().xInfoGroups(KEY_1_BBUFFER).as(StepVerifier::create) //
				.verifyComplete();
	}

	@Test // DATAREDIS-1119
	void xinfoGroupsNoConsumer() {

		nativeCommands.xadd(KEY_1, KEY_2, VALUE_2);
		String lastRecord = nativeCommands.xadd(KEY_1, KEY_3, VALUE_3);
		nativeCommands.xgroupCreate(XReadArgs.StreamOffset.from(KEY_1, "0"), "my-group");

		connection.streamCommands().xInfoGroups(KEY_1_BBUFFER).as(StepVerifier::create) //
				.consumeNextWith(info -> {
					assertThat(info.groupName()).isEqualTo("my-group");
					assertThat(info.consumerCount()).isZero();
					assertThat(info.pendingCount()).isZero();
					assertThat(info.lastDeliveredId()).isEqualTo("0-0");
				}).verifyComplete();
	}

	@Test // DATAREDIS-1119
	void xinfoConsumers() {

		nativeCommands.xadd(KEY_1, KEY_2, VALUE_2);
		nativeCommands.xadd(KEY_1, KEY_3, VALUE_3);
		nativeCommands.xgroupCreate(XReadArgs.StreamOffset.from(KEY_1, "0"), "my-group");
		nativeCommands.xreadgroup(io.lettuce.core.Consumer.from("my-group", "my-consumer"),
				XReadArgs.StreamOffset.from(KEY_1, ">"));

		connection.streamCommands().xInfoConsumers(KEY_1_BBUFFER, "my-group").as(StepVerifier::create) //
				.consumeNextWith(info -> {
					assertThat(info.groupName()).isEqualTo("my-group");
					assertThat(info.consumerName()).isEqualTo("my-consumer");
					assertThat(info.pendingCount()).isEqualTo(2L);
					assertThat(info.idleTimeMs()).isCloseTo(1L, Offset.offset(200L));
				}).verifyComplete();
	}

	@Test // DATAREDIS-1119
	void xinfoConsumersNoConsumer() {

		nativeCommands.xadd(KEY_1, KEY_2, VALUE_2);
		nativeCommands.xadd(KEY_1, KEY_3, VALUE_3);
		nativeCommands.xgroupCreate(XReadArgs.StreamOffset.from(KEY_1, "0"), "my-group");

		connection.streamCommands().xInfoConsumers(KEY_1_BBUFFER, "my-group").as(StepVerifier::create).verifyComplete();
	}

	@Test // DATAREDIS-1226
	void xClaimJustId() {

		String initialMessage = nativeCommands.xadd(KEY_1, KEY_1, VALUE_1);
		nativeCommands.xgroupCreate(XReadArgs.StreamOffset.from(KEY_1, initialMessage), "my-group");

		String expected = nativeCommands.xadd(KEY_1, KEY_2, VALUE_2);

		connection.streamCommands()
				.xReadGroup(Consumer.from("my-group", "my-consumer"),
						StreamOffset.create(KEY_1_BBUFFER, ReadOffset.lastConsumed())) //
				.delayElements(Duration.ofMillis(5)).next() //
				.flatMapMany(record -> connection.streamCommands().xClaimJustId(KEY_1_BBUFFER, "my-group", "my-consumer",
						XClaimOptions.minIdle(Duration.ofMillis(1)).ids(record.getId()))
				).as(StepVerifier::create) //
				.assertNext(it -> assertThat(it.getValue()).isEqualTo(expected)) //
				.verifyComplete();
	}
}
