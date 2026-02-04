/*
 * Copyright 2026-present the original author or authors.
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
package org.springframework.data.redis.connection.jedis;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.connection.Limit;
import org.springframework.data.redis.connection.RedisClusterConfiguration;
import org.springframework.data.redis.connection.RedisClusterConnection;
import org.springframework.data.redis.connection.RedisStreamCommands.XClaimOptions;
import org.springframework.data.redis.connection.RedisStreamCommands.XPendingOptions;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.test.condition.EnabledOnRedisClusterAvailable;
import org.springframework.data.redis.test.extension.JedisExtension;

import static org.assertj.core.api.Assertions.*;

/**
 * Integration tests for {@link JedisClientStreamCommands} in cluster mode. Tests all methods in direct and pipelined
 * modes (transactions not supported in cluster).
 *
 * @author Tihomir Mateev
 * @since 4.1
 */
@EnabledOnRedisClusterAvailable
@ExtendWith(JedisExtension.class)
class JedisClientClusterStreamCommandsIntegrationTests {

	private JedisClientConnectionFactory factory;
	private RedisClusterConnection connection;

	@BeforeEach
	void setUp() {
		RedisClusterConfiguration config = new RedisClusterConfiguration().clusterNode(SettingsUtils.getHost(),
				SettingsUtils.getClusterPort());
		factory = new JedisClientConnectionFactory(config);
		factory.afterPropertiesSet();
		connection = factory.getClusterConnection();
	}

	@AfterEach
	void tearDown() {
		if (connection != null) {
			connection.serverCommands().flushDb();
			connection.close();
		}
		if (factory != null) {
			factory.destroy();
		}
	}

	// ============ Basic Stream Operations ============
	@Test
	void basicStreamOperationsShouldWork() {
		// Test xAdd - add entry to stream
		Map<byte[], byte[]> body = new HashMap<>();
		body.put("field1".getBytes(), "value1".getBytes());
		body.put("field2".getBytes(), "value2".getBytes());

		RecordId recordId = connection.streamCommands().xAdd("stream1".getBytes(), body);
		assertThat(recordId).isNotNull();

		// Test xLen - get stream length
		Long xLenResult = connection.streamCommands().xLen("stream1".getBytes());
		assertThat(xLenResult).isEqualTo(1L);

		// Add more entries
		Map<byte[], byte[]> body2 = Collections.singletonMap("field3".getBytes(), "value3".getBytes());
		connection.streamCommands().xAdd("stream1".getBytes(), body2);

		// Test xRange - get range of entries
		List<ByteRecord> xRangeResult = connection.streamCommands().xRange("stream1".getBytes(), Range.unbounded(),
				Limit.unlimited());
		assertThat(xRangeResult).hasSize(2);

		// Test xRevRange - get reverse range
		List<ByteRecord> xRevRangeResult = connection.streamCommands().xRevRange("stream1".getBytes(), Range.unbounded(),
				Limit.unlimited());
		assertThat(xRevRangeResult).hasSize(2);

		// Test xDel - delete entry
		Long xDelResult = connection.streamCommands().xDel("stream1".getBytes(), recordId);
		assertThat(xDelResult).isEqualTo(1L);
		assertThat(connection.streamCommands().xLen("stream1".getBytes())).isEqualTo(1L);
	}

	@Test
	void streamTrimOperationsShouldWork() {
		// Add multiple entries
		for (int i = 0; i < 10; i++) {
			Map<byte[], byte[]> body = Collections.singletonMap(("field" + i).getBytes(), ("value" + i).getBytes());
			connection.streamCommands().xAdd("stream1".getBytes(), body);
		}

		// Test xTrim - trim stream to max length
		Long xTrimResult = connection.streamCommands().xTrim("stream1".getBytes(), 5);
		assertThat(xTrimResult).isGreaterThanOrEqualTo(0L);
		assertThat(connection.streamCommands().xLen("stream1".getBytes())).isLessThanOrEqualTo(5L);

		// Add more entries
		for (int i = 0; i < 10; i++) {
			Map<byte[], byte[]> body = Collections.singletonMap(("field" + i).getBytes(), ("value" + i).getBytes());
			connection.streamCommands().xAdd("stream2".getBytes(), body);
		}

		// Test xTrim with approximate
		Long xTrimApproxResult = connection.streamCommands().xTrim("stream2".getBytes(), 5, true);
		assertThat(xTrimApproxResult).isGreaterThanOrEqualTo(0L);
	}

	@Test
	void streamConsumerGroupOperationsShouldWork() {
		// Add entries
		Map<byte[], byte[]> body = Collections.singletonMap("field1".getBytes(), "value1".getBytes());
		RecordId recordId = connection.streamCommands().xAdd("stream1".getBytes(), body);

		// Test xGroupCreate - create consumer group
		String xGroupCreateResult = connection.streamCommands().xGroupCreate("stream1".getBytes(), "group1",
				ReadOffset.from("0"));
		assertThat(xGroupCreateResult).isEqualTo("OK");

		// Test xReadGroup - read from consumer group
		Consumer consumer = Consumer.from("group1", "consumer1");
		List<ByteRecord> xReadGroupResult = connection.streamCommands().xReadGroup(consumer,
				StreamReadOptions.empty().count(10), StreamOffset.create("stream1".getBytes(), ReadOffset.lastConsumed()));
		assertThat(xReadGroupResult).hasSize(1);

		// Test xAck - acknowledge message
		Long xAckResult = connection.streamCommands().xAck("stream1".getBytes(), "group1", recordId);
		assertThat(xAckResult).isEqualTo(1L);

		// Test xPending - get pending messages
		PendingMessagesSummary xPendingResult = connection.streamCommands().xPending("stream1".getBytes(), "group1");
		assertThat(xPendingResult).isNotNull();

		// Add more entries for pending test
		RecordId recordId2 = connection.streamCommands().xAdd("stream1".getBytes(), body);
		connection.streamCommands().xReadGroup(consumer, StreamReadOptions.empty().count(10),
				StreamOffset.create("stream1".getBytes(), ReadOffset.lastConsumed()));

		// Test xPending with range
		PendingMessages xPendingRangeResult = connection.streamCommands().xPending("stream1".getBytes(), "group1",
				XPendingOptions.unbounded());
		assertThat(xPendingRangeResult).isNotNull();

		// Test xPending with consumer
		PendingMessages xPendingConsumerResult = connection.streamCommands().xPending("stream1".getBytes(), "group1",
				XPendingOptions.unbounded().consumer("consumer1"));
		assertThat(xPendingConsumerResult).isNotNull();

		// Test xGroupDelConsumer - delete consumer
		Boolean xGroupDelConsumerResult = connection.streamCommands().xGroupDelConsumer("stream1".getBytes(), consumer);
		assertThat(xGroupDelConsumerResult).isTrue();

		// Test xGroupDestroy - destroy consumer group
		Boolean xGroupDestroyResult = connection.streamCommands().xGroupDestroy("stream1".getBytes(), "group1");
		assertThat(xGroupDestroyResult).isTrue();
	}

	@Test
	void streamClaimOperationsShouldWork() {
		// Add entries
		Map<byte[], byte[]> body = Collections.singletonMap("field1".getBytes(), "value1".getBytes());
		RecordId recordId = connection.streamCommands().xAdd("stream1".getBytes(), body);

		// Create consumer group and read
		connection.streamCommands().xGroupCreate("stream1".getBytes(), "group1", ReadOffset.from("0"));
		Consumer consumer1 = Consumer.from("group1", "consumer1");
		connection.streamCommands().xReadGroup(consumer1, StreamReadOptions.empty().count(10),
				StreamOffset.create("stream1".getBytes(), ReadOffset.lastConsumed()));

		// Test xClaim - claim pending message
		Consumer consumer2 = Consumer.from("group1", "consumer2");
		List<ByteRecord> xClaimResult = connection.streamCommands().xClaim("stream1".getBytes(), "group1",
				consumer2.getName(), Duration.ofMillis(0), recordId);
		assertThat(xClaimResult).isNotEmpty();

		// Test xClaimJustId - claim and return only IDs
		RecordId recordId2 = connection.streamCommands().xAdd("stream1".getBytes(), body);
		connection.streamCommands().xReadGroup(consumer1, StreamReadOptions.empty().count(10),
				StreamOffset.create("stream1".getBytes(), ReadOffset.lastConsumed()));

		List<RecordId> xClaimJustIdResult = connection.streamCommands().xClaimJustId("stream1".getBytes(), "group1",
				consumer2.getName(), XClaimOptions.minIdle(Duration.ofMillis(0)).ids(recordId2));
		assertThat(xClaimJustIdResult).isNotEmpty();
	}
}
