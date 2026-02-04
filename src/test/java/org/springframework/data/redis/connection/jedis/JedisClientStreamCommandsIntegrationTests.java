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
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.RedisStreamCommands;
import org.springframework.data.redis.connection.RedisStreamCommands.TrimOptions;
import org.springframework.data.redis.connection.RedisStreamCommands.XAddOptions;
import org.springframework.data.redis.connection.RedisStreamCommands.XClaimOptions;
import org.springframework.data.redis.connection.RedisStreamCommands.XDelOptions;
import org.springframework.data.redis.connection.RedisStreamCommands.XPendingOptions;
import org.springframework.data.redis.connection.RedisStreamCommands.XTrimOptions;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.test.condition.EnabledOnRedisAvailable;
import org.springframework.data.redis.test.extension.JedisExtension;

import static org.assertj.core.api.Assertions.*;

/**
 * Integration tests for {@link JedisClientStreamCommands}. Tests all methods in direct, transaction, and pipelined
 * modes.
 *
 * @author Tihomir Mateev
 * @since 4.1
 */
@EnabledOnRedisAvailable
@ExtendWith(JedisExtension.class)
class JedisClientStreamCommandsIntegrationTests {

	private JedisClientConnectionFactory factory;
	private JedisClientConnection connection;

	@BeforeEach
	void setUp() {
		RedisStandaloneConfiguration config = new RedisStandaloneConfiguration(SettingsUtils.getHost(),
				SettingsUtils.getPort());
		factory = new JedisClientConnectionFactory(config);
		factory.afterPropertiesSet();
		connection = (JedisClientConnection) factory.getConnection();
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
		MapRecord<byte[], byte[], byte[]> record = MapRecord.create("stream1".getBytes(), body);
		RecordId recordId = connection.streamCommands().xAdd(record, XAddOptions.none());
		assertThat(recordId).isNotNull();

		// Test xLen - get stream length
		Long length = connection.streamCommands().xLen("stream1".getBytes());
		assertThat(length).isEqualTo(1L);

		// Add more entries
		body.put("field2".getBytes(), "value2".getBytes());
		MapRecord<byte[], byte[], byte[]> record2 = MapRecord.create("stream1".getBytes(), body);
		connection.streamCommands().xAdd(record2, XAddOptions.none());

		// Test xRange - get range of entries
		List<ByteRecord> rangeResult = connection.streamCommands().xRange("stream1".getBytes(), Range.unbounded(),
				Limit.unlimited());
		assertThat(rangeResult).hasSize(2);

		// Test xRevRange - get reverse range
		List<ByteRecord> revRangeResult = connection.streamCommands().xRevRange("stream1".getBytes(), Range.unbounded(),
				Limit.unlimited());
		assertThat(revRangeResult).hasSize(2);

		// Test xDel - delete entry
		Long delResult = connection.streamCommands().xDel("stream1".getBytes(), recordId);
		assertThat(delResult).isEqualTo(1L);
		assertThat(connection.streamCommands().xLen("stream1".getBytes())).isEqualTo(1L);
	}

	@Test
	void streamTrimOperationsShouldWork() {
		// Add multiple entries
		Map<byte[], byte[]> body = new HashMap<>();
		body.put("field".getBytes(), "value".getBytes());
		for (int i = 0; i < 10; i++) {
			MapRecord<byte[], byte[], byte[]> record = MapRecord.create("stream2".getBytes(), body);
			connection.streamCommands().xAdd(record, XAddOptions.none());
		}
		assertThat(connection.streamCommands().xLen("stream2".getBytes())).isEqualTo(10L);

		// Test xTrim - trim stream to max length
		Long trimResult = connection.streamCommands().xTrim("stream2".getBytes(), 5);
		assertThat(trimResult).isGreaterThan(0L);
		assertThat(connection.streamCommands().xLen("stream2".getBytes())).isLessThanOrEqualTo(5L);

		// Test xTrim with approximate flag
		Long trimApproxResult = connection.streamCommands().xTrim("stream2".getBytes(), 3, true);
		assertThat(connection.streamCommands().xLen("stream2".getBytes())).isLessThanOrEqualTo(5L);

		// Test xTrim with XTrimOptions
		XTrimOptions trimOptions = XTrimOptions.trim(TrimOptions.maxLen(2L));
		Long trimOptionsResult = connection.streamCommands().xTrim("stream2".getBytes(), trimOptions);
		assertThat(connection.streamCommands().xLen("stream2".getBytes())).isLessThanOrEqualTo(2L);
	}

	@Test
	void streamInfoOperationsShouldWork() {
		// Add entry
		Map<byte[], byte[]> body = Collections.singletonMap("field".getBytes(), "value".getBytes());
		MapRecord<byte[], byte[], byte[]> record = MapRecord.create("stream3".getBytes(), body);
		connection.streamCommands().xAdd(record, XAddOptions.none());

		// Test xInfo - get stream info
		StreamInfo.XInfoStream info = connection.streamCommands().xInfo("stream3".getBytes());
		assertThat(info).isNotNull();
		assertThat(info.streamLength()).isEqualTo(1L);
	}

	@Test
	void streamConsumerGroupOperationsShouldWork() {
		// Add entries
		Map<byte[], byte[]> body = Collections.singletonMap("field".getBytes(), "value".getBytes());
		MapRecord<byte[], byte[], byte[]> record = MapRecord.create("stream4".getBytes(), body);
		RecordId id1 = connection.streamCommands().xAdd(record, XAddOptions.none());
		RecordId id2 = connection.streamCommands().xAdd(record, XAddOptions.none());

		// Test xGroupCreate - create consumer group
		String groupCreated = connection.streamCommands().xGroupCreate("stream4".getBytes(), "group1",
				ReadOffset.from("0-0"));
		assertThat(groupCreated).isEqualTo("OK");

		// Test xGroupCreate with mkstream flag
		String groupCreatedMkstream = connection.streamCommands().xGroupCreate("stream5".getBytes(), "group2",
				ReadOffset.from("0-0"), true);
		assertThat(groupCreatedMkstream).isEqualTo("OK");

		// Test xInfoGroups - get consumer group info
		StreamInfo.XInfoGroups groups = connection.streamCommands().xInfoGroups("stream4".getBytes());
		assertThat(groups).isNotNull();
		assertThat(groups.size()).isEqualTo(1);

		// Test xInfoConsumers - get consumer info
		StreamInfo.XInfoConsumers consumers = connection.streamCommands().xInfoConsumers("stream4".getBytes(), "group1");
		assertThat(consumers).isNotNull();

		// Test xAck - acknowledge message
		Long ackResult = connection.streamCommands().xAck("stream4".getBytes(), "group1", id1);
		assertThat(ackResult).isGreaterThanOrEqualTo(0L);

		// Test xPending - get pending messages
		PendingMessagesSummary pending = connection.streamCommands().xPending("stream4".getBytes(), "group1");
		assertThat(pending).isNotNull();

		// Test xPending with options
		XPendingOptions pendingOptions = XPendingOptions.unbounded();
		PendingMessages pendingWithOptions = connection.streamCommands().xPending("stream4".getBytes(), "group1",
				pendingOptions);
		assertThat(pendingWithOptions).isNotNull();

		// Test xGroupDelConsumer - delete consumer
		Boolean delConsumerResult = connection.streamCommands().xGroupDelConsumer("stream4".getBytes(),
				Consumer.from("group1", "consumer1"));
		assertThat(delConsumerResult).isNotNull();

		// Test xGroupDestroy - destroy consumer group
		Boolean destroyResult = connection.streamCommands().xGroupDestroy("stream4".getBytes(), "group1");
		assertThat(destroyResult).isTrue();
	}

	@Test
	void streamClaimOperationsShouldWork() {
		// Add entry and create group
		Map<byte[], byte[]> body = Collections.singletonMap("field".getBytes(), "value".getBytes());
		MapRecord<byte[], byte[], byte[]> record = MapRecord.create("stream6".getBytes(), body);
		RecordId id = connection.streamCommands().xAdd(record, XAddOptions.none());
		connection.streamCommands().xGroupCreate("stream6".getBytes(), "group1", ReadOffset.from("0-0"));

		// Test xClaim - claim pending message
		List<ByteRecord> claimResult = connection.streamCommands().xClaim("stream6".getBytes(), "group1", "consumer1",
				XClaimOptions.minIdleMs(0).ids(id));
		assertThat(claimResult).isNotNull();

		// Test xClaimJustId - claim and return just IDs
		List<RecordId> claimJustIdResult = connection.streamCommands().xClaimJustId("stream6".getBytes(), "group1",
				"consumer2", XClaimOptions.minIdleMs(0).ids(id));
		assertThat(claimJustIdResult).isNotNull();
	}

	@Test
	void streamAdvancedOperationsShouldWork() {
		// Add entry
		Map<byte[], byte[]> body = Collections.singletonMap("field".getBytes(), "value".getBytes());
		MapRecord<byte[], byte[], byte[]> record = MapRecord.create("stream7".getBytes(), body);
		RecordId id = connection.streamCommands().xAdd(record, XAddOptions.none());
		connection.streamCommands().xGroupCreate("stream7".getBytes(), "group1", ReadOffset.from("0-0"));

		// Test xDelEx - delete with options
		XDelOptions delOptions = XDelOptions.defaults();
		List<RedisStreamCommands.StreamEntryDeletionResult> delExResult = connection.streamCommands()
				.xDelEx("stream7".getBytes(), delOptions, id);
		assertThat(delExResult).isNotNull();

		// Add another entry for xAckDel test
		RecordId id2 = connection.streamCommands().xAdd(record, XAddOptions.none());

		// Test xAckDel - acknowledge and delete
		List<RedisStreamCommands.StreamEntryDeletionResult> ackDelResult = connection.streamCommands()
				.xAckDel("stream7".getBytes(), "group1", delOptions, id2);
		assertThat(ackDelResult).isNotNull();
	}

	@Test
	void transactionShouldExecuteAtomically() {
		// Set up initial state
		Map<byte[], byte[]> body = Collections.singletonMap("field".getBytes(), "value".getBytes());
		MapRecord<byte[], byte[], byte[]> record = MapRecord.create("txStream".getBytes(), body);
		RecordId id = connection.streamCommands().xAdd(record, XAddOptions.none());

		// Execute multiple stream operations in a transaction
		connection.multi();
		connection.streamCommands().xAdd(record, XAddOptions.none());
		connection.streamCommands().xLen("txStream".getBytes());
		connection.streamCommands().xRange("txStream".getBytes(), Range.unbounded(), Limit.unlimited());
		connection.streamCommands().xDel("txStream".getBytes(), id);
		List<Object> results = connection.exec();

		// Verify all commands executed
		assertThat(results).hasSize(4);
		assertThat(results.get(0)).isInstanceOf(RecordId.class); // xAdd result
		assertThat(results.get(1)).isEqualTo(2L); // xLen result
		assertThat(results.get(2)).isInstanceOf(List.class); // xRange result
		assertThat(results.get(3)).isEqualTo(1L); // xDel result
	}

	@Test
	void pipelineShouldExecuteMultipleCommands() {
		// Set up initial state
		Map<byte[], byte[]> body = Collections.singletonMap("field".getBytes(), "value".getBytes());
		MapRecord<byte[], byte[], byte[]> record = MapRecord.create("pipeStream".getBytes(), body);
		RecordId id1 = connection.streamCommands().xAdd(record, XAddOptions.none());

		// Execute multiple stream operations in pipeline
		connection.openPipeline();
		connection.streamCommands().xAdd(record, XAddOptions.none());
		connection.streamCommands().xLen("pipeStream".getBytes());
		connection.streamCommands().xRange("pipeStream".getBytes(), Range.unbounded(), Limit.unlimited());
		connection.streamCommands().xTrim("pipeStream".getBytes(), 1);
		List<Object> results = connection.closePipeline();

		// Verify all command results
		assertThat(results).hasSize(4);
		assertThat(results.get(0)).isInstanceOf(RecordId.class); // xAdd result
		assertThat(results.get(1)).isEqualTo(2L); // xLen result
		@SuppressWarnings("unchecked")
		List<ByteRecord> rangeResult = (List<ByteRecord>) results.get(2);
		assertThat(rangeResult).hasSize(2); // xRange result
		assertThat((Long) results.get(3)).isGreaterThanOrEqualTo(0L); // xTrim result
	}
}
