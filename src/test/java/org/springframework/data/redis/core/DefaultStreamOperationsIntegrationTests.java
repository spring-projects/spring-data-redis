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
package org.springframework.data.redis.core;

import static org.assertj.core.api.Assertions.*;
import static org.assertj.core.api.Assumptions.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.MethodSource;

import org.springframework.data.domain.Range;
import org.springframework.data.domain.Range.Bound;
import org.springframework.data.redis.ObjectFactory;
import org.springframework.data.redis.Person;
import org.springframework.data.redis.connection.Limit;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStreamCommands;
import org.springframework.data.redis.connection.RedisStreamCommands.StreamEntryDeletionResult;
import org.springframework.data.redis.connection.RedisStreamCommands.TrimOptions;
import org.springframework.data.redis.connection.RedisStreamCommands.XAddOptions;
import org.springframework.data.redis.connection.RedisStreamCommands.XDelOptions;
import org.springframework.data.redis.connection.RedisStreamCommands.XTrimOptions;
import org.springframework.data.redis.connection.RedisStreamCommands.StreamDeletionPolicy;
import org.springframework.data.redis.connection.jedis.extension.JedisConnectionFactoryExtension;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.lettuce.extension.LettuceConnectionFactoryExtension;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ObjectRecord;
import org.springframework.data.redis.connection.stream.PendingMessages;
import org.springframework.data.redis.connection.stream.PendingMessagesSummary;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.test.condition.EnabledOnCommand;
import org.springframework.data.redis.test.condition.EnabledOnRedisDriver;
import org.springframework.data.redis.test.condition.EnabledOnRedisVersion;
import org.springframework.data.redis.test.condition.RedisDetector;
import org.springframework.data.redis.test.extension.RedisCluster;
import org.springframework.data.redis.test.extension.RedisStandalone;

/**
 * Integration test of {@link DefaultStreamOperations}
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @author Marcin Zielinski
 * @author jinkshower
 */
@ParameterizedClass
@MethodSource("testParams")
@EnabledOnCommand("XADD")
public class DefaultStreamOperationsIntegrationTests<K, HK, HV> {

	private final RedisTemplate<K, ?> redisTemplate;
	private final @EnabledOnRedisDriver.DriverQualifier RedisConnectionFactory connectionFactory;

	private final ObjectFactory<K> keyFactory;
	private final ObjectFactory<HK> hashKeyFactory;
	private final ObjectFactory<HV> hashValueFactory;
	private final StreamOperations<K, HK, HV> streamOps;

	public DefaultStreamOperationsIntegrationTests(RedisTemplate<K, ?> redisTemplate, ObjectFactory<K> keyFactory,
			ObjectFactory<?> objectFactory) {

		this.redisTemplate = redisTemplate;
		this.connectionFactory = redisTemplate.getRequiredConnectionFactory();
		this.keyFactory = keyFactory;
		this.hashKeyFactory = (ObjectFactory<HK>) keyFactory;
		this.hashValueFactory = (ObjectFactory<HV>) objectFactory;
		streamOps = redisTemplate.opsForStream();
	}

	public static Collection<Object[]> testParams() {

		List<Object[]> params = new ArrayList<>();
		params.addAll(AbstractOperationsTestParams
				.testParams(JedisConnectionFactoryExtension.getConnectionFactory(RedisStandalone.class)));

		if (RedisDetector.isClusterAvailable()) {
			params.addAll(AbstractOperationsTestParams
					.testParams(JedisConnectionFactoryExtension.getConnectionFactory(RedisCluster.class)));
		}

		params.addAll(AbstractOperationsTestParams
				.testParams(LettuceConnectionFactoryExtension.getConnectionFactory(RedisStandalone.class)));

		if (RedisDetector.isClusterAvailable()) {
			params.addAll(AbstractOperationsTestParams
					.testParams(LettuceConnectionFactoryExtension.getConnectionFactory(RedisCluster.class)));
		}

		return params;
	}

	@BeforeEach
	void setUp() {

		redisTemplate.execute((RedisCallback<Object>) connection -> {
			connection.flushDb();
			return null;
		});
	}

	@Test // DATAREDIS-864
	void addShouldAddMessage() {

		K key = keyFactory.instance();
		HK hashKey = hashKeyFactory.instance();
		HV value = hashValueFactory.instance();

		RecordId messageId = streamOps.add(key, Collections.singletonMap(hashKey, value));

		List<MapRecord<K, HK, HV>> messages = streamOps.range(key, Range.unbounded());

		assertThat(messages).hasSize(1);

		MapRecord<K, HK, HV> message = messages.get(0);

		assertThat(message.getId()).isEqualTo(messageId);
		assertThat(message.getStream()).isEqualTo(key);

		if (!(key instanceof byte[] || value instanceof byte[])) {
			assertThat(message.getValue()).containsEntry(hashKey, value);
		}
	}

	@Test
	// DATAREDIS-864
	void addShouldAddReadSimpleMessage() {

		K key = keyFactory.instance();
		HV value = hashValueFactory.instance();

		RecordId messageId = streamOps.add(StreamRecords.objectBacked(value).withStreamKey(key));

		List<ObjectRecord<K, HV>> messages = streamOps.range((Class<HV>) value.getClass(), key, Range.unbounded());

		assertThat(messages).hasSize(1);

		ObjectRecord<K, HV> message = messages.get(0);

		assertThat(message.getId()).isEqualTo(messageId);
		assertThat(message.getStream()).isEqualTo(key);

		assertThat(message.getValue()).isEqualTo(value);
	}

	@Test // GH-2915
	void addMaxLenShouldLimitMessagesSize() {

		K key = keyFactory.instance();
		HK hashKey = hashKeyFactory.instance();
		HV value = hashValueFactory.instance();

		streamOps.add(key, Collections.singletonMap(hashKey, value));

		HV newValue = hashValueFactory.instance();

		XAddOptions options = XAddOptions.maxlen(1).approximateTrimming(false);

		RecordId messageId = streamOps.add(key, Collections.singletonMap(hashKey, newValue), options);

		List<MapRecord<K, HK, HV>> messages = streamOps.range(key, Range.unbounded());

		assertThat(messages).hasSize(1);

		MapRecord<K, HK, HV> message = messages.get(0);

		assertThat(message.getId()).isEqualTo(messageId);
		assertThat(message.getStream()).isEqualTo(key);

		if (!(key instanceof byte[] || value instanceof byte[])) {
			assertThat(message.getValue()).containsEntry(hashKey, newValue);
		}
	}

	@Test // GH-2915
	void addMaxLenShouldLimitSimpleMessagesSize() {

		K key = keyFactory.instance();
		HV value = hashValueFactory.instance();

		streamOps.add(StreamRecords.objectBacked(value).withStreamKey(key));

		HV newValue = hashValueFactory.instance();

		XAddOptions options = XAddOptions.maxlen(1).approximateTrimming(false);

		RecordId messageId = streamOps.add(StreamRecords.objectBacked(newValue).withStreamKey(key), options);

		List<ObjectRecord<K, HV>> messages = streamOps.range((Class<HV>) value.getClass(), key, Range.unbounded());

		assertThat(messages).hasSize(1);

		ObjectRecord<K, HV> message = messages.get(0);

		assertThat(message.getId()).isEqualTo(messageId);
		assertThat(message.getStream()).isEqualTo(key);

		assertThat(message.getValue()).isEqualTo(newValue);
	}

	@Test // GH-2915
	void addMinIdShouldEvictLowerIdMessages() {

		K key = keyFactory.instance();
		HK hashKey = hashKeyFactory.instance();
		HV value = hashValueFactory.instance();

		streamOps.add(key, Collections.singletonMap(hashKey, value));
		RecordId messageId1 = streamOps.add(key, Collections.singletonMap(hashKey, value));

		XAddOptions options = XAddOptions.none().minId(messageId1);

		RecordId messageId2 = streamOps.add(key, Collections.singletonMap(hashKey, value), options);

		List<MapRecord<K, HK, HV>> messages = streamOps.range(key, Range.unbounded());

		assertThat(messages).hasSize(2);

		MapRecord<K, HK, HV> message1 = messages.get(0);

		assertThat(message1.getId()).isEqualTo(messageId1);
		assertThat(message1.getStream()).isEqualTo(key);

		MapRecord<K, HK, HV> message2 = messages.get(1);

		assertThat(message2.getId()).isEqualTo(messageId2);
		assertThat(message2.getStream()).isEqualTo(key);

		if (!(key instanceof byte[] || value instanceof byte[])) {
			assertThat(message1.getValue()).containsEntry(hashKey, value);
			assertThat(message2.getValue()).containsEntry(hashKey, value);
		}
	}

	@Test // GH-2915
	void addMinIdShouldEvictLowerIdSimpleMessages() {

		K key = keyFactory.instance();
		HV value = hashValueFactory.instance();

		streamOps.add(StreamRecords.objectBacked(value).withStreamKey(key));
		RecordId messageId1 = streamOps.add(StreamRecords.objectBacked(value).withStreamKey(key));

		XAddOptions options = XAddOptions.none().minId(messageId1);

		RecordId messageId2 = streamOps.add(StreamRecords.objectBacked(value).withStreamKey(key), options);

		List<ObjectRecord<K, HV>> messages = streamOps.range((Class<HV>) value.getClass(), key, Range.unbounded());

		assertThat(messages).hasSize(2);

		ObjectRecord<K, HV> message1 = messages.get(0);

		assertThat(message1.getId()).isEqualTo(messageId1);
		assertThat(message1.getStream()).isEqualTo(key);
		assertThat(message1.getValue()).isEqualTo(value);

		ObjectRecord<K, HV> message2 = messages.get(1);

		assertThat(message2.getId()).isEqualTo(messageId2);
		assertThat(message2.getStream()).isEqualTo(key);
		assertThat(message2.getValue()).isEqualTo(value);
	}

	@Test // GH-2915
	void addMakeNoStreamShouldNotCreateStreamWhenNoStreamExists() {

		K key = keyFactory.instance();
		HV value = hashValueFactory.instance();

		XAddOptions options = XAddOptions.makeNoStream();

		streamOps.add(StreamRecords.objectBacked(value).withStreamKey(key), options);

		assertThat(streamOps.size(key)).isZero();
		assertThat(streamOps.range(key, Range.unbounded())).isEmpty();
	}

	@Test // GH-2915
	void addMakeNoStreamShouldCreateStreamWhenStreamExists() {

		K key = keyFactory.instance();
		HV value = hashValueFactory.instance();

		streamOps.add(StreamRecords.objectBacked(value).withStreamKey(key));

		XAddOptions options = XAddOptions.makeNoStream();

		streamOps.add(StreamRecords.objectBacked(value).withStreamKey(key), options);

		assertThat(streamOps.size(key)).isEqualTo(2);
		assertThat(streamOps.range(key, Range.unbounded())).hasSize(2);
	}

	@Test // GH-3232
	void addWithLimitShouldHonorApproximateTrimming() {

		K key = keyFactory.instance();
		HV value = hashValueFactory.instance();

		XAddOptions options = XAddOptions.trim(TrimOptions.maxLen(100).approximate().limit(50));

		// Add multiple messages with limit
		for (int i = 0; i < 5; i++) {
			streamOps.add(StreamRecords.objectBacked(value).withStreamKey(key), options);
		}

		assertThat(streamOps.size(key)).isGreaterThan(0L);
	}

	@Test // GH-3232
	void addWithExactTrimmingShouldTrimExactly() {

		K key = keyFactory.instance();
		HV value = hashValueFactory.instance();

		XAddOptions options = XAddOptions.trim(TrimOptions.maxLen(2).exact());

		// Add 3 messages with exact trimming to maxlen=2
		streamOps.add(StreamRecords.objectBacked(value).withStreamKey(key), options);
		streamOps.add(StreamRecords.objectBacked(value).withStreamKey(key), options);
		streamOps.add(StreamRecords.objectBacked(value).withStreamKey(key), options);

		// Should have exactly 2 entries
		assertThat(streamOps.size(key)).isEqualTo(2);
	}

	@Test // GH-3232
	void addWithDeletionPolicyShouldApplyPolicy() {

		K key = keyFactory.instance();
		HV value = hashValueFactory.instance();

		XAddOptions options = XAddOptions.trim(TrimOptions.maxLen(5).approximate()
				.deletionPolicy(StreamDeletionPolicy.delete()));

		// Add multiple messages with deletion policy
		for (int i = 0; i < 3; i++) {
			streamOps.add(StreamRecords.objectBacked(value).withStreamKey(key), options);
		}

		assertThat(streamOps.size(key)).isGreaterThan(0L);
	}

	@Test // GH-3232
	void trimShouldTrimStreamWithMaxlen() {

		K key = keyFactory.instance();
		HV value = hashValueFactory.instance();

		// Add 10 messages
		for (int i = 0; i < 10; i++) {
			streamOps.add(StreamRecords.objectBacked(value).withStreamKey(key));
		}

		assertThat(streamOps.size(key)).isEqualTo(10L);

		// Trim to 5 entries
		Long trimmed = streamOps.trim(key, XTrimOptions.trim(TrimOptions.maxLen(5)));

		assertThat(trimmed).isEqualTo(5L); // 5 entries removed
		assertThat(streamOps.size(key)).isEqualTo(5L); // 5 entries remaining
	}

	@Test // GH-3232
	void trimShouldTrimStreamWithMinId() {

		K key = keyFactory.instance();
		HV value = hashValueFactory.instance();

		// Add 5 messages and capture their IDs
		RecordId id1 = streamOps.add(StreamRecords.objectBacked(value).withStreamKey(key));
		RecordId id2 = streamOps.add(StreamRecords.objectBacked(value).withStreamKey(key));
		RecordId id3 = streamOps.add(StreamRecords.objectBacked(value).withStreamKey(key));
		RecordId id4 = streamOps.add(StreamRecords.objectBacked(value).withStreamKey(key));
		RecordId id5 = streamOps.add(StreamRecords.objectBacked(value).withStreamKey(key));

		assertThat(streamOps.size(key)).isEqualTo(5L);

		// Trim using MINID - keep only entries with ID >= id3
		Long trimmed = streamOps.trim(key, XTrimOptions.trim(TrimOptions.minId(id3)));

		assertThat(trimmed).isEqualTo(2L); // 2 entries removed (id1, id2)
		assertThat(streamOps.size(key)).isEqualTo(3L); // 3 entries remaining (id3, id4, id5)
	}

	@Test // GH-3232
	void trimShouldHonorApproximateTrimming() {

		K key = keyFactory.instance();
		HV value = hashValueFactory.instance();

		// Add 100 messages
		for (int i = 0; i < 100; i++) {
			streamOps.add(StreamRecords.objectBacked(value).withStreamKey(key));
		}

		assertThat(streamOps.size(key)).isEqualTo(100L);

		// Trim with approximate trimming
		streamOps.trim(key, XTrimOptions.trim(TrimOptions.maxLen(50).approximate()));

		// With approximate trimming, the result may not be exact but should be around 50
		assertThat(streamOps.size(key)).isGreaterThanOrEqualTo(50L).isLessThanOrEqualTo(100L);
	}

	@Test // GH-3232
	void trimShouldHonorExactTrimming() {

		K key = keyFactory.instance();
		HV value = hashValueFactory.instance();

		// Add 10 messages
		for (int i = 0; i < 10; i++) {
			streamOps.add(StreamRecords.objectBacked(value).withStreamKey(key));
		}

		assertThat(streamOps.size(key)).isEqualTo(10L);

		// Trim with exact trimming
		Long trimmed = streamOps.trim(key, XTrimOptions.trim(TrimOptions.maxLen(5).exact()));

		assertThat(trimmed).isEqualTo(5L); // 5 entries removed
		assertThat(streamOps.size(key)).isEqualTo(5L); // Exactly 5 entries remaining
	}

	@Test // GH-3232
	void trimShouldHonorLimit() {

		K key = keyFactory.instance();
		HV value = hashValueFactory.instance();

		// Add 100 messages
		for (int i = 0; i < 100; i++) {
			streamOps.add(StreamRecords.objectBacked(value).withStreamKey(key));
		}

		assertThat(streamOps.size(key)).isEqualTo(100L);

		// Trim with LIMIT to control trimming effort
		streamOps.trim(key, XTrimOptions.trim(TrimOptions.maxLen(50).approximate().limit(10)));

		// With LIMIT, trimming may not be exact
		assertThat(streamOps.size(key)).isGreaterThanOrEqualTo(50L).isLessThanOrEqualTo(100L);
	}

	@Test // GH-3232
	@EnabledOnRedisVersion("8.2") // Deletion policy requires Redis 8.2+
	void trimShouldHonorDeletionPolicy() {

		K key = keyFactory.instance();
		HV value = hashValueFactory.instance();

		// Add 10 messages
		for (int i = 0; i < 10; i++) {
			streamOps.add(StreamRecords.objectBacked(value).withStreamKey(key));
		}

		assertThat(streamOps.size(key)).isEqualTo(10L);

		// Trim with deletion policy
		streamOps.trim(key, XTrimOptions.trim(TrimOptions.maxLen(5).approximate()
				.deletionPolicy(StreamDeletionPolicy.delete())));

		// Verify trimming was applied
		assertThat(streamOps.size(key)).isGreaterThan(0L).isLessThanOrEqualTo(10L);
	}

	@Test // DATAREDIS-864
	void simpleMessageReadWriteSymmetry() {

		K key = keyFactory.instance();
		HV value = hashValueFactory.instance();

		assumeThat(value).isNotInstanceOf(Person.class);

		RecordId messageId = streamOps.add(StreamRecords.objectBacked(value).withStreamKey(key));

		List<MapRecord<K, HK, HV>> messages = streamOps.range(key, Range.unbounded());

		assertThat(messages).hasSize(1);

		MapRecord<K, HK, HV> message = messages.get(0);

		assertThat(message.getId()).isEqualTo(messageId);
		assertThat(message.getStream()).isEqualTo(key);

		assertThat(message.getValue().values()).containsExactly(value);
	}

	@Test // DATAREDIS-864
	void rangeShouldReportMessages() {

		K key = keyFactory.instance();
		HK hashKey = hashKeyFactory.instance();
		HV value = hashValueFactory.instance();

		RecordId messageId1 = streamOps.add(key, Collections.singletonMap(hashKey, value));
		RecordId messageId2 = streamOps.add(key, Collections.singletonMap(hashKey, value));

		List<MapRecord<K, HK, HV>> messages = streamOps.range(key,
				Range.from(Bound.inclusive(messageId1.getValue())).to(Bound.inclusive(messageId2.getValue())),
				Limit.limit().count(1));

		assertThat(messages).hasSize(1);

		MapRecord<K, HK, HV> message = messages.get(0);

		assertThat(message.getId()).isEqualTo(messageId1);
	}

	@Test // GH-2044
	@EnabledOnRedisVersion("6.2")
	void exclusiveRangeShouldReportMessages() {

		K key = keyFactory.instance();
		HK hashKey = hashKeyFactory.instance();
		HV value = hashValueFactory.instance();

		RecordId messageId1 = streamOps.add(key, Collections.singletonMap(hashKey, value));
		RecordId messageId2 = streamOps.add(key, Collections.singletonMap(hashKey, value));

		List<MapRecord<K, HK, HV>> messages = streamOps.range(key,
				Range.from(Bound.exclusive(messageId1.getValue())).to(Bound.inclusive(messageId2.getValue())));

		assertThat(messages).hasSize(1).extracting(MapRecord::getId).contains(messageId2);

		messages = streamOps.range(key,
				Range.from(Bound.inclusive(messageId1.getValue())).to(Bound.exclusive(messageId2.getValue())));

		assertThat(messages).hasSize(1).extracting(MapRecord::getId).contains(messageId1);
	}

	@Test // DATAREDIS-864
	void reverseRangeShouldReportMessages() {

		K key = keyFactory.instance();
		HK hashKey = hashKeyFactory.instance();
		HV value = hashValueFactory.instance();

		RecordId messageId1 = streamOps.add(key, Collections.singletonMap(hashKey, value));
		RecordId messageId2 = streamOps.add(key, Collections.singletonMap(hashKey, value));

		List<MapRecord<K, HK, HV>> messages = streamOps.reverseRange(key, Range.unbounded());

		assertThat(messages).hasSize(2).extracting("id").containsSequence(messageId2, messageId1);
	}

	@Test // GH-2044
	@EnabledOnRedisVersion("6.2")
	void exclusiveReverseRangeShouldReportMessages() {

		K key = keyFactory.instance();
		HK hashKey = hashKeyFactory.instance();
		HV value = hashValueFactory.instance();

		RecordId messageId1 = streamOps.add(key, Collections.singletonMap(hashKey, value));
		RecordId messageId2 = streamOps.add(key, Collections.singletonMap(hashKey, value));
		RecordId messageId3 = streamOps.add(key, Collections.singletonMap(hashKey, value));

		List<MapRecord<K, HK, HV>> messages = streamOps.reverseRange(key,
				Range.from(Bound.exclusive(messageId1.getValue())).to(Bound.inclusive(messageId3.getValue())));

		assertThat(messages).hasSize(2).extracting(MapRecord::getId).containsSequence(messageId3, messageId2);

		messages = streamOps.reverseRange(key,
				Range.from(Bound.inclusive(messageId1.getValue())).to(Bound.exclusive(messageId3.getValue())));

		assertThat(messages).hasSize(2).extracting(MapRecord::getId).containsSequence(messageId2, messageId1);
	}

	@Test // DATAREDIS-864
	void reverseRangeShouldConvertSimpleMessages() {

		K key = keyFactory.instance();
		HV value = hashValueFactory.instance();

		RecordId messageId1 = streamOps.add(StreamRecords.objectBacked(value).withStreamKey(key));
		RecordId messageId2 = streamOps.add(StreamRecords.objectBacked(value).withStreamKey(key));

		List<ObjectRecord<K, HV>> messages = streamOps.reverseRange((Class<HV>) value.getClass(), key, Range.unbounded());

		assertThat(messages).hasSize(2).extracting("id").containsSequence(messageId2, messageId1);

		ObjectRecord<K, HV> message = messages.get(0);

		assertThat(message.getId()).isEqualTo(messageId2);
		assertThat(message.getStream()).isEqualTo(key);

		assertThat(message.getValue()).isEqualTo(value);
	}

	@Test // DATAREDIS-864
	void readShouldReadMessage() {

		K key = keyFactory.instance();
		HK hashKey = hashKeyFactory.instance();
		HV value = hashValueFactory.instance();

		RecordId messageId = streamOps.add(key, Collections.singletonMap(hashKey, value));

		List<MapRecord<K, HK, HV>> messages = streamOps.read(StreamOffset.create(key, ReadOffset.from("0-0")));

		assertThat(messages).hasSize(1);

		MapRecord<K, HK, HV> message = messages.get(0);

		assertThat(message.getId()).isEqualTo(messageId);
		assertThat(message.getStream()).isEqualTo(key);

		if (!(key instanceof byte[] || value instanceof byte[])) {
			assertThat(message.getValue()).containsEntry(hashKey, value);
		}
	}

	@Test // DATAREDIS-864
	void readShouldReadSimpleMessage() {

		K key = keyFactory.instance();
		HV value = hashValueFactory.instance();

		RecordId messageId1 = streamOps.add(StreamRecords.objectBacked(value).withStreamKey(key));
		streamOps.add(StreamRecords.objectBacked(value).withStreamKey(key));

		List<ObjectRecord<K, HV>> messages = streamOps.read((Class<HV>) value.getClass(),
				StreamOffset.create(key, ReadOffset.from("0-0")));

		assertThat(messages).hasSize(2);

		ObjectRecord<K, HV> message = messages.get(0);

		assertThat(message.getId()).isEqualTo(messageId1);
		assertThat(message.getStream()).isEqualTo(key);

		assertThat(message.getValue()).isEqualTo(value);
	}

	@Test // DATAREDIS-864
	void readShouldReadMessages() {

		K key = keyFactory.instance();
		HK hashKey = hashKeyFactory.instance();
		HV value = hashValueFactory.instance();

		streamOps.add(key, Collections.singletonMap(hashKey, value));
		streamOps.add(key, Collections.singletonMap(hashKey, value));

		List<MapRecord<K, HK, HV>> messages = streamOps.read(StreamReadOptions.empty().count(2),
				StreamOffset.create(key, ReadOffset.from("0-0")));

		assertThat(messages).hasSize(2);
	}

	@Test // DATAREDIS-864
	void readShouldReadMessageWithConsumerGroup() {

		K key = keyFactory.instance();
		HK hashKey = hashKeyFactory.instance();
		HV value = hashValueFactory.instance();

		RecordId messageId = streamOps.add(key, Collections.singletonMap(hashKey, value));
		streamOps.createGroup(key, ReadOffset.from("0-0"), "my-group");

		List<MapRecord<K, HK, HV>> messages = streamOps.read(Consumer.from("my-group", "my-consumer"),
				StreamOffset.create(key, ReadOffset.lastConsumed()));

		assertThat(messages).hasSize(1);

		MapRecord<K, HK, HV> message = messages.get(0);

		assertThat(message.getId()).isEqualTo(messageId);
		assertThat(message.getStream()).isEqualTo(key);

		if (!(key instanceof byte[] || value instanceof byte[])) {
			assertThat(message.getValue()).containsEntry(hashKey, value);
		}
	}

	@Test // DATAREDIS-864
	void sizeShouldReportStreamSize() {

		K key = keyFactory.instance();
		HK hashKey = hashKeyFactory.instance();
		HV value = hashValueFactory.instance();

		streamOps.add(key, Collections.singletonMap(hashKey, value));
		assertThat(streamOps.size(key)).isEqualTo(1);

		streamOps.add(key, Collections.singletonMap(hashKey, value));
		assertThat(streamOps.size(key)).isEqualTo(2);
	}

	@Test // DATAREDIS-1084
	void pendingShouldReadMessageSummary() {
		// XPENDING summary not supported by Jedis
		assumeThat(redisTemplate.getRequiredConnectionFactory()).isInstanceOf(LettuceConnectionFactory.class);

		K key = keyFactory.instance();
		HK hashKey = hashKeyFactory.instance();
		HV value = hashValueFactory.instance();

		RecordId messageId = streamOps.add(key, Collections.singletonMap(hashKey, value));
		streamOps.createGroup(key, ReadOffset.from("0-0"), "my-group");

		streamOps.read(Consumer.from("my-group", "my-consumer"), StreamOffset.create(key, ReadOffset.lastConsumed()));

		PendingMessagesSummary pending = streamOps.pending(key, "my-group");

		assertThat(pending.getTotalPendingMessages()).isOne();
		assertThat(pending.getGroupName()).isEqualTo("my-group");
	}

	@Test // DATAREDIS-1084
	void pendingShouldReadMessageDetails() {

		K key = keyFactory.instance();
		HK hashKey = hashKeyFactory.instance();
		HV value = hashValueFactory.instance();

		RecordId messageId = streamOps.add(key, Collections.singletonMap(hashKey, value));
		streamOps.createGroup(key, ReadOffset.from("0-0"), "my-group");

		streamOps.read(Consumer.from("my-group", "my-consumer"), StreamOffset.create(key, ReadOffset.lastConsumed()));

		PendingMessages pending = streamOps.pending(key, "my-group", Range.unbounded(), 10L);

		assertThat(pending).hasSize(1);
		assertThat(pending.get(0).getGroupName()).isEqualTo("my-group");
		assertThat(pending.get(0).getConsumerName()).isEqualTo("my-consumer");
		assertThat(pending.get(0).getTotalDeliveryCount()).isOne();
	}

	@Test // GH-2465
	void claimShouldReadMessageDetails() {

		K key = keyFactory.instance();
		HK hashKey = hashKeyFactory.instance();
		HV value = hashValueFactory.instance();

		RecordId messageId = streamOps.add(key, Collections.singletonMap(hashKey, value));
		streamOps.createGroup(key, ReadOffset.from("0-0"), "my-group");
		streamOps.read(Consumer.from("my-group", "name"), StreamOffset.create(key, ReadOffset.lastConsumed()));

		List<MapRecord<K, HK, HV>> messages = streamOps.claim(key, "my-group", "new-owner", Duration.ZERO, messageId);

		assertThat(messages).hasSize(1);

		MapRecord<K, HK, HV> message = messages.get(0);

		assertThat(message.getId()).isEqualTo(messageId);
		assertThat(message.getStream()).isEqualTo(key);

		if (!(key instanceof byte[] || value instanceof byte[])) {
			assertThat(message.getValue()).containsEntry(hashKey, value);
		}
	}

	@Nested // GH-3232
	@EnabledOnCommand("XDELEX")
	class DeleteWithOptions {

		@Test
		void shouldDeleteEntries() {

			K key = keyFactory.instance();
			HK hashKey = hashKeyFactory.instance();
			HV value = hashValueFactory.instance();

			RecordId messageId1 = streamOps.add(key, Collections.singletonMap(hashKey, value));
			RecordId messageId2 = streamOps.add(key, Collections.singletonMap(hashKey, value));
			RecordId messageId3 = streamOps.add(key, Collections.singletonMap(hashKey, value));

			assertThat(streamOps.size(key)).isEqualTo(3L);

			XDelOptions options = XDelOptions.defaults();

			List<StreamEntryDeletionResult> results = streamOps.deleteWithOptions(key, options, messageId1, messageId2);

			assertThat(results).hasSize(2);
			assertThat(results.get(0)).isEqualTo(StreamEntryDeletionResult.DELETED);
			assertThat(results.get(1)).isEqualTo(StreamEntryDeletionResult.DELETED);

			assertThat(streamOps.size(key)).isEqualTo(1L);
		}

		@Test
		void usingStringIdsShouldDeleteEntries() {

			K key = keyFactory.instance();
			HK hashKey = hashKeyFactory.instance();
			HV value = hashValueFactory.instance();

			RecordId messageId1 = streamOps.add(key, Collections.singletonMap(hashKey, value));
			RecordId messageId2 = streamOps.add(key, Collections.singletonMap(hashKey, value));

			assertThat(streamOps.size(key)).isEqualTo(2L);

			XDelOptions options = XDelOptions.defaults();

			List<StreamEntryDeletionResult> results = streamOps.deleteWithOptions(key, options, messageId1, messageId2);

			assertThat(results).hasSize(2);
			assertThat(streamOps.size(key)).isEqualTo(0L);
		}

		@Test
		void usingRecordShouldDeleteEntry() {

			K key = keyFactory.instance();
			HK hashKey = hashKeyFactory.instance();
			HV value = hashValueFactory.instance();

			RecordId messageId = streamOps.add(key, Collections.singletonMap(hashKey, value));

			assertThat(streamOps.size(key)).isEqualTo(1L);

			MapRecord<K, HK, HV> record = StreamRecords.newRecord().in(key).withId(messageId)
					.ofMap(Collections.singletonMap(hashKey, value));
			XDelOptions options = XDelOptions.defaults();

			List<StreamEntryDeletionResult> results = streamOps.deleteWithOptions(record, options);

			assertThat(results).hasSize(1);
			assertThat(results.get(0)).isEqualTo(StreamEntryDeletionResult.DELETED);

			assertThat(streamOps.size(key)).isEqualTo(0L);
		}
	}

	@Nested // GH-3232
	@EnabledOnCommand("XACKDEL")
	class AcknowledgeAndDelete {

		@Test
		void shouldAcknowledgeAndDeleteEntries() {

			K key = keyFactory.instance();
			HK hashKey = hashKeyFactory.instance();
			HV value = hashValueFactory.instance();

			RecordId messageId1 = streamOps.add(key, Collections.singletonMap(hashKey, value));
			RecordId messageId2 = streamOps.add(key, Collections.singletonMap(hashKey, value));

			streamOps.createGroup(key, ReadOffset.from("0-0"), "my-group");

			streamOps.read(Consumer.from("my-group", "my-consumer"), StreamOffset.create(key, ReadOffset.lastConsumed()));

			XDelOptions options = XDelOptions.deletionPolicy(StreamDeletionPolicy.removeAcknowledged());

			List<StreamEntryDeletionResult> results = streamOps.acknowledgeAndDelete(key, "my-group", options, messageId1,
					messageId2);

			assertThat(results).hasSize(2);
			assertThat(results.get(0)).isEqualTo(StreamEntryDeletionResult.DELETED);
			assertThat(results.get(1)).isEqualTo(StreamEntryDeletionResult.DELETED);
		}

		@Test
		void usingStringIdsShouldWork() {

			K key = keyFactory.instance();
			HK hashKey = hashKeyFactory.instance();
			HV value = hashValueFactory.instance();

			RecordId messageId1 = streamOps.add(key, Collections.singletonMap(hashKey, value));
			RecordId messageId2 = streamOps.add(key, Collections.singletonMap(hashKey, value));

			streamOps.createGroup(key, ReadOffset.from("0-0"), "my-group");

			streamOps.read(Consumer.from("my-group", "my-consumer"), StreamOffset.create(key, ReadOffset.lastConsumed()));

			XDelOptions options = XDelOptions.deletionPolicy(StreamDeletionPolicy.removeAcknowledged());

			List<StreamEntryDeletionResult> results = streamOps.acknowledgeAndDelete(key, "my-group", options, messageId1,
					messageId2);

			assertThat(results).hasSize(2);
		}

		@Test
		void usingRecordShouldWork() {

			K key = keyFactory.instance();
			HK hashKey = hashKeyFactory.instance();
			HV value = hashValueFactory.instance();

			RecordId messageId = streamOps.add(key, Collections.singletonMap(hashKey, value));

			streamOps.createGroup(key, ReadOffset.from("0-0"), "my-group");

			streamOps.read(Consumer.from("my-group", "my-consumer"), StreamOffset.create(key, ReadOffset.lastConsumed()));

			MapRecord<K, HK, HV> record = StreamRecords.newRecord().in(key).withId(messageId)
					.ofMap(Collections.singletonMap(hashKey, value));
			XDelOptions options = XDelOptions.deletionPolicy(RedisStreamCommands.StreamDeletionPolicy.removeAcknowledged());

			List<StreamEntryDeletionResult> results = streamOps.acknowledgeAndDelete("my-group", record, options);

			assertThat(results).hasSize(1);
			assertThat(results.get(0)).isEqualTo(StreamEntryDeletionResult.DELETED);
		}
	}
}
