/*
 * Copyright 2018-2019 the original author or authors.
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
package org.springframework.data.redis.core;

import static org.assertj.core.api.Assertions.*;
import static org.junit.Assume.*;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.assertj.core.api.Assumptions;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.springframework.data.domain.Range;
import org.springframework.data.domain.Range.Bound;
import org.springframework.data.redis.ConnectionFactoryTracker;
import org.springframework.data.redis.ObjectFactory;
import org.springframework.data.redis.Person;
import org.springframework.data.redis.RedisTestProfileValueSource;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ObjectRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.data.redis.connection.RedisZSetCommands.Limit;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;

/**
 * Integration test of {@link DefaultStreamOperations}
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 */
@RunWith(Parameterized.class)
public class DefaultStreamOperationsTests<K, HK, HV> {

	private RedisTemplate<K, ?> redisTemplate;

	private ObjectFactory<K> keyFactory;

	private ObjectFactory<HK> hashKeyFactory;

	private ObjectFactory<HV> hashValueFactory;

	private StreamOperations<K, HK, HV> streamOps;

	public DefaultStreamOperationsTests(RedisTemplate<K, ?> redisTemplate, ObjectFactory<K> keyFactory,
			ObjectFactory<?> objectFactory) {

		// Currently, only Lettuce supports Redis Streams.
		// See https://github.com/xetorthio/jedis/issues/1820
		assumeTrue(redisTemplate.getConnectionFactory() instanceof LettuceConnectionFactory);

		assumeTrue(RedisTestProfileValueSource.matches("redisVersion", "5.0"));

		this.redisTemplate = redisTemplate;
		this.keyFactory = keyFactory;
		this.hashKeyFactory = (ObjectFactory<HK>) keyFactory;
		this.hashValueFactory = (ObjectFactory<HV>) objectFactory;

		ConnectionFactoryTracker.add(redisTemplate.getConnectionFactory());
	}

	@Parameters
	public static Collection<Object[]> testParams() {
		return AbstractOperationsTestParams.testParams();
	}

	@AfterClass
	public static void cleanUp() {
		ConnectionFactoryTracker.cleanUp();
	}

	@Before
	public void setUp() {
		streamOps = redisTemplate.opsForStream();

		redisTemplate.execute((RedisCallback<Object>) connection -> {
			connection.flushDb();
			return null;
		});
	}

	@Test // DATAREDIS-864
	public void addShouldAddMessage() {

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

	@Test // DATAREDIS-864
	public void addShouldAddReadSimpleMessage() {

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

	@Test // DATAREDIS-864
	public void simpleMessageReadWriteSymmetry() {

		K key = keyFactory.instance();
		HV value = hashValueFactory.instance();

		Assumptions.assumeThat(value).isNotInstanceOf(Person.class);

		RecordId messageId = streamOps.add(StreamRecords.objectBacked(value).withStreamKey(key));

		List<MapRecord<K, HK, HV>> messages = streamOps.range(key, Range.unbounded());

		assertThat(messages).hasSize(1);

		MapRecord<K, HK, HV> message = messages.get(0);

		assertThat(message.getId()).isEqualTo(messageId);
		assertThat(message.getStream()).isEqualTo(key);

		assertThat(message.getValue().values()).containsExactly(value);
	}

	@Test // DATAREDIS-864
	public void rangeShouldReportMessages() {

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

	@Test // DATAREDIS-864
	public void reverseRangeShouldReportMessages() {

		K key = keyFactory.instance();
		HK hashKey = hashKeyFactory.instance();
		HV value = hashValueFactory.instance();

		RecordId messageId1 = streamOps.add(key, Collections.singletonMap(hashKey, value));
		RecordId messageId2 = streamOps.add(key, Collections.singletonMap(hashKey, value));

		List<MapRecord<K, HK, HV>> messages = streamOps.reverseRange(key, Range.unbounded());

		assertThat(messages).hasSize(2).extracting("id").containsSequence(messageId2, messageId1);
	}

	@Test // DATAREDIS-864
	public void reverseRangeShouldConvertSimpleMessages() {

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
	public void readShouldReadMessage() {

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
	public void readShouldReadSimpleMessage() {

		K key = keyFactory.instance();
		HV value = hashValueFactory.instance();

		RecordId messageId1 = streamOps.add(StreamRecords.objectBacked(value).withStreamKey(key));
		streamOps.add(StreamRecords.objectBacked(value).withStreamKey(key));

		List<ObjectRecord<K, HV>> messages = streamOps.read((Class<HV>) value.getClass(), StreamOffset.create(key, ReadOffset.from("0-0")));

		assertThat(messages).hasSize(2);

		ObjectRecord<K, HV> message = messages.get(0);

		assertThat(message.getId()).isEqualTo(messageId1);
		assertThat(message.getStream()).isEqualTo(key);

		assertThat(message.getValue()).isEqualTo(value);
	}

	@Test // DATAREDIS-864
	public void readShouldReadMessages() {

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
	public void readShouldReadMessageWithConsumerGroup() {

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
	public void sizeShouldReportStreamSize() {

		K key = keyFactory.instance();
		HK hashKey = hashKeyFactory.instance();
		HV value = hashValueFactory.instance();

		streamOps.add(key, Collections.singletonMap(hashKey, value));
		assertThat(streamOps.size(key)).isEqualTo(1);

		streamOps.add(key, Collections.singletonMap(hashKey, value));
		assertThat(streamOps.size(key)).isEqualTo(2);
	}
}
