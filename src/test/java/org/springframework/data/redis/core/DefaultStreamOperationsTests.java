/*
 * Copyright 2018 the original author or authors.
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
import org.springframework.data.redis.RedisTestProfileValueSource;
import org.springframework.data.redis.connection.RedisStreamCommands.Consumer;
import org.springframework.data.redis.connection.RedisStreamCommands.ReadOffset;
import org.springframework.data.redis.connection.RedisStreamCommands.StreamMessage;
import org.springframework.data.redis.connection.RedisStreamCommands.StreamOffset;
import org.springframework.data.redis.connection.RedisStreamCommands.StreamReadOptions;
import org.springframework.data.redis.connection.RedisZSetCommands.Limit;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;

/**
 * Integration test of {@link DefaultStreamOperations}
 *
 * @author Mark Paluch
 */
@RunWith(Parameterized.class)
public class DefaultStreamOperationsTests<K, V> {

	private RedisTemplate<K, V> redisTemplate;

	private ObjectFactory<K> keyFactory;

	private ObjectFactory<V> valueFactory;

	private StreamOperations<K, V> streamOps;

	public DefaultStreamOperationsTests(RedisTemplate<K, V> redisTemplate, ObjectFactory<K> keyFactory,
			ObjectFactory<V> valueFactory) {

		// Currently, only Lettuce supports Redis Streams.
		// See https://github.com/xetorthio/jedis/issues/1820
		assumeTrue(redisTemplate.getConnectionFactory() instanceof LettuceConnectionFactory);

		// TODO: Change to 5.0 after Redis 5 GA
		assumeTrue(RedisTestProfileValueSource.matches("redisVersion", "4.9"));

		this.redisTemplate = redisTemplate;
		this.keyFactory = keyFactory;
		this.valueFactory = valueFactory;

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
		V value = valueFactory.instance();

		String messageId = streamOps.add(key, Collections.singletonMap(key, value));

		List<StreamMessage<K, V>> messages = streamOps.range(key, Range.unbounded());

		assertThat(messages).hasSize(1);

		StreamMessage<K, V> message = messages.get(0);

		assertThat(message.getId()).isEqualTo(messageId);
		assertThat(message.getStream()).isEqualTo(key);

		if (!(key instanceof byte[] || value instanceof byte[])) {
			assertThat(message.getBody()).containsEntry(key, value);
		}
	}

	@Test // DATAREDIS-864
	public void rangeShouldReportMessages() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		String messageId1 = streamOps.add(key, Collections.singletonMap(key, value));
		String messageId2 = streamOps.add(key, Collections.singletonMap(key, value));

		List<StreamMessage<K, V>> messages = streamOps.range(key,
				Range.from(Bound.inclusive(messageId1)).to(Bound.inclusive(messageId2)), Limit.limit().count(1));

		assertThat(messages).hasSize(1);

		StreamMessage<K, V> message = messages.get(0);

		assertThat(message.getId()).isEqualTo(messageId1);
	}

	@Test // DATAREDIS-864
	public void reverseRangeShouldReportMessages() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		String messageId1 = streamOps.add(key, Collections.singletonMap(key, value));
		String messageId2 = streamOps.add(key, Collections.singletonMap(key, value));

		List<StreamMessage<K, V>> messages = streamOps.reverseRange(key, Range.unbounded());

		assertThat(messages).hasSize(2).extracting("id").containsSequence(messageId2, messageId1);
	}

	@Test // DATAREDIS-864
	public void readShouldReadMessage() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		String messageId = streamOps.add(key, Collections.singletonMap(key, value));

		List<StreamMessage<K, V>> messages = streamOps.read(StreamOffset.create(key, ReadOffset.from("0-0")));

		assertThat(messages).hasSize(1);

		StreamMessage<K, V> message = messages.get(0);

		assertThat(message.getId()).isEqualTo(messageId);
		assertThat(message.getStream()).isEqualTo(key);

		if (!(key instanceof byte[] || value instanceof byte[])) {
			assertThat(message.getBody()).containsEntry(key, value);
		}
	}

	@Test // DATAREDIS-864
	public void readShouldReadMessages() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		streamOps.add(key, Collections.singletonMap(key, value));
		streamOps.add(key, Collections.singletonMap(key, value));

		List<StreamMessage<K, V>> messages = streamOps.read(StreamReadOptions.empty().count(2),
				StreamOffset.create(key, ReadOffset.from("0-0")));

		assertThat(messages).hasSize(2);
	}

	@Test // DATAREDIS-864
	public void readShouldReadMessageWithConsumerGroup() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		String messageId = streamOps.add(key, Collections.singletonMap(key, value));
		streamOps.createGroup(key, ReadOffset.from("0-0"), "my-group");

		List<StreamMessage<K, V>> messages = streamOps.read(Consumer.from("my-group", "my-consumer"),
				StreamOffset.create(key, ReadOffset.lastConsumed()));

		assertThat(messages).hasSize(1);

		StreamMessage<K, V> message = messages.get(0);

		assertThat(message.getId()).isEqualTo(messageId);
		assertThat(message.getStream()).isEqualTo(key);

		if (!(key instanceof byte[] || value instanceof byte[])) {
			assertThat(message.getBody()).containsEntry(key, value);
		}
	}

	@Test // DATAREDIS-864
	public void sizeShouldReportStreamSize() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		streamOps.add(key, Collections.singletonMap(key, value));
		assertThat(streamOps.size(key)).isEqualTo(1);

		streamOps.add(key, Collections.singletonMap(key, value));
		assertThat(streamOps.size(key)).isEqualTo(2);
	}
}
