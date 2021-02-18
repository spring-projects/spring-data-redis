/*
 * Copyright 2018-2021 the original author or authors.
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

import org.junit.jupiter.api.BeforeEach;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.ObjectFactory;
import org.springframework.data.redis.RawObjectFactory;
import org.springframework.data.redis.StringObjectFactory;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisZSetCommands;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.connection.jedis.extension.JedisConnectionFactoryExtension;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.PendingMessages;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.test.condition.EnabledOnCommand;
import org.springframework.data.redis.test.extension.RedisStanalone;
import org.springframework.data.redis.test.extension.parametrized.MethodSource;
import org.springframework.data.redis.test.extension.parametrized.ParameterizedRedisTest;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for JedisStreamOperations.
 *
 * @author dengliming
 */
@MethodSource("testParams")
@EnabledOnCommand("XREAD")
public class JedisStreamOperationsIntegrationTests<K, HK, HV> {

	private final RedisConnectionFactory connectionFactory;
	private final RedisTemplate<K, ?> redisTemplate;
	private final ObjectFactory<K> keyFactory;
	private final ObjectFactory<HK> hashKeyFactory;
	private final ObjectFactory<HV> hashValueFactory;
	private final StreamOperations<K, HK, HV> streamOps;

	public JedisStreamOperationsIntegrationTests(RedisTemplate<K, ?> redisTemplate, ObjectFactory<K> keyFactory,
			ObjectFactory<HV> objectFactory) {
		this.redisTemplate = redisTemplate;
		this.connectionFactory = redisTemplate.getRequiredConnectionFactory();
		this.keyFactory = keyFactory;
		this.hashKeyFactory = (ObjectFactory<HK>) keyFactory;
		this.hashValueFactory = (ObjectFactory<HV>) objectFactory;
		this.streamOps = redisTemplate.opsForStream();
	}

	public static Collection<Object[]> testParams() {
		ObjectFactory<String> stringFactory = new StringObjectFactory();
		ObjectFactory<byte[]> rawFactory = new RawObjectFactory();

		JedisConnectionFactory jedisConnectionFactory = JedisConnectionFactoryExtension
				.getConnectionFactory(RedisStanalone.class);

		RedisTemplate<String, String> stringTemplate = new StringRedisTemplate();
		stringTemplate.setConnectionFactory(jedisConnectionFactory);
		stringTemplate.afterPropertiesSet();

		RedisTemplate<byte[], byte[]> rawTemplate = new RedisTemplate<>();
		rawTemplate.setConnectionFactory(jedisConnectionFactory);
		rawTemplate.setEnableDefaultSerializer(false);
		rawTemplate.afterPropertiesSet();

		return Arrays.asList(
				new Object[][] { { stringTemplate, stringFactory, stringFactory }, { rawTemplate, rawFactory, rawFactory } });
	}

	@BeforeEach
	void before() {
		redisTemplate.execute((RedisCallback<Object>) connection -> {
			connection.flushDb();
			return null;
		});
	}

	@ParameterizedRedisTest // DATAREDIS-1140
	void add() {

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

	@ParameterizedRedisTest // DATAREDIS-1140
	void range() {

		K key = keyFactory.instance();
		HK hashKey = hashKeyFactory.instance();
		HV value = hashValueFactory.instance();

		RecordId messageId1 = streamOps.add(key, Collections.singletonMap(hashKey, value));
		RecordId messageId2 = streamOps.add(key, Collections.singletonMap(hashKey, value));

		List<MapRecord<K, HK, HV>> messages = streamOps.range(key,
				Range.from(Range.Bound.inclusive(messageId1.getValue())).to(Range.Bound.inclusive(messageId2.getValue())),
				RedisZSetCommands.Limit.limit().count(1));

		assertThat(messages).hasSize(1);

		MapRecord<K, HK, HV> message = messages.get(0);

		assertThat(message.getId()).isEqualTo(messageId1);
	}

	@ParameterizedRedisTest // DATAREDIS-1140
	void reverseRange() {

		K key = keyFactory.instance();
		HK hashKey = hashKeyFactory.instance();
		HV value = hashValueFactory.instance();

		RecordId messageId1 = streamOps.add(key, Collections.singletonMap(hashKey, value));
		RecordId messageId2 = streamOps.add(key, Collections.singletonMap(hashKey, value));

		List<MapRecord<K, HK, HV>> messages = streamOps.reverseRange(key, Range.unbounded());

		assertThat(messages).hasSize(2).extracting("id").containsSequence(messageId2, messageId1);
	}

	@ParameterizedRedisTest // DATAREDIS-1140
	void read() {

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

	@ParameterizedRedisTest // DATAREDIS-1140
	void size() {

		K key = keyFactory.instance();
		HK hashKey = hashKeyFactory.instance();
		HV value = hashValueFactory.instance();

		streamOps.add(key, Collections.singletonMap(hashKey, value));
		assertThat(streamOps.size(key)).isEqualTo(1);

		streamOps.add(key, Collections.singletonMap(hashKey, value));
		assertThat(streamOps.size(key)).isEqualTo(2);
	}

	@ParameterizedRedisTest // DATAREDIS-1140
	void pending() {

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
}
