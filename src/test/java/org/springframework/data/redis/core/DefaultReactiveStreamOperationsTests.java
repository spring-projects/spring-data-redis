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
package org.springframework.data.redis.core;

import static org.assertj.core.api.Assertions.*;
import static org.junit.Assume.*;

import reactor.test.StepVerifier;

import java.util.Collection;
import java.util.Collections;

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
import org.springframework.data.redis.PersonObjectFactory;
import org.springframework.data.redis.RedisTestProfileValueSource;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisZSetCommands.Limit;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.JdkSerializationRedisSerializer;
import org.springframework.data.redis.serializer.OxmSerializer;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.data.redis.serializer.RedisSerializationContext.SerializationPair;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

/**
 * Integration tests for {@link DefaultReactiveStreamOperations}.
 *
 * @author Mark Paluch
 * @auhtor Christoph Strobl
 */
@RunWith(Parameterized.class)
@SuppressWarnings("unchecked")
public class DefaultReactiveStreamOperationsTests<K, HK, HV> {

	private final ReactiveRedisTemplate<K, ?> redisTemplate;
	private final ReactiveStreamOperations<K, HK, HV> streamOperations;

	private final ObjectFactory<K> keyFactory;
	private final ObjectFactory<HK> hashKeyFactory;
	private final ObjectFactory<HV> valueFactory;

	RedisSerializer<?> serializer;

	@Parameters(name = "{4}")
	public static Collection<Object[]> testParams() {
		return ReactiveOperationsTestParams.testParams();
	}

	@AfterClass
	public static void cleanUp() {
		ConnectionFactoryTracker.cleanUp();
	}

	/**
	 * @param redisTemplate
	 * @param keyFactory
	 * @param valueFactory
	 * @param label parameterized test label, no further use besides that.
	 */
	public DefaultReactiveStreamOperationsTests(ReactiveRedisTemplate<K, ?> redisTemplate, ObjectFactory<K> keyFactory,
			ObjectFactory<HV> valueFactory, RedisSerializer serializer, String label) {

		// Currently, only Lettuce supports Redis Streams.
		// See https://github.com/xetorthio/jedis/issues/1820
		assumeTrue(redisTemplate.getConnectionFactory() instanceof LettuceConnectionFactory);

		assumeTrue(RedisTestProfileValueSource.matches("redisVersion", "5.0"));

		RedisSerializationContext<K, ?> context = null;
		if (serializer != null) {
			context = RedisSerializationContext.newSerializationContext().value(SerializationPair.fromSerializer(serializer))
					.hashKey(keyFactory instanceof PersonObjectFactory ? RedisSerializer.java() : RedisSerializer.string())
					.hashValue(serializer)
					.key(keyFactory instanceof PersonObjectFactory ? RedisSerializer.java() : RedisSerializer.string()).build();
		}

		this.redisTemplate = redisTemplate;
		this.streamOperations = serializer != null ? redisTemplate.opsForStream(context) : redisTemplate.opsForStream();
		this.keyFactory = keyFactory;
		this.hashKeyFactory = (ObjectFactory<HK>) keyFactory;
		this.valueFactory = valueFactory;
		this.serializer = serializer;

		ConnectionFactoryTracker.add(redisTemplate.getConnectionFactory());
	}

	@Before
	public void before() {

		RedisConnectionFactory connectionFactory = (RedisConnectionFactory) redisTemplate.getConnectionFactory();
		RedisConnection connection = connectionFactory.getConnection();
		connection.flushAll();
		connection.close();
	}

	@Test // DATAREDIS-864
	public void addShouldAddMessage() {

		K key = keyFactory.instance();
		HK hashKey = hashKeyFactory.instance();
		HV value = valueFactory.instance();

		RecordId messageId = streamOperations.add(key, Collections.singletonMap(hashKey, value)).block();

		streamOperations.range(key, Range.unbounded()) //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> {

					assertThat(actual.getId()).isEqualTo(messageId);
					assertThat(actual.getStream()).isEqualTo(key);

					if (!(key instanceof byte[] || value instanceof byte[])) {
						assertThat(actual.getValue()).containsEntry(hashKey, value);
					}
				}) //
				.verifyComplete();
	}

	@Test // DATAREDIS-864
	public void addShouldAddReadSimpleMessage() {

		assumeTrue(!(serializer instanceof Jackson2JsonRedisSerializer)
				&& !(serializer instanceof GenericJackson2JsonRedisSerializer)
				&& !(serializer instanceof JdkSerializationRedisSerializer) && !(serializer instanceof OxmSerializer));

		K key = keyFactory.instance();
		HV value = valueFactory.instance();

		RecordId messageId = streamOperations.add(StreamRecords.objectBacked(value).withStreamKey(key)).block();

		streamOperations.range((Class<HV>) value.getClass(), key, Range.unbounded()).as(StepVerifier::create) //
				.consumeNextWith(it -> {
					assertThat(it.getId()).isEqualTo(messageId);
					assertThat(it.getStream()).isEqualTo(key);

					assertThat(it.getValue()).isEqualTo(value);

				}) //
				.verifyComplete();
	}

	@Test // DATAREDIS-864
	public void addShouldAddReadSimpleMessageWithRawSerializer() {

		assumeTrue(!(serializer instanceof Jackson2JsonRedisSerializer)
				&& !(serializer instanceof GenericJackson2JsonRedisSerializer));

		SerializationPair<K> keySerializer = redisTemplate.getSerializationContext().getKeySerializationPair();

		RedisSerializationContext<K, String> serializationContext = RedisSerializationContext
				.<K, String> newSerializationContext(StringRedisSerializer.UTF_8).key(keySerializer)
				.hashValue(SerializationPair.raw()).hashKey(SerializationPair.raw()).build();

		ReactiveRedisTemplate<K, String> raw = new ReactiveRedisTemplate<>(redisTemplate.getConnectionFactory(),
				serializationContext);

		K key = keyFactory.instance();
		Person value = new PersonObjectFactory().instance();

		RecordId messageId = raw.opsForStream().add(StreamRecords.objectBacked(value).withStreamKey(key)).block();

		raw.opsForStream().range((Class<HV>) value.getClass(), key, Range.unbounded()).as(StepVerifier::create) //
				.consumeNextWith(it -> {

					assertThat(it.getId()).isEqualTo(messageId);
					assertThat(it.getStream()).isEqualTo(key);
					assertThat(it.getValue()).isEqualTo(value);
				}) //
				.verifyComplete();
	}

	@Test // DATAREDIS-864
	public void rangeShouldReportMessages() {

		K key = keyFactory.instance();
		HK hashKey = hashKeyFactory.instance();
		HV value = valueFactory.instance();

		RecordId messageId1 = streamOperations.add(key, Collections.singletonMap(hashKey, value)).block();
		RecordId messageId2 = streamOperations.add(key, Collections.singletonMap(hashKey, value)).block();

		streamOperations
				.range(key, Range.from(Bound.inclusive(messageId1.getValue())).to(Bound.inclusive(messageId2.getValue())),
						Limit.limit().count(1)) //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> {

					assertThat(actual.getId()).isEqualTo(messageId1);
				}) //
				.verifyComplete();
	}

	@Test // DATAREDIS-864
	public void reverseRangeShouldReportMessages() {

		K key = keyFactory.instance();
		HK hashKey = hashKeyFactory.instance();
		HV value = valueFactory.instance();

		RecordId messageId1 = streamOperations.add(key, Collections.singletonMap(hashKey, value)).block();
		RecordId messageId2 = streamOperations.add(key, Collections.singletonMap(hashKey, value)).block();

		streamOperations.reverseRange(key, Range.unbounded()).map(MapRecord::getId) //
				.as(StepVerifier::create) //
				.expectNext(messageId2, messageId1) //
				.verifyComplete();
	}

	@Test // DATAREDIS-864
	public void reverseRangeShouldConvertSimpleMessages() {

		assumeTrue(!(serializer instanceof Jackson2JsonRedisSerializer)
				&& !(serializer instanceof GenericJackson2JsonRedisSerializer));

		K key = keyFactory.instance();
		HK hashKey = hashKeyFactory.instance();
		HV value = valueFactory.instance();

		RecordId messageId1 = streamOperations.add(StreamRecords.objectBacked(value).withStreamKey(key)).block();
		RecordId messageId2 = streamOperations.add(StreamRecords.objectBacked(value).withStreamKey(key)).block();

		streamOperations.reverseRange((Class<HV>) value.getClass(), key, Range.unbounded()).as(StepVerifier::create)
				.consumeNextWith(it -> assertThat(it.getId()).isEqualTo(messageId2))
				.consumeNextWith(it -> assertThat(it.getId()).isEqualTo(messageId1)).verifyComplete();
	}

	@Test // DATAREDIS-864
	public void readShouldReadMessage() {

		// assumeFalse(valueFactory instanceof PersonObjectFactory);
		// assumeFalse(keyFactory instanceof LongObjectFactory);
		// assumeFalse(keyFactory instanceof DoubleObjectFactory);

		K key = keyFactory.instance();
		HK hashKey = hashKeyFactory.instance();
		HV value = valueFactory.instance();

		RecordId messageId = streamOperations.add(key, Collections.singletonMap(hashKey, value)).block();

		streamOperations.read(StreamOffset.create(key, ReadOffset.from("0-0"))) //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> {

					assertThat(actual.getId()).isEqualTo(messageId);
					assertThat(actual.getStream()).isEqualTo(key);

					if (!(key instanceof byte[] || value instanceof byte[])) {
						assertThat(actual.getValue()).containsEntry(hashKey, value);
					}
				}).verifyComplete();
	}

	@Test // DATAREDIS-864
	public void readShouldReadMessages() {

		assumeFalse(valueFactory instanceof PersonObjectFactory);

		K key = keyFactory.instance();
		HK hashKey = hashKeyFactory.instance();
		HV value = valueFactory.instance();

		streamOperations.add(key, Collections.singletonMap(hashKey, value)).block();
		streamOperations.add(key, Collections.singletonMap(hashKey, value)).block();

		streamOperations.read(StreamReadOptions.empty().count(2), StreamOffset.create(key, ReadOffset.from("0-0"))) //
				.as(StepVerifier::create) //
				.expectNextCount(2) //
				.verifyComplete();
	}

	@Test // DATAREDIS-864
	public void sizeShouldReportStreamSize() {

		K key = keyFactory.instance();
		HK hashKey = hashKeyFactory.instance();
		HV value = valueFactory.instance();

		streamOperations.add(key, Collections.singletonMap(hashKey, value)).as(StepVerifier::create).expectNextCount(1)
				.verifyComplete();
		streamOperations.size(key) //
				.as(StepVerifier::create) //
				.expectNext(1L) //
				.verifyComplete();

		streamOperations.add(key, Collections.singletonMap(hashKey, value)).as(StepVerifier::create).expectNextCount(1)
				.verifyComplete();
		streamOperations.size(key) //
				.as(StepVerifier::create) //
				.expectNext(2L) //
				.verifyComplete();
	}
}
