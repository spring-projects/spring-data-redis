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
import org.springframework.data.redis.PersonObjectFactory;
import org.springframework.data.redis.RedisTestProfileValueSource;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStreamCommands.ReadOffset;
import org.springframework.data.redis.connection.RedisStreamCommands.StreamMessage;
import org.springframework.data.redis.connection.RedisStreamCommands.StreamOffset;
import org.springframework.data.redis.connection.RedisStreamCommands.StreamReadOptions;
import org.springframework.data.redis.connection.RedisZSetCommands.Limit;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.serializer.RedisSerializer;

/**
 * Integration tests for {@link DefaultReactiveStreamOperations}.
 *
 * @author Mark Paluch
 */
@RunWith(Parameterized.class)
@SuppressWarnings("unchecked")
public class DefaultReactiveStreamOperationsTests<K, V> {

	private final ReactiveRedisTemplate<K, V> redisTemplate;
	private final ReactiveStreamOperations<K, V> streamOperations;

	private final ObjectFactory<K> keyFactory;
	private final ObjectFactory<V> valueFactory;

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
	public DefaultReactiveStreamOperationsTests(ReactiveRedisTemplate<K, V> redisTemplate, ObjectFactory<K> keyFactory,
			ObjectFactory<V> valueFactory, RedisSerializer serializer, String label) {

		// Currently, only Lettuce supports Redis Streams.
		// See https://github.com/xetorthio/jedis/issues/1820
		assumeTrue(redisTemplate.getConnectionFactory() instanceof LettuceConnectionFactory);

		// TODO: Change to 5.0 after Redis 5 GA
		assumeTrue(RedisTestProfileValueSource.matches("redisVersion", "4.9"));

		this.redisTemplate = redisTemplate;
		this.streamOperations = redisTemplate.opsForStream();
		this.keyFactory = keyFactory;
		this.valueFactory = valueFactory;

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

		assumeFalse(valueFactory instanceof PersonObjectFactory);

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		String messageId = streamOperations.add(key, Collections.singletonMap(key, value)).block();

		streamOperations.range(key, Range.unbounded()) //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> {

					assertThat(actual.getId()).isEqualTo(messageId);
					assertThat(actual.getStream()).isEqualTo(key);

					if (!(key instanceof byte[] || value instanceof byte[])) {
						assertThat(actual.getBody()).containsEntry(key, value);
					}
				}) //
				.verifyComplete();
	}

	@Test // DATAREDIS-864
	public void rangeShouldReportMessages() {

		assumeFalse(valueFactory instanceof PersonObjectFactory);

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		String messageId1 = streamOperations.add(key, Collections.singletonMap(key, value)).block();
		String messageId2 = streamOperations.add(key, Collections.singletonMap(key, value)).block();

		streamOperations
				.range(key, Range.from(Bound.inclusive(messageId1)).to(Bound.inclusive(messageId2)), Limit.limit().count(1)) //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> {

					assertThat(actual.getId()).isEqualTo(messageId1);
				}) //
				.verifyComplete();
	}

	@Test // DATAREDIS-864
	public void reverseRangeShouldReportMessages() {

		assumeFalse(valueFactory instanceof PersonObjectFactory);

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		String messageId1 = streamOperations.add(key, Collections.singletonMap(key, value)).block();
		String messageId2 = streamOperations.add(key, Collections.singletonMap(key, value)).block();

		streamOperations.reverseRange(key, Range.unbounded()).map(StreamMessage::getId) //
				.as(StepVerifier::create) //
				.expectNext(messageId2, messageId1) //
				.verifyComplete();
	}

	@Test // DATAREDIS-864
	public void readShouldReadMessage() {

		assumeFalse(valueFactory instanceof PersonObjectFactory);

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		String messageId = streamOperations.add(key, Collections.singletonMap(key, value)).block();

		streamOperations.read(StreamOffset.create(key, ReadOffset.from("0-0"))) //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> {

					assertThat(actual.getId()).isEqualTo(messageId);
					assertThat(actual.getStream()).isEqualTo(key);

					if (!(key instanceof byte[] || value instanceof byte[])) {
						assertThat(actual.getBody()).containsEntry(key, value);
					}
				}).verifyComplete();
	}

	@Test // DATAREDIS-864
	public void readShouldReadMessages() {

		assumeFalse(valueFactory instanceof PersonObjectFactory);

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		streamOperations.add(key, Collections.singletonMap(key, value)).block();
		streamOperations.add(key, Collections.singletonMap(key, value)).block();

		streamOperations.read(StreamReadOptions.empty().count(2), StreamOffset.create(key, ReadOffset.from("0-0"))) //
				.as(StepVerifier::create) //
				.expectNextCount(2) //
				.verifyComplete();
	}

	@Test // DATAREDIS-864
	public void sizeShouldReportStreamSize() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		streamOperations.add(key, Collections.singletonMap(key, value)).as(StepVerifier::create).expectNextCount(1)
				.verifyComplete();
		streamOperations.size(key) //
				.as(StepVerifier::create) //
				.expectNext(1L) //
				.verifyComplete();

		streamOperations.add(key, Collections.singletonMap(key, value)).as(StepVerifier::create).expectNextCount(1)
				.verifyComplete();
		streamOperations.size(key) //
				.as(StepVerifier::create) //
				.expectNext(2L) //
				.verifyComplete();
	}
}
