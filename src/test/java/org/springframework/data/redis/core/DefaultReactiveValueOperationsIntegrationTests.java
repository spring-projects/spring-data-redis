/*
 * Copyright 2017-2018 the original author or authors.
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
import static org.springframework.data.redis.connection.BitFieldSubCommands.*;
import static org.springframework.data.redis.connection.BitFieldSubCommands.BitFieldIncrBy.Overflow.*;
import static org.springframework.data.redis.connection.BitFieldSubCommands.BitFieldType.*;
import static org.springframework.data.redis.connection.BitFieldSubCommands.Offset.offset;

import reactor.test.StepVerifier;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.springframework.data.redis.ConnectionFactoryTracker;
import org.springframework.data.redis.ObjectFactory;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

/**
 * Integration tests for {@link DefaultReactiveValueOperations}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @author Jiahe Cai
 */
@RunWith(Parameterized.class)
@SuppressWarnings("unchecked")
public class DefaultReactiveValueOperationsIntegrationTests<K, V> {

	private final ReactiveRedisTemplate<K, V> redisTemplate;
	private final ReactiveValueOperations<K, V> valueOperations;

	private final ObjectFactory<K> keyFactory;
	private final ObjectFactory<V> valueFactory;

	private final RedisSerializer serializer;

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
	public DefaultReactiveValueOperationsIntegrationTests(ReactiveRedisTemplate<K, V> redisTemplate,
			ObjectFactory<K> keyFactory, ObjectFactory<V> valueFactory, RedisSerializer serializer, String label) {

		this.redisTemplate = redisTemplate;
		this.valueOperations = redisTemplate.opsForValue();
		this.keyFactory = keyFactory;
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

	@Test // DATAREDIS-602
	public void set() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		StepVerifier.create(valueOperations.set(key, value)).expectNext(true).verifyComplete();

		StepVerifier.create(valueOperations.get(key)).expectNext(value).verifyComplete();
	}

	@Test // DATAREDIS-602
	public void setWithExpiry() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		StepVerifier.create(valueOperations.set(key, value, Duration.ofSeconds(10))).expectNext(true).expectComplete()
				.verify();

		StepVerifier.create(valueOperations.get(key)).expectNext(value).verifyComplete();

		StepVerifier.create(redisTemplate.getExpire(key)) //
				.consumeNextWith(actual -> assertThat(actual).isGreaterThan(Duration.ofSeconds(8))) //
				.expectComplete() //
				.verify();
	}

	@Test // DATAREDIS-602, DATAREDIS-779
	public void setIfAbsent() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		StepVerifier.create(valueOperations.setIfAbsent(key, value)).expectNext(true).verifyComplete();

		StepVerifier.create(valueOperations.setIfAbsent(key, value)).expectNext(false).verifyComplete();
	}

	@Test // DATAREDIS-782
	public void setIfAbsentWithExpiry() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		StepVerifier.create(valueOperations.setIfAbsent(key, value, Duration.ofSeconds(5))).expectNext(true)
				.expectComplete().verify();

		StepVerifier.create(valueOperations.setIfAbsent(key, value)).expectNext(false).verifyComplete();
		StepVerifier.create(valueOperations.setIfAbsent(key, value, Duration.ofSeconds(5))).expectNext(false)
				.verifyComplete();

		StepVerifier.create(redisTemplate.getExpire(key)) //
				.assertNext(actual -> {

					assertThat(actual).isBetween(Duration.ofMillis(1), Duration.ofSeconds(5));
				}).verifyComplete();
	}

	@Test // DATAREDIS-602, DATAREDIS-779
	public void setIfPresent() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();
		V laterValue = valueFactory.instance();

		StepVerifier.create(valueOperations.setIfPresent(key, value)).expectNext(false).verifyComplete();

		StepVerifier.create(valueOperations.set(key, value)).expectNext(true).verifyComplete();

		StepVerifier.create(valueOperations.setIfPresent(key, laterValue)).expectNext(true).verifyComplete();

		StepVerifier.create(valueOperations.get(key)).expectNext(laterValue).verifyComplete();
	}

	@Test // DATAREDIS-782
	public void setIfPresentWithExpiry() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();
		V laterValue = valueFactory.instance();

		StepVerifier.create(valueOperations.setIfPresent(key, value, Duration.ofSeconds(5))).expectNext(false)
				.verifyComplete();

		StepVerifier.create(valueOperations.set(key, value, Duration.ofSeconds(5))).expectNext(true).verifyComplete();

		StepVerifier.create(valueOperations.setIfPresent(key, laterValue, Duration.ofSeconds(5))).expectNext(true)
				.verifyComplete();

		StepVerifier.create(valueOperations.get(key)).expectNext(laterValue).verifyComplete();

		StepVerifier.create(redisTemplate.getExpire(key)) //
				.assertNext(actual -> {

					assertThat(actual).isBetween(Duration.ofMillis(1), Duration.ofSeconds(5));
				}).verifyComplete();
	}

	@Test // DATAREDIS-602
	public void multiSet() {

		K key1 = keyFactory.instance();
		K key2 = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		Map<K, V> map = new LinkedHashMap<>();
		map.put(key1, value1);
		map.put(key2, value2);

		StepVerifier.create(valueOperations.multiSet(map)).expectNext(true).verifyComplete();

		StepVerifier.create(valueOperations.get(key1)).expectNext(value1).verifyComplete();
		StepVerifier.create(valueOperations.get(key2)).expectNext(value2).verifyComplete();
	}

	@Test // DATAREDIS-602
	public void multiSetIfAbsent() {

		K key1 = keyFactory.instance();
		K key2 = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		Map<K, V> map = new LinkedHashMap<>();

		map.put(key1, value1);

		StepVerifier.create(valueOperations.multiSetIfAbsent(map)).expectNext(true).verifyComplete();

		map.put(key2, value2);
		StepVerifier.create(valueOperations.multiSetIfAbsent(map)).expectNext(false).verifyComplete();

		StepVerifier.create(valueOperations.get(key1)).expectNext(value1).verifyComplete();
		StepVerifier.create(valueOperations.get(key2)).expectNextCount(0).verifyComplete();
	}

	@Test // DATAREDIS-602
	public void get() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		StepVerifier.create(valueOperations.get(key)).verifyComplete();

		StepVerifier.create(valueOperations.set(key, value)).expectNext(true).verifyComplete();

		StepVerifier.create(valueOperations.get(key)).expectNext(value).verifyComplete();
	}

	@Test // DATAREDIS-602
	public void getAndSet() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();
		V nextValue = valueFactory.instance();

		StepVerifier.create(valueOperations.getAndSet(key, nextValue)).verifyComplete();

		StepVerifier.create(valueOperations.set(key, value)).expectNext(true).verifyComplete();

		StepVerifier.create(valueOperations.getAndSet(key, nextValue)).expectNext(value).verifyComplete();

		StepVerifier.create(valueOperations.get(key)).expectNext(nextValue).verifyComplete();
	}

	@Test // DATAREDIS-602
	public void multiGet() {

		K key1 = keyFactory.instance();
		K key2 = keyFactory.instance();
		K absent = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();
		V absentValue = null;

		if (serializer instanceof StringRedisSerializer) {
			absentValue = (V) "";
		}
		if (value1 instanceof ByteBuffer) {
			absentValue = (V) ByteBuffer.wrap(new byte[0]);
		}

		Map<K, V> map = new LinkedHashMap<>();
		map.put(key1, value1);
		map.put(key2, value2);

		StepVerifier.create(valueOperations.multiSet(map)).expectNext(true).verifyComplete();

		StepVerifier.create(valueOperations.multiGet(Arrays.asList(key2, key1, absent)))
				.expectNext(Arrays.asList(value2, value1, absentValue)).verifyComplete();
	}

	@Test // DATAREDIS-602
	public void append() {

		assumeTrue(serializer instanceof StringRedisSerializer);

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		StepVerifier.create(valueOperations.set(key, value)).expectNext(true).verifyComplete();

		StepVerifier.create(valueOperations.append(key, "foo")).expectNextCount(1).verifyComplete();

		StepVerifier.create(valueOperations.get(key)).expectNext((V) (value + "foo")).verifyComplete();
	}

	@Test // DATAREDIS-602
	public void getRange() {

		assumeTrue(serializer instanceof StringRedisSerializer);

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		StepVerifier.create(valueOperations.set(key, value)).expectNext(true).verifyComplete();

		String substring = value.toString().substring(1, 5);

		StepVerifier.create(valueOperations.get(key, 1, 4)).expectNext(substring).verifyComplete();
	}

	@Test // DATAREDIS-602
	public void setRange() {

		assumeTrue(serializer instanceof StringRedisSerializer);

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		StepVerifier.create(valueOperations.set(key, value)).expectNext(true).verifyComplete();
		StepVerifier.create(valueOperations.set(key, (V) "boo", 2)).expectNextCount(1).verifyComplete();

		StepVerifier.create(valueOperations.get(key)).consumeNextWith(actual -> {

			String string = (String) actual;
			String prefix = value.toString().substring(0, 2);

			assertThat(string).startsWith(prefix + "boo");
		}).verifyComplete();
	}

	@Test // DATAREDIS-602
	public void size() {

		assumeTrue(serializer instanceof StringRedisSerializer);

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		StepVerifier.create(valueOperations.set(key, value)).expectNext(true).verifyComplete();
		StepVerifier.create(valueOperations.size(key)).expectNext((long) value.toString().length()).expectComplete()
				.verify();
	}

	@Test // DATAREDIS-602
	public void setBit() {

		K key = keyFactory.instance();

		StepVerifier.create(valueOperations.setBit(key, 0, true)).expectNext(false).expectComplete();
		StepVerifier.create(valueOperations.setBit(key, 2, true)).expectNext(false).expectComplete();
	}

	@Test // DATAREDIS-602
	public void getBit() {

		K key = keyFactory.instance();

		StepVerifier.create(valueOperations.setBit(key, 0, true)).expectNext(false).expectComplete();
		StepVerifier.create(valueOperations.getBit(key, 0)).expectNext(true).expectComplete();
		StepVerifier.create(valueOperations.getBit(key, 1)).expectNext(false).expectComplete();
	}

	@Test // DATAREDIS-562
	public void bitField() {

		K key = keyFactory.instance();

		StepVerifier
				.create(valueOperations.bitField(key, create().incr(unsigned(2)).valueAt(offset(102L)).overflow(FAIL).by(1L)))
				.expectNext(Collections.singletonList(1L)).verifyComplete();
		StepVerifier
				.create(valueOperations.bitField(key, create().incr(unsigned(2)).valueAt(offset(102L)).overflow(FAIL).by(1L)))
				.expectNext(Collections.singletonList(2L)).verifyComplete();
		StepVerifier
				.create(valueOperations.bitField(key, create().incr(unsigned(2)).valueAt(offset(102L)).overflow(FAIL).by(1L)))
				.expectNext(Collections.singletonList(3L)).verifyComplete();
		StepVerifier
				.create(valueOperations.bitField(key, create().incr(unsigned(2)).valueAt(offset(102L)).overflow(FAIL).by(1L)))
				.expectNext(Collections.singletonList(null)).verifyComplete();
	}

	@Test // DATAREDIS-602
	public void delete() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		StepVerifier.create(valueOperations.set(key, value)).expectNext(true).verifyComplete();

		StepVerifier.create(valueOperations.delete(key)).expectNext(true).verifyComplete();

		StepVerifier.create(valueOperations.size(key)).expectNext(0L).verifyComplete();
	}

	@Test // DATAREDIS-784
	public void increment() {

		K key = keyFactory.instance();

		StepVerifier.create(valueOperations.increment(key)).expectNext(1L).verifyComplete();

		StepVerifier.create(valueOperations.increment(key)).expectNext(2L).verifyComplete();
	}

	@Test // DATAREDIS-784
	public void incrementByLongDelta() {

		K key = keyFactory.instance();

		StepVerifier.create(valueOperations.increment(key, 2L)).expectNext(2L).verifyComplete();

		StepVerifier.create(valueOperations.increment(key, -3L)).expectNext(-1L).verifyComplete();

		StepVerifier.create(valueOperations.increment(key, 1L)).expectNext(0L).verifyComplete();
	}

	@Test // DATAREDIS-784
	public void incrementByFloatDelta() {

		K key = keyFactory.instance();

		StepVerifier.create(valueOperations.increment(key, 0.1)).expectNext(0.1).verifyComplete();

		StepVerifier.create(valueOperations.increment(key, -0.3)).expectNext(-0.2).verifyComplete();

		StepVerifier.create(valueOperations.increment(key, 0.2)).expectNext(0.0).verifyComplete();
	}

	@Test // DATAREDIS-784
	public void decrement() {

		K key = keyFactory.instance();

		StepVerifier.create(valueOperations.decrement(key)).expectNext(-1L).verifyComplete();

		StepVerifier.create(valueOperations.decrement(key)).expectNext(-2L).verifyComplete();
	}

	@Test // DATAREDIS-784
	public void decrementByLongDelta() {

		K key = keyFactory.instance();

		StepVerifier.create(valueOperations.decrement(key, 2L)).expectNext(-2L).verifyComplete();

		StepVerifier.create(valueOperations.decrement(key, -3L)).expectNext(1L).verifyComplete();

		StepVerifier.create(valueOperations.decrement(key, 1L)).expectNext(0L).verifyComplete();
	}
}
