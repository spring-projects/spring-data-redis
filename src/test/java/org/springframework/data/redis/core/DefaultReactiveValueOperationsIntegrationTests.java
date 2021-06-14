/*
 * Copyright 2017-2021 the original author or authors.
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

import org.junit.jupiter.api.BeforeEach;

import org.springframework.data.redis.ObjectFactory;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.ReactiveOperationsTestParams.Fixture;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.data.redis.test.condition.EnabledOnCommand;
import org.springframework.data.redis.test.extension.parametrized.MethodSource;
import org.springframework.data.redis.test.extension.parametrized.ParameterizedRedisTest;

/**
 * Integration tests for {@link DefaultReactiveValueOperations}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @author Jiahe Cai
 */
@MethodSource("testParams")
@SuppressWarnings("unchecked")
public class DefaultReactiveValueOperationsIntegrationTests<K, V> {

	private final ReactiveRedisTemplate<K, V> redisTemplate;
	private final ReactiveValueOperations<K, V> valueOperations;

	private final ObjectFactory<K> keyFactory;
	private final ObjectFactory<V> valueFactory;

	private final RedisSerializer<?> serializer;

	public static Collection<Fixture<?, ?>> testParams() {
		return ReactiveOperationsTestParams.testParams();
	}

	public DefaultReactiveValueOperationsIntegrationTests(Fixture<K, V> fixture) {

		this.redisTemplate = fixture.getTemplate();
		this.valueOperations = redisTemplate.opsForValue();
		this.keyFactory = fixture.getKeyFactory();
		this.valueFactory = fixture.getValueFactory();
		this.serializer = fixture.getSerializer();
	}

	@BeforeEach
	void before() {

		RedisConnectionFactory connectionFactory = (RedisConnectionFactory) redisTemplate.getConnectionFactory();
		RedisConnection connection = connectionFactory.getConnection();
		connection.flushAll();
		connection.close();
	}

	@ParameterizedRedisTest // DATAREDIS-602
	void set() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		valueOperations.set(key, value).as(StepVerifier::create).expectNext(true).verifyComplete();

		valueOperations.get(key).as(StepVerifier::create).expectNext(value).verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-602
	void setWithExpiry() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		valueOperations.set(key, value, Duration.ofSeconds(10)).as(StepVerifier::create).expectNext(true).expectComplete()
				.verify();

		valueOperations.get(key).as(StepVerifier::create).expectNext(value).verifyComplete();

		redisTemplate.getExpire(key).as(StepVerifier::create) //
				.consumeNextWith(actual -> assertThat(actual).isGreaterThan(Duration.ofSeconds(8))) //
				.expectComplete() //
				.verify();
	}

	@ParameterizedRedisTest // DATAREDIS-602, DATAREDIS-779
	void setIfAbsent() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		valueOperations.setIfAbsent(key, value).as(StepVerifier::create).expectNext(true).verifyComplete();

		valueOperations.setIfAbsent(key, value).as(StepVerifier::create).expectNext(false).verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-782
	void setIfAbsentWithExpiry() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		valueOperations.setIfAbsent(key, value, Duration.ofSeconds(5)).as(StepVerifier::create).expectNext(true)
				.expectComplete().verify();

		valueOperations.setIfAbsent(key, value).as(StepVerifier::create).expectNext(false).verifyComplete();
		valueOperations.setIfAbsent(key, value, Duration.ofSeconds(5)).as(StepVerifier::create).expectNext(false)
				.verifyComplete();

		redisTemplate.getExpire(key).as(StepVerifier::create) //
				.assertNext(actual -> {

					assertThat(actual).isBetween(Duration.ofMillis(1), Duration.ofSeconds(5));
				}).verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-602, DATAREDIS-779
	void setIfPresent() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();
		V laterValue = valueFactory.instance();

		valueOperations.setIfPresent(key, value).as(StepVerifier::create).expectNext(false).verifyComplete();

		valueOperations.set(key, value).as(StepVerifier::create).expectNext(true).verifyComplete();

		valueOperations.setIfPresent(key, laterValue).as(StepVerifier::create).expectNext(true).verifyComplete();

		valueOperations.get(key).as(StepVerifier::create).expectNext(laterValue).verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-782
	void setIfPresentWithExpiry() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();
		V laterValue = valueFactory.instance();

		valueOperations.setIfPresent(key, value, Duration.ofSeconds(5)).as(StepVerifier::create).expectNext(false)
				.verifyComplete();

		valueOperations.set(key, value, Duration.ofSeconds(5)).as(StepVerifier::create).expectNext(true).verifyComplete();

		valueOperations.setIfPresent(key, laterValue, Duration.ofSeconds(5)).as(StepVerifier::create).expectNext(true)
				.verifyComplete();

		valueOperations.get(key).as(StepVerifier::create).expectNext(laterValue).verifyComplete();

		redisTemplate.getExpire(key).as(StepVerifier::create) //
				.assertNext(actual -> {

					assertThat(actual).isBetween(Duration.ofMillis(1), Duration.ofSeconds(5));
				}).verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-602
	void multiSet() {

		K key1 = keyFactory.instance();
		K key2 = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		Map<K, V> map = new LinkedHashMap<>();
		map.put(key1, value1);
		map.put(key2, value2);

		valueOperations.multiSet(map).as(StepVerifier::create).expectNext(true).verifyComplete();

		valueOperations.get(key1).as(StepVerifier::create).expectNext(value1).verifyComplete();
		valueOperations.get(key2).as(StepVerifier::create).expectNext(value2).verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-602
	void multiSetIfAbsent() {

		K key1 = keyFactory.instance();
		K key2 = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		Map<K, V> map = new LinkedHashMap<>();

		map.put(key1, value1);

		valueOperations.multiSetIfAbsent(map).as(StepVerifier::create).expectNext(true).verifyComplete();

		map.put(key2, value2);
		valueOperations.multiSetIfAbsent(map).as(StepVerifier::create).expectNext(false).verifyComplete();

		valueOperations.get(key1).as(StepVerifier::create).expectNext(value1).verifyComplete();
		valueOperations.get(key2).as(StepVerifier::create).expectNextCount(0).verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-602
	void get() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		valueOperations.get(key).as(StepVerifier::create).verifyComplete();

		valueOperations.set(key, value).as(StepVerifier::create).expectNext(true).verifyComplete();

		valueOperations.get(key).as(StepVerifier::create).expectNext(value).verifyComplete();
	}

	@ParameterizedRedisTest // GH-2050
	@EnabledOnCommand("GETEX")
	void getAndExpire() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		valueOperations.set(key, value).as(StepVerifier::create).expectNext(true).verifyComplete();

		valueOperations.getAndExpire(key, Duration.ofSeconds(10)).as(StepVerifier::create).expectNext(value)
				.verifyComplete();

		redisTemplate.getExpire(key).as(StepVerifier::create)
				.assertNext(actual -> assertThat(actual).isGreaterThan(Duration.ZERO)).verifyComplete();
	}

	@ParameterizedRedisTest // GH-2050
	@EnabledOnCommand("GETDEL")
	void getAndDelete() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		valueOperations.set(key, value).as(StepVerifier::create).expectNext(true).verifyComplete();

		valueOperations.getAndDelete(key).as(StepVerifier::create).expectNext(value).verifyComplete();

		redisTemplate.hasKey(key).as(StepVerifier::create).expectNext(false).verifyComplete();
	}

	@ParameterizedRedisTest // GH-2050
	@EnabledOnCommand("GETEX")
	void getAndPersist() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		valueOperations.set(key, value, Duration.ofSeconds(10)).as(StepVerifier::create).expectNext(true).verifyComplete();

		valueOperations.getAndPersist(key).as(StepVerifier::create).expectNext(value).verifyComplete();

		redisTemplate.getExpire(key).as(StepVerifier::create).expectNext(Duration.ZERO).verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-602
	void getAndSet() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();
		V nextValue = valueFactory.instance();

		valueOperations.getAndSet(key, nextValue).as(StepVerifier::create).verifyComplete();

		valueOperations.set(key, value).as(StepVerifier::create).expectNext(true).verifyComplete();

		valueOperations.getAndSet(key, nextValue).as(StepVerifier::create).expectNext(value).verifyComplete();

		valueOperations.get(key).as(StepVerifier::create).expectNext(nextValue).verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-602
	void multiGet() {

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

		valueOperations.multiSet(map).as(StepVerifier::create).expectNext(true).verifyComplete();

		valueOperations.multiGet(Arrays.asList(key2, key1, absent)).as(StepVerifier::create)
				.expectNext(Arrays.asList(value2, value1, absentValue)).verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-602
	void append() {

		assumeTrue(serializer instanceof StringRedisSerializer);

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		valueOperations.set(key, value).as(StepVerifier::create).expectNext(true).verifyComplete();

		valueOperations.append(key, "foo").as(StepVerifier::create).expectNextCount(1).verifyComplete();

		valueOperations.get(key).as(StepVerifier::create).expectNext((V) (value + "foo")).verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-602
	void getRange() {

		assumeTrue(serializer instanceof StringRedisSerializer);

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		valueOperations.set(key, value).as(StepVerifier::create).expectNext(true).verifyComplete();

		String substring = value.toString().substring(1, 5);

		valueOperations.get(key, 1, 4).as(StepVerifier::create).expectNext(substring).verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-602
	void setRange() {

		assumeTrue(serializer instanceof StringRedisSerializer);

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		valueOperations.set(key, value).as(StepVerifier::create).expectNext(true).verifyComplete();
		valueOperations.set(key, (V) "boo", 2).as(StepVerifier::create).expectNextCount(1).verifyComplete();

		valueOperations.get(key).as(StepVerifier::create).consumeNextWith(actual -> {

			String string = (String) actual;
			String prefix = value.toString().substring(0, 2);

			assertThat(string).startsWith(prefix + "boo");
		}).verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-602
	void size() {

		assumeTrue(serializer instanceof StringRedisSerializer);

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		valueOperations.set(key, value).as(StepVerifier::create).expectNext(true).verifyComplete();
		valueOperations.size(key).as(StepVerifier::create).expectNext((long) value.toString().length()).expectComplete()
				.verify();
	}

	@ParameterizedRedisTest // DATAREDIS-602
	void setBit() {

		K key = keyFactory.instance();

		valueOperations.setBit(key, 0, true).as(StepVerifier::create).expectNext(false).expectComplete();
		valueOperations.setBit(key, 2, true).as(StepVerifier::create).expectNext(false).expectComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-602
	void getBit() {

		K key = keyFactory.instance();

		valueOperations.setBit(key, 0, true).as(StepVerifier::create).expectNext(false).expectComplete();
		valueOperations.getBit(key, 0).as(StepVerifier::create).expectNext(true).expectComplete();
		valueOperations.getBit(key, 1).as(StepVerifier::create).expectNext(false).expectComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-562
	void bitField() {

		K key = keyFactory.instance();

		valueOperations.bitField(key, create().incr(unsigned(2)).valueAt(offset(102L)).overflow(FAIL).by(1L))
				.as(StepVerifier::create)
				.expectNext(Collections.singletonList(1L)).verifyComplete();
		valueOperations.bitField(key, create().incr(unsigned(2)).valueAt(offset(102L)).overflow(FAIL).by(1L))
				.as(StepVerifier::create)
				.expectNext(Collections.singletonList(2L)).verifyComplete();
		valueOperations.bitField(key, create().incr(unsigned(2)).valueAt(offset(102L)).overflow(FAIL).by(1L))
				.as(StepVerifier::create)
				.expectNext(Collections.singletonList(3L)).verifyComplete();
		valueOperations.bitField(key, create().incr(unsigned(2)).valueAt(offset(102L)).overflow(FAIL).by(1L))
				.as(StepVerifier::create)
				.expectNext(Collections.singletonList(null)).verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-602
	void delete() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		valueOperations.set(key, value).as(StepVerifier::create).expectNext(true).verifyComplete();

		valueOperations.delete(key).as(StepVerifier::create).expectNext(true).verifyComplete();

		valueOperations.size(key).as(StepVerifier::create).expectNext(0L).verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-784
	void increment() {

		K key = keyFactory.instance();

		valueOperations.increment(key).as(StepVerifier::create).expectNext(1L).verifyComplete();

		valueOperations.increment(key).as(StepVerifier::create).expectNext(2L).verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-784
	void incrementByLongDelta() {

		K key = keyFactory.instance();

		valueOperations.increment(key, 2L).as(StepVerifier::create).expectNext(2L).verifyComplete();

		valueOperations.increment(key, -3L).as(StepVerifier::create).expectNext(-1L).verifyComplete();

		valueOperations.increment(key, 1L).as(StepVerifier::create).expectNext(0L).verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-784
	void incrementByFloatDelta() {

		K key = keyFactory.instance();

		valueOperations.increment(key, 0.1).as(StepVerifier::create).expectNext(0.1).verifyComplete();

		valueOperations.increment(key, -0.3).as(StepVerifier::create).expectNext(-0.2).verifyComplete();

		valueOperations.increment(key, 0.2).as(StepVerifier::create).expectNext(0.0).verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-784
	void decrement() {

		K key = keyFactory.instance();

		valueOperations.decrement(key).as(StepVerifier::create).expectNext(-1L).verifyComplete();

		valueOperations.decrement(key).as(StepVerifier::create).expectNext(-2L).verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-784
	void decrementByLongDelta() {

		K key = keyFactory.instance();

		valueOperations.decrement(key, 2L).as(StepVerifier::create).expectNext(-2L).verifyComplete();

		valueOperations.decrement(key, -3L).as(StepVerifier::create).expectNext(1L).verifyComplete();

		valueOperations.decrement(key, 1L).as(StepVerifier::create).expectNext(0L).verifyComplete();
	}
}
