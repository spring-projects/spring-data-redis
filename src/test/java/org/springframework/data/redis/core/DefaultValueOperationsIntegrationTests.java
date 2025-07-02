/*
 * Copyright 2013-2025 the original author or authors.
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
import static org.awaitility.Awaitility.*;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.MethodSource;

import org.springframework.data.redis.DoubleObjectFactory;
import org.springframework.data.redis.ObjectFactory;
import org.springframework.data.redis.test.condition.EnabledIfLongRunningTest;
import org.springframework.data.redis.test.condition.EnabledOnCommand;

/**
 * Integration test of {@link DefaultValueOperations}
 *
 * @author Jennifer Hickey
 * @author Christoph Strobl
 * @author David Liu
 * @author Thomas Darimont
 * @author Jiahe Cai
 * @author Mark Paluch
 * @author Hendrik Duerkop
 */
@ParameterizedClass
@MethodSource("testParams")
public class DefaultValueOperationsIntegrationTests<K, V> {

	private final RedisTemplate<K, V> redisTemplate;
	private final ObjectFactory<K> keyFactory;
	private final ObjectFactory<V> valueFactory;
	private final ValueOperations<K, V> valueOps;

	public DefaultValueOperationsIntegrationTests(RedisTemplate<K, V> redisTemplate, ObjectFactory<K> keyFactory,
			ObjectFactory<V> valueFactory) {

		this.redisTemplate = redisTemplate;
		this.keyFactory = keyFactory;
		this.valueFactory = valueFactory;
		this.valueOps = redisTemplate.opsForValue();
	}

	static Collection<Object[]> testParams() {
		return AbstractOperationsTestParams.testParams();
	}

	@BeforeEach
	void setUp() {
		redisTemplate.execute((RedisCallback<Object>) connection -> {
			connection.flushDb();
			return null;
		});
	}

	@Test // DATAREDIS-784
	void testIncrement() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		assumeThat(value).isInstanceOf(Long.class);

		valueOps.set(key, value);

		assertThat(valueOps.increment(key)).isEqualTo(Long.valueOf((Long) value + 1));
		assertThat(valueOps.get(key)).isEqualTo((Long) value + 1);
	}

	@Test
	void testIncrementLong() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		assumeThat(value).isInstanceOf(Long.class);

		valueOps.set(key, value);

		assertThat(valueOps.increment(key, -10)).isEqualTo(Long.valueOf((Long) value - 10));
		assertThat(valueOps.get(key)).isEqualTo((Long) value - 10);

		valueOps.increment(key, -10);
		assertThat(valueOps.get(key)).isEqualTo((Long) value - 20);
	}

	@Test // DATAREDIS-247
	void testIncrementDouble() {

		assumeThat(valueFactory).isInstanceOf(DoubleObjectFactory.class);

		K key = keyFactory.instance();
		Double value = (Double) valueFactory.instance();

		valueOps.set(key, (V) value);

		assertThat(valueOps.increment(key, 1.4)).isBetween(value + 1.39, value + 1.41);
		assertThat((Double) valueOps.get(key)).isBetween(value + 1.39, value + 1.41);

		valueOps.increment(key, -10d);
		assertThat((Double) valueOps.get(key)).isBetween(value + 1.39 - 10, value + 1.41 - 10);
	}

	@Test // DATAREDIS-784
	void testDecrement() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		assumeThat(value).isInstanceOf(Long.class);

		valueOps.set(key, value);

		assertThat(valueOps.decrement(key)).isEqualTo(Long.valueOf((Long) value - 1));
		assertThat(valueOps.get(key)).isEqualTo((Long) value - 1);
	}

	@Test // DATAREDIS-784
	void testDecrementByLong() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		assumeThat(value).isInstanceOf(Long.class);

		valueOps.set(key, value);

		assertThat(valueOps.decrement(key, 5)).isEqualTo(Long.valueOf((Long) value - 5));
		assertThat(valueOps.get(key)).isEqualTo((Long) value - 5);
	}

	@Test
	void testMultiSetIfAbsent() {

		Map<K, V> keysAndValues = new HashMap<>();
		K key1 = keyFactory.instance();
		K key2 = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		keysAndValues.put(key1, value1);
		keysAndValues.put(key2, value2);

		assertThat(valueOps.multiSetIfAbsent(keysAndValues)).isTrue();
		assertThat(valueOps.multiGet(keysAndValues.keySet())).containsExactlyElementsOf(keysAndValues.values());
	}

	@Test
	void testMultiSetIfAbsentFailure() {

		K key1 = keyFactory.instance();
		K key2 = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();
		V value3 = valueFactory.instance();

		valueOps.set(key1, value1);

		Map<K, V> keysAndValues = new HashMap<>();
		keysAndValues.put(key1, value2);
		keysAndValues.put(key2, value3);

		assertThat(valueOps.multiSetIfAbsent(keysAndValues)).isFalse();
	}

	@Test
	void testMultiSet() {

		Map<K, V> keysAndValues = new HashMap<>();
		K key1 = keyFactory.instance();
		K key2 = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		keysAndValues.put(key1, value1);
		keysAndValues.put(key2, value2);

		valueOps.multiSet(keysAndValues);

		assertThat(valueOps.multiGet(keysAndValues.keySet())).containsExactlyElementsOf(keysAndValues.values());
	}

	@Test
	void testGetSet() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		valueOps.set(key, value);

		assertThat(valueOps.get(key)).isEqualTo(value);
	}

	@Test // GH-2050
	@EnabledOnCommand("GETEX")
	void testGetAndExpire() {

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		valueOps.set(key, value1);

		assertThat(valueOps.getAndExpire(key, Duration.ofSeconds(10))).isEqualTo(value1);
		assertThat(redisTemplate.getExpire(key)).isGreaterThan(1);
	}

	@Test // GH-2050
	@EnabledOnCommand("GETEX")
	void testGetAndPersist() {

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();

		valueOps.set(key, value1, Duration.ofSeconds(10));

		assertThat(valueOps.getAndPersist(key)).isEqualTo(value1);
		assertThat(redisTemplate.getExpire(key)).isEqualTo(-1);
	}

	@Test // GH-2050
	@EnabledOnCommand("GETDEL")
	void testGetAndDelete() {

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();

		valueOps.set(key, value1);

		assertThat(valueOps.getAndDelete(key)).isEqualTo(value1);
		assertThat(redisTemplate.hasKey(key)).isFalse();
	}

	@Test
	void testGetAndSet() {

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		valueOps.set(key, value1);

		assertThat(valueOps.getAndSet(key, value2)).isEqualTo(value1);
	}

	@Test
	void testSetWithExpiration() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		valueOps.set(key, value, 5, TimeUnit.SECONDS);

		Long expire = redisTemplate.getExpire(key, TimeUnit.MILLISECONDS);

		assertThat(expire).isLessThan(TimeUnit.SECONDS.toMillis(6)).isGreaterThan(TimeUnit.MILLISECONDS.toMillis(1));
	}

	@Test // DATAREDIS-815
	void testSetWithExpirationEX() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		valueOps.set(key, value, Duration.ofSeconds(5));

		Long expire = redisTemplate.getExpire(key, TimeUnit.MILLISECONDS);

		assertThat(expire).isLessThan(TimeUnit.SECONDS.toMillis(6)).isGreaterThan(TimeUnit.MILLISECONDS.toMillis(1));
	}

	@Test // DATAREDIS-815
	void testSetWithExpirationPX() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		valueOps.set(key, value, Duration.ofMillis(5500));

		Long expire = redisTemplate.getExpire(key, TimeUnit.MILLISECONDS);

		assertThat(expire).isLessThan(TimeUnit.SECONDS.toMillis(6));
		assertThat(expire).isGreaterThan(TimeUnit.MILLISECONDS.toMillis(1));
	}

	@Test // DATAREDIS-271
	@EnabledIfLongRunningTest
	void testSetWithExpirationWithTimeUnitMilliseconds() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		valueOps.set(key, value, 1, TimeUnit.MILLISECONDS);

		await().atMost(Duration.ofMillis(500L)).until(() -> !redisTemplate.hasKey(key));
	}

	@Test
	void testSetGetWithExpiration() {

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		valueOps.set(key, value1);

		assertThat(valueOps.setGet(key, value2, 1, TimeUnit.SECONDS)).isEqualTo(value1);
		assertThat(valueOps.get(key)).isEqualTo(value2);
	}

	@Test
	void testSetGetWithExpirationDuration() {

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		valueOps.set(key, value1);

		assertThat(valueOps.setGet(key, value2, Duration.ofMillis(1000))).isEqualTo(value1);
		assertThat(valueOps.get(key)).isEqualTo(value2);
	}

	@Test
	void testAppend() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		assumeThat(redisTemplate).isInstanceOf(StringRedisTemplate.class);

		valueOps.set(key, value);

		assertThat(valueOps.append(key, "aaa")).isEqualTo(Integer.valueOf(((String) value).length() + 3));
		assertThat(valueOps.get(key)).isEqualTo(value + "aaa");
	}

	@Test
	void testGetRange() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		assumeThat(value).isInstanceOf(String.class);

		valueOps.set(key, value);

		assertThat(valueOps.get(key, 0, 1)).hasSize(2);
	}

	@Test
	void testSetRange() {

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		assumeThat(value1).isInstanceOf(String.class);

		valueOps.set(key, value1);
		valueOps.set(key, value2, 0);

		assertThat(valueOps.get(key)).isEqualTo(value2);
	}

	@Test
	void testSetIfAbsent() {

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		assertThat(valueOps.setIfAbsent(key, value1)).isTrue();
		assertThat(valueOps.setIfAbsent(key, value2)).isFalse();
	}

	@Test // DATAREDIS-782
	void testSetIfAbsentWithExpiration() {

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		assertThat(valueOps.setIfAbsent(key, value1, 5, TimeUnit.SECONDS)).isTrue();
		assertThat(valueOps.setIfAbsent(key, value2, 5, TimeUnit.SECONDS)).isFalse();

		Long expire = redisTemplate.getExpire(key, TimeUnit.MILLISECONDS);

		assertThat(expire).isLessThan(TimeUnit.SECONDS.toMillis(6)).isGreaterThan(TimeUnit.MILLISECONDS.toMillis(1));
	}

	@Test // DATAREDIS-815
	void testSetIfAbsentWithExpirationEX() {

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		assertThat(valueOps.setIfAbsent(key, value1, Duration.ofSeconds(5))).isTrue();
		assertThat(valueOps.setIfAbsent(key, value2, Duration.ofSeconds(5))).isFalse();

		Long expire = redisTemplate.getExpire(key, TimeUnit.MILLISECONDS);

		assertThat(expire).isLessThan(TimeUnit.SECONDS.toMillis(6)).isGreaterThan(TimeUnit.MILLISECONDS.toMillis(1));
	}

	@Test // DATAREDIS-815
	void testSetIfAbsentWithExpirationPX() {

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		assertThat(valueOps.setIfAbsent(key, value1, Duration.ofMillis(5500))).isTrue();
		assertThat(valueOps.setIfAbsent(key, value2, Duration.ofMillis(5500))).isFalse();

		Long expire = redisTemplate.getExpire(key, TimeUnit.MILLISECONDS);

		assertThat(expire).isLessThan(TimeUnit.SECONDS.toMillis(6)).isGreaterThan(TimeUnit.MILLISECONDS.toMillis(1));
	}

	@Test // DATAREDIS-786
	void setIfPresentReturnsTrueWhenKeyExists() {

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		valueOps.set(key, value1);

		assertThat(valueOps.setIfPresent(key, value2)).isTrue();
		assertThat(valueOps.get(key)).isEqualTo(value2);
	}

	@Test // DATAREDIS-786
	void setIfPresentReturnsFalseWhenKeyDoesNotExist() {
		assertThat(valueOps.setIfPresent(keyFactory.instance(), valueFactory.instance())).isFalse();
	}

	@Test // DATAREDIS-786
	void setIfPresentShouldSetExpirationCorrectly() {

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		assertThat(valueOps.setIfPresent(key, value1, 5, TimeUnit.SECONDS)).isFalse();
		valueOps.set(key, value1);

		assertThat(valueOps.setIfPresent(key, value2, 5, TimeUnit.SECONDS)).isTrue();

		Long expire = redisTemplate.getExpire(key, TimeUnit.MILLISECONDS);
		assertThat(expire).isLessThan(TimeUnit.SECONDS.toMillis(6)).isGreaterThan(TimeUnit.MILLISECONDS.toMillis(1));
		assertThat(valueOps.get(key)).isEqualTo(value2);
	}

	@Test // DATAREDIS-815
	void testSetIfPresentWithExpirationEX() {

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		assertThat(valueOps.setIfPresent(key, value1, Duration.ofSeconds(5))).isFalse();
		valueOps.set(key, value1);

		assertThat(valueOps.setIfPresent(key, value2, Duration.ofSeconds(5))).isTrue();

		Long expire = redisTemplate.getExpire(key, TimeUnit.MILLISECONDS);
		assertThat(expire).isLessThan(TimeUnit.SECONDS.toMillis(6)).isGreaterThan(TimeUnit.MILLISECONDS.toMillis(1));
		assertThat(valueOps.get(key)).isEqualTo(value2);
	}

	@Test // DATAREDIS-815
	void testSetIfPresentWithExpirationPX() {

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		assertThat(valueOps.setIfPresent(key, value1, Duration.ofMillis(5500))).isFalse();
		valueOps.set(key, value1);

		assertThat(valueOps.setIfPresent(key, value2, Duration.ofMillis(5500))).isTrue();

		Long expire = redisTemplate.getExpire(key, TimeUnit.MILLISECONDS);
		assertThat(expire).isLessThan(TimeUnit.SECONDS.toMillis(6)).isGreaterThan(TimeUnit.MILLISECONDS.toMillis(1));
		assertThat(valueOps.get(key)).isEqualTo(value2);
	}

	@Test
	void testSize() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		valueOps.set(key, value);

		assertThat(valueOps.size(key)).isGreaterThan(0);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	void testRawKeys() {

		K key1 = keyFactory.instance();
		K key2 = keyFactory.instance();

		byte[][] rawKeys = ((DefaultValueOperations) valueOps).rawKeys(key1, key2);

		assertThat(rawKeys.length).isEqualTo(2);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	void testRawKeysCollection() {

		K key1 = keyFactory.instance();
		K key2 = keyFactory.instance();

		byte[][] rawKeys = ((DefaultValueOperations) valueOps).rawKeys(Arrays.asList(key1, key2));

		assertThat(rawKeys.length).isEqualTo(2);
	}

	@SuppressWarnings("rawtypes")
	@Test
	void testDeserializeKey() {

		K key = keyFactory.instance();

		assumeThat(key).isInstanceOf(byte[].class);

		assertThat(((DefaultValueOperations) valueOps).deserializeKey((byte[]) key)).isNotNull();
	}

	@Test // DATAREDIS-197
	void testSetAndGetBit() {

		assumeThat(redisTemplate).isInstanceOf(StringRedisTemplate.class);

		K key = keyFactory.instance();
		int bitOffset = 65;
		valueOps.setBit(key, bitOffset, true);

		assertThat(valueOps.getBit(key, bitOffset)).isTrue();
	}
}
