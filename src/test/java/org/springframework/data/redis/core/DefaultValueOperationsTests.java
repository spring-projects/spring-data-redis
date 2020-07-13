/*
 * Copyright 2013-2020 the original author or authors.
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
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.junit.Assume.*;
import static org.springframework.data.redis.SpinBarrier.*;

import java.text.DecimalFormat;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import org.springframework.data.redis.ConnectionFactoryTracker;
import org.springframework.data.redis.ObjectFactory;
import org.springframework.data.redis.RedisTestProfileValueSource;

/**
 * Integration test of {@link DefaultValueOperations}
 *
 * @author Jennifer Hickey
 * @author Christoph Strobl
 * @author David Liu
 * @author Thomas Darimont
 * @author Jiahe Cai
 * @author Mark Paluch
 */
@RunWith(Parameterized.class)
public class DefaultValueOperationsTests<K, V> {

	private RedisTemplate<K, V> redisTemplate;

	private ObjectFactory<K> keyFactory;

	private ObjectFactory<V> valueFactory;

	private ValueOperations<K, V> valueOps;

	public DefaultValueOperationsTests(RedisTemplate<K, V> redisTemplate, ObjectFactory<K> keyFactory,
			ObjectFactory<V> valueFactory) {

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
		valueOps = redisTemplate.opsForValue();
	}

	@After
	public void tearDown() {
		redisTemplate.execute((RedisCallback<Object>) connection -> {
			connection.flushDb();
			return null;
		});
	}

	@Test // DATAREDIS-784
	public void testIncrement() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		assumeThat(value).isInstanceOf(Long.class);

		valueOps.set(key, value);

		assertThat(valueOps.increment(key)).isEqualTo(Long.valueOf((Long) value + 1));
		assertThat(valueOps.get(key)).isEqualTo(Long.valueOf((Long) value + 1));
	}

	@Test
	public void testIncrementLong() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		assumeThat(value).isInstanceOf(Long.class);

		valueOps.set(key, value);

		assertThat(valueOps.increment(key, -10)).isEqualTo(Long.valueOf((Long) value - 10));
		assertThat(valueOps.get(key)).isEqualTo(Long.valueOf((Long) value - 10));

		valueOps.increment(key, -10);
		assertThat(valueOps.get(key)).isEqualTo(Long.valueOf((Long) value - 20));
	}

	@Test // DATAREDIS-247
	public void testIncrementDouble() {

		assumeTrue(RedisTestProfileValueSource.matches("redisVersion", "2.6"));

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		assumeTrue(value instanceof Double);

		valueOps.set(key, value);

		DecimalFormat twoDForm = (DecimalFormat) DecimalFormat.getInstance(Locale.US);

		assertThat(valueOps.increment(key, 1.4)).isEqualTo(Double.valueOf(twoDForm.format((Double) value + 1.4)));
		assertThat(valueOps.get(key)).isEqualTo(Double.valueOf(twoDForm.format((Double) value + 1.4)));

		valueOps.increment(key, -10d);
		assertThat(valueOps.get(key)).isEqualTo(Double.valueOf(twoDForm.format((Double) value + 1.4 - 10d)));
	}

	@Test // DATAREDIS-784
	public void testDecrement() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		assumeThat(value).isInstanceOf(Long.class);

		valueOps.set(key, value);

		assertThat(valueOps.decrement(key)).isEqualTo(Long.valueOf((Long) value - 1));
		assertThat(valueOps.get(key)).isEqualTo(Long.valueOf((Long) value - 1));
	}

	@Test // DATAREDIS-784
	public void testDecrementByLong() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		assumeThat(value).isInstanceOf(Long.class);

		valueOps.set(key, value);

		assertThat(valueOps.decrement(key, 5)).isEqualTo(Long.valueOf((Long) value - 5));
		assertThat(valueOps.get(key)).isEqualTo(Long.valueOf((Long) value - 5));
	}

	@Test
	public void testMultiSetIfAbsent() {

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
	public void testMultiSetIfAbsentFailure() {

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
	public void testMultiSet() {

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
	public void testGetSet() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		valueOps.set(key, value);

		assertThat(valueOps.get(key)).isEqualTo(value);
	}

	@Test
	public void testGetAndSet() {

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		valueOps.set(key, value1);

		assertThat(valueOps.getAndSet(key, value2)).isEqualTo(value1);
	}

	@Test
	public void testSetWithExpiration() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		valueOps.set(key, value, 5, TimeUnit.SECONDS);

		Long expire = redisTemplate.getExpire(key, TimeUnit.MILLISECONDS);

		assertThat(expire).isLessThan(TimeUnit.SECONDS.toMillis(6)).isGreaterThan(TimeUnit.MILLISECONDS.toMillis(1));
	}

	@Test // DATAREDIS-815
	public void testSetWithExpirationEX() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		valueOps.set(key, value, Duration.ofSeconds(5));

		Long expire = redisTemplate.getExpire(key, TimeUnit.MILLISECONDS);

		assertThat(expire).isLessThan(TimeUnit.SECONDS.toMillis(6)).isGreaterThan(TimeUnit.MILLISECONDS.toMillis(1));
	}

	@Test // DATAREDIS-815
	public void testSetWithExpirationPX() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		valueOps.set(key, value, Duration.ofMillis(5500));

		Long expire = redisTemplate.getExpire(key, TimeUnit.MILLISECONDS);

		assertThat(expire).isLessThan(TimeUnit.SECONDS.toMillis(6));
		assertThat(expire).isGreaterThan(TimeUnit.MILLISECONDS.toMillis(1));
	}

	@Test // DATAREDIS-271
	public void testSetWithExpirationWithTimeUnitMilliseconds() {

		assumeThat(RedisTestProfileValueSource.matches("runLongTests", "true")).isTrue();

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		valueOps.set(key, value, 1, TimeUnit.MILLISECONDS);

		waitFor(() -> (!redisTemplate.hasKey(key)), 500);
	}

	@Test
	public void testAppend() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		assumeThat(redisTemplate).isInstanceOf(StringRedisTemplate.class);

		valueOps.set(key, value);

		assertThat(valueOps.append(key, "aaa")).isEqualTo(Integer.valueOf(((String) value).length() + 3));
		assertThat(valueOps.get(key)).isEqualTo(value + "aaa");
	}

	@Test
	public void testGetRange() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		assumeThat(value).isInstanceOf(String.class);

		valueOps.set(key, value);

		assertThat(valueOps.get(key, 0, 1)).hasSize(2);
	}

	@Test
	public void testSetRange() {

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		assumeThat(value1).isInstanceOf(String.class);

		valueOps.set(key, value1);
		valueOps.set(key, value2, 0);

		assertThat(valueOps.get(key)).isEqualTo(value2);
	}

	@Test
	public void testSetIfAbsent() {

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		assertThat(valueOps.setIfAbsent(key, value1)).isTrue();
		assertThat(valueOps.setIfAbsent(key, value2)).isFalse();
	}

	@Test // DATAREDIS-782
	public void testSetIfAbsentWithExpiration() {

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		assertThat(valueOps.setIfAbsent(key, value1, 5, TimeUnit.SECONDS)).isTrue();
		assertThat(valueOps.setIfAbsent(key, value2, 5, TimeUnit.SECONDS)).isFalse();

		Long expire = redisTemplate.getExpire(key, TimeUnit.MILLISECONDS);

		assertThat(expire).isLessThan(TimeUnit.SECONDS.toMillis(6)).isGreaterThan(TimeUnit.MILLISECONDS.toMillis(1));
	}

	@Test // DATAREDIS-815
	public void testSetIfAbsentWithExpirationEX() {

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		assertThat(valueOps.setIfAbsent(key, value1, Duration.ofSeconds(5))).isTrue();
		assertThat(valueOps.setIfAbsent(key, value2, Duration.ofSeconds(5))).isFalse();

		Long expire = redisTemplate.getExpire(key, TimeUnit.MILLISECONDS);

		assertThat(expire).isLessThan(TimeUnit.SECONDS.toMillis(6)).isGreaterThan(TimeUnit.MILLISECONDS.toMillis(1));
	}

	@Test // DATAREDIS-815
	public void testSetIfAbsentWithExpirationPX() {

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		assertThat(valueOps.setIfAbsent(key, value1, Duration.ofMillis(5500))).isTrue();
		assertThat(valueOps.setIfAbsent(key, value2, Duration.ofMillis(5500))).isFalse();

		Long expire = redisTemplate.getExpire(key, TimeUnit.MILLISECONDS);

		assertThat(expire).isLessThan(TimeUnit.SECONDS.toMillis(6)).isGreaterThan(TimeUnit.MILLISECONDS.toMillis(1));
	}

	@Test // DATAREDIS-786
	public void setIfPresentReturnsTrueWhenKeyExists() {

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		valueOps.set(key, value1);

		assertThat(valueOps.setIfPresent(key, value2)).isTrue();
		assertThat(valueOps.get(key)).isEqualTo(value2);
	}

	@Test // DATAREDIS-786
	public void setIfPresentReturnsFalseWhenKeyDoesNotExist() {
		assertThat(valueOps.setIfPresent(keyFactory.instance(), valueFactory.instance())).isFalse();
	}

	@Test // DATAREDIS-786
	public void setIfPresentShouldSetExpirationCorrectly() {

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
	public void testSetIfPresentWithExpirationEX() {

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
	public void testSetIfPresentWithExpirationPX() {

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
	public void testSize() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		valueOps.set(key, value);

		assertThat(valueOps.size(key) > 0).isTrue();
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testRawKeys() {

		K key1 = keyFactory.instance();
		K key2 = keyFactory.instance();

		byte[][] rawKeys = ((DefaultValueOperations) valueOps).rawKeys(key1, key2);

		assertThat(rawKeys.length).isEqualTo(2);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testRawKeysCollection() {

		K key1 = keyFactory.instance();
		K key2 = keyFactory.instance();

		byte[][] rawKeys = ((DefaultValueOperations) valueOps).rawKeys(Arrays.asList(new Object[] { key1, key2 }));

		assertThat(rawKeys.length).isEqualTo(2);
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void testDeserializeKey() {

		K key = keyFactory.instance();

		assumeThat(key).isInstanceOf(byte[].class);

		assertThat(((DefaultValueOperations) valueOps).deserializeKey((byte[]) key)).isNotNull();
	}

	@Test // DATAREDIS-197
	public void testSetAndGetBit() {

		assumeThat(redisTemplate).isInstanceOf(StringRedisTemplate.class);

		K key = keyFactory.instance();
		int bitOffset = 65;
		valueOps.setBit(key, bitOffset, true);

		assertThat(valueOps.getBit(key, bitOffset)).isTrue();
	}
}
