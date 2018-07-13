/*
 * Copyright 2013-2018 the original author or authors.
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

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static org.junit.Assume.*;
import static org.springframework.data.redis.SpinBarrier.*;
import static org.springframework.data.redis.matcher.RedisTestMatchers.*;

import java.text.DecimalFormat;
import java.time.Duration;
import java.util.ArrayList;
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

		assumeTrue(value instanceof Long);

		valueOps.set(key, value);

		assertEquals(Long.valueOf((Long) value + 1), valueOps.increment(key));
		assertEquals(Long.valueOf((Long) value + 1), valueOps.get(key));
	}

	@Test
	public void testIncrementLong() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		assumeTrue(value instanceof Long);

		valueOps.set(key, value);

		assertEquals(Long.valueOf((Long) value - 10), valueOps.increment(key, -10));
		assertEquals(Long.valueOf((Long) value - 10), valueOps.get(key));

		valueOps.increment(key, -10);
		assertEquals(Long.valueOf((Long) value - 20), valueOps.get(key));
	}

	@Test // DATAREDIS-247
	public void testIncrementDouble() {

		assumeTrue(RedisTestProfileValueSource.matches("redisVersion", "2.6"));

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		assumeTrue(value instanceof Double);

		valueOps.set(key, value);

		DecimalFormat twoDForm = (DecimalFormat) DecimalFormat.getInstance(Locale.US);

		assertEquals(Double.valueOf(twoDForm.format((Double) value + 1.4)), valueOps.increment(key, 1.4));
		assertEquals(Double.valueOf(twoDForm.format((Double) value + 1.4)), valueOps.get(key));

		valueOps.increment(key, -10d);
		assertEquals(Double.valueOf(twoDForm.format((Double) value + 1.4 - 10d)), valueOps.get(key));
	}

	@Test // DATAREDIS-784
	public void testDecrement() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		assumeTrue(value instanceof Long);

		valueOps.set(key, value);

		assertEquals(Long.valueOf((Long) value - 1), valueOps.decrement(key));
		assertEquals(Long.valueOf((Long) value - 1), valueOps.get(key));
	}

	@Test // DATAREDIS-784
	public void testDecrementByLong() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		assumeTrue(value instanceof Long);

		valueOps.set(key, value);

		assertEquals(Long.valueOf((Long) value - 5), valueOps.decrement(key, 5));
		assertEquals(Long.valueOf((Long) value - 5), valueOps.get(key));
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

		assertTrue(valueOps.multiSetIfAbsent(keysAndValues));
		assertThat(valueOps.multiGet(keysAndValues.keySet()), isEqual(new ArrayList<>(keysAndValues.values())));
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

		assertFalse(valueOps.multiSetIfAbsent(keysAndValues));
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

		assertThat(valueOps.multiGet(keysAndValues.keySet()), isEqual(new ArrayList<>(keysAndValues.values())));
	}

	@Test
	public void testGetSet() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		valueOps.set(key, value);

		assertThat(valueOps.get(key), isEqual(value));
	}

	@Test
	public void testGetAndSet() {

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		valueOps.set(key, value1);

		assertThat(valueOps.getAndSet(key, value2), isEqual(value1));
	}

	@Test
	public void testSetWithExpiration() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		valueOps.set(key, value, 5, TimeUnit.SECONDS);

		Long expire = redisTemplate.getExpire(key, TimeUnit.MILLISECONDS);

		assertThat(expire, is(lessThan(TimeUnit.SECONDS.toMillis(6))));
		assertThat(expire, is(greaterThan(TimeUnit.MILLISECONDS.toMillis(1))));
	}

	@Test // DATAREDIS-815
	public void testSetWithExpirationEX() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		valueOps.set(key, value, Duration.ofSeconds(5));

		Long expire = redisTemplate.getExpire(key, TimeUnit.MILLISECONDS);

		assertThat(expire, is(lessThan(TimeUnit.SECONDS.toMillis(6))));
		assertThat(expire, is(greaterThan(TimeUnit.MILLISECONDS.toMillis(1))));
	}

	@Test // DATAREDIS-815
	public void testSetWithExpirationPX() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		valueOps.set(key, value, Duration.ofMillis(5500));

		Long expire = redisTemplate.getExpire(key, TimeUnit.MILLISECONDS);

		assertThat(expire, is(lessThan(TimeUnit.SECONDS.toMillis(6))));
		assertThat(expire, is(greaterThan(TimeUnit.MILLISECONDS.toMillis(1))));
	}

	@Test // DATAREDIS-271
	public void testSetWithExpirationWithTimeUnitMilliseconds() {

		assumeTrue(RedisTestProfileValueSource.matches("runLongTests", "true"));

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		valueOps.set(key, value, 1, TimeUnit.MILLISECONDS);

		waitFor(() -> (!redisTemplate.hasKey(key)), 500);
	}

	@Test
	public void testAppend() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		assumeTrue(redisTemplate instanceof StringRedisTemplate);

		valueOps.set(key, value);

		assertEquals(Integer.valueOf(((String) value).length() + 3), valueOps.append(key, "aaa"));
		assertEquals((String) value + "aaa", valueOps.get(key));
	}

	@Test
	public void testGetRange() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		assumeTrue(value instanceof String);

		valueOps.set(key, value);

		assertEquals(2, valueOps.get(key, 0, 1).length());
	}

	@Test
	public void testSetRange() {

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		assumeTrue(value1 instanceof String);

		valueOps.set(key, value1);
		valueOps.set(key, value2, 0);

		assertEquals(value2, valueOps.get(key));
	}

	@Test
	public void testSetIfAbsent() {

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		assertTrue(valueOps.setIfAbsent(key, value1));
		assertFalse(valueOps.setIfAbsent(key, value2));
	}

	@Test // DATAREDIS-782
	public void testSetIfAbsentWithExpiration() {

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		assertTrue(valueOps.setIfAbsent(key, value1, 5, TimeUnit.SECONDS));
		assertFalse(valueOps.setIfAbsent(key, value2, 5, TimeUnit.SECONDS));

		Long expire = redisTemplate.getExpire(key, TimeUnit.MILLISECONDS);

		assertThat(expire, is(lessThan(TimeUnit.SECONDS.toMillis(6))));
		assertThat(expire, is(greaterThan(TimeUnit.MILLISECONDS.toMillis(1))));
	}

	@Test // DATAREDIS-815
	public void testSetIfAbsentWithExpirationEX() {

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		assertTrue(valueOps.setIfAbsent(key, value1, Duration.ofSeconds(5)));
		assertFalse(valueOps.setIfAbsent(key, value2, Duration.ofSeconds(5)));

		Long expire = redisTemplate.getExpire(key, TimeUnit.MILLISECONDS);

		assertThat(expire, is(lessThan(TimeUnit.SECONDS.toMillis(6))));
		assertThat(expire, is(greaterThan(TimeUnit.MILLISECONDS.toMillis(1))));
	}

	@Test // DATAREDIS-815
	public void testSetIfAbsentWithExpirationPX() {

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		assertTrue(valueOps.setIfAbsent(key, value1, Duration.ofMillis(5500)));
		assertFalse(valueOps.setIfAbsent(key, value2, Duration.ofMillis(5500)));

		Long expire = redisTemplate.getExpire(key, TimeUnit.MILLISECONDS);

		assertThat(expire, is(lessThan(TimeUnit.SECONDS.toMillis(6))));
		assertThat(expire, is(greaterThan(TimeUnit.MILLISECONDS.toMillis(1))));
	}

	@Test // DATAREDIS-786
	public void setIfPresentReturnsTrueWhenKeyExists() {

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		valueOps.set(key, value1);

		assertTrue(valueOps.setIfPresent(key, value2));
		assertThat(valueOps.get(key), is(value2));
	}

	@Test // DATAREDIS-786
	public void setIfPresentReturnsFalseWhenKeyDoesNotExist() {
		assertFalse(valueOps.setIfPresent(keyFactory.instance(), valueFactory.instance()));
	}

	@Test // DATAREDIS-786
	public void setIfPresentShouldSetExpirationCorrectly() {

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		assertFalse(valueOps.setIfPresent(key, value1, 5, TimeUnit.SECONDS));
		valueOps.set(key, value1);

		assertTrue(valueOps.setIfPresent(key, value2, 5, TimeUnit.SECONDS));

		Long expire = redisTemplate.getExpire(key, TimeUnit.MILLISECONDS);
		assertThat(expire, is(lessThan(TimeUnit.SECONDS.toMillis(6))));
		assertThat(expire, is(greaterThan(TimeUnit.MILLISECONDS.toMillis(1))));
		assertThat(valueOps.get(key), is(value2));
	}

	@Test // DATAREDIS-815
	public void testSetIfPresentWithExpirationEX() {

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		assertFalse(valueOps.setIfPresent(key, value1, Duration.ofSeconds(5)));
		valueOps.set(key, value1);

		assertTrue(valueOps.setIfPresent(key, value2, Duration.ofSeconds(5)));

		Long expire = redisTemplate.getExpire(key, TimeUnit.MILLISECONDS);
		assertThat(expire, is(lessThan(TimeUnit.SECONDS.toMillis(6))));
		assertThat(expire, is(greaterThan(TimeUnit.MILLISECONDS.toMillis(1))));
		assertThat(valueOps.get(key), is(value2));
	}

	@Test // DATAREDIS-815
	public void testSetIfPresentWithExpirationPX() {

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		assertFalse(valueOps.setIfPresent(key, value1, Duration.ofMillis(5500)));
		valueOps.set(key, value1);

		assertTrue(valueOps.setIfPresent(key, value2, Duration.ofMillis(5500)));

		Long expire = redisTemplate.getExpire(key, TimeUnit.MILLISECONDS);
		assertThat(expire, is(lessThan(TimeUnit.SECONDS.toMillis(6))));
		assertThat(expire, is(greaterThan(TimeUnit.MILLISECONDS.toMillis(1))));
		assertThat(valueOps.get(key), is(value2));
	}

	@Test
	public void testSize() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		valueOps.set(key, value);

		assertTrue(valueOps.size(key) > 0);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testRawKeys() {

		K key1 = keyFactory.instance();
		K key2 = keyFactory.instance();

		byte[][] rawKeys = ((DefaultValueOperations) valueOps).rawKeys(key1, key2);

		assertEquals(2, rawKeys.length);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testRawKeysCollection() {

		K key1 = keyFactory.instance();
		K key2 = keyFactory.instance();

		byte[][] rawKeys = ((DefaultValueOperations) valueOps).rawKeys(Arrays.asList(new Object[] { key1, key2 }));

		assertEquals(2, rawKeys.length);
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void testDeserializeKey() {

		K key = keyFactory.instance();

		assumeTrue(key instanceof byte[]);

		assertNotNull(((DefaultValueOperations) valueOps).deserializeKey((byte[]) key));
	}

	@Test // DATAREDIS-197
	public void testSetAndGetBit() {

		assumeTrue(redisTemplate instanceof StringRedisTemplate);

		K key = keyFactory.instance();
		int bitOffset = 65;
		valueOps.setBit(key, bitOffset, true);

		assertEquals(true, valueOps.getBit(key, bitOffset));
	}
}
