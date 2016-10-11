/*
 * Copyright 2013-2014 the original author or authors.
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
import static org.assertj.core.data.Offset.offset;
import static org.junit.Assume.*;
import static org.springframework.data.redis.SpinBarrier.*;

import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.springframework.data.redis.ObjectFactory;
import org.springframework.data.redis.RedisTestProfileValueSource;
import org.springframework.data.redis.TestCondition;
import org.springframework.data.redis.connection.RedisConnection;

/**
 * Integration test of {@link DefaultValueOperations}
 * 
 * @author Jennifer Hickey
 * @author Christoph Strobl
 * @author David Liu
 * @author Thomas Darimont
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
	}

	@Parameters
	public static Collection<Object[]> testParams() {
		return AbstractOperationsTestParams.testParams();
	}

	@Before
	public void setUp() {
		valueOps = redisTemplate.opsForValue();
	}

	@After
	public void tearDown() {
		redisTemplate.execute(new RedisCallback<Object>() {
			public Object doInRedis(RedisConnection connection) {
				connection.flushDb();
				return null;
			}
		});
	}

	@Test
	public void testIncrementLong() throws Exception {
		K key = keyFactory.instance();
		V v1 = valueFactory.instance();
		assumeTrue(v1 instanceof Long);
		valueOps.set(key, v1);
		assertThat(valueOps.increment(key, -10)).isEqualTo(Long.valueOf((Long) v1 - 10));
		assertThat(valueOps.get(key)).isEqualTo(Long.valueOf((Long) v1 - 10));
		valueOps.increment(key, -10);
		assertThat((Long) valueOps.get(key)).isEqualTo(Long.valueOf((Long) v1 - 20));
	}

	@Test // DATAREDIS-247
	public void testIncrementDouble() {

		assumeTrue(RedisTestProfileValueSource.matches("redisVersion", "2.6"));
		K key = keyFactory.instance();
		V v1 = valueFactory.instance();
		assumeTrue(v1 instanceof Double);
		valueOps.set(key, v1);
		DecimalFormat twoDForm = (DecimalFormat) DecimalFormat.getInstance(Locale.US);

		assertThat(valueOps.increment(key, 1.4)).isEqualTo((Double) v1 + 1.4, offset(0.01));
		assertThat((Double) valueOps.get(key)).isEqualTo((Double) v1 + 1.4, offset(0.01));
		valueOps.increment(key, -10d);
		assertThat((Double) valueOps.get(key)).isEqualTo((Double) v1 + 1.4 - 10, offset(0.01));
	}

	@Test
	public void testMultiSetIfAbsent() {
		Map<K, V> keysAndValues = new HashMap<K, V>();
		K key1 = keyFactory.instance();
		K key2 = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();
		keysAndValues.put(key1, value1);
		keysAndValues.put(key2, value2);
		assertThat(valueOps.multiSetIfAbsent(keysAndValues)).isTrue();
		assertThat(valueOps.multiGet(keysAndValues.keySet())).containsAll(keysAndValues.values());
	}

	@Test
	public void testMultiSetIfAbsentFailure() {
		K key1 = keyFactory.instance();
		K key2 = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();
		V value3 = valueFactory.instance();
		valueOps.set(key1, value1);
		Map<K, V> keysAndValues = new HashMap<K, V>();
		keysAndValues.put(key1, value2);
		keysAndValues.put(key2, value3);
		assertThat(valueOps.multiSetIfAbsent(keysAndValues)).isFalse();
	}

	@Test
	public void testMultiSet() {
		Map<K, V> keysAndValues = new HashMap<K, V>();
		K key1 = keyFactory.instance();
		K key2 = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();
		keysAndValues.put(key1, value1);
		keysAndValues.put(key2, value2);
		valueOps.multiSet(keysAndValues);
		assertThat(valueOps.multiGet(keysAndValues.keySet())).containsAll(keysAndValues.values());
	}

	@Test
	public void testGetSet() {
		K key1 = keyFactory.instance();
		V value1 = valueFactory.instance();
		valueOps.set(key1, value1);
		assertThat(valueOps.get(key1)).isEqualTo(value1);
	}

	@Test
	public void testGetAndSet() {
		K key1 = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();
		valueOps.set(key1, value1);
		assertThat(valueOps.getAndSet(key1, value2)).isEqualTo(value1);
	}

	@Test
	public void testSetWithExpiration() {
		assumeTrue(RedisTestProfileValueSource.matches("runLongTests", "true"));
		final K key1 = keyFactory.instance();
		V value1 = valueFactory.instance();
		valueOps.set(key1, value1, 1, TimeUnit.SECONDS);
		waitFor(new TestCondition() {
			public boolean passes() {
				return (!redisTemplate.hasKey(key1));
			}
		}, 1000);
	}

	@Test // DATAREDIS-271
	public void testSetWithExpirationWithTimeUnitMilliseconds() {

		assumeTrue(RedisTestProfileValueSource.matches("runLongTests", "true"));
		final K key1 = keyFactory.instance();
		V value1 = valueFactory.instance();
		valueOps.set(key1, value1, 1, TimeUnit.MILLISECONDS);
		waitFor(new TestCondition() {
			public boolean passes() {
				return (!redisTemplate.hasKey(key1));
			}
		}, 500);
	}

	@Test
	public void testAppend() {
		K key1 = keyFactory.instance();
		V value1 = valueFactory.instance();
		assumeTrue(redisTemplate instanceof StringRedisTemplate);
		valueOps.set(key1, value1);
		assertThat(valueOps.append(key1, "aaa")).isEqualTo(((String) value1).length() + 3);
		assertThat(valueOps.get(key1)).isEqualTo(value1 + "aaa");
	}

	@Test
	public void testGetRange() {
		K key1 = keyFactory.instance();
		V value1 = valueFactory.instance();
		assumeTrue(value1 instanceof String);
		valueOps.set(key1, value1);
		assertThat(valueOps.get(key1, 0, 1).length()).isEqualTo(2);
	}

	@Test
	public void testSetRange() {
		K key1 = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();
		assumeTrue(value1 instanceof String);
		valueOps.set(key1, value1);
		valueOps.set(key1, value2, 0);
		assertThat(valueOps.get(key1)).isEqualTo(value2);
	}

	@Test
	public void testSetIfAbsent() {
		K key1 = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();
		assertThat(valueOps.setIfAbsent(key1, value1)).isTrue();
		assertThat(valueOps.setIfAbsent(key1, value2)).isFalse();
	}

	@Test
	public void testSize() {
		K key1 = keyFactory.instance();
		V value1 = valueFactory.instance();
		valueOps.set(key1, value1);
		assertThat(valueOps.size(key1)).isGreaterThan(0);
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
		K key1 = keyFactory.instance();
		assumeTrue(key1 instanceof byte[]);
		assertThat(((DefaultValueOperations) valueOps).deserializeKey((byte[]) key1)).isNotNull();
	}
	
	@Test // DATAREDIS-197
	public void testSetAndGetBit() {

		assumeTrue(redisTemplate instanceof StringRedisTemplate);

		K key1 = keyFactory.instance();
		int bitOffset = 65;
		valueOps.setBit(key1, bitOffset, true);

		assertThat(valueOps.getBit(key1, bitOffset)).isTrue();
	}

}
