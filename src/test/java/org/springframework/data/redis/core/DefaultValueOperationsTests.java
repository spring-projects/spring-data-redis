/*
 * Copyright 2013 the original author or authors.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.springframework.data.redis.DoubleObjectFactory;
import org.springframework.data.redis.LongObjectFactory;
import org.springframework.data.redis.ObjectFactory;
import org.springframework.data.redis.RedisTestProfileValueSource;
import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.StringObjectFactory;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.srp.SrpConnectionFactory;
import org.springframework.data.redis.serializer.GenericToStringSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

/**
 * Integration test of {@link DefaultValueOperations}
 * 
 * @author Jennifer Hickey
 * 
 */
@RunWith(Parameterized.class)
public class DefaultValueOperationsTests<K,V> {

	private RedisTemplate<K,V> redisTemplate;

	private ObjectFactory<K> keyFactory;

	private ObjectFactory<V> valueFactory;

	private ValueOperations<K, V> valueOps;

	public DefaultValueOperationsTests(RedisTemplate<K,V> redisTemplate, ObjectFactory<K> keyFactory,
			ObjectFactory<V> valueFactory) {
		this.redisTemplate = redisTemplate;
		this.keyFactory = keyFactory;
		this.valueFactory = valueFactory;
	}

	@Parameters
	public static Collection<Object[]> testParams() {
		ObjectFactory<String> stringFactory = new StringObjectFactory();
		ObjectFactory<Long> longFactory = new LongObjectFactory();
		ObjectFactory<Double> doubleFactory = new DoubleObjectFactory();
		SrpConnectionFactory srConnFactory = new SrpConnectionFactory();
		srConnFactory.setPort(SettingsUtils.getPort());
		srConnFactory.setHostName(SettingsUtils.getHost());
		srConnFactory.afterPropertiesSet();
		RedisTemplate<String,String> stringTemplate = new StringRedisTemplate();
		stringTemplate.setConnectionFactory(srConnFactory);
		stringTemplate.afterPropertiesSet();
		RedisTemplate<String,Long> longTemplate = new RedisTemplate<String,Long>();
		longTemplate.setKeySerializer(new StringRedisSerializer());
		longTemplate.setValueSerializer(new GenericToStringSerializer<Long>(Long.class));
		longTemplate.setConnectionFactory(srConnFactory);
		longTemplate.afterPropertiesSet();
		RedisTemplate<String,Double> doubleTemplate = new RedisTemplate<String,Double>();
		doubleTemplate.setKeySerializer(new StringRedisSerializer());
		doubleTemplate.setValueSerializer(new GenericToStringSerializer<Double>(Double.class));
		doubleTemplate.setConnectionFactory(srConnFactory);
		doubleTemplate.afterPropertiesSet();
		return Arrays.asList(new Object[][] { { stringTemplate, stringFactory, stringFactory },
			{ longTemplate, stringFactory, longFactory }, { doubleTemplate, stringFactory, doubleFactory }
		});
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
		assertEquals(Long.valueOf((Long)v1 - 10), valueOps.increment(key, -10));
		assertEquals(Long.valueOf((Long)v1 - 10), (Long)valueOps.get(key));
		valueOps.increment(key, -10);
		assertEquals(Long.valueOf((Long)v1 - 20), (Long)valueOps.get(key));
	}

	@Test
	public void testIncrementDouble() {
		assumeTrue(RedisTestProfileValueSource.matches("redisVersion", "2.6"));
		K key = keyFactory.instance();
		V v1 = valueFactory.instance();
		assumeTrue(v1 instanceof Double);
		valueOps.set(key, v1);
		DecimalFormat twoDForm = new DecimalFormat("#.##");
		assertEquals(twoDForm.format(Double.valueOf((Double)v1 + 1.4)),
				twoDForm.format(valueOps.increment(key, 1.4)));
		assertEquals(twoDForm.format(Double.valueOf((Double)v1 + 1.4)),
				twoDForm.format((Double) valueOps.get(key)));
		valueOps.increment(key, -10d);
		assertEquals(twoDForm.format(Double.valueOf((Double)v1 + 1.4 - 10d)),
				twoDForm.format((Double) valueOps.get(key)));
	}

	@Test
	public void testMultiSetIfAbsent() {
		Map<K,V> keysAndValues = new HashMap<K,V>();
		K key1 = keyFactory.instance();
		K key2 = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();
		keysAndValues.put(key1, value1);
		keysAndValues.put(key2, value2);
		assertTrue(valueOps.multiSetIfAbsent(keysAndValues));
		assertEquals(new HashSet<V>(keysAndValues.values()),
				new HashSet<V>(valueOps.multiGet(keysAndValues.keySet())));
	}

	@Test
	public void testMultiSetIfAbsentFailure() {
		K key1 = keyFactory.instance();
		K key2 = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();
		V value3 = valueFactory.instance();
		valueOps.set(key1, value1);
		Map<K,V> keysAndValues = new HashMap<K,V>();
		keysAndValues.put(key1, value2);
		keysAndValues.put(key2, value3);
		assertFalse(valueOps.multiSetIfAbsent(keysAndValues));
	}
}
