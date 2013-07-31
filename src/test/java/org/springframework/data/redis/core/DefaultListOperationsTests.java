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
import static org.junit.Assert.assertNull;
import static org.junit.Assume.assumeTrue;
import static org.junit.Assert.assertThat;
import static org.springframework.data.redis.matcher.RedisTestMatchers.isEqual;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.springframework.data.redis.ObjectFactory;
import org.springframework.data.redis.RedisTestProfileValueSource;
import org.springframework.data.redis.connection.RedisConnection;

/**
 * Integration test of {@link DefaultListOperations}
 * @author Jennifer Hickey
 *
 * @param <K> Key test
 * @param <V> Value test
 */
@RunWith(Parameterized.class)
public class DefaultListOperationsTests<K,V> {

	private RedisTemplate<K,V> redisTemplate;

	private ObjectFactory<K> keyFactory;

	private ObjectFactory<V> valueFactory;

	private ListOperations<K,V> listOps;

	public DefaultListOperationsTests(RedisTemplate<K,V> redisTemplate, ObjectFactory<K> keyFactory,
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
		listOps = redisTemplate.opsForList();
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
	public void testLeftPushWithPivot() {
		K key = keyFactory.instance();
		V v1 = valueFactory.instance();
		V v2 = valueFactory.instance();
		V v3 = valueFactory.instance();
		System.out.println("Value1" + v1);
		System.out.println("Value2" + v2);
		System.out.println("Value3" + v3);
		assertEquals(Long.valueOf(1),listOps.leftPush(key, v1));
		assertEquals(Long.valueOf(2),listOps.leftPush(key, v2));
		assertEquals(Long.valueOf(3), listOps.leftPush(key, v1, v3));
		assertThat(listOps.range(key, 0, -1), isEqual(Arrays.asList(new Object[] {v2, v3, v1})));
	}

	@Test
	public void testLeftPushIfPresent() {
		K key = keyFactory.instance();
		V v1 = valueFactory.instance();
		V v2 = valueFactory.instance();
		assertEquals(Long.valueOf(0), listOps.leftPushIfPresent(key, v1));
		assertEquals(Long.valueOf(1),listOps.leftPush(key, v1));
		assertEquals(Long.valueOf(2),listOps.leftPushIfPresent(key, v2));
		assertThat(listOps.range(key, 0, -1), isEqual(Arrays.asList(new Object[] {v2, v1})));
	}

	@Test
	public void testRightPopAndLeftPushTimeout() {
		// 1 ms timeout gets upgraded to 1 sec timeout at the moment
		assumeTrue(RedisTestProfileValueSource.matches("runLongTests", "true"));
		K key = keyFactory.instance();
		K key2 = keyFactory.instance();
		V v1 = valueFactory.instance();
		assertNull(listOps.rightPopAndLeftPush(key, key2, 1, TimeUnit.MILLISECONDS));
		listOps.leftPush(key, v1);
		assertThat(listOps.rightPopAndLeftPush(key, key2, 1, TimeUnit.MILLISECONDS), isEqual(v1));
	}

	@Test
	public void testRightPopAndLeftPush() {
		K key = keyFactory.instance();
		K key2 = keyFactory.instance();
		V v1 = valueFactory.instance();
		assertNull(listOps.rightPopAndLeftPush(key, key2));
		listOps.leftPush(key, v1);
		assertThat(listOps.rightPopAndLeftPush(key, key2), isEqual(v1));
	}

	@Test
	public void testRightPushWithPivot() {
		K key = keyFactory.instance();
		V v1 = valueFactory.instance();
		V v2 = valueFactory.instance();
		V v3 = valueFactory.instance();
		assertEquals(Long.valueOf(1),listOps.rightPush(key, v1));
		assertEquals(Long.valueOf(2),listOps.rightPush(key, v2));
		assertEquals(Long.valueOf(3), listOps.rightPush(key, v1, v3));
		assertThat(listOps.range(key, 0, -1),
				isEqual(Arrays.asList(new Object[] {v1, v3, v2})));
	}

	@Test
	public void testRightPushIfPresent() {
		K key = keyFactory.instance();
		V v1 = valueFactory.instance();
		V v2 = valueFactory.instance();
		assertEquals(Long.valueOf(0), listOps.rightPushIfPresent(key, v1));
		assertEquals(Long.valueOf(1),listOps.rightPush(key, v1));
		assertEquals(Long.valueOf(2),listOps.rightPushIfPresent(key, v2));
		assertThat(listOps.range(key, 0, -1),
				isEqual(Arrays.asList(new Object[] {v1, v2})));
	}
}