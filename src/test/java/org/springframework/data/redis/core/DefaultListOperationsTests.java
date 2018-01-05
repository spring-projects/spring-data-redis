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

import static org.junit.Assert.*;
import static org.junit.Assume.*;
import static org.springframework.data.redis.matcher.RedisTestMatchers.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
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
 * Integration test of {@link DefaultListOperations}
 *
 * @author Jennifer Hickey
 * @author Thomas Darimont
 * @author Christoph Strobl
 * @param <K> Key test
 * @param <V> Value test
 */
@RunWith(Parameterized.class)
public class DefaultListOperationsTests<K, V> {

	private RedisTemplate<K, V> redisTemplate;

	private ObjectFactory<K> keyFactory;

	private ObjectFactory<V> valueFactory;

	private ListOperations<K, V> listOps;

	public DefaultListOperationsTests(RedisTemplate<K, V> redisTemplate, ObjectFactory<K> keyFactory,
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
		listOps = redisTemplate.opsForList();
	}

	@After
	public void tearDown() {
		redisTemplate.execute((RedisCallback<Object>) connection -> {
			connection.flushDb();
			return null;
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
		assertEquals(Long.valueOf(1), listOps.leftPush(key, v1));
		assertEquals(Long.valueOf(2), listOps.leftPush(key, v2));
		assertEquals(Long.valueOf(3), listOps.leftPush(key, v1, v3));
		assertThat(listOps.range(key, 0, -1), isEqual(Arrays.asList(new Object[] { v2, v3, v1 })));
	}

	@Test
	public void testLeftPushIfPresent() {
		K key = keyFactory.instance();
		V v1 = valueFactory.instance();
		V v2 = valueFactory.instance();
		assertEquals(Long.valueOf(0), listOps.leftPushIfPresent(key, v1));
		assertEquals(Long.valueOf(1), listOps.leftPush(key, v1));
		assertEquals(Long.valueOf(2), listOps.leftPushIfPresent(key, v2));
		assertThat(listOps.range(key, 0, -1), isEqual(Arrays.asList(new Object[] { v2, v1 })));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testLeftPushAll() {
		K key = keyFactory.instance();
		V v1 = valueFactory.instance();
		V v2 = valueFactory.instance();
		V v3 = valueFactory.instance();
		assertEquals(Long.valueOf(2), listOps.leftPushAll(key, v1, v2));
		assertEquals(Long.valueOf(3), listOps.leftPush(key, v3));
		assertThat(listOps.range(key, 0, -1), isEqual(Arrays.asList(new Object[] { v3, v2, v1 })));
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
		assertEquals(Long.valueOf(1), listOps.rightPush(key, v1));
		assertEquals(Long.valueOf(2), listOps.rightPush(key, v2));
		assertEquals(Long.valueOf(3), listOps.rightPush(key, v1, v3));
		assertThat(listOps.range(key, 0, -1), isEqual(Arrays.asList(new Object[] { v1, v3, v2 })));
	}

	@Test
	public void testRightPushIfPresent() {
		K key = keyFactory.instance();
		V v1 = valueFactory.instance();
		V v2 = valueFactory.instance();
		assertEquals(Long.valueOf(0), listOps.rightPushIfPresent(key, v1));
		assertEquals(Long.valueOf(1), listOps.rightPush(key, v1));
		assertEquals(Long.valueOf(2), listOps.rightPushIfPresent(key, v2));
		assertThat(listOps.range(key, 0, -1), isEqual(Arrays.asList(new Object[] { v1, v2 })));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testRightPushAll() {
		K key = keyFactory.instance();
		V v1 = valueFactory.instance();
		V v2 = valueFactory.instance();
		V v3 = valueFactory.instance();
		assertEquals(Long.valueOf(2), listOps.rightPushAll(key, v1, v2));
		assertEquals(Long.valueOf(3), listOps.rightPush(key, v3));
		assertThat(listOps.range(key, 0, -1), isEqual(Arrays.asList(new Object[] { v1, v2, v3 })));
	}

	@Test // DATAREDIS-288
	@SuppressWarnings("unchecked")
	public void testRightPushAllCollection() {

		K key = keyFactory.instance();

		V v1 = valueFactory.instance();
		V v2 = valueFactory.instance();
		V v3 = valueFactory.instance();

		assertEquals(Long.valueOf(3), listOps.rightPushAll(key, Arrays.<V> asList(v1, v2, v3)));
		assertThat(listOps.range(key, 0, -1), isEqual(Arrays.asList(new Object[] { v1, v2, v3 })));
	}

	@Test(expected = IllegalArgumentException.class) // DATAREDIS-288
	public void rightPushAllShouldThrowExceptionWhenCalledWithEmptyCollection() {
		listOps.rightPushAll(keyFactory.instance(), Collections.<V> emptyList());
	}

	@SuppressWarnings("unchecked")
	@Test(expected = IllegalArgumentException.class) // DATAREDIS-288
	public void rightPushAllShouldThrowExceptionWhenCollectionContainsNullValue() {
		listOps.rightPushAll(keyFactory.instance(), Arrays.asList(valueFactory.instance(), null));
	}

	@Test(expected = IllegalArgumentException.class) // DATAREDIS-288
	public void rightPushAllShouldThrowExceptionWhenCalledWithNull() {
		listOps.rightPushAll(keyFactory.instance(), (Collection<V>) null);
	}

	@Test // DATAREDIS-288
	@SuppressWarnings("unchecked")
	public void testLeftPushAllCollection() {

		K key = keyFactory.instance();

		V v1 = valueFactory.instance();
		V v2 = valueFactory.instance();
		V v3 = valueFactory.instance();

		assertEquals(Long.valueOf(3), listOps.leftPushAll(key, Arrays.<V> asList(v1, v2, v3)));
		assertThat(listOps.range(key, 0, -1), isEqual(Arrays.asList(new Object[] { v3, v2, v1 })));
	}

	@Test(expected = IllegalArgumentException.class) // DATAREDIS-288
	public void leftPushAllShouldThrowExceptionWhenCalledWithEmptyCollection() {
		listOps.leftPushAll(keyFactory.instance(), Collections.<V> emptyList());
	}

	@SuppressWarnings("unchecked")
	@Test(expected = IllegalArgumentException.class) // DATAREDIS-288
	public void leftPushAllShouldThrowExceptionWhenCollectionContainsNullValue() {
		listOps.leftPushAll(keyFactory.instance(), Arrays.asList(valueFactory.instance(), null));
	}

	@Test(expected = IllegalArgumentException.class) // DATAREDIS-288
	public void leftPushAllShouldThrowExceptionWhenCalledWithNull() {
		listOps.leftPushAll(keyFactory.instance(), (Collection<V>) null);
	}
}
