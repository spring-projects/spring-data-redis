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

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.springframework.data.redis.ObjectFactory;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.ZSetOperations.TypedTuple;

/**
 * Integration test of {@link DefaultZSetOperations}
 * @author Jennifer Hickey
 *
 * @param <K> Key type
 * @param <V> Value type
 */
@RunWith(Parameterized.class)
public class DefaultZSetOperationsTests<K,V> {

	private RedisTemplate<K,V> redisTemplate;

	private ObjectFactory<K> keyFactory;

	private ObjectFactory<V> valueFactory;

	private ZSetOperations<K, V> zSetOps;

	public DefaultZSetOperationsTests(RedisTemplate<K,V> redisTemplate, ObjectFactory<K> keyFactory,
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
		zSetOps = redisTemplate.opsForZSet();
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
	public void testCount() {
		K key1 = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();
		zSetOps.add(key1, value1, 2.5);
		zSetOps.add(key1, value2, 5.5);
		assertEquals(Long.valueOf(1), zSetOps.count(key1, 2.7, 5.7));
	}

	@Test
	public void testIncrementScore() {
		K key1 = keyFactory.instance();
		V value1 = valueFactory.instance();
		zSetOps.add(key1, value1, 2.5);
		assertEquals(Double.valueOf(5.7), zSetOps.incrementScore(key1, value1, 3.2));
		Set<TypedTuple<V>> values = zSetOps.rangeWithScores(key1, 0, -1);
		assertEquals(1,values.size());
		TypedTuple<V> tuple = values.iterator().next();
		assertEquals(value1, tuple.getValue());
		assertEquals(Double.valueOf(5.7), tuple.getScore());
	}

	@Test
	public void testRangeByScoreOffsetCount() {
		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();
		V value3 = valueFactory.instance();
		zSetOps.add(key, value1, 1.9);
		zSetOps.add(key, value2, 3.7);
		zSetOps.add(key, value3, 5.8);
		assertEquals(Collections.singleton(value1), zSetOps.rangeByScore(key, 1.5, 4.7, 0, 1));
	}

	@Test
	public void testRangeByScoreWithScoresOffsetCount() {
		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();
		V value3 = valueFactory.instance();
		zSetOps.add(key, value1, 1.9);
		zSetOps.add(key, value2, 3.7);
		zSetOps.add(key, value3, 5.8);
		Set<TypedTuple<V>> tuples = zSetOps.rangeByScoreWithScores(key, 1.5, 4.7, 0, 1);
		assertEquals(1, tuples.size());
		TypedTuple<V> tuple = tuples.iterator().next();
		assertEquals(value1,tuple.getValue());
		assertEquals(Double.valueOf(1.9), tuple.getScore());
	}

	@Test
	public void testReverseRangeByScoreOffsetCount() {
		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();
		V value3 = valueFactory.instance();
		zSetOps.add(key, value1, 1.9);
		zSetOps.add(key, value2, 3.7);
		zSetOps.add(key, value3, 5.8);
		assertEquals(Collections.singleton(value2), zSetOps.reverseRangeByScore(key, 1.5, 4.7, 0, 1));
	}

	@Test
	public void testReverseRangeByScoreWithScoresOffsetCount() {
		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();
		V value3 = valueFactory.instance();
		zSetOps.add(key, value1, 1.9);
		zSetOps.add(key, value2, 3.7);
		zSetOps.add(key, value3, 5.8);
		Set<TypedTuple<V>> tuples = zSetOps.reverseRangeByScoreWithScores(key, 1.5, 4.7, 0, 1);
		assertEquals(1, tuples.size());
		TypedTuple<V> tuple = tuples.iterator().next();
		assertEquals(value2,tuple.getValue());
		assertEquals(Double.valueOf(3.7), tuple.getScore());
	}
}
