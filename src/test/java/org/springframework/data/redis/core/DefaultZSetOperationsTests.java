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
import static org.springframework.data.redis.matcher.RedisTestMatchers.*;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.springframework.data.redis.ConnectionFactoryTracker;
import org.springframework.data.redis.DoubleAsStringObjectFactory;
import org.springframework.data.redis.DoubleObjectFactory;
import org.springframework.data.redis.LongAsStringObjectFactory;
import org.springframework.data.redis.LongObjectFactory;
import org.springframework.data.redis.ObjectFactory;
import org.springframework.data.redis.connection.RedisZSetCommands;
import org.springframework.data.redis.connection.RedisZSetCommands.Weights;
import org.springframework.data.redis.core.ZSetOperations.TypedTuple;
import org.springframework.data.redis.test.util.MinimumRedisVersionRule;
import org.springframework.test.annotation.IfProfileValue;

/**
 * Integration test of {@link DefaultZSetOperations}
 *
 * @author Jennifer Hickey
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Wongoo (望哥)
 * @param <K> Key type
 * @param <V> Value type
 */
@RunWith(Parameterized.class)
public class DefaultZSetOperationsTests<K, V> {

	public @Rule MinimumRedisVersionRule versionRule = new MinimumRedisVersionRule();

	private RedisTemplate<K, V> redisTemplate;

	private ObjectFactory<K> keyFactory;

	private ObjectFactory<V> valueFactory;

	private ZSetOperations<K, V> zSetOps;

	public DefaultZSetOperationsTests(RedisTemplate<K, V> redisTemplate, ObjectFactory<K> keyFactory,
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
		zSetOps = redisTemplate.opsForZSet();
	}

	@After
	public void tearDown() {
		redisTemplate.execute((RedisCallback<Object>) connection -> {
			connection.flushDb();
			return null;
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
		assertEquals(1, values.size());
		TypedTuple<V> tuple = values.iterator().next();
		assertEquals(new DefaultTypedTuple<>(value1, 5.7), tuple);
	}

	@Test
	public void testRangeByScoreOffsetCount() {
		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();
		V value3 = valueFactory.instance();
		System.out.println(value1 + "*" + value2 + "*" + value3);
		zSetOps.add(key, value1, 1.9);
		zSetOps.add(key, value2, 3.7);
		zSetOps.add(key, value3, 5.8);
		assertThat(zSetOps.rangeByScore(key, 1.5, 4.7, 0, 1), isEqual(Collections.singleton(value1)));
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
		assertThat(tuple, isEqual(new DefaultTypedTuple<>(value1, 1.9)));
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
		assertThat(zSetOps.reverseRangeByScore(key, 1.5, 4.7, 0, 1), isEqual(Collections.singleton(value2)));
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
		assertThat(tuple, isEqual(new DefaultTypedTuple<>(value2, 3.7)));
	}

	@Test // DATAREDIS-407
	public void testRangeByLexUnbounded() {

		assumeThat(valueFactory, anyOf(instanceOf(DoubleObjectFactory.class), instanceOf(DoubleAsStringObjectFactory.class),
				instanceOf(LongAsStringObjectFactory.class), instanceOf(LongObjectFactory.class)));

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();
		V value3 = valueFactory.instance();

		zSetOps.add(key, value1, 1.9);
		zSetOps.add(key, value2, 3.7);
		zSetOps.add(key, value3, 5.8);
		Set<V> tuples = zSetOps.rangeByLex(key, RedisZSetCommands.Range.unbounded());

		assertEquals(3, tuples.size());
		V tuple = tuples.iterator().next();
		assertThat(tuple, isEqual(value1));
	}

	@Test // DATAREDIS-407
	public void testRangeByLexBounded() {

		assumeThat(valueFactory, anyOf(instanceOf(DoubleObjectFactory.class), instanceOf(DoubleAsStringObjectFactory.class),
				instanceOf(LongAsStringObjectFactory.class), instanceOf(LongObjectFactory.class)));

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();
		V value3 = valueFactory.instance();

		zSetOps.add(key, value1, 1.9);
		zSetOps.add(key, value2, 3.7);
		zSetOps.add(key, value3, 5.8);
		Set<V> tuples = zSetOps.rangeByLex(key, RedisZSetCommands.Range.range().gt(value1).lt(value3));

		assertEquals(1, tuples.size());
		V tuple = tuples.iterator().next();
		assertThat(tuple, isEqual(value2));
	}

	@Test // DATAREDIS-407
	public void testRangeByLexUnboundedWithLimit() {

		assumeThat(valueFactory, anyOf(instanceOf(DoubleObjectFactory.class), instanceOf(DoubleAsStringObjectFactory.class),
				instanceOf(LongAsStringObjectFactory.class), instanceOf(LongObjectFactory.class)));

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();
		V value3 = valueFactory.instance();

		zSetOps.add(key, value1, 1.9);
		zSetOps.add(key, value2, 3.7);
		zSetOps.add(key, value3, 5.8);
		Set<V> tuples = zSetOps.rangeByLex(key, RedisZSetCommands.Range.unbounded(),
				RedisZSetCommands.Limit.limit().count(1).offset(1));

		assertEquals(1, tuples.size());
		V tuple = tuples.iterator().next();
		assertThat(tuple, isEqual(value2));
	}

	@Test // DATAREDIS-407
	public void testRangeByLexBoundedWithLimit() {

		assumeThat(valueFactory, anyOf(instanceOf(DoubleObjectFactory.class), instanceOf(DoubleAsStringObjectFactory.class),
				instanceOf(LongAsStringObjectFactory.class), instanceOf(LongObjectFactory.class)));

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();
		V value3 = valueFactory.instance();

		zSetOps.add(key, value1, 1.9);
		zSetOps.add(key, value2, 3.7);
		zSetOps.add(key, value3, 5.8);
		Set<V> tuples = zSetOps.rangeByLex(key, RedisZSetCommands.Range.range().gte(value1),
				RedisZSetCommands.Limit.limit().count(1).offset(1));

		assertEquals(1, tuples.size());
		V tuple = tuples.iterator().next();
		assertThat(tuple, isEqual(value2));
	}

	@Test
	public void testAddMultiple() {
		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();
		V value3 = valueFactory.instance();
		Set<TypedTuple<V>> values = new HashSet<>();
		values.add(new DefaultTypedTuple<>(value1, 1.7));
		values.add(new DefaultTypedTuple<>(value2, 3.2));
		values.add(new DefaultTypedTuple<>(value3, 0.8));
		assertEquals(Long.valueOf(3), zSetOps.add(key, values));
		Set<V> expected = new LinkedHashSet<>();
		expected.add(value3);
		expected.add(value1);
		expected.add(value2);
		assertThat(zSetOps.range(key, 0, -1), isEqual(expected));
	}

	@Test
	public void testRemove() {
		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();
		V value3 = valueFactory.instance();
		Set<TypedTuple<V>> values = new HashSet<>();
		values.add(new DefaultTypedTuple<>(value1, 1.7));
		values.add(new DefaultTypedTuple<>(value2, 3.2));
		values.add(new DefaultTypedTuple<>(value3, 0.8));
		assertEquals(Long.valueOf(3), zSetOps.add(key, values));
		assertEquals(Long.valueOf(2), zSetOps.remove(key, value1, value3));
		Set<V> expected = new LinkedHashSet<>();
		expected.add(value2);
		assertThat(zSetOps.range(key, 0, -1), isEqual(expected));
	}

	@Test
	public void zCardRetrievesDataCorrectly() {

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();
		V value3 = valueFactory.instance();

		Set<TypedTuple<V>> values = new HashSet<>();
		values.add(new DefaultTypedTuple<>(value1, 1.7));
		values.add(new DefaultTypedTuple<>(value2, 3.2));
		values.add(new DefaultTypedTuple<>(value3, 0.8));
		zSetOps.add(key, values);

		assertThat(zSetOps.zCard(key), equalTo(3L));
	}

	@Test
	public void sizeRetrievesDataCorrectly() {

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();
		V value3 = valueFactory.instance();

		Set<TypedTuple<V>> values = new HashSet<>();
		values.add(new DefaultTypedTuple<>(value1, 1.7));
		values.add(new DefaultTypedTuple<>(value2, 3.2));
		values.add(new DefaultTypedTuple<>(value3, 0.8));
		zSetOps.add(key, values);

		assertThat(zSetOps.size(key), equalTo(3L));
	}

	@Test // DATAREDIS-306
	@IfProfileValue(name = "redisVersion", value = "2.8+")
	public void testZScanShouldReadEntireValueRange() throws IOException {

		K key = keyFactory.instance();

		final TypedTuple<V> tuple1 = new DefaultTypedTuple<>(valueFactory.instance(), 1.7);
		final TypedTuple<V> tuple2 = new DefaultTypedTuple<>(valueFactory.instance(), 3.2);
		final TypedTuple<V> tuple3 = new DefaultTypedTuple<>(valueFactory.instance(), 0.8);

		Set<TypedTuple<V>> values = new HashSet<TypedTuple<V>>() {
			{
				add(tuple1);
				add(tuple2);
				add(tuple3);
			}
		};

		zSetOps.add(key, values);

		int count = 0;
		Cursor<TypedTuple<V>> it = zSetOps.scan(key, ScanOptions.scanOptions().count(2).build());
		while (it.hasNext()) {
			assertThat(it.next(), anyOf(equalTo(tuple1), equalTo(tuple2), equalTo(tuple3)));
			count++;
		}

		it.close();
		assertThat(count, equalTo(3));
	}

	@Test // DATAREDIS-746
	public void testZsetUnionWithAggregate() {

		K key1 = keyFactory.instance();
		K key2 = keyFactory.instance();

		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		zSetOps.add(key1, value1, 1.0);
		zSetOps.add(key1, value2, 2.0);
		zSetOps.add(key2, value2, 3.0);

		zSetOps.unionAndStore(key1, Collections.singletonList(key2), key1, RedisZSetCommands.Aggregate.MIN);

		assertThat(zSetOps.score(key1, value2), closeTo(2.0, 0.1));
	}

	@Test // DATAREDIS-746
	public void testZsetUnionWithAggregateWeights() {

		K key1 = keyFactory.instance();
		K key2 = keyFactory.instance();
		V value1 = valueFactory.instance();

		zSetOps.add(key1, value1, 4.0);
		zSetOps.add(key2, value1, 3.0);

		zSetOps.unionAndStore(key1, Collections.singletonList(key2), key1, RedisZSetCommands.Aggregate.MAX,
				Weights.of(1, 2));

		assertThat(zSetOps.score(key1, value1), closeTo(6.0, 0.1));
	}

	@Test // DATAREDIS-746
	public void testZsetIntersectWithAggregate() {

		K key1 = keyFactory.instance();
		K key2 = keyFactory.instance();

		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		zSetOps.add(key1, value1, 1.0);
		zSetOps.add(key1, value2, 2.0);
		zSetOps.add(key2, value2, 3.0);

		zSetOps.intersectAndStore(key1, Collections.singletonList(key2), key1, RedisZSetCommands.Aggregate.MIN);

		assertThat(zSetOps.score(key1, value2), closeTo(2.0, 0.1));
	}

	@Test // DATAREDIS-746
	public void testZsetIntersectWithAggregateWeights() {

		K key1 = keyFactory.instance();
		K key2 = keyFactory.instance();
		V value1 = valueFactory.instance();

		zSetOps.add(key1, value1, 4.0);
		zSetOps.add(key2, value1, 3.0);

		zSetOps.intersectAndStore(key1, Collections.singletonList(key2), key1, RedisZSetCommands.Aggregate.MAX,
				Weights.of(1, 2));

		assertThat(zSetOps.score(key1, value1), closeTo(6.0, 0.1));
	}
}
