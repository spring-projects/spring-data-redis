/*
 * Copyright 2011-2016 the original author or authors.
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
package org.springframework.data.redis.support.collections;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import static org.junit.Assume.*;
import static org.springframework.data.redis.matcher.RedisTestMatchers.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.springframework.data.redis.DoubleAsStringObjectFactory;
import org.springframework.data.redis.DoubleObjectFactory;
import org.springframework.data.redis.LongAsStringObjectFactory;
import org.springframework.data.redis.LongObjectFactory;
import org.springframework.data.redis.ObjectFactory;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisZSetCommands;
import org.springframework.data.redis.core.BoundZSetOperations;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ZSetOperations.TypedTuple;
import org.springframework.data.redis.test.util.MinimumRedisVersionRule;
import org.springframework.data.redis.test.util.RedisClientRule;
import org.springframework.data.redis.test.util.RedisDriver;
import org.springframework.data.redis.test.util.WithRedisDriver;
import org.springframework.test.annotation.IfProfileValue;

/**
 * Integration test for Redis ZSet.
 * 
 * @author Costin Leau
 * @author Jennifer Hickey
 * @author Thomas Darimont
 * @author Mark Paluch
 */
public abstract class AbstractRedisZSetTest<T> extends AbstractRedisCollectionTests<T> {

	public @Rule RedisClientRule clientRule = new RedisClientRule() {
		public RedisConnectionFactory getConnectionFactory() {
			return template.getConnectionFactory();
		}
	};

	public @Rule MinimumRedisVersionRule versionRule = new MinimumRedisVersionRule();

	protected RedisZSet<T> zSet;

	/**
	 * Constructs a new <code>AbstractRedisZSetTest</code> instance.
	 * 
	 * @param factory
	 * @param template
	 */
	@SuppressWarnings("rawtypes")
	public AbstractRedisZSetTest(ObjectFactory<T> factory, RedisTemplate template) {
		super(factory, template);
	}

	@SuppressWarnings("unchecked")
	@Before
	public void setUp() throws Exception {
		super.setUp();
		zSet = (RedisZSet<T>) collection;
	}

	@Test
	public void testAddWithScore() {
		T t1 = getT();
		T t2 = getT();
		T t3 = getT();

		zSet.add(t1, 3);
		zSet.add(t2, 4);
		zSet.add(t3, 5);

		Iterator<T> iterator = zSet.iterator();
		assertThat(iterator.next(), isEqual(t1));
		assertThat(iterator.next(), isEqual(t2));
		assertThat(iterator.next(), isEqual(t3));
		assertFalse(iterator.hasNext());
	}

	@Test
	public void testAdd() {
		T t1 = getT();
		T t2 = getT();
		T t3 = getT();

		zSet.add(t1);
		zSet.add(t2);
		zSet.add(t3);

		Double d = new Double("1");

		assertEquals(d, zSet.score(t1));
		assertEquals(d, zSet.score(t2));
		assertEquals(d, zSet.score(t3));
	}

	@Test
	public void testFirst() {
		T t1 = getT();
		T t2 = getT();
		T t3 = getT();

		zSet.add(t1, 3);
		zSet.add(t2, 4);
		zSet.add(t3, 5);

		assertEquals(3, zSet.size());
		assertThat(zSet.first(), isEqual(t1));
	}

	@Test(expected = NoSuchElementException.class)
	public void testFirstException() throws Exception {
		zSet.first();
	}

	@Test
	public void testLast() {
		T t1 = getT();
		T t2 = getT();
		T t3 = getT();

		zSet.add(t1, 3);
		zSet.add(t2, 4);
		zSet.add(t3, 5);

		assertEquals(3, zSet.size());
		assertThat(zSet.last(), isEqual(t3));
	}

	@Test(expected = NoSuchElementException.class)
	public void testLastException() throws Exception {
		zSet.last();
	}

	@Test
	public void testRank() {
		T t1 = getT();
		T t2 = getT();
		T t3 = getT();

		zSet.add(t1, 3);
		zSet.add(t2, 4);
		zSet.add(t3, 5);

		assertEquals(Long.valueOf(0), zSet.rank(t1));
		assertEquals(Long.valueOf(1), zSet.rank(t2));
		assertEquals(Long.valueOf(2), zSet.rank(t3));
		assertNull(zSet.rank(getT()));
		// assertNull();
	}

	@Test
	public void testReverseRank() {
		T t1 = getT();
		T t2 = getT();
		T t3 = getT();

		zSet.add(t1, 3);
		zSet.add(t2, 4);
		zSet.add(t3, 5);

		assertEquals(Long.valueOf(0), zSet.reverseRank(t3));
		assertEquals(Long.valueOf(1), zSet.reverseRank(t2));
		assertEquals(Long.valueOf(2), zSet.reverseRank(t1));
		assertNull(zSet.rank(getT()));
	}

	@Test
	public void testScore() {
		T t1 = getT();
		T t2 = getT();
		T t3 = getT();

		zSet.add(t1, 3);
		zSet.add(t2, 4);
		zSet.add(t3, 5);

		assertNull(zSet.score(getT()));
		assertEquals(Double.valueOf(3), zSet.score(t1));
		assertEquals(Double.valueOf(4), zSet.score(t2));
		assertEquals(Double.valueOf(5), zSet.score(t3));
	}

	@Test
	public void testDefaultScore() {
		assertEquals(1, zSet.getDefaultScore(), 0);
	}

	@SuppressWarnings("unchecked")
	private RedisZSet<T> createZSetFor(String key) {
		return new DefaultRedisZSet<T>((BoundZSetOperations<String, T>) zSet.getOperations().boundZSetOps(key));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testIntersectAndStore() {

		RedisZSet<T> interSet1 = createZSetFor("test:zset:inter1");
		RedisZSet<T> interSet2 = createZSetFor("test:zset:inter");

		T t1 = getT();
		T t2 = getT();
		T t3 = getT();
		T t4 = getT();

		zSet.add(t1, 1);
		zSet.add(t2, 2);
		zSet.add(t3, 3);

		interSet1.add(t2, 2);
		interSet1.add(t4, 3);
		interSet2.add(t2, 2);
		interSet2.add(t3, 3);

		String resultName = "test:zset:inter:result:1";
		RedisZSet<T> inter = zSet.intersectAndStore(Arrays.asList(interSet1, interSet2), resultName);

		assertEquals(1, inter.size());
		assertThat(inter, hasItem(t2));
		assertEquals(Double.valueOf(6), inter.score(t2));
		assertEquals(resultName, inter.getKey());
	}

	@Test
	public void testRange() {
		T t1 = getT();
		T t2 = getT();
		T t3 = getT();

		zSet.add(t1, 1);
		zSet.add(t2, 2);
		zSet.add(t3, 3);

		Set<T> range = zSet.range(1, 2);
		assertEquals(2, range.size());
		Iterator<T> iterator = range.iterator();
		assertThat(iterator.next(), isEqual(t2));
		assertThat(iterator.next(), isEqual(t3));
	}

	@Test
	public void testRangeWithScores() {

		T t1 = getT();
		T t2 = getT();
		T t3 = getT();

		zSet.add(t1, 1);
		zSet.add(t2, 2);
		zSet.add(t3, 3);

		Set<TypedTuple<T>> range = zSet.rangeWithScores(1, 2);
		assertEquals(2, range.size());

		Iterator<TypedTuple<T>> iterator = range.iterator();
		TypedTuple<T> tuple1 = iterator.next();
		assertThat(tuple1.getValue(), isEqual(t2));
		assertThat(tuple1.getScore(), isEqual(Double.valueOf(2)));

		TypedTuple<T> tuple2 = iterator.next();
		assertThat(tuple2.getValue(), isEqual(t3));
		assertThat(tuple2.getScore(), isEqual(Double.valueOf(3)));
	}

	@Test
	public void testReverseRange() {
		T t1 = getT();
		T t2 = getT();
		T t3 = getT();

		zSet.add(t1, 1);
		zSet.add(t2, 2);
		zSet.add(t3, 3);

		Set<T> range = zSet.reverseRange(1, 2);
		assertEquals(2, range.size());
		Iterator<T> iterator = range.iterator();
		assertThat(iterator.next(), isEqual(t2));
		assertThat(iterator.next(), isEqual(t1));
	}

	@Test
	public void testReverseRangeWithScores() {

		T t1 = getT();
		T t2 = getT();
		T t3 = getT();

		zSet.add(t1, 1);
		zSet.add(t2, 2);
		zSet.add(t3, 3);

		Set<TypedTuple<T>> range = zSet.reverseRangeWithScores(1, 2);
		assertEquals(2, range.size());

		Iterator<TypedTuple<T>> iterator = range.iterator();
		TypedTuple<T> tuple1 = iterator.next();
		assertThat(tuple1.getValue(), isEqual(t2));
		assertThat(tuple1.getScore(), isEqual(Double.valueOf(2)));

		TypedTuple<T> tuple2 = iterator.next();
		assertThat(tuple2.getValue(), isEqual(t1));
		assertThat(tuple2.getScore(), isEqual(Double.valueOf(1)));
	}

	/**
	 * @see DATAREDIS-407
	 */
	@Test
	public void testRangeByLexUnbounded() {

		assumeThat(factory, anyOf(instanceOf(DoubleObjectFactory.class), instanceOf(DoubleAsStringObjectFactory.class),
				instanceOf(LongAsStringObjectFactory.class), instanceOf(LongObjectFactory.class)));

		T t1 = getT();
		T t2 = getT();
		T t3 = getT();

		zSet.add(t1, 1);
		zSet.add(t2, 2);
		zSet.add(t3, 3);
		Set<T> tuples = zSet.rangeByLex(RedisZSetCommands.Range.unbounded());

		assertEquals(3, tuples.size());
		T tuple = tuples.iterator().next();
		assertThat(tuple, isEqual(t1));
	}

	/**
	 * @see DATAREDIS-407
	 */
	@Test
	public void testRangeByLexBounded() {

		assumeThat(factory, anyOf(instanceOf(DoubleObjectFactory.class), instanceOf(DoubleAsStringObjectFactory.class),
				instanceOf(LongAsStringObjectFactory.class), instanceOf(LongObjectFactory.class)));

		T t1 = getT();
		T t2 = getT();
		T t3 = getT();

		zSet.add(t1, 1);
		zSet.add(t2, 2);
		zSet.add(t3, 3);
		Set<T> tuples = zSet.rangeByLex(RedisZSetCommands.Range.range().gt(t1).lt(t3));

		assertEquals(1, tuples.size());
		T tuple = tuples.iterator().next();
		assertThat(tuple, isEqual(t2));
	}

	/**
	 * @see DATAREDIS-407
	 */
	@Test
	public void testRangeByLexUnboundedWithLimit() {

		assumeThat(factory, anyOf(instanceOf(DoubleObjectFactory.class), instanceOf(DoubleAsStringObjectFactory.class),
				instanceOf(LongAsStringObjectFactory.class), instanceOf(LongObjectFactory.class)));

		T t1 = getT();
		T t2 = getT();
		T t3 = getT();

		zSet.add(t1, 1);
		zSet.add(t2, 2);
		zSet.add(t3, 3);
		Set<T> tuples = zSet.rangeByLex(RedisZSetCommands.Range.unbounded(),
				RedisZSetCommands.Limit.limit().count(1).offset(1));

		assertEquals(1, tuples.size());
		T tuple = tuples.iterator().next();
		assertThat(tuple, isEqual(t2));
	}

	/**
	 * @see DATAREDIS-407
	 */
	@Test
	public void testRangeByLexBoundedWithLimit() {

		assumeThat(factory, anyOf(instanceOf(DoubleObjectFactory.class), instanceOf(DoubleAsStringObjectFactory.class),
				instanceOf(LongAsStringObjectFactory.class), instanceOf(LongObjectFactory.class)));

		T t1 = getT();
		T t2 = getT();
		T t3 = getT();

		zSet.add(t1, 1);
		zSet.add(t2, 2);
		zSet.add(t3, 3);
		Set<T> tuples = zSet.rangeByLex(RedisZSetCommands.Range.range().gte(t1),
				RedisZSetCommands.Limit.limit().count(1).offset(1));

		assertEquals(1, tuples.size());
		T tuple = tuples.iterator().next();
		assertThat(tuple, isEqual(t2));
	}

	@Test
	public void testReverseRangeByScore() {

		T t1 = getT();
		T t2 = getT();
		T t3 = getT();

		zSet.add(t1, 1);
		zSet.add(t2, 2);
		zSet.add(t3, 3);

		Set<T> range = zSet.reverseRangeByScore(1.5, 3.5);
		assertEquals(2, range.size());
		Iterator<T> iterator = range.iterator();
		assertThat(iterator.next(), isEqual(t3));
		assertThat(iterator.next(), isEqual(t2));
	}

	@Test
	public void testReverseRangeByScoreWithScores() {

		T t1 = getT();
		T t2 = getT();
		T t3 = getT();

		zSet.add(t1, 1);
		zSet.add(t2, 2);
		zSet.add(t3, 3);

		Set<TypedTuple<T>> range = zSet.reverseRangeByScoreWithScores(1.5, 3.5);
		assertEquals(2, range.size());

		Iterator<TypedTuple<T>> iterator = range.iterator();
		TypedTuple<T> tuple1 = iterator.next();
		assertThat(tuple1.getValue(), isEqual(t3));
		assertThat(tuple1.getScore(), isEqual(Double.valueOf(3)));

		TypedTuple<T> tuple2 = iterator.next();
		assertThat(tuple2.getValue(), isEqual(t2));
		assertThat(tuple2.getScore(), isEqual(Double.valueOf(2)));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testRangeByScore() {
		T t1 = getT();
		T t2 = getT();
		T t3 = getT();

		zSet.add(t1, 1);
		zSet.add(t2, 2);
		zSet.add(t3, 3);

		Set<T> range = zSet.rangeByScore(1.5, 3.5);
		assertEquals(2, range.size());
		assertThat(range, hasItems(t2, t3));

		Iterator<T> iterator = range.iterator();
		assertThat(iterator.next(), isEqual(t2));
		assertThat(iterator.next(), isEqual(t3));
	}

	@Test
	public void testRangeByScoreWithScores() {

		T t1 = getT();
		T t2 = getT();
		T t3 = getT();

		zSet.add(t1, 1);
		zSet.add(t2, 2);
		zSet.add(t3, 3);

		Set<TypedTuple<T>> range = zSet.rangeByScoreWithScores(1.5, 3.5);
		assertEquals(2, range.size());

		Iterator<TypedTuple<T>> iterator = range.iterator();
		TypedTuple<T> tuple1 = iterator.next();
		assertThat(tuple1.getValue(), isEqual(t2));
		assertThat(tuple1.getScore(), isEqual(Double.valueOf(2)));

		TypedTuple<T> tuple2 = iterator.next();
		assertThat(tuple2.getValue(), isEqual(t3));
		assertThat(tuple2.getScore(), isEqual(Double.valueOf(3)));
	}

	@Test
	public void testRemove() {
		T t1 = getT();
		T t2 = getT();
		T t3 = getT();
		T t4 = getT();

		zSet.add(t1, 1);
		zSet.add(t2, 2);
		zSet.add(t3, 3);
		zSet.add(t4, 4);

		zSet.remove(1, 2);

		assertEquals(2, zSet.size());
		Iterator<T> iterator = zSet.iterator();
		assertThat(iterator.next(), isEqual(t1));
		assertThat(iterator.next(), isEqual(t4));
	}

	@Test
	public void testRemoveByScore() {
		T t1 = getT();
		T t2 = getT();
		T t3 = getT();
		T t4 = getT();

		zSet.add(t1, 1);
		zSet.add(t2, 2);
		zSet.add(t3, 3);
		zSet.add(t4, 4);

		zSet.removeByScore(1.5, 2.5);

		assertEquals(3, zSet.size());
		Iterator<T> iterator = zSet.iterator();
		assertThat(iterator.next(), isEqual(t1));
		assertThat(iterator.next(), isEqual(t3));
		assertThat(iterator.next(), isEqual(t4));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testUnionAndStore() {

		RedisZSet<T> unionSet1 = createZSetFor("test:zset:union1");
		RedisZSet<T> unionSet2 = createZSetFor("test:zset:union2");

		T t1 = getT();
		T t2 = getT();
		T t3 = getT();
		T t4 = getT();

		zSet.add(t1, 1);
		zSet.add(t2, 2);

		unionSet1.add(t2, 2);
		unionSet1.add(t4, 5);
		unionSet2.add(t3, 6);

		String resultName = "test:zset:union:result:1";
		RedisZSet<T> union = zSet.unionAndStore(Arrays.asList(unionSet1, unionSet2), resultName);
		assertEquals(4, union.size());
		assertThat(union, hasItems(t1, t2, t3, t4));
		assertEquals(resultName, union.getKey());

		assertEquals(Double.valueOf(1), union.score(t1));
		assertEquals(Double.valueOf(4), union.score(t2));
		assertEquals(Double.valueOf(6), union.score(t3));
		assertEquals(Double.valueOf(5), union.score(t4));
	}

	@Test
	public void testIterator() {
		T t1 = getT();
		T t2 = getT();
		T t3 = getT();
		T t4 = getT();

		zSet.add(t1, 1);
		zSet.add(t2, 2);
		zSet.add(t3, 3);
		zSet.add(t4, 4);

		Iterator<T> iterator = collection.iterator();

		assertThat(iterator.next(), isEqual(t1));
		assertThat(iterator.next(), isEqual(t2));
		assertThat(iterator.next(), isEqual(t3));
		assertThat(iterator.next(), isEqual(t4));
		assertFalse(iterator.hasNext());
	}

	@Test
	public void testToArray() {
		T t1 = getT();
		T t2 = getT();
		T t3 = getT();
		T t4 = getT();

		zSet.add(t1, 1);
		zSet.add(t2, 2);
		zSet.add(t3, 3);
		zSet.add(t4, 4);

		Object[] array = collection.toArray();
		assertArrayEquals(new Object[] { t1, t2, t3, t4 }, array);
	}

	@Test
	public void testToArrayWithGenerics() {
		T t1 = getT();
		T t2 = getT();
		T t3 = getT();
		T t4 = getT();

		zSet.add(t1, 1);
		zSet.add(t2, 2);
		zSet.add(t3, 3);
		zSet.add(t4, 4);

		Object[] array = collection.toArray(new Object[zSet.size()]);
		assertArrayEquals(new Object[] { t1, t2, t3, t4 }, array);
	}

	/**
	 * @see DATAREDIS-314
	 */
	@IfProfileValue(name = "redisVersion", value = "2.8+")
	@Test
	@WithRedisDriver({ RedisDriver.JEDIS, RedisDriver.LETTUCE })
	public void testScanWorksCorrectly() throws IOException {

		T t1 = getT();
		T t2 = getT();
		T t3 = getT();
		T t4 = getT();

		zSet.add(t1, 1);
		zSet.add(t2, 2);
		zSet.add(t3, 3);
		zSet.add(t4, 4);

		Cursor<T> cursor = (Cursor<T>) zSet.scan();
		while (cursor.hasNext()) {
			assertThat(cursor.next(), anyOf(equalTo(t1), equalTo(t2), equalTo(t3), equalTo(t4)));
		}

		cursor.close();

	}
}
