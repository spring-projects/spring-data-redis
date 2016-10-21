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

import static org.assertj.core.api.Assertions.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assume.*;

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
import org.springframework.data.redis.connection.ConnectionUtils;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisZSetCommands;
import org.springframework.data.redis.core.BoundZSetOperations;
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
		assertThat(iterator.next()).isEqualTo(t1);
		assertThat(iterator.next()).isEqualTo(t2);
		assertThat(iterator.next()).isEqualTo(t3);
		assertThat(iterator.hasNext()).isFalse();
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

		assertThat(zSet.score(t1)).isEqualTo(d);
		assertThat(zSet.score(t2)).isEqualTo(d);
		assertThat(zSet.score(t3)).isEqualTo(d);
	}

	@Test
	public void testFirst() {
		T t1 = getT();
		T t2 = getT();
		T t3 = getT();

		zSet.add(t1, 3);
		zSet.add(t2, 4);
		zSet.add(t3, 5);

		assertThat(zSet).hasSize(3);
		assertThat(zSet.first()).isEqualTo(t1);
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

		assertThat(zSet).hasSize(3);
		assertThat(zSet.last()).isEqualTo(t3);
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

		assertThat(zSet.rank(t1)).isEqualTo(Long.valueOf(0));
		assertThat(zSet.rank(t2)).isEqualTo(Long.valueOf(1));
		assertThat(zSet.rank(t3)).isEqualTo(Long.valueOf(2));
		assertThat(zSet.rank(getT())).isNull();
		// assertThat().isNull();
	}

	@Test
	public void testReverseRank() {
		T t1 = getT();
		T t2 = getT();
		T t3 = getT();

		zSet.add(t1, 3);
		zSet.add(t2, 4);
		zSet.add(t3, 5);

		assertThat(zSet.reverseRank(t3)).isEqualTo(Long.valueOf(0));
		assertThat(zSet.reverseRank(t2)).isEqualTo(Long.valueOf(1));
		assertThat(zSet.reverseRank(t1)).isEqualTo(Long.valueOf(2));
		assertThat(zSet.rank(getT())).isNull();
	}

	@Test
	public void testScore() {
		T t1 = getT();
		T t2 = getT();
		T t3 = getT();

		zSet.add(t1, 3);
		zSet.add(t2, 4);
		zSet.add(t3, 5);

		assertThat(zSet.score(getT())).isNull();
		assertThat(zSet.score(t1)).isEqualTo(Double.valueOf(3));
		assertThat(zSet.score(t2)).isEqualTo(Double.valueOf(4));
		assertThat(zSet.score(t3)).isEqualTo(Double.valueOf(5));
	}

	@Test
	public void testDefaultScore() {
		assertThat(zSet.getDefaultScore()).isEqualTo(1);
	}

	@SuppressWarnings("unchecked")
	private RedisZSet<T> createZSetFor(String key) {
		return new DefaultRedisZSet<T>((BoundZSetOperations<String, T>) zSet.getOperations().boundZSetOps(key));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testIntersectAndStore() {
		assumeTrue(!ConnectionUtils.isJredis(template.getConnectionFactory()));
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

		assertThat(inter).hasSize(1);
		assertThat(inter).contains(t2);
		assertThat(inter.score(t2)).isEqualTo(Double.valueOf(6));
		assertThat(inter.getKey()).isEqualTo(resultName);
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
		assertThat(range).hasSize(2);
		Iterator<T> iterator = range.iterator();
		assertThat(iterator.next()).isEqualTo(t2);
		assertThat(iterator.next()).isEqualTo(t3);
	}

	@Test
	public void testRangeWithScores() {
		assumeTrue(!ConnectionUtils.isJredis(template.getConnectionFactory()));
		T t1 = getT();
		T t2 = getT();
		T t3 = getT();

		zSet.add(t1, 1);
		zSet.add(t2, 2);
		zSet.add(t3, 3);

		Set<TypedTuple<T>> range = zSet.rangeWithScores(1, 2);
		assertThat(range).hasSize(2);

		Iterator<TypedTuple<T>> iterator = range.iterator();
		TypedTuple<T> tuple1 = iterator.next();
		assertThat(tuple1.getValue()).isEqualTo(t2);
		assertThat(tuple1.getScore()).isEqualTo(Double.valueOf(2));

		TypedTuple<T> tuple2 = iterator.next();
		assertThat(tuple2.getValue()).isEqualTo(t3);
		assertThat(tuple2.getScore()).isEqualTo(Double.valueOf(3));
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
		assertThat(range).hasSize(2);
		Iterator<T> iterator = range.iterator();
		assertThat(iterator.next()).isEqualTo(t2);
		assertThat(iterator.next()).isEqualTo(t1);
	}

	@Test
	public void testReverseRangeWithScores() {
		assumeTrue(!ConnectionUtils.isJredis(template.getConnectionFactory()));
		T t1 = getT();
		T t2 = getT();
		T t3 = getT();

		zSet.add(t1, 1);
		zSet.add(t2, 2);
		zSet.add(t3, 3);

		Set<TypedTuple<T>> range = zSet.reverseRangeWithScores(1, 2);
		assertThat(range).hasSize(2);

		Iterator<TypedTuple<T>> iterator = range.iterator();
		TypedTuple<T> tuple1 = iterator.next();
		assertThat(tuple1.getValue()).isEqualTo(t2);
		assertThat(tuple1.getScore()).isEqualTo(Double.valueOf(2));

		TypedTuple<T> tuple2 = iterator.next();
		assertThat(tuple2.getValue()).isEqualTo(t1);
		assertThat(tuple2.getScore()).isEqualTo(Double.valueOf(1));
	}

	@Test // DATAREDIS-407
	public void testRangeByLexUnbounded() {

		assumeThat(
				factory,
				anyOf(instanceOf(DoubleObjectFactory.class), instanceOf(DoubleAsStringObjectFactory.class),
						instanceOf(LongAsStringObjectFactory.class), instanceOf(LongObjectFactory.class)));

		T t1 = getT();
		T t2 = getT();
		T t3 = getT();

		zSet.add(t1, 1);
		zSet.add(t2, 2);
		zSet.add(t3, 3);
		Set<T> tuples = zSet.rangeByLex(RedisZSetCommands.Range.unbounded());

		assertThat(tuples).hasSize(3).contains(t1, t2, t3);
	}

	@Test // DATAREDIS-407
	public void testRangeByLexBounded() {

		assumeThat(
				factory,
				anyOf(instanceOf(DoubleObjectFactory.class), instanceOf(DoubleAsStringObjectFactory.class),
						instanceOf(LongAsStringObjectFactory.class), instanceOf(LongObjectFactory.class)));

		T t1 = getT();
		T t2 = getT();
		T t3 = getT();

		zSet.add(t1, 1);
		zSet.add(t2, 2);
		zSet.add(t3, 3);
		Set<T> tuples = zSet.rangeByLex(RedisZSetCommands.Range.range().gt(t1).lt(t3));

		assertThat(tuples).hasSize(1).contains(t2);
	}

	@Test // DATAREDIS-407
	public void testRangeByLexUnboundedWithLimit() {

		assumeThat(
				factory,
				anyOf(instanceOf(DoubleObjectFactory.class), instanceOf(DoubleAsStringObjectFactory.class),
						instanceOf(LongAsStringObjectFactory.class), instanceOf(LongObjectFactory.class)));

		T t1 = getT();
		T t2 = getT();
		T t3 = getT();

		zSet.add(t1, 1);
		zSet.add(t2, 2);
		zSet.add(t3, 3);
		Set<T> tuples = zSet.rangeByLex(RedisZSetCommands.Range.unbounded(), RedisZSetCommands.Limit.limit().count(1)
				.offset(1));

		assertThat(tuples).hasSize(1).contains(t2);
	}

	@Test // DATAREDIS-407
	public void testRangeByLexBoundedWithLimit() {

		assumeThat(
				factory,
				anyOf(instanceOf(DoubleObjectFactory.class), instanceOf(DoubleAsStringObjectFactory.class),
						instanceOf(LongAsStringObjectFactory.class), instanceOf(LongObjectFactory.class)));

		T t1 = getT();
		T t2 = getT();
		T t3 = getT();

		zSet.add(t1, 1);
		zSet.add(t2, 2);
		zSet.add(t3, 3);
		Set<T> tuples = zSet.rangeByLex(RedisZSetCommands.Range.range().gte(t1), RedisZSetCommands.Limit.limit().count(1)
				.offset(1));

		assertThat(tuples).hasSize(1).contains(t2);
	}

	@Test
	public void testReverseRangeByScore() {
		assumeTrue(!ConnectionUtils.isJredis(template.getConnectionFactory()));
		T t1 = getT();
		T t2 = getT();
		T t3 = getT();

		zSet.add(t1, 1);
		zSet.add(t2, 2);
		zSet.add(t3, 3);

		Set<T> range = zSet.reverseRangeByScore(1.5, 3.5);
		assertThat(range).hasSize(2).contains(t3, t2);
	}

	@Test
	public void testReverseRangeByScoreWithScores() {
		assumeTrue(!ConnectionUtils.isJredis(template.getConnectionFactory()));
		T t1 = getT();
		T t2 = getT();
		T t3 = getT();

		zSet.add(t1, 1);
		zSet.add(t2, 2);
		zSet.add(t3, 3);

		Set<TypedTuple<T>> range = zSet.reverseRangeByScoreWithScores(1.5, 3.5);
		assertThat(range).hasSize(2);

		Iterator<TypedTuple<T>> iterator = range.iterator();
		TypedTuple<T> tuple1 = iterator.next();
		assertThat(tuple1.getValue()).isEqualTo(t3);
		assertThat(tuple1.getScore()).isEqualTo(Double.valueOf(3));

		TypedTuple<T> tuple2 = iterator.next();
		assertThat(tuple2.getValue()).isEqualTo(t2);
		assertThat(tuple2.getScore()).isEqualTo(Double.valueOf(2));
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
		assertThat(range).hasSize(2);
		assertThat(range).contains(t2, t3);

		Iterator<T> iterator = range.iterator();
		assertThat(iterator.next()).isEqualTo(t2);
		assertThat(iterator.next()).isEqualTo(t3);
	}

	@Test
	public void testRangeByScoreWithScores() {
		assumeTrue(!ConnectionUtils.isJredis(template.getConnectionFactory()));
		T t1 = getT();
		T t2 = getT();
		T t3 = getT();

		zSet.add(t1, 1);
		zSet.add(t2, 2);
		zSet.add(t3, 3);

		Set<TypedTuple<T>> range = zSet.rangeByScoreWithScores(1.5, 3.5);
		assertThat(range).hasSize(2);

		Iterator<TypedTuple<T>> iterator = range.iterator();
		TypedTuple<T> tuple1 = iterator.next();
		assertThat(tuple1.getValue()).isEqualTo(t2);
		assertThat(tuple1.getScore()).isEqualTo(Double.valueOf(2));

		TypedTuple<T> tuple2 = iterator.next();
		assertThat(tuple2.getValue()).isEqualTo(t3);
		assertThat(tuple2.getScore()).isEqualTo(Double.valueOf(3));
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

		assertThat(zSet).hasSize(2);
		Iterator<T> iterator = zSet.iterator();
		assertThat(iterator.next()).isEqualTo(t1);
		assertThat(iterator.next()).isEqualTo(t4);
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

		assertThat(zSet).hasSize(3);
		Iterator<T> iterator = zSet.iterator();
		assertThat(iterator.next()).isEqualTo(t1);
		assertThat(iterator.next()).isEqualTo(t3);
		assertThat(iterator.next()).isEqualTo(t4);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testUnionAndStore() {
		assumeTrue(!ConnectionUtils.isJredis(template.getConnectionFactory()));
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
		assertThat(union).hasSize(4);
		assertThat(union).contains(t1, t2, t3, t4);
		assertThat(union.getKey()).isEqualTo(resultName);

		assertThat(union.score(t1)).isEqualTo(Double.valueOf(1));
		assertThat(union.score(t2)).isEqualTo(Double.valueOf(4));
		assertThat(union.score(t3)).isEqualTo(Double.valueOf(6));
		assertThat(union.score(t4)).isEqualTo(Double.valueOf(5));
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

		assertThat(iterator.next()).isEqualTo(t1);
		assertThat(iterator.next()).isEqualTo(t2);
		assertThat(iterator.next()).isEqualTo(t3);
		assertThat(iterator.next()).isEqualTo(t4);
		assertThat(iterator.hasNext()).isFalse();
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
		assertThat(array).isEqualTo(new Object[] { t1, t2, t3, t4 });
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
		assertThat(array).isEqualTo(new Object[] { t1, t2, t3, t4 });
	}

	@IfProfileValue(name = "redisVersion", value = "2.8+")
	@Test // DATAREDIS-314
	@WithRedisDriver({ RedisDriver.JEDIS, RedisDriver.LETTUCE })
	public void testScanWorksCorrectly() {

		T t1 = getT();
		T t2 = getT();
		T t3 = getT();
		T t4 = getT();

		zSet.add(t1, 1);
		zSet.add(t2, 2);
		zSet.add(t3, 3);
		zSet.add(t4, 4);

		Iterator<T> cursor = zSet.scan();
		assertThat(cursor).contains(t1, t2, t3, t4);
	}
}
