/*
 * Copyright 2013-2022 the original author or authors.
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
import static org.assertj.core.api.Assumptions.*;
import static org.assertj.core.data.Offset.offset;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;

import org.springframework.data.domain.Range;
import org.springframework.data.redis.DoubleAsStringObjectFactory;
import org.springframework.data.redis.DoubleObjectFactory;
import org.springframework.data.redis.LongAsStringObjectFactory;
import org.springframework.data.redis.LongObjectFactory;
import org.springframework.data.redis.ObjectFactory;
import org.springframework.data.redis.RawObjectFactory;
import org.springframework.data.redis.connection.Limit;
import org.springframework.data.redis.connection.zset.Aggregate;
import org.springframework.data.redis.connection.zset.Weights;
import org.springframework.data.redis.core.ZSetOperations.TypedTuple;
import org.springframework.data.redis.test.condition.EnabledOnCommand;
import org.springframework.data.redis.test.extension.parametrized.MethodSource;
import org.springframework.data.redis.test.extension.parametrized.ParameterizedRedisTest;

/**
 * Integration test of {@link DefaultZSetOperations}
 *
 * @author Jennifer Hickey
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Wongoo (望哥)
 * @author Andrey Shlykov
 * @param <K> Key type
 * @param <V> Value type
 */
@SuppressWarnings("unchecked")
@MethodSource("testParams")
public class DefaultZSetOperationsIntegrationTests<K, V> {

	private final RedisTemplate<K, V> redisTemplate;
	private final ObjectFactory<K> keyFactory;
	private final ObjectFactory<V> valueFactory;
	private final ZSetOperations<K, V> zSetOps;

	public DefaultZSetOperationsIntegrationTests(RedisTemplate<K, V> redisTemplate, ObjectFactory<K> keyFactory,
			ObjectFactory<V> valueFactory) {

		this.redisTemplate = redisTemplate;
		this.keyFactory = keyFactory;
		this.valueFactory = valueFactory;
		this.zSetOps = redisTemplate.opsForZSet();
	}

	public static Collection<Object[]> testParams() {
		return AbstractOperationsTestParams.testParams();
	}

	@BeforeEach
	void setUp() {
		redisTemplate.execute((RedisCallback<Object>) connection -> {
			connection.flushDb();
			return null;
		});
	}

	@ParameterizedRedisTest
	void testCount() {

		K key1 = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		zSetOps.add(key1, value1, 2.5);
		zSetOps.add(key1, value2, 5.5);

		assertThat(zSetOps.count(key1, 2.7, 5.7)).isEqualTo(Long.valueOf(1));
	}

	@ParameterizedRedisTest // DATAREDIS-729
	void testLexCountUnbounded() {

		assumeThat(valueFactory).isOfAnyClassIn(DoubleObjectFactory.class, DoubleAsStringObjectFactory.class,
				LongAsStringObjectFactory.class, LongObjectFactory.class);

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();
		V value3 = valueFactory.instance();

		zSetOps.add(key, value1, 0);
		zSetOps.add(key, value2, 0);
		zSetOps.add(key, value3, 0);

		assertThat(zSetOps.lexCount(key, Range.unbounded())).isEqualTo(3);
	}

	@ParameterizedRedisTest // DATAREDIS-729
	void testLexCountBounded() {

		assumeThat(valueFactory).isOfAnyClassIn(DoubleObjectFactory.class, DoubleAsStringObjectFactory.class,
				LongAsStringObjectFactory.class, LongObjectFactory.class);

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();
		V value3 = valueFactory.instance();

		zSetOps.add(key, value1, 0);
		zSetOps.add(key, value2, 0);
		zSetOps.add(key, value3, 0);

		assertThat(zSetOps.lexCount(key, Range.rightUnbounded(Range.Bound.exclusive(value1.toString())))).isEqualTo(2);
	}

	@ParameterizedRedisTest // GH-2007
	@EnabledOnCommand("ZPOPMIN")
	void testPopMin() {

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();
		V value3 = valueFactory.instance();
		V value4 = valueFactory.instance();

		zSetOps.add(key, value1, 1);
		zSetOps.add(key, value2, 2);
		zSetOps.add(key, value3, 3);
		zSetOps.add(key, value4, 4);

		assertThat(zSetOps.popMin(key)).isEqualTo(new DefaultTypedTuple<>(value1, 1d));
		assertThat(zSetOps.popMin(key, 2)).containsExactly(new DefaultTypedTuple<>(value2, 2d),
				new DefaultTypedTuple<>(value3, 3d));
		assertThat(zSetOps.popMin(key, 1, TimeUnit.SECONDS)).isEqualTo(new DefaultTypedTuple<>(value4, 4d));
	}

	@ParameterizedRedisTest // GH-2007
	@EnabledOnCommand("ZPOPMAX")
	void testPopMax() {

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();
		V value3 = valueFactory.instance();
		V value4 = valueFactory.instance();

		zSetOps.add(key, value1, 1);
		zSetOps.add(key, value2, 2);
		zSetOps.add(key, value3, 3);
		zSetOps.add(key, value4, 4);

		assertThat(zSetOps.popMax(key)).isEqualTo(new DefaultTypedTuple<>(value4, 4d));
		assertThat(zSetOps.popMax(key, 2)).containsExactly(new DefaultTypedTuple<>(value3, 3d),
				new DefaultTypedTuple<>(value2, 2d));
		assertThat(zSetOps.popMax(key, 1, TimeUnit.SECONDS)).isEqualTo(new DefaultTypedTuple<>(value1, 1d));
	}

	@ParameterizedRedisTest
	void testIncrementScore() {

		K key1 = keyFactory.instance();
		V value1 = valueFactory.instance();

		zSetOps.add(key1, value1, 2.5);

		assertThat(zSetOps.incrementScore(key1, value1, 3.2)).isEqualTo(Double.valueOf(5.7));
		Set<TypedTuple<V>> values = zSetOps.rangeWithScores(key1, 0, -1);
		assertThat(values).hasSize(1);
		TypedTuple<V> tuple = values.iterator().next();
		assertThat(tuple).isEqualTo(new DefaultTypedTuple<>(value1, 5.7));
	}

	@ParameterizedRedisTest // GH-2049
	@EnabledOnCommand("ZRANDMEMBER")
	void testRandomMember() {

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();
		V value3 = valueFactory.instance();

		zSetOps.add(key, value1, 1.9);
		zSetOps.add(key, value2, 3.7);
		zSetOps.add(key, value3, 5.8);

		assertThat(zSetOps.randomMember(key)).isIn(value1, value2, value3);
		assertThat(zSetOps.randomMembers(key, 2)).hasSize(2).containsAnyOf(value1, value2, value3);
		assertThat(zSetOps.distinctRandomMembers(key, 2)).hasSize(2).containsAnyOf(value1, value2, value3);
	}

	@ParameterizedRedisTest // GH-2049
	@Disabled("https://github.com/redis/redis/issues/9160")
	@EnabledOnCommand("ZRANDMEMBER")
	void testRandomMemberWithScore() {

		assumeThat(valueFactory).isNotInstanceOf(RawObjectFactory.class);

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();
		V value3 = valueFactory.instance();

		zSetOps.add(key, value1, 1.9);
		zSetOps.add(key, value2, 3.7);
		zSetOps.add(key, value3, 5.8);

		assertThat(zSetOps.randomMemberWithScore(key)).isIn(new DefaultTypedTuple<>(value1, 1.9d),
				new DefaultTypedTuple<>(value2, 3.7d), new DefaultTypedTuple<>(value3, 5.8d));
		assertThat(zSetOps.randomMembersWithScore(key, 2)).hasSize(2).containsAnyOf(new DefaultTypedTuple<>(value1, 1.9d),
				new DefaultTypedTuple<>(value2, 3.7d), new DefaultTypedTuple<>(value3, 5.8d));
		assertThat(zSetOps.distinctRandomMembersWithScore(key, 2)).hasSize(2).containsAnyOf(
				new DefaultTypedTuple<>(value1, 1.9d), new DefaultTypedTuple<>(value2, 3.7d),
				new DefaultTypedTuple<>(value3, 5.8d));
	}

	@ParameterizedRedisTest
	void testRangeByScoreOffsetCount() {

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();
		V value3 = valueFactory.instance();

		zSetOps.add(key, value1, 1.9);
		zSetOps.add(key, value2, 3.7);
		zSetOps.add(key, value3, 5.8);

		assertThat(zSetOps.rangeByScore(key, 1.5, 4.7, 0, 1)).containsOnly(value1);
	}

	@ParameterizedRedisTest
	void testRangeByScoreWithScoresOffsetCount() {

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();
		V value3 = valueFactory.instance();

		zSetOps.add(key, value1, 1.9);
		zSetOps.add(key, value2, 3.7);
		zSetOps.add(key, value3, 5.8);

		Set<TypedTuple<V>> tuples = zSetOps.rangeByScoreWithScores(key, 1.5, 4.7, 0, 1);
		assertThat(tuples).hasSize(1).contains(new DefaultTypedTuple<>(value1, 1.9));
	}

	@ParameterizedRedisTest
	void testReverseRangeByScoreOffsetCount() {

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();
		V value3 = valueFactory.instance();

		zSetOps.add(key, value1, 1.9);
		zSetOps.add(key, value2, 3.7);
		zSetOps.add(key, value3, 5.8);

		assertThat(zSetOps.reverseRangeByScore(key, 1.5, 4.7, 0, 1)).containsOnly(value2);
	}

	@ParameterizedRedisTest
	void testReverseRangeByScoreWithScoresOffsetCount() {

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();
		V value3 = valueFactory.instance();

		zSetOps.add(key, value1, 1.9);
		zSetOps.add(key, value2, 3.7);
		zSetOps.add(key, value3, 5.8);

		Set<TypedTuple<V>> tuples = zSetOps.reverseRangeByScoreWithScores(key, 1.5, 4.7, 0, 1);

		assertThat(tuples).hasSize(1).contains(new DefaultTypedTuple<>(value2, 3.7));
	}

	@ParameterizedRedisTest // DATAREDIS-407
	void testRangeByLexUnbounded() {

		assumeThat(valueFactory).isOfAnyClassIn(DoubleObjectFactory.class, DoubleAsStringObjectFactory.class,
				LongAsStringObjectFactory.class, LongObjectFactory.class);

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();
		V value3 = valueFactory.instance();

		zSetOps.add(key, value1, 1.9);
		zSetOps.add(key, value2, 3.7);
		zSetOps.add(key, value3, 5.8);
		Set<V> tuples = zSetOps.rangeByLex(key, Range.unbounded());

		assertThat(tuples).hasSize(3).contains(value1);
	}

	@ParameterizedRedisTest // DATAREDIS-407
	void testRangeByLexBounded() {

		assumeThat(valueFactory).isOfAnyClassIn(DoubleObjectFactory.class, DoubleAsStringObjectFactory.class,
				LongAsStringObjectFactory.class, LongObjectFactory.class);

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();
		V value3 = valueFactory.instance();

		zSetOps.add(key, value1, 1.9);
		zSetOps.add(key, value2, 3.7);
		zSetOps.add(key, value3, 5.8);
		Set<V> tuples = zSetOps.rangeByLex(key, Range.open(value1.toString(), value3.toString()));

		assertThat(tuples).hasSize(1).contains(value2);
	}

	@ParameterizedRedisTest // DATAREDIS-407
	void testRangeByLexUnboundedWithLimit() {

		assumeThat(valueFactory).isOfAnyClassIn(DoubleObjectFactory.class, DoubleAsStringObjectFactory.class,
				LongAsStringObjectFactory.class, LongObjectFactory.class);

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();
		V value3 = valueFactory.instance();

		zSetOps.add(key, value1, 1.9);
		zSetOps.add(key, value2, 3.7);
		zSetOps.add(key, value3, 5.8);
		Set<V> tuples = zSetOps.rangeByLex(key, Range.unbounded(), Limit.limit().count(2).offset(1));

		assertThat(tuples).hasSize(2).containsSequence(value2, value3);
	}

	@ParameterizedRedisTest // DATAREDIS-729
	void testReverseRangeByLexUnboundedWithLimit() {

		assumeThat(valueFactory).isOfAnyClassIn(DoubleObjectFactory.class, DoubleAsStringObjectFactory.class,
				LongAsStringObjectFactory.class, LongObjectFactory.class);

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();
		V value3 = valueFactory.instance();

		zSetOps.add(key, value1, 1.9);
		zSetOps.add(key, value2, 3.7);
		zSetOps.add(key, value3, 5.8);
		Set<V> tuples = zSetOps.reverseRangeByLex(key, Range.unbounded(), Limit.limit().count(2).offset(1));

		assertThat(tuples).hasSize(2).containsSequence(value2, value1);
	}

	@ParameterizedRedisTest // DATAREDIS-407
	void testRangeByLexBoundedWithLimit() {

		assumeThat(valueFactory).isOfAnyClassIn(DoubleObjectFactory.class, DoubleAsStringObjectFactory.class,
				LongAsStringObjectFactory.class, LongObjectFactory.class);

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();
		V value3 = valueFactory.instance();

		zSetOps.add(key, value1, 1.9);
		zSetOps.add(key, value2, 3.7);
		zSetOps.add(key, value3, 5.8);
		Set<V> tuples = zSetOps.rangeByLex(key, Range.rightUnbounded(Range.Bound.inclusive(value1.toString())),
				Limit.limit().count(1).offset(1));

		assertThat(tuples).hasSize(1).startsWith(value2);
	}

	@ParameterizedRedisTest
	void testAddMultiple() {

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();
		V value3 = valueFactory.instance();

		Set<TypedTuple<V>> values = new HashSet<>();
		values.add(new DefaultTypedTuple<>(value1, 1.7));
		values.add(new DefaultTypedTuple<>(value2, 3.2));
		values.add(new DefaultTypedTuple<>(value3, 0.8));

		assertThat(zSetOps.add(key, values)).isEqualTo(Long.valueOf(3));
		assertThat(zSetOps.range(key, 0, -1)).containsExactly(value3, value1, value2);
	}

	@ParameterizedRedisTest
	void testRemove() {

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();
		V value3 = valueFactory.instance();

		Set<TypedTuple<V>> values = new HashSet<>();
		values.add(new DefaultTypedTuple<>(value1, 1.7));
		values.add(new DefaultTypedTuple<>(value2, 3.2));
		values.add(new DefaultTypedTuple<>(value3, 0.8));

		assertThat(zSetOps.add(key, values)).isEqualTo(Long.valueOf(3));
		assertThat(zSetOps.remove(key, value1, value3)).isEqualTo(Long.valueOf(2));
		assertThat(zSetOps.range(key, 0, -1)).containsOnly(value2);
	}

	@ParameterizedRedisTest
	void testScore() {

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();
		V value3 = valueFactory.instance();

		zSetOps.add(key, value1, 1.9);
		zSetOps.add(key, value2, 3.7);
		zSetOps.add(key, value3, 5.8);

		assertThat(zSetOps.score(key, value1, value2, valueFactory.instance())).containsExactly(1.9d, 3.7d, null);
	}

	@ParameterizedRedisTest
	void zCardRetrievesDataCorrectly() {

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();
		V value3 = valueFactory.instance();

		Set<TypedTuple<V>> values = new HashSet<>();
		values.add(new DefaultTypedTuple<>(value1, 1.7));
		values.add(new DefaultTypedTuple<>(value2, 3.2));
		values.add(new DefaultTypedTuple<>(value3, 0.8));
		zSetOps.add(key, values);

		assertThat(zSetOps.zCard(key)).isEqualTo(3L);
	}

	@ParameterizedRedisTest
	void sizeRetrievesDataCorrectly() {

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();
		V value3 = valueFactory.instance();

		Set<TypedTuple<V>> values = new HashSet<>();
		values.add(new DefaultTypedTuple<>(value1, 1.7));
		values.add(new DefaultTypedTuple<>(value2, 3.2));
		values.add(new DefaultTypedTuple<>(value3, 0.8));
		zSetOps.add(key, values);

		assertThat(zSetOps.size(key)).isEqualTo(3L);
	}

	@ParameterizedRedisTest // DATAREDIS-306
	void testZScanShouldReadEntireValueRange() throws IOException {

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
		try (Cursor<TypedTuple<V>> it = zSetOps.scan(key, ScanOptions.scanOptions().count(2).build())) {
			while (it.hasNext()) {
				assertThat(it.next()).isIn(tuple1, tuple2, tuple3);
				count++;
			}
		}

		assertThat(count).isEqualTo(3);
	}

	@ParameterizedRedisTest // GH-2042
	@EnabledOnCommand("ZDIFF")
	void testZsetDiff() {

		K key1 = keyFactory.instance();
		K key2 = keyFactory.instance();

		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		zSetOps.add(key1, value1, 1.0);
		zSetOps.add(key1, value2, 2.0);
		zSetOps.add(key2, value2, 3.0);

		assertThat(zSetOps.difference(key1, key2)).containsOnly(value1);
		assertThat(zSetOps.differenceWithScores(key1, key2)).containsOnly(new DefaultTypedTuple<>(value1, 1d));
	}

	@ParameterizedRedisTest // DATAREDIS-746, GH-2042
	@EnabledOnCommand("ZDIFFSTORE")
	void testZsetDiffStore() {

		K key1 = keyFactory.instance();
		K key2 = keyFactory.instance();
		K key3 = keyFactory.instance();

		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		zSetOps.add(key1, value1, 1.0);
		zSetOps.add(key1, value2, 2.0);
		zSetOps.add(key2, value2, 3.0);

		assertThat(zSetOps.differenceAndStore(key1, Collections.singletonList(key2), key3)).isEqualTo(1);

		assertThat(zSetOps.rangeWithScores(key3, 0, -1)).containsOnly(new DefaultTypedTuple<>(value1, 1d));
	}

	@ParameterizedRedisTest // GH-2042
	@EnabledOnCommand("ZINTER")
	void testZsetIntersect() {

		K key1 = keyFactory.instance();
		K key2 = keyFactory.instance();

		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		zSetOps.add(key1, value1, 1.0);
		zSetOps.add(key1, value2, 2.0);
		zSetOps.add(key2, value2, 3.0);

		assertThat(zSetOps.intersect(key1, Collections.singletonList(key2))).containsExactly(value2);
	}

	@ParameterizedRedisTest // DATAREDIS-746, GH-2042
	@EnabledOnCommand("ZINTER")
	void testZsetIntersectWithAggregate() {

		K key1 = keyFactory.instance();
		K key2 = keyFactory.instance();

		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		zSetOps.add(key1, value1, 1.0);
		zSetOps.add(key1, value2, 2.0);
		zSetOps.add(key2, value2, 3.0);

		assertThat(zSetOps.intersectWithScores(key1, Collections.singletonList(key2), Aggregate.MIN))
				.contains(new DefaultTypedTuple<>(value2, 2d));

		zSetOps.intersectAndStore(key1, Collections.singletonList(key2), key1, Aggregate.MIN);
		assertThat(zSetOps.score(key1, value2)).isCloseTo(2.0, offset(0.1));
	}

	@ParameterizedRedisTest // DATAREDIS-746
	void testZsetIntersectWithAggregateWeights() {

		K key1 = keyFactory.instance();
		K key2 = keyFactory.instance();
		V value1 = valueFactory.instance();

		zSetOps.add(key1, value1, 4.0);
		zSetOps.add(key2, value1, 3.0);

		zSetOps.intersectAndStore(key1, Collections.singletonList(key2), key1, Aggregate.MAX,
				Weights.of(1, 2));

		assertThat(zSetOps.score(key1, value1)).isCloseTo(6.0, offset(0.1));
	}

	@ParameterizedRedisTest // GH-2042
	@EnabledOnCommand("ZUNION")
	void testZsetUnion() {

		K key1 = keyFactory.instance();
		K key2 = keyFactory.instance();

		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		zSetOps.add(key1, value1, 1.0);
		zSetOps.add(key1, value2, 2.0);
		zSetOps.add(key2, value2, 3.0);

		assertThat(zSetOps.union(key1, Collections.singletonList(key2))).containsOnly(value1, value2);
	}

	@ParameterizedRedisTest // DATAREDIS-746, GH-2042
	@EnabledOnCommand("ZUNION")
	void testZsetUnionWithAggregate() {

		K key1 = keyFactory.instance();
		K key2 = keyFactory.instance();

		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		zSetOps.add(key1, value1, 1.0);
		zSetOps.add(key1, value2, 2.0);
		zSetOps.add(key2, value2, 3.0);

		assertThat(zSetOps.unionWithScores(key1, Collections.singletonList(key2)))
				.containsOnly(new DefaultTypedTuple<>(value1, 1d), new DefaultTypedTuple<>(value2, 5d));

		zSetOps.unionAndStore(key1, Collections.singletonList(key2), key1, Aggregate.MIN);

		assertThat(zSetOps.score(key1, value2)).isCloseTo(2.0, offset(0.1));
	}

	@ParameterizedRedisTest // DATAREDIS-746
	void testZsetUnionWithAggregateWeights() {

		K key1 = keyFactory.instance();
		K key2 = keyFactory.instance();
		V value1 = valueFactory.instance();

		zSetOps.add(key1, value1, 4.0);
		zSetOps.add(key2, value1, 3.0);

		zSetOps.unionAndStore(key1, Collections.singletonList(key2), key1, Aggregate.MAX,
				Weights.of(1, 2));

		assertThat(zSetOps.score(key1, value1)).isCloseTo(6.0, offset(0.1));
	}

}
