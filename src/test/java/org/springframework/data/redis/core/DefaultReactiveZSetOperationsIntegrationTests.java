/*
 * Copyright 2017-2020 the original author or authors.
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
import static org.junit.Assume.*;

import reactor.test.StepVerifier;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.ByteBufferObjectFactory;
import org.springframework.data.redis.ConnectionFactoryTracker;
import org.springframework.data.redis.ObjectFactory;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisZSetCommands.Aggregate;
import org.springframework.data.redis.connection.RedisZSetCommands.Limit;
import org.springframework.data.redis.connection.RedisZSetCommands.Weights;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

/**
 * Integration tests for {@link DefaultReactiveZSetOperations}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 */
@RunWith(Parameterized.class)
@SuppressWarnings("unchecked")
public class DefaultReactiveZSetOperationsIntegrationTests<K, V> {

	private static final Range<Long> ZERO_RANGE = Range.just(0L);
	private static final Range<Long> ZERO_TO_FIVE = Range.closed(0L, 5L);

	private static final Range<Double> ZERO_TO_HUNDRED_DOUBLE = Range.closed(0d, 100d);
	private static final Range<Double> NINE_TO_ELEVEN_DOUBLE = Range.closed(9d, 11d);

	private final ReactiveRedisTemplate<K, V> redisTemplate;
	private final ReactiveZSetOperations<K, V> zSetOperations;

	private final ObjectFactory<K> keyFactory;
	private final ObjectFactory<V> valueFactory;

	private final RedisSerializer serializer;

	@Parameters(name = "{4}")
	public static Collection<Object[]> testParams() {
		return ReactiveOperationsTestParams.testParams();
	}

	@AfterClass
	public static void cleanUp() {
		ConnectionFactoryTracker.cleanUp();
	}

	/**
	 * @param redisTemplate
	 * @param keyFactory
	 * @param valueFactory
	 * @param label parameterized test label, no further use besides that.
	 */
	public DefaultReactiveZSetOperationsIntegrationTests(ReactiveRedisTemplate<K, V> redisTemplate,
			ObjectFactory<K> keyFactory, ObjectFactory<V> valueFactory, RedisSerializer serializer, String label) {

		this.redisTemplate = redisTemplate;
		this.zSetOperations = redisTemplate.opsForZSet();
		this.keyFactory = keyFactory;
		this.valueFactory = valueFactory;
		this.serializer = serializer;

		ConnectionFactoryTracker.add(redisTemplate.getConnectionFactory());
	}

	@Before
	public void before() {

		RedisConnectionFactory connectionFactory = (RedisConnectionFactory) redisTemplate.getConnectionFactory();
		RedisConnection connection = connectionFactory.getConnection();
		connection.flushAll();
		connection.close();
	}

	@Test // DATAREDIS-602
	public void add() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		zSetOperations.add(key, value, 42.1).as(StepVerifier::create).expectNext(true).verifyComplete();
	}

	@Test // DATAREDIS-602
	public void addAll() {

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		List<DefaultTypedTuple<V>> tuples = Arrays.asList(new DefaultTypedTuple<>(value1, 42.1d),
				new DefaultTypedTuple<>(value2, 10d));

		zSetOperations.addAll(key, tuples).as(StepVerifier::create).expectNext(2L).verifyComplete();

		List<DefaultTypedTuple<V>> updated = Arrays.asList(new DefaultTypedTuple<>(value1, 52.1d),
				new DefaultTypedTuple<>(value2, 10d));

		zSetOperations.addAll(key, updated).as(StepVerifier::create).expectNext(0L).verifyComplete();
		zSetOperations.score(key, value1).as(StepVerifier::create).expectNext(52.1d).verifyComplete();
	}

	@Test // DATAREDIS-602
	public void remove() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		zSetOperations.add(key, value, 42.1).as(StepVerifier::create).expectNext(true).verifyComplete();

		zSetOperations.remove(key, value).as(StepVerifier::create).expectNext(1L).verifyComplete();

		zSetOperations.remove(key, value).as(StepVerifier::create).expectNext(0L).verifyComplete();
	}

	@Test // DATAREDIS-602
	public void incrementScore() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		zSetOperations.add(key, value, 42.1).as(StepVerifier::create).expectNext(true).verifyComplete();

		zSetOperations.incrementScore(key, value, 1.1).as(StepVerifier::create).expectNext(43.2).verifyComplete();
	}

	@Test // DATAREDIS-602
	public void rank() {

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		zSetOperations.add(key, value1, 42.1).as(StepVerifier::create).expectNext(true).verifyComplete();
		zSetOperations.add(key, value2, 10).as(StepVerifier::create).expectNext(true).verifyComplete();

		zSetOperations.rank(key, value1).as(StepVerifier::create).expectNext(1L).verifyComplete();
	}

	@Test // DATAREDIS-602
	public void reverseRank() {

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		zSetOperations.add(key, value1, 42.1).as(StepVerifier::create).expectNext(true).verifyComplete();
		zSetOperations.add(key, value2, 10).as(StepVerifier::create).expectNext(true).verifyComplete();

		zSetOperations.reverseRank(key, value1).as(StepVerifier::create).expectNext(0L).verifyComplete();
	}

	@Test // DATAREDIS-602
	public void range() {

		assumeFalse(valueFactory instanceof ByteBufferObjectFactory);

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		zSetOperations.add(key, value1, 42.1).as(StepVerifier::create).expectNext(true).verifyComplete();
		zSetOperations.add(key, value2, 10).as(StepVerifier::create).expectNext(true).verifyComplete();

		zSetOperations.range(key, ZERO_RANGE).as(StepVerifier::create) //
				.expectNext(value2) //
				.verifyComplete();

	}

	@Test // DATAREDIS-602
	public void rangeWithScores() {

		assumeFalse(valueFactory instanceof ByteBufferObjectFactory);

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		zSetOperations.add(key, value1, 42.1).as(StepVerifier::create).expectNext(true).verifyComplete();
		zSetOperations.add(key, value2, 10).as(StepVerifier::create).expectNext(true).verifyComplete();

		zSetOperations.rangeWithScores(key, ZERO_RANGE).as(StepVerifier::create) //
				.expectNext(new DefaultTypedTuple<>(value2, 10d)) //
				.verifyComplete();
	}

	@Test // DATAREDIS-602
	public void rangeByScore() {

		assumeFalse(valueFactory instanceof ByteBufferObjectFactory);

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		zSetOperations.add(key, value1, 42.1).as(StepVerifier::create).expectNext(true).verifyComplete();
		zSetOperations.add(key, value2, 10).as(StepVerifier::create).expectNext(true).verifyComplete();

		zSetOperations.rangeByScore(key, NINE_TO_ELEVEN_DOUBLE).as(StepVerifier::create) //
				.expectNext(value2) //
				.verifyComplete();
	}

	@Test // DATAREDIS-602
	public void rangeByScoreWithScores() {

		assumeFalse(valueFactory instanceof ByteBufferObjectFactory);

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		zSetOperations.add(key, value1, 42.1).as(StepVerifier::create).expectNext(true).verifyComplete();
		zSetOperations.add(key, value2, 10).as(StepVerifier::create).expectNext(true).verifyComplete();

		zSetOperations.rangeByScoreWithScores(key, NINE_TO_ELEVEN_DOUBLE).as(StepVerifier::create) //
				.expectNext(new DefaultTypedTuple<>(value2, 10d)) //
				.verifyComplete();
	}

	@Test // DATAREDIS-602
	public void rangeByScoreWithLimit() {

		assumeFalse(valueFactory instanceof ByteBufferObjectFactory);

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		zSetOperations.add(key, value1, 42.1).as(StepVerifier::create).expectNext(true).verifyComplete();
		zSetOperations.add(key, value2, 10).as(StepVerifier::create).expectNext(true).verifyComplete();

		zSetOperations.rangeByScore(key, ZERO_TO_HUNDRED_DOUBLE, //
				Limit.limit().offset(1).count(10)).as(StepVerifier::create) //
				.expectNext(value1) //
				.verifyComplete();
	}

	@Test // DATAREDIS-602
	public void rangeByScoreWithScoresWithLimit() {

		assumeFalse(valueFactory instanceof ByteBufferObjectFactory);

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		zSetOperations.add(key, value1, 42.1).as(StepVerifier::create).expectNext(true).verifyComplete();
		zSetOperations.add(key, value2, 10).as(StepVerifier::create).expectNext(true).verifyComplete();

		zSetOperations.rangeByScoreWithScores(key, ZERO_TO_HUNDRED_DOUBLE, //
				Limit.limit().offset(1).count(10)).as(StepVerifier::create) //
				.expectNext(new DefaultTypedTuple<>(value1, 42.1)) //
				.verifyComplete();
	}

	@Test // DATAREDIS-602
	public void reverseRange() {

		assumeFalse(valueFactory instanceof ByteBufferObjectFactory);

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		zSetOperations.add(key, value1, 42.1).as(StepVerifier::create).expectNext(true).verifyComplete();
		zSetOperations.add(key, value2, 10).as(StepVerifier::create).expectNext(true).verifyComplete();

		zSetOperations.reverseRange(key, ZERO_RANGE).as(StepVerifier::create) //
				.expectNext(value1) //
				.verifyComplete();

	}

	@Test // DATAREDIS-602
	public void reverseRangeWithScores() {

		assumeFalse(valueFactory instanceof ByteBufferObjectFactory);

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		zSetOperations.add(key, value1, 42.1).as(StepVerifier::create).expectNext(true).verifyComplete();
		zSetOperations.add(key, value2, 10).as(StepVerifier::create).expectNext(true).verifyComplete();

		zSetOperations.reverseRangeWithScores(key, ZERO_RANGE).as(StepVerifier::create) //
				.expectNext(new DefaultTypedTuple<>(value1, 42.1)) //
				.verifyComplete();
	}

	@Test // DATAREDIS-602
	public void reverseRangeByScore() {

		assumeFalse(valueFactory instanceof ByteBufferObjectFactory);

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		zSetOperations.add(key, value1, 42.1).as(StepVerifier::create).expectNext(true).verifyComplete();
		zSetOperations.add(key, value2, 10).as(StepVerifier::create).expectNext(true).verifyComplete();

		zSetOperations.reverseRangeByScore(key, NINE_TO_ELEVEN_DOUBLE).as(StepVerifier::create) //
				.expectNext(value2) //
				.verifyComplete();
	}

	@Test // DATAREDIS-602
	public void reverseRangeByScoreWithScores() {

		assumeFalse(valueFactory instanceof ByteBufferObjectFactory);

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		zSetOperations.add(key, value1, 42.1).as(StepVerifier::create).expectNext(true).verifyComplete();
		zSetOperations.add(key, value2, 10).as(StepVerifier::create).expectNext(true).verifyComplete();

		zSetOperations.reverseRangeByScoreWithScores(key, NINE_TO_ELEVEN_DOUBLE).as(StepVerifier::create) //
				.expectNext(new DefaultTypedTuple<>(value2, 10d)) //
				.verifyComplete();
	}

	@Test // DATAREDIS-602
	public void reverseRangeByScoreWithLimit() {

		assumeFalse(valueFactory instanceof ByteBufferObjectFactory);

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		zSetOperations.add(key, value1, 42.1).as(StepVerifier::create).expectNext(true).verifyComplete();
		zSetOperations.add(key, value2, 10).as(StepVerifier::create).expectNext(true).verifyComplete();

		zSetOperations.reverseRangeByScore(key, ZERO_TO_HUNDRED_DOUBLE, //
				Limit.limit().offset(1).count(10)).as(StepVerifier::create) //
				.expectNext(value2) //
				.verifyComplete();
	}

	@Test // DATAREDIS-602
	public void reverseRangeByScoreWithScoresWithLimit() {

		assumeFalse(valueFactory instanceof ByteBufferObjectFactory);

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		zSetOperations.add(key, value1, 42.1).as(StepVerifier::create).expectNext(true).verifyComplete();
		zSetOperations.add(key, value2, 10).as(StepVerifier::create).expectNext(true).verifyComplete();

		zSetOperations.reverseRangeByScoreWithScores(key, ZERO_TO_HUNDRED_DOUBLE, //
				Limit.limit().offset(1).count(10)).as(StepVerifier::create) //
				.expectNext(new DefaultTypedTuple<>(value2, 10d)) //
				.verifyComplete();
	}

	@Test // DATAREDIS-743
	public void scan() {

		assumeFalse(valueFactory instanceof ByteBufferObjectFactory);

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		zSetOperations.add(key, value1, 42.1).as(StepVerifier::create) //
				.expectNext(true) //
				.verifyComplete();
		zSetOperations.add(key, value2, 10).as(StepVerifier::create) //
				.expectNext(true) //
				.verifyComplete();

		zSetOperations.scan(key).as(StepVerifier::create) //
				.consumeNextWith(actual -> assertThat(actual).isIn(new DefaultTypedTuple<>(value1, 42.1),
						new DefaultTypedTuple<>(value2, 10d))) //
				.expectNextCount(1) //
				.verifyComplete();
	}

	@Test // DATAREDIS-602
	public void count() {

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		zSetOperations.add(key, value1, 42.1).as(StepVerifier::create).expectNext(true).verifyComplete();
		zSetOperations.add(key, value2, 10).as(StepVerifier::create).expectNext(true).verifyComplete();

		zSetOperations.count(key, ZERO_TO_HUNDRED_DOUBLE).as(StepVerifier::create).expectNext(2L).expectComplete().verify();
		zSetOperations.count(key, Range.closed(0d, 10d)).as(StepVerifier::create).expectNext(1L).verifyComplete();
	}

	@Test // DATAREDIS-602
	public void size() {

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		zSetOperations.add(key, value1, 42.1).as(StepVerifier::create).expectNext(true).verifyComplete();
		zSetOperations.add(key, value2, 10).as(StepVerifier::create).expectNext(true).verifyComplete();

		zSetOperations.size(key).as(StepVerifier::create).expectNext(2L).verifyComplete();
	}

	@Test // DATAREDIS-602
	public void score() {

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		zSetOperations.add(key, value1, 42.1).as(StepVerifier::create).expectNext(true).verifyComplete();
		zSetOperations.add(key, value2, 10).as(StepVerifier::create).expectNext(true).verifyComplete();

		zSetOperations.score(key, value1).as(StepVerifier::create).expectNext(42.1d).verifyComplete();
		zSetOperations.score(key, value2).as(StepVerifier::create).expectNext(10d).verifyComplete();
	}

	@Test // DATAREDIS-602
	public void removeRange() {

		assumeFalse(valueFactory instanceof ByteBufferObjectFactory);

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		zSetOperations.add(key, value1, 42.1).as(StepVerifier::create).expectNext(true).verifyComplete();
		zSetOperations.add(key, value2, 10).as(StepVerifier::create).expectNext(true).verifyComplete();

		zSetOperations.removeRange(key, ZERO_RANGE).as(StepVerifier::create).expectNext(1L).verifyComplete();
		zSetOperations.range(key, ZERO_TO_FIVE).as(StepVerifier::create).expectNext(value1).verifyComplete();
	}

	@Test // DATAREDIS-602
	public void removeRangeByScore() {

		assumeFalse(valueFactory instanceof ByteBufferObjectFactory);

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		zSetOperations.add(key, value1, 42.1).as(StepVerifier::create).expectNext(true).verifyComplete();
		zSetOperations.add(key, value2, 10).as(StepVerifier::create).expectNext(true).verifyComplete();

		zSetOperations.removeRangeByScore(key, NINE_TO_ELEVEN_DOUBLE).as(StepVerifier::create).expectNext(1L)
				.expectComplete()
				.verify();
		zSetOperations.range(key, ZERO_TO_FIVE).as(StepVerifier::create) //
				.expectNext(value1) //
				.verifyComplete();
	}

	@Test // DATAREDIS-602
	public void unionAndStore() {

		K key = keyFactory.instance();
		K otherKey = keyFactory.instance();
		K destKey = keyFactory.instance();

		V onlyInKey = valueFactory.instance();
		V shared = valueFactory.instance();
		V onlyInOtherKey = valueFactory.instance();

		zSetOperations.add(key, onlyInKey, 10).as(StepVerifier::create).expectNext(true).verifyComplete();
		zSetOperations.add(key, shared, 11).as(StepVerifier::create).expectNext(true).verifyComplete();

		zSetOperations.add(otherKey, onlyInOtherKey, 10).as(StepVerifier::create).expectNext(true).verifyComplete();
		zSetOperations.add(otherKey, shared, 11).as(StepVerifier::create).expectNext(true).verifyComplete();

		zSetOperations.unionAndStore(key, otherKey, destKey).as(StepVerifier::create).expectNext(3L).verifyComplete();
		zSetOperations.range(destKey, Range.closed(0L, 100L)).as(StepVerifier::create).expectNextCount(3).verifyComplete();
	}

	@Test // DATAREDIS-746
	public void unionAndStoreWithAggregation() {

		K key = keyFactory.instance();
		K otherKey = keyFactory.instance();
		K destKey = keyFactory.instance();

		V onlyInKey = valueFactory.instance();
		V shared = valueFactory.instance();
		V onlyInOtherKey = valueFactory.instance();

		zSetOperations.add(key, onlyInKey, 10).as(StepVerifier::create).expectNext(true).verifyComplete();
		zSetOperations.add(key, shared, 11).as(StepVerifier::create).expectNext(true).verifyComplete();

		zSetOperations.add(otherKey, onlyInOtherKey, 10).as(StepVerifier::create).expectNext(true).verifyComplete();
		zSetOperations.add(otherKey, shared, 11).as(StepVerifier::create).expectNext(true).verifyComplete();

		zSetOperations.unionAndStore(key, Collections.singleton(otherKey), destKey, Aggregate.SUM).as(StepVerifier::create)
				.expectNext(3L).verifyComplete();
		zSetOperations.score(destKey, shared).as(StepVerifier::create).expectNext(22d).verifyComplete();

		zSetOperations.unionAndStore(key, Collections.singleton(otherKey), destKey, Aggregate.SUM, Weights.of(2, 1))
				.as(StepVerifier::create)
				.expectNext(3L).verifyComplete();
		zSetOperations.score(destKey, shared).as(StepVerifier::create).expectNext(33d).verifyComplete();
	}

	@Test // DATAREDIS-602
	public void intersectAndStore() {

		K key = keyFactory.instance();
		K otherKey = keyFactory.instance();
		K destKey = keyFactory.instance();

		V onlyInKey = valueFactory.instance();
		V shared = valueFactory.instance();
		V onlyInOtherKey = valueFactory.instance();

		zSetOperations.add(key, onlyInKey, 10).as(StepVerifier::create).expectNext(true).verifyComplete();
		zSetOperations.add(key, shared, 11).as(StepVerifier::create).expectNext(true).verifyComplete();

		zSetOperations.add(otherKey, onlyInOtherKey, 10).as(StepVerifier::create).expectNext(true).verifyComplete();
		zSetOperations.add(otherKey, shared, 11).as(StepVerifier::create).expectNext(true).verifyComplete();

		zSetOperations.intersectAndStore(key, otherKey, destKey).as(StepVerifier::create).expectNext(1L).expectComplete()
				.verify();

		zSetOperations.range(destKey, ZERO_TO_FIVE).as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();
	}

	@Test // DATAREDIS-746
	public void intersectAndStoreWithAggregation() {

		K key = keyFactory.instance();
		K otherKey = keyFactory.instance();
		K destKey = keyFactory.instance();

		V onlyInKey = valueFactory.instance();
		V shared = valueFactory.instance();
		V onlyInOtherKey = valueFactory.instance();

		zSetOperations.add(key, onlyInKey, 10).as(StepVerifier::create).expectNext(true).verifyComplete();
		zSetOperations.add(key, shared, 11).as(StepVerifier::create).expectNext(true).verifyComplete();

		zSetOperations.add(otherKey, onlyInOtherKey, 10).as(StepVerifier::create).expectNext(true).verifyComplete();
		zSetOperations.add(otherKey, shared, 11).as(StepVerifier::create).expectNext(true).verifyComplete();

		zSetOperations.intersectAndStore(key, Collections.singletonList(otherKey), destKey, Aggregate.SUM)
				.as(StepVerifier::create)
				.expectNext(1L).expectComplete().verify();

		zSetOperations.score(destKey, shared).as(StepVerifier::create) //
				.expectNext(22d) //
				.verifyComplete();

		zSetOperations.intersectAndStore(key, Collections.singletonList(otherKey), destKey, Aggregate.SUM, Weights.of(1, 2))
				.as(StepVerifier::create).expectNext(1L).expectComplete().verify();

		zSetOperations.score(destKey, shared).as(StepVerifier::create) //
				.expectNext(33d) //
				.verifyComplete();
	}

	@Test // DATAREDIS-602
	public void rangeByLex() {

		assumeTrue(serializer instanceof StringRedisSerializer);

		K key = keyFactory.instance();
		V a = (V) "a";
		V b = (V) "b";

		zSetOperations.add(key, a, 10).as(StepVerifier::create).expectNext(true).verifyComplete();
		zSetOperations.add(key, b, 11).as(StepVerifier::create).expectNext(true).verifyComplete();

		zSetOperations.rangeByLex(key, Range.just("a")).as(StepVerifier::create) //
				.expectNext(a) //
				.verifyComplete();

	}

	@Test // DATAREDIS-602
	public void rangeByLexWithLimit() {

		assumeTrue(serializer instanceof StringRedisSerializer);

		K key = keyFactory.instance();
		V a = (V) "a";
		V b = (V) "b";

		zSetOperations.add(key, a, 10).as(StepVerifier::create).expectNext(true).verifyComplete();
		zSetOperations.add(key, b, 11).as(StepVerifier::create).expectNext(true).verifyComplete();

		Range<String> aToZ = Range.closed("a", "z");

		zSetOperations.rangeByLex(key, aToZ, Limit.limit().offset(0).count(10)).as(StepVerifier::create) //
				.expectNext(a, b) //
				.verifyComplete();

		zSetOperations.rangeByLex(key, aToZ, Limit.limit().offset(1).count(10)).as(StepVerifier::create) //
				.expectNext(b) //
				.verifyComplete();
	}

	@Test // DATAREDIS-602
	public void reverseRangeByLex() {

		assumeTrue(serializer instanceof StringRedisSerializer);

		K key = keyFactory.instance();
		V a = (V) "a";
		V b = (V) "b";

		zSetOperations.add(key, a, 10).as(StepVerifier::create).expectNext(true).verifyComplete();
		zSetOperations.add(key, b, 11).as(StepVerifier::create).expectNext(true).verifyComplete();

		zSetOperations.reverseRangeByLex(key, Range.just("a")).as(StepVerifier::create) //
				.expectNext(a) //
				.verifyComplete();
	}

	@Test // DATAREDIS-602
	public void reverseRangeByLexLimit() {

		assumeTrue(serializer instanceof StringRedisSerializer);

		K key = keyFactory.instance();
		V a = (V) "a";
		V b = (V) "b";

		zSetOperations.add(key, a, 10).as(StepVerifier::create).expectNext(true).verifyComplete();
		zSetOperations.add(key, b, 11).as(StepVerifier::create).expectNext(true).verifyComplete();

		Range<String> aToZ = Range.closed("a", "z");

		zSetOperations.reverseRangeByLex(key, aToZ, Limit.limit().offset(0).count(10)).as(StepVerifier::create) //
				.expectNext(b, a) //
				.verifyComplete();

		zSetOperations.reverseRangeByLex(key, aToZ, Limit.limit().offset(1).count(10)).as(StepVerifier::create) //
				.expectNext(a) //
				.verifyComplete();
	}

	@Test // DATAREDIS-602
	public void delete() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		zSetOperations.add(key, value, 10).as(StepVerifier::create).expectNext(true).verifyComplete();

		zSetOperations.delete(key).as(StepVerifier::create).expectNext(true).verifyComplete();

		zSetOperations.size(key).as(StepVerifier::create) //
				.expectNext(0L) //
				.verifyComplete();
	}
}
