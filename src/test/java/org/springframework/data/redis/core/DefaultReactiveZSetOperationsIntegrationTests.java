/*
 * Copyright 2017-2022 the original author or authors.
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

import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;

import org.springframework.data.domain.Range;
import org.springframework.data.redis.ByteBufferObjectFactory;
import org.springframework.data.redis.ObjectFactory;
import org.springframework.data.redis.connection.Limit;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.zset.Aggregate;
import org.springframework.data.redis.connection.zset.Weights;
import org.springframework.data.redis.core.ReactiveOperationsTestParams.Fixture;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.data.redis.test.condition.EnabledOnCommand;
import org.springframework.data.redis.test.extension.parametrized.MethodSource;
import org.springframework.data.redis.test.extension.parametrized.ParameterizedRedisTest;

/**
 * Integration tests for {@link DefaultReactiveZSetOperations}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @author Andrey Shlykov
 */
@MethodSource("testParams")
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

	private final RedisSerializer<?> serializer;

	public static Collection<Fixture<?, ?>> testParams() {
		return ReactiveOperationsTestParams.testParams();
	}

	public DefaultReactiveZSetOperationsIntegrationTests(Fixture<K, V> fixture) {

		this.redisTemplate = fixture.getTemplate();
		this.zSetOperations = redisTemplate.opsForZSet();
		this.keyFactory = fixture.getKeyFactory();
		this.valueFactory = fixture.getValueFactory();
		this.serializer = fixture.getSerializer();
	}

	@BeforeEach
	public void before() {

		RedisConnectionFactory connectionFactory = (RedisConnectionFactory) redisTemplate.getConnectionFactory();
		RedisConnection connection = connectionFactory.getConnection();
		connection.flushAll();
		connection.close();
	}

	@ParameterizedRedisTest // DATAREDIS-602
	void add() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		zSetOperations.add(key, value, 42.1).as(StepVerifier::create).expectNext(true).verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-602
	void addAll() {

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

	@ParameterizedRedisTest // DATAREDIS-602
	void remove() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		zSetOperations.add(key, value, 42.1).as(StepVerifier::create).expectNext(true).verifyComplete();

		zSetOperations.remove(key, value).as(StepVerifier::create).expectNext(1L).verifyComplete();

		zSetOperations.remove(key, value).as(StepVerifier::create).expectNext(0L).verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-602
	void incrementScore() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		zSetOperations.add(key, value, 42.1).as(StepVerifier::create).expectNext(true).verifyComplete();

		zSetOperations.incrementScore(key, value, 1.1).as(StepVerifier::create).expectNext(43.2).verifyComplete();
	}

	@ParameterizedRedisTest // GH-2049
	@EnabledOnCommand("ZRANDMEMBER")
	void randomMember() {

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		zSetOperations.add(key, value1, 42.1).as(StepVerifier::create).expectNext(true).verifyComplete();
		zSetOperations.add(key, value2, 10).as(StepVerifier::create).expectNext(true).verifyComplete();

		zSetOperations.randomMember(key).as(StepVerifier::create).consumeNextWith(actual -> {

			assertThat(actual).isIn(value1, value2);
		}).verifyComplete();

		zSetOperations.randomMembers(key, 2).as(StepVerifier::create).expectNextCount(2).verifyComplete();
		zSetOperations.distinctRandomMembers(key, 2).as(StepVerifier::create).expectNextCount(2).verifyComplete();
	}

	@ParameterizedRedisTest // GH-2049
	@Disabled("https://github.com/redis/redis/issues/9160")
	@EnabledOnCommand("ZRANDMEMBER")
	void randomMemberWithScore() {

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		zSetOperations.add(key, value1, 42.1).as(StepVerifier::create).expectNext(true).verifyComplete();
		zSetOperations.add(key, value2, 10).as(StepVerifier::create).expectNext(true).verifyComplete();

		zSetOperations.randomMemberWithScore(key).as(StepVerifier::create).consumeNextWith(actual -> {

			assertThat(actual).isIn(new DefaultTypedTuple<>(value1, 42.1d), new DefaultTypedTuple<>(value2, 10d));
		}).verifyComplete();

		zSetOperations.randomMembersWithScore(key, 2).as(StepVerifier::create).expectNextCount(2).verifyComplete();
		zSetOperations.distinctRandomMembersWithScore(key, 2).as(StepVerifier::create).expectNextCount(2).verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-602
	void rank() {

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		zSetOperations.add(key, value1, 42.1).as(StepVerifier::create).expectNext(true).verifyComplete();
		zSetOperations.add(key, value2, 10).as(StepVerifier::create).expectNext(true).verifyComplete();

		zSetOperations.rank(key, value1).as(StepVerifier::create).expectNext(1L).verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-602
	void reverseRank() {

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		zSetOperations.add(key, value1, 42.1).as(StepVerifier::create).expectNext(true).verifyComplete();
		zSetOperations.add(key, value2, 10).as(StepVerifier::create).expectNext(true).verifyComplete();

		zSetOperations.reverseRank(key, value1).as(StepVerifier::create).expectNext(0L).verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-602
	void range() {

		assumeThat(valueFactory instanceof ByteBufferObjectFactory).isFalse();

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		zSetOperations.add(key, value1, 42.1).as(StepVerifier::create).expectNext(true).verifyComplete();
		zSetOperations.add(key, value2, 10).as(StepVerifier::create).expectNext(true).verifyComplete();

		zSetOperations.range(key, ZERO_RANGE).as(StepVerifier::create) //
				.expectNext(value2) //
				.verifyComplete();

	}

	@ParameterizedRedisTest // DATAREDIS-602
	void rangeWithScores() {

		assumeThat(valueFactory instanceof ByteBufferObjectFactory).isFalse();

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		zSetOperations.add(key, value1, 42.1).as(StepVerifier::create).expectNext(true).verifyComplete();
		zSetOperations.add(key, value2, 10).as(StepVerifier::create).expectNext(true).verifyComplete();

		zSetOperations.rangeWithScores(key, ZERO_RANGE).as(StepVerifier::create) //
				.expectNext(new DefaultTypedTuple<>(value2, 10d)) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-602
	void rangeByScore() {

		assumeThat(valueFactory instanceof ByteBufferObjectFactory).isFalse();

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		zSetOperations.add(key, value1, 42.1).as(StepVerifier::create).expectNext(true).verifyComplete();
		zSetOperations.add(key, value2, 10).as(StepVerifier::create).expectNext(true).verifyComplete();

		zSetOperations.rangeByScore(key, NINE_TO_ELEVEN_DOUBLE).as(StepVerifier::create) //
				.expectNext(value2) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-602
	void rangeByScoreWithScores() {

		assumeThat(valueFactory instanceof ByteBufferObjectFactory).isFalse();

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		zSetOperations.add(key, value1, 42.1).as(StepVerifier::create).expectNext(true).verifyComplete();
		zSetOperations.add(key, value2, 10).as(StepVerifier::create).expectNext(true).verifyComplete();

		zSetOperations.rangeByScoreWithScores(key, NINE_TO_ELEVEN_DOUBLE).as(StepVerifier::create) //
				.expectNext(new DefaultTypedTuple<>(value2, 10d)) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-602
	void rangeByScoreWithLimit() {

		assumeThat(valueFactory instanceof ByteBufferObjectFactory).isFalse();

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

	@ParameterizedRedisTest // DATAREDIS-602
	void rangeByScoreWithScoresWithLimit() {

		assumeThat(valueFactory instanceof ByteBufferObjectFactory).isFalse();

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

	@ParameterizedRedisTest // DATAREDIS-602
	void reverseRange() {

		assumeThat(valueFactory instanceof ByteBufferObjectFactory).isFalse();

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		zSetOperations.add(key, value1, 42.1).as(StepVerifier::create).expectNext(true).verifyComplete();
		zSetOperations.add(key, value2, 10).as(StepVerifier::create).expectNext(true).verifyComplete();

		zSetOperations.reverseRange(key, ZERO_RANGE).as(StepVerifier::create) //
				.expectNext(value1) //
				.verifyComplete();

	}

	@ParameterizedRedisTest // DATAREDIS-602
	void reverseRangeWithScores() {

		assumeThat(valueFactory instanceof ByteBufferObjectFactory).isFalse();

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		zSetOperations.add(key, value1, 42.1).as(StepVerifier::create).expectNext(true).verifyComplete();
		zSetOperations.add(key, value2, 10).as(StepVerifier::create).expectNext(true).verifyComplete();

		zSetOperations.reverseRangeWithScores(key, ZERO_RANGE).as(StepVerifier::create) //
				.expectNext(new DefaultTypedTuple<>(value1, 42.1)) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-602
	void reverseRangeByScore() {

		assumeThat(valueFactory instanceof ByteBufferObjectFactory).isFalse();

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		zSetOperations.add(key, value1, 42.1).as(StepVerifier::create).expectNext(true).verifyComplete();
		zSetOperations.add(key, value2, 10).as(StepVerifier::create).expectNext(true).verifyComplete();

		zSetOperations.reverseRangeByScore(key, NINE_TO_ELEVEN_DOUBLE).as(StepVerifier::create) //
				.expectNext(value2) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-602
	void reverseRangeByScoreWithScores() {

		assumeThat(valueFactory instanceof ByteBufferObjectFactory).isFalse();

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		zSetOperations.add(key, value1, 42.1).as(StepVerifier::create).expectNext(true).verifyComplete();
		zSetOperations.add(key, value2, 10).as(StepVerifier::create).expectNext(true).verifyComplete();

		zSetOperations.reverseRangeByScoreWithScores(key, NINE_TO_ELEVEN_DOUBLE).as(StepVerifier::create) //
				.expectNext(new DefaultTypedTuple<>(value2, 10d)) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-602
	void reverseRangeByScoreWithLimit() {

		assumeThat(valueFactory instanceof ByteBufferObjectFactory).isFalse();

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

	@ParameterizedRedisTest // DATAREDIS-602
	void reverseRangeByScoreWithScoresWithLimit() {

		assumeThat(valueFactory instanceof ByteBufferObjectFactory).isFalse();

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

	@ParameterizedRedisTest // GH-2345
	void rangeAndStoreByLex() {

		assumeThat(serializer instanceof StringRedisSerializer).isTrue();

		K key = keyFactory.instance();
		K destKey = keyFactory.instance();
		V a = (V) "a";
		V b = (V) "b";
		V c = (V) "c";

		zSetOperations.add(key, a, 1).as(StepVerifier::create).expectNext(true).verifyComplete();
		zSetOperations.add(key, b, 2).as(StepVerifier::create).expectNext(true).verifyComplete();
		zSetOperations.add(key, c, 3).as(StepVerifier::create).expectNext(true).verifyComplete();

		zSetOperations.rangeAndStoreByLex(key, destKey, Range.closed("a", "b")).as(StepVerifier::create) //
				.expectNext(2L) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // GH-2345
	void rangeAndStoreByScore() {

		assumeThat(valueFactory instanceof ByteBufferObjectFactory).isFalse();

		K key = keyFactory.instance();
		K destKey = keyFactory.instance();
		V a = valueFactory.instance();
		V b = valueFactory.instance();
		V c = valueFactory.instance();

		zSetOperations.add(key, a, 1).as(StepVerifier::create).expectNext(true).verifyComplete();
		zSetOperations.add(key, b, 2).as(StepVerifier::create).expectNext(true).verifyComplete();
		zSetOperations.add(key, c, 3).as(StepVerifier::create).expectNext(true).verifyComplete();

		zSetOperations.rangeAndStoreByScore(key, destKey, Range.closed(1.0, 2.0)).as(StepVerifier::create) //
				.expectNext(2L) //
				.verifyComplete();

		zSetOperations.range(destKey, Range.unbounded()).as(StepVerifier::create) //
				.expectNext(a) //
				.expectNext(b) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // GH-2345
	void reverseRangeAndStoreByLex() {

		assumeThat(serializer instanceof StringRedisSerializer).isTrue();

		K key = keyFactory.instance();
		K destKey = keyFactory.instance();
		V a = (V) "a";
		V b = (V) "b";
		V c = (V) "c";

		zSetOperations.add(key, a, 1).as(StepVerifier::create).expectNext(true).verifyComplete();
		zSetOperations.add(key, b, 2).as(StepVerifier::create).expectNext(true).verifyComplete();
		zSetOperations.add(key, c, 3).as(StepVerifier::create).expectNext(true).verifyComplete();

		zSetOperations.reverseRangeAndStoreByLex(key, destKey, Range.closed("a", "b")).as(StepVerifier::create) //
				.expectNext(2L) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // GH-2345
	void reverseRangeAndStoreByScore() {

		assumeThat(valueFactory instanceof ByteBufferObjectFactory).isFalse();

		K key = keyFactory.instance();
		K destKey = keyFactory.instance();
		V a = valueFactory.instance();
		V b = valueFactory.instance();
		V c = valueFactory.instance();

		zSetOperations.add(key, a, 1).as(StepVerifier::create).expectNext(true).verifyComplete();
		zSetOperations.add(key, b, 2).as(StepVerifier::create).expectNext(true).verifyComplete();
		zSetOperations.add(key, c, 3).as(StepVerifier::create).expectNext(true).verifyComplete();

		zSetOperations.reverseRangeAndStoreByScore(key, destKey, Range.closed(1.0, 2.0)).as(StepVerifier::create) //
				.expectNext(2L) //
				.verifyComplete();

		zSetOperations.range(destKey, Range.unbounded()).as(StepVerifier::create) //
				.expectNext(a) //
				.expectNext(b) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-743
	void scan() {

		assumeThat(valueFactory instanceof ByteBufferObjectFactory).isFalse();

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

	@ParameterizedRedisTest // DATAREDIS-602
	void count() {

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		zSetOperations.add(key, value1, 42.1).as(StepVerifier::create).expectNext(true).verifyComplete();
		zSetOperations.add(key, value2, 10).as(StepVerifier::create).expectNext(true).verifyComplete();

		zSetOperations.count(key, ZERO_TO_HUNDRED_DOUBLE).as(StepVerifier::create).expectNext(2L).expectComplete().verify();
		zSetOperations.count(key, Range.closed(0d, 10d)).as(StepVerifier::create).expectNext(1L).verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-729
	void lexCount() {

		assumeThat(serializer instanceof StringRedisSerializer).isTrue();

		K key = keyFactory.instance();

		zSetOperations.add(key, (V) "a", 0).as(StepVerifier::create).expectNext(true).verifyComplete();
		zSetOperations.add(key, (V) "b", 0).as(StepVerifier::create).expectNext(true).verifyComplete();
		zSetOperations.add(key, (V) "c", 0).as(StepVerifier::create).expectNext(true).verifyComplete();
		zSetOperations.add(key, (V) "d", 0).as(StepVerifier::create).expectNext(true).verifyComplete();
		zSetOperations.add(key, (V) "e", 0).as(StepVerifier::create).expectNext(true).verifyComplete();
		zSetOperations.add(key, (V) "f", 0).as(StepVerifier::create).expectNext(true).verifyComplete();
		zSetOperations.add(key, (V) "g", 0).as(StepVerifier::create).expectNext(true).verifyComplete();

		zSetOperations.lexCount(key, Range.unbounded()).as(StepVerifier::create).expectNext(7L).verifyComplete();
		zSetOperations.lexCount(key, Range.leftOpen("b", "f")).as(StepVerifier::create).expectNext(4L).verifyComplete();
		zSetOperations.lexCount(key, Range.rightOpen("b", "f")).as(StepVerifier::create).expectNext(4L).verifyComplete();
	}

	@ParameterizedRedisTest // GH-2007
	@EnabledOnCommand("ZPOPMIN")
	void popMin() {

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();
		V value3 = valueFactory.instance();
		V value4 = valueFactory.instance();

		zSetOperations.add(key, value1, 1).as(StepVerifier::create).expectNext(true).verifyComplete();
		zSetOperations.add(key, value2, 2).as(StepVerifier::create).expectNext(true).verifyComplete();
		zSetOperations.add(key, value3, 3).as(StepVerifier::create).expectNext(true).verifyComplete();
		zSetOperations.add(key, value4, 4).as(StepVerifier::create).expectNext(true).verifyComplete();

		zSetOperations.popMin(key).as(StepVerifier::create).expectNext(new DefaultTypedTuple<>(value1, 1D))
				.verifyComplete();
		zSetOperations.popMin(key, Duration.ofSeconds(1)).as(StepVerifier::create)
				.expectNext(new DefaultTypedTuple<>(value2, 2D)).verifyComplete();
		zSetOperations.popMin(key, 2).as(StepVerifier::create).expectNext(new DefaultTypedTuple<>(value3, 3D))
				.expectNext(new DefaultTypedTuple<>(value4, 4D)).verifyComplete();
	}

	@ParameterizedRedisTest // GH-2007
	@EnabledOnCommand("ZPOPMAX")
	void popMax() {

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();
		V value3 = valueFactory.instance();
		V value4 = valueFactory.instance();

		zSetOperations.add(key, value1, 1).as(StepVerifier::create).expectNext(true).verifyComplete();
		zSetOperations.add(key, value2, 2).as(StepVerifier::create).expectNext(true).verifyComplete();
		zSetOperations.add(key, value3, 3).as(StepVerifier::create).expectNext(true).verifyComplete();
		zSetOperations.add(key, value4, 4).as(StepVerifier::create).expectNext(true).verifyComplete();

		zSetOperations.popMax(key).as(StepVerifier::create).expectNext(new DefaultTypedTuple<>(value4, 4D))
				.verifyComplete();
		zSetOperations.popMax(key, Duration.ofSeconds(1)).as(StepVerifier::create)
				.expectNext(new DefaultTypedTuple<>(value3, 3D)).verifyComplete();
		zSetOperations.popMax(key, 2).as(StepVerifier::create).expectNext(new DefaultTypedTuple<>(value2, 2D))
				.expectNext(new DefaultTypedTuple<>(value1, 1D)).verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-602
	void size() {

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		zSetOperations.add(key, value1, 42.1).as(StepVerifier::create).expectNext(true).verifyComplete();
		zSetOperations.add(key, value2, 10).as(StepVerifier::create).expectNext(true).verifyComplete();

		zSetOperations.size(key).as(StepVerifier::create).expectNext(2L).verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-602
	void score() {

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		zSetOperations.add(key, value1, 42.1).as(StepVerifier::create).expectNext(true).verifyComplete();
		zSetOperations.add(key, value2, 10).as(StepVerifier::create).expectNext(true).verifyComplete();

		zSetOperations.score(key, value1).as(StepVerifier::create).expectNext(42.1d).verifyComplete();
		zSetOperations.score(key, value2).as(StepVerifier::create).expectNext(10d).verifyComplete();
	}

	@ParameterizedRedisTest // GH-2038
	@EnabledOnCommand("ZMSCORE")
	void scores() {

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		zSetOperations.add(key, value1, 42.1).as(StepVerifier::create).expectNext(true).verifyComplete();
		zSetOperations.add(key, value2, 10).as(StepVerifier::create).expectNext(true).verifyComplete();

		zSetOperations.score(key, value1, value2, valueFactory.instance()).as(StepVerifier::create)
				.expectNext(Arrays.asList(42.1d, 10d, null)).verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-602
	void removeRange() {

		assumeThat(valueFactory instanceof ByteBufferObjectFactory).isFalse();

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		zSetOperations.add(key, value1, 42.1).as(StepVerifier::create).expectNext(true).verifyComplete();
		zSetOperations.add(key, value2, 10).as(StepVerifier::create).expectNext(true).verifyComplete();

		zSetOperations.removeRange(key, ZERO_RANGE).as(StepVerifier::create).expectNext(1L).verifyComplete();
		zSetOperations.range(key, ZERO_TO_FIVE).as(StepVerifier::create).expectNext(value1).verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-602
	void removeRangeByScore() {

		assumeThat(valueFactory instanceof ByteBufferObjectFactory).isFalse();

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		zSetOperations.add(key, value1, 42.1).as(StepVerifier::create).expectNext(true).verifyComplete();
		zSetOperations.add(key, value2, 10).as(StepVerifier::create).expectNext(true).verifyComplete();

		zSetOperations.removeRangeByScore(key, NINE_TO_ELEVEN_DOUBLE).as(StepVerifier::create).expectNext(1L)
				.expectComplete().verify();
		zSetOperations.range(key, ZERO_TO_FIVE).as(StepVerifier::create) //
				.expectNext(value1) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // GH-2041
	void difference() {

		K key = keyFactory.instance();
		K otherKey = keyFactory.instance();

		V onlyInKey = valueFactory.instance();
		V shared = valueFactory.instance();
		V onlyInOtherKey = valueFactory.instance();

		zSetOperations.add(key, onlyInKey, 10).as(StepVerifier::create).expectNext(true).verifyComplete();
		zSetOperations.add(key, shared, 11).as(StepVerifier::create).expectNext(true).verifyComplete();

		zSetOperations.add(otherKey, onlyInOtherKey, 10).as(StepVerifier::create).expectNext(true).verifyComplete();
		zSetOperations.add(otherKey, shared, 11).as(StepVerifier::create).expectNext(true).verifyComplete();

		zSetOperations.difference(key, otherKey).as(StepVerifier::create).expectNext(onlyInKey).verifyComplete();

		zSetOperations.differenceWithScores(key, otherKey).as(StepVerifier::create)
				.expectNext(new DefaultTypedTuple<>(onlyInKey, 10D)).verifyComplete();
	}

	@ParameterizedRedisTest // GH-2041
	void differenceAndStore() {

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

		zSetOperations.differenceAndStore(key, otherKey, destKey).as(StepVerifier::create).expectNext(1L).verifyComplete();

		zSetOperations.range(destKey, ZERO_TO_FIVE).as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // GH-2042
	@EnabledOnCommand("ZINTER")
	void intersect() {

		K key = keyFactory.instance();
		K otherKey = keyFactory.instance();

		V onlyInKey = valueFactory.instance();
		V shared = valueFactory.instance();
		V onlyInOtherKey = valueFactory.instance();

		zSetOperations.add(key, onlyInKey, 10).as(StepVerifier::create).expectNext(true).verifyComplete();
		zSetOperations.add(key, shared, 11).as(StepVerifier::create).expectNext(true).verifyComplete();

		zSetOperations.add(otherKey, onlyInOtherKey, 10).as(StepVerifier::create).expectNext(true).verifyComplete();
		zSetOperations.add(otherKey, shared, 11).as(StepVerifier::create).expectNext(true).verifyComplete();

		zSetOperations.intersect(key, otherKey).as(StepVerifier::create).expectNext(shared).verifyComplete();

		zSetOperations.intersectWithScores(key, otherKey).as(StepVerifier::create)
				.expectNext(new DefaultTypedTuple<>(shared, 22D)).verifyComplete();

		zSetOperations.intersectWithScores(key, Collections.singleton(otherKey), Aggregate.SUM, Weights.of(1, 2))
				.as(StepVerifier::create).expectNext(new DefaultTypedTuple<>(shared, 33D)).verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-602
	void intersectAndStore() {

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

	@ParameterizedRedisTest // DATAREDIS-746
	void intersectAndStoreWithAggregation() {

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
				.as(StepVerifier::create).expectNext(1L).expectComplete().verify();

		zSetOperations.score(destKey, shared).as(StepVerifier::create) //
				.expectNext(22d) //
				.verifyComplete();

		zSetOperations.intersectAndStore(key, Collections.singletonList(otherKey), destKey, Aggregate.SUM, Weights.of(1, 2))
				.as(StepVerifier::create).expectNext(1L).expectComplete().verify();

		zSetOperations.score(destKey, shared).as(StepVerifier::create) //
				.expectNext(33d) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // GH-2042
	@EnabledOnCommand("ZUNION")
	void union() {

		K key = keyFactory.instance();
		K otherKey = keyFactory.instance();

		V onlyInKey = valueFactory.instance();
		V shared = valueFactory.instance();
		V onlyInOtherKey = valueFactory.instance();

		zSetOperations.add(key, onlyInKey, 10).as(StepVerifier::create).expectNext(true).verifyComplete();
		zSetOperations.add(key, shared, 11).as(StepVerifier::create).expectNext(true).verifyComplete();

		zSetOperations.add(otherKey, onlyInOtherKey, 10).as(StepVerifier::create).expectNext(true).verifyComplete();
		zSetOperations.add(otherKey, shared, 11).as(StepVerifier::create).expectNext(true).verifyComplete();

		zSetOperations.union(key, otherKey).as(StepVerifier::create).expectNextCount(3).verifyComplete();

		zSetOperations.unionWithScores(key, otherKey).collectList().as(StepVerifier::create).assertNext(actual -> {
			assertThat(actual).containsOnly(new DefaultTypedTuple<>(onlyInKey, 10D), new DefaultTypedTuple<>(shared, 22D),
					new DefaultTypedTuple<>(onlyInOtherKey, 10D));

		}).verifyComplete();

		zSetOperations.unionWithScores(key, Collections.singleton(otherKey), Aggregate.SUM, Weights.of(1, 2)).collectList()
				.as(StepVerifier::create).assertNext(actual -> {
					assertThat(actual).containsOnly(new DefaultTypedTuple<>(onlyInKey, 10D), new DefaultTypedTuple<>(shared, 33D),
							new DefaultTypedTuple<>(onlyInOtherKey, 20D));

				}).verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-602
	void unionAndStore() {

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

	@ParameterizedRedisTest // DATAREDIS-746
	void unionAndStoreWithAggregation() {

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
				.as(StepVerifier::create).expectNext(3L).verifyComplete();
		zSetOperations.score(destKey, shared).as(StepVerifier::create).expectNext(33d).verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-602
	void rangeByLex() {

		assumeThat(serializer instanceof StringRedisSerializer).isTrue();

		K key = keyFactory.instance();
		V a = (V) "a";
		V b = (V) "b";

		zSetOperations.add(key, a, 10).as(StepVerifier::create).expectNext(true).verifyComplete();
		zSetOperations.add(key, b, 11).as(StepVerifier::create).expectNext(true).verifyComplete();

		zSetOperations.rangeByLex(key, Range.just("a")).as(StepVerifier::create) //
				.expectNext(a) //
				.verifyComplete();

	}

	@ParameterizedRedisTest // DATAREDIS-602
	void rangeByLexWithLimit() {

		assumeThat(serializer instanceof StringRedisSerializer).isTrue();

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

	@ParameterizedRedisTest // DATAREDIS-602
	void reverseRangeByLex() {

		assumeThat(serializer instanceof StringRedisSerializer).isTrue();

		K key = keyFactory.instance();
		V a = (V) "a";
		V b = (V) "b";

		zSetOperations.add(key, a, 10).as(StepVerifier::create).expectNext(true).verifyComplete();
		zSetOperations.add(key, b, 11).as(StepVerifier::create).expectNext(true).verifyComplete();

		zSetOperations.reverseRangeByLex(key, Range.just("a")).as(StepVerifier::create) //
				.expectNext(a) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-602
	void reverseRangeByLexLimit() {

		assumeThat(serializer instanceof StringRedisSerializer).isTrue();

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

	@ParameterizedRedisTest // DATAREDIS-602
	void delete() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		zSetOperations.add(key, value, 10).as(StepVerifier::create).expectNext(true).verifyComplete();

		zSetOperations.delete(key).as(StepVerifier::create).expectNext(true).verifyComplete();

		zSetOperations.size(key).as(StepVerifier::create) //
				.expectNext(0L) //
				.verifyComplete();
	}

}
