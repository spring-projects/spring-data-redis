/*
 * Copyright 2017-2025 the original author or authors.
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

import java.util.Arrays;
import java.util.Collection;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.MethodSource;

import org.springframework.data.redis.ByteBufferObjectFactory;
import org.springframework.data.redis.ObjectFactory;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.ReactiveOperationsTestParams.Fixture;
import org.springframework.data.redis.test.condition.EnabledOnCommand;

/**
 * Integration tests for {@link DefaultReactiveSetOperations}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @author Mingi Lee
 */
@ParameterizedClass
@MethodSource("testParams")
@SuppressWarnings("unchecked")
public class DefaultReactiveSetOperationsIntegrationTests<K, V> {

	private final ReactiveRedisTemplate<K, V> redisTemplate;
	private final ReactiveSetOperations<K, V> setOperations;

	private final ObjectFactory<K> keyFactory;
	private final ObjectFactory<V> valueFactory;

	public static Collection<Fixture<?, ?>> testParams() {
		return ReactiveOperationsTestParams.testParams();
	}

	public DefaultReactiveSetOperationsIntegrationTests(Fixture<K, V> fixture) {

		this.redisTemplate = fixture.getTemplate();
		this.setOperations = redisTemplate.opsForSet();
		this.keyFactory = fixture.getKeyFactory();
		this.valueFactory = fixture.getValueFactory();
	}

	@BeforeEach
	void before() {

		RedisConnectionFactory connectionFactory = (RedisConnectionFactory) redisTemplate.getConnectionFactory();
		RedisConnection connection = connectionFactory.getConnection();
		connection.flushAll();
		connection.close();
	}

	@Test
	// DATAREDIS-602
	void add() {

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		setOperations.add(key, value1).as(StepVerifier::create).expectNext(1L).verifyComplete();
		setOperations.add(key, value1, value2).as(StepVerifier::create).expectNext(1L).verifyComplete();
	}

	@Test // DATAREDIS-602
	void remove() {

		assumeThat(valueFactory instanceof ByteBufferObjectFactory).isFalse();

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		setOperations.add(key, value1, value2).as(StepVerifier::create).expectNext(2L).verifyComplete();
		setOperations.size(key).as(StepVerifier::create).expectNext(2L).verifyComplete();
		setOperations.remove(key, value2).as(StepVerifier::create).expectNext(1L).verifyComplete();
		setOperations.size(key).as(StepVerifier::create).expectNext(1L).verifyComplete();
		setOperations.remove(key, value1, value2).as(StepVerifier::create).expectNext(1L).verifyComplete();
	}

	@Test // DATAREDIS-602
	void pop() {

		assumeThat(valueFactory instanceof ByteBufferObjectFactory).isFalse();

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		setOperations.add(key, value1, value2).as(StepVerifier::create).expectNext(2L).verifyComplete();
		setOperations.pop(key).as(StepVerifier::create).consumeNextWith(actual -> {
			assertThat(actual).isIn(value1, value2);
		}).verifyComplete();
	}

	@Test // DATAREDIS-668
	void popWithCount() {

		assumeThat(valueFactory instanceof ByteBufferObjectFactory).isFalse();

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();
		V value3 = valueFactory.instance();

		setOperations.add(key, value1, value2, value3).as(StepVerifier::create).expectNext(3L).verifyComplete();
		setOperations.pop(key, 2).as(StepVerifier::create).expectNextCount(2).verifyComplete();
		setOperations.size(key).as(StepVerifier::create).expectNext(1L).verifyComplete();
	}

	@Test // DATAREDIS-602
	void move() {

		K key = keyFactory.instance();
		K otherKey = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		setOperations.add(key, value1, value2).as(StepVerifier::create).expectNext(2L).verifyComplete();
		setOperations.move(key, value1, otherKey).as(StepVerifier::create).expectNext(true).verifyComplete();

		setOperations.size(otherKey).as(StepVerifier::create).expectNext(1L).verifyComplete();
	}

	@Test // DATAREDIS-602
	void isMember() {

		assumeThat(valueFactory instanceof ByteBufferObjectFactory).isFalse();

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		setOperations.add(key, value1, value2).as(StepVerifier::create).expectNext(2L).verifyComplete();
		setOperations.isMember(key, value1).as(StepVerifier::create).expectNext(true).verifyComplete();
	}

	@Test // GH-2037
	@EnabledOnCommand("SMISMEMBER")
	void isMembers() {

		assumeThat(valueFactory instanceof ByteBufferObjectFactory).isFalse();

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		setOperations.add(key, value1).as(StepVerifier::create).expectNext(1L).verifyComplete();
		setOperations.isMember(key, value1, value2).as(StepVerifier::create).consumeNextWith(actual -> {

			assertThat(actual).containsEntry(value1, true).containsEntry(value2, false);
		}).verifyComplete();
	}

	@Test // DATAREDIS-602, DATAREDIS-873
	void intersect() {

		assumeThat(valueFactory instanceof ByteBufferObjectFactory).isFalse();

		K key = keyFactory.instance();
		K otherKey = keyFactory.instance();

		V onlyInKey = valueFactory.instance();
		V shared = valueFactory.instance();
		V onlyInOtherKey = valueFactory.instance();

		setOperations.add(key, onlyInKey, shared).as(StepVerifier::create).expectNext(2L).verifyComplete();
		setOperations.add(otherKey, onlyInOtherKey, shared).as(StepVerifier::create).expectNext(2L).verifyComplete();

		setOperations.intersect(key, otherKey).as(StepVerifier::create) //
				.consumeNextWith(actual -> {
					assertThat(actual).isEqualTo(shared);
				}) //
				.verifyComplete();

		setOperations.intersect(Arrays.asList(key, otherKey)).as(StepVerifier::create) //
				.consumeNextWith(actual -> {
					assertThat(actual).isEqualTo(shared);
				}) //
				.verifyComplete();
	}

	@Test // DATAREDIS-602, DATAREDIS-873
	void intersectAndStore() {

		K key = keyFactory.instance();
		K otherKey = keyFactory.instance();
		K destKey = keyFactory.instance();

		V onlyInKey = valueFactory.instance();
		V shared = valueFactory.instance();
		V onlyInOtherKey = valueFactory.instance();

		setOperations.add(key, onlyInKey, shared).as(StepVerifier::create).expectNext(2L).verifyComplete();
		setOperations.add(otherKey, onlyInOtherKey, shared).as(StepVerifier::create).expectNext(2L).verifyComplete();

		setOperations.intersectAndStore(key, otherKey, destKey).as(StepVerifier::create).expectNext(1L).expectComplete()
				.verify();

		setOperations.isMember(destKey, shared).as(StepVerifier::create).expectNext(true).verifyComplete();

		setOperations.delete(destKey).as(StepVerifier::create).expectNext(true).verifyComplete();

		setOperations.intersectAndStore(Arrays.asList(key, otherKey), destKey).as(StepVerifier::create).expectNext(1L)
				.verifyComplete();

		setOperations.isMember(destKey, shared).as(StepVerifier::create).expectNext(true).verifyComplete();
	}

	@Test
	@EnabledOnCommand("SINTERCARD")
	void intersectSize() {

		K key = keyFactory.instance();
		K otherKey = keyFactory.instance();
		K thirdKey = keyFactory.instance();

		V onlyInKey = valueFactory.instance();
		V shared1 = valueFactory.instance();
		V shared2 = valueFactory.instance();
		V onlyInOtherKey = valueFactory.instance();

		setOperations.add(key, onlyInKey, shared1, shared2).as(StepVerifier::create).expectNext(3L).verifyComplete();
		setOperations.add(otherKey, onlyInOtherKey, shared1, shared2).as(StepVerifier::create).expectNext(3L)
				.verifyComplete();
		setOperations.add(thirdKey, shared1).as(StepVerifier::create).expectNext(1L).verifyComplete();

		// Test intersectSize(key, otherKey)
		setOperations.intersectSize(key, otherKey).as(StepVerifier::create).expectNext(2L).verifyComplete();

		// Test intersectSize(key, Collection)
		setOperations.intersectSize(key, Arrays.asList(otherKey)).as(StepVerifier::create).expectNext(2L).verifyComplete();

		// Test intersectSize(Collection) with multiple keys
		setOperations.intersectSize(Arrays.asList(key, otherKey, thirdKey)).as(StepVerifier::create).expectNext(1L)
				.verifyComplete();

		// Test with empty intersection
		K emptyKey = keyFactory.instance();
		setOperations.intersectSize(key, emptyKey).as(StepVerifier::create).expectNext(0L).verifyComplete();
	}

	@Test // DATAREDIS-602, DATAREDIS-873
	void difference() {

		assumeThat(valueFactory instanceof ByteBufferObjectFactory).isFalse();

		K key = keyFactory.instance();
		K otherKey = keyFactory.instance();

		V onlyInKey = valueFactory.instance();
		V shared = valueFactory.instance();
		V onlyInOtherKey = valueFactory.instance();

		setOperations.add(key, onlyInKey, shared).as(StepVerifier::create).expectNext(2L).verifyComplete();
		setOperations.add(otherKey, onlyInOtherKey, shared).as(StepVerifier::create).expectNext(2L).verifyComplete();

		setOperations.difference(key, otherKey).as(StepVerifier::create) //
				.consumeNextWith(actual -> {
					assertThat(actual).isEqualTo(onlyInKey);
				}) //
				.verifyComplete();

		setOperations.difference(Arrays.asList(key, otherKey)).as(StepVerifier::create) //
				.consumeNextWith(actual -> {
					assertThat(actual).isEqualTo(onlyInKey);
				}) //
				.verifyComplete();
	}

	@Test // DATAREDIS-602, DATAREDIS-873
	void differenceAndStore() {

		K key = keyFactory.instance();
		K otherKey = keyFactory.instance();
		K destKey = keyFactory.instance();

		V onlyInKey = valueFactory.instance();
		V shared = valueFactory.instance();
		V onlyInOtherKey = valueFactory.instance();

		setOperations.add(key, onlyInKey, shared).as(StepVerifier::create).expectNext(2L).verifyComplete();
		setOperations.add(otherKey, onlyInOtherKey, shared).as(StepVerifier::create).expectNext(2L).verifyComplete();

		setOperations.differenceAndStore(key, otherKey, destKey).as(StepVerifier::create).expectNext(1L).expectComplete()
				.verify();

		setOperations.differenceAndStore(Arrays.asList(key, otherKey), destKey).as(StepVerifier::create).expectNext(1L)
				.verifyComplete();

		setOperations.isMember(destKey, onlyInKey).as(StepVerifier::create).expectNext(true).verifyComplete();
	}

	@Test // DATAREDIS-602, DATAREDIS-873
	void union() {

		assumeThat(valueFactory instanceof ByteBufferObjectFactory).isFalse();

		K key = keyFactory.instance();
		K otherKey = keyFactory.instance();

		V onlyInKey = valueFactory.instance();
		V shared = valueFactory.instance();
		V onlyInOtherKey = valueFactory.instance();

		setOperations.add(key, onlyInKey, shared).as(StepVerifier::create).expectNext(2L).verifyComplete();
		setOperations.add(otherKey, onlyInOtherKey, shared).as(StepVerifier::create).expectNext(2L).verifyComplete();

		setOperations.union(key, otherKey) //
				.as(StepVerifier::create) //
				.expectNextCount(3) //
				.verifyComplete();

		setOperations.union(Arrays.asList(key, otherKey)) //
				.as(StepVerifier::create) //
				.expectNextCount(3) //
				.verifyComplete();
	}

	@Test // DATAREDIS-602, DATAREDIS-873
	void unionAndStore() {

		K key = keyFactory.instance();
		K otherKey = keyFactory.instance();
		K destKey = keyFactory.instance();

		V onlyInKey = valueFactory.instance();
		V shared = valueFactory.instance();
		V onlyInOtherKey = valueFactory.instance();

		setOperations.add(key, onlyInKey, shared).as(StepVerifier::create).expectNext(2L).verifyComplete();
		setOperations.add(otherKey, onlyInOtherKey, shared).as(StepVerifier::create).expectNext(2L).verifyComplete();

		setOperations.unionAndStore(key, otherKey, destKey).as(StepVerifier::create).expectNext(3L).verifyComplete();

		setOperations.delete(destKey).as(StepVerifier::create).expectNext(true).verifyComplete();

		setOperations.unionAndStore(Arrays.asList(key, otherKey), destKey).as(StepVerifier::create).expectNext(3L)
				.verifyComplete();

		setOperations.isMember(destKey, onlyInKey).as(StepVerifier::create).expectNext(true).verifyComplete();
		setOperations.isMember(destKey, shared).as(StepVerifier::create).expectNext(true).verifyComplete();
		setOperations.isMember(destKey, onlyInOtherKey).as(StepVerifier::create).expectNext(true).verifyComplete();
	}

	@Test // DATAREDIS-602
	void members() {

		assumeThat(valueFactory instanceof ByteBufferObjectFactory).isFalse();

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		setOperations.add(key, value1, value2).as(StepVerifier::create).expectNext(2L).verifyComplete();
		setOperations.members(key).as(StepVerifier::create) //
				.consumeNextWith(actual -> assertThat(actual).isIn(value1, value2)).expectNextCount(1).verifyComplete();
	}

	@Test // DATAREDIS-743
	void scan() {

		assumeThat(valueFactory instanceof ByteBufferObjectFactory).isFalse();

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		setOperations.add(key, value1, value2).as(StepVerifier::create) //
				.expectNext(2L) //
				.verifyComplete();

		setOperations.scan(key).as(StepVerifier::create) //
				.consumeNextWith(actual -> assertThat(actual).isIn(value1, value2)) //
				.expectNextCount(1) //
				.verifyComplete();
	}

	@Test // DATAREDIS-602
	void randomMember() {

		assumeThat(valueFactory instanceof ByteBufferObjectFactory).isFalse();

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		setOperations.add(key, value1, value2).as(StepVerifier::create).expectNext(2L).verifyComplete();

		setOperations.randomMember(key).as(StepVerifier::create).consumeNextWith(actual -> {
			assertThat(actual).isIn(value1, value2);
		}).verifyComplete();
	}

	@Test // DATAREDIS-602
	void randomMembers() {

		assumeThat(valueFactory instanceof ByteBufferObjectFactory).isFalse();

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		setOperations.add(key, value1, value2).as(StepVerifier::create).expectNext(2L).verifyComplete();

		setOperations.randomMembers(key, 3).as(StepVerifier::create).expectNextCount(3).verifyComplete();
	}

	@Test // DATAREDIS-602
	void distinctRandomMembers() {

		assumeThat(valueFactory instanceof ByteBufferObjectFactory).isFalse();

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		setOperations.add(key, value1, value2).as(StepVerifier::create).expectNext(2L).verifyComplete();

		setOperations.distinctRandomMembers(key, 2).as(StepVerifier::create) //
				.expectNextCount(2) //
				.verifyComplete();
	}

	@Test // DATAREDIS-602
	void delete() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		setOperations.add(key, value).as(StepVerifier::create).expectNext(1L).verifyComplete();

		setOperations.delete(key).as(StepVerifier::create).expectNext(true).verifyComplete();

		setOperations.size(key).as(StepVerifier::create).expectNext(0L).verifyComplete();
	}
}
