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

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.springframework.data.redis.ByteBufferObjectFactory;
import org.springframework.data.redis.ConnectionFactoryTracker;
import org.springframework.data.redis.ObjectFactory;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.serializer.RedisSerializer;

/**
 * Integration tests for {@link DefaultReactiveSetOperations}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 */
@RunWith(Parameterized.class)
@SuppressWarnings("unchecked")
public class DefaultReactiveSetOperationsIntegrationTests<K, V> {

	private final ReactiveRedisTemplate<K, V> redisTemplate;
	private final ReactiveSetOperations<K, V> setOperations;

	private final ObjectFactory<K> keyFactory;
	private final ObjectFactory<V> valueFactory;

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
	public DefaultReactiveSetOperationsIntegrationTests(ReactiveRedisTemplate<K, V> redisTemplate,
			ObjectFactory<K> keyFactory, ObjectFactory<V> valueFactory, RedisSerializer serializer, String label) {

		this.redisTemplate = redisTemplate;
		this.setOperations = redisTemplate.opsForSet();
		this.keyFactory = keyFactory;
		this.valueFactory = valueFactory;

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
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		setOperations.add(key, value1).as(StepVerifier::create).expectNext(1L).verifyComplete();
		setOperations.add(key, value1, value2).as(StepVerifier::create).expectNext(1L).verifyComplete();
	}

	@Test // DATAREDIS-602
	public void remove() {

		assumeFalse(valueFactory instanceof ByteBufferObjectFactory);

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
	public void pop() {

		assumeFalse(valueFactory instanceof ByteBufferObjectFactory);

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		setOperations.add(key, value1, value2).as(StepVerifier::create).expectNext(2L).verifyComplete();
		setOperations.pop(key).as(StepVerifier::create).consumeNextWith(actual -> {
			assertThat(actual).isIn(value1, value2);
		}).verifyComplete();
	}

	@Test // DATAREDIS-668
	public void popWithCount() {

		assumeFalse(valueFactory instanceof ByteBufferObjectFactory);

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();
		V value3 = valueFactory.instance();

		setOperations.add(key, value1, value2, value3).as(StepVerifier::create).expectNext(3L).verifyComplete();
		setOperations.pop(key, 2).as(StepVerifier::create).expectNextCount(2).verifyComplete();
		setOperations.size(key).as(StepVerifier::create).expectNext(1L).verifyComplete();
	}

	@Test // DATAREDIS-602
	public void move() {

		K key = keyFactory.instance();
		K otherKey = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		setOperations.add(key, value1, value2).as(StepVerifier::create).expectNext(2L).verifyComplete();
		setOperations.move(key, value1, otherKey).as(StepVerifier::create).expectNext(true).verifyComplete();

		setOperations.size(otherKey).as(StepVerifier::create).expectNext(1L).verifyComplete();
	}

	@Test // DATAREDIS-602
	public void isMember() {

		assumeFalse(valueFactory instanceof ByteBufferObjectFactory);

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		setOperations.add(key, value1, value2).as(StepVerifier::create).expectNext(2L).verifyComplete();
		setOperations.isMember(key, value1).as(StepVerifier::create).expectNext(true).verifyComplete();
	}

	@Test // DATAREDIS-602, DATAREDIS-873
	public void intersect() {

		assumeFalse(valueFactory instanceof ByteBufferObjectFactory);

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
	public void intersectAndStore() {

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

	@Test // DATAREDIS-602, DATAREDIS-873
	public void difference() {

		assumeFalse(valueFactory instanceof ByteBufferObjectFactory);

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
	public void differenceAndStore() {

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
	public void union() {

		assumeFalse(valueFactory instanceof ByteBufferObjectFactory);

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
	public void unionAndStore() {

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
	public void members() {

		assumeFalse(valueFactory instanceof ByteBufferObjectFactory);

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		setOperations.add(key, value1, value2).as(StepVerifier::create).expectNext(2L).verifyComplete();
		setOperations.members(key).as(StepVerifier::create) //
				.consumeNextWith(actual -> assertThat(actual).isIn(value1, value2)).expectNextCount(1).verifyComplete();
	}

	@Test // DATAREDIS-743
	public void scan() {

		assumeFalse(valueFactory instanceof ByteBufferObjectFactory);

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
	public void randomMember() {

		assumeFalse(valueFactory instanceof ByteBufferObjectFactory);

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		setOperations.add(key, value1, value2).as(StepVerifier::create).expectNext(2L).verifyComplete();

		setOperations.randomMember(key).as(StepVerifier::create).consumeNextWith(actual -> {
			assertThat(actual).isIn(value1, value2);
		}).verifyComplete();
	}

	@Test // DATAREDIS-602
	public void randomMembers() {

		assumeFalse(valueFactory instanceof ByteBufferObjectFactory);

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		setOperations.add(key, value1, value2).as(StepVerifier::create).expectNext(2L).verifyComplete();

		setOperations.randomMembers(key, 3).as(StepVerifier::create).expectNextCount(3).verifyComplete();
	}

	@Test // DATAREDIS-602
	public void distinctRandomMembers() {

		assumeFalse(valueFactory instanceof ByteBufferObjectFactory);

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		setOperations.add(key, value1, value2).as(StepVerifier::create).expectNext(2L).verifyComplete();

		setOperations.distinctRandomMembers(key, 2).as(StepVerifier::create) //
				.expectNextCount(2) //
				.verifyComplete();
	}

	@Test // DATAREDIS-602
	public void delete() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		setOperations.add(key, value).as(StepVerifier::create).expectNext(1L).verifyComplete();

		setOperations.delete(key).as(StepVerifier::create).expectNext(true).verifyComplete();

		setOperations.size(key).as(StepVerifier::create).expectNext(0L).verifyComplete();
	}
}
