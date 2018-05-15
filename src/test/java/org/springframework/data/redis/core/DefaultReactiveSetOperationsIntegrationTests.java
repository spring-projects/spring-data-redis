/*
 * Copyright 2017-2018 the original author or authors.
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

import static org.assertj.core.api.Assertions.*;
import static org.junit.Assume.*;

import reactor.test.StepVerifier;

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

		StepVerifier.create(setOperations.add(key, value1)).expectNext(1L).verifyComplete();
		StepVerifier.create(setOperations.add(key, value1, value2)).expectNext(1L).verifyComplete();
	}

	@Test // DATAREDIS-602
	public void remove() {

		assumeFalse(valueFactory instanceof ByteBufferObjectFactory);

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		StepVerifier.create(setOperations.add(key, value1, value2)).expectNext(2L).verifyComplete();
		StepVerifier.create(setOperations.size(key)).expectNext(2L).verifyComplete();
		StepVerifier.create(setOperations.remove(key, value2)).expectNext(1L).verifyComplete();
		StepVerifier.create(setOperations.size(key)).expectNext(1L).verifyComplete();
		StepVerifier.create(setOperations.remove(key, value1, value2)).expectNext(1L).verifyComplete();
	}

	@Test // DATAREDIS-602
	public void pop() {

		assumeFalse(valueFactory instanceof ByteBufferObjectFactory);

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		StepVerifier.create(setOperations.add(key, value1, value2)).expectNext(2L).verifyComplete();
		StepVerifier.create(setOperations.pop(key)).consumeNextWith(actual -> {
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

		StepVerifier.create(setOperations.add(key, value1, value2, value3)).expectNext(3L).verifyComplete();
		StepVerifier.create(setOperations.pop(key, 2)).expectNextCount(2).verifyComplete();
		StepVerifier.create(setOperations.size(key)).expectNext(1L).verifyComplete();
	}

	@Test // DATAREDIS-602
	public void move() {

		K key = keyFactory.instance();
		K otherKey = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		StepVerifier.create(setOperations.add(key, value1, value2)).expectNext(2L).verifyComplete();
		StepVerifier.create(setOperations.move(key, value1, otherKey)).expectNext(true).verifyComplete();

		StepVerifier.create(setOperations.size(otherKey)).expectNext(1L).verifyComplete();
	}

	@Test // DATAREDIS-602
	public void isMember() {

		assumeFalse(valueFactory instanceof ByteBufferObjectFactory);

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		StepVerifier.create(setOperations.add(key, value1, value2)).expectNext(2L).verifyComplete();
		StepVerifier.create(setOperations.isMember(key, value1)).expectNext(true).verifyComplete();
	}

	@Test // DATAREDIS-602
	public void intersect() {

		assumeFalse(valueFactory instanceof ByteBufferObjectFactory);

		K key = keyFactory.instance();
		K otherKey = keyFactory.instance();

		V onlyInKey = valueFactory.instance();
		V shared = valueFactory.instance();
		V onlyInOtherKey = valueFactory.instance();

		StepVerifier.create(setOperations.add(key, onlyInKey, shared)).expectNext(2L).verifyComplete();
		StepVerifier.create(setOperations.add(otherKey, onlyInOtherKey, shared)).expectNext(2L).verifyComplete();

		StepVerifier.create(setOperations.intersect(key, otherKey)) //
				.consumeNextWith(actual -> {
					assertThat(actual).isEqualTo(shared);
				}) //
				.verifyComplete();
	}

	@Test // DATAREDIS-602
	public void intersectAndStore() {

		K key = keyFactory.instance();
		K otherKey = keyFactory.instance();
		K destKey = keyFactory.instance();

		V onlyInKey = valueFactory.instance();
		V shared = valueFactory.instance();
		V onlyInOtherKey = valueFactory.instance();

		StepVerifier.create(setOperations.add(key, onlyInKey, shared)).expectNext(2L).verifyComplete();
		StepVerifier.create(setOperations.add(otherKey, onlyInOtherKey, shared)).expectNext(2L).verifyComplete();

		StepVerifier.create(setOperations.intersectAndStore(key, otherKey, destKey)).expectNext(1L).expectComplete()
				.verify();

		StepVerifier.create(setOperations.isMember(destKey, shared)).expectNext(true).verifyComplete();
	}

	@Test // DATAREDIS-602
	public void difference() {

		assumeFalse(valueFactory instanceof ByteBufferObjectFactory);

		K key = keyFactory.instance();
		K otherKey = keyFactory.instance();

		V onlyInKey = valueFactory.instance();
		V shared = valueFactory.instance();
		V onlyInOtherKey = valueFactory.instance();

		StepVerifier.create(setOperations.add(key, onlyInKey, shared)).expectNext(2L).verifyComplete();
		StepVerifier.create(setOperations.add(otherKey, onlyInOtherKey, shared)).expectNext(2L).verifyComplete();

		StepVerifier.create(setOperations.difference(key, otherKey)) //
				.consumeNextWith(actual -> {
					assertThat(actual).isEqualTo(onlyInKey);
				}) //
				.verifyComplete();
	}

	@Test // DATAREDIS-602
	public void differenceAndStore() {

		K key = keyFactory.instance();
		K otherKey = keyFactory.instance();
		K destKey = keyFactory.instance();

		V onlyInKey = valueFactory.instance();
		V shared = valueFactory.instance();
		V onlyInOtherKey = valueFactory.instance();

		StepVerifier.create(setOperations.add(key, onlyInKey, shared)).expectNext(2L).verifyComplete();
		StepVerifier.create(setOperations.add(otherKey, onlyInOtherKey, shared)).expectNext(2L).verifyComplete();

		StepVerifier.create(setOperations.differenceAndStore(key, otherKey, destKey)).expectNext(1L).expectComplete()
				.verify();

		StepVerifier.create(setOperations.isMember(destKey, onlyInKey)).expectNext(true).verifyComplete();
	}

	@Test // DATAREDIS-602
	public void union() {

		assumeFalse(valueFactory instanceof ByteBufferObjectFactory);

		K key = keyFactory.instance();
		K otherKey = keyFactory.instance();

		V onlyInKey = valueFactory.instance();
		V shared = valueFactory.instance();
		V onlyInOtherKey = valueFactory.instance();

		StepVerifier.create(setOperations.add(key, onlyInKey, shared)).expectNext(2L).verifyComplete();
		StepVerifier.create(setOperations.add(otherKey, onlyInOtherKey, shared)).expectNext(2L).verifyComplete();

		StepVerifier.create(setOperations.union(key, otherKey)) //
				.expectNextCount(3) //
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

		StepVerifier.create(setOperations.add(key, onlyInKey, shared)).expectNext(2L).verifyComplete();
		StepVerifier.create(setOperations.add(otherKey, onlyInOtherKey, shared)).expectNext(2L).verifyComplete();

		StepVerifier.create(setOperations.unionAndStore(key, otherKey, destKey)).expectNext(3L).verifyComplete();

		StepVerifier.create(setOperations.isMember(destKey, onlyInKey)).expectNext(true).verifyComplete();
		StepVerifier.create(setOperations.isMember(destKey, shared)).expectNext(true).verifyComplete();
		StepVerifier.create(setOperations.isMember(destKey, onlyInOtherKey)).expectNext(true).verifyComplete();
	}

	@Test // DATAREDIS-602
	public void members() {

		assumeFalse(valueFactory instanceof ByteBufferObjectFactory);

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		StepVerifier.create(setOperations.add(key, value1, value2)).expectNext(2L).verifyComplete();
		StepVerifier.create(setOperations.members(key)) //
				.consumeNextWith(actual -> assertThat(actual).isIn(value1, value2)).expectNextCount(1).verifyComplete();
	}

	@Test // DATAREDIS-743
	public void scan() {

		assumeFalse(valueFactory instanceof ByteBufferObjectFactory);

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		StepVerifier.create(setOperations.add(key, value1, value2)) //
				.expectNext(2L) //
				.verifyComplete();

		StepVerifier.create(setOperations.scan(key)) //
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

		StepVerifier.create(setOperations.add(key, value1, value2)).expectNext(2L).verifyComplete();

		StepVerifier.create(setOperations.randomMember(key)).consumeNextWith(actual -> {
			assertThat(actual).isIn(value1, value2);
		}).verifyComplete();
	}

	@Test // DATAREDIS-602
	public void randomMembers() {

		assumeFalse(valueFactory instanceof ByteBufferObjectFactory);

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		StepVerifier.create(setOperations.add(key, value1, value2)).expectNext(2L).verifyComplete();

		StepVerifier.create(setOperations.randomMembers(key, 3)).expectNextCount(3).verifyComplete();
	}

	@Test // DATAREDIS-602
	public void distinctRandomMembers() {

		assumeFalse(valueFactory instanceof ByteBufferObjectFactory);

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		StepVerifier.create(setOperations.add(key, value1, value2)).expectNext(2L).verifyComplete();

		StepVerifier.create(setOperations.distinctRandomMembers(key, 2)) //
				.expectNextCount(2) //
				.verifyComplete();
	}

	@Test // DATAREDIS-602
	public void delete() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		StepVerifier.create(setOperations.add(key, value)).expectNext(1L).verifyComplete();

		StepVerifier.create(setOperations.delete(key)).expectNext(true).verifyComplete();

		StepVerifier.create(setOperations.size(key)).expectNext(0L).verifyComplete();
	}
}
