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

import static org.junit.Assume.*;

import reactor.test.StepVerifier;

import java.time.Duration;
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
 * Integration tests for {@link DefaultReactiveListOperations}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 */
@RunWith(Parameterized.class)
@SuppressWarnings("unchecked")
public class DefaultReactiveListOperationsIntegrationTests<K, V> {

	private final ReactiveRedisTemplate<K, V> redisTemplate;
	private final ReactiveListOperations<K, V> listOperations;

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
	public DefaultReactiveListOperationsIntegrationTests(ReactiveRedisTemplate<K, V> redisTemplate,
			ObjectFactory<K> keyFactory, ObjectFactory<V> valueFactory, RedisSerializer serializer, String label) {

		this.redisTemplate = redisTemplate;
		this.listOperations = redisTemplate.opsForList();
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
	public void trim() {

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		StepVerifier.create(listOperations.rightPushAll(key, value1, value2)) //
				.expectNext(2L) //
				.verifyComplete();

		StepVerifier.create(listOperations.trim(key, 0, 0)) //
				.expectNext(true) //
				.verifyComplete();

		StepVerifier.create(listOperations.size(key)) //
				.expectNext(1L) //
				.verifyComplete();
	}

	@Test // DATAREDIS-602
	public void size() {

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();

		StepVerifier.create(listOperations.size(key)) //
				.expectNext(0L) //
				.verifyComplete();

		StepVerifier.create(listOperations.rightPush(key, value1)) //
				.expectNext(1L) //
				.verifyComplete();

		StepVerifier.create(listOperations.size(key)) //
				.expectNext(1L) //
				.verifyComplete();
	}

	@Test // DATAREDIS-602
	public void leftPush() {

		assumeFalse(valueFactory instanceof ByteBufferObjectFactory);

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		StepVerifier.create(listOperations.leftPush(key, value1)) //
				.expectNext(1L) //
				.verifyComplete();

		StepVerifier.create(listOperations.leftPush(key, value2)) //
				.expectNext(2L) //
				.verifyComplete();

		StepVerifier.create(listOperations.range(key, 0, -1)) //
				.expectNext(value2) //
				.expectNext(value1) //
				.verifyComplete();
	}

	@Test // DATAREDIS-602
	public void leftPushAll() {

		assumeFalse(valueFactory instanceof ByteBufferObjectFactory);

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		StepVerifier.create(listOperations.leftPushAll(key, value1, value2)) //
				.expectNext(2L) //
				.verifyComplete();

		StepVerifier.create(listOperations.range(key, 0, -1)) //
				.expectNext(value2) //
				.expectNext(value1) //
				.verifyComplete();
	}

	@Test // DATAREDIS-602
	public void leftPushIfPresent() {

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		StepVerifier.create(listOperations.leftPushIfPresent(key, value1)) //
				.expectNext(0L) //
				.verifyComplete();

		StepVerifier.create(listOperations.leftPush(key, value1)) //
				.expectNext(1L) //
				.verifyComplete();

		StepVerifier.create(listOperations.leftPushIfPresent(key, value2)) //
				.expectNext(2L) //
				.verifyComplete();
	}

	@Test // DATAREDIS-602
	public void leftPushWithPivot() {

		assumeFalse(valueFactory instanceof ByteBufferObjectFactory);

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();
		V value3 = valueFactory.instance();

		StepVerifier.create(listOperations.leftPushAll(key, value1, value2)) //
				.expectNext(2L) //
				.verifyComplete();

		StepVerifier.create(listOperations.leftPush(key, value1, value3)) //
				.expectNext(3L) //
				.verifyComplete();

		StepVerifier.create(listOperations.range(key, 0, -1)) //
				.expectNext(value2) //
				.expectNext(value3) //
				.expectNext(value1) //
				.verifyComplete();
	}

	@Test // DATAREDIS-602
	public void rightPush() {

		assumeFalse(valueFactory instanceof ByteBufferObjectFactory);

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		StepVerifier.create(listOperations.rightPush(key, value1)) //
				.expectNext(1L) //
				.verifyComplete();
		StepVerifier.create(listOperations.rightPush(key, value2)) //
				.expectNext(2L) //
				.verifyComplete();

		StepVerifier.create(listOperations.range(key, 0, -1)) //
				.expectNext(value1) //
				.expectNext(value2) //
				.verifyComplete();
	}

	@Test // DATAREDIS-602
	public void rightPushAll() {

		assumeFalse(valueFactory instanceof ByteBufferObjectFactory);

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		StepVerifier.create(listOperations.rightPushAll(key, value1, value2)).expectNext(2L).verifyComplete();

		StepVerifier.create(listOperations.range(key, 0, -1)) //
				.expectNext(value1) //
				.expectNext(value2) //
				.verifyComplete();
	}

	@Test // DATAREDIS-602
	public void rightPushIfPresent() {

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		StepVerifier.create(listOperations.rightPushIfPresent(key, value1)).expectNext(0L).verifyComplete();
		StepVerifier.create(listOperations.rightPush(key, value1)).expectNext(1L).verifyComplete();
		StepVerifier.create(listOperations.rightPushIfPresent(key, value2)).expectNext(2L).verifyComplete();
	}

	@Test // DATAREDIS-602
	public void rightPushWithPivot() {

		assumeFalse(valueFactory instanceof ByteBufferObjectFactory);

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();
		V value3 = valueFactory.instance();

		StepVerifier.create(listOperations.rightPushAll(key, value1, value2)).expectNext(2L).verifyComplete();

		StepVerifier.create(listOperations.rightPush(key, value1, value3)).expectNext(3L).verifyComplete();

		StepVerifier.create(listOperations.range(key, 0, -1)) //
				.expectNext(value1) //
				.expectNext(value3) //
				.expectNext(value2) //
				.verifyComplete();
	}

	@Test // DATAREDIS-602
	public void set() {

		assumeFalse(valueFactory instanceof ByteBufferObjectFactory);

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		StepVerifier.create(listOperations.rightPushAll(key, value1, value2)).expectNext(2L).verifyComplete();

		StepVerifier.create(listOperations.set(key, 1, value1)).expectNext(true).verifyComplete();

		StepVerifier.create(listOperations.range(key, 0, -1)) //
				.expectNext(value1) //
				.expectNext(value1) //
				.verifyComplete();
	}

	@Test // DATAREDIS-602
	public void remove() {

		assumeFalse(valueFactory instanceof ByteBufferObjectFactory);

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		StepVerifier.create(listOperations.rightPushAll(key, value1, value2)).expectNext(2L).verifyComplete();

		StepVerifier.create(listOperations.remove(key, 1, value1)).expectNext(1L).verifyComplete();

		StepVerifier.create(listOperations.range(key, 0, -1)) //
				.expectNext(value2) //
				.verifyComplete();
	}

	@Test // DATAREDIS-602
	public void index() {

		assumeFalse(valueFactory instanceof ByteBufferObjectFactory);

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		StepVerifier.create(listOperations.rightPushAll(key, value1, value2)).expectNext(2L).verifyComplete();

		StepVerifier.create(listOperations.index(key, 1)).expectNext(value2).verifyComplete();
	}

	@Test // DATAREDIS-602
	public void leftPop() {

		assumeFalse(valueFactory instanceof ByteBufferObjectFactory);

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		StepVerifier.create(listOperations.leftPushAll(key, value1, value2)).expectNext(2L).verifyComplete();

		StepVerifier.create(listOperations.leftPop(key)).expectNext(value2).verifyComplete();
	}

	@Test // DATAREDIS-602
	public void rightPop() {

		assumeFalse(valueFactory instanceof ByteBufferObjectFactory);

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		StepVerifier.create(listOperations.rightPushAll(key, value1, value2)).expectNext(2L).verifyComplete();

		StepVerifier.create(listOperations.rightPop(key)).expectNext(value2).verifyComplete();
	}

	@Test // DATAREDIS-602
	public void leftPopWithTimeout() {

		assumeFalse(valueFactory instanceof ByteBufferObjectFactory);

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		StepVerifier.create(listOperations.leftPushAll(key, value1, value2)).expectNext(2L).verifyComplete();

		StepVerifier.create(listOperations.leftPop(key, Duration.ZERO)).expectNext(value2).verifyComplete();
	}

	@Test(expected = IllegalArgumentException.class) // DATAREDIS-602
	public void leftPopWithMillisecondTimeoutShouldFail() {

		K key = keyFactory.instance();

		listOperations.leftPop(key, Duration.ofMillis(1001));
	}

	@Test // DATAREDIS-602
	public void rightPopWithTimeout() {

		assumeFalse(valueFactory instanceof ByteBufferObjectFactory);

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		StepVerifier.create(listOperations.rightPushAll(key, value1, value2)).expectNext(2L).verifyComplete();

		StepVerifier.create(listOperations.rightPop(key, Duration.ZERO)).expectNext(value2).verifyComplete();
	}

	@Test // DATAREDIS-602
	public void rightPopAndLeftPush() {

		assumeFalse(valueFactory instanceof ByteBufferObjectFactory);

		K source = keyFactory.instance();
		K target = keyFactory.instance();
		V value = valueFactory.instance();

		StepVerifier.create(listOperations.rightPush(source, value)).expectNext(1L).verifyComplete();

		StepVerifier.create(listOperations.rightPopAndLeftPush(source, target)).expectNext(value).verifyComplete();

		StepVerifier.create(listOperations.size(source)).expectNext(0L).verifyComplete();
		StepVerifier.create(listOperations.size(target)).expectNext(1L).verifyComplete();
	}

	@Test // DATAREDIS-602
	public void rightPopAndLeftPushWithTimeout() {

		assumeFalse(valueFactory instanceof ByteBufferObjectFactory);

		K source = keyFactory.instance();
		K target = keyFactory.instance();
		V value = valueFactory.instance();

		StepVerifier.create(listOperations.rightPopAndLeftPush(source, target, Duration.ofSeconds(1))).expectComplete()
				.verify();

		StepVerifier.create(listOperations.rightPush(source, value)).expectNext(1L).verifyComplete();

		StepVerifier.create(listOperations.rightPopAndLeftPush(source, target, Duration.ZERO)).expectNext(value)
				.verifyComplete();

		StepVerifier.create(listOperations.size(source)).expectNext(0L).verifyComplete();
		StepVerifier.create(listOperations.size(target)).expectNext(1L).verifyComplete();
	}

	@Test // DATAREDIS-602
	public void delete() {

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();

		StepVerifier.create(listOperations.rightPush(key, value1)).expectNext(1L).verifyComplete();
		StepVerifier.create(listOperations.delete(key)).expectNext(true).verifyComplete();

		StepVerifier.create(listOperations.size(key)).expectNext(0L).verifyComplete();
	}
}
