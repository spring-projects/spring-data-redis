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

import reactor.test.StepVerifier;

import java.util.Collection;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.springframework.data.redis.ConnectionFactoryTracker;
import org.springframework.data.redis.ObjectFactory;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.serializer.RedisSerializer;

/**
 * Integration tests for {@link DefaultReactiveHyperLogLogOperations}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 */
@RunWith(Parameterized.class)
@SuppressWarnings("unchecked")
public class DefaultReactiveHyperLogLogOperationsIntegrationTests<K, V> {

	private final ReactiveRedisTemplate<K, V> redisTemplate;
	private final ReactiveHyperLogLogOperations<K, V> hyperLogLogOperations;

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
	public DefaultReactiveHyperLogLogOperationsIntegrationTests(ReactiveRedisTemplate<K, V> redisTemplate,
			ObjectFactory<K> keyFactory, ObjectFactory<V> valueFactory, RedisSerializer serializer, String label) {

		this.redisTemplate = redisTemplate;
		this.hyperLogLogOperations = redisTemplate.opsForHyperLogLog();
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

		hyperLogLogOperations.add(key, value1, value2).as(StepVerifier::create).expectNext(1L).verifyComplete();

		hyperLogLogOperations.size(key).as(StepVerifier::create).expectNext(2L).verifyComplete();
	}

	@Test // DATAREDIS-602
	public void union() {

		K mergedKey = keyFactory.instance();
		V sharedValue = valueFactory.instance();

		K key1 = keyFactory.instance();
		V value1 = valueFactory.instance();

		K key2 = keyFactory.instance();
		V value2 = valueFactory.instance();

		hyperLogLogOperations.add(key1, value1, sharedValue).as(StepVerifier::create).expectNext(1L).verifyComplete();
		hyperLogLogOperations.add(key2, value2, sharedValue).as(StepVerifier::create).expectNext(1L).verifyComplete();

		hyperLogLogOperations.union(mergedKey, key1, key2).as(StepVerifier::create).expectNext(true).verifyComplete();
		hyperLogLogOperations.size(mergedKey).as(StepVerifier::create).expectNext(3L).verifyComplete();
	}

	@Test // DATAREDIS-602
	public void delete() {

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		hyperLogLogOperations.add(key, value1, value2).as(StepVerifier::create).expectNext(1L).verifyComplete();
		hyperLogLogOperations.delete(key).as(StepVerifier::create).expectNext(true).verifyComplete();

		hyperLogLogOperations.size(key).as(StepVerifier::create).expectNext(0L).verifyComplete();
	}
}
