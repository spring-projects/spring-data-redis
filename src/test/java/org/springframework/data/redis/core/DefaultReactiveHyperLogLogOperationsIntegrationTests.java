/*
 * Copyright 2017-2023 the original author or authors.
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

import reactor.test.StepVerifier;

import java.util.Collection;

import org.junit.jupiter.api.BeforeEach;

import org.springframework.data.redis.ObjectFactory;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.ReactiveOperationsTestParams.Fixture;
import org.springframework.data.redis.test.extension.parametrized.MethodSource;
import org.springframework.data.redis.test.extension.parametrized.ParameterizedRedisTest;

/**
 * Integration tests for {@link DefaultReactiveHyperLogLogOperations}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 */
@MethodSource("testParams")
@SuppressWarnings("unchecked")
public class DefaultReactiveHyperLogLogOperationsIntegrationTests<K, V> {

	private final ReactiveRedisTemplate<K, V> redisTemplate;
	private final ReactiveHyperLogLogOperations<K, V> hyperLogLogOperations;

	private final ObjectFactory<K> keyFactory;
	private final ObjectFactory<V> valueFactory;

	public static Collection<Fixture<?, ?>> testParams() {
		return ReactiveOperationsTestParams.testParams();
	}

	public DefaultReactiveHyperLogLogOperationsIntegrationTests(Fixture<K, V> fixture) {

		this.redisTemplate = fixture.getTemplate();
		this.hyperLogLogOperations = redisTemplate.opsForHyperLogLog();
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

	@ParameterizedRedisTest // DATAREDIS-602
	void add() {

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		hyperLogLogOperations.add(key, value1, value2).as(StepVerifier::create).expectNext(1L).verifyComplete();

		hyperLogLogOperations.size(key).as(StepVerifier::create).expectNext(2L).verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-602
	void union() {

		K mergedKey = keyFactory.instance();
		V sharedValue = valueFactory.instance();

		K key1 = keyFactory.instance();
		V value1 = valueFactory.instance();

		K key2 = keyFactory.instance();
		V value2 = valueFactory.instance();

		hyperLogLogOperations.add(key1, value1, sharedValue).as(StepVerifier::create).expectNext(1L).verifyComplete();
		hyperLogLogOperations.add(key2, value2, sharedValue).as(StepVerifier::create).expectNext(1L).verifyComplete();

		hyperLogLogOperations.union(mergedKey, key1, key2).as(StepVerifier::create).expectNext(true).verifyComplete();
		hyperLogLogOperations.size(mergedKey).as(StepVerifier::create).assertNext(actual -> {

			assertThat(actual).isBetween(2L, 3L);
		}).verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-602
	void delete() {

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		hyperLogLogOperations.add(key, value1, value2).as(StepVerifier::create).expectNext(1L).verifyComplete();
		hyperLogLogOperations.delete(key).as(StepVerifier::create).expectNext(true).verifyComplete();

		hyperLogLogOperations.size(key).as(StepVerifier::create).expectNext(0L).verifyComplete();
	}
}
