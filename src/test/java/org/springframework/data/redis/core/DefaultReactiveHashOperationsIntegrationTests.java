/*
 * Copyright 2017-2021 the original author or authors.
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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.junit.jupiter.api.BeforeEach;

import org.springframework.data.redis.ObjectFactory;
import org.springframework.data.redis.RawObjectFactory;
import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.StringObjectFactory;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.data.redis.test.condition.EnabledOnCommand;
import org.springframework.data.redis.test.extension.LettuceTestClientResources;
import org.springframework.data.redis.test.extension.parametrized.MethodSource;
import org.springframework.data.redis.test.extension.parametrized.ParameterizedRedisTest;

/**
 * Integration tests for {@link DefaultReactiveHashOperations}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 */
@MethodSource("testParams")
@SuppressWarnings("unchecked")
public class DefaultReactiveHashOperationsIntegrationTests<K, HK, HV> {

	private final ReactiveRedisTemplate<K, ?> redisTemplate;
	private final ReactiveHashOperations<K, HK, HV> hashOperations;

	private final ObjectFactory<K> keyFactory;
	private final ObjectFactory<HK> hashKeyFactory;
	private final ObjectFactory<HV> hashValueFactory;

	public DefaultReactiveHashOperationsIntegrationTests(ReactiveRedisTemplate<K, ?> redisTemplate,
			ObjectFactory<K> keyFactory, ObjectFactory<HK> hashKeyFactory, ObjectFactory<HV> hashValueFactory) {

		this.redisTemplate = redisTemplate;
		this.hashOperations = redisTemplate.opsForHash();
		this.keyFactory = keyFactory;
		this.hashKeyFactory = hashKeyFactory;
		this.hashValueFactory = hashValueFactory;
	}

	public static Collection<Object[]> testParams() {

		ObjectFactory<String> stringFactory = new StringObjectFactory();
		ObjectFactory<byte[]> rawFactory = new RawObjectFactory();

		LettuceConnectionFactory lettuceConnectionFactory = new LettuceConnectionFactory();
		lettuceConnectionFactory.setClientResources(LettuceTestClientResources.getSharedClientResources());
		lettuceConnectionFactory.setPort(SettingsUtils.getPort());
		lettuceConnectionFactory.setHostName(SettingsUtils.getHost());
		lettuceConnectionFactory.afterPropertiesSet();

		RedisSerializationContext<String, String> serializationContext = RedisSerializationContext
				.fromSerializer(StringRedisSerializer.UTF_8);
		ReactiveRedisTemplate<String, String> stringTemplate = new ReactiveRedisTemplate<>(lettuceConnectionFactory,
				serializationContext);

		ReactiveRedisTemplate<byte[], byte[]> rawTemplate = new ReactiveRedisTemplate(lettuceConnectionFactory,
				RedisSerializationContext.raw());

		return Arrays.asList(new Object[][] { { stringTemplate, stringFactory, stringFactory, stringFactory, "String" },
				{ rawTemplate, rawFactory, rawFactory, rawFactory, "raw" } });
	}

	@BeforeEach
	void before() {

		RedisConnectionFactory connectionFactory = (RedisConnectionFactory) redisTemplate.getConnectionFactory();
		RedisConnection connection = connectionFactory.getConnection();
		connection.flushAll();
		connection.close();
	}

	@ParameterizedRedisTest // DATAREDIS-602
	void remove() {

		K key = keyFactory.instance();
		HK hashkey1 = hashKeyFactory.instance();
		HV hashvalue1 = hashValueFactory.instance();

		HK hashkey2 = hashKeyFactory.instance();
		HV hashvalue2 = hashValueFactory.instance();

		putAll(key, hashkey1, hashvalue1, hashkey2, hashvalue2);

		hashOperations.remove(key, hashkey1, hashkey2) //
				.as(StepVerifier::create) //
				.expectNext(2L) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-602
	void hasKey() {

		K key = keyFactory.instance();
		HK hashkey = hashKeyFactory.instance();
		HV hashvalue = hashValueFactory.instance();

		hashOperations.put(key, hashkey, hashvalue) //
				.as(StepVerifier::create) //
				.expectNext(true) //
				.verifyComplete();

		hashOperations.hasKey(key, hashkey) //
				.as(StepVerifier::create) //
				.expectNext(true) //
				.verifyComplete();

		hashOperations.hasKey(key, hashKeyFactory.instance()) //
				.as(StepVerifier::create) //
				.expectNext(false) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-602
	void get() {

		K key = keyFactory.instance();
		HK hashkey = hashKeyFactory.instance();
		HV hashvalue = hashValueFactory.instance();

		hashOperations.put(key, hashkey, hashvalue) //
				.as(StepVerifier::create) //
				.expectNext(true) //
				.verifyComplete();

		hashOperations.get(key, hashkey) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-824
	void getAbsentKey() {

		hashOperations.get(keyFactory.instance(), hashKeyFactory.instance()).as(StepVerifier::create) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-602
	void multiGet() {

		assumeThat(hashKeyFactory instanceof StringObjectFactory && hashValueFactory instanceof StringObjectFactory)
				.isTrue();

		K key = keyFactory.instance();
		HK hashkey1 = hashKeyFactory.instance();
		HV hashvalue1 = hashValueFactory.instance();

		HK hashkey2 = hashKeyFactory.instance();
		HV hashvalue2 = hashValueFactory.instance();

		putAll(key, hashkey1, hashvalue1, hashkey2, hashvalue2);

		hashOperations.multiGet(key, Arrays.asList(hashkey1, hashkey2)).as(StepVerifier::create) //
				.consumeNextWith(actual -> {
					assertThat(actual).hasSize(2).containsSequence(hashvalue1, hashvalue2);
				}) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-824
	void multiGetAbsentKeys() {

		assumeThat(hashKeyFactory instanceof StringObjectFactory && hashValueFactory instanceof StringObjectFactory)
				.isTrue();

		hashOperations.multiGet(keyFactory.instance(), Arrays.asList(hashKeyFactory.instance(), hashKeyFactory.instance()))
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> {
					assertThat(actual).hasSize(2).containsSequence(null, null);
				}) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-602
	void increment() {

		assumeThat(hashValueFactory instanceof StringObjectFactory).isTrue();

		K key = keyFactory.instance();
		HK hashkey = hashKeyFactory.instance();
		HV hashvalue = (HV) "1";

		hashOperations.put(key, hashkey, hashvalue) //
				.as(StepVerifier::create) //
				.expectNext(true) //
				.verifyComplete();

		hashOperations.increment(key, hashkey, 1L) //
				.as(StepVerifier::create) //
				.expectNext(2L) //
				.verifyComplete();

		hashOperations.get(key, hashkey) //
				.as(StepVerifier::create) //
				.expectNext((HV) "2") //
				.verifyComplete();
	}

	@ParameterizedRedisTest // GH-2048
	@EnabledOnCommand("HRANDFIELD")
	void randomField() {

		assumeThat(hashValueFactory).isNotInstanceOf(RawObjectFactory.class);

		K key = keyFactory.instance();
		HK hashkey1 = hashKeyFactory.instance();
		HV hashvalue1 = hashValueFactory.instance();
		HK hashkey2 = hashKeyFactory.instance();
		HV hashvalue2 = hashValueFactory.instance();

		hashOperations.put(key, hashkey1, hashvalue1) //
				.as(StepVerifier::create) //
				.expectNext(true) //
				.verifyComplete();

		hashOperations.put(key, hashkey2, hashvalue2) //
				.as(StepVerifier::create) //
				.expectNext(true) //
				.verifyComplete();

		hashOperations.randomField(key) //
				.as(StepVerifier::create) //
				.assertNext(actual -> {
					assertThat(actual).isIn(hashkey1, hashkey2);
				}).verifyComplete();

		hashOperations.randomFields(key, -10) //
				.collectList().as(StepVerifier::create) //
				.assertNext(actual -> {
					assertThat(actual).hasSize(10);
				}).verifyComplete();
	}

	@ParameterizedRedisTest // GH-2048
	@EnabledOnCommand("HRANDFIELD")
	void randomValue() {

		assumeThat(hashValueFactory).isNotInstanceOf(RawObjectFactory.class);

		K key = keyFactory.instance();
		HK hashkey1 = hashKeyFactory.instance();
		HV hashvalue1 = hashValueFactory.instance();
		HK hashkey2 = hashKeyFactory.instance();
		HV hashvalue2 = hashValueFactory.instance();

		hashOperations.put(key, hashkey1, hashvalue1) //
				.as(StepVerifier::create) //
				.expectNext(true) //
				.verifyComplete();

		hashOperations.put(key, hashkey2, hashvalue2) //
				.as(StepVerifier::create) //
				.expectNext(true) //
				.verifyComplete();

		hashOperations.randomValue(key) //
				.as(StepVerifier::create) //
				.assertNext(actual -> {

					if (actual.getKey().equals(hashkey1)) {
						assertThat(actual.getValue()).isEqualTo(hashvalue1);
					} else {
						assertThat(actual.getValue()).isEqualTo(hashvalue2);
					}
				}).verifyComplete();

		hashOperations.randomValues(key, -10) //
				.collectList().as(StepVerifier::create) //
				.assertNext(actual -> {
					assertThat(actual).hasSize(10);
				}).verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-602
	@SuppressWarnings("unchecked")
	void incrementDouble() {

		assumeThat(hashValueFactory instanceof StringObjectFactory).isTrue();

		K key = keyFactory.instance();
		HK hashkey = hashKeyFactory.instance();
		HV hashvalue = (HV) "1";

		hashOperations.put(key, hashkey, hashvalue) //
				.as(StepVerifier::create) //
				.expectNext(true) //
				.verifyComplete();

		hashOperations.increment(key, hashkey, 1.1d) //
				.as(StepVerifier::create) //
				.expectNext(2.1d) //
				.verifyComplete();

		hashOperations.get(key, hashkey) //
				.as(StepVerifier::create) //
				.expectNext((HV) "2.1") //
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-602
	void keys() {

		assumeThat(hashKeyFactory instanceof StringObjectFactory).isTrue();

		K key = keyFactory.instance();
		HK hashkey1 = hashKeyFactory.instance();
		HV hashvalue1 = hashValueFactory.instance();

		HK hashkey2 = hashKeyFactory.instance();
		HV hashvalue2 = hashValueFactory.instance();

		putAll(key, hashkey1, hashvalue1, hashkey2, hashvalue2);

		hashOperations.keys(key).buffer(2) //
				.as(StepVerifier::create) //
				.consumeNextWith(list -> assertThat(list).containsExactlyInAnyOrder(hashkey1, hashkey2)) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-602
	void size() {

		K key = keyFactory.instance();
		HK hashkey1 = hashKeyFactory.instance();
		HV hashvalue1 = hashValueFactory.instance();

		HK hashkey2 = hashKeyFactory.instance();
		HV hashvalue2 = hashValueFactory.instance();

		putAll(key, hashkey1, hashvalue1, hashkey2, hashvalue2);

		hashOperations.size(key) //
				.as(StepVerifier::create) //
				.expectNext(2L) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-602
	void putAll() {

		K key = keyFactory.instance();
		HK hashkey1 = hashKeyFactory.instance();
		HV hashvalue1 = hashValueFactory.instance();

		HK hashkey2 = hashKeyFactory.instance();
		HV hashvalue2 = hashValueFactory.instance();

		putAll(key, hashkey1, hashvalue1, hashkey2, hashvalue2);

		hashOperations.hasKey(key, hashkey1) //
				.as(StepVerifier::create) //
				.expectNext(true) //
				.verifyComplete();

		hashOperations.hasKey(key, hashkey2) //
				.as(StepVerifier::create) //
				.expectNext(true) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-602
	void put() {

		K key = keyFactory.instance();
		HK hashkey = hashKeyFactory.instance();
		HV hashvalue = hashValueFactory.instance();

		hashOperations.put(key, hashkey, hashvalue) //
				.as(StepVerifier::create) //
				.expectNext(true) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-602
	void putIfAbsent() {

		K key = keyFactory.instance();
		HK hashkey = hashKeyFactory.instance();
		HV hashvalue = hashValueFactory.instance();
		HV hashvalue2 = hashValueFactory.instance();

		hashOperations.putIfAbsent(key, hashkey, hashvalue) //
				.as(StepVerifier::create) //
				.expectNext(true) //
				.verifyComplete();

		hashOperations.putIfAbsent(key, hashkey, hashvalue2) //
				.as(StepVerifier::create) //
				.expectNext(false) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-602
	void values() {

		assumeThat(hashValueFactory instanceof StringObjectFactory).isTrue();

		K key = keyFactory.instance();
		HK hashkey1 = hashKeyFactory.instance();
		HV hashvalue1 = hashValueFactory.instance();

		HK hashkey2 = hashKeyFactory.instance();
		HV hashvalue2 = hashValueFactory.instance();

		putAll(key, hashkey1, hashvalue1, hashkey2, hashvalue2);

		hashOperations.values(key) //
				.as(StepVerifier::create) //
				.expectNextCount(2) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-602
	void entries() {

		assumeThat(hashKeyFactory instanceof StringObjectFactory && hashValueFactory instanceof StringObjectFactory)
				.isTrue();

		K key = keyFactory.instance();
		HK hashkey1 = hashKeyFactory.instance();
		HV hashvalue1 = hashValueFactory.instance();

		HK hashkey2 = hashKeyFactory.instance();
		HV hashvalue2 = hashValueFactory.instance();

		putAll(key, hashkey1, hashvalue1, hashkey2, hashvalue2);

		hashOperations.entries(key).buffer(2) //
				.as(StepVerifier::create) //
				.consumeNextWith(list -> {

					Entry<HK, HV> entry1 = Collections.singletonMap(hashkey1, hashvalue1).entrySet().iterator().next();
					Entry<HK, HV> entry2 = Collections.singletonMap(hashkey2, hashvalue2).entrySet().iterator().next();

					assertThat(list).containsExactlyInAnyOrder(entry1, entry2);
				}) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-743
	void scan() {

		assumeThat(hashKeyFactory instanceof StringObjectFactory && hashValueFactory instanceof StringObjectFactory)
				.isTrue();

		K key = keyFactory.instance();
		HK hashkey1 = hashKeyFactory.instance();
		HV hashvalue1 = hashValueFactory.instance();

		HK hashkey2 = hashKeyFactory.instance();
		HV hashvalue2 = hashValueFactory.instance();

		putAll(key, hashkey1, hashvalue1, hashkey2, hashvalue2);

		hashOperations.scan(key).buffer(2) //
				.as(StepVerifier::create) //
				.consumeNextWith(list -> {

					Entry<HK, HV> entry1 = Collections.singletonMap(hashkey1, hashvalue1).entrySet().iterator().next();
					Entry<HK, HV> entry2 = Collections.singletonMap(hashkey2, hashvalue2).entrySet().iterator().next();

					assertThat(list).containsExactlyInAnyOrder(entry1, entry2);
				}) //
				.verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-602
	void delete() {

		K key = keyFactory.instance();
		HK hashkey = hashKeyFactory.instance();
		HV hashvalue = hashValueFactory.instance();

		hashOperations.put(key, hashkey, hashvalue) //
				.as(StepVerifier::create) //
				.expectNext(true) //
				.verifyComplete();

		hashOperations.delete(key) //
				.as(StepVerifier::create) //
				.expectNext(true) //
				.verifyComplete();

		hashOperations.size(key) //
				.as(StepVerifier::create) //
				.expectNext(0L) //
				.verifyComplete();
	}

	private void putAll(K key, HK hashkey1, HV hashvalue1, HK hashkey2, HV hashvalue2) {

		Map<HK, HV> map = new HashMap<>();
		map.put(hashkey1, hashvalue1);
		map.put(hashkey2, hashvalue2);

		hashOperations.putAll(key, map) //
				.as(StepVerifier::create) //
				.expectNext(true) //
				.verifyComplete();
	}
}
