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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.junit.jupiter.api.condition.OS.MAC;

import org.springframework.data.redis.connection.Hash.FieldExpirationOptions;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.springframework.data.redis.ObjectFactory;
import org.springframework.data.redis.RawObjectFactory;
import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.StringObjectFactory;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.convert.Converters;
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
		lettuceConnectionFactory.start();

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

		hashOperations.randomKey(key) //
				.as(StepVerifier::create) //
				.assertNext(actual -> {
					assertThat(actual).isIn(hashkey1, hashkey2);
				}).verifyComplete();

		hashOperations.randomKeys(key, -10) //
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

		hashOperations.randomEntry(key) //
				.as(StepVerifier::create) //
				.assertNext(actual -> {

					if (actual.getKey().equals(hashkey1)) {
						assertThat(actual.getValue()).isEqualTo(hashvalue1);
					} else {
						assertThat(actual.getValue()).isEqualTo(hashvalue2);
					}
				}).verifyComplete();

		hashOperations.randomEntries(key, -10) //
				.collectList().as(StepVerifier::create) //
				.assertNext(actual -> {
					assertThat(actual).hasSize(10);
				}).verifyComplete();
	}

	@ParameterizedRedisTest // DATAREDIS-602
	@DisabledOnOs(value = MAC, architectures = "aarch64")
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

					Entry<HK, HV> entry1 = Converters.entryOf(hashkey1, hashvalue1);
					Entry<HK, HV> entry2 = Converters.entryOf(hashkey2, hashvalue2);

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

					Entry<HK, HV> entry1 = Converters.entryOf(hashkey1, hashvalue1);
					Entry<HK, HV> entry2 = Converters.entryOf(hashkey2, hashvalue2);

					assertThat(list).containsExactlyInAnyOrder(entry1, entry2);
				}) //
				.verifyComplete();
	}

	@EnabledOnCommand("HEXPIRE")
	@ParameterizedRedisTest
	void testExpireAndGetExpireMillis() {

		K key = keyFactory.instance();
		HK key1 = hashKeyFactory.instance();
		HV val1 = hashValueFactory.instance();
		HK key2 = hashKeyFactory.instance();
		HV val2 = hashValueFactory.instance();

		putAll(key, key1, val1, key2, val2);

		hashOperations.expire(key, Duration.ofMillis(1500), List.of(key1)) //
				.as(StepVerifier::create)//
				.assertNext(changes -> {
					assertThat(changes.allOk()).isTrue();
				}).verifyComplete();

		hashOperations.getExpire(key, List.of(key1)) //
				.as(StepVerifier::create) //
				.assertNext(it -> {
					assertThat(it.expirationOf(key1).raw()).isBetween(0L, 2L);
				}).verifyComplete();
	}

	@ParameterizedRedisTest
	@EnabledOnCommand("HEXPIRE")
	void testExpireWithOptions() {

		K key = keyFactory.instance();
		HK key1 = hashKeyFactory.instance();
		HV val1 = hashValueFactory.instance();
		HK key2 = hashKeyFactory.instance();
		HV val2 = hashValueFactory.instance();

		putAll(key, key1, val1, key2, val2);

		hashOperations.expire(key, org.springframework.data.redis.core.types.Expiration.seconds(20), FieldExpirationOptions.none(), List.of(key1)).as(StepVerifier::create)//
			.assertNext(changes -> {
				assertThat(changes.allOk()).isTrue();
			}).verifyComplete();
		hashOperations.expire(key, org.springframework.data.redis.core.types.Expiration.seconds(60), FieldExpirationOptions.none(), List.of(key2)).as(StepVerifier::create)//
			.assertNext(changes -> {
				assertThat(changes.allOk()).isTrue();
			}).verifyComplete();

		hashOperations.expire(key, org.springframework.data.redis.core.types.Expiration.seconds(30), FieldExpirationOptions.builder().gt().build(), List.of(key1, key2)).as(StepVerifier::create)//
			.assertNext(changes -> {
				assertThat(changes.ok()).containsExactly(key1);
				assertThat(changes.skipped()).containsExactly(key2);
			}).verifyComplete();
	}

	@ParameterizedRedisTest
	@EnabledOnCommand("HEXPIRE")
	void testExpireAndGetExpireSeconds() {

		K key = keyFactory.instance();
		HK key1 = hashKeyFactory.instance();
		HV val1 = hashValueFactory.instance();
		HK key2 = hashKeyFactory.instance();
		HV val2 = hashValueFactory.instance();

		putAll(key, key1, val1, key2, val2);

		hashOperations.expire(key, Duration.ofSeconds(5), List.of(key1, key2)) //
				.as(StepVerifier::create)//
				.assertNext(changes -> {
					assertThat(changes.allOk()).isTrue();
				}).verifyComplete();

		hashOperations.getExpire(key, TimeUnit.SECONDS, List.of(key1, key2)) //
				.as(StepVerifier::create) //
				.assertNext(it -> {
					assertThat(it.expirationOf(key1).raw()).isBetween(0L, 5L);
					assertThat(it.expirationOf(key2).raw()).isBetween(0L, 5L);
				}).verifyComplete();

	}

	@ParameterizedRedisTest
	@EnabledOnCommand("HEXPIRE")
	void testExpireAtAndGetExpireMillis() {

		K key = keyFactory.instance();
		HK key1 = hashKeyFactory.instance();
		HV val1 = hashValueFactory.instance();
		HK key2 = hashKeyFactory.instance();
		HV val2 = hashValueFactory.instance();

		putAll(key, key1, val1, key2, val2);

		redisTemplate.opsForHash().expireAt(key, Instant.now().plusMillis(1500), List.of(key1, key2))
				.as(StepVerifier::create)//
				.assertNext(changes -> {
					assertThat(changes.allOk()).isTrue();
				}).verifyComplete();

		redisTemplate.opsForHash().getExpire(key, List.of(key1, key2)).as(StepVerifier::create)//
				.assertNext(it -> {
					assertThat(it.expirationOf(key1).raw()).isBetween(0L, 2L);
					assertThat(it.expirationOf(key2).raw()).isBetween(0L, 2L);
				}).verifyComplete();
	}

	@ParameterizedRedisTest
	@EnabledOnCommand("HEXPIRE")
	void testPersistAndGetExpireMillis() {

		K key = keyFactory.instance();
		HK key1 = hashKeyFactory.instance();
		HV val1 = hashValueFactory.instance();
		HK key2 = hashKeyFactory.instance();
		HV val2 = hashValueFactory.instance();

		putAll(key, key1, val1, key2, val2);

		redisTemplate.opsForHash().expireAt(key, Instant.now().plusMillis(1500), List.of(key1, key2))
				.as(StepVerifier::create)//
				.assertNext(changes -> {
					assertThat(changes.allOk()).isTrue();
				}).verifyComplete();

		redisTemplate.opsForHash().persist(key, List.of(key1, key2)).as(StepVerifier::create)//
				.assertNext(changes -> {
					assertThat(changes.allOk()).isTrue();
				}).verifyComplete();

		redisTemplate.opsForHash().getExpire(key, List.of(key1, key2)).as(StepVerifier::create)//
				.assertNext(expirations -> {
					assertThat(expirations.persistent()).contains(key1, key2);
				}).verifyComplete();

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
