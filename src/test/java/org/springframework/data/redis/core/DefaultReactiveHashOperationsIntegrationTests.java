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
import static org.junit.jupiter.api.condition.OS.*;

import org.springframework.data.redis.connection.RedisHashCommands;
import org.springframework.data.redis.core.types.Expiration;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.MethodSource;

import org.springframework.data.redis.ObjectFactory;
import org.springframework.data.redis.RawObjectFactory;
import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.StringObjectFactory;
import org.springframework.data.redis.connection.ExpirationOptions;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.convert.Converters;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.data.redis.test.condition.EnabledOnCommand;
import org.springframework.data.redis.test.extension.LettuceTestClientResources;

/**
 * Integration tests for {@link DefaultReactiveHashOperations}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 */
@ParameterizedClass
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

	@Test
	// DATAREDIS-602
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

	@Test // DATAREDIS-602
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

	@Test // DATAREDIS-602
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

	@Test // DATAREDIS-824
	void getAbsentKey() {

		hashOperations.get(keyFactory.instance(), hashKeyFactory.instance()).as(StepVerifier::create) //
				.verifyComplete();
	}

	@Test // DATAREDIS-602
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

	@Test // DATAREDIS-824
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

	@Test // DATAREDIS-602
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

	@Test // GH-2048
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

	@Test // GH-2048
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

	@Test // DATAREDIS-602
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

	@Test // DATAREDIS-602
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

	@Test // DATAREDIS-602
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

	@Test // DATAREDIS-602
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

	@Test // DATAREDIS-602
	void put() {

		K key = keyFactory.instance();
		HK hashkey = hashKeyFactory.instance();
		HV hashvalue = hashValueFactory.instance();

		hashOperations.put(key, hashkey, hashvalue) //
				.as(StepVerifier::create) //
				.expectNext(true) //
				.verifyComplete();
	}

	@Test // DATAREDIS-602
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

	@Test // DATAREDIS-602
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

	@Test // DATAREDIS-602
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

	@Test // DATAREDIS-743
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

	@EnabledOnCommand("HEXPIRE") // GH-3054
	@Test
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

		hashOperations.getTimeToLive(key, List.of(key1)) //
				.as(StepVerifier::create) //
				.assertNext(it -> {
					assertThat(it.expirationOf(key1).raw()).isBetween(0L, 2L);
				}).verifyComplete();
	}

	@Test // GH-3054
	@EnabledOnCommand("HEXPIRE")
	void testExpireWithOptions() {

		K key = keyFactory.instance();
		HK key1 = hashKeyFactory.instance();
		HV val1 = hashValueFactory.instance();
		HK key2 = hashKeyFactory.instance();
		HV val2 = hashValueFactory.instance();

		putAll(key, key1, val1, key2, val2);

		hashOperations
				.expire(key, org.springframework.data.redis.core.types.Expiration.seconds(20), ExpirationOptions.none(),
						List.of(key1))
				.as(StepVerifier::create)//
				.assertNext(changes -> {
					assertThat(changes.allOk()).isTrue();
				}).verifyComplete();
		hashOperations
				.expire(key, org.springframework.data.redis.core.types.Expiration.seconds(60), ExpirationOptions.none(),
						List.of(key2))
				.as(StepVerifier::create)//
				.assertNext(changes -> {
					assertThat(changes.allOk()).isTrue();
				}).verifyComplete();

		hashOperations
				.expire(key, org.springframework.data.redis.core.types.Expiration.seconds(30),
						ExpirationOptions.builder().gt().build(), List.of(key1, key2))
				.as(StepVerifier::create)//
				.assertNext(changes -> {
					assertThat(changes.ok()).containsExactly(key1);
					assertThat(changes.skipped()).containsExactly(key2);
				}).verifyComplete();
	}

	@Test // GH-3054
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

		hashOperations.getTimeToLive(key, TimeUnit.SECONDS, List.of(key1, key2)) //
				.as(StepVerifier::create) //
				.assertNext(it -> {
					assertThat(it.expirationOf(key1).raw()).isBetween(0L, 5L);
					assertThat(it.expirationOf(key2).raw()).isBetween(0L, 5L);
				}).verifyComplete();
	}

	@Test // GH-3054
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

		redisTemplate.opsForHash().getTimeToLive(key, List.of(key1, key2)).as(StepVerifier::create)//
				.assertNext(it -> {
					assertThat(it.expirationOf(key1).raw()).isBetween(0L, 2L);
					assertThat(it.expirationOf(key2).raw()).isBetween(0L, 2L);
				}).verifyComplete();
	}

	@Test // GH-3054
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

		redisTemplate.opsForHash().getTimeToLive(key, List.of(key1, key2)).as(StepVerifier::create)//
				.assertNext(expirations -> {
					assertThat(expirations.persistent()).contains(key1, key2);
				}).verifyComplete();
	}

	@Test // DATAREDIS-602
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

	@Test // GH-3211
	@EnabledOnCommand("HGETDEL")
	void getAndDeleteSingleKey() {

		assumeThat(hashKeyFactory instanceof StringObjectFactory && hashValueFactory instanceof StringObjectFactory)
				.isTrue();

		K key = keyFactory.instance();
		HK hashkey1 = hashKeyFactory.instance();
		HV hashvalue1 = hashValueFactory.instance();
		HK hashkey2 = hashKeyFactory.instance();
		HV hashvalue2 = hashValueFactory.instance();

		putAll(key, hashkey1, hashvalue1, hashkey2, hashvalue2);

		hashOperations.getAndDelete(key, Arrays.asList(hashkey1)).as(StepVerifier::create)
				.consumeNextWith(actual -> {
					assertThat(actual).hasSize(1).containsExactly(hashvalue1);
				})
				.verifyComplete();

		hashOperations.hasKey(key, hashkey1).as(StepVerifier::create)
				.expectNext(false)
				.verifyComplete();

		hashOperations.hasKey(key, hashkey2).as(StepVerifier::create)
				.expectNext(true)
				.verifyComplete();
	}

	@Test // GH-3211
	@EnabledOnCommand("HGETDEL")
	void getAndDeletePartialKeys() {

		assumeThat(hashKeyFactory instanceof StringObjectFactory && hashValueFactory instanceof StringObjectFactory)
				.isTrue();

		K key = keyFactory.instance();
		HK hashkey1 = hashKeyFactory.instance();
		HV hashvalue1 = hashValueFactory.instance();
		HK hashkey2 = hashKeyFactory.instance();
		HV hashvalue2 = hashValueFactory.instance();

		putAll(key, hashkey1, hashvalue1, hashkey2, hashvalue2);

		hashOperations.getAndDelete(key, Arrays.asList(hashkey1)).as(StepVerifier::create)
				.consumeNextWith(actual -> {
					assertThat(actual).hasSize(1).containsExactly(hashvalue1);
				})
				.verifyComplete();

		hashOperations.hasKey(key, hashkey1).as(StepVerifier::create)
				.expectNext(false)
				.verifyComplete();

		hashOperations.hasKey(key, hashkey2).as(StepVerifier::create)
				.expectNext(true)
				.verifyComplete();

		hashOperations.get(key, hashkey2).as(StepVerifier::create)
				.expectNext(hashvalue2)
				.verifyComplete();
	}

	@Test // GH-3211
	@EnabledOnCommand("HGETDEL")
	void getAndDeleteNonExistentKeys() {

		assumeThat(hashKeyFactory instanceof StringObjectFactory && hashValueFactory instanceof StringObjectFactory)
				.isTrue();

		K key = keyFactory.instance();
		HK hashkey1 = hashKeyFactory.instance();
		HV hashvalue1 = hashValueFactory.instance();
		HK hashkey2 = hashKeyFactory.instance();
		HV hashvalue2 = hashValueFactory.instance();
		HK nonExistentKey = hashKeyFactory.instance();

		putAll(key, hashkey1, hashvalue1, hashkey2, hashvalue2);

		hashOperations.getAndDelete(key, Arrays.asList(nonExistentKey)).as(StepVerifier::create)
				.consumeNextWith(actual -> {
					assertThat(actual).hasSize(1).containsExactly((HV) null);
				})
				.verifyComplete();

		hashOperations.hasKey(key, hashkey1).as(StepVerifier::create)
				.expectNext(true)
				.verifyComplete();

		hashOperations.hasKey(key, hashkey2).as(StepVerifier::create)
				.expectNext(true)
				.verifyComplete();
	}

	@Test // GH-3211
	@EnabledOnCommand("HGETDEL")
	void getAndDeleteKeyDeletionBehavior() {

		assumeThat(hashKeyFactory instanceof StringObjectFactory && hashValueFactory instanceof StringObjectFactory)
				.isTrue();

		K key = keyFactory.instance();
		HK hashkey1 = hashKeyFactory.instance();
		HV hashvalue1 = hashValueFactory.instance();
		HK hashkey2 = hashKeyFactory.instance();
		HV hashvalue2 = hashValueFactory.instance();

		putAll(key, hashkey1, hashvalue1, hashkey2, hashvalue2);

		redisTemplate.hasKey(key).as(StepVerifier::create)
				.expectNext(true)
				.verifyComplete();

		hashOperations.getAndDelete(key, Arrays.asList(hashkey1, hashkey2)).as(StepVerifier::create)
				.consumeNextWith(actual -> {
					assertThat(actual).hasSize(2).containsSequence(hashvalue1, hashvalue2);
				})
				.verifyComplete();

		hashOperations.size(key).as(StepVerifier::create)
				.expectNext(0L)
				.verifyComplete();

        redisTemplate.hasKey(key).as(StepVerifier::create)
				.expectNext(false)
				.verifyComplete();
	}

	@Test // GH-3211
	@EnabledOnCommand("HGETDEL")
	void getAndDeleteFromNonExistentHash() {

		assumeThat(hashKeyFactory instanceof StringObjectFactory && hashValueFactory instanceof StringObjectFactory)
				.isTrue();

		K nonExistentKey = keyFactory.instance();
		HK hashkey = hashKeyFactory.instance();

		hashOperations.getAndDelete(nonExistentKey, Arrays.asList(hashkey)).as(StepVerifier::create)
				.consumeNextWith(actual -> {
					assertThat(actual).hasSize(1).containsExactly((HV) null);
				})
				.verifyComplete();
	}

	@Test // GH-3211
	@EnabledOnCommand("HGETEX")
	void getAndExpireSingleKey() {

		assumeThat(hashKeyFactory instanceof StringObjectFactory && hashValueFactory instanceof StringObjectFactory)
				.isTrue();

		K key = keyFactory.instance();
		HK hashkey1 = hashKeyFactory.instance();
		HV hashvalue1 = hashValueFactory.instance();
		HK hashkey2 = hashKeyFactory.instance();
		HV hashvalue2 = hashValueFactory.instance();

		putAll(key, hashkey1, hashvalue1, hashkey2, hashvalue2);

		hashOperations.getAndExpire(key, Expiration.seconds(60), Arrays.asList(hashkey1)).as(StepVerifier::create)
				.consumeNextWith(actual -> {
					assertThat(actual).hasSize(1).containsExactly(hashvalue1);
				})
				.verifyComplete();

		hashOperations.hasKey(key, hashkey1).as(StepVerifier::create)
				.expectNext(true)
				.verifyComplete();

		hashOperations.hasKey(key, hashkey2).as(StepVerifier::create)
				.expectNext(true)
				.verifyComplete();
	}

	@Test // GH-3211
	@EnabledOnCommand("HGETEX")
	void getAndExpireMultipleKeys() {

		assumeThat(hashKeyFactory instanceof StringObjectFactory && hashValueFactory instanceof StringObjectFactory)
				.isTrue();

		K key = keyFactory.instance();
		HK hashkey1 = hashKeyFactory.instance();
		HV hashvalue1 = hashValueFactory.instance();
		HK hashkey2 = hashKeyFactory.instance();
		HV hashvalue2 = hashValueFactory.instance();

		putAll(key, hashkey1, hashvalue1, hashkey2, hashvalue2);

		hashOperations.getAndExpire(key, Expiration.seconds(120), Arrays.asList(hashkey1, hashkey2)).as(StepVerifier::create)
				.consumeNextWith(actual -> {
					assertThat(actual).hasSize(2).containsExactly(hashvalue1, hashvalue2);
				})
				.verifyComplete();

		hashOperations.hasKey(key, hashkey1).as(StepVerifier::create)
				.expectNext(true)
				.verifyComplete();

		hashOperations.hasKey(key, hashkey2).as(StepVerifier::create)
				.expectNext(true)
				.verifyComplete();
	}

	@Test // GH-3211
	@EnabledOnCommand("HGETEX")
	void getAndExpireNonExistentKey() {

		assumeThat(hashKeyFactory instanceof StringObjectFactory && hashValueFactory instanceof StringObjectFactory)
				.isTrue();

		K key = keyFactory.instance();
		HK hashkey1 = hashKeyFactory.instance();

		hashOperations.getAndExpire(key, Expiration.seconds(60), Arrays.asList(hashkey1)).as(StepVerifier::create)
				.consumeNextWith(actual -> {
					assertThat(actual).hasSize(1).containsExactly((HV) null);
				})
				.verifyComplete();
	}

	@Test // GH-3211
	@EnabledOnCommand("HSETEX")
	void putAndExpireUpsert() {

		assumeThat(hashKeyFactory instanceof StringObjectFactory && hashValueFactory instanceof StringObjectFactory)
				.isTrue();

		K key = keyFactory.instance();
		HK hashkey1 = hashKeyFactory.instance();
		HV hashvalue1 = hashValueFactory.instance();
		HK hashkey2 = hashKeyFactory.instance();
		HV hashvalue2 = hashValueFactory.instance();

		Map<HK, HV> fieldMap = Map.of(hashkey1, hashvalue1, hashkey2, hashvalue2);

		hashOperations.putAndExpire(key, fieldMap, RedisHashCommands.HashFieldSetOption.upsert(), Expiration.seconds(60))
				.as(StepVerifier::create)
				.expectNext(true)
				.verifyComplete();

		// Verify fields were set
		hashOperations.hasKey(key, hashkey1).as(StepVerifier::create)
				.expectNext(true)
				.verifyComplete();

		hashOperations.hasKey(key, hashkey2).as(StepVerifier::create)
				.expectNext(true)
				.verifyComplete();

		hashOperations.get(key, hashkey1).as(StepVerifier::create)
				.expectNext(hashvalue1)
				.verifyComplete();

		hashOperations.get(key, hashkey2).as(StepVerifier::create)
				.expectNext(hashvalue2)
				.verifyComplete();
	}

	@Test // GH-3211
	@EnabledOnCommand("HSETEX")
	void putAndExpireIfNoneExist() {

		assumeThat(hashKeyFactory instanceof StringObjectFactory && hashValueFactory instanceof StringObjectFactory)
				.isTrue();

		K key = keyFactory.instance();
		HK hashkey1 = hashKeyFactory.instance();
		HV hashvalue1 = hashValueFactory.instance();
		HK hashkey2 = hashKeyFactory.instance();
		HV hashvalue2 = hashValueFactory.instance();
		HK hashkey3 = hashKeyFactory.instance();
		HV hashvalue3 = hashValueFactory.instance();

		// Set up existing field
		hashOperations.put(key, hashkey1, hashvalue1).as(StepVerifier::create).expectNext(true).verifyComplete();

		// Try to set fields where one already exists - should fail
		Map<HK, HV> fieldMap = Map.of(hashkey1, hashvalue2, hashkey2, hashvalue2);

		hashOperations.putAndExpire(key, fieldMap, RedisHashCommands.HashFieldSetOption.ifNoneExist(), Expiration.seconds(60))
				.as(StepVerifier::create)
				.expectNext(false)
				.verifyComplete();

		// Verify original value unchanged and new field not set
		hashOperations.get(key, hashkey1).as(StepVerifier::create)
				.expectNext(hashvalue1)
				.verifyComplete();

		hashOperations.hasKey(key, hashkey2).as(StepVerifier::create)
				.expectNext(false)
				.verifyComplete();

		// Try with all new fields - should succeed
		Map<HK, HV> newFieldMap = Map.of(hashkey2, hashvalue2, hashkey3, hashvalue3);

		hashOperations.putAndExpire(key, newFieldMap, RedisHashCommands.HashFieldSetOption.ifNoneExist(), Expiration.seconds(120))
				.as(StepVerifier::create)
				.expectNext(true)
				.verifyComplete();

		// Verify new fields were set
		hashOperations.hasKey(key, hashkey2).as(StepVerifier::create)
				.expectNext(true)
				.verifyComplete();

		hashOperations.hasKey(key, hashkey3).as(StepVerifier::create)
				.expectNext(true)
				.verifyComplete();
	}

	@Test // GH-3211
	@EnabledOnCommand("HSETEX")
	void putAndExpireIfAllExist() {

		assumeThat(hashKeyFactory instanceof StringObjectFactory && hashValueFactory instanceof StringObjectFactory)
				.isTrue();

		K key = keyFactory.instance();
		HK hashkey1 = hashKeyFactory.instance();
		HV hashvalue1 = hashValueFactory.instance();
		HK hashkey2 = hashKeyFactory.instance();
		HV hashvalue2 = hashValueFactory.instance();
		HK hashkey3 = hashKeyFactory.instance();
		HV hashvalue3 = hashValueFactory.instance();

		// Set up existing fields
		putAll(key, hashkey1, hashvalue1, hashkey2, hashvalue2);

		// Try to update existing fields - should succeed
		Map<HK, HV> fieldMap = Map.of(hashkey1, hashvalue3, hashkey2, hashvalue3);

		hashOperations.putAndExpire(key, fieldMap, RedisHashCommands.HashFieldSetOption.ifAllExist(), Expiration.seconds(60))
				.as(StepVerifier::create)
				.expectNext(true)
				.verifyComplete();

		// Verify values were updated
		hashOperations.get(key, hashkey1).as(StepVerifier::create)
				.expectNext(hashvalue3)
				.verifyComplete();

		hashOperations.get(key, hashkey2).as(StepVerifier::create)
				.expectNext(hashvalue3)
				.verifyComplete();

		// Try with non-existent field - should fail
		Map<HK, HV> mixedFieldMap = Map.of(hashkey1, hashvalue1, hashkey3, hashvalue1);

		hashOperations.putAndExpire(key, mixedFieldMap, RedisHashCommands.HashFieldSetOption.ifAllExist(), Expiration.seconds(120))
				.as(StepVerifier::create)
				.expectNext(false)
				.verifyComplete();

		// Verify values unchanged
		hashOperations.get(key, hashkey1).as(StepVerifier::create)
				.expectNext(hashvalue3)
				.verifyComplete();

		hashOperations.hasKey(key, hashkey3).as(StepVerifier::create)
				.expectNext(false)
				.verifyComplete();
	}

	@Test // GH-3211
	@EnabledOnCommand("HSETEX")
	void putAndExpireWithDifferentExpirationPolicies() {

		K key = keyFactory.instance();
		HK hashkey1 = hashKeyFactory.instance();
		HV hashvalue1 = hashValueFactory.instance();
		HK hashkey2 = hashKeyFactory.instance();
		HV hashvalue2 = hashValueFactory.instance();
		HK hashkey3 = hashKeyFactory.instance();
		HV hashvalue3 = hashValueFactory.instance();
		HK hashkey4 = hashKeyFactory.instance();
		HV hashvalue4 = hashValueFactory.instance();
		HK hashkey5 = hashKeyFactory.instance();
		HV hashvalue5 = hashValueFactory.instance();

		// Test with seconds expiration
		Map<HK, HV> fieldMap1 = Map.of(hashkey1, hashvalue1);
		hashOperations.putAndExpire(key, fieldMap1, RedisHashCommands.HashFieldSetOption.upsert(), Expiration.seconds(60))
				.as(StepVerifier::create)
				.expectNext(true)
				.verifyComplete();

		hashOperations.hasKey(key, hashkey1).as(StepVerifier::create)
				.expectNext(true)
				.verifyComplete();

		// Test with milliseconds expiration
		Map<HK, HV> fieldMap2 = Map.of(hashkey2, hashvalue2);
		hashOperations.putAndExpire(key, fieldMap2, RedisHashCommands.HashFieldSetOption.upsert(), Expiration.milliseconds(120000))
				.as(StepVerifier::create)
				.expectNext(true)
				.verifyComplete();

		hashOperations.hasKey(key, hashkey2).as(StepVerifier::create)
				.expectNext(true)
				.verifyComplete();

		// Test with Duration expiration
		Map<HK, HV> fieldMap3 = Map.of(hashkey3, hashvalue3);
		hashOperations.putAndExpire(key, fieldMap3, RedisHashCommands.HashFieldSetOption.upsert(), Expiration.from(Duration.ofMinutes(3)))
				.as(StepVerifier::create)
				.expectNext(true)
				.verifyComplete();

		hashOperations.hasKey(key, hashkey3).as(StepVerifier::create)
				.expectNext(true)
				.verifyComplete();

		// Test with unix timestamp expiration (5 minutes from now)
		long futureTimestamp = System.currentTimeMillis() / 1000 + 300; // 5 minutes from now
		Map<HK, HV> fieldMap4 = Map.of(hashkey4, hashvalue4);
		hashOperations.putAndExpire(key, fieldMap4, RedisHashCommands.HashFieldSetOption.upsert(), Expiration.unixTimestamp(futureTimestamp, TimeUnit.SECONDS))
				.as(StepVerifier::create)
				.expectNext(true)
				.verifyComplete();

		hashOperations.hasKey(key, hashkey4).as(StepVerifier::create)
				.expectNext(true)
				.verifyComplete();

		// Test with keepTtl expiration
		// First set a field with TTL, then update it with keepTtl
		hashOperations.put(key, hashkey5, hashvalue5).as(StepVerifier::create).expectNext(true).verifyComplete();
		hashOperations.expire(key, Duration.ofMinutes(4), Arrays.asList(hashkey5)).as(StepVerifier::create).expectNextCount(1).verifyComplete();

		Map<HK, HV> fieldMap5 = Map.of(hashkey5, hashvalue5);
		hashOperations.putAndExpire(key, fieldMap5, RedisHashCommands.HashFieldSetOption.upsert(), Expiration.keepTtl())
				.as(StepVerifier::create)
				.expectNext(true)
				.verifyComplete();

		hashOperations.hasKey(key, hashkey5).as(StepVerifier::create)
				.expectNext(true)
				.verifyComplete();
	}
}
