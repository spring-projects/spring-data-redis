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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.springframework.data.redis.ConnectionFactoryTracker;
import org.springframework.data.redis.ObjectFactory;
import org.springframework.data.redis.RawObjectFactory;
import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.StringObjectFactory;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceTestClientResources;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.data.redis.serializer.StringRedisSerializer;

/**
 * Integration tests for {@link DefaultReactiveHashOperations}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 */
@RunWith(Parameterized.class)
@SuppressWarnings("unchecked")
public class DefaultReactiveHashOperationsIntegrationTests<K, HK, HV> {

	private final ReactiveRedisTemplate<K, ?> redisTemplate;
	private final ReactiveHashOperations<K, HK, HV> hashOperations;

	private final ObjectFactory<K> keyFactory;
	private final ObjectFactory<HK> hashKeyFactory;
	private final ObjectFactory<HV> hashValueFactory;

	@Parameters(name = "{4}")
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

	@AfterClass
	public static void cleanUp() {
		ConnectionFactoryTracker.cleanUp();
	}

	public DefaultReactiveHashOperationsIntegrationTests(ReactiveRedisTemplate<K, ?> redisTemplate,
			ObjectFactory<K> keyFactory, ObjectFactory<HK> hashKeyFactory, ObjectFactory<HV> hashValueFactory,
			String testName) {

		this.redisTemplate = redisTemplate;
		this.hashOperations = redisTemplate.opsForHash();
		this.keyFactory = keyFactory;
		this.hashKeyFactory = hashKeyFactory;
		this.hashValueFactory = hashValueFactory;

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
	public void remove() {

		K key = keyFactory.instance();
		HK hashkey1 = hashKeyFactory.instance();
		HV hashvalue1 = hashValueFactory.instance();

		HK hashkey2 = hashKeyFactory.instance();
		HV hashvalue2 = hashValueFactory.instance();

		putAll(key, hashkey1, hashvalue1, hashkey2, hashvalue2);

		StepVerifier.create(hashOperations.remove(key, hashkey1, hashkey2)) //
				.expectNext(2L) //
				.verifyComplete();
	}

	@Test // DATAREDIS-602
	public void hasKey() {

		K key = keyFactory.instance();
		HK hashkey = hashKeyFactory.instance();
		HV hashvalue = hashValueFactory.instance();

		StepVerifier.create(hashOperations.put(key, hashkey, hashvalue)) //
				.expectNext(true) //
				.verifyComplete();

		StepVerifier.create(hashOperations.hasKey(key, hashkey)) //
				.expectNext(true) //
				.verifyComplete();

		StepVerifier.create(hashOperations.hasKey(key, hashKeyFactory.instance())) //
				.expectNext(false) //
				.verifyComplete();
	}

	@Test // DATAREDIS-602
	public void get() {

		K key = keyFactory.instance();
		HK hashkey = hashKeyFactory.instance();
		HV hashvalue = hashValueFactory.instance();

		StepVerifier.create(hashOperations.put(key, hashkey, hashvalue)) //
				.expectNext(true) //
				.verifyComplete();

		StepVerifier.create(hashOperations.get(key, hashkey)) //
				.expectNextCount(1) //
				.verifyComplete();
	}

	@Test // DATAREDIS-824
	public void getAbsentKey() {

		hashOperations.get(keyFactory.instance(), hashKeyFactory.instance()).as(StepVerifier::create) //
				.verifyComplete();
	}

	@Test // DATAREDIS-602
	public void multiGet() {

		assumeTrue(hashKeyFactory instanceof StringObjectFactory && hashValueFactory instanceof StringObjectFactory);

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
	public void multiGetAbsentKeys() {

		assumeTrue(hashKeyFactory instanceof StringObjectFactory && hashValueFactory instanceof StringObjectFactory);

		hashOperations.multiGet(keyFactory.instance(), Arrays.asList(hashKeyFactory.instance(), hashKeyFactory.instance()))
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> {
					assertThat(actual).hasSize(2).containsSequence(null, null);
				}) //
				.verifyComplete();
	}

	@Test // DATAREDIS-602
	public void increment() {

		assumeTrue(hashValueFactory instanceof StringObjectFactory);

		K key = keyFactory.instance();
		HK hashkey = hashKeyFactory.instance();
		HV hashvalue = (HV) "1";

		StepVerifier.create(hashOperations.put(key, hashkey, hashvalue)) //
				.expectNext(true) //
				.verifyComplete();

		StepVerifier.create(hashOperations.increment(key, hashkey, 1L)) //
				.expectNext(2L) //
				.verifyComplete();

		StepVerifier.create(hashOperations.get(key, hashkey)) //
				.expectNext((HV) "2") //
				.verifyComplete();
	}

	@Test // DATAREDIS-602
	@SuppressWarnings("unchecked")
	public void incrementDouble() {

		assumeTrue(hashValueFactory instanceof StringObjectFactory);

		K key = keyFactory.instance();
		HK hashkey = hashKeyFactory.instance();
		HV hashvalue = (HV) "1";

		StepVerifier.create(hashOperations.put(key, hashkey, hashvalue)) //
				.expectNext(true) //
				.verifyComplete();

		StepVerifier.create(hashOperations.increment(key, hashkey, 1.1d)) //
				.expectNext(2.1d) //
				.verifyComplete();

		StepVerifier.create(hashOperations.get(key, hashkey)) //
				.expectNext((HV) "2.1") //
				.verifyComplete();
	}

	@Test // DATAREDIS-602
	public void keys() {

		assumeTrue(hashKeyFactory instanceof StringObjectFactory);

		K key = keyFactory.instance();
		HK hashkey1 = hashKeyFactory.instance();
		HV hashvalue1 = hashValueFactory.instance();

		HK hashkey2 = hashKeyFactory.instance();
		HV hashvalue2 = hashValueFactory.instance();

		putAll(key, hashkey1, hashvalue1, hashkey2, hashvalue2);

		StepVerifier.create(hashOperations.keys(key).buffer(2)) //
				.consumeNextWith(list -> assertThat(list).containsExactlyInAnyOrder(hashkey1, hashkey2)) //
				.verifyComplete();
	}

	@Test // DATAREDIS-602
	public void size() {

		K key = keyFactory.instance();
		HK hashkey1 = hashKeyFactory.instance();
		HV hashvalue1 = hashValueFactory.instance();

		HK hashkey2 = hashKeyFactory.instance();
		HV hashvalue2 = hashValueFactory.instance();

		putAll(key, hashkey1, hashvalue1, hashkey2, hashvalue2);

		StepVerifier.create(hashOperations.size(key)) //
				.expectNext(2L) //
				.verifyComplete();
	}

	@Test // DATAREDIS-602
	public void putAll() {

		K key = keyFactory.instance();
		HK hashkey1 = hashKeyFactory.instance();
		HV hashvalue1 = hashValueFactory.instance();

		HK hashkey2 = hashKeyFactory.instance();
		HV hashvalue2 = hashValueFactory.instance();

		putAll(key, hashkey1, hashvalue1, hashkey2, hashvalue2);

		StepVerifier.create(hashOperations.hasKey(key, hashkey1)) //
				.expectNext(true) //
				.verifyComplete();

		StepVerifier.create(hashOperations.hasKey(key, hashkey2)) //
				.expectNext(true) //
				.verifyComplete();
	}

	@Test // DATAREDIS-602
	public void put() {

		K key = keyFactory.instance();
		HK hashkey = hashKeyFactory.instance();
		HV hashvalue = hashValueFactory.instance();

		StepVerifier.create(hashOperations.put(key, hashkey, hashvalue)) //
				.expectNext(true) //
				.verifyComplete();
	}

	@Test // DATAREDIS-602
	public void putIfAbsent() {

		K key = keyFactory.instance();
		HK hashkey = hashKeyFactory.instance();
		HV hashvalue = hashValueFactory.instance();
		HV hashvalue2 = hashValueFactory.instance();

		StepVerifier.create(hashOperations.putIfAbsent(key, hashkey, hashvalue)) //
				.expectNext(true) //
				.verifyComplete();

		StepVerifier.create(hashOperations.putIfAbsent(key, hashkey, hashvalue2)) //
				.expectNext(false) //
				.verifyComplete();
	}

	@Test // DATAREDIS-602
	public void values() {

		assumeTrue(hashValueFactory instanceof StringObjectFactory);

		K key = keyFactory.instance();
		HK hashkey1 = hashKeyFactory.instance();
		HV hashvalue1 = hashValueFactory.instance();

		HK hashkey2 = hashKeyFactory.instance();
		HV hashvalue2 = hashValueFactory.instance();

		putAll(key, hashkey1, hashvalue1, hashkey2, hashvalue2);

		StepVerifier.create(hashOperations.values(key)) //
				.expectNextCount(2) //
				.verifyComplete();
	}

	@Test // DATAREDIS-602
	public void entries() {

		assumeTrue(hashKeyFactory instanceof StringObjectFactory && hashValueFactory instanceof StringObjectFactory);

		K key = keyFactory.instance();
		HK hashkey1 = hashKeyFactory.instance();
		HV hashvalue1 = hashValueFactory.instance();

		HK hashkey2 = hashKeyFactory.instance();
		HV hashvalue2 = hashValueFactory.instance();

		putAll(key, hashkey1, hashvalue1, hashkey2, hashvalue2);

		StepVerifier.create(hashOperations.entries(key).buffer(2)) //
				.consumeNextWith(list -> {

					Entry<HK, HV> entry1 = Collections.singletonMap(hashkey1, hashvalue1).entrySet().iterator().next();
					Entry<HK, HV> entry2 = Collections.singletonMap(hashkey2, hashvalue2).entrySet().iterator().next();

					assertThat(list).containsExactlyInAnyOrder(entry1, entry2);
				}) //
				.verifyComplete();
	}

	@Test // DATAREDIS-743
	public void scan() {

		assumeTrue(hashKeyFactory instanceof StringObjectFactory && hashValueFactory instanceof StringObjectFactory);

		K key = keyFactory.instance();
		HK hashkey1 = hashKeyFactory.instance();
		HV hashvalue1 = hashValueFactory.instance();

		HK hashkey2 = hashKeyFactory.instance();
		HV hashvalue2 = hashValueFactory.instance();

		putAll(key, hashkey1, hashvalue1, hashkey2, hashvalue2);

		StepVerifier.create(hashOperations.scan(key).buffer(2)) //
				.consumeNextWith(list -> {

					Entry<HK, HV> entry1 = Collections.singletonMap(hashkey1, hashvalue1).entrySet().iterator().next();
					Entry<HK, HV> entry2 = Collections.singletonMap(hashkey2, hashvalue2).entrySet().iterator().next();

					assertThat(list).containsExactlyInAnyOrder(entry1, entry2);
				}) //
				.verifyComplete();
	}

	@Test // DATAREDIS-602
	public void delete() {

		K key = keyFactory.instance();
		HK hashkey = hashKeyFactory.instance();
		HV hashvalue = hashValueFactory.instance();

		StepVerifier.create(hashOperations.put(key, hashkey, hashvalue)) //
				.expectNext(true) //
				.verifyComplete();

		StepVerifier.create(hashOperations.delete(key)) //
				.expectNext(true) //
				.verifyComplete();

		StepVerifier.create(hashOperations.size(key)) //
				.expectNext(0L) //
				.verifyComplete();
	}

	private void putAll(K key, HK hashkey1, HV hashvalue1, HK hashkey2, HV hashvalue2) {

		Map<HK, HV> map = new HashMap<>();
		map.put(hashkey1, hashvalue1);
		map.put(hashkey2, hashvalue2);

		StepVerifier.create(hashOperations.putAll(key, map)) //
				.expectNext(true) //
				.verifyComplete();
	}
}
