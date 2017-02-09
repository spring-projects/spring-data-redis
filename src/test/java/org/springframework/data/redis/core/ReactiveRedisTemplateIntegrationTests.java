/*
 * Copyright 2017 the original author or authors.
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

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.ConnectionFactoryTracker;
import org.springframework.data.redis.ObjectFactory;
import org.springframework.data.redis.Person;
import org.springframework.data.redis.PersonObjectFactory;
import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.connection.ReactiveRedisClusterConnection;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.serializer.JdkSerializationRedisSerializer;
import org.springframework.data.redis.serializer.ReactiveSerializationContext;
import org.springframework.data.redis.serializer.StringRedisSerializer;

/**
 * Integration tests for {@link ReactiveRedisTemplate}.
 *
 * @author Mark Paluch
 */
@RunWith(Parameterized.class)
public class ReactiveRedisTemplateIntegrationTests<K, V> {

	private final ReactiveRedisTemplate<K, V> redisTemplate;

	private final ObjectFactory<K> keyFactory;

	private final ObjectFactory<V> valueFactory;

	@Parameters(name = "{3}")
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
	public ReactiveRedisTemplateIntegrationTests(ReactiveRedisTemplate<K, V> redisTemplate, ObjectFactory<K> keyFactory,
			ObjectFactory<V> valueFactory, String label) {

		this.redisTemplate = redisTemplate;
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
	public void exists() {

		K key = keyFactory.instance();

		StepVerifier.create(redisTemplate.hasKey(key)).expectNext(false).verifyComplete();

		StepVerifier.create(redisTemplate.opsForValue().set(key, valueFactory.instance())).expectNext(true)
				.verifyComplete();

		StepVerifier.create(redisTemplate.hasKey(key)).expectNext(true).verifyComplete();
	}

	@Test // DATAREDIS-602
	public void type() {

		K key = keyFactory.instance();

		StepVerifier.create(redisTemplate.type(key)).expectNext(DataType.NONE).verifyComplete();

		StepVerifier.create(redisTemplate.opsForValue().set(key, valueFactory.instance())).expectNext(true)
				.verifyComplete();

		StepVerifier.create(redisTemplate.type(key)).expectNext(DataType.STRING).verifyComplete();
	}

	@Test // DATAREDIS-602
	public void rename() {

		K oldName = keyFactory.instance();
		K newName = keyFactory.instance();

		StepVerifier.create(redisTemplate.opsForValue().set(oldName, valueFactory.instance())).expectNext(true)
				.verifyComplete();

		StepVerifier.create(redisTemplate.rename(oldName, newName)) //
				.expectNext(true) //
				.expectComplete() //
				.verify();
	}

	@Test // DATAREDIS-602
	public void renameNx() {

		K oldName = keyFactory.instance();
		K existing = keyFactory.instance();
		K newName = keyFactory.instance();

		StepVerifier.create(redisTemplate.opsForValue().set(oldName, valueFactory.instance())).expectNext(true)
				.verifyComplete();
		StepVerifier.create(redisTemplate.opsForValue().set(existing, valueFactory.instance())).expectNext(true)
				.verifyComplete();

		StepVerifier.create(redisTemplate.renameIfAbsent(oldName, newName)) //
				.expectNext(true) //
				.expectComplete() //
				.verify();

		StepVerifier.create(redisTemplate.opsForValue().set(existing, valueFactory.instance())).expectNext(true)
				.verifyComplete();

		StepVerifier.create(redisTemplate.renameIfAbsent(newName, existing)).expectNext(false) //
				.expectComplete() //
				.verify();
	}

	@Test // DATAREDIS-602
	public void expire() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		StepVerifier.create(redisTemplate.opsForValue().set(key, value)).expectNext(true).verifyComplete();

		StepVerifier.create(redisTemplate.expire(key, Duration.ofSeconds(10))).expectNext(true).verifyComplete();

		StepVerifier.create(redisTemplate.getExpire(key)) //
				.consumeNextWith(actual -> assertThat(actual).isGreaterThan(Duration.ofSeconds(8))).verifyComplete();
	}

	@Test // DATAREDIS-602
	public void preciseExpire() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		StepVerifier.create(redisTemplate.opsForValue().set(key, value)).expectNext(true).verifyComplete();

		StepVerifier.create(redisTemplate.expire(key, Duration.ofMillis(10_001))).expectNext(true).verifyComplete();

		StepVerifier.create(redisTemplate.getExpire(key)) //
				.consumeNextWith(actual -> assertThat(actual).isGreaterThan(Duration.ofSeconds(8))).verifyComplete();
	}

	@Test // DATAREDIS-602
	public void expireAt() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		StepVerifier.create(redisTemplate.opsForValue().set(key, value)).expectNext(true).verifyComplete();

		Instant expireAt = Instant.ofEpochSecond(Instant.now().plus(Duration.ofSeconds(10)).getEpochSecond());

		StepVerifier.create(redisTemplate.expireAt(key, expireAt)).expectNext(true).verifyComplete();

		StepVerifier.create(redisTemplate.getExpire(key)) //
				.consumeNextWith(actual -> assertThat(actual).isGreaterThan(Duration.ofSeconds(8))) //
				.verifyComplete();
	}

	@Test // DATAREDIS-602
	public void preciseExpireAt() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		StepVerifier.create(redisTemplate.opsForValue().set(key, value)).expectNext(true).verifyComplete();

		Instant expireAt = Instant.ofEpochSecond(Instant.now().plus(Duration.ofSeconds(10)).getEpochSecond(), 5);

		StepVerifier.create(redisTemplate.expireAt(key, expireAt)).expectNext(true).verifyComplete();

		StepVerifier.create(redisTemplate.getExpire(key)) //
				.consumeNextWith(actual -> assertThat(actual).isGreaterThan(Duration.ofSeconds(8))) //
				.verifyComplete();
	}

	@Test // DATAREDIS-602
	public void getTtlForAbsentKeyShouldCompleteWithoutValue() {

		K key = keyFactory.instance();

		StepVerifier.create(redisTemplate.getExpire(key)).verifyComplete();
	}

	@Test // DATAREDIS-602
	public void getTtlForKeyWithoutExpiryShouldCompleteWithZeroDuration() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		StepVerifier.create(redisTemplate.opsForValue().set(key, value)).expectNext(true).verifyComplete();

		StepVerifier.create(redisTemplate.getExpire(key)).expectNext(Duration.ZERO).verifyComplete();
	}

	@Test // DATAREDIS-602
	public void move() {

		ReactiveRedisClusterConnection connection = null;
		try {
			connection = redisTemplate.getConnectionFactory().getReactiveClusterConnection();
			assumeTrue(connection == null);
		} catch (InvalidDataAccessApiUsageException e) {} finally {
			if (connection != null) {
				connection.close();
			}
		}

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		StepVerifier.create(redisTemplate.opsForValue().set(key, value)).expectNext(true).verifyComplete();

		StepVerifier.create(redisTemplate.move(key, 5)).expectNext(true).verifyComplete();

		StepVerifier.create(redisTemplate.hasKey(key)).expectNext(false).verifyComplete();
	}

	@Test // DATAREDIS-602
	public void shouldApplyCustomSerializationContextToValues() {

		Person key = new PersonObjectFactory().instance();
		Person value = new PersonObjectFactory().instance();

		JdkSerializationRedisSerializer jdkSerializer = new JdkSerializationRedisSerializer();
		ReactiveSerializationContext<Object, Object> objectSerializers = ReactiveSerializationContext.builder()
				.key(jdkSerializer) //
				.value(jdkSerializer) //
				.hashKey(jdkSerializer) //
				.hashValue(jdkSerializer) //
				.build();

		ReactiveValueOperations<Object, Object> valueOperations = redisTemplate.opsForValue(objectSerializers);

		StepVerifier.create(valueOperations.set(key, value)).expectNext(true).verifyComplete();

		StepVerifier.create(valueOperations.get(key)).expectNext(value).verifyComplete();
	}

	@Test // DATAREDIS-602
	public void shouldApplyCustomSerializationContextToHash() {

		ReactiveSerializationContext<K, V> serializationContext = redisTemplate.getSerializationContext();

		K key = keyFactory.instance();
		String hashField = "foo";
		Person hashValue = new PersonObjectFactory().instance();

		JdkSerializationRedisSerializer jdkSerializer = new JdkSerializationRedisSerializer();
		ReactiveSerializationContext<K, V> objectSerializers = ReactiveSerializationContext.<K, V> builder()
				.key(serializationContext.key()) //
				.value(serializationContext.value()) //
				.hashKey(new StringRedisSerializer()) //
				.hashValue(jdkSerializer) //
				.build();

		ReactiveHashOperations<K, String, Object> hashOperations = redisTemplate.opsForHash(objectSerializers);

		StepVerifier.create(hashOperations.put(key, hashField, hashValue)).expectNext(true).verifyComplete();

		StepVerifier.create(hashOperations.get(key, hashField)).expectNext(hashValue).verifyComplete();
	}
}
