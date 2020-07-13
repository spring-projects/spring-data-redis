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

import org.springframework.data.redis.StringObjectFactory;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

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
import org.springframework.data.redis.connection.ReactiveSubscription.ChannelMessage;
import org.springframework.data.redis.connection.ReactiveSubscription.Message;
import org.springframework.data.redis.connection.ReactiveSubscription.PatternMessage;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.JdkSerializationRedisSerializer;
import org.springframework.data.redis.serializer.RedisElementReader;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.data.redis.serializer.RedisSerializationContext.SerializationPair;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

/**
 * Integration tests for {@link ReactiveRedisTemplate}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 */
@RunWith(Parameterized.class)
public class ReactiveRedisTemplateIntegrationTests<K, V> {

	private final ReactiveRedisTemplate<K, V> redisTemplate;

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
	public ReactiveRedisTemplateIntegrationTests(ReactiveRedisTemplate<K, V> redisTemplate, ObjectFactory<K> keyFactory,
			ObjectFactory<V> valueFactory, RedisSerializer serializer, String label) {

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

		redisTemplate.hasKey(key).as(StepVerifier::create).expectNext(false).verifyComplete();

		redisTemplate.opsForValue().set(key, valueFactory.instance()).as(StepVerifier::create).expectNext(true)
				.verifyComplete();

		redisTemplate.hasKey(key).as(StepVerifier::create).expectNext(true).verifyComplete();
	}

	@Test // DATAREDIS-743
	public void scan() {

		assumeFalse(valueFactory.instance() instanceof Person);

		Map<K, V> tuples = new HashMap<>();
		tuples.put(keyFactory.instance(), valueFactory.instance());
		tuples.put(keyFactory.instance(), valueFactory.instance());
		tuples.put(keyFactory.instance(), valueFactory.instance());

		redisTemplate.opsForValue().multiSet(tuples).as(StepVerifier::create).expectNext(true).verifyComplete();

		redisTemplate.scan().collectList().as(StepVerifier::create) //
				.consumeNextWith(actual -> assertThat(actual).containsAll(tuples.keySet())) //
				.verifyComplete();
	}

	@Test // DATAREDIS-602
	public void type() {

		K key = keyFactory.instance();

		redisTemplate.type(key).as(StepVerifier::create).expectNext(DataType.NONE).verifyComplete();

		redisTemplate.opsForValue().set(key, valueFactory.instance()).as(StepVerifier::create).expectNext(true)
				.verifyComplete();

		redisTemplate.type(key).as(StepVerifier::create).expectNext(DataType.STRING).verifyComplete();
	}

	@Test // DATAREDIS-602
	public void rename() {

		K oldName = keyFactory.instance();
		K newName = keyFactory.instance();

		redisTemplate.opsForValue().set(oldName, valueFactory.instance()).as(StepVerifier::create).expectNext(true)
				.verifyComplete();

		redisTemplate.rename(oldName, newName).as(StepVerifier::create) //
				.expectNext(true) //
				.expectComplete() //
				.verify();
	}

	@Test // DATAREDIS-602
	public void renameNx() {

		K oldName = keyFactory.instance();
		K existing = keyFactory.instance();
		K newName = keyFactory.instance();

		redisTemplate.opsForValue().set(oldName, valueFactory.instance()).as(StepVerifier::create).expectNext(true)
				.verifyComplete();
		redisTemplate.opsForValue().set(existing, valueFactory.instance()).as(StepVerifier::create).expectNext(true)
				.verifyComplete();

		redisTemplate.renameIfAbsent(oldName, newName).as(StepVerifier::create) //
				.expectNext(true) //
				.expectComplete() //
				.verify();

		redisTemplate.opsForValue().set(existing, valueFactory.instance()).as(StepVerifier::create).expectNext(true)
				.verifyComplete();

		redisTemplate.renameIfAbsent(newName, existing).as(StepVerifier::create).expectNext(false) //
				.expectComplete() //
				.verify();
	}

	@Test // DATAREDIS-693
	public void unlink() {

		K single = keyFactory.instance();

		redisTemplate.opsForValue().set(single, valueFactory.instance()).as(StepVerifier::create).expectNext(true)
				.verifyComplete();

		redisTemplate.unlink(single).as(StepVerifier::create).expectNext(1L).verifyComplete();

		redisTemplate.hasKey(single).as(StepVerifier::create).expectNext(false).verifyComplete();
	}

	@Test // DATAREDIS-693
	public void unlinkMany() {

		K key1 = keyFactory.instance();
		K key2 = keyFactory.instance();

		redisTemplate.opsForValue().set(key1, valueFactory.instance()).as(StepVerifier::create).expectNext(true)
				.verifyComplete();
		redisTemplate.opsForValue().set(key2, valueFactory.instance()).as(StepVerifier::create).expectNext(true)
				.verifyComplete();

		redisTemplate.unlink(key1, key2).as(StepVerifier::create).expectNext(2L).verifyComplete();

		redisTemplate.hasKey(key1).as(StepVerifier::create).expectNext(false).verifyComplete();
		redisTemplate.hasKey(key2).as(StepVerifier::create).expectNext(false).verifyComplete();
	}
	
	@Test // DATAREDIS-913
	public void unlinkManyPublisher() {

		K key1 = keyFactory.instance();
		K key2 = keyFactory.instance();
		
		assumeTrue(key1 instanceof String && valueFactory instanceof StringObjectFactory);

		redisTemplate.opsForValue().set(key1, valueFactory.instance()).as(StepVerifier::create).expectNext(true)
				.verifyComplete();
		redisTemplate.opsForValue().set(key2, valueFactory.instance()).as(StepVerifier::create).expectNext(true)
				.verifyComplete();

		redisTemplate.unlink(redisTemplate.keys((K) "*")).as(StepVerifier::create).expectNext(2L).verifyComplete();

		redisTemplate.hasKey(key1).as(StepVerifier::create).expectNext(false).verifyComplete();
		redisTemplate.hasKey(key2).as(StepVerifier::create).expectNext(false).verifyComplete();
	}
	
	@Test // DATAREDIS-913
	public void deleteManyPublisher() {

		K key1 = keyFactory.instance();
		K key2 = keyFactory.instance();
		
		assumeTrue(key1 instanceof String && valueFactory instanceof StringObjectFactory);

		redisTemplate.opsForValue().set(key1, valueFactory.instance()).as(StepVerifier::create).expectNext(true)
				.verifyComplete();
		redisTemplate.opsForValue().set(key2, valueFactory.instance()).as(StepVerifier::create).expectNext(true)
				.verifyComplete();

		redisTemplate.delete(redisTemplate.keys((K) "*")).as(StepVerifier::create).expectNext(2L).verifyComplete();

		redisTemplate.hasKey(key1).as(StepVerifier::create).expectNext(false).verifyComplete();
		redisTemplate.hasKey(key2).as(StepVerifier::create).expectNext(false).verifyComplete();
	}

	@Test // DATAREDIS-683
	@SuppressWarnings("unchecked")
	public void executeScript() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		assumeFalse(value instanceof Long);

		redisTemplate.opsForValue().set(key, value).as(StepVerifier::create).expectNext(true).verifyComplete();

		Flux<V> execute = redisTemplate.execute(
				new DefaultRedisScript<>("return redis.call('get', KEYS[1])", (Class<V>) value.getClass()),
				Collections.singletonList(key));

		execute.as(StepVerifier::create).expectNext(value).verifyComplete();
	}

	@Test // DATAREDIS-683
	public void executeScriptWithElementReaderAndWriter() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		SerializationPair json = SerializationPair.fromSerializer(new Jackson2JsonRedisSerializer<>(Person.class));
		RedisElementReader<String> resultReader = RedisElementReader.from(StringRedisSerializer.UTF_8);

		assumeFalse(value instanceof Long);

		Person person = new Person("Walter", "White", 51);
		redisTemplate
				.execute(new DefaultRedisScript<>("return redis.call('set', KEYS[1], ARGV[1])", String.class),
						Collections.singletonList(key), Collections.singletonList(person), json.getWriter(), resultReader)
				.as(StepVerifier::create)
				.expectNext("OK").verifyComplete();

		Flux<Person> execute = redisTemplate.execute(
				new DefaultRedisScript<>("return redis.call('get', KEYS[1])", Person.class), Collections.singletonList(key),
				Collections.emptyList(), json.getWriter(), json.getReader());

		execute.as(StepVerifier::create).expectNext(person).verifyComplete();
	}

	@Test // DATAREDIS-602
	public void expire() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		redisTemplate.opsForValue().set(key, value).as(StepVerifier::create).expectNext(true).verifyComplete();

		redisTemplate.expire(key, Duration.ofSeconds(10)).as(StepVerifier::create).expectNext(true).verifyComplete();

		redisTemplate.getExpire(key).as(StepVerifier::create) //
				.consumeNextWith(actual -> assertThat(actual).isGreaterThan(Duration.ofSeconds(8))).verifyComplete();
	}

	@Test // DATAREDIS-602
	public void preciseExpire() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		redisTemplate.opsForValue().set(key, value).as(StepVerifier::create).expectNext(true).verifyComplete();

		redisTemplate.expire(key, Duration.ofMillis(10_001)).as(StepVerifier::create).expectNext(true).verifyComplete();

		redisTemplate.getExpire(key).as(StepVerifier::create) //
				.consumeNextWith(actual -> assertThat(actual).isGreaterThan(Duration.ofSeconds(8))).verifyComplete();
	}

	@Test // DATAREDIS-602
	public void expireAt() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		redisTemplate.opsForValue().set(key, value).as(StepVerifier::create).expectNext(true).verifyComplete();

		Instant expireAt = Instant.ofEpochSecond(Instant.now().plus(Duration.ofSeconds(10)).getEpochSecond());

		redisTemplate.expireAt(key, expireAt).as(StepVerifier::create).expectNext(true).verifyComplete();

		redisTemplate.getExpire(key).as(StepVerifier::create) //
				.consumeNextWith(actual -> assertThat(actual).isGreaterThan(Duration.ofSeconds(8))) //
				.verifyComplete();
	}

	@Test // DATAREDIS-602
	public void preciseExpireAt() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		redisTemplate.opsForValue().set(key, value).as(StepVerifier::create).expectNext(true).verifyComplete();

		Instant expireAt = Instant.ofEpochSecond(Instant.now().plus(Duration.ofSeconds(10)).getEpochSecond(), 5);

		redisTemplate.expireAt(key, expireAt).as(StepVerifier::create).expectNext(true).verifyComplete();

		redisTemplate.getExpire(key).as(StepVerifier::create) //
				.consumeNextWith(actual -> assertThat(actual).isGreaterThan(Duration.ofSeconds(8))) //
				.verifyComplete();
	}

	@Test // DATAREDIS-602
	public void getTtlForAbsentKeyShouldCompleteWithoutValue() {

		K key = keyFactory.instance();

		redisTemplate.getExpire(key).as(StepVerifier::create).verifyComplete();
	}

	@Test // DATAREDIS-602
	public void getTtlForKeyWithoutExpiryShouldCompleteWithZeroDuration() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		redisTemplate.opsForValue().set(key, value).as(StepVerifier::create).expectNext(true).verifyComplete();

		redisTemplate.getExpire(key).as(StepVerifier::create).expectNext(Duration.ZERO).verifyComplete();
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

		redisTemplate.opsForValue().set(key, value).as(StepVerifier::create).expectNext(true).verifyComplete();

		redisTemplate.move(key, 5).as(StepVerifier::create).expectNext(true).verifyComplete();

		redisTemplate.hasKey(key).as(StepVerifier::create).expectNext(false).verifyComplete();
	}

	@Test // DATAREDIS-602
	public void shouldApplyCustomSerializationContextToValues() {

		Person key = new PersonObjectFactory().instance();
		Person value = new PersonObjectFactory().instance();

		JdkSerializationRedisSerializer jdkSerializer = new JdkSerializationRedisSerializer();
		RedisSerializationContext<Object, Object> objectSerializers = RedisSerializationContext.newSerializationContext()
				.key(jdkSerializer) //
				.value(jdkSerializer) //
				.hashKey(jdkSerializer) //
				.hashValue(jdkSerializer) //
				.build();

		ReactiveValueOperations<Object, Object> valueOperations = redisTemplate.opsForValue(objectSerializers);

		valueOperations.set(key, value).as(StepVerifier::create).expectNext(true).verifyComplete();

		valueOperations.get(key).as(StepVerifier::create).expectNext(value).verifyComplete();
	}

	@Test // DATAREDIS-602
	public void shouldApplyCustomSerializationContextToHash() {

		RedisSerializationContext<K, V> serializationContext = redisTemplate.getSerializationContext();

		K key = keyFactory.instance();
		String hashField = "foo";
		Person hashValue = new PersonObjectFactory().instance();

		RedisSerializationContext<K, V> objectSerializers = RedisSerializationContext.<K, V> newSerializationContext()
				.key(serializationContext.getKeySerializationPair()) //
				.value(serializationContext.getValueSerializationPair()) //
				.hashKey(StringRedisSerializer.UTF_8) //
				.hashValue(new JdkSerializationRedisSerializer()) //
				.build();

		ReactiveHashOperations<K, String, Object> hashOperations = redisTemplate.opsForHash(objectSerializers);

		hashOperations.put(key, hashField, hashValue).as(StepVerifier::create).expectNext(true).verifyComplete();

		hashOperations.get(key, hashField).as(StepVerifier::create).expectNext(hashValue).verifyComplete();
	}

	@Test // DATAREDIS-612
	public void listenToChannelShouldReceiveChannelMessagesCorrectly() throws InterruptedException {

		String channel = "my-channel";

		V message = valueFactory.instance();

		redisTemplate.listenToChannel(channel).as(StepVerifier::create) //
				.thenAwait(Duration.ofMillis(500)) // just make sure we the subscription completed
				.then(() -> redisTemplate.convertAndSend(channel, message).block()) //
				.assertNext(received -> {

					assertThat(received).isInstanceOf(ChannelMessage.class);
					assertThat(received.getMessage()).isEqualTo(message);
					assertThat(received.getChannel()).isEqualTo(channel);
				}) //
				.thenAwait(Duration.ofMillis(10)) //
				.thenCancel() //
				.verify(Duration.ofSeconds(3));
	}

	@Test // DATAREDIS-612
	public void listenToChannelPatternShouldReceiveChannelMessagesCorrectly() throws InterruptedException {

		String channel = "my-channel";
		String pattern = "my-*";

		V message = valueFactory.instance();

		Flux<? extends Message<String, V>> stream = redisTemplate.listenToPattern(pattern);

		stream.as(StepVerifier::create) //
				.thenAwait(Duration.ofMillis(500)) // just make sure we the subscription completed
				.then(() -> redisTemplate.convertAndSend(channel, message).block()) //
				.assertNext(received -> {

					assertThat(received).isInstanceOf(PatternMessage.class);
					assertThat(received.getMessage()).isEqualTo(message);
					assertThat(received.getChannel()).isEqualTo(channel);
					assertThat(((PatternMessage) received).getPattern()).isEqualTo(pattern);
				}) //
				.thenCancel() //
				.verify(Duration.ofSeconds(3));
	}
}
