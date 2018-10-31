/*
 * Copyright 2018 the original author or authors.
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
package org.springframework.data.redis.stream;

import static org.assertj.core.api.Assertions.*;
import static org.junit.Assume.*;

import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.Collections;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.data.redis.ConnectionFactoryTracker;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.RedisVersionUtils;
import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.RedisStreamCommands.Consumer;
import org.springframework.data.redis.connection.RedisStreamCommands.MapRecord;
import org.springframework.data.redis.connection.RedisStreamCommands.ReadOffset;
import org.springframework.data.redis.connection.RedisStreamCommands.StreamOffset;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceTestClientResources;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.stream.StreamReceiver.StreamReceiverOptions;

/**
 * Integration tests for {@link StreamReceiver}.
 *
 * @author Mark Paluch
 */
public class StreamReceiverIntegrationTests {

	private static final RedisStandaloneConfiguration standaloneConfiguration = new RedisStandaloneConfiguration(
			SettingsUtils.getHost(), SettingsUtils.getPort());

	private static LettuceConnectionFactory connectionFactory;
	StringRedisTemplate redisTemplate = new StringRedisTemplate(connectionFactory);

	@BeforeClass
	public static void beforeClass() {

		LettuceClientConfiguration clientConfiguration = LettuceClientConfiguration.builder() //
				.shutdownTimeout(Duration.ZERO) //
				.clientResources(LettuceTestClientResources.getSharedClientResources()) //
				.build();

		LettuceConnectionFactory lettuceConnectionFactory = new LettuceConnectionFactory(standaloneConfiguration,
				clientConfiguration);
		lettuceConnectionFactory.afterPropertiesSet();

		ConnectionFactoryTracker.add(lettuceConnectionFactory);

		connectionFactory = lettuceConnectionFactory;

		// TODO: Upgrade to 5.0
		assumeTrue(RedisVersionUtils.atLeast("5.0", connectionFactory.getConnection()));
	}

	@AfterClass
	public static void tearDown() {
		ConnectionFactoryTracker.cleanUp();
	}

	@Before
	public void before() {

		RedisConnection connection = connectionFactory.getConnection();
		connection.flushDb();
		connection.close();
	}

	@Test // DATAREDIS-864
	public void shouldReceiveMessages() {

		StreamReceiver<String, String, String> receiver = StreamReceiver.create(connectionFactory);

		Flux<MapRecord<String, String, String>> messages = receiver
				.receive(StreamOffset.create("my-stream", ReadOffset.from("0-0")));

		messages.as(StepVerifier::create) //
				.then(() -> redisTemplate.opsForStream().add("my-stream", Collections.singletonMap("key", "value")))
				.consumeNextWith(it -> {

					assertThat(it.getStream()).isEqualTo("my-stream");
					// assertThat(it.getValue()).containsEntry("key", "value");
				}) //
				.thenCancel() //
				.verify(Duration.ofSeconds(5));
	}

	@Test // DATAREDIS-864
	public void latestModeLosesMessages() {

		// XADD/XREAD highly timing-dependent as this tests require a poll subscription to receive messages using $ offset.

		StreamReceiverOptions<String, String, String> options = StreamReceiverOptions.builder()
				.pollTimeout(Duration.ofSeconds(4)).build();
		StreamReceiver<String, String, String> receiver = StreamReceiver.create(connectionFactory, options);

		Flux<MapRecord<String, String, String>> messages = receiver
				.receive(StreamOffset.create("my-stream", ReadOffset.latest()));

		messages.as(publisher -> StepVerifier.create(publisher, 0)) //
				.thenRequest(1) //
				.then(() -> {
					try {
						Thread.sleep(500);
						redisTemplate.opsForStream().add("my-stream", Collections.singletonMap("key", "value1"));
					} catch (InterruptedException e) {}
				}) //
				.expectNextCount(1) //
				.then(() -> {
					redisTemplate.opsForStream().add("my-stream", Collections.singletonMap("key", "value2"));
				}) //
				.thenRequest(1) //
				.then(() -> {
					try {
						Thread.sleep(500);
						redisTemplate.opsForStream().add("my-stream", Collections.singletonMap("key", "value3"));
					} catch (InterruptedException e) {}
				}).consumeNextWith(it -> {

					assertThat(it.getStream()).isEqualTo("my-stream");
					// assertThat(it.getValue()).containsEntry("key", "value3");
				}) //
				.thenCancel() //
				.verify(Duration.ofSeconds(5));
	}

	@Test // DATAREDIS-864
	public void shouldReceiveAsConsumerGroupMessages() {

		StreamReceiver<String, String, String> receiver = StreamReceiver.create(connectionFactory);

		Flux<MapRecord<String, String, String>> messages = receiver.receive(Consumer.from("my-group", "my-consumer-id"),
				StreamOffset.create("my-stream", ReadOffset.lastConsumed()));

		// required to initialize stream
		redisTemplate.opsForStream().add("my-stream", Collections.singletonMap("key", "value"));
		redisTemplate.opsForStream().createGroup("my-stream", ReadOffset.from("0-0"), "my-group");
		redisTemplate.opsForStream().add("my-stream", Collections.singletonMap("key2", "value2"));

		messages.as(StepVerifier::create) //
				.consumeNextWith(it -> {

					assertThat(it.getStream()).isEqualTo("my-stream");
					// assertThat(it.getValue()).containsEntry("key", "value");
					assertThat(it.getValue()).containsValue("value");
				}).consumeNextWith(it -> {

					assertThat(it.getStream()).isEqualTo("my-stream");
					// assertThat(it.getValue()).containsEntry("key2", "value2");
					assertThat(it.getValue()).containsValue("value2");
				}) //
				.thenCancel() //
				.verify(Duration.ofSeconds(5));
	}

	@Test // DATAREDIS-864
	public void shouldStopReceivingOnError() {

		StreamReceiverOptions<String, String, String> options = StreamReceiverOptions.builder()
				.pollTimeout(Duration.ofMillis(100)).build();

		StreamReceiver<String, String, String> receiver = StreamReceiver.create(connectionFactory, options);

		Flux<MapRecord<String, String, String>> messages = receiver.receive(Consumer.from("my-group", "my-consumer-id"),
				StreamOffset.create("my-stream", ReadOffset.lastConsumed()));

		// required to initialize stream
		redisTemplate.opsForStream().add("my-stream", Collections.singletonMap("key", "value"));
		redisTemplate.opsForStream().createGroup("my-stream", ReadOffset.from("0-0"), "my-group");

		messages.as(StepVerifier::create) //
				.expectNextCount(1) //
				.then(() -> redisTemplate.delete("my-stream")) //
				.expectError(RedisSystemException.class) //
				.verify(Duration.ofSeconds(5));
	}
}
