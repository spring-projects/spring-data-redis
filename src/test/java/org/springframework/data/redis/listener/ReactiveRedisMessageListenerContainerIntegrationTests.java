/*
 * Copyright 2018-2020 the original author or authors.
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
package org.springframework.data.redis.listener;

import static org.assertj.core.api.Assertions.*;

import reactor.core.Disposable;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import org.springframework.data.redis.connection.ReactiveSubscription;
import org.springframework.data.redis.connection.ReactiveSubscription.ChannelMessage;
import org.springframework.data.redis.connection.ReactiveSubscription.PatternMessage;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.data.redis.test.extension.parametrized.MethodSource;
import org.springframework.data.redis.test.extension.parametrized.ParameterizedRedisTest;
import org.springframework.lang.Nullable;

/**
 * Integration tests for {@link ReactiveRedisMessageListenerContainer} via Lettuce.
 *
 * @author Mark Paluch
 */
@MethodSource("testParams")
public class ReactiveRedisMessageListenerContainerIntegrationTests {

	private static final String CHANNEL1 = "my-channel";
	private static final String PATTERN1 = "my-chan*";
	private static final String MESSAGE = "hello world";

	private final LettuceConnectionFactory connectionFactory;
	private @Nullable RedisConnection connection;

	/**
	 * @param connectionFactory
	 * @param label parameterized test label, no further use besides that.
	 */
	public ReactiveRedisMessageListenerContainerIntegrationTests(LettuceConnectionFactory connectionFactory,
			String label) {
		this.connectionFactory = connectionFactory;
	}

	public static Collection<Object[]> testParams() {
		return ReactiveOperationsTestParams.testParams();
	}

	@BeforeEach
	void before() {
		connection = connectionFactory.getConnection();
	}

	@AfterEach
	void tearDown() {

		if (connection != null) {
			connection.close();
		}
	}

	@ParameterizedRedisTest // DATAREDIS-612
	void shouldReceiveChannelMessages() {

		ReactiveRedisMessageListenerContainer container = new ReactiveRedisMessageListenerContainer(connectionFactory);

		container.receive(ChannelTopic.of(CHANNEL1)).as(StepVerifier::create) //
				.then(awaitSubscription(container::getActiveSubscriptions))
				.then(() -> connection.publish(CHANNEL1.getBytes(), MESSAGE.getBytes())) //
				.assertNext(c -> {

					assertThat(c.getChannel()).isEqualTo(CHANNEL1);
					assertThat(c.getMessage()).isEqualTo(MESSAGE);
				}) //
				.thenCancel().verify();

		container.destroy();
	}

	@ParameterizedRedisTest // DATAREDIS-612
	void shouldReceivePatternMessages() {

		ReactiveRedisMessageListenerContainer container = new ReactiveRedisMessageListenerContainer(connectionFactory);

		container.receive(PatternTopic.of(PATTERN1)).as(StepVerifier::create) //
				.then(awaitSubscription(container::getActiveSubscriptions))
				.then(() -> connection.publish(CHANNEL1.getBytes(), MESSAGE.getBytes())) //
				.assertNext(c -> {

					assertThat(c.getPattern()).isEqualTo(PATTERN1);
					assertThat(c.getChannel()).isEqualTo(CHANNEL1);
					assertThat(c.getMessage()).isEqualTo(MESSAGE);
				}) //
				.thenCancel().verify();

		container.destroy();
	}

	@ParameterizedRedisTest // DATAREDIS-612
	void shouldPublishAndReceiveMessage() throws InterruptedException {

		ReactiveRedisMessageListenerContainer container = new ReactiveRedisMessageListenerContainer(connectionFactory);
		ReactiveRedisTemplate<String, String> template = new ReactiveRedisTemplate<>(connectionFactory,
				RedisSerializationContext.string());

		BlockingQueue<PatternMessage<String, String, String>> messages = new LinkedBlockingDeque<>();
		Disposable subscription = container.receive(PatternTopic.of(PATTERN1)).doOnNext(messages::add).subscribe();

		StepVerifier.create(template.convertAndSend(CHANNEL1, MESSAGE), 0) //
				.then(awaitSubscription(container::getActiveSubscriptions)) //
				.thenRequest(1).expectNextCount(1) //
				.verifyComplete();

		PatternMessage<String, String, String> message = messages.poll(1, TimeUnit.SECONDS);

		assertThat(message).isNotNull();
		assertThat(message.getPattern()).isEqualTo(PATTERN1);
		assertThat(message.getChannel()).isEqualTo(CHANNEL1);
		assertThat(message.getMessage()).isEqualTo(MESSAGE);

		subscription.dispose();
		container.destroy();
	}

	@ParameterizedRedisTest // DATAREDIS-612
	void listenToChannelShouldReceiveChannelMessagesCorrectly() throws InterruptedException {

		ReactiveRedisTemplate<String, String> template = new ReactiveRedisTemplate<>(connectionFactory,
				RedisSerializationContext.string());

		template.listenToChannel(CHANNEL1).as(StepVerifier::create) //
				.thenAwait(Duration.ofMillis(100)) // just make sure we the subscription completed
				.then(() -> connection.publish(CHANNEL1.getBytes(), MESSAGE.getBytes())) //
				.assertNext(message -> {

					assertThat(message).isInstanceOf(ChannelMessage.class);
					assertThat(message.getMessage()).isEqualTo(MESSAGE);
					assertThat(((ChannelMessage) message).getChannel()).isEqualTo(CHANNEL1);
				}) //
				.thenCancel() //
				.verify();
	}

	@ParameterizedRedisTest // DATAREDIS-612
	void listenToPatternShouldReceiveMessagesCorrectly() {

		ReactiveRedisTemplate<String, String> template = new ReactiveRedisTemplate<>(connectionFactory,
				RedisSerializationContext.string());

		template.listenToPattern(PATTERN1).as(StepVerifier::create) //
				.thenAwait(Duration.ofMillis(100)) // just make sure we the subscription completed
				.then(() -> connection.publish(CHANNEL1.getBytes(), MESSAGE.getBytes())) //
				.assertNext(message -> {

					assertThat(message).isInstanceOf(PatternMessage.class);
					assertThat(((PatternMessage) message).getPattern()).isEqualTo(PATTERN1);
					assertThat(((PatternMessage) message).getChannel()).isEqualTo(CHANNEL1);
					assertThat(message.getMessage()).isEqualTo(MESSAGE);
				}) //
				.thenCancel() //
				.verify();
	}

	private static Runnable awaitSubscription(Supplier<Collection<ReactiveSubscription>> activeSubscriptions) {

		return () -> {
			Awaitility.await().until(() -> !activeSubscriptions.get().isEmpty());
		};
	}
}
