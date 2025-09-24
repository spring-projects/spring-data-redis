/*
 * Copyright 2018-2025 the original author or authors.
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
package org.springframework.data.redis.stream;

import static org.assertj.core.api.Assertions.*;

import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.output.NestedMultiOutput;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import org.springframework.core.convert.ConversionFailedException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceConnection;
import org.springframework.data.redis.connection.stream.ByteRecord;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ObjectRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.Record;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.stream.StreamMessageListenerContainer.StreamMessageListenerContainerOptions;
import org.springframework.data.redis.stream.StreamMessageListenerContainer.StreamReadRequest;
import org.springframework.data.redis.test.condition.EnabledOnCommand;
import org.springframework.util.NumberUtils;

/**
 * Integration tests for {@link StreamMessageListenerContainer}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @author John Blum
 */
@EnabledOnCommand("XREAD")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class AbstractStreamMessageListenerContainerIntegrationTests {

	private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(2);

	private final RedisConnectionFactory connectionFactory;
	private final StringRedisTemplate redisTemplate;
	private final StreamMessageListenerContainerOptions<String, MapRecord<String, String, String>> containerOptions = StreamMessageListenerContainerOptions
			.builder().pollTimeout(Duration.ofMillis(100)).build();

	AbstractStreamMessageListenerContainerIntegrationTests(RedisConnectionFactory connectionFactory) {
		this.connectionFactory = connectionFactory;
		this.redisTemplate = new StringRedisTemplate(connectionFactory);
		this.redisTemplate.afterPropertiesSet();
	}

	@BeforeEach
	void before() {

		RedisConnection connection = connectionFactory.getConnection();
		connection.flushDb();
		connection.close();
	}

	@Test // DATAREDIS-864
	void shouldReceiveMapMessages() throws InterruptedException {

		StreamMessageListenerContainer<String, MapRecord<String, String, String>> container = StreamMessageListenerContainer
				.create(connectionFactory, containerOptions);
		BlockingQueue<MapRecord<String, String, String>> queue = new LinkedBlockingQueue<>();

		container.start();
		Subscription subscription = container.receive(StreamOffset.create("my-stream", ReadOffset.from("0-0")), queue::add);

		subscription.await(DEFAULT_TIMEOUT);

		redisTemplate.opsForStream().add("my-stream", Collections.singletonMap("key", "value1"));
		redisTemplate.opsForStream().add("my-stream", Collections.singletonMap("key", "value2"));
		redisTemplate.opsForStream().add("my-stream", Collections.singletonMap("key", "value3"));

		assertThat(queue.poll(1, TimeUnit.SECONDS)).isNotNull();
		assertThat(queue.poll(1, TimeUnit.SECONDS)).isNotNull();
		assertThat(queue.poll(1, TimeUnit.SECONDS)).isNotNull();

		cancelAwait(subscription);

		assertThat(subscription.isActive()).isFalse();
	}

	@Test // DATAREDIS-864
	void shouldReceiveSimpleObjectHashRecords() throws InterruptedException {

		StreamMessageListenerContainerOptions<String, ObjectRecord<String, String>> containerOptions = StreamMessageListenerContainerOptions
				.builder().pollTimeout(Duration.ofMillis(100)).targetType(String.class).build();

		StreamMessageListenerContainer<String, ObjectRecord<String, String>> container = StreamMessageListenerContainer
				.create(connectionFactory, containerOptions);
		BlockingQueue<ObjectRecord<String, String>> queue = new LinkedBlockingQueue<>();

		container.start();
		Subscription subscription = container.receive(StreamOffset.create("my-stream", ReadOffset.from("0-0")), queue::add);

		subscription.await(DEFAULT_TIMEOUT);

		redisTemplate.opsForStream().add(ObjectRecord.create("my-stream", "value1"));

		assertThat(queue.poll(1, TimeUnit.SECONDS)).isNotNull().extracting(Record::getValue).isEqualTo("value1");

		cancelAwait(subscription);

		assertThat(subscription.isActive()).isFalse();
	}

	@Test // DATAREDIS-864
	void shouldReceiveObjectHashRecords() throws InterruptedException {

		StreamMessageListenerContainerOptions<String, ObjectRecord<String, LoginEvent>> containerOptions = StreamMessageListenerContainerOptions
				.builder().pollTimeout(Duration.ofMillis(100)).targetType(LoginEvent.class).build();

		StreamMessageListenerContainer<String, ObjectRecord<String, LoginEvent>> container = StreamMessageListenerContainer
				.create(connectionFactory, containerOptions);
		BlockingQueue<ObjectRecord<String, LoginEvent>> queue = new LinkedBlockingQueue<>();

		container.start();
		Subscription subscription = container.receive(StreamOffset.create("my-stream", ReadOffset.from("0-0")), queue::add);

		subscription.await(DEFAULT_TIMEOUT);

		redisTemplate.opsForStream().add(ObjectRecord.create("my-stream", new LoginEvent("Walter", "White")));

		assertThat(queue.poll(1, TimeUnit.SECONDS)).isNotNull().extracting(Record::getValue)
				.isEqualTo(new LoginEvent("Walter", "White"));

		cancelAwait(subscription);

		assertThat(subscription.isActive()).isFalse();
	}

	@Test // DATAREDIS-864, DATAREDIS-1079
	void shouldReceiveMessagesInConsumerGroup() throws InterruptedException {

		StreamMessageListenerContainer<String, MapRecord<String, String, String>> container = StreamMessageListenerContainer
				.create(connectionFactory, containerOptions);
		BlockingQueue<MapRecord<String, String, String>> queue = new LinkedBlockingQueue<>();
		RecordId messageId = redisTemplate.opsForStream().add("my-stream", Collections.singletonMap("key", "value1"));
		redisTemplate.opsForStream().createGroup("my-stream", ReadOffset.from(messageId), "my-group");

		container.start();
		Subscription subscription = container.receive(Consumer.from("my-group", "my-consumer"),
				StreamOffset.create("my-stream", ReadOffset.lastConsumed()), queue::add);

		subscription.await(DEFAULT_TIMEOUT);

		redisTemplate.opsForStream().add("my-stream", Collections.singletonMap("key", "value2"));

		MapRecord<String, String, String> message = queue.poll(1, TimeUnit.SECONDS);
		assertThat(message).isNotNull();
		assertThat(message.getValue()).containsEntry("key", "value2");

		assertThat(getNumberOfPending("my-stream", "my-group")).isOne();

		cancelAwait(subscription);
	}

	@Test // DATAREDIS-1079
	void shouldReceiveAndAckMessagesInConsumerGroup() throws InterruptedException {

		StreamMessageListenerContainer<String, MapRecord<String, String, String>> container = StreamMessageListenerContainer
				.create(connectionFactory, containerOptions);
		BlockingQueue<MapRecord<String, String, String>> queue = new LinkedBlockingQueue<>();
		RecordId messageId = redisTemplate.opsForStream().add("my-stream", Collections.singletonMap("key", "value1"));
		redisTemplate.opsForStream().createGroup("my-stream", ReadOffset.from(messageId), "my-group");

		container.start();
		Subscription subscription = container.receiveAutoAck(Consumer.from("my-group", "my-consumer"),
				StreamOffset.create("my-stream", ReadOffset.lastConsumed()), queue::add);

		subscription.await(DEFAULT_TIMEOUT);

		redisTemplate.opsForStream().add("my-stream", Collections.singletonMap("key", "value2"));

		MapRecord<String, String, String> message = queue.poll(1, TimeUnit.SECONDS);
		assertThat(message).isNotNull();
		assertThat(message.getValue()).containsEntry("key", "value2");

		assertThat(getNumberOfPending("my-stream", "my-group")).isZero();

		cancelAwait(subscription);
	}

	@Test // DATAREDIS-864
	void shouldUseCustomErrorHandler() throws InterruptedException {

		BlockingQueue<Throwable> failures = new LinkedBlockingQueue<>();

		StreamMessageListenerContainerOptions<String, MapRecord<String, String, String>> containerOptions = StreamMessageListenerContainerOptions
				.builder().errorHandler(failures::add).pollTimeout(Duration.ofMillis(100)).build();
		StreamMessageListenerContainer<String, MapRecord<String, String, String>> container = StreamMessageListenerContainer
				.create(connectionFactory, containerOptions);

		container.start();
		Subscription subscription = container.receive(Consumer.from("my-group", "my-consumer"),
				StreamOffset.create("my-stream", ReadOffset.lastConsumed()), it -> {});

		subscription.await(DEFAULT_TIMEOUT);

		Throwable error = failures.poll(1, TimeUnit.SECONDS);
		assertThat(failures).isEmpty();
		assertThat(error).isNotNull();

		cancelAwait(subscription);
	}

	@Test // DATAREDIS-864
	void errorShouldStopListening() throws InterruptedException {

		BlockingQueue<Throwable> failures = new LinkedBlockingQueue<>();

		StreamMessageListenerContainer<String, MapRecord<String, String, String>> container = StreamMessageListenerContainer
				.create(connectionFactory, containerOptions);

		StreamReadRequest<String> readRequest = StreamReadRequest
				.builder(StreamOffset.create("my-stream", ReadOffset.lastConsumed())).errorHandler(failures::add)
				.consumer(Consumer.from("my-group", "my-consumer")).build();

		RecordId messageId = redisTemplate.opsForStream().add("my-stream", Collections.singletonMap("key", "value1"));
		redisTemplate.opsForStream().createGroup("my-stream", ReadOffset.from(messageId), "my-group");

		container.start();
		Subscription subscription = container.register(readRequest, it -> {});
		subscription.await(Duration.ofSeconds(1));

		redisTemplate.delete("my-stream");

		assertThat(failures.poll(3, TimeUnit.SECONDS)).isNotNull();

		Awaitility.await().until(() -> !subscription.isActive());
		assertThat(subscription.isActive()).isFalse();

		cancelAwait(subscription);
	}

	@Test // DATAREDIS-864
	void customizedCancelPredicateShouldNotStopListening() throws InterruptedException {

		BlockingQueue<Throwable> failures = new LinkedBlockingQueue<>();

		StreamMessageListenerContainer<String, MapRecord<String, String, String>> container = StreamMessageListenerContainer
				.create(connectionFactory, containerOptions);

		StreamReadRequest<String> readRequest = StreamReadRequest
				.builder(StreamOffset.create("my-stream", ReadOffset.lastConsumed())) //
				.errorHandler(failures::add) //
				.cancelOnError(t -> false) //
				.consumer(Consumer.from("my-group", "my-consumer")) //
				.build();

		RecordId messageId = redisTemplate.opsForStream().add("my-stream", Collections.singletonMap("key", "value1"));
		redisTemplate.opsForStream().createGroup("my-stream", ReadOffset.from(messageId), "my-group");

		container.start();
		Subscription subscription = container.register(readRequest, it -> {});

		subscription.await(DEFAULT_TIMEOUT);

		redisTemplate.delete("my-stream");

		assertThat(failures.poll(1, TimeUnit.SECONDS)).isNotNull();
		assertThat(failures.poll(1, TimeUnit.SECONDS)).isNotNull();
		assertThat(subscription.isActive()).isTrue();

		cancelAwait(subscription);
	}

	@Test // DATAREDIS-1230
	void deserializationShouldContinueStreamRead() throws InterruptedException {

		StreamMessageListenerContainerOptions<String, ObjectRecord<String, Long>> containerOptions = StreamMessageListenerContainerOptions
				.builder().batchSize(1).pollTimeout(Duration.ofMillis(100)).targetType(Long.class).build();

		BlockingQueue<ObjectRecord<String, Long>> records = new LinkedBlockingQueue<>();
		BlockingQueue<Throwable> failures = new LinkedBlockingQueue<>();

		StreamMessageListenerContainer<String, ObjectRecord<String, Long>> container = StreamMessageListenerContainer
				.create(connectionFactory, containerOptions);

		StreamReadRequest<String> readRequest = StreamReadRequest
				.builder(StreamOffset.create("my-stream", ReadOffset.from("0-0"))) //
				.errorHandler(failures::add) //
				.cancelOnError(t -> false) //
				.build();

		redisTemplate.opsForStream().add("my-stream", Collections.singletonMap("payload", "1"));
		redisTemplate.opsForStream().add("my-stream", Collections.singletonMap("payload", "foo"));
		redisTemplate.opsForStream().add("my-stream", Collections.singletonMap("payload", "3"));

		container.start();
		Subscription subscription = container.register(readRequest, records::add);

		subscription.await(DEFAULT_TIMEOUT);

		ObjectRecord<String, Long> first = records.poll(1, TimeUnit.SECONDS);
		Throwable conversionFailure = failures.poll(1, TimeUnit.SECONDS);
		ObjectRecord<String, Long> third = records.poll(1, TimeUnit.SECONDS);

		assertThat(first).isNotNull();
		assertThat(first.getValue()).isEqualTo(1L);

		assertThat(conversionFailure).isInstanceOf(ConversionFailedException.class)
				.hasCauseInstanceOf(ConversionFailedException.class).hasRootCauseInstanceOf(NumberFormatException.class);
		assertThat(((ConversionFailedException) conversionFailure).getValue()).isInstanceOf(ByteRecord.class);

		assertThat(third).isNotNull();
		assertThat(third.getValue()).isEqualTo(3L);

		assertThat(subscription.isActive()).isTrue();

		cancelAwait(subscription);
	}

	@Test // DATAREDIS-864
	void cancelledStreamShouldNotReceiveMessages() throws InterruptedException {

		StreamMessageListenerContainer<String, MapRecord<String, String, String>> container = StreamMessageListenerContainer
				.create(connectionFactory, containerOptions);
		BlockingQueue<MapRecord<String, String, String>> queue = new LinkedBlockingQueue<>();

		container.start();
		Subscription subscription = container.receive(StreamOffset.create("my-stream", ReadOffset.from("0-0")), queue::add);

		subscription.await(DEFAULT_TIMEOUT);
		cancelAwait(subscription);

		redisTemplate.opsForStream().add("my-stream", Collections.singletonMap("key", "value4"));

		assertThat(queue.poll(200, TimeUnit.MILLISECONDS)).isNull();
	}

	@Test // DATAREDIS-864
	void containerRestartShouldRestartSubscription() throws InterruptedException {

		StreamMessageListenerContainer<String, MapRecord<String, String, String>> container = StreamMessageListenerContainer
				.create(connectionFactory, containerOptions);
		BlockingQueue<MapRecord<String, String, String>> queue = new LinkedBlockingQueue<>();

		container.start();
		Subscription subscription = container.receive(StreamOffset.create("my-stream", ReadOffset.from("0-0")), queue::add);

		subscription.await(DEFAULT_TIMEOUT);

		container.stop();

		Awaitility.await().atMost(DEFAULT_TIMEOUT).until(() -> !subscription.isActive());

		container.start();

		subscription.await(DEFAULT_TIMEOUT);

		redisTemplate.opsForStream().add("my-stream", Collections.singletonMap("key", "value1"));

		assertThat(queue.poll(1, TimeUnit.SECONDS)).isNotNull();

		cancelAwait(subscription);
	}

    @Test // GH-3208
    void defaultPhaseShouldBeMaxValue() {
        StreamMessageListenerContainer<String, MapRecord<String, String, String>> container = StreamMessageListenerContainer
                .create(connectionFactory, containerOptions);

        assertThat(container.getPhase()).isEqualTo(Integer.MAX_VALUE);
    }

    @Test // GH-3208
    void shouldApplyConfiguredPhase() {
        StreamMessageListenerContainerOptions<String, MapRecord<String, String, String>> options = StreamMessageListenerContainerOptions.builder()
                .phase(3208)
                .build();
        StreamMessageListenerContainer<String, MapRecord<String, String, String>> container = StreamMessageListenerContainer
                .create(connectionFactory, options);

        assertThat(container.getPhase()).isEqualTo(3208);
    }

    @Test // GH-3208
    void defaultAutoStartupShouldBeFalse() {
        StreamMessageListenerContainer<String, MapRecord<String, String, String>> container = StreamMessageListenerContainer
                .create(connectionFactory, containerOptions);

        assertThat(container.isAutoStartup()).isEqualTo(false);
    }

    @Test // GH-3208
    void shouldApplyConfiguredAutoStartup() {
        StreamMessageListenerContainerOptions<String, MapRecord<String, String, String>> options = StreamMessageListenerContainerOptions.builder()
                .autoStartup(true)
                .build();
        StreamMessageListenerContainer<String, MapRecord<String, String, String>> container = StreamMessageListenerContainer
                .create(connectionFactory, options);

        assertThat(container.isAutoStartup()).isEqualTo(true);
    }

	private static void cancelAwait(Subscription subscription) {

		subscription.cancel();

		Awaitility.await().atMost(DEFAULT_TIMEOUT).until(() -> !subscription.isActive());
	}

	private int getNumberOfPending(String stream, String group) {

		RedisConnection connection = connectionFactory.getConnection();

		if (connection instanceof LettuceConnection lettuce) {

			String value = ((List) lettuce.execute("XPENDING",
					new NestedMultiOutput<>(StringCodec.UTF8), new byte[][] { stream.getBytes(), group.getBytes() })).get(0)
							.toString();
			return NumberUtils.parseNumber(value, Integer.class);
		}

		String value = ((List) connectionFactory.getConnection().execute("XPENDING", stream.getBytes(), group.getBytes()))
				.get(0).toString();
		return NumberUtils.parseNumber(value, Integer.class);
	}

	static class LoginEvent {

		String firstName, lastName;

		LoginEvent(String firstName, String lastName) {
			this.firstName = firstName;
			this.lastName = lastName;
		}

		public String getFirstName() {
			return this.firstName;
		}

		public void setFirstName(String firstName) {
			this.firstName = firstName;
		}

		public String getLastName() {
			return this.lastName;
		}

		public void setLastName(String lastName) {
			this.lastName = lastName;
		}

		@Override
		public boolean equals(Object obj) {

			if (this == obj) {
				return true;
			}

			if (!(obj instanceof LoginEvent that)) {
				return false;
			}

			return Objects.equals(this.getFirstName(), that.getFirstName())
				&& Objects.equals(this.getLastName(), that.getLastName());
		}

		@Override
		public int hashCode() {
			return Objects.hash(getFirstName(), getLastName());
		}

		@Override
		public String toString() {

			return "LoginEvent{" +
				"firstname='" + firstName + '\'' +
				", lastname='" + lastName + '\'' +
				'}';
		}
	}
}
