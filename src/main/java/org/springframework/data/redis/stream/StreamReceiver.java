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

import org.springframework.data.redis.connection.RedisStreamCommands.MapRecord;
import reactor.core.publisher.Flux;

import java.nio.ByteBuffer;
import java.time.Duration;

import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStreamCommands.Consumer;
import org.springframework.data.redis.connection.RedisStreamCommands.ReadOffset;
import org.springframework.data.redis.connection.RedisStreamCommands.StreamOffset;
import org.springframework.data.redis.serializer.RedisSerializationContext.SerializationPair;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.util.Assert;

/**
 * A receiver to consume Redis Streams using reactive infrastructure.
 * <p/>
 * Once created, a {@link StreamReceiver} can subscribe to a Redis Stream and consume incoming {@link org.springframework.data.redis.connection.RedisStreamCommands.Record
 * messages}. Consider a {@link Flux} of {@link Record} infinite. Cancelling the
 * {@link org.reactivestreams.Subscription} terminates eventually background polling. Messages are converted using
 * {@link SerializationPair key and value serializers} to support various serialization strategies. <br/>
 * {@link StreamReceiver} supports three modes of stream consumption:
 * <ul>
 * <li>Standalone</li>
 * <li>Using a {@link Consumer} with external
 * {@link org.springframework.data.redis.core.ReactiveStreamOperations#acknowledge(Object, String, String...)
 * acknowledge}</li>
 * <li>Using a {@link Consumer} with auto-acknowledge</li>
 * </ul>
 * Reading from a stream requires polling and a strategy to advance stream offsets. Depending on the initial
 * {@link ReadOffset}, {@link StreamReceiver} applies an individual strategy to obtain the next {@link ReadOffset}:
 * <br/>
 * <strong>Standalone</strong>
 * <ul>
 * <li>{@link ReadOffset#from(String)} Offset using a particular message Id: Start with the given offset and use the
 * last seen {@link Record#getId() message Id}.</li>
 * <li>{@link ReadOffset#lastConsumed()} Last consumed: Start with the latest offset ({@code $}) and use the last seen
 * {@link Record#getId() message Id}.</li>
 * <li>{@link ReadOffset#latest()} Last consumed: Start with the latest offset ({@code $}) and use latest offset
 * ({@code $}) for subsequent reads.</li>
 * </ul>
 * <br/>
 * <strong>Using {@link Consumer}</strong>
 * <ul>
 * <li>{@link ReadOffset#from(String)} Offset using a particular message Id: Start with the given offset and use the
 * last seen {@link Record#getId() message Id}.</li>
 * <li>{@link ReadOffset#lastConsumed()} Last consumed: Start with the last consumed message by the consumer ({@code >})
 * and use the last consumed message by the consumer ({@code >}) for subsequent reads.</li>
 * <li>{@link ReadOffset#latest()} Last consumed: Start with the latest offset ({@code $}) and use latest offset
 * ({@code $}) for subsequent reads.</li>
 * </ul>
 * <strong>Note: Using {@link ReadOffset#latest()} bears the chance of dropped messages as messages can arrive in the
 * time during polling is suspended. Use messagedId's as offset or {@link ReadOffset#lastConsumed()} to minimize the
 * chance of message loss.</strong>
 * <p/>
 * See the following example code how to use {@link StreamReceiver}:
 *
 * <pre class="code">
 * ReactiveRedisConnectionFactory factory = …;
 *
 * StreamReceiver<String, String> receiver = StreamReceiver.create(factory);
 * Flux<StreamMessage<String, String>> messages = receiver.receive(StreamOffset.create("my-stream", ReadOffset.from("0-0")));
 *
 * messageFlux.doOnNext(message -> …);
 * </pre>
 *
 * @author Mark Paluch
 * @param <K> Stream key and Stream field type.
 * @param <V> Stream value type.
 * @since 2.2
 * @see StreamReceiverOptions#builder()
 * @see org.springframework.data.redis.core.ReactiveStreamOperations
 * @see ReactiveRedisConnectionFactory
 * @see StreamMessageListenerContainer
 */
public interface StreamReceiver<K, HK, HV> {

	/**
	 * Create a new {@link StreamReceiver} using {@link StringRedisSerializer string serializers} given
	 * {@link ReactiveRedisConnectionFactory}.
	 *
	 * @param connectionFactory must not be {@literal null}.
	 * @return the new {@link StreamReceiver}.
	 */
	static StreamReceiver<String, String, String> create(ReactiveRedisConnectionFactory connectionFactory) {

		Assert.notNull(connectionFactory, "ReactiveRedisConnectionFactory must not be null!");

		SerializationPair<String> serializationPair = SerializationPair.fromSerializer(StringRedisSerializer.UTF_8);
		return create(connectionFactory, StreamReceiverOptions.builder().serializer(serializationPair).build());
	}

	/**
	 * Create a new {@link StreamReceiver} given {@link ReactiveRedisConnectionFactory} and {@link StreamReceiverOptions}.
	 *
	 * @param connectionFactory must not be {@literal null}.
	 * @param options must not be {@literal null}.
	 * @return the new {@link StreamReceiver}.
	 */
	static <K, HK, HV> StreamReceiver<K, HK, HV> create(ReactiveRedisConnectionFactory connectionFactory,
			StreamReceiverOptions<K, HK, HV> options) {

		Assert.notNull(connectionFactory, "ReactiveRedisConnectionFactory must not be null!");
		Assert.notNull(options, "StreamReceiverOptions must not be null!");

		return new DefaultStreamReceiver<>(connectionFactory, options);
	}

	/**
	 * Starts a Redis Stream consumer that consumes {@link Record messages} from the {@link StreamOffset stream}.
	 * Messages are consumed from Redis and delivered on the returned {@link Flux} when requests are made on the Flux. The
	 * receiver is closed when the returned {@link Flux} terminates.
	 * <p/>
	 * Every message must be acknowledged using
	 * {@link org.springframework.data.redis.connection.ReactiveStreamCommands#xAck(ByteBuffer, String, String...)}
	 *
	 * @param streamOffset the stream along its offset.
	 * @return Flux of inbound {@link Record}s.
	 * @see StreamOffset#create(Object, ReadOffset)
	 */
	Flux<MapRecord<K, HK, HV>> receive(StreamOffset<K> streamOffset);

	/**
	 * Starts a Redis Stream consumer that consumes {@link Record messages} from the {@link StreamOffset stream}.
	 * Messages are consumed from Redis and delivered on the returned {@link Flux} when requests are made on the Flux. The
	 * receiver is closed when the returned {@link Flux} terminates.
	 * <p/>
	 * Every message is acknowledged when received.
	 *
	 * @param consumer consumer group, must not be {@literal null}.
	 * @param streamOffset the stream along its offset.
	 * @return Flux of inbound {@link Record}s.
	 * @see StreamOffset#create(Object, ReadOffset)
	 * @see ReadOffset#lastConsumed()
	 */
	Flux<MapRecord<K, HK, HV>> receiveAutoAck(Consumer consumer, StreamOffset<K> streamOffset);

	/**
	 * Starts a Redis Stream consumer that consumes {@link Record messages} from the {@link StreamOffset stream}.
	 * Messages are consumed from Redis and delivered on the returned {@link Flux} when requests are made on the Flux. The
	 * receiver is closed when the returned {@link Flux} terminates.
	 * <p/>
	 * Every message must be acknowledged using
	 * {@link org.springframework.data.redis.core.ReactiveStreamOperations#acknowledge(Object, String, String...)} after
	 * processing.
	 *
	 * @param consumer consumer group, must not be {@literal null}.
	 * @param streamOffset the stream along its offset.
	 * @return Flux of inbound {@link Record}s.
	 * @see StreamOffset#create(Object, ReadOffset)
	 * @see ReadOffset#lastConsumed()
	 */
	Flux<MapRecord<K, HK, HV>> receive(Consumer consumer, StreamOffset<K> streamOffset);

	/**
	 * Options for {@link StreamReceiver}.
	 *
	 * @param <K> Stream key and Stream field type.
	 * @param <V> Stream value type.
	 * @see StreamReceiverOptionsBuilder
	 */
	class StreamReceiverOptions<K, HK, HV> {

		private final Duration pollTimeout;
		private final int batchSize;
		private final SerializationPair<K> keySerializer;
		private final SerializationPair<HK> bodySerializer;
		private final SerializationPair<HV> vaueSerializer;

		private StreamReceiverOptions(Duration pollTimeout, int batchSize, SerializationPair<K> keySerializer,
				SerializationPair<HK> bodySerializer, SerializationPair<HV> valueSerializer) {
			this.pollTimeout = pollTimeout;
			this.batchSize = batchSize;
			this.keySerializer = keySerializer;
			this.bodySerializer = bodySerializer;
			this.vaueSerializer = valueSerializer;
		}

		/**
		 * @return a new builder for {@link StreamReceiverOptions}.
		 */
		static StreamReceiverOptionsBuilder<String, String, String> builder() {

			SerializationPair<String> serializer = SerializationPair.fromSerializer(StringRedisSerializer.UTF_8);
			return new StreamReceiverOptionsBuilder<>().serializer(serializer);
		}

		/**
		 * Timeout for blocking polling using the {@code BLOCK} option during reads.
		 *
		 * @return
		 */
		public Duration getPollTimeout() {
			return pollTimeout;
		}

		/**
		 * Batch size polling using the {@code COUNT} option during reads.
		 *
		 * @return
		 */
		public int getBatchSize() {
			return batchSize;
		}

		public SerializationPair<K> getKeySerializer() {
			return keySerializer;
		}

		public SerializationPair<HK> getBodySerializer() {
			return bodySerializer;
		}
	}

	/**
	 * Builder for {@link StreamReceiverOptions}.
	 *
	 * @param <K> Stream key and Stream field type.
	 */
	class StreamReceiverOptionsBuilder<K, HK, HV> {

		private Duration pollTimeout = Duration.ofSeconds(2);
		private int batchSize = 1;
		private SerializationPair<K> keySerializer;
		private SerializationPair<HK> bodySerializer;
		private SerializationPair<HV> valueSerializer;

		private StreamReceiverOptionsBuilder() {}

		/**
		 * Configure a poll timeout for the {@code BLOCK} option during reading.
		 *
		 * @param pollTimeout must not be {@literal null} or negative.
		 * @return {@code this} {@link StreamReceiverOptionsBuilder}.
		 */
		public StreamReceiverOptionsBuilder<K, HK, HV> pollTimeout(Duration pollTimeout) {

			Assert.notNull(pollTimeout, "Poll timeout must not be null!");
			Assert.isTrue(!pollTimeout.isNegative(), "Poll timeout must not be negative!");

			this.pollTimeout = pollTimeout;
			return this;
		}

		/**
		 * Configure a batch size for the {@code COUNT} option during reading.
		 *
		 * @param messagesPerPoll must not be greater zero.
		 * @return {@code this} {@link StreamReceiverOptionsBuilder}.
		 */
		public StreamReceiverOptionsBuilder<K, HK, HV> batchSize(int messagesPerPoll) {

			Assert.isTrue(messagesPerPoll > 0, "Batch size must be greater zero!");

			this.batchSize = messagesPerPoll;
			return this;
		}

		/**
		 * Configure a key and value serializer.
		 *
		 * @param pair must not be {@literal null}.
		 * @return {@code this} {@link StreamReceiverOptionsBuilder}.
		 */
		public <T> StreamReceiverOptionsBuilder<T, T, T> serializer(SerializationPair<T> pair) {

			this.keySerializer = (SerializationPair) pair;
			this.bodySerializer = (SerializationPair) pair;
			this.valueSerializer = (SerializationPair) pair;
			return (StreamReceiverOptionsBuilder) this;
		}

		/**
		 * Configure a key serializer.
		 *
		 * @param pair must not be {@literal null}.
		 * @return {@code this} {@link StreamReceiverOptionsBuilder}.
		 */
		public <NK> StreamReceiverOptionsBuilder<NK, HK, HV> keySerializer(SerializationPair<NK> pair) {

			this.keySerializer = (SerializationPair) pair;
			return (StreamReceiverOptionsBuilder) this;
		}

		/**
		 * Configure a value serializer.
		 *
		 * @param pair must not be {@literal null}.
		 * @return {@code this} {@link StreamReceiverOptionsBuilder}.
		 */
		public <NV> StreamReceiverOptionsBuilder<K, HK, HV> bodySerializer(SerializationPair<NV> pair) {

			this.bodySerializer = (SerializationPair) pair;
			return (StreamReceiverOptionsBuilder) this;
		}

		/**
		 * Build new {@link StreamReceiverOptions}.
		 *
		 * @return new {@link StreamReceiverOptions}.
		 */
		public StreamReceiverOptions<K, HK, HV> build() {
			return new StreamReceiverOptions<>(pollTimeout, batchSize, keySerializer, bodySerializer, valueSerializer);
		}
	}
}
