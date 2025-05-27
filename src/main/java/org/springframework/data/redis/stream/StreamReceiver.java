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

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.OptionalInt;
import java.util.function.Function;

import org.jspecify.annotations.Nullable;
import org.reactivestreams.Publisher;

import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ObjectRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.Record;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.hash.HashMapper;
import org.springframework.data.redis.hash.ObjectHashMapper;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.data.redis.serializer.RedisSerializationContext.SerializationPair;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.util.Assert;

/**
 * A receiver to consume Redis Streams using reactive infrastructure.
 * <p>
 * Once created, a {@link StreamReceiver} can subscribe to a Redis Stream and consume incoming {@link Record records}.
 * Consider a {@link Flux} of {@link Record} infinite. Cancelling the {@link org.reactivestreams.Subscription}
 * terminates eventually background polling. Records are converted using {@link SerializationPair key and value
 * serializers} to support various serialization strategies. <br/>
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
 * <li>{@link ReadOffset#from(String)} Offset using a particular record Id: Start with the given offset and use the last
 * seen {@link Record#getId() record Id}.</li>
 * <li>{@link ReadOffset#lastConsumed()} Last consumed: Start with the latest offset ({@code $}) and use the last seen
 * {@link Record#getId() record Id}.</li>
 * <li>{@link ReadOffset#latest()} Last consumed: Start with the latest offset ({@code $}) and use latest offset
 * ({@code $}) for subsequent reads.</li>
 * </ul>
 * <br/>
 * <strong>Using {@link Consumer}</strong>
 * <ul>
 * <li>{@link ReadOffset#from(String)} Offset using a particular record Id: Start with the given offset and use the last
 * seen {@link Record#getId() record Id}.</li>
 * <li>{@link ReadOffset#lastConsumed()} Last consumed: Start with the last consumed record by the consumer ({@code >})
 * and use the last consumed record by the consumer ({@code >}) for subsequent reads.</li>
 * <li>{@link ReadOffset#latest()} Last consumed: Start with the latest offset ({@code $}) and use latest offset
 * ({@code $}) for subsequent reads.</li>
 * </ul>
 * <strong>Note: Using {@link ReadOffset#latest()} bears the chance of dropped records as records can arrive in the time
 * during polling is suspended. Use recordId's as offset or {@link ReadOffset#lastConsumed()} to minimize the chance of
 * record loss.</strong>
 * <p>
 * {@link StreamReceiver} propagates errors during stream reads and deserialization as terminal error signal by default.
 * Configuring a {@link StreamReceiverOptions#getResumeFunction() resume function} allows conditional resumption by
 * dropping the record or by propagating the error to terminate the subscription.
 * <p>
 * See the following example code how to use {@link StreamReceiver}:
 *
 * <pre class="code">
 * ReactiveRedisConnectionFactory factory = …;
 *
 * StreamReceiver&lt;String, String, String&gt; receiver = StreamReceiver.create(factory);
 * Flux&lt;MapRecord&lt;String, String, String&gt;&gt; records = receiver.receive(StreamOffset.fromStart("my-stream"));
 *
 * recordFlux.doOnNext(record -&gt; …);
 * </pre>
 *
 * @author Mark Paluch
 * @author Eddie McDaniel
 * @param <K> Stream key and Stream field type.
 * @param <V> Stream value type.
 * @since 2.2
 * @see StreamReceiverOptions#builder()
 * @see org.springframework.data.redis.core.ReactiveStreamOperations
 * @see ReactiveRedisConnectionFactory
 * @see StreamMessageListenerContainer
 */
public interface StreamReceiver<K, V extends Record<K, ?>> {

	/**
	 * Create a new {@link StreamReceiver} using {@link StringRedisSerializer string serializers} given
	 * {@link ReactiveRedisConnectionFactory}.
	 *
	 * @param connectionFactory must not be {@literal null}.
	 * @return the new {@link StreamReceiver}.
	 */
	static StreamReceiver<String, MapRecord<String, String, String>> create(
			ReactiveRedisConnectionFactory connectionFactory) {

		Assert.notNull(connectionFactory, "ReactiveRedisConnectionFactory must not be null");

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
	static <K, V extends Record<K, ?>> StreamReceiver<K, V> create(ReactiveRedisConnectionFactory connectionFactory,
			StreamReceiverOptions<K, V> options) {

		Assert.notNull(connectionFactory, "ReactiveRedisConnectionFactory must not be null");
		Assert.notNull(options, "StreamReceiverOptions must not be null");

		return new DefaultStreamReceiver<>(connectionFactory, options);
	}

	/**
	 * Starts a Redis Stream consumer that consumes {@link Record records} from the {@link StreamOffset stream}. Records
	 * are consumed from Redis and delivered on the returned {@link Flux} when requests are made on the Flux. The receiver
	 * is closed when the returned {@link Flux} terminates.
	 * <p>
	 * Every record must be acknowledged using
	 * {@link org.springframework.data.redis.connection.ReactiveStreamCommands#xAck(ByteBuffer, String, String...)}
	 *
	 * @param streamOffset the stream along its offset.
	 * @return Flux of inbound {@link Record}s.
	 * @see StreamOffset#create(Object, ReadOffset)
	 */
	Flux<V> receive(StreamOffset<K> streamOffset);

	/**
	 * Starts a Redis Stream consumer that consumes {@link Record records} from the {@link StreamOffset stream}. Records
	 * are consumed from Redis and delivered on the returned {@link Flux} when requests are made on the Flux. The receiver
	 * is closed when the returned {@link Flux} terminates.
	 * <p>
	 * Every record is acknowledged when received.
	 *
	 * @param consumer consumer group, must not be {@literal null}.
	 * @param streamOffset the stream along its offset.
	 * @return Flux of inbound {@link Record}s.
	 * @see StreamOffset#create(Object, ReadOffset)
	 * @see ReadOffset#lastConsumed()
	 */
	Flux<V> receiveAutoAck(Consumer consumer, StreamOffset<K> streamOffset);

	/**
	 * Starts a Redis Stream consumer that consumes {@link Record records} from the {@link StreamOffset stream}. Records
	 * are consumed from Redis and delivered on the returned {@link Flux} when requests are made on the Flux. The receiver
	 * is closed when the returned {@link Flux} terminates.
	 * <p>
	 * Every record must be acknowledged using
	 * {@link org.springframework.data.redis.core.ReactiveStreamOperations#acknowledge(Object, String, String...)} after
	 * processing.
	 *
	 * @param consumer consumer group, must not be {@literal null}.
	 * @param streamOffset the stream along its offset.
	 * @return Flux of inbound {@link Record}s.
	 * @see StreamOffset#create(Object, ReadOffset)
	 * @see ReadOffset#lastConsumed()
	 */
	Flux<V> receive(Consumer consumer, StreamOffset<K> streamOffset);

	/**
	 * Options for {@link StreamReceiver}.
	 *
	 * @param <K> Stream key and Stream field type.
	 * @param <V> Stream value type.
	 * @see StreamReceiverOptionsBuilder
	 */
	class StreamReceiverOptions<K, V extends Record<K, ?>> {

		private final Duration pollTimeout;
		private final @Nullable Integer batchSize;
		private final Function<? super Throwable, ? extends Publisher<Void>> resumeFunction;
		private final SerializationPair<K> keySerializer;
		private final SerializationPair<Object> hashKeySerializer;
		private final SerializationPair<Object> hashValueSerializer;
		private final @Nullable Class<Object> targetType;
		private final @Nullable HashMapper<Object, Object, Object> hashMapper;

		@SuppressWarnings({ "unchecked", "rawtypes" })
		private StreamReceiverOptions(Duration pollTimeout, @Nullable Integer batchSize,
				Function<? super Throwable, ? extends Publisher<Void>> resumeFunction, SerializationPair<K> keySerializer,
				SerializationPair<Object> hashKeySerializer, SerializationPair<Object> hashValueSerializer,
				@Nullable Class<?> targetType, @Nullable HashMapper<V, ?, ?> hashMapper) {

			this.pollTimeout = pollTimeout;
			this.batchSize = batchSize;
			this.resumeFunction = resumeFunction;
			this.keySerializer = keySerializer;
			this.hashKeySerializer = hashKeySerializer;
			this.hashValueSerializer = hashValueSerializer;
			this.targetType = (Class) targetType;
			this.hashMapper = (HashMapper) hashMapper;
		}

		/**
		 * @return a new builder for {@link StreamReceiverOptions}.
		 */
		public static StreamReceiverOptionsBuilder<String, MapRecord<String, String, String>> builder() {

			SerializationPair<String> serializer = SerializationPair.fromSerializer(StringRedisSerializer.UTF_8);
			return new StreamReceiverOptionsBuilder<>().serializer(serializer);
		}

		/**
		 * @return a new builder for {@link StreamReceiverOptions}.
		 */
		@SuppressWarnings("unchecked")
		public static <T> StreamReceiverOptionsBuilder<String, ObjectRecord<String, T>> builder(
				HashMapper<T, byte[], byte[]> hashMapper) {

			SerializationPair<String> serializer = SerializationPair.fromSerializer(StringRedisSerializer.UTF_8);
			SerializationPair<ByteBuffer> raw = SerializationPair.raw();
			return new StreamReceiverOptionsBuilder<>().keySerializer(serializer).hashKeySerializer(raw)
					.hashValueSerializer(raw).objectMapper(hashMapper);
		}

		/**
		 * Timeout for blocking polling using the {@code BLOCK} option during reads.
		 *
		 * @return the actual timeout.
		 */
		public Duration getPollTimeout() {
			return pollTimeout;
		}

		/**
		 * Batch size polling using the {@code COUNT} option during reads.
		 *
		 * @return the batch size.
		 */
		public OptionalInt getBatchSize() {
			return batchSize != null ? OptionalInt.of(batchSize) : OptionalInt.empty();
		}

		public Function<? super Throwable, ? extends Publisher<Void>> getResumeFunction() {
			return resumeFunction;
		}

		public SerializationPair<K> getKeySerializer() {
			return keySerializer;
		}

		public SerializationPair<Object> getHashKeySerializer() {
			return hashKeySerializer;
		}

		public SerializationPair<Object> getHashValueSerializer() {
			return hashValueSerializer;
		}

		public @Nullable HashMapper<Object, Object, Object> getHashMapper() {
			return hashMapper;
		}

		@SuppressWarnings("NullAway")
		public HashMapper<Object, Object, Object> getRequiredHashMapper() {

			if (!hasHashMapper()) {
				throw new IllegalStateException("No HashMapper configured");
			}

			return hashMapper;
		}

		public boolean hasHashMapper() {
			return this.hashMapper != null;
		}

		public Class<Object> getTargetType() {

			if (this.targetType != null) {
				return targetType;
			}

			return Object.class;
		}

	}

	/**
	 * Builder for {@link StreamReceiverOptions}.
	 *
	 * @param <K> Stream key and Stream field type.
	 */
	class StreamReceiverOptionsBuilder<K, V extends Record<K, ?>> {

		private Duration pollTimeout = Duration.ofSeconds(2);
		private @Nullable Integer batchSize;
		private SerializationPair<K> keySerializer;
		private SerializationPair<Object> hashKeySerializer;
		private SerializationPair<Object> hashValueSerializer;
		private Function<? super Throwable, ? extends Publisher<Void>> resumeFunction = Mono::error;
		private @Nullable HashMapper<V, ?, ?> hashMapper;
		private @Nullable Class<?> targetType;

		@SuppressWarnings("NullAway")
		private StreamReceiverOptionsBuilder() {}

		/**
		 * Configure a poll timeout for the {@code BLOCK} option during reading.
		 *
		 * @param pollTimeout must not be {@literal null} or negative.
		 * @return {@code this} {@link StreamReceiverOptionsBuilder}.
		 */
		public StreamReceiverOptionsBuilder<K, V> pollTimeout(Duration pollTimeout) {

			Assert.notNull(pollTimeout, "Poll timeout must not be null");
			Assert.isTrue(!pollTimeout.isNegative(), "Poll timeout must not be negative");

			this.pollTimeout = pollTimeout;
			return this;
		}

		/**
		 * Configure a batch size for the {@code COUNT} option during reading.
		 *
		 * @param recordsPerPoll must be greater zero.
		 * @return {@code this} {@link StreamReceiverOptionsBuilder}.
		 */
		public StreamReceiverOptionsBuilder<K, V> batchSize(int recordsPerPoll) {

			Assert.isTrue(recordsPerPoll > 0, "Batch size must be greater zero");

			this.batchSize = recordsPerPoll;
			return this;
		}

		/**
		 * Configure a resume {@link Function} to resume the main sequence when polling the stream fails. The function can
		 * either resume by suppressing the error or fail the main sequence by emitting the error to stop receiving. Receive
		 * errors (Redis errors, Serialization failures) stop receiving by default.
		 *
		 * @param resumeFunction must not be {@literal null}.
		 * @return {@code this} {@link StreamReceiverOptionsBuilder}.
		 * @since 2.x
		 * @see Flux#onErrorResume(Function)
		 */
		public StreamReceiverOptionsBuilder<K, V> onErrorResume(
				Function<? super Throwable, ? extends Publisher<Void>> resumeFunction) {

			Assert.notNull(resumeFunction, "Resume function must not be null");

			this.resumeFunction = resumeFunction;
			return this;
		}

		/**
		 * Configure a key, hash key and hash value serializer.
		 *
		 * @param pair must not be {@literal null}.
		 * @return {@code this} {@link StreamReceiverOptionsBuilder}.
		 */
		@SuppressWarnings({ "unchecked", "rawtypes" })
		public <T> StreamReceiverOptionsBuilder<T, MapRecord<T, T, T>> serializer(SerializationPair<T> pair) {

			Assert.notNull(pair, "SerializationPair must not be null");

			this.keySerializer = (SerializationPair) pair;
			this.hashKeySerializer = (SerializationPair) pair;
			this.hashValueSerializer = (SerializationPair) pair;
			return (StreamReceiverOptionsBuilder) this;
		}

		/**
		 * Configure a key, hash key and hash value serializer.
		 *
		 * @param serializationContext must not be {@literal null}.
		 * @return {@code this} {@link StreamReceiverOptionsBuilder}.
		 */
		@SuppressWarnings({ "unchecked", "rawtypes" })
		public <T> StreamReceiverOptionsBuilder<T, MapRecord<T, T, T>> serializer(
				RedisSerializationContext<T, ?> serializationContext) {

			Assert.notNull(serializationContext, "RedisSerializationContext must not be null");

			this.keySerializer = (SerializationPair) serializationContext.getKeySerializationPair();
			this.hashKeySerializer = serializationContext.getHashKeySerializationPair();
			this.hashValueSerializer = serializationContext.getHashValueSerializationPair();

			return (StreamReceiverOptionsBuilder) this;
		}

		/**
		 * Configure a key serializer.
		 *
		 * @param pair must not be {@literal null}.
		 * @return {@code this} {@link StreamReceiverOptionsBuilder}.
		 */
		@SuppressWarnings({ "unchecked", "rawtypes" })
		public <NK, NV extends Record<NK, ?>> StreamReceiverOptionsBuilder<NK, NV> keySerializer(
				SerializationPair<NK> pair) {

			Assert.notNull(pair, "SerializationPair must not be null");

			this.keySerializer = (SerializationPair) pair;
			return (StreamReceiverOptionsBuilder) this;
		}

		/**
		 * Configure a hash key serializer.
		 *
		 * @param pair must not be {@literal null}.
		 * @return {@code this} {@link StreamReceiverOptionsBuilder}.
		 */
		@SuppressWarnings({ "unchecked", "rawtypes" })
		public <HK, HV> StreamReceiverOptionsBuilder<K, MapRecord<K, HK, HV>> hashKeySerializer(
				SerializationPair<HK> pair) {

			Assert.notNull(pair, "SerializationPair must not be null");

			this.hashKeySerializer = (SerializationPair) pair;
			return (StreamReceiverOptionsBuilder) this;
		}

		/**
		 * Configure a hash value serializer.
		 *
		 * @param pair must not be {@literal null}.
		 * @return {@code this} {@link StreamReceiverOptionsBuilder}.
		 */
		@SuppressWarnings({ "unchecked", "rawtypes" })
		public <HK, HV> StreamReceiverOptionsBuilder<K, MapRecord<K, HK, HV>> hashValueSerializer(
				SerializationPair<HV> pair) {

			Assert.notNull(pair, "SerializationPair must not be null");

			this.hashValueSerializer = (SerializationPair) pair;
			return (StreamReceiverOptionsBuilder) this;
		}

		/**
		 * Configure a hash target type. Changes the emitted {@link Record} type to {@link ObjectRecord}.
		 *
		 * @param targetType must not be {@literal null}.
		 * @return {@code this} {@link StreamReceiverOptionsBuilder}.
		 */
		@SuppressWarnings({ "unchecked", "rawtypes" })
		public <NV> StreamReceiverOptionsBuilder<K, ObjectRecord<K, NV>> targetType(Class<NV> targetType) {

			Assert.notNull(targetType, "Target type must not be null");

			this.targetType = targetType;

			if (this.hashMapper == null) {

				hashKeySerializer(SerializationPair.raw());
				hashValueSerializer(SerializationPair.raw());
				return (StreamReceiverOptionsBuilder) objectMapper(ObjectHashMapper.getSharedInstance());
			}

			return (StreamReceiverOptionsBuilder) this;
		}

		/**
		 * Configure a hash mapper. Changes the emitted {@link Record} type to {@link ObjectRecord}.
		 *
		 * @param hashMapper must not be {@literal null}.
		 * @return {@code this} {@link StreamReceiverOptionsBuilder}.
		 */
		@SuppressWarnings({ "unchecked", "rawtypes" })
		public <NV> StreamReceiverOptionsBuilder<K, ObjectRecord<K, NV>> objectMapper(HashMapper<NV, ?, ?> hashMapper) {

			Assert.notNull(hashMapper, "HashMapper must not be null");

			this.hashMapper = (HashMapper) hashMapper;
			return (StreamReceiverOptionsBuilder) this;
		}

		/**
		 * Build new {@link StreamReceiverOptions}.
		 *
		 * @return new {@link StreamReceiverOptions}.
		 */
		public StreamReceiverOptions<K, V> build() {
			return new StreamReceiverOptions<>(pollTimeout, batchSize, resumeFunction, keySerializer, hashKeySerializer,
					hashValueSerializer,
					targetType, hashMapper);
		}
	}
}
