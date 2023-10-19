/*
 * Copyright 2018-2023 the original author or authors.
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

import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Operators;
import reactor.util.concurrent.Queues;
import reactor.util.context.Context;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;

import org.springframework.core.convert.ConversionFailedException;
import org.springframework.core.convert.TypeDescriptor;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.connection.stream.ByteBufferRecord;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.Record;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.ReactiveStreamOperations;
import org.springframework.data.redis.serializer.RedisSerializationContext;

/**
 * Default implementation of {@link StreamReceiver}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.2
 */
class DefaultStreamReceiver<K, V extends Record<K, ?>> implements StreamReceiver<K, V> {

	private final Log logger = LogFactory.getLog(getClass());
	private final ReactiveRedisTemplate<K, ?> template;
	private final ReactiveStreamOperations<K, Object, Object> streamOperations;
	private final StreamReadOptions readOptions;
	private final StreamReceiverOptions<K, V> receiverOptions;

	/**
	 * Create a new {@link DefaultStreamReceiver} given {@link ReactiveRedisConnectionFactory} and
	 * {@link org.springframework.data.redis.stream.StreamReceiver.StreamReceiverOptions}.
	 *
	 * @param connectionFactory must not be {@literal null}.
	 * @param options must not be {@literal null}.
	 */
	@SuppressWarnings("unchecked")
	DefaultStreamReceiver(ReactiveRedisConnectionFactory connectionFactory, StreamReceiverOptions<K, V> options) {

		receiverOptions = options;

		RedisSerializationContext<K, Object> serializationContext = RedisSerializationContext
				.<K, Object> newSerializationContext(options.getKeySerializer()) //
				.key(options.getKeySerializer()).hashKey(options.getHashKeySerializer())
				.hashValue(options.getHashValueSerializer()).build();

		StreamReadOptions readOptions = StreamReadOptions.empty();

		if (options.getBatchSize().isPresent()) {
			readOptions = readOptions.count(options.getBatchSize().getAsInt());
		}
		if (!options.getPollTimeout().isZero()) {
			readOptions = readOptions.block(options.getPollTimeout());
		}

		this.readOptions = readOptions;
		this.template = new ReactiveRedisTemplate(connectionFactory, serializationContext);

		if (options.hasHashMapper()) {
			this.streamOperations = this.template.opsForStream(options.getRequiredHashMapper());
		} else {
			this.streamOperations = this.template.opsForStream();
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	public Flux<V> receive(StreamOffset<K> streamOffset) {

		if (logger.isDebugEnabled()) {
			logger.debug(String.format("receive(%s)", streamOffset));
		}

		RedisSerializationContext.SerializationPair<K> keySerializer = template.getSerializationContext()
				.getKeySerializationPair();
		ByteBuffer rawKey = keySerializer.write(streamOffset.getKey());

		Function<ReadOffset, Flux<ByteBufferRecord>> readFunction = readOffset -> template.execute(connection -> connection
				.streamCommands().xRead(readOptions, StreamOffset.create(rawKey.asReadOnlyBuffer(), readOffset)));

		return Flux.defer(() -> {

			PollState pollState = PollState.standalone(streamOffset.getOffset());
			return Flux.create(
					sink -> new StreamSubscription(sink, streamOffset.getKey(), pollState, readFunction,
							receiverOptions.getResumeFunction()).arm());
		});
	}

	@Override
	@SuppressWarnings("unchecked")
	public Flux<V> receiveAutoAck(Consumer consumer, StreamOffset<K> streamOffset) {

		if (logger.isDebugEnabled()) {
			logger.debug(String.format("receiveAutoAck(%s, %s)", consumer, streamOffset));
		}

		Function<ReadOffset, Flux<ByteBufferRecord>> readFunction = getConsumeReadFunction(streamOffset.getKey(), consumer,
				this.readOptions.autoAcknowledge());


		return Flux.defer(() -> {

			PollState pollState = PollState.consumer(consumer, streamOffset.getOffset());
			return Flux.create(
					sink -> new StreamSubscription(sink, streamOffset.getKey(), pollState, readFunction,
							receiverOptions.getResumeFunction()).arm());
		});
	}

	@Override
	@SuppressWarnings("unchecked")
	public Flux<V> receive(Consumer consumer, StreamOffset<K> streamOffset) {

		if (logger.isDebugEnabled()) {
			logger.debug(String.format("receive(%s, %s)", consumer, streamOffset));
		}

		Function<ReadOffset, Flux<ByteBufferRecord>> readFunction = getConsumeReadFunction(streamOffset.getKey(), consumer,
				this.readOptions);

		return Flux.defer(() -> {
			PollState pollState = PollState.consumer(consumer, streamOffset.getOffset());
			return Flux.create(
					sink -> new StreamSubscription(sink, streamOffset.getKey(), pollState, readFunction,
							receiverOptions.getResumeFunction()).arm());
		});
	}

	@SuppressWarnings("unchecked")
	private Function<ReadOffset, Flux<ByteBufferRecord>> getConsumeReadFunction(K key, Consumer consumer,
			StreamReadOptions readOptions) {

		RedisSerializationContext.SerializationPair<K> keySerializer = template.getSerializationContext()
				.getKeySerializationPair();
		ByteBuffer rawKey = keySerializer.write(key);

		return readOffset -> template.execute(connection -> connection.streamCommands().xReadGroup(consumer, readOptions,
				StreamOffset.create(rawKey.asReadOnlyBuffer(), readOffset)));
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private Function<ByteBufferRecord, V> getDeserializer() {

		Function<ByteBufferRecord, MapRecord<K, Object, Object>> deserializer = streamOperations::deserializeRecord;

		if (receiverOptions.getHashMapper() == null) {
			return (Function) deserializer;
		}

		return source -> {

			MapRecord<K, Object, Object> intermediate = deserializer.apply(source);
			return (V) streamOperations.map(intermediate, this.receiverOptions.getTargetType());
		};
	}

	/**
	 * A stateful Redis Stream subscription.
	 */
	class StreamSubscription {

		private final Queue<V> overflow = Queues.<V> small().get();

		private final FluxSink<V> sink;
		private final K key;
		private final PollState pollState;
		private final Function<ReadOffset, Flux<ByteBufferRecord>> readFunction;
		private final Function<? super Throwable, ? extends Publisher<Void>> resumeFunction;
		private final Function<ByteBufferRecord, V> deserializer;
		private final TypeDescriptor targetType;

		protected StreamSubscription(FluxSink<V> sink, K key, PollState pollState,
				Function<ReadOffset, Flux<ByteBufferRecord>> readFunction,
				Function<? super Throwable, ? extends Publisher<Void>> resumeFunction) {

			this.sink = sink;
			this.key = key;
			this.pollState = pollState;
			this.readFunction = readFunction;
			this.resumeFunction = resumeFunction;
			this.deserializer = getDeserializer();
			this.targetType = TypeDescriptor
					.valueOf(receiverOptions.hasHashMapper() ? receiverOptions.getTargetType() : MapRecord.class);
		}

		/**
		 * Arm the subscription so {@link Subscription#request(long) demand} activates polling.
		 */
		void arm() {

			sink.onRequest(toAdd -> {

				if (logger.isDebugEnabled()) {
					logger.debug(String.format("[stream: %s] onRequest(%d)", key, toAdd));
				}

				if (pollState.isSubscriptionActive()) {

					long r, u;
					for (;;) {
						r = pollState.getRequested();
						if (r == Long.MAX_VALUE) {
							scheduleIfRequired();
							return;
						}
						u = Operators.addCap(r, toAdd);
						if (pollState.setRequested(r, u)) {
							if (u > 0) {
								scheduleIfRequired();
							}
							return;
						}
					}
				} else {
					if (logger.isDebugEnabled()) {
						logger.debug(String.format("[stream: %s] onRequest(%d): Dropping, subscription canceled", key, toAdd));
					}
				}
			});

			sink.onCancel(pollState::cancel);
		}

		@SuppressWarnings({ "unchecked", "ConstantConditions" })
		private void scheduleIfRequired() {

			if (logger.isDebugEnabled()) {
				logger.debug(String.format("[stream: %s] scheduleIfRequired()", key));
			}
			if (pollState.isScheduled()) {
				if (logger.isDebugEnabled()) {
					logger.debug(String.format("[stream: %s] scheduleIfRequired(): Already scheduled", key));
				}
				return;
			}

			if (!pollState.isSubscriptionActive()) {
				if (logger.isDebugEnabled()) {
					logger.debug(String.format("[stream: %s] scheduleIfRequired(): Subscription cancelled", key));
				}
				return;
			}

			if (pollState.getRequested() > 0 && !overflow.isEmpty()) {
				if (logger.isDebugEnabled()) {
					logger.info(String.format("[stream: %s] scheduleIfRequired(): Requested: %d Emit from buffer", key,
							pollState.getRequested()));
				}
				emitBuffer();
			}

			if (pollState.getRequested() == 0) {

				if (logger.isDebugEnabled()) {
					logger.debug(String
							.format("[stream: %s] scheduleIfRequired(): Subscriber has no demand; Suspending subscription", key));
				}
				return;
			}

			if (pollState.getRequested() <= 0) {
				return;
			}

			if (pollState.activateSchedule()) {

				if (logger.isDebugEnabled()) {
					logger.debug(String.format("[stream: %s] scheduleIfRequired(): Activating subscription", key));
				}

				ReadOffset readOffset = pollState.getCurrentReadOffset();

				if (logger.isDebugEnabled()) {
					logger.debug(
							String.format("[stream: %s] scheduleIfRequired(): Activating subscription, offset %s", key, readOffset));
				}

				Flux<ByteBufferRecord> poll = readFunction.apply(readOffset)
						.onErrorResume(throwable -> Flux.from(resumeFunction.apply(throwable)).then().cast(ByteBufferRecord.class));

				poll.map(it -> {

					if (logger.isDebugEnabled()) {
						logger.debug(String.format("[stream: %s] onStreamMessage(%s)", key, it));
					}

					pollState.updateReadOffset(it.getId().getValue());

					try {
						return deserializer.apply(it);
					} catch (RuntimeException ex) {
						throw new ConversionFailedException(TypeDescriptor.forObject(it), targetType, it, ex);
					}
				}).onErrorResume(throwable -> Flux.from(resumeFunction.apply(throwable)).then().map(it -> (V) new Object())) //
						.subscribe(getSubscriber());
			}
		}

		private CoreSubscriber<V> getSubscriber() {

			return new CoreSubscriber<V>() {

				@Override
				public void onSubscribe(Subscription s) {
					s.request(Long.MAX_VALUE);
				}

				@Override
				public void onNext(V message) {
					onStreamMessage(message);
				}

				@Override
				public void onError(Throwable t) {
					onStreamError(t);
				}

				@Override
				public void onComplete() {

					if (logger.isDebugEnabled()) {
						logger.debug(String.format("[stream: %s] onComplete()", key));
					}

					pollState.scheduleCompleted();

					scheduleIfRequired();
				}

				@Override
				public Context currentContext() {
					return sink.currentContext();
				}
			};
		}

		private void onStreamMessage(V message) {

			long requested = pollState.getRequested();

			if (requested > 0) {

				if (requested == Long.MAX_VALUE) {

					if (logger.isDebugEnabled()) {
						logger.debug(String.format("[stream: %s] onStreamMessage(%s): Emitting item, fast-path", key, message));
					}
					sink.next(message);
				} else {

					if (pollState.decrementRequested()) {
						if (logger.isDebugEnabled()) {
							logger.debug(String.format("[stream: %s] onStreamMessage(%s): Emitting item, slow-path", key, message));
						}
						sink.next(message);
					} else {

						if (logger.isDebugEnabled()) {
							logger.debug(String.format("[stream: %s] onStreamMessage(%s): Buffering overflow", key, message));
						}
						overflow.offer(message);
					}
				}

			} else {

				if (logger.isDebugEnabled()) {
					logger.debug(String.format("[stream: %s] onStreamMessage(%s): Buffering overflow", key, message));
				}
				overflow.offer(message);
			}
		}

		private void onStreamError(Throwable t) {

			if (logger.isDebugEnabled()) {
				logger.debug(String.format("[stream: %s] onStreamError(%s)", key, t));
			}

			pollState.cancel();
			sink.error(t);
		}

		private void emitBuffer() {

			while (!overflow.isEmpty()) {

				long demand = pollState.getRequested();

				if (demand <= 0) {
					break;
				}

				if (demand == Long.MAX_VALUE) {

					V message = overflow.poll();

					if (message == null) {
						if (logger.isDebugEnabled()) {
							logger.debug(String.format("[stream: %s] emitBuffer(): emission missed", key));
						}
						break;
					}

					if (logger.isDebugEnabled()) {
						logger.debug(
								String.format("[stream: %s] emitBuffer(%s): Emitting item from buffer, fast-path", key, message));
					}

					sink.next(message);

				} else if (pollState.setRequested(demand, demand - 1)) {

					V message = overflow.poll();

					if (message == null) {

						if (logger.isDebugEnabled()) {
							logger.debug(String.format("[stream: %s] emitBuffer(): emission missed", key));
						}
						pollState.incrementRequested();
						break;
					}

					if (logger.isDebugEnabled()) {
						logger.debug(
								String.format("[stream: %s] emitBuffer(%s): Emitting item from buffer, slow-path", key, message));
					}

					sink.next(message);
				}
			}
		}
	}

	/**
	 * Object representing the current polling state for a particular stream subscription.
	 */
	static class PollState {

		private final AtomicLong requestsPending = new AtomicLong();
		private final AtomicBoolean active = new AtomicBoolean(true);
		private final AtomicBoolean scheduled = new AtomicBoolean(false);

		private final ReadOffsetStrategy readOffsetStrategy;
		private final AtomicReference<ReadOffset> currentOffset;
		private final Optional<Consumer> consumer;

		private PollState(Optional<Consumer> consumer, ReadOffsetStrategy readOffsetStrategy, ReadOffset currentOffset) {

			this.readOffsetStrategy = readOffsetStrategy;
			this.currentOffset = new AtomicReference<>(currentOffset);
			this.consumer = consumer;
		}

		/**
		 * Create a new state object for standalone-read.
		 *
		 * @param offset the {@link ReadOffset} from where to start. Must not be {@literal null}.
		 * @return new instance of {@link PollState}.
		 */
		static PollState standalone(ReadOffset offset) {

			ReadOffsetStrategy strategy = ReadOffsetStrategy.getStrategy(offset);
			return new PollState(Optional.empty(), strategy, strategy.getFirst(offset, Optional.empty()));
		}

		/**
		 * Create a new state object for consumergroup-read.
		 *
		 * @param consumer the {@link Consumer}. Must not be {@literal null}.
		 * @param offset the {@link ReadOffset} from where to start. Must not be {@literal null}.
		 * @return new instance of {@link PollState}.
		 */
		static PollState consumer(Consumer consumer, ReadOffset offset) {

			ReadOffsetStrategy strategy = ReadOffsetStrategy.getStrategy(offset);
			Optional<Consumer> optionalConsumer = Optional.of(consumer);
			return new PollState(optionalConsumer, strategy, strategy.getFirst(offset, optionalConsumer));
		}

		/**
		 * @return {@literal true} if the subscription is active.
		 */
		public boolean isSubscriptionActive() {
			return active.get();
		}

		/**
		 * Cancel the subscription.
		 */
		public void cancel() {
			active.set(false);
		}

		/**
		 * Decrement request count to indicate that an element was emitted.
		 *
		 * @return {@literal true} if there are still requests pending and the decrement has been successful.
		 */
		boolean decrementRequested() {

			long demand = requestsPending.get();

			if (demand > 0) {
				return requestsPending.compareAndSet(demand, demand - 1);
			}

			return false;
		}

		/**
		 * Increment request count.
		 */
		void incrementRequested() {
			requestsPending.incrementAndGet();
		}

		/**
		 * @return the number of requested items.
		 */
		public long getRequested() {
			return requestsPending.get();
		}

		/**
		 * Update demand.
		 *
		 * @param expect the number of requests still pending.
		 * @param update the increment to apply
		 * @return {@literal true} if successful.
		 */
		boolean setRequested(long expect, long update) {
			return requestsPending.compareAndSet(expect, update);
		}

		/**
		 * Activate the schedule and return the synchronization state.
		 *
		 * @return {@literal true} if the schedule was activated by this call or {@literal false} if a different process
		 *         activated the schedule.
		 */
		boolean activateSchedule() {
			return scheduled.compareAndSet(false, true);
		}

		/**
		 * @return {@literal true} if the schedule is activated.
		 */
		public boolean isScheduled() {
			return scheduled.get();
		}

		/**
		 * Deactivate the schedule.
		 */
		void scheduleCompleted() {
			scheduled.compareAndSet(true, false);
		}

		/**
		 * Advance the {@link ReadOffset}.
		 */
		void updateReadOffset(String messageId) {

			ReadOffset next = readOffsetStrategy.getNext(getCurrentReadOffset(), consumer, messageId);
			this.currentOffset.set(next);
		}

		ReadOffset getCurrentReadOffset() {
			return currentOffset.get();
		}
	}
}
