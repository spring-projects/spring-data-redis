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
package org.springframework.data.redis.listener;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.scheduler.Schedulers;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.connection.ReactivePubSubCommands;
import org.springframework.data.redis.connection.ReactiveRedisConnection;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.connection.ReactiveSubscription;
import org.springframework.data.redis.connection.ReactiveSubscription.ChannelMessage;
import org.springframework.data.redis.connection.ReactiveSubscription.Message;
import org.springframework.data.redis.connection.ReactiveSubscription.PatternMessage;
import org.springframework.data.redis.serializer.RedisElementReader;
import org.springframework.data.redis.serializer.RedisSerializationContext.SerializationPair;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

/**
 * Container providing a stream of {@link ChannelMessage} for messages received via Redis Pub/Sub listeners. The stream
 * is infinite and registers Redis subscriptions. Handles the low level details of listening, converting and message
 * dispatching.
 * <p />
 * Note the container allocates a single connection when it is created and releases the connection on
 * {@link #destroy()}. Connections are allocated eagerly to not interfere with non-blocking use during application
 * operations. Using reactive infrastructure allows usage of a single connection due to channel multiplexing.
 * <p />
 * This class is thread-safe and allows subscription by multiple concurrent threads.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.1
 * @see ReactiveSubscription
 * @see ReactivePubSubCommands
 */
public class ReactiveRedisMessageListenerContainer implements DisposableBean {

	private final SerializationPair<String> stringSerializationPair = SerializationPair
			.fromSerializer(RedisSerializer.string());
	private final Map<ReactiveSubscription, Subscribers> subscriptions = new ConcurrentHashMap<>();

	private volatile @Nullable ReactiveRedisConnection connection;

	/**
	 * Create a new {@link ReactiveRedisMessageListenerContainer} given {@link ReactiveRedisConnectionFactory}.
	 *
	 * @param connectionFactory must not be {@literal null}.
	 */
	public ReactiveRedisMessageListenerContainer(ReactiveRedisConnectionFactory connectionFactory) {

		Assert.notNull(connectionFactory, "ReactiveRedisConnectionFactory must not be null!");
		this.connection = connectionFactory.getReactiveConnection();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.DisposableBean#destroy()
	 */
	@Override
	public void destroy() {
		destroyLater().block();
	}

	/**
	 * @return the {@link Mono} signalling container termination.
	 */
	public Mono<Void> destroyLater() {

		if (connection != null) {

			Flux<Void> terminationSignals = null;
			while (!subscriptions.isEmpty()) {

				Map<ReactiveSubscription, Subscribers> local = new HashMap<>(subscriptions);
				List<Mono<Void>> monos = local.keySet().stream() //
						.peek(subscriptions::remove) //
						.map(ReactiveSubscription::cancel) //
						.collect(Collectors.toList());

				if (terminationSignals == null) {
					terminationSignals = Flux.concat(monos);
				} else {
					terminationSignals = terminationSignals.mergeWith(Flux.concat(monos));
				}
			}

			if (terminationSignals != null) {
				return terminationSignals.collectList()
						.doFinally(signalType -> connection.closeLater().subscribeOn(Schedulers.immediate()))
						.flatMap(all -> Mono.empty());
			}
			this.connection = null;
		}

		return Mono.empty();
	}

	/**
	 * Return the currently active {@link ReactiveSubscription subscriptions}.
	 *
	 * @return {@link Set} of active {@link ReactiveSubscription}
	 */
	public Collection<ReactiveSubscription> getActiveSubscriptions() {

		return subscriptions.entrySet().stream().filter(entry -> entry.getValue().hasRegistration())
				.map(entry -> entry.getKey()).collect(Collectors.toList());
	}

	/**
	 * Subscribe to one or more {@link ChannelTopic}s and receive a stream of {@link ChannelMessage}. Messages and channel
	 * names are treated as {@link String}. The message stream subscribes lazily to the Redis channels and unsubscribes if
	 * the {@link org.reactivestreams.Subscription} is {@link org.reactivestreams.Subscription#cancel() cancelled}.
	 *
	 * @param channelTopics the channels to subscribe.
	 * @return the message stream.
	 * @throws InvalidDataAccessApiUsageException if {@code patternTopics} is empty.
	 * @see #receive(Iterable, SerializationPair, SerializationPair)
	 */
	public Flux<Message<String, String>> receive(ChannelTopic... channelTopics) {

		Assert.notNull(channelTopics, "ChannelTopics must not be null!");
		Assert.noNullElements(channelTopics, "ChannelTopics must not contain null elements!");

		return receive(Arrays.asList(channelTopics), stringSerializationPair, stringSerializationPair);
	}

	/**
	 * Subscribe to one or more {@link PatternTopic}s and receive a stream of {@link PatternMessage}. Messages, pattern,
	 * and channel names are treated as {@link String}. The message stream subscribes lazily to the Redis channels and
	 * unsubscribes if the {@link org.reactivestreams.Subscription} is {@link org.reactivestreams.Subscription#cancel()
	 * cancelled}.
	 *
	 * @param patternTopics the channels to subscribe.
	 * @return the message stream.
	 * @throws InvalidDataAccessApiUsageException if {@code patternTopics} is empty.
	 * @see #receive(Iterable, SerializationPair, SerializationPair)
	 */
	@SuppressWarnings("unchecked")
	public Flux<PatternMessage<String, String, String>> receive(PatternTopic... patternTopics) {

		Assert.notNull(patternTopics, "PatternTopic must not be null!");
		Assert.noNullElements(patternTopics, "PatternTopic must not contain null elements!");

		return receive(Arrays.asList(patternTopics), stringSerializationPair, stringSerializationPair)
				.map(m -> (PatternMessage<String, String, String>) m);
	}

	/**
	 * Subscribe to one or more {@link Topic}s and receive a stream of {@link ChannelMessage} The stream may contain
	 * {@link PatternMessage} if subscribed to patterns. Messages, and channel names are serialized/deserialized using the
	 * given {@code channelSerializer} and {@code messageSerializer}. The message stream subscribes lazily to the Redis
	 * channels and unsubscribes if the {@link org.reactivestreams.Subscription} is
	 * {@link org.reactivestreams.Subscription#cancel() cancelled}.
	 *
	 * @param topics the channels to subscribe.
	 * @return the message stream.
	 * @see #receive(Iterable, SerializationPair, SerializationPair)
	 * @throws InvalidDataAccessApiUsageException if {@code topics} is empty.
	 */
	public <C, B> Flux<Message<C, B>> receive(Iterable<? extends Topic> topics, SerializationPair<C> channelSerializer,
			SerializationPair<B> messageSerializer) {

		Assert.notNull(topics, "Topics must not be null!");

		verifyConnection();

		ByteBuffer[] patterns = getTargets(topics, PatternTopic.class);
		ByteBuffer[] channels = getTargets(topics, ChannelTopic.class);

		if (ObjectUtils.isEmpty(patterns) && ObjectUtils.isEmpty(channels)) {
			throw new InvalidDataAccessApiUsageException("No channels or patterns to subscribe to.");
		}

		return doReceive(channelSerializer, messageSerializer, connection.pubSubCommands().createSubscription(), patterns,
				channels);
	}

	private <C, B> Flux<Message<C, B>> doReceive(SerializationPair<C> channelSerializer,
			SerializationPair<B> messageSerializer, Mono<ReactiveSubscription> subscription, ByteBuffer[] patterns,
			ByteBuffer[] channels) {

		Flux<Message<ByteBuffer, ByteBuffer>> messageStream = subscription.flatMapMany(it -> {

			Mono<Void> subscribe = subscribe(patterns, channels, it);

			MonoProcessor<ChannelMessage<ByteBuffer, ByteBuffer>> terminalProcessor = MonoProcessor.create();
			return it.receive().mergeWith(subscribe.then(Mono.defer(() -> {

				getSubscribers(it).registered();

				return Mono.empty();
			}))).doOnCancel(() -> {

				Subscribers subscribers = getSubscribers(it);
				if (subscribers.unregister()) {
					subscriptions.remove(it);
					it.unsubscribe().subscribe(v -> terminalProcessor.onComplete(), terminalProcessor::onError);
				}
			}).mergeWith(terminalProcessor);
		});

		return messageStream
				.map(message -> readMessage(channelSerializer.getReader(), messageSerializer.getReader(), message));
	}

	private static Mono<Void> subscribe(ByteBuffer[] patterns, ByteBuffer[] channels, ReactiveSubscription it) {

		Assert.isTrue(!ObjectUtils.isEmpty(channels) || !ObjectUtils.isEmpty(patterns),
				"Must provide either channels or patterns!");

		Mono<Void> subscribe = null;

		if (!ObjectUtils.isEmpty(patterns)) {
			subscribe = it.pSubscribe(patterns);
		}

		if (!ObjectUtils.isEmpty(channels)) {

			Mono<Void> channelsSubscribe = it.subscribe(channels);

			if (subscribe == null) {
				subscribe = channelsSubscribe;
			} else {
				subscribe = subscribe.and(channelsSubscribe);
			}
		}

		return subscribe;
	}

	private boolean isActive() {
		return connection != null;
	}

	private void verifyConnection() {

		if (!isActive()) {
			throw new IllegalStateException("ReactiveRedisMessageListenerContainer is already disposed!");
		}
	}

	private Subscribers getSubscribers(ReactiveSubscription it) {
		return subscriptions.computeIfAbsent(it, key -> new Subscribers());
	}

	private ByteBuffer[] getTargets(Iterable<? extends Topic> topics, Class<?> classFilter) {

		return StreamSupport.stream(topics.spliterator(), false) //
				.filter(classFilter::isInstance) //
				.map(Topic::getTopic) //
				.map(stringSerializationPair::write) //
				.toArray(ByteBuffer[]::new);
	}

	@SuppressWarnings("unchecked")
	private <C, B> Message<C, B> readMessage(RedisElementReader<C> channelSerializer,
			RedisElementReader<B> messageSerializer, Message<ByteBuffer, ByteBuffer> message) {

		if (message instanceof PatternMessage) {

			PatternMessage<ByteBuffer, ByteBuffer, ByteBuffer> patternMessage = (PatternMessage) message;

			String pattern = read(stringSerializationPair.getReader(), patternMessage.getPattern());
			C channel = read(channelSerializer, patternMessage.getChannel());
			B body = read(messageSerializer, patternMessage.getMessage());

			return new PatternMessage<>(pattern, channel, body);
		}

		C channel = read(channelSerializer, message.getChannel());
		B body = read(messageSerializer, message.getMessage());

		return new ChannelMessage<>(channel, body);
	}

	private static <C> C read(RedisElementReader<C> reader, ByteBuffer buffer) {

		try {
			buffer.mark();
			return reader.read(buffer);
		} finally {
			buffer.reset();
		}
	}

	/**
	 * Object to track subscriber count and to determine the last unsubscribed subscriber.
	 *
	 * @author Mark Paluch
	 */
	static class Subscribers {

		private static final AtomicLongFieldUpdater<Subscribers> SUBSCRIBERS = AtomicLongFieldUpdater
				.newUpdater(Subscribers.class, "subscribers");

		// accessed via SUBSCRIBERS
		@SuppressWarnings("unused") private volatile long subscribers;

		/**
		 * Register a subscriber and increment subscriber count.
		 */
		void registered() {
			SUBSCRIBERS.incrementAndGet(this);
		}

		/**
		 * @return {@literal true} if at least one subscriber registered via {@link #registered()}.
		 */
		boolean hasRegistration() {
			return SUBSCRIBERS.get(this) > 0;
		}

		/**
		 * Unregister a subscriber and decrement subscriber count.
		 * 
		 * @return {@literal true} if this was the last unregistered subscriber.
		 */
		boolean unregister() {

			long value = SUBSCRIBERS.get(this);

			if (value <= 0) {
				return false;
			}

			if (SUBSCRIBERS.compareAndSet(this, value, value - 1) && value == 1) {
				return true;
			}

			return false;
		}
	}
}
