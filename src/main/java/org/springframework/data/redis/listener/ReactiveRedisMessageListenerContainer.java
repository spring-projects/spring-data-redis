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
package org.springframework.data.redis.listener;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
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
import org.springframework.data.redis.connection.SubscriptionListener;
import org.springframework.data.redis.connection.util.ByteArrayWrapper;
import org.springframework.data.redis.serializer.RedisElementReader;
import org.springframework.data.redis.serializer.RedisSerializationContext.SerializationPair;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.util.ByteUtils;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

/**
 * Container providing a stream of {@link ChannelMessage} for messages received via Redis Pub/Sub listeners. The stream
 * is infinite and registers Redis subscriptions. Handles the low level details of listening, converting and message
 * dispatching.
 * <p>
 * Note the container allocates a single connection when it is created and releases the connection on
 * {@link #destroy()}. Connections are allocated eagerly to not interfere with non-blocking use during application
 * operations. Using reactive infrastructure allows usage of a single connection due to channel multiplexing.
 * <p>
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
		return Mono.defer(this::doDestroy);
	}

	private Mono<Void> doDestroy() {

		if (this.connection == null) {
			return Mono.empty();
		}

		ReactiveRedisConnection connection = getRequiredConnection();

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

		this.connection = null;
		return terminationSignals != null ? terminationSignals.then(connection.closeLater()) : connection.closeLater();
	}

	/**
	 * Return the currently active {@link ReactiveSubscription subscriptions}.
	 *
	 * @return {@link Set} of active {@link ReactiveSubscription}
	 */
	public Collection<ReactiveSubscription> getActiveSubscriptions() {

		return subscriptions.entrySet().stream().filter(entry -> entry.getValue().hasRegistration())
				.map(Map.Entry::getKey).collect(Collectors.toList());
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
	 * Subscribe to one or more {@link ChannelTopic}s and receive a stream of {@link ChannelMessage} once the returned
	 * {@link Mono} completes. Messages and channel names are treated as {@link String}. The message stream subscribes
	 * lazily to the Redis channels and unsubscribes if the inner {@link org.reactivestreams.Subscription} is
	 * {@link org.reactivestreams.Subscription#cancel() cancelled}.
	 * <p>
	 * The returned {@link Mono} completes once the connection has been subscribed to the given {@link Topic topics}. Note
	 * that cancelling the returned {@link Mono} can leave the connection in a subscribed state.
	 *
	 * @param channelTopics the channels to subscribe.
	 * @return the message stream.
	 * @throws InvalidDataAccessApiUsageException if {@code patternTopics} is empty.
	 * @since 2.6
	 */
	public Mono<Flux<Message<String, String>>> receiveLater(ChannelTopic... channelTopics) {

		Assert.notNull(channelTopics, "ChannelTopics must not be null!");
		Assert.noNullElements(channelTopics, "ChannelTopics must not contain null elements!");

		return receiveLater(Arrays.asList(channelTopics), stringSerializationPair, stringSerializationPair);
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
	 * Subscribe to one or more {@link PatternTopic}s and receive a stream of {@link PatternMessage} once the returned
	 * {@link Mono} completes. Messages, pattern, and channel names are treated as {@link String}. The message stream
	 * subscribes lazily to the Redis channels and unsubscribes if the inner {@link org.reactivestreams.Subscription} is
	 * {@link org.reactivestreams.Subscription#cancel() cancelled}.
	 * <p>
	 * The returned {@link Mono} completes once the connection has been subscribed to the given {@link Topic topics}. Note
	 * that cancelling the returned {@link Mono} can leave the connection in a subscribed state.
	 *
	 * @param patternTopics the channels to subscribe.
	 * @return the message stream.
	 * @throws InvalidDataAccessApiUsageException if {@code patternTopics} is empty.
	 * @since 2.6
	 */
	@SuppressWarnings("unchecked")
	public Mono<Flux<PatternMessage<String, String, String>>> receiveLater(PatternTopic... patternTopics) {

		Assert.notNull(patternTopics, "PatternTopic must not be null!");
		Assert.noNullElements(patternTopics, "PatternTopic must not contain null elements!");

		return receiveLater(Arrays.asList(patternTopics), stringSerializationPair, stringSerializationPair)
				.map(it -> it.map(m -> (PatternMessage<String, String, String>) m));
	}

	/**
	 * Subscribe to one or more {@link Topic}s and receive a stream of {@link ChannelMessage}. The stream may contain
	 * {@link PatternMessage} if subscribed to patterns. Messages, and channel names are serialized/deserialized using the
	 * given {@code channelSerializer} and {@code messageSerializer}. The message stream subscribes lazily to the Redis
	 * channels and unsubscribes if the {@link org.reactivestreams.Subscription} is
	 * {@link org.reactivestreams.Subscription#cancel() cancelled}.
	 *
	 * @param topics the channels/patterns to subscribe.
	 * @param subscriptionListener listener to receive subscription/unsubscription notifications.
	 * @return the message stream.
	 * @throws InvalidDataAccessApiUsageException if {@code patternTopics} is empty.
	 * @see #receive(Iterable, SerializationPair, SerializationPair)
	 * @since 2.6
	 */
	public Flux<Message<String, String>> receive(Iterable<? extends Topic> topics,
			SubscriptionListener subscriptionListener) {
		return receive(topics, stringSerializationPair, stringSerializationPair, subscriptionListener);
	}

	/**
	 * Subscribe to one or more {@link Topic}s and receive a stream of {@link ChannelMessage}. The stream may contain
	 * {@link PatternMessage} if subscribed to patterns. Messages, and channel names are serialized/deserialized using the
	 * given {@code channelSerializer} and {@code messageSerializer}. The message stream subscribes lazily to the Redis
	 * channels and unsubscribes if the {@link org.reactivestreams.Subscription} is
	 * {@link org.reactivestreams.Subscription#cancel() cancelled}.
	 *
	 * @param topics the channels/patterns to subscribe.
	 * @return the message stream.
	 * @see #receive(Iterable, SerializationPair, SerializationPair)
	 * @throws InvalidDataAccessApiUsageException if {@code topics} is empty.
	 */
	public <C, B> Flux<Message<C, B>> receive(Iterable<? extends Topic> topics, SerializationPair<C> channelSerializer,
			SerializationPair<B> messageSerializer) {
		return receive(topics, channelSerializer, messageSerializer, SubscriptionListener.NO_OP_SUBSCRIPTION_LISTENER);
	}

	/**
	 * Subscribe to one or more {@link Topic}s and receive a stream of {@link ChannelMessage}. The stream may contain
	 * {@link PatternMessage} if subscribed to patterns. Messages, and channel names are serialized/deserialized using the
	 * given {@code channelSerializer} and {@code messageSerializer}. The message stream subscribes lazily to the Redis
	 * channels and unsubscribes if the {@link org.reactivestreams.Subscription} is
	 * {@link org.reactivestreams.Subscription#cancel() cancelled}. {@link SubscriptionListener} is notified upon
	 * subscription/unsubscription and can be used for synchronization.
	 *
	 * @param topics the channels to subscribe.
	 * @param channelSerializer serialization pair to decode the channel/pattern name.
	 * @param messageSerializer serialization pair to decode the message body.
	 * @param subscriptionListener listener to receive subscription/unsubscription notifications.
	 * @return the message stream.
	 * @see #receive(Iterable, SerializationPair, SerializationPair)
	 * @throws InvalidDataAccessApiUsageException if {@code topics} is empty.
	 * @since 2.6
	 */
	public <C, B> Flux<Message<C, B>> receive(Iterable<? extends Topic> topics, SerializationPair<C> channelSerializer,
			SerializationPair<B> messageSerializer, SubscriptionListener subscriptionListener) {

		Assert.notNull(topics, "Topics must not be null!");
		Assert.notNull(channelSerializer, "Channel serializer must not be null!");
		Assert.notNull(messageSerializer, "Message serializer must not be null!");
		Assert.notNull(subscriptionListener, "SubscriptionListener must not be null!");

		verifyConnection();

		ByteBuffer[] patterns = getTargets(topics, PatternTopic.class);
		ByteBuffer[] channels = getTargets(topics, ChannelTopic.class);

		if (ObjectUtils.isEmpty(patterns) && ObjectUtils.isEmpty(channels)) {
			throw new InvalidDataAccessApiUsageException("No channels or patterns to subscribe to.");
		}

		return doReceive(channelSerializer, messageSerializer,
				getRequiredConnection().pubSubCommands().createSubscription(subscriptionListener), patterns,
				channels);
	}

	private <C, B> Flux<Message<C, B>> doReceive(SerializationPair<C> channelSerializer,
			SerializationPair<B> messageSerializer, Mono<ReactiveSubscription> subscription, ByteBuffer[] patterns,
			ByteBuffer[] channels) {

		Flux<Message<ByteBuffer, ByteBuffer>> messageStream = subscription.flatMapMany(it -> {

			Mono<Void> subscribe = subscribe(patterns, channels, it);

			Sinks.One<Message<ByteBuffer, ByteBuffer>> terminalSink = Sinks.one();
			return it.receive().mergeWith(subscribe.then(Mono.defer(() -> {

				getSubscribers(it).registered();

				return Mono.empty();
			}))).doOnCancel(() -> {

				Subscribers subscribers = getSubscribers(it);
				if (subscribers.unregister()) {
					subscriptions.remove(it);
					it.cancel().subscribe(v -> terminalSink.tryEmitEmpty(), terminalSink::tryEmitError);
				}
			}).mergeWith(terminalSink.asMono());
		});

		return messageStream
				.map(message -> readMessage(channelSerializer.getReader(), messageSerializer.getReader(), message));
	}

	/**
	 * Subscribe to one or more {@link Topic}s and receive a stream of {@link ChannelMessage}. The returned {@link Mono}
	 * completes once the connection has been subscribed to the given {@link Topic topics}. Note that cancelling the
	 * returned {@link Mono} can leave the connection in a subscribed state.
	 *
	 * @param topics the channels to subscribe.
	 * @param channelSerializer serialization pair to decode the channel/pattern name.
	 * @param messageSerializer serialization pair to decode the message body.
	 * @return the message stream.
	 * @throws InvalidDataAccessApiUsageException if {@code topics} is empty.
	 * @since 2.6
	 */
	public <C, B> Mono<Flux<Message<C, B>>> receiveLater(Iterable<? extends Topic> topics,
			SerializationPair<C> channelSerializer, SerializationPair<B> messageSerializer) {

		Assert.notNull(topics, "Topics must not be null!");
		Assert.notNull(channelSerializer, "Channel serializer must not be null!");
		Assert.notNull(messageSerializer, "Message serializer must not be null!");

		verifyConnection();

		ByteBuffer[] patterns = getTargets(topics, PatternTopic.class);
		ByteBuffer[] channels = getTargets(topics, ChannelTopic.class);

		if (ObjectUtils.isEmpty(patterns) && ObjectUtils.isEmpty(channels)) {
			throw new InvalidDataAccessApiUsageException("No channels or patterns to subscribe to.");
		}

		return Mono.defer(() -> {

			SubscriptionReadyListener readyListener = SubscriptionReadyListener.create(topics, stringSerializationPair);

			return doReceiveLater(channelSerializer, messageSerializer,
					getRequiredConnection().pubSubCommands().createSubscription(readyListener), patterns, channels)
							.delayUntil(it -> readyListener.getTrigger());
		});
	}

	private <C, B> Mono<Flux<Message<C, B>>> doReceiveLater(SerializationPair<C> channelSerializer,
			SerializationPair<B> messageSerializer, Mono<ReactiveSubscription> subscription, ByteBuffer[] patterns,
			ByteBuffer[] channels) {

		return subscription.flatMap(it -> {

			Mono<Void> subscribe = subscribe(patterns, channels, it).doOnSuccess(v -> getSubscribers(it).registered());

			Sinks.One<Message<ByteBuffer, ByteBuffer>> terminalSink = Sinks.one();

			Flux<Message<C, B>> receiver = it.receive().doOnCancel(() -> {

				Subscribers subscribers = getSubscribers(it);
				if (subscribers.unregister()) {
					subscriptions.remove(it);
					it.cancel().subscribe(v -> terminalSink.tryEmitEmpty(), terminalSink::tryEmitError);
				}
			}).mergeWith(terminalSink.asMono())
					.map(message -> readMessage(channelSerializer.getReader(), messageSerializer.getReader(), message));

			return subscribe.then(Mono.just(receiver));
		});
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

		return subscribe == null ? Mono.empty() : subscribe;
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

	private <C, B> Message<C, B> readMessage(RedisElementReader<C> channelSerializer,
			RedisElementReader<B> messageSerializer, Message<ByteBuffer, ByteBuffer> message) {

		if (message instanceof PatternMessage) {

			PatternMessage<ByteBuffer, ByteBuffer, ByteBuffer> patternMessage = (PatternMessage<ByteBuffer, ByteBuffer, ByteBuffer>) message;

			String pattern = read(stringSerializationPair.getReader(), patternMessage.getPattern());
			C channel = read(channelSerializer, patternMessage.getChannel());
			B body = read(messageSerializer, patternMessage.getMessage());

			return new PatternMessage<>(pattern, channel, body);
		}

		C channel = read(channelSerializer, message.getChannel());
		B body = read(messageSerializer, message.getMessage());

		return new ChannelMessage<>(channel, body);
	}

	private ReactiveRedisConnection getRequiredConnection() {

		ReactiveRedisConnection connection = this.connection;

		if (connection == null) {
			throw new IllegalStateException("Connection no longer available");
		}

		return connection;
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

	static class SubscriptionReadyListener extends AtomicBoolean implements SubscriptionListener {

		private final Set<ByteArrayWrapper> toSubscribe;
		private final Sinks.Empty<Void> sink = Sinks.empty();

		private SubscriptionReadyListener(Set<ByteArrayWrapper> topics) {
			this.toSubscribe = topics;
		}

		public static SubscriptionReadyListener create(Iterable<? extends Topic> topics,
				SerializationPair<String> serializationPair) {

			Set<ByteArrayWrapper> wrappers = new HashSet<>();

			for (Topic topic : topics) {
				wrappers.add(new ByteArrayWrapper(ByteUtils.getBytes(serializationPair.getWriter().write(topic.getTopic()))));
			}

			return new SubscriptionReadyListener(wrappers);
		}

		@Override
		public void onChannelSubscribed(byte[] channel, long count) {
			removeRemaining(channel);
		}

		@Override
		public void onPatternSubscribed(byte[] pattern, long count) {
			removeRemaining(pattern);
		}

		private void removeRemaining(byte[] channel) {

			boolean done;

			synchronized (toSubscribe) {
				toSubscribe.remove(new ByteArrayWrapper(channel));
				done = toSubscribe.isEmpty();
			}

			if (done && compareAndSet(false, true)) {
				sink.emitEmpty(Sinks.EmitFailureHandler.FAIL_FAST);
			}
		}

		public Mono<Void> getTrigger() {
			return sink.asMono();
		}
	}
}
