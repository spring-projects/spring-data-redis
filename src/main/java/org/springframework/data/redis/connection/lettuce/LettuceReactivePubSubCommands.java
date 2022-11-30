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
package org.springframework.data.redis.connection.lettuce;

import io.lettuce.core.pubsub.api.reactive.RedisPubSubReactiveCommands;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.reactivestreams.Publisher;
import org.springframework.data.redis.connection.ReactivePubSubCommands;
import org.springframework.data.redis.connection.ReactiveSubscription;
import org.springframework.data.redis.connection.ReactiveSubscription.ChannelMessage;
import org.springframework.data.redis.connection.SubscriptionListener;
import org.springframework.data.redis.connection.util.ByteArrayWrapper;
import org.springframework.data.redis.util.ByteUtils;
import org.springframework.util.Assert;

/**
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.1
 */
class LettuceReactivePubSubCommands implements ReactivePubSubCommands {

	private final LettuceReactiveRedisConnection connection;

	private final Map<ByteArrayWrapper, Target> channels = new ConcurrentHashMap<>();

	private final Map<ByteArrayWrapper, Target> patterns = new ConcurrentHashMap<>();

	LettuceReactivePubSubCommands(LettuceReactiveRedisConnection connection) {
		this.connection = connection;
	}

	public Map<ByteArrayWrapper, Target> getChannels() {
		return channels;
	}

	public Map<ByteArrayWrapper, Target> getPatterns() {
		return patterns;
	}

	@Override
	public Mono<ReactiveSubscription> createSubscription(SubscriptionListener listener) {

		return connection.getPubSubConnection().map(pubSubConnection -> new LettuceReactiveSubscription(listener,
				pubSubConnection, this, connection.translateException()));
	}

	@Override
	public Flux<Long> publish(Publisher<ChannelMessage<ByteBuffer, ByteBuffer>> messageStream) {

		Assert.notNull(messageStream, "ChannelMessage stream must not be null");

		return connection.getCommands().flatMapMany(commands -> Flux.from(messageStream)
				.flatMap(message -> commands.publish(message.getChannel(), message.getMessage())));
	}

	@Override
	public Mono<Void> subscribe(ByteBuffer... channels) {

		Assert.notNull(channels, "Channels must not be null");

		Target.trackSubscriptions(channels, this.channels); // track usage but do not limit what to subscribe to

		return doWithPubSub(commands -> commands.subscribe(channels));
	}

	public Mono<Void> unsubscribe(ByteBuffer... channels) {

		Assert.notNull(patterns, "Patterns must not be null");

		ByteBuffer[] actualUnsubscribe = Target.trackUnsubscriptions(channels, this.channels);

		if (actualUnsubscribe.length == 0 && channels.length != 0) {
			return Mono.empty();
		}

		return doWithPubSub(commands -> commands.unsubscribe(actualUnsubscribe));
	}

	@Override
	public Mono<Void> pSubscribe(ByteBuffer... patterns) {

		Assert.notNull(patterns, "Patterns must not be null");

		Target.trackSubscriptions(patterns, this.patterns); // track usage but do not limit what to subscribe to

		return doWithPubSub(commands -> commands.psubscribe(patterns));
	}

	public Mono<Void> pUnsubscribe(ByteBuffer... patterns) {

		Assert.notNull(patterns, "Patterns must not be null");

		ByteBuffer[] actualUnsubscribe = Target.trackUnsubscriptions(patterns, this.patterns);

		if (actualUnsubscribe.length == 0 && patterns.length != 0) {
			return Mono.empty();
		}

		return doWithPubSub(commands -> commands.punsubscribe(actualUnsubscribe));
	}

	private <T> Mono<T> doWithPubSub(Function<RedisPubSubReactiveCommands<ByteBuffer, ByteBuffer>, Mono<T>> function) {

		return connection.getPubSubConnection().flatMap(pubSubConnection -> function.apply(pubSubConnection.reactive()))
				.onErrorMap(connection.translateException());
	}

	static class Target {

		private static final AtomicLongFieldUpdater<Target> SUBSCRIBERS = AtomicLongFieldUpdater.newUpdater(Target.class,
				"subscribers");

		private final byte[] raw;

		private volatile long subscribers;

		Target(byte[] raw) {
			this.raw = raw;
		}

		/**
		 * Record the subscriptions to {@code targets} and store these in {@code targetMap}.
		 *
		 * @param targets
		 * @param targetMap
		 */
		public static void trackSubscriptions(ByteBuffer[] targets, Map<ByteArrayWrapper, Target> targetMap) {
			doWithTargets(targets, targetMap, Target::allocate);
		}

		/**
		 * Record the un-subscriptions to {@code targets} and store these in {@code targetMap}. Returns the targets to
		 * actually unsubscribe from if there are no subscribers to a particular target.
		 *
		 * @param targets
		 * @param targetMap
		 */
		public static ByteBuffer[] trackUnsubscriptions(ByteBuffer[] targets, Map<ByteArrayWrapper, Target> targetMap) {
			return doWithTargets(targets, targetMap, Target::deallocate);
		}

		static ByteBuffer[] doWithTargets(ByteBuffer[] targets, Map<ByteArrayWrapper, Target> targetMap,
				BiFunction<ByteBuffer, Map<ByteArrayWrapper, Target>, Boolean> f) {

			List<ByteBuffer> toSubscribe = new ArrayList<>(targets.length);

			synchronized (targetMap) {
				for (ByteBuffer target : targets) {
					if (f.apply(target, targetMap)) {
						toSubscribe.add(target);
					}
				}
			}

			return toSubscribe.toArray(new ByteBuffer[0]);
		}

		boolean increment() {
			return SUBSCRIBERS.incrementAndGet(this) == 1;
		}

		boolean decrement() {

			long l = SUBSCRIBERS.get(this);

			if (l > 0) {
				if (SUBSCRIBERS.compareAndSet(this, l, l - 1)) {
					return l == 1; // return true if this was the last subscriber
				}
			}

			return false;
		}

		static boolean allocate(ByteBuffer buffer, Map<ByteArrayWrapper, Target> targets) {

			byte[] raw = ByteUtils.getBytes(buffer);

			ByteArrayWrapper wrapper = new ByteArrayWrapper(raw);
			Target targetToUse = targets.get(wrapper);

			if (targetToUse == null) {
				targetToUse = new Target(raw);
				targets.put(wrapper, targetToUse);
			}

			return targetToUse.increment();
		}

		static boolean deallocate(ByteBuffer buffer, Map<ByteArrayWrapper, Target> targets) {

			byte[] raw = ByteUtils.getBytes(buffer);

			ByteArrayWrapper wrapper = new ByteArrayWrapper(raw);
			Target targetToUse = targets.get(wrapper);

			if (targetToUse == null) {
				return false;
			}

			if (targetToUse.decrement()) {
				targets.remove(wrapper);
				return true;
			}

			return false;
		}

		@Override
		public String toString() {
			return String.format("%s: Subscribers: %s", new String(raw), SUBSCRIBERS.get(this));
		}
	}
}
