/*
 * Copyright 2018-present the original author or authors.
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.jspecify.annotations.Nullable;
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
 * @author Daeryeon Choi
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

		Assert.notNull(channels, "Channels must not be null");
		Assert.noNullElements(channels, "Channels must not contain null elements");

		return doUnsubscribe(channels, this.channels, RedisPubSubReactiveCommands::unsubscribe);
	}

	@Override
	public Mono<Void> pSubscribe(ByteBuffer... patterns) {

		Assert.notNull(patterns, "Patterns must not be null");

		Target.trackSubscriptions(patterns, this.patterns); // track usage but do not limit what to subscribe to

		return doWithPubSub(commands -> commands.psubscribe(patterns));
	}

	public Mono<Void> pUnsubscribe(ByteBuffer... patterns) {

		Assert.notNull(patterns, "Patterns must not be null");
		Assert.noNullElements(patterns, "Patterns must not contain null elements");

		return doUnsubscribe(patterns, this.patterns, RedisPubSubReactiveCommands::punsubscribe);
	}

	private Mono<Void> doUnsubscribe(ByteBuffer[] targets, Map<ByteArrayWrapper, Target> targetMap,
			BiFunction<RedisPubSubReactiveCommands<ByteBuffer, ByteBuffer>, ByteBuffer[], Mono<Void>> command) {

		return Mono.defer(() -> {

			List<ByteBuffer> decremented = new ArrayList<>(targets.length);
			ByteBuffer[] actualUnsubscribe = Target.trackUnsubscriptions(targets, targetMap, decremented);

			if (actualUnsubscribe.length == 0 && targets.length != 0) {
				return Mono.empty();
			}

			ByteBuffer[] toRestore = decremented.toArray(new ByteBuffer[0]);
			AtomicBoolean settled = new AtomicBoolean();

			return doWithPubSub(commands -> command.apply(commands, actualUnsubscribe)) //
					.doOnSuccess(ignored -> settled.compareAndSet(false, true)) //
					.doOnError(ignored -> {
						if (settled.compareAndSet(false, true)) {
							Target.trackSubscriptions(toRestore, targetMap);
						}
					}) //
					.doOnCancel(() -> {
						if (settled.compareAndSet(false, true)) {
							Target.trackSubscriptions(toRestore, targetMap);
						}
					});
		});
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
			return trackUnsubscriptions(targets, targetMap, null);
		}

		static ByteBuffer[] trackUnsubscriptions(ByteBuffer[] targets, Map<ByteArrayWrapper, Target> targetMap,
				@Nullable List<ByteBuffer> decremented) {

			List<ByteBuffer> toUnsubscribe = new ArrayList<>(targets.length);

			synchronized (targetMap) {
				for (ByteBuffer target : targets) {
					ByteArrayWrapper wrapper = new ByteArrayWrapper(ByteUtils.getBytes(target));
					Target targetToUse = targetMap.get(wrapper);
					if (targetToUse != null && SUBSCRIBERS.get(targetToUse) > 0) {
						if (decremented != null) {
							decremented.add(target);
						}
						if (targetToUse.decrement()) {
							targetMap.remove(wrapper);
							toUnsubscribe.add(target);
						}
					}
				}
			}

			return toUnsubscribe.toArray(new ByteBuffer[0]);
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

		@Override
		public String toString() {
			return "%s: Subscribers: %s".formatted(LettuceConverters.toString(raw), SUBSCRIBERS.get(this));
		}
	}
}
