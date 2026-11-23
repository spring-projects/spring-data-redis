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

import io.lettuce.core.pubsub.RedisPubSubAdapter;
import io.lettuce.core.pubsub.RedisPubSubListener;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.pubsub.api.reactive.RedisPubSubReactiveCommands;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.core.publisher.Sinks;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

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

	private final Object lifecycleMonitor = new Object();

	private final Map<ByteArrayWrapper, CleanupHandle> activeChannelCleanups = new ConcurrentHashMap<>();

	private final Map<ByteArrayWrapper, CleanupHandle> activePatternCleanups = new ConcurrentHashMap<>();

	private final Set<ByteArrayWrapper> serverChannels = ConcurrentHashMap.newKeySet();

	private final Set<ByteArrayWrapper> serverPatterns = ConcurrentHashMap.newKeySet();

	private final Set<LettuceReactiveSubscription> subscriptions = ConcurrentHashMap.newKeySet();

	private volatile boolean closed;

	private @Nullable StatefulRedisPubSubConnection<ByteBuffer, ByteBuffer> registeredConnection;

	private @Nullable RedisPubSubListener<ByteBuffer, ByteBuffer> orphanCleanupListener;

	LettuceReactivePubSubCommands(LettuceReactiveRedisConnection connection) {
		this.connection = connection;
	}

	public Map<ByteArrayWrapper, Target> getChannels() {
		return channels;
	}

	public Map<ByteArrayWrapper, Target> getPatterns() {
		return patterns;
	}

	Map<ByteArrayWrapper, CleanupHandle> getActiveChannelCleanups() {
		return activeChannelCleanups;
	}

	Map<ByteArrayWrapper, CleanupHandle> getActivePatternCleanups() {
		return activePatternCleanups;
	}

	@Override
	public Mono<ReactiveSubscription> createSubscription(SubscriptionListener listener) {

		return getPubSubConnection().handle((pubSubConnection, sink) -> {
			synchronized (this.lifecycleMonitor) {
				if (this.closed) {
					sink.error(new IllegalStateException("Unable to connect; Connection is closed"));
					return;
				}
				LettuceReactiveSubscription subscription = new LettuceReactiveSubscription(listener, pubSubConnection,
						this, connection.translateException());
				this.subscriptions.add(subscription);
				sink.next(subscription);
			}
		});
	}

	void unregisterSubscription(LettuceReactiveSubscription subscription) {
		this.subscriptions.remove(subscription);
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
		Assert.noNullElements(channels, "Channels must not contain null elements");

		return doSubscribe(channels, this.channels, this.activeChannelCleanups, false,
				commands -> commands.subscribe(channels));
	}

	public Mono<Void> unsubscribe(ByteBuffer... channels) {

		Assert.notNull(channels, "Channels must not be null");
		Assert.noNullElements(channels, "Channels must not contain null elements");

		return doUnsubscribe(channels, this.channels, this.activeChannelCleanups,
				RedisPubSubReactiveCommands::unsubscribe);
	}

	@Override
	public Mono<Void> pSubscribe(ByteBuffer... patterns) {

		Assert.notNull(patterns, "Patterns must not be null");
		Assert.noNullElements(patterns, "Patterns must not contain null elements");

		return doSubscribe(patterns, this.patterns, this.activePatternCleanups, true,
				commands -> commands.psubscribe(patterns));
	}

	public Mono<Void> pUnsubscribe(ByteBuffer... patterns) {

		Assert.notNull(patterns, "Patterns must not be null");
		Assert.noNullElements(patterns, "Patterns must not contain null elements");

		return doUnsubscribe(patterns, this.patterns, this.activePatternCleanups,
				RedisPubSubReactiveCommands::punsubscribe);
	}

	private Mono<Void> doSubscribe(ByteBuffer[] targets, Map<ByteArrayWrapper, Target> targetMap,
			Map<ByteArrayWrapper, CleanupHandle> activeCleanups, boolean pattern,
			Function<RedisPubSubReactiveCommands<ByteBuffer, ByteBuffer>, Mono<Void>> command) {

		return Mono.defer(() -> {

			Target.trackSubscriptions(targets, targetMap); // track usage but do not limit what to subscribe to
			AtomicBoolean settled = new AtomicBoolean();

			return getPubSubConnection() //
					.flatMap(pubSubConnection -> awaitInFlightCleanup(targets, targetMap, activeCleanups)
							.then(Mono.defer(() -> subscribeUnderTargetLock(targetMap, () -> {
								if (this.closed) {
									return Mono.error(
											new IllegalStateException("Unable to connect; Connection is closed"));
								}
								return command.apply(pubSubConnection.reactive());
							})))) //
					.onErrorMap(connection.translateException()) //
					.doOnSuccess(ignored -> settled.compareAndSet(false, true)) //
					.doOnError(ignored -> {
						if (settled.compareAndSet(false, true)) {
							rollbackSubscriptions(targets, targetMap, pattern);
						}
					}) //
					.doOnCancel(() -> {
						if (settled.compareAndSet(false, true)) {
							rollbackSubscriptions(targets, targetMap, pattern);
						}
					});
		});
	}

	private void rollbackSubscriptions(ByteBuffer[] targets, Map<ByteArrayWrapper, Target> targetMap, boolean pattern) {

		ByteBuffer[] orphaned = Target.trackUnsubscriptions(targets, targetMap);
		StatefulRedisPubSubConnection<ByteBuffer, ByteBuffer> pubSubConnection = this.registeredConnection;
		if (pubSubConnection == null || this.closed || orphaned.length == 0) {
			return;
		}
		Set<ByteArrayWrapper> serverSet = pattern ? this.serverPatterns : this.serverChannels;
		for (ByteBuffer target : orphaned) {
			byte[] raw = ByteUtils.getBytes(target);
			if (serverSet.contains(new ByteArrayWrapper(raw))) {
				cleanupOrphanedSubscription(pubSubConnection, raw, pattern);
			}
		}
	}

	void close() {
		for (LettuceReactiveSubscription subscription : drainLifecycleOnClose()) {
			subscription.detachOnClose();
		}
	}

	Mono<Void> closeLater() {

		List<LettuceReactiveSubscription> active = drainLifecycleOnClose();
		if (active.isEmpty()) {
			return Mono.empty();
		}
		return Flux.fromIterable(active).flatMap(LettuceReactiveSubscription::closeOnConnectionClose).then();
	}

	private List<LettuceReactiveSubscription> drainLifecycleOnClose() {

		List<LettuceReactiveSubscription> active;
		synchronized (this.lifecycleMonitor) {
			if (this.closed) {
				return Collections.emptyList();
			}
			this.closed = true;

			StatefulRedisPubSubConnection<ByteBuffer, ByteBuffer> pubSubConnection = this.registeredConnection;
			RedisPubSubListener<ByteBuffer, ByteBuffer> listener = this.orphanCleanupListener;
			this.registeredConnection = null;
			this.orphanCleanupListener = null;

			if (pubSubConnection != null && listener != null) {
				pubSubConnection.removeListener(listener);
			}

			active = this.subscriptions.isEmpty() ? Collections.emptyList() : new ArrayList<>(this.subscriptions);
			this.subscriptions.clear();
		}

		List<CleanupHandle> drained = new ArrayList<>();
		synchronized (this.channels) {
			drainCleanups(this.activeChannelCleanups, drained);
		}
		synchronized (this.patterns) {
			drainCleanups(this.activePatternCleanups, drained);
		}
		this.serverChannels.clear();
		this.serverPatterns.clear();

		if (!drained.isEmpty()) {
			IllegalStateException closedException = new IllegalStateException("Unable to connect; Connection is closed");
			for (CleanupHandle handle : drained) {
				handle.settled.tryEmitError(closedException);
				if (handle.disposable != null) {
					handle.disposable.dispose();
				}
			}
		}

		return active;
	}

	private static void drainCleanups(Map<ByteArrayWrapper, CleanupHandle> activeCleanups, List<CleanupHandle> handles) {

		handles.addAll(activeCleanups.values());
		activeCleanups.clear();
	}

	private Mono<Void> awaitInFlightCleanup(ByteBuffer[] targets, Map<ByteArrayWrapper, Target> targetMap,
			Map<ByteArrayWrapper, CleanupHandle> activeCleanups) {

		if (this.closed) {
			return Mono.error(new IllegalStateException("Unable to connect; Connection is closed"));
		}

		if (activeCleanups.isEmpty()) {
			return Mono.empty();
		}

		List<Mono<Void>> barriers = null;
		synchronized (targetMap) {
			if (this.closed) {
				return Mono.error(new IllegalStateException("Unable to connect; Connection is closed"));
			}
			for (ByteBuffer buffer : targets) {
				CleanupHandle handle = activeCleanups.get(new ByteArrayWrapper(ByteUtils.getBytes(buffer)));
				if (handle != null) {
					if (barriers == null) {
						barriers = new ArrayList<>(targets.length);
					}
					barriers.add(handle.settled.asMono());
				}
			}
		}

		return barriers == null ? Mono.empty() : Mono.whenDelayError(barriers);
	}

	private static <T> Mono<T> subscribeUnderTargetLock(Map<ByteArrayWrapper, Target> targetMap,
			Supplier<Mono<T>> publisherSupplier) {

		return new Mono<>() {

			@Override
			public void subscribe(CoreSubscriber<? super T> actual) {
				synchronized (targetMap) {
					publisherSupplier.get().subscribe(actual);
				}
			}
		};
	}

	private Mono<Void> doUnsubscribe(ByteBuffer[] targets, Map<ByteArrayWrapper, Target> targetMap,
			Map<ByteArrayWrapper, CleanupHandle> activeCleanups,
			BiFunction<RedisPubSubReactiveCommands<ByteBuffer, ByteBuffer>, ByteBuffer[], Mono<Void>> command) {

		return Mono.defer(() -> {

			List<ByteBuffer> decremented = new ArrayList<>(targets.length);
			List<ByteArrayWrapper> barrierKeys = new ArrayList<>(targets.length);
			CleanupHandle barrier = new CleanupHandle();
			ByteBuffer[] actualUnsubscribe;

			synchronized (targetMap) {
				actualUnsubscribe = Target.trackUnsubscriptions(targets, targetMap, decremented);
				if (!this.closed) {
					for (ByteBuffer buf : actualUnsubscribe) {
						ByteArrayWrapper key = new ByteArrayWrapper(ByteUtils.getBytes(buf));
						if (activeCleanups.putIfAbsent(key, barrier) == null) {
							barrierKeys.add(key);
						}
					}
				}
			}

			if (actualUnsubscribe.length == 0 && targets.length != 0) {
				return Mono.empty();
			}

			ByteBuffer[] toRestore = decremented.toArray(new ByteBuffer[0]);
			AtomicBoolean settled = new AtomicBoolean();

			return getPubSubConnection()
					.flatMap(pubSubConnection -> subscribeUnderTargetLock(targetMap,
							() -> command.apply(pubSubConnection.reactive(), actualUnsubscribe)))
					.onErrorMap(connection.translateException()) //
					.doOnSuccess(ignored -> {
						if (settled.compareAndSet(false, true)) {
							releaseBarrier(targetMap, activeCleanups, barrierKeys, barrier);
						}
					}) //
					.doOnError(ignored -> {
						if (settled.compareAndSet(false, true)) {
							Target.trackSubscriptions(toRestore, targetMap);
							releaseBarrier(targetMap, activeCleanups, barrierKeys, barrier);
						}
					}) //
					.doOnCancel(() -> {
						if (settled.compareAndSet(false, true)) {
							Target.trackSubscriptions(toRestore, targetMap);
							releaseBarrier(targetMap, activeCleanups, barrierKeys, barrier);
						}
					});
		});
	}

	private static void releaseBarrier(Map<ByteArrayWrapper, Target> targetMap,
			Map<ByteArrayWrapper, CleanupHandle> activeCleanups, List<ByteArrayWrapper> barrierKeys, CleanupHandle barrier) {

		if (!barrierKeys.isEmpty()) {
			synchronized (targetMap) {
				for (ByteArrayWrapper key : barrierKeys) {
					activeCleanups.remove(key, barrier);
				}
			}
		}
		barrier.settled.tryEmitEmpty();
	}

	private Mono<StatefulRedisPubSubConnection<ByteBuffer, ByteBuffer>> getPubSubConnection() {
		return connection.getPubSubConnection().doOnNext(this::registerOrphanCleanupListener);
	}

	private void registerOrphanCleanupListener(StatefulRedisPubSubConnection<ByteBuffer, ByteBuffer> pubSubConnection) {

		synchronized (this.lifecycleMonitor) {
			if (this.closed || this.orphanCleanupListener != null) {
				return;
			}

			RedisPubSubListener<ByteBuffer, ByteBuffer> listener = createOrphanCleanupListener(pubSubConnection);
			pubSubConnection.addListener(listener);
			this.registeredConnection = pubSubConnection;
			this.orphanCleanupListener = listener;
		}
	}

	private RedisPubSubListener<ByteBuffer, ByteBuffer> createOrphanCleanupListener(
			StatefulRedisPubSubConnection<ByteBuffer, ByteBuffer> pubSubConnection) {

		return new RedisPubSubAdapter<>() {

			@Override
			public void subscribed(ByteBuffer channel, long count) {
				if (channel != null) {
					byte[] raw = ByteUtils.getBytes(channel);
					serverChannels.add(new ByteArrayWrapper(raw));
					cleanupOrphanedSubscription(pubSubConnection, raw, false);
				}
			}

			@Override
			public void unsubscribed(ByteBuffer channel, long count) {
				if (channel != null) {
					serverChannels.remove(new ByteArrayWrapper(ByteUtils.getBytes(channel)));
				}
			}

			@Override
			public void psubscribed(ByteBuffer pattern, long count) {
				if (pattern != null) {
					byte[] raw = ByteUtils.getBytes(pattern);
					serverPatterns.add(new ByteArrayWrapper(raw));
					cleanupOrphanedSubscription(pubSubConnection, raw, true);
				}
			}

			@Override
			public void punsubscribed(ByteBuffer pattern, long count) {
				if (pattern != null) {
					serverPatterns.remove(new ByteArrayWrapper(ByteUtils.getBytes(pattern)));
				}
			}
		};
	}

	private void cleanupOrphanedSubscription(StatefulRedisPubSubConnection<ByteBuffer, ByteBuffer> pubSubConnection,
			byte[] rawTarget, boolean pattern) {

		Map<ByteArrayWrapper, Target> targetMap = pattern ? this.patterns : this.channels;
		Map<ByteArrayWrapper, CleanupHandle> activeCleanups = pattern ? this.activePatternCleanups
				: this.activeChannelCleanups;
		ByteArrayWrapper wrapper = new ByteArrayWrapper(rawTarget);

		synchronized (targetMap) {
			if (this.closed || !isOrphaned(wrapper, targetMap)) {
				return;
			}

			CleanupHandle existing = activeCleanups.get(wrapper);
			if (existing != null) {
				existing.recheck = true;
				return;
			}

			RedisPubSubReactiveCommands<ByteBuffer, ByteBuffer> reactive = pubSubConnection.reactive();
			ByteBuffer target = ByteBuffer.wrap(rawTarget);
			Mono<Void> unsubscribeMono = pattern ? reactive.punsubscribe(target) : reactive.unsubscribe(target);

			CleanupHandle handle = new CleanupHandle();
			activeCleanups.put(wrapper, handle);

			Disposable disposable = unsubscribeMono //
					.doFinally(signal -> onOrphanCleanupFinished(pubSubConnection, rawTarget, pattern, wrapper, handle,
							signal == SignalType.ON_COMPLETE)) //
					.subscribe(ignored -> {}, ex -> {});
			handle.disposable = disposable;
			if (this.closed) {
				disposable.dispose();
			}
		}
	}

	private void onOrphanCleanupFinished(StatefulRedisPubSubConnection<ByteBuffer, ByteBuffer> pubSubConnection,
			byte[] rawTarget, boolean pattern, ByteArrayWrapper wrapper, CleanupHandle handle, boolean succeeded) {

		Map<ByteArrayWrapper, Target> targetMap = pattern ? this.patterns : this.channels;
		Map<ByteArrayWrapper, CleanupHandle> activeCleanups = pattern ? this.activePatternCleanups
				: this.activeChannelCleanups;

		if (succeeded) {
			(pattern ? this.serverPatterns : this.serverChannels).remove(wrapper);
		}

		boolean triggerFollowUp = false;
		synchronized (targetMap) {
			activeCleanups.remove(wrapper, handle);
			if (!this.closed && handle.recheck && isOrphaned(wrapper, targetMap)) {
				triggerFollowUp = true;
			}
		}

		handle.settled.tryEmitEmpty();

		if (triggerFollowUp) {
			cleanupOrphanedSubscription(pubSubConnection, rawTarget, pattern);
		}
	}

	private static boolean isOrphaned(ByteArrayWrapper wrapper, Map<ByteArrayWrapper, Target> targetMap) {

		synchronized (targetMap) {
			Target target = targetMap.get(wrapper);
			return target == null || !target.hasSubscribers();
		}
	}

	static final class CleanupHandle {

		final Sinks.Empty<Void> settled = Sinks.empty();
		volatile @Nullable Disposable disposable;
		boolean recheck;
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

		boolean hasSubscribers() {
			return SUBSCRIBERS.get(this) > 0;
		}

		long getSubscriberCount() {
			return SUBSCRIBERS.get(this);
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
