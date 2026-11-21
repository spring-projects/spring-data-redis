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

import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.pubsub.api.reactive.RedisPubSubReactiveCommands;
import reactor.core.Disposable;
import reactor.core.publisher.ConnectableFlux;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.jspecify.annotations.Nullable;
import org.springframework.data.redis.connection.ReactiveSubscription;
import org.springframework.data.redis.connection.SubscriptionListener;
import org.springframework.data.redis.connection.util.ByteArrayWrapper;
import org.springframework.data.redis.util.ByteUtils;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

/**
 * Lettuce-specific implementation of {@link ReactiveSubscription}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @author Daeryeon Choi
 * @since 2.1
 */
class LettuceReactiveSubscription implements ReactiveSubscription {

	private final LettuceByteBufferPubSubListenerWrapper listener;
	private final StatefulRedisPubSubConnection<ByteBuffer, ByteBuffer> connection;

	private final RedisPubSubReactiveCommands<ByteBuffer, ByteBuffer> reactive;
	private final LettuceReactivePubSubCommands commands;

	private final State patternState;
	private final State channelState;

	private final AtomicBoolean cancelled = new AtomicBoolean();
	private final AtomicBoolean cancelInProgress = new AtomicBoolean();
	private final AtomicBoolean reconnectObservedDuringCancel = new AtomicBoolean();
	private final AtomicBoolean listenerRemoved = new AtomicBoolean();
	private final AtomicReference<@Nullable Mono<Void>> activeCancel = new AtomicReference<>();
	private final Set<ByteArrayWrapper> cleaningUpChannels = ConcurrentHashMap.newKeySet();
	private final Set<ByteArrayWrapper> cleaningUpPatterns = ConcurrentHashMap.newKeySet();

	LettuceReactiveSubscription(SubscriptionListener subscriptionListener,
			StatefulRedisPubSubConnection<ByteBuffer, ByteBuffer> connection, LettuceReactivePubSubCommands commands,
			Function<Throwable, Throwable> exceptionTranslator) {

		SubscriptionListener listenerToUse = new SubscriptionListener() {

			@Override
			public void onChannelSubscribed(byte[] channel, long count) {
				subscriptionListener.onChannelSubscribed(channel, count);
				handleReconnectIfCancelled(channel, false);
			}

			@Override
			public void onChannelUnsubscribed(byte[] channel, long count) {
				subscriptionListener.onChannelUnsubscribed(channel, count);
			}

			@Override
			public void onPatternSubscribed(byte[] pattern, long count) {
				subscriptionListener.onPatternSubscribed(pattern, count);
				handleReconnectIfCancelled(pattern, true);
			}

			@Override
			public void onPatternUnsubscribed(byte[] pattern, long count) {
				subscriptionListener.onPatternUnsubscribed(pattern, count);
			}
		};

		this.listener = new LettuceByteBufferPubSubListenerWrapper(
				new LettuceMessageListener((messages, pattern) -> {}, listenerToUse));
		this.connection = connection;
		this.reactive = connection.reactive();
		this.commands = commands;
		connection.addListener(listener);

		this.patternState = new State(exceptionTranslator);
		this.channelState = new State(exceptionTranslator);
	}

	@Override
	public Mono<Void> subscribe(ByteBuffer... channels) {

		Assert.notNull(channels, "Channels must not be null");
		Assert.noNullElements(channels, "Channels must not contain null elements");

		return channelState.subscribe(channels, commands::subscribe) //
				.doOnSuccess(ignored -> {
					if (this.cancelled.get()) {
						unsubscribe(channels).doOnSuccess(v -> removeListenerIfTerminated()).subscribe(v -> {}, ex -> {});
					}
				});
	}

	@Override
	public Mono<Void> pSubscribe(ByteBuffer... patterns) {

		Assert.notNull(patterns, "Patterns must not be null");
		Assert.noNullElements(patterns, "Patterns must not contain null elements");

		return patternState.subscribe(patterns, commands::pSubscribe) //
				.doOnSuccess(ignored -> {
					if (this.cancelled.get()) {
						pUnsubscribe(patterns).doOnSuccess(v -> removeListenerIfTerminated()).subscribe(v -> {}, ex -> {});
					}
				});
	}

	@Override
	public Mono<Void> unsubscribe() {
		return unsubscribe(channelState.getTargets().toArray(new ByteBuffer[0]));
	}

	@Override
	public Mono<Void> unsubscribe(ByteBuffer... channels) {

		Assert.notNull(channels, "Channels must not be null");
		Assert.noNullElements(channels, "Channels must not contain null elements");

		return ObjectUtils.isEmpty(channels) ? Mono.empty() : channelState.unsubscribe(channels, commands::unsubscribe);
	}

	@Override
	public Mono<Void> pUnsubscribe() {
		return pUnsubscribe(patternState.getTargets().toArray(new ByteBuffer[0]));
	}

	@Override
	public Mono<Void> pUnsubscribe(ByteBuffer... patterns) {

		Assert.notNull(patterns, "Patterns must not be null");
		Assert.noNullElements(patterns, "Patterns must not contain null elements");

		return ObjectUtils.isEmpty(patterns) ? Mono.empty() : patternState.unsubscribe(patterns, commands::pUnsubscribe);
	}

	@Override
	public Set<ByteBuffer> getChannels() {
		return channelState.getTargets();
	}

	@Override
	public Set<ByteBuffer> getPatterns() {
		return patternState.getTargets();
	}

	@Override
	public Flux<Message<ByteBuffer, ByteBuffer>> receive() {

		Flux<Message<ByteBuffer, ByteBuffer>> channelMessages = channelState.receive(() -> reactive.observeChannels() //
				.filter(message -> channelState.contains(message.getChannel())) //
				.map(message -> new ChannelMessage<>(message.getChannel(), message.getMessage())));

		Flux<Message<ByteBuffer, ByteBuffer>> patternMessages = patternState.receive(() -> reactive.observePatterns() //
				.filter(message -> patternState.contains(message.getPattern())) //
				.map(message -> new PatternMessage<>(message.getPattern(), message.getChannel(), message.getMessage())));

		return channelMessages.mergeWith(patternMessages);
	}

	@Override
	public Mono<Void> cancel() {

		return Mono.defer(() -> {

			while (true) {
				Mono<Void> existing = this.activeCancel.get();
				if (existing != null) {
					return existing;
				}

				AtomicReference<@Nullable Mono<Void>> selfRef = new AtomicReference<>();
				Mono<Void> shared = doCancel().doFinally(signal -> this.activeCancel.compareAndSet(selfRef.get(), null))
						.cache();
				selfRef.set(shared);
				if (this.activeCancel.compareAndSet(null, shared)) {
					return shared;
				}
			}
		});
	}

	private Mono<Void> doCancel() {

		return Mono.defer(() -> {

			this.cancelled.set(true);
			this.cancelInProgress.set(true);
			this.reconnectObservedDuringCancel.set(false);

			boolean hadTargets = !channelState.isEmpty() || !patternState.isEmpty();

			return unsubscribe().then(pUnsubscribe()) //
					.doOnSuccess(ignored -> this.cancelInProgress.set(false)) //
					.doOnError(ignored -> {
						this.cancelInProgress.set(false);
						terminateStates();
						if (this.reconnectObservedDuringCancel.compareAndSet(true, false)) {
							cleanupRemainingTargets();
						}
					}) //
					.doOnCancel(() -> {
						this.cancelInProgress.set(false);
						terminateStates();
					}) //
					.then(Mono.defer(() -> {

						terminateStates();

						if (!hadTargets) {
							removeListenerIfTerminated();
							return Mono.empty();
						}

						// this is to ensure completion of the futures and result processing. Since we're unsubscribing first, we
						// expect that we receive pub/sub confirmations before the PING response.
						return reactive.ping().doFinally(signal -> removeListenerIfTerminated()).then();
					}));
		});
	}

	private void terminateStates() {
		channelState.terminate();
		patternState.terminate();
	}

	private void handleReconnectIfCancelled(byte[] target, boolean pattern) {

		if (!this.cancelled.get()) {
			return;
		}

		if (this.cancelInProgress.get()) {
			this.reconnectObservedDuringCancel.set(true);
			return;
		}

		cleanupCancelledTarget(target, pattern);
	}

	private void cleanupRemainingTargets() {

		for (ByteBuffer channel : channelState.getTargets()) {
			cleanupCancelledTarget(ByteUtils.getBytes(channel), false);
		}

		for (ByteBuffer pattern : patternState.getTargets()) {
			cleanupCancelledTarget(ByteUtils.getBytes(pattern), true);
		}
	}

	private void cleanupCancelledTarget(byte[] target, boolean pattern) {

		ByteBuffer buffer = ByteBuffer.wrap(target);
		ByteArrayWrapper wrapper = new ByteArrayWrapper(target);
		State state = pattern ? this.patternState : this.channelState;

		if (!state.contains(buffer)) {
			removeListenerIfTerminated();
			return;
		}

		Set<ByteArrayWrapper> cleaningUp = pattern ? this.cleaningUpPatterns : this.cleaningUpChannels;
		if (!cleaningUp.add(wrapper)) {
			return;
		}

		Mono<Void> unsubscribeMono = pattern ? pUnsubscribe(buffer) : unsubscribe(buffer);
		unsubscribeMono //
				.doFinally(signal -> cleaningUp.remove(wrapper)) //
				.doOnSuccess(ignored -> removeListenerIfTerminated()) //
				.subscribe(ignored -> {}, ex -> {});
	}

	private void removeListenerIfTerminated() {

		if (this.cancelled.get() && channelState.isEmpty() && patternState.isEmpty()
				&& this.listenerRemoved.compareAndSet(false, true)) {
			connection.removeListener(listener);
		}
	}

	/**
	 * Subscription state holder.
	 *
	 * @author Mark Paluch
	 */
	static class State {

		private final Set<ByteArrayWrapper> targets = new ConcurrentSkipListSet<>();
		private final Set<ByteArrayWrapper> unsubscribing = ConcurrentHashMap.newKeySet();
		private final AtomicLong subscribers = new AtomicLong();
		private final AtomicReference<@Nullable Flux<?>> flux = new AtomicReference<>();
		private final Function<Throwable, Throwable> exceptionTranslator;

		private volatile @Nullable Disposable disposable;

		State(Function<Throwable, Throwable> exceptionTranslator) {
			this.exceptionTranslator = exceptionTranslator;
		}

		/**
		 * Subscribe to {@code targets} using subscribe {@link Function} and register {@code targets} after subscription.
		 *
		 * @param targets
		 * @param subscribeFunction
		 * @return
		 */
		Mono<Void> subscribe(ByteBuffer[] targets, Function<ByteBuffer[], Mono<Void>> subscribeFunction) {

			return Mono.defer(() -> {

				Set<ByteArrayWrapper> added = new LinkedHashSet<>(targets.length);
				List<ByteBuffer> toSubscribe = new ArrayList<>(targets.length);
				synchronized (this.targets) {
					for (ByteBuffer target : targets) {
						ByteArrayWrapper wrapper = getWrapper(target);
						if (!this.targets.contains(wrapper) && added.add(wrapper)) {
							toSubscribe.add(target);
						}
					}
				}

				if (toSubscribe.isEmpty()) {
					return Mono.empty();
				}

				return subscribeFunction.apply(toSubscribe.toArray(new ByteBuffer[0])).doOnSuccess((discard) -> {
					synchronized (this.targets) {
						this.targets.addAll(added);
					}
				}).onErrorMap(exceptionTranslator);
			});
		}

		/**
		 * Unsubscribe from to {@code targets} using unsubscribe {@link Function} and register {@code targets} after
		 * subscription.
		 *
		 * @param targets
		 * @param unsubscribeFunction
		 * @return
		 */
		Mono<Void> unsubscribe(ByteBuffer[] targets, Function<ByteBuffer[], Mono<Void>> unsubscribeFunction) {

			return Mono.defer(() -> {

				Set<ByteArrayWrapper> claimed = new LinkedHashSet<>(targets.length);
				List<ByteBuffer> toUnsubscribe = new ArrayList<>(targets.length);
				synchronized (this.targets) {
					for (ByteBuffer byteBuffer : targets) {
						ByteArrayWrapper wrapper = getWrapper(byteBuffer);
						if (this.targets.contains(wrapper) && this.unsubscribing.add(wrapper) && claimed.add(wrapper)) {
							toUnsubscribe.add(byteBuffer);
						}
					}
				}

				if (toUnsubscribe.isEmpty()) {
					return Mono.empty();
				}

				AtomicBoolean settled = new AtomicBoolean();
				return unsubscribeFunction.apply(toUnsubscribe.toArray(new ByteBuffer[0])).doOnSuccess((discard) -> {
					settled.set(true);
					synchronized (this.targets) {
						this.targets.removeAll(claimed);
						this.unsubscribing.removeAll(claimed);
					}
				}).doFinally(signal -> {
					if (!settled.get()) {
						synchronized (this.targets) {
							this.unsubscribing.removeAll(claimed);
						}
					}
				}).onErrorMap(exceptionTranslator);
			});
		}

		Set<ByteBuffer> getTargets() {
			return targets.stream().map(ByteArrayWrapper::getArray).map(ByteBuffer::wrap)
					.collect(Collectors.toUnmodifiableSet());
		}

		/**
		 * Create a message stream from connect {@link Function}. Multiple calls to this method are lock-free synchronized.
		 * The first successful caller creates the actual stream. Other concurrent callers that do not pass the
		 * synchronization use the stream created by the first successful caller.
		 * <p>
		 * The stream registers a disposal function upon subscription for external {@link #terminate() termination}.
		 *
		 * @param connectFunction
		 * @param <T> message type.
		 * @return
		 */
		@SuppressWarnings({ "unchecked", "NullAway" })
		<T> Flux<T> receive(Supplier<Flux<T>> connectFunction) {

			Flux<?> fastPath = flux.get();

			if (fastPath != null) {
				return (Flux) fastPath;
			}

			ConnectableFlux<T> connectableFlux = connectFunction.get().onErrorMap(exceptionTranslator).publish();
			Flux<T> fluxToUse = connectableFlux.doOnSubscribe(subscription -> {

				if (subscribers.incrementAndGet() == 1) {
					disposable = connectableFlux.connect();
				}
			}).doFinally(signalType -> {

				// TODO: do new need to care about what happened?
				if (subscribers.decrementAndGet() == 0) {

					this.flux.compareAndSet(connectableFlux, null);
					terminate();
				}
			});

			if (this.flux.compareAndSet(null, fluxToUse)) {
				return fluxToUse;
			}

			return (Flux) this.flux.get();
		}

		void terminate() {

			this.flux.set(null);

			Disposable disposable = this.disposable;

			if (disposable != null && !disposable.isDisposed()) {
				disposable.dispose();
			}
		}

		public boolean contains(ByteBuffer target) {
			return this.targets.contains(getWrapper(target));
		}

		boolean isEmpty() {
			return this.targets.isEmpty();
		}

		private static ByteArrayWrapper getWrapper(ByteBuffer byteBuffer) {
			return new ByteArrayWrapper(byteBuffer);
		}
	}
}
