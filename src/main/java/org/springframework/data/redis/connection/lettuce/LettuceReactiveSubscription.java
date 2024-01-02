/*
 * Copyright 2018-2024 the original author or authors.
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
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.springframework.data.redis.connection.ReactiveSubscription;
import org.springframework.data.redis.connection.SubscriptionListener;
import org.springframework.data.redis.connection.util.ByteArrayWrapper;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

/**
 * Lettuce-specific implementation of {@link ReactiveSubscription}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.1
 */
class LettuceReactiveSubscription implements ReactiveSubscription {

	private final LettuceByteBufferPubSubListenerWrapper listener;
	private final StatefulRedisPubSubConnection<ByteBuffer, ByteBuffer> connection;

	private final RedisPubSubReactiveCommands<ByteBuffer, ByteBuffer> reactive;
	private final LettuceReactivePubSubCommands commands;

	private final State patternState;
	private final State channelState;

	LettuceReactiveSubscription(SubscriptionListener subscriptionListener,
			StatefulRedisPubSubConnection<ByteBuffer, ByteBuffer> connection, LettuceReactivePubSubCommands commands,
			Function<Throwable, Throwable> exceptionTranslator) {

		this.listener = new LettuceByteBufferPubSubListenerWrapper(
				new LettuceMessageListener((messages, pattern) -> {}, subscriptionListener));
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

		return channelState.subscribe(channels, commands::subscribe);
	}

	@Override
	public Mono<Void> pSubscribe(ByteBuffer... patterns) {

		Assert.notNull(patterns, "Patterns must not be null");
		Assert.noNullElements(patterns, "Patterns must not contain null elements");

		return patternState.subscribe(patterns, commands::pSubscribe);
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

		return unsubscribe().then(pUnsubscribe()).then(Mono.defer(() -> {

			channelState.terminate();
			patternState.terminate();

			// this is to ensure completion of the futures and result processing. Since we're unsubscribing first, we expect
			// that we receive pub/sub confirmations before the PING response.
			return reactive.ping().then(Mono.fromRunnable(() -> {
				connection.removeListener(listener);
			}));
		}));
	}

	/**
	 * Subscription state holder.
	 *
	 * @author Mark Paluch
	 */
	static class State {

		private final Set<ByteArrayWrapper> targets = new ConcurrentSkipListSet<>();
		private final AtomicLong subscribers = new AtomicLong();
		private final AtomicReference<Flux<?>> flux = new AtomicReference<>();
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

			return subscribeFunction.apply(targets).doOnSuccess((discard) -> {

				for (ByteBuffer target : targets) {
					this.targets.add(getWrapper(target));
				}
			}).onErrorMap(exceptionTranslator);
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

				return unsubscribeFunction.apply(targets).doOnSuccess((discard) -> {

					for (ByteBuffer byteBuffer : targets) {
						this.targets.remove(getWrapper(byteBuffer));
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
		@SuppressWarnings("unchecked")
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

		private static ByteArrayWrapper getWrapper(ByteBuffer byteBuffer) {
			return new ByteArrayWrapper(byteBuffer);
		}
	}
}
