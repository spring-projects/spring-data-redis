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
package org.springframework.data.redis.connection.lettuce;

import io.lettuce.core.pubsub.api.reactive.RedisPubSubReactiveCommands;
import lombok.RequiredArgsConstructor;
import reactor.core.Disposable;
import reactor.core.publisher.ConnectableFlux;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

import org.springframework.data.redis.connection.ReactiveSubscription;
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

	private final RedisPubSubReactiveCommands<ByteBuffer, ByteBuffer> commands;

	private final State patternState;
	private final State channelState;

	LettuceReactiveSubscription(RedisPubSubReactiveCommands<ByteBuffer, ByteBuffer> commands,
			Function<Throwable, Throwable> exceptionTranslator) {

		this.commands = commands;
		this.patternState = new State(exceptionTranslator);
		this.channelState = new State(exceptionTranslator);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveSubscription#subscribe(java.nio.ByteBuffer[])
	 */
	@Override
	public Mono<Void> subscribe(ByteBuffer... channels) {

		Assert.notNull(channels, "Channels must not be null!");
		Assert.noNullElements(channels, "Channels must not contain null elements!");

		return channelState.subscribe(channels, commands::subscribe);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveSubscription#pSubscribe(java.nio.ByteBuffer[])
	 */
	@Override
	public Mono<Void> pSubscribe(ByteBuffer... patterns) {

		Assert.notNull(patterns, "Patterns must not be null!");
		Assert.noNullElements(patterns, "Patterns must not contain null elements!");

		return patternState.subscribe(patterns, commands::psubscribe);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveSubscription#unsubscribe()
	 */
	@Override
	public Mono<Void> unsubscribe() {
		return unsubscribe(channelState.getTargets().toArray(new ByteBuffer[channelState.getTargets().size()]));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveSubscription#unsubscribe(java.nio.ByteBuffer[])
	 */
	@Override
	public Mono<Void> unsubscribe(ByteBuffer... channels) {

		Assert.notNull(channels, "Channels must not be null!");
		Assert.noNullElements(channels, "Channels must not contain null elements!");

		return ObjectUtils.isEmpty(channels) ? Mono.empty() : channelState.unsubscribe(channels, commands::unsubscribe);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveSubscription#pUnsubscribe()
	 */
	@Override
	public Mono<Void> pUnsubscribe() {
		return pUnsubscribe(patternState.getTargets().toArray(new ByteBuffer[patternState.getTargets().size()]));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveSubscription#pUnsubscribe(java.nio.ByteBuffer[])
	 */
	@Override
	public Mono<Void> pUnsubscribe(ByteBuffer... patterns) {

		Assert.notNull(patterns, "Patterns must not be null!");
		Assert.noNullElements(patterns, "Patterns must not contain null elements!");

		return ObjectUtils.isEmpty(patterns) ? Mono.empty() : patternState.unsubscribe(patterns, commands::punsubscribe);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveSubscription#getChannels()
	 */
	@Override
	public Set<ByteBuffer> getChannels() {
		return channelState.getTargets();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveSubscription#getPatterns()
	 */
	@Override
	public Set<ByteBuffer> getPatterns() {
		return patternState.getTargets();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveSubscription#receive()
	 */
	@Override
	public Flux<Message<ByteBuffer, ByteBuffer>> receive() {

		Flux<Message<ByteBuffer, ByteBuffer>> channelMessages = channelState.receive(() -> commands.observeChannels() //
				.filter(message -> channelState.getTargets().contains(message.getChannel())) //
				.map(message -> new ChannelMessage<>(message.getChannel(), message.getMessage())));

		Flux<Message<ByteBuffer, ByteBuffer>> patternMessages = patternState.receive(() -> commands.observePatterns() //
				.filter(message -> patternState.getTargets().contains(message.getPattern())) //
				.map(message -> new PatternMessage<>(message.getPattern(), message.getChannel(), message.getMessage())));

		return channelMessages.mergeWith(patternMessages);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveSubscription#terminate()
	 */
	@Override
	public Mono<Void> cancel() {

		return unsubscribe().then(pUnsubscribe()).then(Mono.defer(() -> {

			channelState.terminate();
			patternState.terminate();
			return Mono.empty();
		}));
	}

	/**
	 * Subscription state holder.
	 * 
	 * @author Mark Paluch
	 */
	@RequiredArgsConstructor
	static class State {

		private final Set<ByteBuffer> targets = new ConcurrentSkipListSet<>();
		private final AtomicLong subscribers = new AtomicLong();
		private final AtomicReference<Flux<?>> flux = new AtomicReference<>();
		private final Function<Throwable, Throwable> exceptionTranslator;

		private volatile @Nullable Disposable disposable;

		/**
		 * Subscribe to {@code targets} using subscribe {@link Function} and register {@code targets} after subscription.
		 * 
		 * @param targets
		 * @param subscribeFunction
		 * @return
		 */
		Mono<Void> subscribe(ByteBuffer[] targets, Function<ByteBuffer[], Mono<Void>> subscribeFunction) {

			return subscribeFunction.apply(targets).doOnSuccess((discard) -> this.targets.addAll(Arrays.asList(targets)))
					.onErrorMap(exceptionTranslator);
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

				List<ByteBuffer> targetCollection = Arrays.asList(targets);

				return unsubscribeFunction.apply(targets).doOnSuccess((discard) -> {
					this.targets.removeAll(targetCollection);
				}).onErrorMap(exceptionTranslator);
			});
		}

		Set<ByteBuffer> getTargets() {
			return Collections.unmodifiableSet(targets);
		}

		/**
		 * Create a message stream from connect {@link Function}. Multiple calls to this method are lock-free synchronized.
		 * The first successful caller creates the actual stream. Other concurrent callers that do not pass the
		 * synchronization use the stream created by the first successful caller.
		 * <p/>
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
	}
}
