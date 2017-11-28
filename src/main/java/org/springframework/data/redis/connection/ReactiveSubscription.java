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
package org.springframework.data.redis.connection;

import lombok.EqualsAndHashCode;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.util.Set;

import org.springframework.util.Assert;

/**
 * Subscription for Redis channels using reactive infrastructure. A {@link ReactiveSubscription} allows subscribing to
 * {@link #subscribe(ByteBuffer...) channels} and {@link #pSubscribe(ByteBuffer...) patterns}. It provides access to the
 * {@link ChannelMessage} {@link #receive() stream} that emits only messages for channels and patterns registered in
 * this {@link ReactiveSubscription}.
 * <p />
 * A reactive Redis connection can have multiple subscriptions. If two or more subscriptions subscribe to the same
 * target (channel/pattern) and one unsubscribes, then the other one will no longer receive messages for the target due
 * to how Redis handled Pub/Sub subscription.
 * 
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.1
 */
public interface ReactiveSubscription {

	/**
	 * Subscribes to the {@code channels} and adds these to the current subscription.
	 *
	 * @param channels channel names. Must not be empty.
	 * @return empty {@link Mono} that completes once the channel subscription is registered.
	 */
	Mono<Void> subscribe(ByteBuffer... channels);

	/**
	 * Subscribes to the channel {@code patterns} and adds these to the current subscription.
	 *
	 * @param patterns channel patterns. Must not be empty.
	 * @return empty {@link Mono} that completes once the pattern subscription is registered.
	 */
	Mono<Void> pSubscribe(ByteBuffer... patterns);

	/**
	 * Cancels the current subscription for all {@link #getChannels() channels}.
	 *
	 * @return empty {@link Mono} that completes once the channel subscriptions are unregistered.
	 */
	Mono<Void> unsubscribe();

	/**
	 * Cancels the current subscription for all given channels.
	 *
	 * @param channels channel names. Must not be empty.
	 * @return empty {@link Mono} that completes once the channel subscriptions are unregistered.
	 */
	Mono<Void> unsubscribe(ByteBuffer... channels);

	/**
	 * Cancels the subscription for all channels matched by {@link #getPatterns()} patterns}.
	 *
	 * @return empty {@link Mono} that completes once the patterns subscriptions are unregistered.
	 */
	Mono<Void> pUnsubscribe();

	/**
	 * Cancels the subscription for all channels matching the given patterns.
	 *
	 * @param patterns must not be empty.
	 * @return empty {@link Mono} that completes once the patterns subscriptions are unregistered.
	 */
	Mono<Void> pUnsubscribe(ByteBuffer... patterns);

	/**
	 * Returns the (named) channels for this subscription.
	 *
	 * @return {@link Set} of named channels.
	 */
	Set<ByteBuffer> getChannels();

	/**
	 * Returns the channel patters for this subscription.
	 *
	 * @return {@link Set} of channel patterns.
	 */
	Set<ByteBuffer> getPatterns();

	/**
	 * Retrieve the message stream emitting {@link Message messages}. The resulting message stream contains only messages
	 * for subscribed and registered {@link #getChannels() channels} and {@link #getPatterns() patterns}.
	 * <p />
	 * Stream publishing uses {@link reactor.core.publisher.ConnectableFlux} turning the stream into a hot sequence.
	 * Emission is paused if there is no demand. Messages received in that time are buffered. This stream terminates
	 * either if all subscribers unsubscribe or if this {@link Subscription} is {@link #cancel() is terminated}.
	 *
	 * @return {@link Flux} emitting the {@link Message} stream.
	 */
	Flux<Message<ByteBuffer, ByteBuffer>> receive();

	/**
	 * Unsubscribe from all {@link #getChannels() channels} and {@link #getPatterns() patterns} and request termination of
	 * all active {@link #receive() message streams}. Active streams will terminate with a
	 * {@link java.util.concurrent.CancellationException}.
	 *
	 * @return a {@link Mono} that completes once termination is finished.
	 */
	Mono<Void> cancel();

	/**
	 * {@link Message} represents a Redis channel message within Redis pub/sub.
	 *
	 * @param <C> channel representation type.
	 * @param <M> message representation type.
	 * @author Christoph Strobl
	 * @since 2.1
	 */
	interface Message<C, M> {

		/**
		 * Get the channel the message published to.
		 *
		 * @return never {@literal null}.
		 */
		C getChannel();

		/**
		 * Get the actual message body.
		 *
		 * @return never {@literal null}.
		 */
		M getMessage();
	}

	/**
	 * Value object for a Redis channel message.
	 *
	 * @param <C> type of how the channel name is represented.
	 * @param <M> type of how the message is represented.
	 * @author Mark Paluch
	 * @author Christoph Strobl
	 * @since 2.1
	 */
	@EqualsAndHashCode
	class ChannelMessage<C, M> implements Message<C, M> {

		private final C channel;
		private final M message;

		/**
		 * Create a new {@link ChannelMessage}.
		 *
		 * @param channel must not be {@literal null}.
		 * @param message must not be {@literal null}.
		 */
		public ChannelMessage(C channel, M message) {

			Assert.notNull(channel, "Channel must not be null!");
			Assert.notNull(message, "Message must not be null!");

			this.channel = channel;
			this.message = message;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.connection.ReactiveSubscription.Message#getChannel()
		 */
		@Override
		public C getChannel() {
			return channel;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.connection.ReactiveSubscription.Message#getMessage()
		 */
		@Override
		public M getMessage() {
			return message;
		}

		@Override
		public String toString() {
			return "ChannelMessage {" + "channel=" + channel + ", message=" + message + '}';
		}
	}

	/**
	 * Value object for a Redis channel message received from a pattern subscription.
	 *
	 * @param <P> type of how the pattern is represented.
	 * @param <C> type of how the channel name is represented.
	 * @param <M> type of how the message is represented.
	 * @author Mark Paluch
	 * @author Christoph Strobl
	 * @since 2.1
	 */
	@EqualsAndHashCode(callSuper = true)
	class PatternMessage<P, C, M> extends ChannelMessage<C, M> {

		private final P pattern;

		/**
		 * Create a new {@link PatternMessage}.
		 * 
		 * @param pattern must not be {@literal null}.
		 * @param channel must not be {@literal null}.
		 * @param message must not be {@literal null}.
		 */
		public PatternMessage(P pattern, C channel, M message) {

			super(channel, message);

			Assert.notNull(pattern, "Pattern must not be null!");
			this.pattern = pattern;
		}

		/**
		 * Get the pattern that matched the channel.
		 *
		 * @return never {@literal null}.
		 */
		public P getPattern() {
			return pattern;
		}

		@Override
		public String toString() {
			return "PatternMessage{" + "channel=" + getChannel() + ", pattern=" + pattern + ", message=" + getMessage() + '}';
		}
	}
}
