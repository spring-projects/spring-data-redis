/*
 * Copyright 2022-2025 the original author or authors.
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.BiFunction;

import org.jspecify.annotations.Nullable;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.SubscriptionListener;
import org.springframework.data.redis.connection.util.ByteArrayWrapper;

/**
 * Synchronizing {@link MessageListener} and {@link SubscriptionListener} that allows notifying a {@link Runnable}
 * (through {@link SubscriptionSynchronization}) upon completing subscriptions to channels or patterns.
 *
 * @author Mark Paluch
 * @since 3.0
 */
class SynchronizingMessageListener implements MessageListener, SubscriptionListener {

	private final MessageListener messageListener;
	private final SubscriptionListener subscriptionListener;
	private final List<SubscriptionSynchronization> synchronizations = new CopyOnWriteArrayList<>();

	public SynchronizingMessageListener(MessageListener messageListener, SubscriptionListener subscriptionListener) {
		this.messageListener = messageListener;
		this.subscriptionListener = subscriptionListener;
	}

	/**
	 * Register a {@link SubscriptionSynchronization}.
	 *
	 * @param synchronization must not be {@literal null}.
	 */
	public void addSynchronization(SubscriptionSynchronization synchronization) {
		this.synchronizations.add(synchronization);
	}

	@Override
	public void onMessage(Message message, byte @Nullable [] pattern) {
		messageListener.onMessage(message, pattern);
	}

	@Override
	public void onChannelSubscribed(byte[] channel, long count) {

		subscriptionListener.onChannelSubscribed(channel, count);
		handleSubscription(channel, SubscriptionSynchronization::onChannelSubscribed);
	}

	@Override
	public void onChannelUnsubscribed(byte[] channel, long count) {
		subscriptionListener.onChannelUnsubscribed(channel, count);
	}

	@Override
	public void onPatternSubscribed(byte[] pattern, long count) {

		subscriptionListener.onPatternSubscribed(pattern, count);
		handleSubscription(pattern, SubscriptionSynchronization::onPatternSubscribed);
	}

	@Override
	public void onPatternUnsubscribed(byte[] pattern, long count) {
		subscriptionListener.onPatternUnsubscribed(pattern, count);
	}

	void handleSubscription(byte[] topic,
			BiFunction<SubscriptionSynchronization, ByteArrayWrapper, Boolean> synchronizerCallback) {

		if (synchronizations.isEmpty()) {
			return;
		}

		ByteArrayWrapper binaryChannel = new ByteArrayWrapper(topic);
		List<SubscriptionSynchronization> finalized = new ArrayList<>(synchronizations.size());

		for (SubscriptionSynchronization synchronizer : synchronizations) {

			if (synchronizerCallback.apply(synchronizer, binaryChannel)) {
				finalized.add(synchronizer);
			}
		}

		synchronizations.removeAll(finalized);
	}

	/**
	 * Synchronization to await subscriptions for channels and patterns.
	 */
	static class SubscriptionSynchronization {

		private static final AtomicIntegerFieldUpdater<SubscriptionSynchronization> DONE = AtomicIntegerFieldUpdater
				.newUpdater(SubscriptionSynchronization.class, "done");

		private static final int NOT_DONE = 0;
		private static final int DONE_DONE = 0;

		private volatile int done = NOT_DONE;

		private final Runnable doneCallback;

		private final Set<ByteArrayWrapper> remainingPatterns;
		private final Set<ByteArrayWrapper> remainingChannels;

		public SubscriptionSynchronization(Collection<byte[]> remainingPatterns, Collection<byte[]> remainingChannels,
				Runnable doneCallback) {

			if (remainingPatterns.isEmpty()) {
				this.remainingPatterns = Collections.emptySet();
			} else {
				this.remainingPatterns = ConcurrentHashMap.newKeySet(remainingPatterns.size());
				this.remainingPatterns.addAll(remainingPatterns.stream().map(ByteArrayWrapper::new).toList());
			}

			if (remainingChannels.isEmpty()) {
				this.remainingChannels = Collections.emptySet();
			} else {
				this.remainingChannels = ConcurrentHashMap.newKeySet(remainingChannels.size());
				this.remainingChannels.addAll(remainingChannels.stream().map(ByteArrayWrapper::new).toList());
			}

			this.doneCallback = doneCallback;
		}

		boolean onChannelSubscribed(ByteArrayWrapper channel) {

			if (DONE.get(this) == NOT_DONE) {
				remainingChannels.remove(channel);
				return postSubscribe();
			}

			return false;
		}

		boolean onPatternSubscribed(ByteArrayWrapper pattern) {

			if (DONE.get(this) == NOT_DONE) {
				remainingPatterns.remove(pattern);
				return postSubscribe();
			}

			return false;
		}

		/**
		 * @return whether the synchronization is finished and can be removed.
		 */
		private boolean postSubscribe() {

			if (remainingChannels.isEmpty() && remainingPatterns.isEmpty() && DONE.compareAndSet(this, NOT_DONE, DONE_DONE)) {
				this.doneCallback.run();

				return true;
			}

			return false;
		}

	}

}
