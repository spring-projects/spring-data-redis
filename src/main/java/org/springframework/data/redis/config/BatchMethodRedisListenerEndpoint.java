/*
 * Copyright 2026-present the original author or authors.
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
package org.springframework.data.redis.config;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.jspecify.annotations.Nullable;

import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.SubscriptionListener;
import org.springframework.util.Assert;

/**
 * {@link RedisListenerEndpoint} combining several {@link MethodRedisListenerEndpoint}s of the same bean and topic
 * into a single {@link MessageListener}. Methods are invoked sequentially for each message; subscription callbacks
 * are forwarded once, to the first endpoint listener implementing {@link SubscriptionListener}.
 * <p>
 * Only {@link MethodRedisListenerEndpoint#createListener()} of the combined endpoints is used. This endpoint owns
 * registration and lifecycle, so overrides of {@code register}, {@code start} or {@code stop} in the combined
 * endpoints are not invoked.
 *
 * @author Moritz Halbritter
 * @since 4.2
 */
public class BatchMethodRedisListenerEndpoint extends AbstractRedisListenerEndpoint {

	private final List<MethodRedisListenerEndpoint> endpoints;

	/**
	 * Create a new {@code BatchMethodRedisListenerEndpoint}.
	 *
	 * @param endpoints fully configured endpoints sharing the same bean and topic, must not be empty
	 */
	public BatchMethodRedisListenerEndpoint(List<MethodRedisListenerEndpoint> endpoints) {

		Assert.notEmpty(endpoints, "Endpoints must not be empty");

		MethodRedisListenerEndpoint first = endpoints.get(0);
		for (MethodRedisListenerEndpoint endpoint : endpoints) {
			Assert.isTrue(endpoint.getBean() == first.getBean(), "All endpoints must share the same bean");
			Assert.isTrue(Objects.equals(endpoint.getTopic(), first.getTopic()), "All endpoints must share the same topic");
		}

		this.endpoints = List.copyOf(endpoints);
		setTopic(first.getTopic());
	}

	@Override
	protected @Nullable MessageListener createListener() {

		List<MessageListener> listeners = new ArrayList<>(this.endpoints.size());
		for (MethodRedisListenerEndpoint endpoint : this.endpoints) {
			listeners.add(endpoint.createListener());
		}

		for (MessageListener listener : listeners) {
			if (listener instanceof SubscriptionListener subscriptionListener) {
				return new SubscriptionAwareBatchListener(listeners, subscriptionListener);
			}
		}

		return new BatchListener(listeners);
	}

	@Override
	protected StringBuilder getEndpointDescription() {
		return super.getEndpointDescription().append(" | endpoints=").append(this.endpoints);
	}

	/**
	 * {@link MessageListener} invoking a fixed list of delegates sequentially for every message. A failing delegate
	 * doesn't prevent the others from being invoked.
	 */
	private static class BatchListener implements MessageListener {

		private final List<MessageListener> listeners;

		BatchListener(List<MessageListener> listeners) {
			this.listeners = listeners;
		}

		@Override
		public void onMessage(Message message, byte @Nullable [] pattern) {

			RuntimeException failure = null;

			for (MessageListener listener : this.listeners) {
				try {
					listener.onMessage(message, pattern);
				} catch (RuntimeException ex) {
					if (failure == null) {
						failure = ex;
					} else {
						failure.addSuppressed(ex);
					}
				}
			}

			if (failure != null) {
				throw failure;
			}
		}
	}

	/**
	 * {@link BatchListener} forwarding subscription callbacks to a single delegate. Other endpoint listeners implementing
	 * {@code SubscriptionListener} are not notified, the container only sees this listener.
	 */
	private static final class SubscriptionAwareBatchListener extends BatchListener implements DelegatingSubscriptionListener {

		private final SubscriptionListener delegate;

		SubscriptionAwareBatchListener(List<MessageListener> listeners, SubscriptionListener delegate) {
			super(listeners);
			this.delegate = delegate;
		}

		@Override
		public SubscriptionListener delegate() {
			return this.delegate;
		}
	}

}
