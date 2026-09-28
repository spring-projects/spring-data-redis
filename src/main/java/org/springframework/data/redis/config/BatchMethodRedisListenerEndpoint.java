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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.jspecify.annotations.Nullable;

import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.SubscriptionListener;
import org.springframework.data.redis.connection.util.ByteArrayWrapper;
import org.springframework.data.redis.listener.PatternTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.listener.Topic;
import org.springframework.data.redis.listener.support.SimpleTopicResolver;
import org.springframework.data.redis.listener.support.TopicResolver;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.util.Assert;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.util.StringUtils;

/**
 * {@link RedisListenerEndpoint} combining several {@link MethodRedisListenerEndpoint}s of the same bean into a single
 * {@link MessageListener} subscribed to all their topics. Each message is dispatched sequentially to the methods
 * listening to the topic it was received on. Subscription callbacks are forwarded once per topic, to the first
 * endpoint listener implementing {@link SubscriptionListener}.
 * <p>
 * For example, combining methods {@code a} and {@code b} on {@code ch1} with method {@code c} on {@code news.*}
 * results in one listener: messages on {@code ch1} invoke {@code a} and {@code b}, messages matching {@code news.*}
 * invoke {@code c}.
 * <p>
 * Only {@link MethodRedisListenerEndpoint#createListener()} of the combined endpoints is used. This endpoint owns
 * registration and lifecycle, so overrides of {@code register}, {@code start} or {@code stop} in the combined
 * endpoints are not invoked.
 *
 * @author Moritz Halbritter
 * @since 4.2
 */
public class BatchMethodRedisListenerEndpoint extends AbstractRedisListenerEndpoint {

	private static final TopicResolver<Topic> TOPIC_RESOLVER = new SimpleTopicResolver();

	private final List<MethodRedisListenerEndpoint> endpoints;

	private @Nullable RedisSerializer<String> topicSerializer;

	/**
	 * Create a new {@code BatchMethodRedisListenerEndpoint}.
	 *
	 * @param endpoints fully configured endpoints sharing the same bean, each with a topic, must not be empty
	 */
	public BatchMethodRedisListenerEndpoint(List<MethodRedisListenerEndpoint> endpoints) {

		Assert.notEmpty(endpoints, "Endpoints must not be empty");

		Object bean = endpoints.get(0).getBean();
		for (MethodRedisListenerEndpoint endpoint : endpoints) {
			Assert.isTrue(endpoint.getBean() == bean, "All endpoints must share the same bean");
			Assert.isTrue(StringUtils.hasText(endpoint.getTopic()), "All endpoints must have a topic");
		}

		this.endpoints = List.copyOf(endpoints);
	}

	@Override
	public void register(RedisMessageListenerContainer listenerContainer) {

		this.topicSerializer = listenerContainer.getTopicSerializer();
		super.register(listenerContainer);
	}

	@Override
	protected @Nullable MessageListener createListener() {

		Assert.state(this.topicSerializer != null, "Endpoint not registered");

		Routes routes = new Routes();
		SubscriptionListener subscriptionListener = null;

		for (MethodRedisListenerEndpoint endpoint : this.endpoints) {

			MessageListener listener = endpoint.createListener();
			routes.add(resolveTopic(endpoint), this.topicSerializer, listener);

			if (subscriptionListener == null && listener instanceof SubscriptionListener candidate) {
				subscriptionListener = candidate;
			}
		}

		if (subscriptionListener != null) {
			return new SubscriptionAwareRoutingListener(routes, subscriptionListener);
		}

		return new RoutingListener(routes);
	}

	@Override
	protected void subscribe(RedisMessageListenerContainer listenerContainer, MessageListener messageListener) {

		Set<Topic> topics = new LinkedHashSet<>();
		for (MethodRedisListenerEndpoint endpoint : this.endpoints) {
			topics.add(resolveTopic(endpoint));
		}

		listenerContainer.addMessageListener(messageListener, topics);
	}

	@Override
	protected StringBuilder getEndpointDescription() {
		return super.getEndpointDescription().append(" | endpoints=").append(this.endpoints);
	}

	private static Topic resolveTopic(MethodRedisListenerEndpoint endpoint) {

		String topic = endpoint.getTopic();
		Assert.state(StringUtils.hasText(topic), "Topic must not be null or empty");

		return TOPIC_RESOLVER.resolveTopic(topic);
	}

	/**
	 * Listeners by serialized channel and pattern, mirroring the container's dispatch. For example, a message published
	 * on {@code news.1} is received once for channel {@code news.1} and once for pattern {@code news.*}; each time only
	 * the listeners of that subscription are invoked.
	 */
	private static final class Routes {

		private final MultiValueMap<ByteArrayWrapper, MessageListener> channels = new LinkedMultiValueMap<>();

		private final MultiValueMap<ByteArrayWrapper, MessageListener> patterns = new LinkedMultiValueMap<>();

		void add(Topic topic, RedisSerializer<String> serializer, MessageListener listener) {

			ByteArrayWrapper key = new ByteArrayWrapper(serializer.serialize(topic.getTopic()));
			MultiValueMap<ByteArrayWrapper, MessageListener> target = (topic instanceof PatternTopic ? this.patterns
					: this.channels);
			target.add(key, listener);
		}

		List<MessageListener> get(Message message, byte @Nullable [] pattern) {

			List<MessageListener> listeners = (pattern != null && pattern.length > 0)
					? this.patterns.get(new ByteArrayWrapper(pattern))
					: this.channels.get(new ByteArrayWrapper(message.getChannel()));

			return (listeners != null ? listeners : List.of());
		}

	}

	/**
	 * {@link MessageListener} invoking the listeners routed for every message sequentially. A failing listener doesn't
	 * prevent the others from being invoked.
	 */
	private static class RoutingListener implements MessageListener {

		private final Routes routes;

		RoutingListener(Routes routes) {
			this.routes = routes;
		}

		@Override
		public void onMessage(Message message, byte @Nullable [] pattern) {

			RuntimeException failure = null;

			for (MessageListener listener : this.routes.get(message, pattern)) {
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
	 * {@link RoutingListener} forwarding subscription callbacks to a single delegate. Other endpoint listeners
	 * implementing {@code SubscriptionListener} are not notified, the container only sees this listener.
	 */
	private static final class SubscriptionAwareRoutingListener extends RoutingListener
			implements DelegatingSubscriptionListener {

		private final SubscriptionListener delegate;

		SubscriptionAwareRoutingListener(Routes routes, SubscriptionListener delegate) {
			super(routes);
			this.delegate = delegate;
		}

		@Override
		public SubscriptionListener delegate() {
			return this.delegate;
		}
	}

}
