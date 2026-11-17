/*
 * Copyright 2016-present the original author or authors.
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

import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.redis.util.ThingsToWaitFor.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.hamcrest.Matchers;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.MethodSource;

import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.SubscriptionListener;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.connection.jedis.extension.JedisConnectionFactoryExtension;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.lettuce.extension.LettuceConnectionFactoryExtension;
import org.springframework.data.redis.test.extension.RedisStandalone;

/**
 * Integration tests for {@link RedisMessageListenerContainer}.
 *
 * @author Mark Paluch
 */
@ParameterizedClass
@MethodSource("testParams")
class RedisMessageListenerContainerIntegrationTests {

	private RedisConnectionFactory connectionFactory;
	private RedisMessageListenerContainer container;

	public RedisMessageListenerContainerIntegrationTests(RedisConnectionFactory connectionFactory) {
		this.connectionFactory = connectionFactory;
	}

	@BeforeEach
	void setUp() {

		container = new RedisMessageListenerContainer();
		container.setRecoveryInterval(100);
		container.setConnectionFactory(connectionFactory);
		container.setBeanName("container");
		container.afterPropertiesSet();
	}

	public static Collection<Object[]> testParams() {

		// Jedis
		JedisConnectionFactory jedisConnFactory = JedisConnectionFactoryExtension
				.getConnectionFactory(RedisStandalone.class);

		// Lettuce
		LettuceConnectionFactory lettuceConnFactory = LettuceConnectionFactoryExtension
				.getConnectionFactory(RedisStandalone.class);

		return Arrays.asList(new Object[][] { { jedisConnFactory }, { lettuceConnFactory } });
	}

	@AfterEach
	void tearDown() throws Exception {
		container.destroy();
	}

	@Test
	void notifiesChannelSubscriptionState() throws Exception {

		AtomicReference<String> onSubscribe = new AtomicReference<>();
		AtomicReference<String> onUnsubscribe = new AtomicReference<>();
		CompletableFuture<Void> subscribe = new CompletableFuture<>();
		CompletableFuture<Void> unsubscribe = new CompletableFuture<>();

		CompositeListener listener = new CompositeListener() {
			@Override
			public void onMessage(Message message, byte @Nullable [] pattern) {

			}

			@Override
			public void onChannelSubscribed(byte[] channel, long count) {
				onSubscribe.set(new String(channel));
				subscribe.complete(null);
			}

			@Override
			public void onChannelUnsubscribed(byte[] channel, long count) {
				onUnsubscribe.set(new String(channel));
				unsubscribe.complete(null);
			}
		};

		container.addMessageListener(listener, new ChannelTopic("a"));
		container.start();

		subscribe.get(10, TimeUnit.SECONDS);

		container.destroy();

		unsubscribe.get(10, TimeUnit.SECONDS);

		assertThat(onSubscribe).hasValue("a");
		assertThat(onUnsubscribe).hasValue("a");
	}

	@Test
	void notifiesPatternSubscriptionState() throws Exception {

		AtomicReference<String> onPsubscribe = new AtomicReference<>();
		AtomicReference<String> onPunsubscribe = new AtomicReference<>();
		CompletableFuture<Void> psubscribe = new CompletableFuture<>();
		CompletableFuture<Void> punsubscribe = new CompletableFuture<>();

		CompositeListener listener = new CompositeListener() {
			@Override
			public void onMessage(Message message, byte @Nullable [] pattern) {

			}

			@Override
			public void onPatternSubscribed(byte[] pattern, long count) {
				onPsubscribe.set(new String(pattern));
				psubscribe.complete(null);
			}

			@Override
			public void onPatternUnsubscribed(byte[] pattern, long count) {
				onPunsubscribe.set(new String(pattern));
				punsubscribe.complete(null);
			}
		};

		container.addMessageListener(listener, new PatternTopic("a"));
		container.start();

		psubscribe.get(10, TimeUnit.SECONDS);

		container.destroy();

		punsubscribe.get(10, TimeUnit.SECONDS);

		assertThat(onPsubscribe).hasValue("a");
		assertThat(onPunsubscribe).hasValue("a");
	}

	@Test
	void repeatedSubscribeShouldNotifyOnlyOnce() throws Exception {

		AtomicInteger subscriptions1 = new AtomicInteger();
		AtomicInteger subscriptions2 = new AtomicInteger();
		CountDownLatch received = new CountDownLatch(2);

		CompositeListener listener1 = new CompositeListener() {
			@Override
			public void onMessage(Message message, byte @Nullable [] pattern) {
				received.countDown();
			}

			@Override
			public void onPatternSubscribed(byte[] pattern, long count) {
				subscriptions1.incrementAndGet();
			}
		};

		CompositeListener listener2 = new CompositeListener() {
			@Override
			public void onMessage(Message message, byte @Nullable [] pattern) {
				received.countDown();
			}

			@Override
			public void onPatternSubscribed(byte[] pattern, long count) {
				subscriptions2.incrementAndGet();
			}
		};

		container.addMessageListener(listener1, new PatternTopic("a"));
		container.addMessageListener(listener2, new PatternTopic("a"));

		container.start();

		try (RedisConnection connection = connectionFactory.getConnection()) {
			connection.publish("a".getBytes(), "hello".getBytes());
		}

		received.await(2, TimeUnit.SECONDS);
		container.destroy();

		shortAwait().until(() -> subscriptions1.get() > 0 || subscriptions2.get() > 0);

		assertThat(subscriptions1.get() + subscriptions2.get()).isGreaterThan(0);
	}

	@Test // GH-964
	void subscribeAfterStart() throws Exception {

		AtomicInteger subscriptions1 = new AtomicInteger();
		AtomicInteger subscriptions2 = new AtomicInteger();
		CountDownLatch received = new CountDownLatch(2);

		CompositeListener listener1 = new CompositeListener() {
			@Override
			public void onMessage(Message message, byte @Nullable [] pattern) {
				received.countDown();
			}

			@Override
			public void onPatternSubscribed(byte[] pattern, long count) {
				subscriptions1.incrementAndGet();
			}
		};

		CompositeListener listener2 = new CompositeListener() {
			@Override
			public void onMessage(Message message, byte @Nullable [] pattern) {
				received.countDown();
			}

			@Override
			public void onPatternSubscribed(byte[] pattern, long count) {
				subscriptions2.incrementAndGet();
			}
		};

		container.start();

		container.addMessageListener(listener1, new PatternTopic("a"));
		container.addMessageListener(listener2, new PatternTopic("a"));

		try (RedisConnection connection = connectionFactory.getConnection()) {
			connection.publish("a".getBytes(), "hello".getBytes());
		}

		assertThat(received.await(2, TimeUnit.SECONDS)).isTrue();
		container.destroy();

		shortAwait().until(() -> subscriptions1.get() > 0 || subscriptions2.get() > 0);

		assertThat(subscriptions1.get() + subscriptions2.get()).isGreaterThan(0);
	}

	@Test // GH-964
	void multipleStarts() throws Exception {

		AtomicInteger subscriptions = new AtomicInteger();
		CountDownLatch received = new CountDownLatch(1);

		CompositeListener listener1 = new CompositeListener() {
			@Override
			public void onMessage(Message message, byte @Nullable [] pattern) {
				received.countDown();
			}

			@Override
			public void onPatternSubscribed(byte[] pattern, long count) {
				subscriptions.incrementAndGet();
			}
		};

		container.start();
		container.addMessageListener(listener1, new PatternTopic("a"));
		container.stop();
		container.start();

		// Listeners run on a listener executor and they can be notified later
		shortAwait().untilAtomic(subscriptions, Matchers.is(2));
		assertThat(subscriptions.get()).isEqualTo(2);

		try (RedisConnection connection = connectionFactory.getConnection()) {
			connection.publish("a".getBytes(), "hello".getBytes());
		}

		assertThat(received.await(2, TimeUnit.SECONDS)).isTrue();
		container.destroy();
	}

	@Test // GH-964
	void shouldRegisterChannelsAndTopics() throws Exception {

		AtomicInteger subscriptions = new AtomicInteger();
		CountDownLatch received = new CountDownLatch(2);

		CompositeListener patternListener = new CompositeListener() {
			@Override
			public void onMessage(Message message, byte @Nullable [] pattern) {
				if (message.toString().contains("pattern")) {
					received.countDown();
				}
			}

			@Override
			public void onPatternSubscribed(byte[] pattern, long count) {
				subscriptions.incrementAndGet();
			}
		};

		CompositeListener channelListener = new CompositeListener() {
			@Override
			public void onMessage(Message message, byte @Nullable [] pattern) {
				if (message.toString().contains("channel")) {
					received.countDown();
				}
			}

			@Override
			public void onChannelSubscribed(byte[] channel, long count) {
				subscriptions.incrementAndGet();
			}
		};

		container.start();

		container.addMessageListener(patternListener, new PatternTopic("a-pattern-0"));
		container.addMessageListener(patternListener, new PatternTopic("a-pattern-1"));
		container.addMessageListener(channelListener, new ChannelTopic("a-channel-0"));
		container.addMessageListener(channelListener, new ChannelTopic("a-channel-1"));

		// Listeners run on a listener executor and they can be notified later
		shortAwait().untilAtomic(subscriptions, Matchers.is(4));
		assertThat(subscriptions.get()).isEqualTo(4);

		try (RedisConnection connection = connectionFactory.getConnection()) {
			connection.publish("a-pattern-1".getBytes(), "pattern".getBytes());
			connection.publish("a-channel-0".getBytes(), "channel".getBytes());
		}

		assertThat(received.await(2, TimeUnit.SECONDS)).isTrue();
		container.destroy();
	}

	/**
	 * Tests the variants of {@link RedisMessageListenerContainer#removeMessageListener} to ensure the removal stops
	 * message delivery and that the pattern and channel listeners do not interfere with one another upon removal.
	 */
	@Test // GH-3237
	void shouldRemoveListenerFromChannelsAndTopics() throws Exception {

		AtomicInteger subscriptions = new AtomicInteger();
		Map<String, String> patternsMessages = new HashMap<>();
		Map<String, String> channelsMessages = new HashMap<>();

		MessageListener patternListener = new CompositeListener() {
			@Override
			public void onMessage(Message message, byte @Nullable [] pattern) {
				patternsMessages.put(new String(pattern), message.toString());
			}

			@Override
			public void onPatternSubscribed(byte[] pattern, long count) {
				subscriptions.incrementAndGet();
			}

		};

		CompositeListener channelListener = new CompositeListener() {
			@Override
			public void onMessage(Message message, byte @Nullable [] pattern) {
				channelsMessages.put(new String(pattern), message.toString());
			}

			@Override
			public void onChannelSubscribed(byte[] channel, long count) {
				subscriptions.incrementAndGet();
			}

		};

		try {
			container.start();
			container.addMessageListener(patternListener, new PatternTopic("a-pattern-0"));
			container.addMessageListener(patternListener, new PatternTopic("a-pattern-1"));
			container.addMessageListener(patternListener, new PatternTopic("a-pattern-2"));
			container.addMessageListener(channelListener, new ChannelTopic("a-channel-0"));
			container.addMessageListener(channelListener, new ChannelTopic("a-channel-1"));

			// Wait for the subscriptions to register
			shortAwait().untilAtomic(subscriptions, Matchers.is(5));

			try (RedisConnection connection = connectionFactory.getConnection()) {

				// remove patternListener from single topic
				container.removeMessageListener(patternListener, new PatternTopic("a-pattern-0"));

				// send each topic a msg - removed listener should not get msg
				connection.publish("a-pattern-0".getBytes(), "pattern100".getBytes());
				connection.publish("a-pattern-1".getBytes(), "pattern101".getBytes());
				connection.publish("a-pattern-2".getBytes(), "pattern102".getBytes());
				shortAwait().untilAsserted(() -> {
					assertThat(patternsMessages).doesNotContain(entry("a-pattern-0", "pattern100"))
							.contains(entry("a-pattern-1", "pattern101"), entry("a-pattern-2", "pattern102"));
				});
				patternsMessages.clear();

				// remove pattern listener from multiple (2 remaining topics)
				container.removeMessageListener(patternListener,
						Set.of(new PatternTopic("a-pattern-1"), new PatternTopic("a-pattern-2")));

				// no listener should receive msgs
				connection.publish("a-pattern-0".getBytes(), "pattern100".getBytes());
				connection.publish("a-pattern-1".getBytes(), "pattern101".getBytes());
				connection.publish("a-pattern-2".getBytes(), "pattern102".getBytes());
				shortAwait().untilAsserted(() -> assertThat(patternsMessages).isEmpty());

				// sanity check sending msgs to channels (removal of pattern listeners did not disrupt)
				connection.publish("a-channel-0".getBytes(), "channel100".getBytes());
				connection.publish("a-channel-1".getBytes(), "channel101".getBytes());
				shortAwait().untilAsserted(() -> {
					assertThat(channelsMessages).containsExactly(entry("a-channel-0", "channel100"),
							entry("a-channel-1", "channel101"));
				});
				channelsMessages.clear();

				// remove channel listener from all channel topics
				container.removeMessageListener(channelListener);

				// should receive no msgs
				connection.publish("a-channel-0".getBytes(), "channel100".getBytes());
				connection.publish("a-channel-1".getBytes(), "channel101".getBytes());
				shortAwait().untilAsserted(() -> assertThat(channelsMessages).isEmpty());
			}
		}
		finally {
			container.destroy();
		}
	}

	interface CompositeListener extends MessageListener, SubscriptionListener {

	}
}
