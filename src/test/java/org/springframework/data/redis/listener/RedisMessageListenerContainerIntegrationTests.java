/*
 * Copyright 2016-2021 the original author or authors.
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

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.SubscriptionListener;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.connection.jedis.extension.JedisConnectionFactoryExtension;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.lettuce.extension.LettuceConnectionFactoryExtension;
import org.springframework.data.redis.test.extension.RedisStanalone;
import org.springframework.data.redis.test.extension.parametrized.MethodSource;
import org.springframework.data.redis.test.extension.parametrized.ParameterizedRedisTest;
import org.springframework.lang.Nullable;

/**
 * Integration tests for {@link RedisMessageListenerContainer}.
 *
 * @author Mark Paluch
 */
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
		container.setConnectionFactory(connectionFactory);
		container.setBeanName("container");
		container.afterPropertiesSet();
	}

	public static Collection<Object[]> testParams() {

		// Jedis
		JedisConnectionFactory jedisConnFactory = JedisConnectionFactoryExtension
				.getConnectionFactory(RedisStanalone.class);

		// Lettuce
		LettuceConnectionFactory lettuceConnFactory = LettuceConnectionFactoryExtension
				.getConnectionFactory(RedisStanalone.class);

		return Arrays.asList(new Object[][] { { jedisConnFactory }, { lettuceConnFactory } });
	}

	@AfterEach
	void tearDown() throws Exception {
		container.destroy();
	}

	@ParameterizedRedisTest
	void notifiesChannelSubscriptionState() throws Exception {

		AtomicReference<String> onSubscribe = new AtomicReference<>();
		AtomicReference<String> onUnsubscribe = new AtomicReference<>();
		CompletableFuture<Void> subscribe = new CompletableFuture<>();
		CompletableFuture<Void> unsubscribe = new CompletableFuture<>();

		CompositeListener listener = new CompositeListener() {
			@Override
			public void onMessage(Message message, @Nullable byte[] pattern) {

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

	@ParameterizedRedisTest
	void notifiesPatternSubscriptionState() throws Exception {

		AtomicReference<String> onPsubscribe = new AtomicReference<>();
		AtomicReference<String> onPunsubscribe = new AtomicReference<>();
		CompletableFuture<Void> psubscribe = new CompletableFuture<>();
		CompletableFuture<Void> punsubscribe = new CompletableFuture<>();

		CompositeListener listener = new CompositeListener() {
			@Override
			public void onMessage(Message message, @Nullable byte[] pattern) {

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

	@ParameterizedRedisTest
	void repeatedSubscribeShouldNotifyOnlyOnce() throws Exception {

		AtomicInteger subscriptions1 = new AtomicInteger();
		AtomicInteger subscriptions2 = new AtomicInteger();
		CountDownLatch received = new CountDownLatch(2);

		CompositeListener listener1 = new CompositeListener() {
			@Override
			public void onMessage(Message message, @Nullable byte[] pattern) {
				received.countDown();
			}

			@Override
			public void onPatternSubscribed(byte[] pattern, long count) {
				subscriptions1.incrementAndGet();
			}
		};

		CompositeListener listener2 = new CompositeListener() {
			@Override
			public void onMessage(Message message, @Nullable byte[] pattern) {
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

		assertThat(subscriptions1).hasValue(1);
		assertThat(subscriptions2).hasValue(1);
	}

	interface CompositeListener extends MessageListener, SubscriptionListener {

	}
}
