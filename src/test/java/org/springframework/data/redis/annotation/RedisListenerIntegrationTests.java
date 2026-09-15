/*
 * Copyright 2026 the original author or authors.
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
package org.springframework.data.redis.annotation;

import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.*;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.config.RedisListenerConfigUtils;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.SubscriptionListener;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.connection.jedis.extension.JedisConnectionFactoryExtension;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.lettuce.extension.LettuceConnectionFactoryExtension;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.test.extension.RedisStandalone;

/**
 * Integration test for {@link EnableRedisListeners} and {@link RedisListener}.
 *
 * @author Mark Paluch
 * @author Ilyass Bougati
 */
@ParameterizedClass
@MethodSource("testParams")
public class RedisListenerIntegrationTests {

	private RedisConnectionFactory connectionFactory;
	private final AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();

	public RedisListenerIntegrationTests(RedisConnectionFactory connectionFactory) {
		this.connectionFactory = connectionFactory;
	}

	static Collection<Arguments> testParams() {
		// Jedis
		JedisConnectionFactory jedisConnFactory = JedisConnectionFactoryExtension
				.getConnectionFactory(RedisStandalone.class);

		// Lettuce
		LettuceConnectionFactory lettuceConnFactory = LettuceConnectionFactoryExtension
				.getConnectionFactory(RedisStandalone.class);

		return List.of(Arguments.argumentSet("Jedis", jedisConnFactory),
				Arguments.argumentSet("Lettuce", lettuceConnFactory));
	}

	@Test // GH-1004
	void shouldListenForMessage() throws InterruptedException {

		startContext(Config.class);

		StringRedisTemplate template = new StringRedisTemplate();
		template.setConnectionFactory(connectionFactory);
		template.afterPropertiesSet();

		MyListener bean = context.getBean(MyListener.class);
		bean.message.clear();

		template.convertAndSend("my-channel-listener", "Hello Redis!");

		String message = bean.message.poll(10, TimeUnit.SECONDS);
		assertThat(message).isEqualTo("Hello Redis!");
	}

	@AfterEach
	void tearDown() {
		context.stop();
	}

	@Test // GH-3439
	void oneSubscriptionPerInstance() {

		startContext(Config.class, SameChannelListener.class);

		SameChannelListener bean = context.getBean(SameChannelListener.class);

		assertSubscribed(bean, "my-subscription-channel");
	}

	@Test // GH-3439
	void oneSubscriptionPerChannel() {

		startContext(Config.class, DifferentChannelsListener.class);

		DifferentChannelsListener bean = context.getBean(DifferentChannelsListener.class);

		assertSubscribed(bean, "my-channel-1", "my-channel-2");
	}

	@Test // GH-3439
	void oneSubscriptionPerPattern() {

		startContext(Config.class, SamePatternListener.class);

		SamePatternListener bean = context.getBean(SamePatternListener.class);

		assertSubscribed(bean, "my-pattern-*");
	}

	@Test // GH-3439
	void notifiesAgainWhenAnotherBeanSubscribes() {

		startContext(Config.class, FirstListener.class, SecondListener.class);

		FirstListener first = context.getBean(FirstListener.class);
		SecondListener second = context.getBean(SecondListener.class);

		// the second bean's SUBSCRIBE is confirmed to both beans
		assertSubscribed(first, "my-shared-channel", "my-shared-channel");
		assertSubscribed(second, "my-shared-channel");
	}

	private void startContext(Class<?>... componentClasses) {

		context.registerBean(RedisListenerConfigUtils.REDIS_MESSAGE_LISTENER_BEAN_NAME, RedisMessageListenerContainer.class,
				() -> {

			RedisMessageListenerContainer container = new RedisMessageListenerContainer();
			container.setRecoveryInterval(100);
			container.setConnectionFactory(connectionFactory);
			return container;
		});

		context.register(componentClasses);
		context.refresh();
	}

	private static void assertSubscribed(RecordingListener listener, String... topics) {
		await().during(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(10))
				.untilAsserted(() -> assertThat(listener.subscribed).containsExactlyInAnyOrder(topics));
	}

	@Configuration
	@EnableRedisListeners
	static class Config {

		@Bean
		MyListener myListener() {
			return new MyListener();
		}
	}

	static class MyListener {

		LinkedBlockingQueue<String> message = new LinkedBlockingQueue<>();

		@RedisListener("my-channel-listener")
		void onMessage(String msg) {
			message.offer(msg);
		}

	}

	abstract static class RecordingListener implements SubscriptionListener {

		final List<String> subscribed = new CopyOnWriteArrayList<>();

		@Override
		public void onChannelSubscribed(byte[] channel, long count) {
			subscribed.add(new String(channel));
		}

		@Override
		public void onPatternSubscribed(byte[] pattern, long count) {
			subscribed.add(new String(pattern));
		}

	}

	static class SameChannelListener extends RecordingListener {

		@RedisListener("my-subscription-channel")
		void a(String msg) {}

		@RedisListener("my-subscription-channel")
		void b(String msg) {}

	}

	static class DifferentChannelsListener extends RecordingListener {

		@RedisListener("my-channel-1")
		void a(String msg) {}

		@RedisListener("my-channel-2")
		void b(String msg) {}

	}

	static class SamePatternListener extends RecordingListener {

		@RedisListener("my-pattern-*")
		void a(String msg) {}

		@RedisListener("my-pattern-*")
		void b(String msg) {}

	}

	static class FirstListener extends RecordingListener {

		@RedisListener("my-shared-channel")
		void a(String msg) {}

	}

	static class SecondListener extends RecordingListener {

		@RedisListener("my-shared-channel")
		void a(String msg) {}

	}

}
