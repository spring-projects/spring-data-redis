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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import org.springframework.context.annotation.AnnotationConfigApplicationContext;
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

		startContext(Config.class, MyListener.class);

		MyListener bean = context.getBean(MyListener.class);
		bean.message.clear();

		template().convertAndSend("my-channel-listener", "Hello Redis!");

		String message = bean.message.poll(10, TimeUnit.SECONDS);
		assertThat(message).isEqualTo("Hello Redis!");
	}

	@AfterEach
	void tearDown() {
		context.stop();
	}

	@Test // GH-3439
	void shouldNotifySubscriptionListenerOncePerAnnotationByDefault() throws InterruptedException {

		startContext(Config.class, SubscriptionAwareListener.class);

		SubscriptionAwareListener bean = context.getBean(SubscriptionAwareListener.class);

		// two methods on the same channel register two listeners; Redis' confirmation is copied to both, so the bean
		// is notified more than once for what is conceptually a single subscription event (unlike the grouped case)
		assertThat(bean.subscribedChannel.poll(10, TimeUnit.SECONDS)).isEqualTo("my-subscription-channel");
		assertThat(bean.subscribedChannel.poll(10, TimeUnit.SECONDS)).isEqualTo("my-subscription-channel");
	}

	@Test // GH-3439
	void shouldInvokeAllMethodsWhenGrouped() throws InterruptedException {

		startContext(GroupedConfig.class, GroupedListener.class);

		GroupedListener bean = context.getBean(GroupedListener.class);

		template().convertAndSend("my-channel-grouped", "Hello Redis!");

		assertThat(bean.messagesA.poll(10, TimeUnit.SECONDS)).isEqualTo("Hello Redis!");
		assertThat(bean.messagesB.poll(10, TimeUnit.SECONDS)).isEqualTo("Hello Redis!");
	}

	@Test // GH-3439
	void shouldNotifySubscriptionListenerOnceWhenGrouped() throws InterruptedException {

		startContext(GroupedConfig.class, SubscriptionAwareListener.class);

		SubscriptionAwareListener bean = context.getBean(SubscriptionAwareListener.class);

		assertThat(bean.subscribedChannel.poll(10, TimeUnit.SECONDS)).isEqualTo("my-subscription-channel");

		// only this bean listens on the channel: no further confirmation is re-sent
		await().during(Duration.ofMillis(500)).atMost(Duration.ofSeconds(2)).until(() -> bean.subscribedChannel.isEmpty());
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

	private StringRedisTemplate template() {

		StringRedisTemplate template = new StringRedisTemplate();
		template.setConnectionFactory(connectionFactory);
		template.afterPropertiesSet();
		return template;
	}

	@Configuration
	@EnableRedisListeners
	static class Config {

	}

	static class MyListener {

		LinkedBlockingQueue<String> message = new LinkedBlockingQueue<>();

		@RedisListener("my-channel-listener")
		void onMessage(String msg) {
			message.offer(msg);
		}

	}

	@Configuration
	@EnableRedisListeners(grouping = ListenerGrouping.PER_BEAN_AND_TOPIC)
	static class GroupedConfig {

	}

	static class SubscriptionAwareListener implements SubscriptionListener {

		LinkedBlockingQueue<String> subscribedChannel = new LinkedBlockingQueue<>();

		@RedisListener("my-subscription-channel")
		void a(String msg) {}

		@RedisListener("my-subscription-channel")
		void b(String msg) {}

		@Override
		public void onChannelSubscribed(byte[] channel, long count) {
			subscribedChannel.offer(new String(channel));
		}

	}

	static class GroupedListener {

		LinkedBlockingQueue<String> messagesA = new LinkedBlockingQueue<>();
		LinkedBlockingQueue<String> messagesB = new LinkedBlockingQueue<>();

		@RedisListener("my-channel-grouped")
		void a(String msg) {
			messagesA.offer(msg);
		}

		@RedisListener("my-channel-grouped")
		void b(String msg) {
			messagesB.offer(msg);
		}

	}

}
