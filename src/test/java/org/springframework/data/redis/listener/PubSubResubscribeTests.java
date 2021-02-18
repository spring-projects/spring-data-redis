/*
 * Copyright 2011-2021 the original author or authors.
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

import static org.awaitility.Awaitility.*;
import static org.junit.Assume.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.connection.jedis.extension.JedisConnectionFactoryExtension;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.lettuce.extension.LettuceConnectionFactoryExtension;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.listener.adapter.MessageListenerAdapter;
import org.springframework.data.redis.test.condition.EnabledIfLongRunningTest;
import org.springframework.data.redis.test.condition.RedisDetector;
import org.springframework.data.redis.test.extension.RedisCluster;
import org.springframework.data.redis.test.extension.RedisStanalone;
import org.springframework.data.redis.test.extension.parametrized.MethodSource;
import org.springframework.data.redis.test.extension.parametrized.ParameterizedRedisTest;

/**
 * @author Costin Leau
 * @author Jennifer Hickey
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@MethodSource("testParams")
@EnabledIfLongRunningTest
public class PubSubResubscribeTests {

	private static final String CHANNEL = "pubsub::test";

	private final BlockingDeque<String> bag = new LinkedBlockingDeque<>(99);
	private final Object handler = new MessageHandler("handler1", bag);
	private final MessageListenerAdapter adapter = new MessageListenerAdapter(handler);

	private RedisMessageListenerContainer container;
	private RedisConnectionFactory factory;

	@SuppressWarnings("rawtypes") //
	private RedisTemplate template;

	public PubSubResubscribeTests(RedisConnectionFactory connectionFactory) {
		this.factory = connectionFactory;
	}

	public static Collection<Object[]> testParams() {

		int port = SettingsUtils.getPort();
		String host = SettingsUtils.getHost();

		List<RedisConnectionFactory> factories = new ArrayList<>(3);

		// Jedis
		JedisConnectionFactory jedisConnFactory = JedisConnectionFactoryExtension
				.getConnectionFactory(RedisStanalone.class);

		factories.add(jedisConnFactory);

		// Lettuce
		LettuceConnectionFactory lettuceConnFactory = LettuceConnectionFactoryExtension
				.getConnectionFactory(RedisStanalone.class);

		factories.add(lettuceConnFactory);

		if (clusterAvailable()) {

			LettuceConnectionFactory lettuceClusterConnFactory = LettuceConnectionFactoryExtension
					.getConnectionFactory(RedisCluster.class);

			factories.add(lettuceClusterConnFactory);
		}

		return factories.stream().map(factory -> new Object[] { factory }).collect(Collectors.toList());
	}

	@BeforeEach
	void setUp() throws Exception {

		template = new StringRedisTemplate(factory);

		adapter.setSerializer(template.getValueSerializer());
		adapter.afterPropertiesSet();

		container = new RedisMessageListenerContainer();
		container.setConnectionFactory(template.getConnectionFactory());
		container.setBeanName("container");
		container.setTaskExecutor(new SyncTaskExecutor());
		container.setSubscriptionExecutor(new SimpleAsyncTaskExecutor());
		container.afterPropertiesSet();
		container.start();
	}

	@AfterEach
	void tearDown() {
		container.stop();
		bag.clear();
	}

	@ParameterizedRedisTest
	@EnabledIfLongRunningTest
	void testContainerPatternResubscribe() throws Exception {

		String payload1 = "do";
		String payload2 = "re mi";

		final String PATTERN = "p*";
		final String ANOTHER_CHANNEL = "pubsub::test::extra";

		BlockingDeque<String> bag2 = new LinkedBlockingDeque<>(99);
		MessageListenerAdapter anotherListener = new MessageListenerAdapter(new MessageHandler("handler2", bag2));
		anotherListener.setSerializer(template.getValueSerializer());
		anotherListener.afterPropertiesSet();

		// remove adapter from all channels
		container.addMessageListener(anotherListener, new PatternTopic(PATTERN));

		// Wait for async subscription tasks to setup
		Thread.sleep(400);

		// test no messages are sent just to patterns
		template.convertAndSend(CHANNEL, payload1);
		template.convertAndSend(ANOTHER_CHANNEL, payload2);

		await().atMost(Duration.ofSeconds(2)).until(() -> bag2.contains(payload1) && bag2.contains(payload2));

		// bind original listener on another channel
		container.addMessageListener(adapter, new ChannelTopic(ANOTHER_CHANNEL));

		// Wait for async subscription tasks to setup
		Thread.sleep(400);

		template.convertAndSend(CHANNEL, payload1);
		template.convertAndSend(ANOTHER_CHANNEL, payload2);

		await().atMost(Duration.ofSeconds(2)).until(() -> bag.contains(payload2));

		// another listener receives messages on both channels
		await().atMost(Duration.ofSeconds(2)).until(() -> bag2.contains(payload1) && bag2.contains(payload2));
	}

	@ParameterizedRedisTest
	void testContainerChannelResubscribe() throws Exception {

		String payload1 = "do";
		String payload2 = "re mi";

		String anotherPayload1 = "od";
		String anotherPayload2 = "mi er";

		String ANOTHER_CHANNEL = "pubsub::test::extra";

		// bind listener on another channel
		container.addMessageListener(adapter, new ChannelTopic(ANOTHER_CHANNEL));
		container.removeMessageListener(null, new ChannelTopic(CHANNEL));

		// timing: There's currently no other way to synchronize
		// than to hope the subscribe/unsubscribe are executed within the time.
		Thread.sleep(400);

		// Listener removed from channel
		template.convertAndSend(CHANNEL, payload1);
		template.convertAndSend(CHANNEL, payload2);

		// Listener receives messages on another channel
		template.convertAndSend(ANOTHER_CHANNEL, anotherPayload1);
		template.convertAndSend(ANOTHER_CHANNEL, anotherPayload2);

		await().atMost(Duration.ofSeconds(2)).until(() -> bag.contains(anotherPayload1) && bag.contains(anotherPayload2));
	}

	/**
	 * Validates the behavior of {@link RedisMessageListenerContainer} when it needs to spin up a thread executing its
	 * PatternSubscriptionTask
	 *
	 * @throws Exception
	 */
	@ParameterizedRedisTest
	void testInitializeContainerWithMultipleTopicsIncludingPattern() throws Exception {

		assumeFalse(isClusterAware(template.getConnectionFactory()));

		container.stop();

		String uniqueChannel = "random-" + UUID.randomUUID();
		PubSubAwaitUtil.runAndAwaitPatternSubscription(template.getConnectionFactory(), () -> {

			container.addMessageListener(adapter,
					Arrays.asList(new Topic[] { new ChannelTopic(uniqueChannel), new PatternTopic("s*") }));
			container.start();
		});

		// timing: There's currently no other way to synchronize
		// than to hope the subscribe/unsubscribe are executed within the time.
		Thread.sleep(250);

		template.convertAndSend("somechannel", "HELLO");
		template.convertAndSend(uniqueChannel, "WORLD");

		await().atMost(Duration.ofSeconds(2)).until(() -> bag.contains("HELLO") && bag.contains("WORLD"));
	}

	private class MessageHandler {

		private final BlockingDeque<String> bag;
		private final String name;

		MessageHandler(String name, BlockingDeque<String> bag) {

			this.bag = bag;
			this.name = name;
		}

		@SuppressWarnings("unused")
		public void handleMessage(String message) {
			bag.add(message);
		}
	}

	private static boolean clusterAvailable() {
		return RedisDetector.isClusterAvailable();
	}

	private static boolean isClusterAware(RedisConnectionFactory connectionFactory) {

		if (connectionFactory instanceof LettuceConnectionFactory) {
			return ((LettuceConnectionFactory) connectionFactory).isClusterAware();
		} else if (connectionFactory instanceof JedisConnectionFactory) {
			return ((JedisConnectionFactory) connectionFactory).isRedisClusterAware();
		}
		return false;
	}
}
