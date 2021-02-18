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

import static org.assertj.core.api.Assertions.*;
import static org.assertj.core.api.Assumptions.*;
import static org.awaitility.Awaitility.*;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.data.redis.ObjectFactory;
import org.springframework.data.redis.connection.ConnectionUtils;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.listener.adapter.MessageListenerAdapter;
import org.springframework.data.redis.test.condition.EnabledIfLongRunningTest;
import org.springframework.data.redis.test.extension.parametrized.MethodSource;
import org.springframework.data.redis.test.extension.parametrized.ParameterizedRedisTest;

/**
 * Base test class for PubSub integration tests
 *
 * @author Costin Leau
 * @author Jennifer Hickey
 * @author Mark Paluch
 */
@MethodSource("testParams")
public class PubSubTests<T> {

	private static final String CHANNEL = "pubsub::test";

	protected RedisMessageListenerContainer container;
	protected ObjectFactory<T> factory;
	@SuppressWarnings("rawtypes") protected RedisTemplate template;

	private final BlockingDeque<Object> bag = new LinkedBlockingDeque<>(99);

	private final Object handler = new Object() {
		@SuppressWarnings("unused")
		public void handleMessage(Object message) {
			bag.add(message);
		}
	};

	private final MessageListenerAdapter adapter = new MessageListenerAdapter(handler);

	@SuppressWarnings("rawtypes")
	public PubSubTests(ObjectFactory<T> factory, RedisTemplate template) {
		this.factory = factory;
		this.template = template;
	}

	public static Collection<Object[]> testParams() {
		return PubSubTestParams.testParams();
	}

	@BeforeEach
	void setUp() throws Exception {
		bag.clear();

		adapter.setSerializer(template.getValueSerializer());
		adapter.afterPropertiesSet();

		Phaser phaser = new Phaser(1);

		container = new RedisMessageListenerContainer();
		container.setConnectionFactory(template.getConnectionFactory());
		container.setBeanName("container");
		container.addMessageListener(adapter, Arrays.asList(new ChannelTopic(CHANNEL)));
		container.setTaskExecutor(new SyncTaskExecutor());
		container.setSubscriptionExecutor(new SimpleAsyncTaskExecutor() {
			@Override
			protected void doExecute(Runnable task) {
				super.doExecute(() -> {
					phaser.arriveAndDeregister();
					task.run();
				});
			}
		});
		container.afterPropertiesSet();
		container.start();

		phaser.arriveAndAwaitAdvance();
		Thread.sleep(250);
	}

	@AfterEach
	void tearDown() throws Exception {
		container.destroy();
	}

	/**
	 * Return a new instance of T
	 *
	 * @return
	 */
	T getT() {
		return factory.instance();
	}

	@SuppressWarnings("unchecked")
	@ParameterizedRedisTest
	void testContainerSubscribe() throws Exception {
		T payload1 = getT();
		T payload2 = getT();

		template.convertAndSend(CHANNEL, payload1);
		template.convertAndSend(CHANNEL, payload2);

		await().atMost(Duration.ofSeconds(2)).until(() -> bag.contains(payload1) && bag.contains(payload2));
	}

	@ParameterizedRedisTest
	void testMessageBatch() throws Exception {
		int COUNT = 10;
		for (int i = 0; i < COUNT; i++) {
			template.convertAndSend(CHANNEL, getT());
		}

		for (int i = 0; i < COUNT; i++) {
			assertThat(bag.poll(1, TimeUnit.SECONDS)).as("message #" + i).isNotNull();
		}
	}

	@ParameterizedRedisTest
	@EnabledIfLongRunningTest
	void testContainerUnsubscribe() throws Exception {
		T payload1 = getT();
		T payload2 = getT();

		container.removeMessageListener(adapter, new ChannelTopic(CHANNEL));
		template.convertAndSend(CHANNEL, payload1);
		template.convertAndSend(CHANNEL, payload2);

		assertThat(bag.poll(200, TimeUnit.MILLISECONDS)).isNull();
	}

	@ParameterizedRedisTest
	void testStartNoListeners() {
		container.removeMessageListener(adapter, new ChannelTopic(CHANNEL));
		container.stop();
		// DATREDIS-207 This test previously took 5 seconds on start due to monitor wait
		container.start();
	}

	@SuppressWarnings("unchecked")
	@ParameterizedRedisTest // DATAREDIS-251
	void testStartListenersToNoSpecificChannelTest() throws InterruptedException {

		assumeThat(isClusterAware(template.getConnectionFactory())).isFalse();
		assumeThat(ConnectionUtils.isJedis(template.getConnectionFactory())).isTrue();

		PubSubAwaitUtil.runAndAwaitPatternSubscription(template.getRequiredConnectionFactory(), () -> {

			container.removeMessageListener(adapter, new ChannelTopic(CHANNEL));
			container.addMessageListener(adapter, Collections.singletonList(new PatternTopic(CHANNEL + "*")));
			container.start();
		});

		T payload = getT();

		template.convertAndSend(CHANNEL, payload);

		await().atMost(Duration.ofSeconds(2)).until(() -> bag.contains(payload));
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
