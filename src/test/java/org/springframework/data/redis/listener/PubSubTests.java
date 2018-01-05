/*
 * Copyright 2011-2018 the original author or authors.
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
package org.springframework.data.redis.listener;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import static org.junit.Assume.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.data.redis.ConnectionFactoryTracker;
import org.springframework.data.redis.ObjectFactory;
import org.springframework.data.redis.RedisTestProfileValueSource;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.listener.adapter.MessageListenerAdapter;
import org.springframework.data.redis.test.util.RedisSentinelRule;

/**
 * Base test class for PubSub integration tests
 *
 * @author Costin Leau
 * @author Jennifer Hickey
 */
@RunWith(Parameterized.class)
public class PubSubTests<T> {

	public @Rule RedisSentinelRule sentinelRule = RedisSentinelRule.withDefaultConfig().sentinelsDisabled();

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

	@BeforeClass
	public static void shouldRun() {
		assumeTrue(RedisTestProfileValueSource.matches("runLongTests", "true"));
	}

	@Before
	public void setUp() throws Exception {
		bag.clear();

		adapter.setSerializer(template.getValueSerializer());
		adapter.afterPropertiesSet();

		container = new RedisMessageListenerContainer();
		container.setConnectionFactory(template.getConnectionFactory());
		container.setBeanName("container");
		container.addMessageListener(adapter, Arrays.asList(new ChannelTopic(CHANNEL)));
		container.setTaskExecutor(new SyncTaskExecutor());
		container.setSubscriptionExecutor(new SimpleAsyncTaskExecutor());
		container.afterPropertiesSet();
		container.start();

		Thread.sleep(1000);
	}

	@After
	public void tearDown() throws Exception {
		container.destroy();
	}

	@SuppressWarnings("rawtypes")
	public PubSubTests(ObjectFactory<T> factory, RedisTemplate template) {
		this.factory = factory;
		this.template = template;
		ConnectionFactoryTracker.add(template.getConnectionFactory());
	}

	@AfterClass
	public static void cleanUp() {
		ConnectionFactoryTracker.cleanUp();
	}

	@Parameters
	public static Collection<Object[]> testParams() {
		return PubSubTestParams.testParams();
	}

	/**
	 * Return a new instance of T
	 *
	 * @return
	 */
	protected T getT() {
		return factory.instance();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testContainerSubscribe() throws Exception {
		T payload1 = getT();
		T payload2 = getT();

		template.convertAndSend(CHANNEL, payload1);
		template.convertAndSend(CHANNEL, payload2);

		Set<T> set = new LinkedHashSet<>();
		set.add((T) bag.poll(1, TimeUnit.SECONDS));
		set.add((T) bag.poll(1, TimeUnit.SECONDS));

		assertThat(set, hasItems(payload1, payload2));
	}

	@Test
	public void testMessageBatch() throws Exception {
		int COUNT = 10;
		for (int i = 0; i < COUNT; i++) {
			template.convertAndSend(CHANNEL, getT());
		}

		Thread.sleep(1000);
		assertEquals(COUNT, bag.size());
	}

	@Test
	public void testContainerUnsubscribe() throws Exception {
		T payload1 = getT();
		T payload2 = getT();

		container.removeMessageListener(adapter, new ChannelTopic(CHANNEL));
		template.convertAndSend(CHANNEL, payload1);
		template.convertAndSend(CHANNEL, payload2);

		assertNull(bag.poll(1, TimeUnit.SECONDS));
	}

	@Test
	public void testStartNoListeners() {
		container.removeMessageListener(adapter, new ChannelTopic(CHANNEL));
		container.stop();
		// DATREDIS-207 This test previously took 5 seconds on start due to monitor wait
		container.start();
	}

	@SuppressWarnings("unchecked")
	@Test // DATAREDIS-251
	public void testStartListenersToNoSpecificChannelTest() throws InterruptedException {
		container.removeMessageListener(adapter, new ChannelTopic(CHANNEL));
		container.addMessageListener(adapter, Arrays.asList(new PatternTopic("*")));
		container.start();

		Thread.sleep(1000); // give the container a little time to recover

		T payload = getT();

		template.convertAndSend(CHANNEL, payload);

		Set<T> set = new LinkedHashSet<>();
		set.add((T) bag.poll(3, TimeUnit.SECONDS));

		assertThat(set, hasItems(payload));
	}
}
