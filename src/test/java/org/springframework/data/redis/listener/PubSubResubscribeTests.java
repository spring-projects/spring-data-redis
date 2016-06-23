/*
 * Copyright 2011-2016 the original author or authors.
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

import static org.junit.Assert.*;
import static org.junit.Assume.*;
import static org.springframework.data.redis.SpinBarrier.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.data.redis.ConnectionFactoryTracker;
import org.springframework.data.redis.RedisTestProfileValueSource;
import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.TestCondition;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceTestClientResources;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.listener.adapter.MessageListenerAdapter;

/**
 * @author Costin Leau
 * @author Jennifer Hickey
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@RunWith(Parameterized.class)
public class PubSubResubscribeTests {

	private static final String CHANNEL = "pubsub::test";

	private final BlockingDeque<String> bag = new LinkedBlockingDeque<String>(99);
	private final Object handler = new MessageHandler("handler1", bag);
	private final MessageListenerAdapter adapter = new MessageListenerAdapter(handler);

	private RedisMessageListenerContainer container;
	private RedisConnectionFactory factory;

	@SuppressWarnings("rawtypes") //
	private RedisTemplate template;

	public PubSubResubscribeTests(RedisConnectionFactory connectionFactory) {

		this.factory = connectionFactory;
		ConnectionFactoryTracker.add(factory);
	}

	@BeforeClass
	public static void shouldRun() {
		assumeTrue(RedisTestProfileValueSource.matches("runLongTests", "true"));
	}

	@AfterClass
	public static void cleanUp() {
		ConnectionFactoryTracker.cleanUp();
	}

	@Parameters
	public static Collection<Object[]> testParams() {

		int port = SettingsUtils.getPort();
		String host = SettingsUtils.getHost();

		// Jedis
		JedisConnectionFactory jedisConnFactory = new JedisConnectionFactory();
		jedisConnFactory.setUsePool(false);
		jedisConnFactory.setPort(port);
		jedisConnFactory.setHostName(host);
		jedisConnFactory.setDatabase(2);
		jedisConnFactory.afterPropertiesSet();

		// Lettuce
		LettuceConnectionFactory lettuceConnFactory = new LettuceConnectionFactory();
		lettuceConnFactory.setClientResources(LettuceTestClientResources.getSharedClientResources());
		lettuceConnFactory.setPort(port);
		lettuceConnFactory.setHostName(host);
		lettuceConnFactory.setDatabase(2);
		lettuceConnFactory.setValidateConnection(true);
		lettuceConnFactory.afterPropertiesSet();

		return Arrays.asList(new Object[][] { { jedisConnFactory }, { lettuceConnFactory } });
	}

	@Before
	public void setUp() throws Exception {

		template = new StringRedisTemplate(factory);

		adapter.setSerializer(template.getValueSerializer());
		adapter.afterPropertiesSet();

		container = new RedisMessageListenerContainer();
		container.setConnectionFactory(template.getConnectionFactory());
		container.setBeanName("container");
		container.addMessageListener(adapter, new ChannelTopic(CHANNEL));
		container.setTaskExecutor(new SyncTaskExecutor());
		container.setSubscriptionExecutor(new SimpleAsyncTaskExecutor());
		container.afterPropertiesSet();
		container.start();

		waitFor(new TestCondition() {
			@Override
			public boolean passes() {
				return container.getConnectionFactory().getConnection().isSubscribed();
			}
		}, 1000);
	}

	@After
	public void tearDown() {
		bag.clear();
	}

	@Test
	public void testContainerPatternResubscribe() throws Exception {

		String payload1 = "do";
		String payload2 = "re mi";

		final String PATTERN = "p*";
		final String ANOTHER_CHANNEL = "pubsub::test::extra";

		BlockingDeque<String> bag2 = new LinkedBlockingDeque<String>(99);
		MessageListenerAdapter anotherListener = new MessageListenerAdapter(new MessageHandler("handler2", bag2));
		anotherListener.setSerializer(template.getValueSerializer());
		anotherListener.afterPropertiesSet();

		// remove adapter from all channels
		container.addMessageListener(anotherListener, new PatternTopic(PATTERN));
		container.removeMessageListener(adapter);

		// Wait for async subscription tasks to setup
		Thread.sleep(400);

		// test no messages are sent just to patterns
		template.convertAndSend(CHANNEL, payload1);
		template.convertAndSend(ANOTHER_CHANNEL, payload2);

		// anotherListener receives both messages
		List<String> msgs = new ArrayList<String>();
		msgs.add(bag2.poll(500, TimeUnit.MILLISECONDS));
		msgs.add(bag2.poll(500, TimeUnit.MILLISECONDS));

		assertEquals(2, msgs.size());
		assertTrue(msgs.contains(payload1));
		assertTrue(msgs.contains(payload2));
		msgs.clear();

		// unsubscribed adapter did not receive message
		assertNull(bag.poll(500, TimeUnit.MILLISECONDS));

		// bind original listener on another channel
		container.addMessageListener(adapter, new ChannelTopic(ANOTHER_CHANNEL));

		// Wait for async subscription tasks to setup
		Thread.sleep(400);

		template.convertAndSend(CHANNEL, payload1);
		template.convertAndSend(ANOTHER_CHANNEL, payload2);

		// original listener received only one message on another channel
		msgs.clear();
		msgs.add(bag.poll(500, TimeUnit.MILLISECONDS));
		msgs.add(bag.poll(500, TimeUnit.MILLISECONDS));

		assertTrue(msgs.contains(payload2));
		assertTrue(msgs.contains(null));

		// another listener receives messages on both channels
		msgs.clear();
		msgs.add(bag2.poll(500, TimeUnit.MILLISECONDS));
		msgs.add(bag2.poll(500, TimeUnit.MILLISECONDS));
		assertEquals(2, msgs.size());
		assertTrue(msgs.contains(payload1));
		assertTrue(msgs.contains(payload2));
	}

	@Test
	public void testContainerChannelResubscribe() throws Exception {

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

		Set<String> set = new LinkedHashSet<String>();
		set.add(bag.poll(500, TimeUnit.MILLISECONDS));
		set.add(bag.poll(500, TimeUnit.MILLISECONDS));

		assertFalse(set.contains(payload1));
		assertFalse(set.contains(payload2));

		assertTrue(set.contains(anotherPayload1));
		assertTrue(set.contains(anotherPayload2));
	}

	/**
	 * Validates the behavior of {@link RedisMessageListenerContainer} when it needs to spin up a thread executing its
	 * PatternSubscriptionTask
	 * 
	 * @throws Exception
	 */
	@Test
	public void testInitializeContainerWithMultipleTopicsIncludingPattern() throws Exception {

		container.removeMessageListener(adapter);
		container.stop();
		container.addMessageListener(adapter,
				Arrays.asList(new Topic[] { new ChannelTopic(CHANNEL), new PatternTopic("s*") }));
		container.start();

		// timing: There's currently no other way to synchronize
		// than to hope the subscribe/unsubscribe are executed within the time.
		Thread.sleep(1000);

		template.convertAndSend("somechannel", "HELLO");
		template.convertAndSend(CHANNEL, "WORLD");

		Set<String> set = new LinkedHashSet<String>();
		set.add(bag.poll(500, TimeUnit.MILLISECONDS));
		set.add(bag.poll(500, TimeUnit.MILLISECONDS));

		assertEquals(new HashSet<String>(Arrays.asList(new String[] { "HELLO", "WORLD" })), set);
	}

	private class MessageHandler {

		private final BlockingDeque<String> bag;
		private final String name;

		public MessageHandler(String name, BlockingDeque<String> bag) {

			this.bag = bag;
			this.name = name;
		}

		@SuppressWarnings("unused")
		public void handleMessage(String message) {
			bag.add(message);
		}
	}
}
