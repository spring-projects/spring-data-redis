/*
 * Copyright 2011-2020 the original author or authors.
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
import static org.junit.Assume.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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
import org.springframework.data.redis.connection.ClusterTestVariables;
import org.springframework.data.redis.connection.RedisClusterConfiguration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceTestClientResources;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.listener.adapter.MessageListenerAdapter;
import org.springframework.data.redis.test.util.RedisClusterRule;

/**
 * @author Costin Leau
 * @author Jennifer Hickey
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@RunWith(Parameterized.class)
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

		List<RedisConnectionFactory> factories = new ArrayList<>(3);

		// Jedis
		JedisConnectionFactory jedisConnFactory = new JedisConnectionFactory();
		jedisConnFactory.setUsePool(false);
		jedisConnFactory.setPort(port);
		jedisConnFactory.setHostName(host);
		jedisConnFactory.setDatabase(2);
		jedisConnFactory.afterPropertiesSet();

		factories.add(jedisConnFactory);

		// Lettuce
		LettuceConnectionFactory lettuceConnFactory = new LettuceConnectionFactory();
		lettuceConnFactory.setClientResources(LettuceTestClientResources.getSharedClientResources());
		lettuceConnFactory.setPort(port);
		lettuceConnFactory.setHostName(host);
		lettuceConnFactory.setDatabase(2);
		lettuceConnFactory.setValidateConnection(true);
		lettuceConnFactory.afterPropertiesSet();

		factories.add(lettuceConnFactory);

		if (clusterAvailable()) {

			LettuceConnectionFactory lettuceClusterConnFactory = new LettuceConnectionFactory(
					new RedisClusterConfiguration().clusterNode(ClusterTestVariables.CLUSTER_NODE_1));
			lettuceClusterConnFactory.setClientResources(LettuceTestClientResources.getSharedClientResources());
			lettuceClusterConnFactory.setPort(port);
			lettuceClusterConnFactory.setHostName(host);
			lettuceClusterConnFactory.setValidateConnection(true);
			lettuceClusterConnFactory.afterPropertiesSet();

			factories.add(lettuceClusterConnFactory);
		}

		return factories.stream().map(factory -> new Object[] { factory }).collect(Collectors.toList());
	}

	@Before
	public void setUp() throws Exception {

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

	@After
	public void tearDown() {
		container.stop();
		bag.clear();
	}

	@Test
	public void testContainerPatternResubscribe() throws Exception {

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

		// anotherListener receives both messages
		List<String> msgs = new ArrayList<>();
		msgs.add(bag2.poll(500, TimeUnit.MILLISECONDS));
		msgs.add(bag2.poll(500, TimeUnit.MILLISECONDS));

		assertThat(msgs.size()).isEqualTo(2);
		assertThat(msgs.contains(payload1)).isTrue();
		assertThat(msgs.contains(payload2)).isTrue();
		msgs.clear();

		// unsubscribed adapter did not receive message
		assertThat(bag.poll(500, TimeUnit.MILLISECONDS)).isNull();

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

		assertThat(msgs.contains(payload2)).isTrue();
		assertThat(msgs.contains(null)).isTrue();

		// another listener receives messages on both channels
		msgs.clear();
		msgs.add(bag2.poll(500, TimeUnit.MILLISECONDS));
		msgs.add(bag2.poll(500, TimeUnit.MILLISECONDS));
		assertThat(msgs.size()).isEqualTo(2);
		assertThat(msgs.contains(payload1)).isTrue();
		assertThat(msgs.contains(payload2)).isTrue();
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

		Set<String> set = new LinkedHashSet<>();
		set.add(bag.poll(500, TimeUnit.MILLISECONDS));
		set.add(bag.poll(500, TimeUnit.MILLISECONDS));

		assertThat(set.contains(payload1)).isFalse();
		assertThat(set.contains(payload2)).isFalse();

		assertThat(set.contains(anotherPayload1)).isTrue();
		assertThat(set.contains(anotherPayload2)).isTrue();
	}

	/**
	 * Validates the behavior of {@link RedisMessageListenerContainer} when it needs to spin up a thread executing its
	 * PatternSubscriptionTask
	 *
	 * @throws Exception
	 */
	@Test
	public void testInitializeContainerWithMultipleTopicsIncludingPattern() throws Exception {

		assumeFalse(isClusterAware(template.getConnectionFactory()));

		container.stop();

		String uniqueChannel = "random-" + UUID.randomUUID().toString();
		PubSubAwaitUtil.runAndAwaitChannelSubscription(template.getConnectionFactory(), uniqueChannel, () -> {

			container.addMessageListener(adapter,
					Arrays.asList(new Topic[] { new ChannelTopic(uniqueChannel), new PatternTopic("s*") }));
			container.start();
		});

		// timing: There's currently no other way to synchronize
		// than to hope the subscribe/unsubscribe are executed within the time.
		Thread.sleep(50);

		template.convertAndSend("somechannel", "HELLO");
		template.convertAndSend(uniqueChannel, "WORLD");

		Set<String> set = new LinkedHashSet<>();
		set.add(bag.poll(500, TimeUnit.MILLISECONDS));
		set.add(bag.poll(500, TimeUnit.MILLISECONDS));

		assertThat(set).isEqualTo(new HashSet<>(Arrays.asList(new String[] { "HELLO", "WORLD" })));
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

	private static boolean clusterAvailable() {
		return new RedisClusterRule().isAvailable();
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
