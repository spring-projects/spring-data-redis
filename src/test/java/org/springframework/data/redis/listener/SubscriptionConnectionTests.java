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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.data.redis.ConnectionFactoryTracker;
import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceTestClientResources;
import org.springframework.data.redis.listener.adapter.MessageListenerAdapter;

/**
 * Integration tests confirming that {@link RedisMessageListenerContainer} closes connections after unsubscribing
 * 
 * @author Jennifer Hickey
 * @author Thomas Darimont
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@RunWith(Parameterized.class)
public class SubscriptionConnectionTests {

	private static final Log logger = LogFactory.getLog(SubscriptionConnectionTests.class);
	private static final String CHANNEL = "pubsub::test";

	private RedisConnectionFactory connectionFactory;

	private List<RedisMessageListenerContainer> containers = new ArrayList<RedisMessageListenerContainer>();

	private final Object handler = new Object() {
		@SuppressWarnings("unused")
		public void handleMessage(String message) {
			logger.debug(message);
		}
	};

	public SubscriptionConnectionTests(RedisConnectionFactory connectionFactory) {
		this.connectionFactory = connectionFactory;
		ConnectionFactoryTracker.add(connectionFactory);
	}

	@After
	public void tearDown() throws Exception {
		for (RedisMessageListenerContainer container : containers) {
			if (container.isActive()) {
				container.destroy();
			}
		}
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

	@Test
	public void testStopMessageListenerContainers() throws Exception {
		// Grab all 8 connections from the pool. They should be released on
		// container stop
		for (int i = 0; i < 8; i++) {
			RedisMessageListenerContainer container = new RedisMessageListenerContainer();
			container.setConnectionFactory(connectionFactory);
			container.setBeanName("container" + i);
			container.addMessageListener(new MessageListenerAdapter(handler), Arrays.asList(new ChannelTopic(CHANNEL)));
			container.setTaskExecutor(new SyncTaskExecutor());
			container.setSubscriptionExecutor(new SimpleAsyncTaskExecutor());
			container.afterPropertiesSet();
			container.start();

			if (connectionFactory instanceof JedisConnectionFactory) {
				// Need to sleep shortly as jedis cannot deal propery with multiple repsonses within one connection
				// @see https://github.com/xetorthio/jedis/issues/186
				Thread.sleep(100);
			}

			container.stop();
			containers.add(container);
		}

		// verify we can now get a connection from the pool
		RedisConnection connection = connectionFactory.getConnection();
		connection.close();
	}

	@Test
	public void testRemoveLastListener() throws Exception {
		// Grab all 8 connections from the pool
		MessageListener listener = new MessageListenerAdapter(handler);
		for (int i = 0; i < 8; i++) {
			RedisMessageListenerContainer container = new RedisMessageListenerContainer();
			container.setConnectionFactory(connectionFactory);
			container.setBeanName("container" + i);
			container.addMessageListener(listener, Arrays.asList(new ChannelTopic(CHANNEL)));
			container.setTaskExecutor(new SyncTaskExecutor());
			container.setSubscriptionExecutor(new SimpleAsyncTaskExecutor());
			container.afterPropertiesSet();
			container.start();
			containers.add(container);
		}

		// Removing the sole listener from the container should free up a
		// connection
		containers.get(0).removeMessageListener(listener);

		// verify we can now get a connection from the pool
		RedisConnection connection = connectionFactory.getConnection();
		connection.close();
	}

	@Test
	public void testStopListening() throws InterruptedException {
		// Grab all 8 connections from the pool.
		MessageListener listener = new MessageListenerAdapter(handler);
		for (int i = 0; i < 8; i++) {
			RedisMessageListenerContainer container = new RedisMessageListenerContainer();
			container.setConnectionFactory(connectionFactory);
			container.setBeanName("container" + i);
			container.addMessageListener(listener, Arrays.asList(new ChannelTopic(CHANNEL)));
			container.setTaskExecutor(new SyncTaskExecutor());
			container.setSubscriptionExecutor(new SimpleAsyncTaskExecutor());
			container.afterPropertiesSet();
			container.start();
			containers.add(container);
		}

		// Unsubscribe all listeners from all topics, freeing up a connection
		containers.get(0).removeMessageListener(null, Arrays.asList(new Topic[] {}));

		// verify we can now get a connection from the pool
		RedisConnection connection = connectionFactory.getConnection();
		connection.close();
	}

}
