/*
 * Copyright 2011-2013 the original author or authors.
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
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.listener.adapter.MessageListenerAdapter;

import redis.clients.jedis.JedisPoolConfig;

/**
 * Integration tests confirming that {@link RedisMessageListenerContainer}
 * closes connections after unsubscribing
 *
 * @author Jennifer Hickey
 *
 */
public class SubscriptionConnectionTests {

	private static final String CHANNEL = "pubsub::test";

	private JedisConnectionFactory connectionFactory;

	private List<RedisMessageListenerContainer> containers = new ArrayList<RedisMessageListenerContainer>();

	private final Object handler = new Object() {
		@SuppressWarnings("unused")
		public void handleMessage(String message) {
			System.out.println(message);
		}
	};

	@Before
	public void setUp() {
		connectionFactory = new JedisConnectionFactory();
		connectionFactory.setUsePool(true);
		connectionFactory.setPort(SettingsUtils.getPort());
		connectionFactory.setHostName(SettingsUtils.getHost());
		connectionFactory.setDatabase(2);
		JedisPoolConfig config = new JedisPoolConfig();
		config.setMaxWait(3000l);
		connectionFactory.setPoolConfig(config);
		connectionFactory.afterPropertiesSet();
	}

	@After
	public void tearDown() throws Exception {
		for (RedisMessageListenerContainer container : containers) {
			if (container.isActive()) {
				container.destroy();
			}
		}
		connectionFactory.destroy();
	}

	@Test
	public void testStopMessageListenerContainers() throws Exception {
		// Grab all 8 connections from the pool. They should be released on
		// container stop
		for (int i = 0; i < 8; i++) {
			RedisMessageListenerContainer container = new RedisMessageListenerContainer();
			container.setConnectionFactory(connectionFactory);
			container.setBeanName("container" + i);
			container.addMessageListener(new MessageListenerAdapter(handler),
					Arrays.asList(new ChannelTopic(CHANNEL)));
			container.setTaskExecutor(new SyncTaskExecutor());
			container.setSubscriptionExecutor(new SimpleAsyncTaskExecutor());
			container.afterPropertiesSet();
			container.start();
			// DATAREDIS-170 Need time for subscription to fully complete or
			// cancelTask won't close connection b/c subscription is null
			Thread.sleep(100);
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
			container.addMessageListener(listener,
					Arrays.asList(new ChannelTopic(CHANNEL)));
			container.setTaskExecutor(new SyncTaskExecutor());
			container.setSubscriptionExecutor(new SimpleAsyncTaskExecutor());
			container.afterPropertiesSet();
			container.start();
			containers.add(container);
		}

		// DATAREDIS-170 Need time for subscription to fully complete or
		// cancelTask won't close connection b/c subscription is null
		Thread.sleep(100);

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
			container.addMessageListener(listener,
					Arrays.asList(new ChannelTopic(CHANNEL)));
			container.setTaskExecutor(new SyncTaskExecutor());
			container.setSubscriptionExecutor(new SimpleAsyncTaskExecutor());
			container.afterPropertiesSet();
			container.start();
			containers.add(container);
		}

		// DATAREDIS-170 Need time for subscription to fully complete or
		// cancelTask won't close connection b/c subscription is null
		Thread.sleep(100);

		// Unsubscribe all listeners from all topics, freeing up a connection
		containers.get(0).removeMessageListener(null,
				Arrays.asList(new Topic[] {}));

		// verify we can now get a connection from the pool
		RedisConnection connection = connectionFactory.getConnection();
		connection.close();
	}

}
