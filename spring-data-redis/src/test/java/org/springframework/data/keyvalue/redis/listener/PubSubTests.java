/*
 * Copyright 2011 the original author or authors.
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
package org.springframework.data.keyvalue.redis.listener;

import static org.junit.Assert.*;

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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.data.keyvalue.redis.connection.RedisConnectionFactory;
import org.springframework.data.keyvalue.redis.core.RedisTemplate;
import org.springframework.data.keyvalue.redis.listener.adapter.MessageListenerAdapter;
import org.springframework.data.keyvalue.redis.support.collections.ObjectFactory;

/**
 * Base test class for PubSub integration tests
 * 
 * @author Costin Leau
 */
@RunWith(Parameterized.class)
public class PubSubTests<T> {

	private static final String CHANNEL = "pubsub::test";

	protected RedisMessageListenerContainer container;
	protected ObjectFactory<T> factory;
	protected RedisTemplate template;
	private static Set<RedisConnectionFactory> connFactories = new LinkedHashSet<RedisConnectionFactory>();

	private final BlockingDeque<String> bag = new LinkedBlockingDeque<String>(99);

	private final Object handler = new Object() {
		void handleMessage(String message) {
			System.out.println("Received message " + message);
			bag.add(message);
		}
	};

	private final MessageListenerAdapter adapter = new MessageListenerAdapter(handler);

	@Before
	public void setUp() throws Exception {
		adapter.setSerializer(template.getValueSerializer());

		container = new RedisMessageListenerContainer();
		container.setConnectionFactory(template.getConnectionFactory());
		container.setBeanName("container");
		container.afterPropertiesSet();

	}

	@After
	public void tearDown() throws Exception {
		container.destroy();
	}

	public PubSubTests(ObjectFactory<T> factory, RedisTemplate template) {
		this.factory = factory;
		this.template = template;
		connFactories.add(template.getConnectionFactory());
	}

	@AfterClass
	public static void cleanUp() {
		if (connFactories != null) {
			for (RedisConnectionFactory connectionFactory : connFactories) {
				try {
					((DisposableBean) connectionFactory).destroy();
					System.out.println("Succesfully cleaned up factory " + connectionFactory);
				} catch (Exception ex) {
					System.err.println("Cannot clean factory " + connectionFactory + ex);
				}
			}
		}
	}

	@Parameters
	public static Collection<Object[]> testParams() {
		return PubSubTestParams.testParams();
	}

	/**
	 * Return a new instance of T
	 * @return
	 */
	protected T getT() {
		return factory.instance();
	}

	@Test
	public void testContainerSubscribe() throws Exception {

		container.addMessageListener(adapter, Arrays.asList(new ChannelTopic(CHANNEL)));

		// wait for the container to start the registration

		Thread.sleep(500);
		String payload1 = "do";
		String payload2 = "re mi";
		template.convertAndSend(CHANNEL, payload1);
		template.convertAndSend(CHANNEL, payload2);

		Set<String> set = new LinkedHashSet<String>();
		set.add(bag.poll(1, TimeUnit.SECONDS));
		set.add(bag.poll(1, TimeUnit.SECONDS));


		assertTrue(set.contains(payload1));
		assertTrue(set.contains(payload2));
	}

	@Test
	public void testMessageBatch() throws Exception {

		container.addMessageListener(adapter, Arrays.asList(new ChannelTopic(CHANNEL)));

		// wait for the container to start the registration

		int COUNT = 10;
		Thread.sleep(500);
		for (int i = 0; i < COUNT; i++) {
			template.convertAndSend(CHANNEL, "message=" + i);
		}

		Thread.sleep(1000);
		assertEquals(COUNT, bag.size());
	}
}