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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
import org.springframework.data.redis.ConnectionFactoryTracker;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.listener.adapter.MessageListenerAdapter;
import org.springframework.data.redis.support.collections.ObjectFactory;

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

	private final BlockingDeque<String> bag = new LinkedBlockingDeque<String>(99);

	private final Object handler = new Object() {
		public void handleMessage(String message) {
			bag.add(message);
		}
	};

	private final MessageListenerAdapter adapter = new MessageListenerAdapter(handler);

	@Before
	public void setUp() throws Exception {
		adapter.setSerializer(template.getValueSerializer());
		adapter.afterPropertiesSet();

		container = new RedisMessageListenerContainer();
		container.setConnectionFactory(template.getConnectionFactory());
		container.setBeanName("container");
		container.addMessageListener(adapter, Arrays.asList(new ChannelTopic(CHANNEL)));
		container.afterPropertiesSet();
		container.start();

		Thread.sleep(1000);
	}

	@After
	public void tearDown() throws Exception {
		container.destroy();
	}

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
	 * @return
	 */
	protected T getT() {
		return factory.instance();
	}

	@Test
	public void testContainerSubscribe() throws Exception {
		String payload1 = "do";
		String payload2 = "re mi";

		template.convertAndSend(CHANNEL, payload1);
		template.convertAndSend(CHANNEL, payload2);

		Set<String> set = new LinkedHashSet<String>();
		set.add(bag.poll(1, TimeUnit.SECONDS));
		set.add(bag.poll(1, TimeUnit.SECONDS));

		System.out.println(set);

		assertTrue(set.contains(payload1));
		assertTrue(set.contains(payload2));
	}

	@Test
	public void testMessageBatch() throws Exception {
		int COUNT = 10;
		for (int i = 0; i < COUNT; i++) {
			template.convertAndSend(CHANNEL, "message=" + i);
		}

		Thread.sleep(1000);
		assertEquals(COUNT, bag.size());
	}
}