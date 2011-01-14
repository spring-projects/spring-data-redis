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

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.data.keyvalue.redis.connection.Message;
import org.springframework.data.keyvalue.redis.connection.MessageListener;
import org.springframework.data.keyvalue.redis.connection.RedisConnectionFactory;
import org.springframework.data.keyvalue.redis.core.RedisTemplate;
import org.springframework.data.keyvalue.redis.support.collections.ObjectFactory;

/**
 * Base test class for PubSub integration tests
 * 
 * @author Costin Leau
 */
@RunWith(Parameterized.class)
public class PubSubTests<T> {

	private static final String CHANNEL = "pubsub::test";

	protected RedisListenerContainer container;
	protected ObjectFactory<T> factory;
	protected RedisTemplate template;
	private static Set<RedisConnectionFactory> connFactories = new LinkedHashSet<RedisConnectionFactory>();

	private MessageListener testListener;

	@Before
	public void setUp() throws Exception {
		container = new RedisListenerContainer();
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
		final BlockingQueue<Message> bag = new ArrayBlockingQueue<Message>(4);

		container.addMessageListener(new MessageListener() {

			@Override
			public void onMessage(Message message, byte[] pattern) {
				System.out.println("Received message " + message + " and pattern=" + pattern);
				bag.add(message);
			}
		}, Arrays.asList(new ChannelTopic(CHANNEL)));

		Thread.sleep(500);
		template.convertAndSend(CHANNEL, "bar");
		template.convertAndSend(CHANNEL, "bar1");
		System.out.println("Found in bag " + bag.poll(1, TimeUnit.SECONDS));
		System.out.println("Found in bag " + bag.poll(1, TimeUnit.SECONDS));
	}
}