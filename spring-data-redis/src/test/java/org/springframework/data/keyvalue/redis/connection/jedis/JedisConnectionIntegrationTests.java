/*
 * Copyright 2010 the original author or authors.
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

package org.springframework.data.keyvalue.redis.connection.jedis;

import static org.junit.Assert.*;

import org.junit.Test;
import org.springframework.data.keyvalue.redis.SettingsUtils;
import org.springframework.data.keyvalue.redis.connection.AbstractConnectionIntegrationTests;
import org.springframework.data.keyvalue.redis.connection.MessageListener;
import org.springframework.data.keyvalue.redis.connection.RedisConnectionFactory;

public class JedisConnectionIntegrationTests extends AbstractConnectionIntegrationTests {

	JedisConnectionFactory factory;

	public JedisConnectionIntegrationTests() {
		factory = new JedisConnectionFactory();
		factory.setUsePool(true);

		factory.setPort(SettingsUtils.getPort());
		factory.setHostName(SettingsUtils.getHost());

		factory.afterPropertiesSet();
	}

	@Override
	protected RedisConnectionFactory getConnectionFactory() {
		return factory;
	}

	@Test
	public void testPubSubWithNamedChannels() {
		final byte[] expectedChannel = "channel1".getBytes();
		final byte[] expectedMessage = "msg".getBytes();

		MessageListener listener = new MessageListener() {

			@Override
			public void onMessage(byte[] message, byte[] channel, byte[] pattern) {
				assertArrayEquals(expectedChannel, channel);
				assertArrayEquals(expectedMessage, message);
				System.out.println("Received message '" + new String(message) + "'");
			}
		};

		Thread th = new Thread(new Runnable() {
			@Override
			public void run() {
				// sleep 1 second to let the registration happen
				try {
					Thread.currentThread().sleep(1000);
				} catch (InterruptedException ex) {
					throw new RuntimeException(ex);
				}

				// open a new connection
				JedisConnection connection2 = factory.getConnection();
				connection2.publish(expectedMessage, expectedChannel);
				connection2.close();
				// unsubscribe connection
				connection.getSubscription().unsubscribe();
			}
		});

		th.start();
		connection.subscribe(listener, expectedChannel);
	}

	@Test
	public void testPubSubWithPatterns() {
		final byte[] expectedPattern = "channel*".getBytes();
		final byte[] expectedMessage = "msg".getBytes();

		MessageListener listener = new MessageListener() {

			@Override
			public void onMessage(byte[] message, byte[] channel, byte[] pattern) {
				assertArrayEquals(expectedPattern, pattern);
				assertArrayEquals(expectedMessage, message);
				System.out.println("Received message '" + new String(message) + "'");
			}
		};

		Thread th = new Thread(new Runnable() {
			@Override
			public void run() {
				// sleep 1 second to let the registration happen
				try {
					Thread.currentThread().sleep(1000);
				} catch (InterruptedException ex) {
					throw new RuntimeException(ex);
				}

				// open a new connection
				JedisConnection connection2 = factory.getConnection();
				connection2.publish(expectedMessage, "channel1".getBytes());
				connection2.publish(expectedMessage, "channel2".getBytes());
				connection2.close();
				// unsubscribe connection
				connection.getSubscription().pUnsubscribe(expectedPattern);
			}
		});

		th.start();
		connection.pSubscribe(listener, expectedPattern);
	}

//	@Test
//	public void setAdd() {
//		connection.sadd("s1", "1");
//		connection.sadd("s1", "2");
//		connection.sadd("s1", "3");
//		connection.sadd("s2", "2");
//		connection.sadd("s2", "3");
//		Set<String> intersection = connection.sinter("s1", "s2");
//		System.out.println(intersection);
//
//
//	}
//
//	@Test
//	public void setIntersectionTests() {
//		RedisTemplate template = new RedisTemplate(clientFactory);
//		RedisSet s1 = new RedisSet(template, "s1");
//		s1.add("1");
//		s1.add("2");
//		s1.add("3");
//		RedisSet s2 = new RedisSet(template, "s2");
//		s2.add("2");
//		s2.add("3");
//		Set s3 = s1.intersection("s3", s1, s2);
//		for (Object object : s3) {
//			System.out.println(object);
//		}
}