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
package org.springframework.data.redis.connection.jedis;

import java.util.Collection;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.never;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertArrayEquals;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.RedisInvalidSubscriptionException;

import redis.clients.jedis.BinaryJedisPubSub;

/**
 * Unit test of {@link JedisSubscription}
 *
 * @author Jennifer Hickey
 */
public class JedisSubscriptionTests {

	private JedisSubscription subscription;

	private BinaryJedisPubSub jedisPubSub;

	private MessageListener listener;

	@Before
	public void setUp() {
		jedisPubSub = Mockito.mock(BinaryJedisPubSub.class);
		listener = Mockito.mock(MessageListener.class);
		subscription = new JedisSubscription(listener, jedisPubSub, null, null);
	}

	@Test
	public void testUnsubscribeAllAndClose() {
		subscription.subscribe(new byte[][] { "a".getBytes() });
		subscription.unsubscribe();
		verify(jedisPubSub, times(1)).unsubscribe();
		verify(jedisPubSub, never()).punsubscribe();
		assertFalse(subscription.isAlive());
		assertTrue(subscription.getChannels().isEmpty());
		assertTrue(subscription.getPatterns().isEmpty());
	}

	@Test
	public void testUnsubscribeAllChannelsWithPatterns() {
		subscription.subscribe(new byte[][] { "a".getBytes() });
		subscription.pSubscribe(new byte[][] { "s*".getBytes() });
		subscription.unsubscribe();
		verify(jedisPubSub, times(1)).unsubscribe();
		verify(jedisPubSub, never()).punsubscribe();
		assertTrue(subscription.isAlive());
		assertTrue(subscription.getChannels().isEmpty());
		Collection<byte[]> patterns = subscription.getPatterns();
		assertEquals(1, patterns.size());
		assertArrayEquals("s*".getBytes(), patterns.iterator().next());
	}

	@Test
	public void testUnsubscribeChannelAndClose() {
		byte[][] channel = new byte[][] { "a".getBytes() };
		subscription.subscribe(channel);
		subscription.unsubscribe(channel);
		verify(jedisPubSub, times(1)).unsubscribe(channel);
		verify(jedisPubSub, never()).punsubscribe();
		assertFalse(subscription.isAlive());
		assertTrue(subscription.getChannels().isEmpty());
		assertTrue(subscription.getPatterns().isEmpty());
	}

	@Test
	public void testUnsubscribeChannelSomeLeft() {
		byte[][] channels = new byte[][] { "a".getBytes(), "b".getBytes() };
		subscription.subscribe(channels);
		subscription.unsubscribe(new byte[][] { "a".getBytes() });
		verify(jedisPubSub, times(1)).unsubscribe(new byte[][] { "a".getBytes() });
		verify(jedisPubSub, never()).punsubscribe();
		assertTrue(subscription.isAlive());
		Collection<byte[]> subChannels = subscription.getChannels();
		assertEquals(1, subChannels.size());
		assertArrayEquals("b".getBytes(), subChannels.iterator().next());
		assertTrue(subscription.getPatterns().isEmpty());
	}

	@Test
	public void testUnsubscribeChannelWithPatterns() {
		byte[][] channel = new byte[][] { "a".getBytes() };
		subscription.subscribe(channel);
		subscription.pSubscribe(new byte[][] { "s*".getBytes() });
		subscription.unsubscribe(channel);
		verify(jedisPubSub, times(1)).unsubscribe(channel);
		verify(jedisPubSub, never()).punsubscribe();
		assertTrue(subscription.isAlive());
		assertTrue(subscription.getChannels().isEmpty());
		Collection<byte[]> patterns = subscription.getPatterns();
		assertEquals(1, patterns.size());
		assertArrayEquals("s*".getBytes(), patterns.iterator().next());
	}

	@Test
	public void testUnsubscribeChannelWithPatternsSomeLeft() {
		byte[][] channel = new byte[][] { "a".getBytes() };
		subscription.subscribe(new byte[][] { "a".getBytes(), "b".getBytes() });
		subscription.pSubscribe(new byte[][] { "s*".getBytes() });
		subscription.unsubscribe(channel);
		verify(jedisPubSub, times(1)).unsubscribe(channel);
		verify(jedisPubSub, never()).punsubscribe();
		assertTrue(subscription.isAlive());
		Collection<byte[]> channels = subscription.getChannels();
		assertEquals(1, channels.size());
		assertArrayEquals("b".getBytes(), channels.iterator().next());
		Collection<byte[]> patterns = subscription.getPatterns();
		assertEquals(1, patterns.size());
		assertArrayEquals("s*".getBytes(), patterns.iterator().next());
	}

	@Test
	public void testUnsubscribeAllNoChannels() {
		subscription.pSubscribe(new byte[][] { "s*".getBytes() });
		subscription.unsubscribe();
		verify(jedisPubSub, never()).unsubscribe();
		verify(jedisPubSub, never()).punsubscribe();
		assertTrue(subscription.isAlive());
		assertTrue(subscription.getChannels().isEmpty());
		Collection<byte[]> patterns = subscription.getPatterns();
		assertEquals(1, patterns.size());
		assertArrayEquals("s*".getBytes(), patterns.iterator().next());
	}

	@Test
	public void testUnsubscribeNotAlive() {
		subscription.subscribe(new byte[][] { "a".getBytes() });
		subscription.unsubscribe();
		assertFalse(subscription.isAlive());
		subscription.unsubscribe();
		verify(jedisPubSub, times(1)).unsubscribe();
		verify(jedisPubSub, never()).punsubscribe();
	}

	@Test(expected = RedisInvalidSubscriptionException.class)
	public void testSubscribeNotAlive() {
		subscription.subscribe(new byte[][] { "a".getBytes() });
		subscription.unsubscribe();
		assertFalse(subscription.isAlive());
		subscription.subscribe(new byte[][] { "s".getBytes() });
	}

	@Test
	public void testPUnsubscribeAllAndClose() {
		subscription.pSubscribe(new byte[][] { "a*".getBytes() });
		subscription.pUnsubscribe();
		verify(jedisPubSub, never()).unsubscribe();
		verify(jedisPubSub, times(1)).punsubscribe();
		assertFalse(subscription.isAlive());
		assertTrue(subscription.getChannels().isEmpty());
		assertTrue(subscription.getPatterns().isEmpty());
	}

	@Test
	public void testPUnsubscribeAllPatternsWithChannels() {
		subscription.subscribe(new byte[][] { "a".getBytes() });
		subscription.pSubscribe(new byte[][] { "s*".getBytes() });
		subscription.pUnsubscribe();
		verify(jedisPubSub, never()).unsubscribe();
		verify(jedisPubSub, times(1)).punsubscribe();
		assertTrue(subscription.isAlive());
		assertTrue(subscription.getPatterns().isEmpty());
		Collection<byte[]> channels = subscription.getChannels();
		assertEquals(1, channels.size());
		assertArrayEquals("a".getBytes(), channels.iterator().next());
	}

	@Test
	public void testPUnsubscribeAndClose() {
		byte[][] pattern = new byte[][] { "a*".getBytes() };
		subscription.pSubscribe(pattern);
		subscription.pUnsubscribe(pattern);
		verify(jedisPubSub, never()).unsubscribe();
		verify(jedisPubSub, times(1)).punsubscribe(pattern);
		assertFalse(subscription.isAlive());
		assertTrue(subscription.getChannels().isEmpty());
		assertTrue(subscription.getPatterns().isEmpty());
	}

	@Test
	public void testPUnsubscribePatternSomeLeft() {
		byte[][] patterns = new byte[][] { "a*".getBytes(), "b*".getBytes() };
		subscription.pSubscribe(patterns);
		subscription.pUnsubscribe(new byte[][] { "a*".getBytes() });
		verify(jedisPubSub, times(1)).punsubscribe(new byte[][] { "a*".getBytes() });
		verify(jedisPubSub, never()).unsubscribe();
		assertTrue(subscription.isAlive());
		Collection<byte[]> subPatterns = subscription.getPatterns();
		assertEquals(1, subPatterns.size());
		assertArrayEquals("b*".getBytes(), subPatterns.iterator().next());
		assertTrue(subscription.getChannels().isEmpty());
	}

	@Test
	public void testPUnsubscribePatternWithChannels() {
		byte[][] pattern = new byte[][] { "s*".getBytes() };
		subscription.subscribe(new byte[][] { "a".getBytes() });
		subscription.pSubscribe(pattern);
		subscription.pUnsubscribe(pattern);
		verify(jedisPubSub, times(1)).punsubscribe(pattern);
		verify(jedisPubSub, never()).unsubscribe();
		assertTrue(subscription.isAlive());
		assertTrue(subscription.getPatterns().isEmpty());
		Collection<byte[]> channels = subscription.getChannels();
		assertEquals(1, channels.size());
		assertArrayEquals("a".getBytes(), channels.iterator().next());
	}

	@Test
	public void testUnsubscribePatternWithChannelsSomeLeft() {
		byte[][] pattern = new byte[][] { "a*".getBytes() };
		subscription.pSubscribe(new byte[][] { "a*".getBytes(), "b*".getBytes() });
		subscription.subscribe(new byte[][] { "a".getBytes() });
		subscription.pUnsubscribe(pattern);
		verify(jedisPubSub, never()).unsubscribe();
		verify(jedisPubSub, times(1)).punsubscribe(pattern);
		assertTrue(subscription.isAlive());
		Collection<byte[]> channels = subscription.getChannels();
		assertEquals(1, channels.size());
		assertArrayEquals("a".getBytes(), channels.iterator().next());
		Collection<byte[]> patterns = subscription.getPatterns();
		assertEquals(1, patterns.size());
		assertArrayEquals("b*".getBytes(), patterns.iterator().next());
	}

	@Test
	public void testPUnsubscribeAllNoPatterns() {
		subscription.subscribe(new byte[][] { "s".getBytes() });
		subscription.pUnsubscribe();
		verify(jedisPubSub, never()).unsubscribe();
		verify(jedisPubSub, never()).punsubscribe();
		assertTrue(subscription.isAlive());
		assertTrue(subscription.getPatterns().isEmpty());
		Collection<byte[]> channels = subscription.getChannels();
		assertEquals(1, channels.size());
		assertArrayEquals("s".getBytes(), channels.iterator().next());
	}

	@Test
	public void testPUnsubscribeNotAlive() {
		subscription.subscribe(new byte[][] { "a".getBytes() });
		subscription.unsubscribe();
		assertFalse(subscription.isAlive());
		subscription.pUnsubscribe();
		verify(jedisPubSub, times(1)).unsubscribe();
		verify(jedisPubSub, never()).punsubscribe();
	}

	@Test(expected = RedisInvalidSubscriptionException.class)
	public void testPSubscribeNotAlive() {
		subscription.subscribe(new byte[][] { "a".getBytes() });
		subscription.unsubscribe();
		assertFalse(subscription.isAlive());
		subscription.pSubscribe(new byte[][] { "s*".getBytes() });
	}

	@Test
	public void testDoCloseNotSubscribed() {
		subscription.doClose();
		verify(jedisPubSub, never()).unsubscribe();
		verify(jedisPubSub, never()).punsubscribe();
	}

	@Test
	public void testDoCloseSubscribedChannels() {
		subscription.subscribe(new byte[][] { "a".getBytes() });
		subscription.doClose();
		verify(jedisPubSub, times(1)).unsubscribe();
		verify(jedisPubSub, never()).punsubscribe();
	}

	@Test
	public void testDoCloseSubscribedPatterns() {
		subscription.pSubscribe(new byte[][] { "a*".getBytes() });
		subscription.doClose();
		verify(jedisPubSub, never()).unsubscribe();
		verify(jedisPubSub, times(1)).punsubscribe();
	}

}
