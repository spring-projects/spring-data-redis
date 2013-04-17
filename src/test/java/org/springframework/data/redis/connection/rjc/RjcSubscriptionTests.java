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
package org.springframework.data.redis.connection.rjc;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Collection;

import org.idevlab.rjc.message.RedisNodeSubscriber;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.RedisInvalidSubscriptionException;

/**
 * Unit test of {@link RjcSubscription}
 *
 * @author Jennifer Hickey
 *
 */
public class RjcSubscriptionTests {

	private RjcSubscription subscription;

	private RedisNodeSubscriber subscriber;

	private MessageListener listener;

	@Before
	public void setUp() {
		subscriber = Mockito.mock(RedisNodeSubscriber.class);
		listener = Mockito.mock(MessageListener.class);
		subscription = new RjcSubscription(listener, subscriber);
	}

	@Test
	public void testUnsubscribeAllAndClose() {
		byte[][] channel = new byte[][] { "a".getBytes() };
		subscription.subscribe(channel);
		subscription.unsubscribe();
		verify(subscriber, never()).close();
		verify(subscriber, times(1)).unsubscribe(RjcUtils.decodeMultiple(channel));
		assertFalse(subscription.isAlive());
		assertTrue(subscription.getChannels().isEmpty());
		assertTrue(subscription.getPatterns().isEmpty());
	}

	@Test
	public void testUnsubscribeAllChannelsWithPatterns() {
		byte[][] channel = new byte[][] { "a".getBytes() };
		subscription.subscribe(channel);
		subscription.pSubscribe(new byte[][] { "s*".getBytes() });
		subscription.unsubscribe();
		verify(subscriber, times(1)).unsubscribe(RjcUtils.decodeMultiple(channel));
		verify(subscriber, never()).close();
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
		verify(subscriber, times(1)).unsubscribe(RjcUtils.decodeMultiple(channel));
		verify(subscriber, never()).close();
		assertFalse(subscription.isAlive());
		assertTrue(subscription.getChannels().isEmpty());
		assertTrue(subscription.getPatterns().isEmpty());
	}

	@Test
	public void testUnsubscribeChannelSomeLeft() {
		byte[][] channels = new byte[][] { "a".getBytes(), "b".getBytes() };
		subscription.subscribe(channels);
		subscription.unsubscribe(new byte[][] { "a".getBytes() });
		verify(subscriber, times(1)).unsubscribe(
				RjcUtils.decodeMultiple(new byte[][] { "a".getBytes() }));
		verify(subscriber, never()).close();
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
		verify(subscriber, times(1)).unsubscribe(RjcUtils.decodeMultiple(channel));
		verify(subscriber, never()).close();
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
		verify(subscriber, times(1)).unsubscribe(RjcUtils.decodeMultiple(channel));
		verify(subscriber, never()).close();
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
		verify(subscriber, never()).close();
		assertTrue(subscription.isAlive());
		assertTrue(subscription.getChannels().isEmpty());
		Collection<byte[]> patterns = subscription.getPatterns();
		assertEquals(1, patterns.size());
		assertArrayEquals("s*".getBytes(), patterns.iterator().next());
	}

	@Test
	public void testUnsubscribeNotAlive() {
		byte[][] channel = new byte[][] { "a".getBytes() };
		subscription.subscribe(channel);
		subscription.unsubscribe();
		assertFalse(subscription.isAlive());
		subscription.unsubscribe();
		verify(subscriber, times(1)).unsubscribe(RjcUtils.decodeMultiple(channel));
		verify(subscriber, never()).close();
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
		byte[][] pattern = new byte[][] { "a*".getBytes() };
		subscription.pSubscribe(pattern);
		subscription.pUnsubscribe();
		verify(subscriber, never()).close();
		verify(subscriber, times(1)).punsubscribe(RjcUtils.decodeMultiple(pattern));
		assertFalse(subscription.isAlive());
		assertTrue(subscription.getChannels().isEmpty());
		assertTrue(subscription.getPatterns().isEmpty());
	}

	@Test
	public void testPUnsubscribeAllPatternsWithChannels() {
		subscription.subscribe(new byte[][] { "a".getBytes() });
		byte[][] patterns = new byte[][] { "s*".getBytes() };
		subscription.pSubscribe(patterns);
		subscription.pUnsubscribe();
		verify(subscriber, never()).close();
		verify(subscriber, times(1)).punsubscribe(RjcUtils.decodeMultiple(patterns));
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
		verify(subscriber, never()).close();
		verify(subscriber, times(1)).punsubscribe(RjcUtils.decodeMultiple(pattern));
		assertFalse(subscription.isAlive());
		assertTrue(subscription.getChannels().isEmpty());
		assertTrue(subscription.getPatterns().isEmpty());
	}

	@Test
	public void testPUnsubscribePatternSomeLeft() {
		byte[][] patterns = new byte[][] { "a*".getBytes(), "b*".getBytes() };
		subscription.pSubscribe(patterns);
		byte[][] pattern = new byte[][] { "a*".getBytes() };
		subscription.pUnsubscribe(pattern);
		verify(subscriber, times(1)).punsubscribe(RjcUtils.decodeMultiple(pattern));
		verify(subscriber, never()).close();
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
		verify(subscriber, times(1)).punsubscribe(RjcUtils.decodeMultiple(pattern));
		verify(subscriber, never()).close();
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
		verify(subscriber, never()).close();
		verify(subscriber, times(1)).punsubscribe(RjcUtils.decodeMultiple(pattern));
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
		verify(subscriber, never()).close();
		assertTrue(subscription.isAlive());
		assertTrue(subscription.getPatterns().isEmpty());
		Collection<byte[]> channels = subscription.getChannels();
		assertEquals(1, channels.size());
		assertArrayEquals("s".getBytes(), channels.iterator().next());
	}

	@Test
	public void testPUnsubscribeNotAlive() {
		byte[][] channels = new byte[][] { "a".getBytes() };
		subscription.subscribe(channels);
		subscription.unsubscribe();
		assertFalse(subscription.isAlive());
		subscription.pUnsubscribe();
		verify(subscriber, times(1)).unsubscribe(RjcUtils.decodeMultiple(channels));
		verify(subscriber, never()).close();
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
		verify(subscriber, never()).close();
	}

	@Test
	public void testDoCloseSubscribedChannels() {
		subscription.subscribe(new byte[][] { "a".getBytes() });
		subscription.doClose();
		verify(subscriber, times(1)).close();
	}

	@Test
	public void testDoCloseSubscribedPatterns() {
		subscription.pSubscribe(new byte[][] { "a*".getBytes() });
		subscription.doClose();
		verify(subscriber, times(1)).close();
	}

}
