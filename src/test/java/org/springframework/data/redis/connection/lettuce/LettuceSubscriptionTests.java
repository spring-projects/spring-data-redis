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
package org.springframework.data.redis.connection.lettuce;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.pubsub.api.sync.RedisPubSubCommands;

import java.util.Collection;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.RedisInvalidSubscriptionException;

/**
 * Unit test of {@link LettuceSubscription}
 *
 * @author Jennifer Hickey
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public class LettuceSubscriptionTests {

	private LettuceSubscription subscription;

	StatefulRedisPubSubConnection<byte[], byte[]> pubsub;

	private MessageListener listener;

	private RedisPubSubCommands<byte[], byte[]> asyncCommands;

	private LettuceConnectionProvider connectionProvider;

	@SuppressWarnings("unchecked")
	@Before
	public void setUp() {

		pubsub = Mockito.mock(StatefulRedisPubSubConnection.class);
		listener = Mockito.mock(MessageListener.class);
		asyncCommands = Mockito.mock(RedisPubSubCommands.class);
		connectionProvider = Mockito.mock(LettuceConnectionProvider.class);

		Mockito.when(pubsub.sync()).thenReturn(asyncCommands);
		subscription = new LettuceSubscription(listener, pubsub, connectionProvider);
	}

	@Test
	public void testUnsubscribeAllAndClose() {
		subscription.subscribe(new byte[][] { "a".getBytes() });
		subscription.unsubscribe();
		verify(asyncCommands, times(1)).unsubscribe(new byte[][] { "a".getBytes() });
		verify(asyncCommands, never()).unsubscribe(new byte[0]);
		verify(asyncCommands, never()).punsubscribe(new byte[0]);
		verify(connectionProvider).release(pubsub);
		verify(pubsub).removeListener(any(LettuceMessageListener.class));
		assertFalse(subscription.isAlive());
		assertTrue(subscription.getChannels().isEmpty());
		assertTrue(subscription.getPatterns().isEmpty());
	}

	@Test
	public void testUnsubscribeAllChannelsWithPatterns() {
		subscription.subscribe(new byte[][] { "a".getBytes() });
		subscription.pSubscribe(new byte[][] { "s*".getBytes() });
		subscription.unsubscribe();
		verify(asyncCommands, times(1)).unsubscribe(new byte[][] { "a".getBytes() });
		verify(asyncCommands, never()).unsubscribe(new byte[0]);
		verify(asyncCommands, never()).punsubscribe(new byte[0]);
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
		verify(asyncCommands, times(1)).unsubscribe(channel);
		verify(asyncCommands, never()).unsubscribe(new byte[0]);
		verify(asyncCommands, never()).punsubscribe(new byte[0]);
		verify(connectionProvider).release(pubsub);
		verify(pubsub).removeListener(any(LettuceMessageListener.class));
		assertFalse(subscription.isAlive());
		assertTrue(subscription.getChannels().isEmpty());
		assertTrue(subscription.getPatterns().isEmpty());
	}

	@Test
	public void testUnsubscribeChannelSomeLeft() {
		byte[][] channels = new byte[][] { "a".getBytes(), "b".getBytes() };
		subscription.subscribe(channels);
		subscription.unsubscribe(new byte[][] { "a".getBytes() });
		verify(asyncCommands, times(1)).unsubscribe(new byte[][] { "a".getBytes() });
		verify(asyncCommands, never()).unsubscribe(new byte[0]);
		verify(asyncCommands, never()).punsubscribe(new byte[0]);
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
		verify(asyncCommands, times(1)).unsubscribe(channel);
		verify(asyncCommands, never()).unsubscribe(new byte[0]);
		verify(asyncCommands, never()).punsubscribe(new byte[0]);
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
		verify(asyncCommands, times(1)).unsubscribe(channel);
		verify(asyncCommands, never()).unsubscribe(new byte[0]);
		verify(asyncCommands, never()).punsubscribe(new byte[0]);
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
		verify(asyncCommands, never()).unsubscribe(new byte[0]);
		verify(asyncCommands, never()).punsubscribe(new byte[0]);
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
		verify(connectionProvider, times(1)).release(pubsub);
		verify(pubsub, times(1)).removeListener(any(LettuceMessageListener.class));
		assertFalse(subscription.isAlive());
		subscription.unsubscribe();
		verify(asyncCommands, times(1)).unsubscribe(new byte[][] { "a".getBytes() });
		verify(asyncCommands, never()).unsubscribe(new byte[0]);
		verify(asyncCommands, never()).punsubscribe(new byte[0]);
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
		verify(asyncCommands, never()).unsubscribe(new byte[0]);
		verify(asyncCommands, never()).punsubscribe(new byte[0]);
		verify(asyncCommands, times(1)).punsubscribe(new byte[][] { "a*".getBytes() });
		assertFalse(subscription.isAlive());
		verify(connectionProvider).release(pubsub);
		verify(pubsub).removeListener(any(LettuceMessageListener.class));
		assertTrue(subscription.getChannels().isEmpty());
		assertTrue(subscription.getPatterns().isEmpty());
	}

	@Test
	public void testPUnsubscribeAllPatternsWithChannels() {
		subscription.subscribe(new byte[][] { "a".getBytes() });
		subscription.pSubscribe(new byte[][] { "s*".getBytes() });
		subscription.pUnsubscribe();
		verify(asyncCommands, never()).unsubscribe(new byte[0]);
		verify(asyncCommands, never()).punsubscribe(new byte[0]);
		verify(asyncCommands, times(1)).punsubscribe(new byte[][] { "s*".getBytes() });
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
		verify(asyncCommands, never()).unsubscribe(new byte[0]);
		verify(asyncCommands, never()).punsubscribe(new byte[0]);
		verify(asyncCommands, times(1)).punsubscribe(pattern);
		verify(connectionProvider).release(pubsub);
		verify(pubsub).removeListener(any(LettuceMessageListener.class));
		assertFalse(subscription.isAlive());
		assertTrue(subscription.getChannels().isEmpty());
		assertTrue(subscription.getPatterns().isEmpty());
	}

	@Test
	public void testPUnsubscribePatternSomeLeft() {
		byte[][] patterns = new byte[][] { "a*".getBytes(), "b*".getBytes() };
		subscription.pSubscribe(patterns);
		subscription.pUnsubscribe(new byte[][] { "a*".getBytes() });
		verify(asyncCommands, times(1)).punsubscribe(new byte[][] { "a*".getBytes() });
		verify(asyncCommands, never()).unsubscribe(new byte[0]);
		verify(asyncCommands, never()).punsubscribe(new byte[0]);
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
		verify(asyncCommands, times(1)).punsubscribe(pattern);
		verify(asyncCommands, never()).unsubscribe(new byte[0]);
		verify(asyncCommands, never()).punsubscribe(new byte[0]);
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
		verify(asyncCommands, never()).unsubscribe(new byte[0]);
		verify(asyncCommands, never()).punsubscribe(new byte[0]);
		verify(asyncCommands, times(1)).punsubscribe(pattern);
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
		verify(asyncCommands, never()).unsubscribe(new byte[0]);
		verify(asyncCommands, never()).punsubscribe(new byte[0]);
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
		verify(connectionProvider, times(1)).release(pubsub);
		verify(pubsub, times(1)).removeListener(any(LettuceMessageListener.class));
		verify(asyncCommands, times(1)).unsubscribe(new byte[][] { "a".getBytes() });
		verify(asyncCommands, never()).unsubscribe(new byte[0]);
		verify(asyncCommands, never()).punsubscribe(new byte[0]);
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
		verify(asyncCommands, never()).unsubscribe(new byte[0]);
		verify(asyncCommands, never()).punsubscribe(new byte[0]);
	}

	@Test
	public void testDoCloseSubscribedChannels() {
		subscription.subscribe(new byte[][] { "a".getBytes() });
		subscription.doClose();
		verify(asyncCommands, times(1)).unsubscribe(new byte[0]);
		verify(asyncCommands, never()).punsubscribe(new byte[0]);
	}

	@Test
	public void testDoCloseSubscribedPatterns() {
		subscription.pSubscribe(new byte[][] { "a*".getBytes() });
		subscription.doClose();
		verify(asyncCommands, never()).unsubscribe(new byte[0]);
		verify(asyncCommands, times(1)).punsubscribe(new byte[0]);
	}
}
