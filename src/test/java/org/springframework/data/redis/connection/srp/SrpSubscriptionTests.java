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
package org.springframework.data.redis.connection.srp;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.util.Collection;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.RedisInvalidSubscriptionException;

import redis.client.RedisClient;
import redis.client.ReplyListener;

/**
 * Unit test of {@link SrpSubscription}
 * 
 * @author Jennifer Hickey
 */
public class SrpSubscriptionTests {

	private SrpSubscription subscription;

	private RedisClient redisClient;

	private MessageListener listener;

	@Before
	public void setUp() {
		redisClient = Mockito.mock(RedisClient.class);
		listener = Mockito.mock(MessageListener.class);
		subscription = new SrpSubscription(listener, redisClient);
	}

	@Test
	public void testUnsubscribeAllAndClose() {
		subscription.subscribe(new byte[][] { "a".getBytes() });
		subscription.unsubscribe();
		verify(redisClient, times(1)).unsubscribe((Object[]) null);
		verify(redisClient, never()).punsubscribe((Object[]) null);
		assertThat(subscription.isAlive()).isFalse();
		verify(redisClient).removeListener(any(ReplyListener.class));
		assertThat(subscription.getChannels().isEmpty()).isTrue();
		assertThat(subscription.getPatterns().isEmpty()).isTrue();
	}

	@Test
	public void testUnsubscribeAllChannelsWithPatterns() {
		subscription.subscribe(new byte[][] { "a".getBytes() });
		subscription.pSubscribe(new byte[][] { "s*".getBytes() });
		subscription.unsubscribe();
		verify(redisClient, times(1)).unsubscribe((Object[]) null);
		verify(redisClient, never()).punsubscribe((Object[]) null);
		assertThat(subscription.isAlive()).isTrue();
		assertThat(subscription.getChannels().isEmpty()).isTrue();
		Collection<byte[]> patterns = subscription.getPatterns();
		assertThat(patterns).hasSize(1);
		assertThat(patterns.iterator().next()).isEqualTo("s*".getBytes());
	}

	@Test
	public void testUnsubscribeChannelAndClose() {
		byte[][] channel = new byte[][] { "a".getBytes() };
		subscription.subscribe(channel);
		subscription.unsubscribe(channel);
		verify(redisClient, times(1)).unsubscribe((Object[]) channel);
		verify(redisClient, never()).punsubscribe((Object[]) null);
		verify(redisClient).removeListener(any(ReplyListener.class));
		assertThat(subscription.isAlive()).isFalse();
		assertThat(subscription.getChannels().isEmpty()).isTrue();
		assertThat(subscription.getPatterns().isEmpty()).isTrue();
	}

	@Test
	public void testUnsubscribeChannelSomeLeft() {
		byte[][] channels = new byte[][] { "a".getBytes(), "b".getBytes() };
		subscription.subscribe(channels);
		subscription.unsubscribe(new byte[][] { "a".getBytes() });
		verify(redisClient, times(1)).unsubscribe((Object[]) new byte[][] { "a".getBytes() });
		verify(redisClient, never()).punsubscribe((Object[]) null);
		assertThat(subscription.isAlive()).isTrue();
		Collection<byte[]> subChannels = subscription.getChannels();
		assertThat(subChannels).hasSize(1);
		assertThat(subChannels.iterator().next()).isEqualTo("b".getBytes());
		assertThat(subscription.getPatterns().isEmpty()).isTrue();
	}

	@Test
	public void testUnsubscribeChannelWithPatterns() {
		byte[][] channel = new byte[][] { "a".getBytes() };
		subscription.subscribe(channel);
		subscription.pSubscribe(new byte[][] { "s*".getBytes() });
		subscription.unsubscribe(channel);
		verify(redisClient, times(1)).unsubscribe((Object[]) channel);
		verify(redisClient, never()).punsubscribe((Object[]) null);
		assertThat(subscription.isAlive()).isTrue();
		assertThat(subscription.getChannels().isEmpty()).isTrue();
		Collection<byte[]> patterns = subscription.getPatterns();
		assertThat(patterns).hasSize(1);
		assertThat(patterns.iterator().next()).isEqualTo("s*".getBytes());
	}

	@Test
	public void testUnsubscribeChannelWithPatternsSomeLeft() {
		byte[][] channel = new byte[][] { "a".getBytes() };
		subscription.subscribe(new byte[][] { "a".getBytes(), "b".getBytes() });
		subscription.pSubscribe(new byte[][] { "s*".getBytes() });
		subscription.unsubscribe(channel);
		verify(redisClient, times(1)).unsubscribe((Object[]) channel);
		verify(redisClient, never()).punsubscribe((Object[]) null);
		assertThat(subscription.isAlive()).isTrue();
		Collection<byte[]> channels = subscription.getChannels();
		assertThat(channels).hasSize(1);
		assertThat(channels.iterator().next()).isEqualTo("b".getBytes());
		Collection<byte[]> patterns = subscription.getPatterns();
		assertThat(patterns).hasSize(1);
		assertThat(patterns.iterator().next()).isEqualTo("s*".getBytes());
	}

	@Test
	public void testUnsubscribeAllNoChannels() {
		subscription.pSubscribe(new byte[][] { "s*".getBytes() });
		subscription.unsubscribe();
		verify(redisClient, never()).unsubscribe((Object[]) null);
		verify(redisClient, never()).punsubscribe((Object[]) null);
		assertThat(subscription.isAlive()).isTrue();
		assertThat(subscription.getChannels().isEmpty()).isTrue();
		Collection<byte[]> patterns = subscription.getPatterns();
		assertThat(patterns).hasSize(1);
		assertThat(patterns.iterator().next()).isEqualTo("s*".getBytes());
	}

	@Test
	public void testUnsubscribeNotAlive() {
		subscription.subscribe(new byte[][] { "a".getBytes() });
		subscription.unsubscribe();
		assertThat(subscription.isAlive()).isFalse();
		subscription.unsubscribe();
		verify(redisClient, times(1)).removeListener(any(ReplyListener.class));
		verify(redisClient, times(1)).unsubscribe((Object[]) null);
		verify(redisClient, never()).punsubscribe((Object[]) null);
	}

	@Test(expected = RedisInvalidSubscriptionException.class)
	public void testSubscribeNotAlive() {
		subscription.subscribe(new byte[][] { "a".getBytes() });
		subscription.unsubscribe();
		assertThat(subscription.isAlive()).isFalse();
		subscription.subscribe(new byte[][] { "s".getBytes() });
	}

	@Test
	public void testPUnsubscribeAllAndClose() {
		subscription.pSubscribe(new byte[][] { "a*".getBytes() });
		subscription.pUnsubscribe();
		verify(redisClient, never()).unsubscribe((Object[]) null);
		verify(redisClient, times(1)).punsubscribe((Object[]) null);
		assertThat(subscription.isAlive()).isFalse();
		verify(redisClient).removeListener(any(ReplyListener.class));
		assertThat(subscription.getChannels().isEmpty()).isTrue();
		assertThat(subscription.getPatterns().isEmpty()).isTrue();
	}

	@Test
	public void testPUnsubscribeAllPatternsWithChannels() {
		subscription.subscribe(new byte[][] { "a".getBytes() });
		subscription.pSubscribe(new byte[][] { "s*".getBytes() });
		subscription.pUnsubscribe();
		verify(redisClient, never()).unsubscribe((Object[]) null);
		verify(redisClient, times(1)).punsubscribe((Object[]) null);
		assertThat(subscription.isAlive()).isTrue();
		assertThat(subscription.getPatterns().isEmpty()).isTrue();
		Collection<byte[]> channels = subscription.getChannels();
		assertThat(channels).hasSize(1);
		assertThat(channels.iterator().next()).isEqualTo("a".getBytes());
	}

	@Test
	public void testPUnsubscribeAndClose() {
		byte[][] pattern = new byte[][] { "a*".getBytes() };
		subscription.pSubscribe(pattern);
		subscription.pUnsubscribe(pattern);
		verify(redisClient, never()).unsubscribe((Object[]) null);
		verify(redisClient, times(1)).punsubscribe((Object[]) pattern);
		assertThat(subscription.isAlive()).isFalse();
		verify(redisClient).removeListener(any(ReplyListener.class));
		assertThat(subscription.getChannels().isEmpty()).isTrue();
		assertThat(subscription.getPatterns().isEmpty()).isTrue();
	}

	@Test
	public void testPUnsubscribePatternSomeLeft() {
		byte[][] patterns = new byte[][] { "a*".getBytes(), "b*".getBytes() };
		subscription.pSubscribe(patterns);
		subscription.pUnsubscribe(new byte[][] { "a*".getBytes() });
		verify(redisClient, times(1)).punsubscribe((Object[]) new byte[][] { "a*".getBytes() });
		verify(redisClient, never()).unsubscribe((Object[]) null);
		assertThat(subscription.isAlive()).isTrue();
		Collection<byte[]> subPatterns = subscription.getPatterns();
		assertThat(subPatterns).hasSize(1);
		assertThat(subPatterns.iterator().next()).isEqualTo("b*".getBytes());
		assertThat(subscription.getChannels().isEmpty()).isTrue();
	}

	@Test
	public void testPUnsubscribePatternWithChannels() {
		byte[][] pattern = new byte[][] { "s*".getBytes() };
		subscription.subscribe(new byte[][] { "a".getBytes() });
		subscription.pSubscribe(pattern);
		subscription.pUnsubscribe(pattern);
		verify(redisClient, times(1)).punsubscribe((Object[]) pattern);
		verify(redisClient, never()).unsubscribe((Object[]) null);
		assertThat(subscription.isAlive()).isTrue();
		assertThat(subscription.getPatterns().isEmpty()).isTrue();
		Collection<byte[]> channels = subscription.getChannels();
		assertThat(channels).hasSize(1);
		assertThat(channels.iterator().next()).isEqualTo("a".getBytes());
	}

	@Test
	public void testUnsubscribePatternWithChannelsSomeLeft() {
		byte[][] pattern = new byte[][] { "a*".getBytes() };
		subscription.pSubscribe(new byte[][] { "a*".getBytes(), "b*".getBytes() });
		subscription.subscribe(new byte[][] { "a".getBytes() });
		subscription.pUnsubscribe(pattern);
		verify(redisClient, never()).unsubscribe((Object[]) null);
		verify(redisClient, times(1)).punsubscribe((Object[]) pattern);
		assertThat(subscription.isAlive()).isTrue();
		Collection<byte[]> channels = subscription.getChannels();
		assertThat(channels).hasSize(1);
		assertThat(channels.iterator().next()).isEqualTo("a".getBytes());
		Collection<byte[]> patterns = subscription.getPatterns();
		assertThat(patterns).hasSize(1);
		assertThat(patterns.iterator().next()).isEqualTo("b*".getBytes());
	}

	@Test
	public void testPUnsubscribeAllNoPatterns() {
		subscription.subscribe(new byte[][] { "s".getBytes() });
		subscription.pUnsubscribe();
		verify(redisClient, never()).unsubscribe((Object[]) null);
		verify(redisClient, never()).punsubscribe((Object[]) null);
		assertThat(subscription.isAlive()).isTrue();
		assertThat(subscription.getPatterns().isEmpty()).isTrue();
		Collection<byte[]> channels = subscription.getChannels();
		assertThat(channels).hasSize(1);
		assertThat(channels.iterator().next()).isEqualTo("s".getBytes());
	}

	@Test
	public void testPUnsubscribeNotAlive() {
		subscription.subscribe(new byte[][] { "a".getBytes() });
		subscription.unsubscribe();
		assertThat(subscription.isAlive()).isFalse();
		subscription.pUnsubscribe();
		verify(redisClient, times(1)).unsubscribe((Object[]) null);
		verify(redisClient, never()).punsubscribe((Object[]) null);
		verify(redisClient, times(1)).removeListener(any(ReplyListener.class));
	}

	@Test(expected = RedisInvalidSubscriptionException.class)
	public void testPSubscribeNotAlive() {
		subscription.subscribe(new byte[][] { "a".getBytes() });
		subscription.unsubscribe();
		assertThat(subscription.isAlive()).isFalse();
		subscription.pSubscribe(new byte[][] { "s*".getBytes() });
	}

	@Test
	public void testDoCloseNotSubscribed() {
		subscription.doClose();
		verify(redisClient, never()).unsubscribe((Object[]) null);
		verify(redisClient, never()).punsubscribe((Object[]) null);
	}

	@Test
	public void testDoCloseSubscribedChannels() {
		subscription.subscribe(new byte[][] { "a".getBytes() });
		subscription.doClose();
		verify(redisClient, times(1)).unsubscribe((Object[]) null);
		verify(redisClient, never()).punsubscribe((Object[]) null);
	}

	@Test
	public void testDoCloseSubscribedPatterns() {
		subscription.pSubscribe(new byte[][] { "a*".getBytes() });
		subscription.doClose();
		verify(redisClient, never()).unsubscribe((Object[]) null);
		verify(redisClient, times(1)).punsubscribe((Object[]) null);
	}

}
