/*
 * Copyright 2011-2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.redis.connection.jedis;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import redis.clients.jedis.BinaryJedisPubSub;

import java.util.Collection;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.RedisInvalidSubscriptionException;

/**
 * Unit test of {@link JedisSubscription}
 *
 * @author Jennifer Hickey
 */
@ExtendWith(MockitoExtension.class)
class JedisSubscriptionUnitTests {

	@Mock BinaryJedisPubSub jedisPubSub;

	@Mock MessageListener listener;

	JedisSubscription subscription;

	@BeforeEach
	void setUp() {
		subscription = new JedisSubscription(listener, jedisPubSub, null, null);
	}

	@Test
	void testUnsubscribeAllAndClose() {
		subscription.subscribe(new byte[][] { "a".getBytes() });
		subscription.unsubscribe();
		verify(jedisPubSub, times(1)).unsubscribe();
		verify(jedisPubSub, never()).punsubscribe();
		assertThat(subscription.isAlive()).isFalse();
		assertThat(subscription.getChannels()).isEmpty();
		assertThat(subscription.getPatterns()).isEmpty();
	}

	@Test
	void testUnsubscribeAllChannelsWithPatterns() {
		subscription.subscribe(new byte[][] { "a".getBytes() });
		subscription.pSubscribe(new byte[][] { "s*".getBytes() });
		subscription.unsubscribe();
		verify(jedisPubSub, times(1)).unsubscribe();
		verify(jedisPubSub, never()).punsubscribe();
		assertThat(subscription.isAlive()).isTrue();
		assertThat(subscription.getChannels()).isEmpty();
		Collection<byte[]> patterns = subscription.getPatterns();
		assertThat(patterns).hasSize(1);
		assertThat(patterns.iterator().next()).isEqualTo("s*".getBytes());
	}

	@Test
	void testUnsubscribeChannelAndClose() {
		byte[][] channel = new byte[][] { "a".getBytes() };
		subscription.subscribe(channel);
		subscription.unsubscribe(channel);
		verify(jedisPubSub, times(1)).unsubscribe(channel);
		verify(jedisPubSub, never()).punsubscribe();
		assertThat(subscription.isAlive()).isFalse();
		assertThat(subscription.getChannels()).isEmpty();
		assertThat(subscription.getPatterns()).isEmpty();
	}

	@Test
	void testUnsubscribeChannelSomeLeft() {
		byte[][] channels = new byte[][] { "a".getBytes(), "b".getBytes() };
		subscription.subscribe(channels);
		subscription.unsubscribe(new byte[][] { "a".getBytes() });
		verify(jedisPubSub, times(1)).unsubscribe(new byte[][] { "a".getBytes() });
		verify(jedisPubSub, never()).punsubscribe();
		assertThat(subscription.isAlive()).isTrue();
		Collection<byte[]> subChannels = subscription.getChannels();
		assertThat(subChannels).hasSize(1);
		assertThat(subChannels.iterator().next()).isEqualTo("b".getBytes());
		assertThat(subscription.getPatterns()).isEmpty();
	}

	@Test
	void testUnsubscribeChannelWithPatterns() {
		byte[][] channel = new byte[][] { "a".getBytes() };
		subscription.subscribe(channel);
		subscription.pSubscribe(new byte[][] { "s*".getBytes() });
		subscription.unsubscribe(channel);
		verify(jedisPubSub, times(1)).unsubscribe(channel);
		verify(jedisPubSub, never()).punsubscribe();
		assertThat(subscription.isAlive()).isTrue();
		assertThat(subscription.getChannels()).isEmpty();
		Collection<byte[]> patterns = subscription.getPatterns();
		assertThat(patterns).hasSize(1);
		assertThat(patterns.iterator().next()).isEqualTo("s*".getBytes());
	}

	@Test
	void testUnsubscribeChannelWithPatternsSomeLeft() {
		byte[][] channel = new byte[][] { "a".getBytes() };
		subscription.subscribe(new byte[][] { "a".getBytes(), "b".getBytes() });
		subscription.pSubscribe(new byte[][] { "s*".getBytes() });
		subscription.unsubscribe(channel);
		verify(jedisPubSub, times(1)).unsubscribe(channel);
		verify(jedisPubSub, never()).punsubscribe();
		assertThat(subscription.isAlive()).isTrue();
		Collection<byte[]> channels = subscription.getChannels();
		assertThat(channels).hasSize(1);
		assertThat(channels.iterator().next()).isEqualTo("b".getBytes());
		Collection<byte[]> patterns = subscription.getPatterns();
		assertThat(patterns).hasSize(1);
		assertThat(patterns.iterator().next()).isEqualTo("s*".getBytes());
	}

	@Test
	void testUnsubscribeAllNoChannels() {
		subscription.pSubscribe(new byte[][] { "s*".getBytes() });
		subscription.unsubscribe();
		verify(jedisPubSub, never()).unsubscribe();
		verify(jedisPubSub, never()).punsubscribe();
		assertThat(subscription.isAlive()).isTrue();
		assertThat(subscription.getChannels()).isEmpty();
		Collection<byte[]> patterns = subscription.getPatterns();
		assertThat(patterns).hasSize(1);
		assertThat(patterns.iterator().next()).isEqualTo("s*".getBytes());
	}

	@Test
	void testUnsubscribeNotAlive() {
		subscription.subscribe(new byte[][] { "a".getBytes() });
		subscription.unsubscribe();
		assertThat(subscription.isAlive()).isFalse();
		subscription.unsubscribe();
		verify(jedisPubSub, times(1)).unsubscribe();
		verify(jedisPubSub, never()).punsubscribe();
	}

	@Test
	void testSubscribeNotAlive() {
		subscription.subscribe(new byte[][] { "a".getBytes() });
		subscription.unsubscribe();
		assertThat(subscription.isAlive()).isFalse();

		assertThatExceptionOfType(RedisInvalidSubscriptionException.class)
				.isThrownBy(() -> subscription.subscribe(new byte[][] { "s".getBytes() }));
	}

	@Test
	void testPUnsubscribeAllAndClose() {
		subscription.pSubscribe(new byte[][] { "a*".getBytes() });
		subscription.pUnsubscribe();
		verify(jedisPubSub, never()).unsubscribe();
		verify(jedisPubSub, times(1)).punsubscribe();
		assertThat(subscription.isAlive()).isFalse();
		assertThat(subscription.getChannels()).isEmpty();
		assertThat(subscription.getPatterns()).isEmpty();
	}

	@Test
	void testPUnsubscribeAllPatternsWithChannels() {
		subscription.subscribe(new byte[][] { "a".getBytes() });
		subscription.pSubscribe(new byte[][] { "s*".getBytes() });
		subscription.pUnsubscribe();
		verify(jedisPubSub, never()).unsubscribe();
		verify(jedisPubSub, times(1)).punsubscribe();
		assertThat(subscription.isAlive()).isTrue();
		assertThat(subscription.getPatterns()).isEmpty();
		Collection<byte[]> channels = subscription.getChannels();
		assertThat(channels).hasSize(1);
		assertThat(channels.iterator().next()).isEqualTo("a".getBytes());
	}

	@Test
	void testPUnsubscribeAndClose() {
		byte[][] pattern = new byte[][] { "a*".getBytes() };
		subscription.pSubscribe(pattern);
		subscription.pUnsubscribe(pattern);
		verify(jedisPubSub, never()).unsubscribe();
		verify(jedisPubSub, times(1)).punsubscribe(pattern);
		assertThat(subscription.isAlive()).isFalse();
		assertThat(subscription.getChannels()).isEmpty();
		assertThat(subscription.getPatterns()).isEmpty();
	}

	@Test
	void testPUnsubscribePatternSomeLeft() {
		byte[][] patterns = new byte[][] { "a*".getBytes(), "b*".getBytes() };
		subscription.pSubscribe(patterns);
		subscription.pUnsubscribe(new byte[][] { "a*".getBytes() });
		verify(jedisPubSub, times(1)).punsubscribe(new byte[][] { "a*".getBytes() });
		verify(jedisPubSub, never()).unsubscribe();
		assertThat(subscription.isAlive()).isTrue();
		Collection<byte[]> subPatterns = subscription.getPatterns();
		assertThat(subPatterns).hasSize(1);
		assertThat(subPatterns.iterator().next()).isEqualTo("b*".getBytes());
		assertThat(subscription.getChannels()).isEmpty();
	}

	@Test
	void testPUnsubscribePatternWithChannels() {
		byte[][] pattern = new byte[][] { "s*".getBytes() };
		subscription.subscribe(new byte[][] { "a".getBytes() });
		subscription.pSubscribe(pattern);
		subscription.pUnsubscribe(pattern);
		verify(jedisPubSub, times(1)).punsubscribe(pattern);
		verify(jedisPubSub, never()).unsubscribe();
		assertThat(subscription.isAlive()).isTrue();
		assertThat(subscription.getPatterns()).isEmpty();
		Collection<byte[]> channels = subscription.getChannels();
		assertThat(channels).hasSize(1);
		assertThat(channels.iterator().next()).isEqualTo("a".getBytes());
	}

	@Test
	void testUnsubscribePatternWithChannelsSomeLeft() {
		byte[][] pattern = new byte[][] { "a*".getBytes() };
		subscription.pSubscribe(new byte[][] { "a*".getBytes(), "b*".getBytes() });
		subscription.subscribe(new byte[][] { "a".getBytes() });
		subscription.pUnsubscribe(pattern);
		verify(jedisPubSub, never()).unsubscribe();
		verify(jedisPubSub, times(1)).punsubscribe(pattern);
		assertThat(subscription.isAlive()).isTrue();
		Collection<byte[]> channels = subscription.getChannels();
		assertThat(channels).hasSize(1);
		assertThat(channels.iterator().next()).isEqualTo("a".getBytes());
		Collection<byte[]> patterns = subscription.getPatterns();
		assertThat(patterns).hasSize(1);
		assertThat(patterns.iterator().next()).isEqualTo("b*".getBytes());
	}

	@Test
	void testPUnsubscribeAllNoPatterns() {
		subscription.subscribe(new byte[][] { "s".getBytes() });
		subscription.pUnsubscribe();
		verify(jedisPubSub, never()).unsubscribe();
		verify(jedisPubSub, never()).punsubscribe();
		assertThat(subscription.isAlive()).isTrue();
		assertThat(subscription.getPatterns()).isEmpty();
		Collection<byte[]> channels = subscription.getChannels();
		assertThat(channels).hasSize(1);
		assertThat(channels.iterator().next()).isEqualTo("s".getBytes());
	}

	@Test
	void testPUnsubscribeNotAlive() {
		subscription.subscribe(new byte[][] { "a".getBytes() });
		subscription.unsubscribe();
		assertThat(subscription.isAlive()).isFalse();
		subscription.pUnsubscribe();
		verify(jedisPubSub, times(1)).unsubscribe();
		verify(jedisPubSub, never()).punsubscribe();
	}

	@Test
	void testPSubscribeNotAlive() {
		subscription.subscribe(new byte[][] { "a".getBytes() });
		subscription.unsubscribe();
		assertThat(subscription.isAlive()).isFalse();

		assertThatExceptionOfType(RedisInvalidSubscriptionException.class)
				.isThrownBy(() -> subscription.pSubscribe(new byte[][] { "s*".getBytes() }));
	}

	@Test
	void testDoCloseNotSubscribed() {
		subscription.doClose();
		verify(jedisPubSub, never()).unsubscribe();
		verify(jedisPubSub, never()).punsubscribe();
	}

	@Test
	void testDoCloseSubscribedChannels() {
		subscription.subscribe(new byte[][] { "a".getBytes() });
		subscription.doClose();
		verify(jedisPubSub, times(1)).unsubscribe();
		verify(jedisPubSub, never()).punsubscribe();
	}

	@Test
	void testDoCloseSubscribedPatterns() {
		subscription.pSubscribe(new byte[][] { "a*".getBytes() });
		subscription.doClose();
		verify(jedisPubSub, never()).unsubscribe();
		verify(jedisPubSub, times(1)).punsubscribe();
	}

}
