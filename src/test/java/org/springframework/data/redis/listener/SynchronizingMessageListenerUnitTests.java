/*
 * Copyright 2026-present the original author or authors.
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
package org.springframework.data.redis.listener;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.SubscriptionListener;
import org.springframework.data.redis.connection.util.ByteArrayWrapper;
import org.springframework.data.redis.listener.SynchronizingMessageListener.SubscriptionSynchronization;

/**
 * Unit tests for {@link SynchronizingMessageListener}.
 *
 * @author Daeryeon Choi
 */
class SynchronizingMessageListenerUnitTests {

	@Test // GH-3508
	void doneCallbackShouldBeInvokedOnceEvenWhenSubscribedRepeatedly() {

		SynchronizingMessageListener listener = new SynchronizingMessageListener(mock(MessageListener.class),
				mock(SubscriptionListener.class));
		AtomicInteger invocations = new AtomicInteger();
		byte[] channel = "ch-1".getBytes(StandardCharsets.UTF_8);
		byte[] pattern = "pat-*".getBytes(StandardCharsets.UTF_8);

		SubscriptionSynchronization synchronization = new SubscriptionSynchronization(List.of(pattern), List.of(channel),
				invocations::incrementAndGet);
		listener.addSynchronization(synchronization);

		listener.onChannelSubscribed(channel, 1);
		assertThat(invocations).hasValue(0);

		listener.onPatternSubscribed(pattern, 2);
		assertThat(invocations).hasValue(1);

		assertThat(synchronization.onChannelSubscribed(new ByteArrayWrapper(channel))).isFalse();
		assertThat(synchronization.onPatternSubscribed(new ByteArrayWrapper(pattern))).isFalse();
		listener.onChannelSubscribed(channel, 3);
		listener.onPatternSubscribed(pattern, 4);

		assertThat(invocations).hasValue(1);
	}

	@Test // GH-3508
	void removeSynchronizationShouldPreventCallbackExecution() {

		SynchronizingMessageListener listener = new SynchronizingMessageListener(mock(MessageListener.class),
				mock(SubscriptionListener.class));
		AtomicInteger invocations = new AtomicInteger();
		byte[] channel = "ch-1".getBytes(StandardCharsets.UTF_8);

		SubscriptionSynchronization synchronization = new SubscriptionSynchronization(List.of(), List.of(channel),
				invocations::incrementAndGet);
		listener.addSynchronization(synchronization);
		listener.removeSynchronization(synchronization);

		assertThat(synchronization.onChannelSubscribed(new ByteArrayWrapper(channel))).isFalse();
		listener.onChannelSubscribed(channel, 1);

		SubscriptionSynchronization second = new SubscriptionSynchronization(List.of(), List.of(channel),
				invocations::incrementAndGet);
		listener.addSynchronization(second);
		listener.clearSynchronizations();

		assertThat(second.onChannelSubscribed(new ByteArrayWrapper(channel))).isFalse();
		listener.onChannelSubscribed(channel, 2);

		assertThat(invocations).hasValue(0);
	}
}
