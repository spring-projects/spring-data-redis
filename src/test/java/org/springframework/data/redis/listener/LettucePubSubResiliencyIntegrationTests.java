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
import static org.awaitility.Awaitility.*;
import static org.mockito.AdditionalAnswers.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.pubsub.api.reactive.RedisPubSubReactiveCommands;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.dao.QueryTimeoutException;
import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.ReactiveRedisConnection;
import org.springframework.data.redis.connection.ReactiveSubscription;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.SubscriptionListener;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionProvider;
import org.springframework.data.redis.connection.lettuce.LettucePoolingClientConfiguration;
import org.springframework.data.redis.connection.util.ByteArrayWrapper;
import org.springframework.data.redis.listener.adapter.RedisListenerExecutionFailedException;
import org.springframework.data.redis.test.condition.EnabledOnRedisAvailable;
import org.springframework.data.redis.test.extension.LettuceTestClientResources;
import org.springframework.test.util.ReflectionTestUtils;

/**
 * Integration tests verifying Pub/Sub state consistency, rollback, and reconnect self-healing against a real Redis
 * instance via a fault-injecting TCP proxy.
 *
 * @author Daeryeon Choi
 */
@EnabledOnRedisAvailable
class LettucePubSubResiliencyIntegrationTests {

	private static final Duration COMMAND_TIMEOUT = Duration.ofMillis(250);

	private RedisTcpProxy proxy;
	private LettuceConnectionFactory proxiedFactory;
	private RedisClient controlClient;
	private StatefulRedisConnection<byte[], byte[]> controlConnection;

	@BeforeEach
	void setUp() throws Exception {

		proxy = new RedisTcpProxy(SettingsUtils.getHost(), SettingsUtils.getPort());
		proxy.start();

		LettuceClientConfiguration clientConfig = LettuceClientConfiguration.builder()
				.clientResources(LettuceTestClientResources.getSharedClientResources())
				.clientOptions(ClientOptions.builder().autoReconnect(true).build()) //
				.commandTimeout(COMMAND_TIMEOUT) //
				.shutdownTimeout(Duration.ZERO) //
				.shutdownQuietPeriod(Duration.ZERO) //
				.build();

		proxiedFactory = new LettuceConnectionFactory(new RedisStandaloneConfiguration("127.0.0.1", proxy.getPort()),
				clientConfig);
		proxiedFactory.afterPropertiesSet();
		proxiedFactory.start();

		controlClient = RedisClient.create(LettuceTestClientResources.getSharedClientResources(),
				RedisURI.create(SettingsUtils.getHost(), SettingsUtils.getPort()));
		controlConnection = controlClient.connect(ByteArrayCodec.INSTANCE);
	}

	@AfterEach
	void tearDown() throws Exception {

		if (controlConnection != null) {
			controlConnection.close();
		}
		if (controlClient != null) {
			controlClient.shutdown(0, 0, TimeUnit.MILLISECONDS);
		}
		if (proxiedFactory != null) {
			proxiedFactory.destroy();
		}
		if (proxy != null) {
			proxy.close();
		}
	}

	@Test // GH-3508
	void partialUnsubscribeTimeoutShouldRollbackAndAllowRetryToZeroNumsub() throws Exception {

		String suffix = UUID.randomUUID().toString();
		String ch1 = "pubsub:unsubscribe:ch1:" + suffix;
		String ch2 = "pubsub:unsubscribe:ch2:" + suffix;
		String ch3 = "pubsub:unsubscribe:ch3:" + suffix;

		RedisMessageListenerContainer container = createStartedContainer();
		try {
			BlockingQueue<String> ch2Messages = new LinkedBlockingDeque<>();
			BlockingQueue<String> ch3Messages = new LinkedBlockingDeque<>();

			MessageListener listener1 = (msg, pat) -> {};
			MessageListener listener2 = (msg, pat) -> ch2Messages.add(new String(msg.getBody(), StandardCharsets.UTF_8));
			MessageListener listener3 = (msg, pat) -> ch3Messages.add(new String(msg.getBody(), StandardCharsets.UTF_8));

			container.addMessageListener(listener1, new ChannelTopic(ch1));
			container.addMessageListener(listener2, new ChannelTopic(ch2));
			container.addMessageListener(listener3, new ChannelTopic(ch3));

			await().atMost(Duration.ofSeconds(5))
					.until(() -> numSub(ch1) == 1L && numSub(ch2) == 1L && numSub(ch3) == 1L);

			// Pause client -> server so UNSUBSCRIBE times out without reaching Redis, then reset socket so Lettuce reconnects
			proxy.setPauseClientToServer(true);

			assertThatExceptionOfType(QueryTimeoutException.class)
					.isThrownBy(() -> container.removeMessageListener(listener1, new ChannelTopic(ch1)));

			assertThat(numSub(ch1)).isEqualTo(1L);

			// Drop the stalled TCP session before unpausing so stale UNSUBSCRIBE never leaks through and Lettuce reconnects
			proxy.disconnectActiveConnections();
			proxy.setPauseClientToServer(false);

			// Wait for Lettuce reconnect to re-establish ch3 delivery
			await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
				publish(ch3, "reconnected");
				assertThat(ch3Messages.poll(200, TimeUnit.MILLISECONDS)).isEqualTo("reconnected");
			});

			// Retry partial unsubscribe by topic: must actually send UNSUBSCRIBE ch1
			container.removeMessageListener(listener1, new ChannelTopic(ch1));
			await().atMost(Duration.ofSeconds(5)).until(() -> numSub(ch1) == 0L);
			assertThat(numSub(ch2)).isEqualTo(1L);
			assertThat(numSub(ch3)).isEqualTo(1L);

			publish(ch2, "alive");
			assertThat(ch2Messages.poll(3, TimeUnit.SECONDS)).isEqualTo("alive");

			// Second failure on removeMessageListener(listener) (partial, while listener3 remains) must also rollback and retry
			proxy.setPauseClientToServer(true);
			assertThatExceptionOfType(QueryTimeoutException.class)
					.isThrownBy(() -> container.removeMessageListener(listener2));
			proxy.disconnectActiveConnections();
			proxy.setPauseClientToServer(false);

			await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
				publish(ch3, "reconnected-2");
				assertThat(ch3Messages.poll(200, TimeUnit.MILLISECONDS)).isEqualTo("reconnected-2");
			});

			container.removeMessageListener(listener2);
			await().atMost(Duration.ofSeconds(5)).until(() -> numSub(ch2) == 0L);
			assertThat(numSub(ch3)).isEqualTo(1L);
		} finally {
			container.destroy();
		}
	}

	@Test // GH-3508
	void dynamicSubscribeTimeoutShouldRollbackAndSelfHealDelayedOrReconnectedOrphan() throws Exception {

		String suffix = UUID.randomUUID().toString();
		String ch1 = "pubsub:subscribe:ch1:" + suffix;
		String ch2 = "pubsub:subscribe:ch2:" + suffix;

		RedisMessageListenerContainer container = createStartedContainer();
		try {
			BlockingQueue<String> ch1Messages = new LinkedBlockingDeque<>();
			MessageListener listener1 = (msg, pat) -> ch1Messages.add(new String(msg.getBody(), StandardCharsets.UTF_8));
			MessageListener listener2 = (msg, pat) -> {};

			container.addMessageListener(listener1, new ChannelTopic(ch1));
			await().atMost(Duration.ofSeconds(5)).until(() -> numSub(ch1) == 1L);

			// Pause server -> client replies so SUBSCRIBE ch2 reaches Redis, but client times out
			proxy.setPauseServerToClient(true);

			assertThatExceptionOfType(RedisListenerExecutionFailedException.class)
					.isThrownBy(() -> container.addMessageListener(listener2, new ChannelTopic(ch2)))
					.withCauseInstanceOf(QueryTimeoutException.class);

			// Release delayed ACK and trigger disconnect/reconnect so resubscribe also fires
			proxy.setPauseServerToClient(false);
			proxy.disconnectActiveConnections();

			// Orphaned ch2 must be self-healed to 0 while ch1 stays subscribed and receives messages
			await().atMost(Duration.ofSeconds(10)).until(() -> numSub(ch2) == 0L && numSub(ch1) == 1L);

			publish(ch1, "still-here");
			assertThat(ch1Messages.poll(5, TimeUnit.SECONDS)).isEqualTo("still-here");

			// Retrying addMessageListener on ch2 succeeds cleanly
			container.addMessageListener(listener2, new ChannelTopic(ch2));
			await().atMost(Duration.ofSeconds(5)).until(() -> numSub(ch2) == 1L);
		} finally {
			container.destroy();
		}
	}

	@Test // GH-3508
	void reactiveSecondSubscriberTimeoutShouldNotCorruptFirstSubscriberOrLeakNativeListener() throws Exception {

		String suffix = UUID.randomUUID().toString();
		String channel = "pubsub:reactive:shared:" + suffix;

		ReactiveRedisMessageListenerContainer container = new ReactiveRedisMessageListenerContainer(proxiedFactory);
		try {
			BlockingQueue<String> received = new LinkedBlockingDeque<>();
			CompletableFuture<Void> sub1Ready = new CompletableFuture<>();

			Disposable sub1 = container.receive(List.of(new ChannelTopic(channel)), new SubscriptionListener() {
				@Override
				public void onChannelSubscribed(byte[] ch, long count) {
					if (channel.equals(new String(ch, StandardCharsets.UTF_8))) {
						sub1Ready.complete(null);
					}
				}
			}).doOnNext(msg -> received.add(msg.getMessage())).subscribe();

			sub1Ready.get(5, TimeUnit.SECONDS);
			await().atMost(Duration.ofSeconds(5)).until(() -> numSub(channel) == 1L);
			assertThat(countWrapperListeners(container)).isEqualTo(1);

			// Pause server -> client replies so sub2 / receiveLater time out while keeping RESP framing intact
			proxy.setPauseServerToClient(true);

			container.receive(new ChannelTopic(channel)) //
					.as(StepVerifier::create) //
					.expectError(QueryTimeoutException.class) //
					.verify(Duration.ofSeconds(5));

			// Also test receiveLater failure cleanup
			container.receiveLater(new ChannelTopic(channel)) //
					.as(StepVerifier::create) //
					.expectError(QueryTimeoutException.class) //
					.verify(Duration.ofSeconds(5));

			proxy.setPauseServerToClient(false);

			// No native listener leak from failed sub2 / receiveLater attempts
			await().atMost(Duration.ofSeconds(5)).until(() -> countWrapperListeners(container) == 1);

			// First subscriber still receives messages
			publish(channel, "hello-sub1");
			assertThat(received.poll(5, TimeUnit.SECONDS)).isEqualTo("hello-sub1");

			// Disposing sub1 must actually send UNSUBSCRIBE (ref count was not corrupted)
			sub1.dispose();
			await().atMost(Duration.ofSeconds(5)).until(() -> numSub(channel) == 0L);
			await().atMost(Duration.ofSeconds(5)).until(() -> countWrapperListeners(container) == 0);
		} finally {
			container.destroy();
		}
	}

	@Test // GH-3508
	void reactiveDisposeTimeoutShouldCleanupZombieSubscriptionAfterReconnect() throws Exception {

		String suffix = UUID.randomUUID().toString();
		String channel = "pubsub:reactive:cancel:" + suffix;

		ReactiveRedisMessageListenerContainer container = new ReactiveRedisMessageListenerContainer(proxiedFactory);
		try {
			CompletableFuture<Void> ready = new CompletableFuture<>();
			Disposable subscription = container.receive(List.of(new ChannelTopic(channel)), new SubscriptionListener() {
				@Override
				public void onChannelSubscribed(byte[] ch, long count) {
					if (channel.equals(new String(ch, StandardCharsets.UTF_8))) {
						ready.complete(null);
					}
				}
			}).subscribe();

			ready.get(5, TimeUnit.SECONDS);
			await().atMost(Duration.ofSeconds(5)).until(() -> numSub(channel) == 1L);
			assertThat(countWrapperListeners(container)).isEqualTo(1);

			// Pause client -> server so dispose() -> cancel() -> UNSUBSCRIBE times out without reaching Redis
			proxy.setPauseClientToServer(true);
			subscription.dispose();

			// Wait longer than COMMAND_TIMEOUT so cancel()'s UNSUBSCRIBE fails
			Thread.sleep(COMMAND_TIMEOUT.toMillis() + 150);
			assertThat(numSub(channel)).isEqualTo(1L);

			// Bounce socket before unpausing so Lettuce ConnectionWatchdog reconnects and replays SUBSCRIBE
			proxy.disconnectActiveConnections();
			proxy.setPauseClientToServer(false);

			await().atMost(Duration.ofSeconds(10)).until(() -> numSub(channel) == 0L);
			await().atMost(Duration.ofSeconds(10)).until(() -> countWrapperListeners(container) == 0);
		} finally {
			container.destroy();
		}
	}

	@Test // GH-3508
	void partialUnsubscribeDelayedAckAndReconnectShouldRestoreRetainedChannelDelivery() throws Exception {

		String suffix = UUID.randomUUID().toString();
		String chRetain = "pubsub:unsubscribe:retain:" + suffix;
		String chSurvive = "pubsub:unsubscribe:survive:" + suffix;

		RedisMessageListenerContainer container = createStartedContainer();
		try {
			BlockingQueue<String> retainedMessages = new LinkedBlockingDeque<>();
			MessageListener retainedListener = (msg, pat) -> retainedMessages
					.add(new String(msg.getBody(), StandardCharsets.UTF_8));
			MessageListener survivingListener = (msg, pat) -> {};

			container.addMessageListener(retainedListener, new ChannelTopic(chRetain));
			container.addMessageListener(survivingListener, new ChannelTopic(chSurvive));
			await().atMost(Duration.ofSeconds(5)).until(() -> numSub(chRetain) == 1L && numSub(chSurvive) == 1L);

			// Pause S->C so Redis executes UNSUBSCRIBE chRetain (NUMSUB -> 0) while the client times out and rolls back
			proxy.setPauseServerToClient(true);
			assertThatExceptionOfType(QueryTimeoutException.class)
					.isThrownBy(() -> container.removeMessageListener(retainedListener, new ChannelTopic(chRetain)));
			await().atMost(Duration.ofSeconds(5)).until(() -> numSub(chRetain) == 0L);

			// Release delayed UNSUBSCRIBE ACK and bounce TCP session; container must restore chRetain across reconnect
			proxy.setPauseServerToClient(false);
			await().atMost(Duration.ofSeconds(10)).until(() -> numSub(chRetain) == 1L && numSub(chSurvive) == 1L);

			proxy.disconnectActiveConnections();
			await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
				publish(chRetain, "still-subscribed");
				assertThat(retainedMessages.poll(200, TimeUnit.MILLISECONDS)).isEqualTo("still-subscribed");
			});

			container.removeMessageListener(retainedListener, new ChannelTopic(chRetain));
			await().atMost(Duration.ofSeconds(5)).until(() -> numSub(chRetain) == 0L);
		} finally {
			container.destroy();
		}
	}

	@Test // GH-3508
	void reactiveDuplicateCancelDuringSlowUnsubscribeShouldPreserveOtherSubscriberOnSharedChannel() throws Exception {

		String suffix = UUID.randomUUID().toString();
		String sharedChannel = "pubsub:reactive:dup-cancel-shared:" + suffix;
		String slowChannel = "pubsub:reactive:dup-cancel-slow:" + suffix;

		ReactiveRedisConnection reactiveConnection = proxiedFactory.getReactiveConnection();
		try {
			ReactiveSubscription subA = reactiveConnection.pubSubCommands()
					.createSubscription(SubscriptionListener.NO_OP_SUBSCRIPTION_LISTENER).block(Duration.ofSeconds(5));
			ReactiveSubscription subB = reactiveConnection.pubSubCommands()
					.createSubscription(SubscriptionListener.NO_OP_SUBSCRIPTION_LISTENER).block(Duration.ofSeconds(5));

			BlockingQueue<String> subBMessages = new LinkedBlockingDeque<>();
			Disposable receiveB = subB.receive().doOnNext(
					msg -> subBMessages.add(StandardCharsets.UTF_8.decode(msg.getMessage().duplicate()).toString())).subscribe();

			try {
				subA.subscribe(ByteBuffer.wrap(sharedChannel.getBytes(StandardCharsets.UTF_8)),
						ByteBuffer.wrap(slowChannel.getBytes(StandardCharsets.UTF_8))).block(Duration.ofSeconds(5));
				subB.subscribe(ByteBuffer.wrap(sharedChannel.getBytes(StandardCharsets.UTF_8))).block(Duration.ofSeconds(5));
				await().atMost(Duration.ofSeconds(5))
						.until(() -> numSub(sharedChannel) == 1L && numSub(slowChannel) == 1L);

				// Hold S->C briefly while two concurrent cancel() calls execute on subA
				proxy.setPauseServerToClient(true);
				CompletableFuture<Void> cancel1 = subA.cancel().toFuture();
				CompletableFuture<Void> cancel2 = subA.cancel().toFuture();
				proxy.setPauseServerToClient(false);

				cancel1.get(5, TimeUnit.SECONDS);
				cancel2.get(5, TimeUnit.SECONDS);

				assertThat(numSub(slowChannel)).isZero();
				assertThat(numSub(sharedChannel)).isEqualTo(1L);

				publish(sharedChannel, "for-subB");
				assertThat(subBMessages.poll(5, TimeUnit.SECONDS)).isEqualTo("for-subB");
			} finally {
				receiveB.dispose();
				subB.cancel().block(Duration.ofSeconds(5));
			}
		} finally {
			reactiveConnection.closeLater().block(Duration.ofSeconds(5));
		}
	}

	@Test // GH-3508
	void emptyContainerInitialAddListenerFailureShouldRollbackCleanupOrphanAndNotLeakOnNextAdd() throws Exception {

		String suffix = UUID.randomUUID().toString();
		String chFail = "pubsub:empty:fail:" + suffix;
		String chNext = "pubsub:empty:next:" + suffix;

		RedisMessageListenerContainer container = createStartedContainer();
		try {
			BlockingQueue<String> nextMessages = new LinkedBlockingDeque<>();
			MessageListener failListener = (msg, pat) -> {};
			MessageListener nextListener = (msg, pat) -> nextMessages.add(new String(msg.getBody(), StandardCharsets.UTF_8));

			// Delayed S->C ACK after SUBSCRIBE reaches Redis causes initial addMessageListener failure
			proxy.setPauseServerToClientOnSubscribe(true);
			assertThatExceptionOfType(RedisListenerExecutionFailedException.class)
					.isThrownBy(() -> container.addMessageListener(failListener, new ChannelTopic(chFail)))
					.withCauseInstanceOf(QueryTimeoutException.class);

			proxy.setPauseServerToClient(false);
			await().atMost(Duration.ofSeconds(5)).until(() -> numSub(chFail) == 0L && !container.isListening());

			// Subsequent healthy addMessageListener subscribes ONLY chNext, never resurrected chFail
			container.addMessageListener(nextListener, new ChannelTopic(chNext));
			await().atMost(Duration.ofSeconds(5)).until(() -> numSub(chNext) == 1L);
			assertThat(numSub(chFail)).isZero();

			publish(chNext, "hello-next");
			assertThat(nextMessages.poll(5, TimeUnit.SECONDS)).isEqualTo("hello-next");
		} finally {
			container.destroy();
		}
	}

	@Test // GH-3508
	void staticListenerRegisteredBeforeStartShouldSurviveStartFailureAndSubscribeOnRestart() throws Exception {

		String suffix = UUID.randomUUID().toString();
		String chStatic = "pubsub:static:ch0:" + suffix;
		String chFailedRuntime = "pubsub:static:ch1:" + suffix;

		RedisMessageListenerContainer container = new RedisMessageListenerContainer();
		container.setConnectionFactory(proxiedFactory);
		container.setRecoveryInterval(50);
		container.setBeanName("static-restart-container");
		container.afterPropertiesSet();

		try {
			BlockingQueue<String> staticMessages = new LinkedBlockingDeque<>();
			MessageListener staticListener = (msg, pat) -> staticMessages
					.add(new String(msg.getBody(), StandardCharsets.UTF_8));
			MessageListener failedRuntimeListener = (msg, pat) -> {};

			container.addMessageListener(staticListener, new ChannelTopic(chStatic));

			proxy.setPauseServerToClientOnSubscribe(true);
			assertThatExceptionOfType(RedisListenerExecutionFailedException.class).isThrownBy(container::start)
					.withCauseInstanceOf(QueryTimeoutException.class);
			assertThat(container.isRunning()).isTrue();
			assertThat(container.isListening()).isFalse();

			proxy.setPauseServerToClient(false);
			await().atMost(Duration.ofSeconds(5)).until(() -> numSub(chStatic) == 0L);

			// Runtime failure while not listening rolls back only chFailedRuntime, preserving chStatic
			proxy.setPauseServerToClientOnSubscribe(true);
			assertThatExceptionOfType(RedisListenerExecutionFailedException.class)
					.isThrownBy(() -> container.addMessageListener(failedRuntimeListener, new ChannelTopic(chFailedRuntime)))
					.withCauseInstanceOf(QueryTimeoutException.class);

			proxy.setPauseServerToClient(false);
			await().atMost(Duration.ofSeconds(5)).until(() -> numSub(chStatic) == 0L && numSub(chFailedRuntime) == 0L);

			// stop() + start() re-initializes and subscribes staticListener on chStatic only
			container.stop();
			container.start();

			await().atMost(Duration.ofSeconds(5)).until(() -> numSub(chStatic) == 1L);
			assertThat(numSub(chFailedRuntime)).isZero();

			publish(chStatic, "hello-static");
			assertThat(staticMessages.poll(5, TimeUnit.SECONDS)).isEqualTo("hello-static");
		} finally {
			container.destroy();
		}
	}

	@Test // GH-3508
	void reactiveFirstSubscriberTimeoutOnNewChannelsShouldSelfHealOnLateAckAndReconnect() throws Exception {

		String suffix = UUID.randomUUID().toString();
		String warmupChannel = "pubsub:reactive:warmup:" + suffix;
		String chReceive = "pubsub:reactive:first-receive:" + suffix;
		String chReceiveLater = "pubsub:reactive:first-receive-later:" + suffix;
		String chReconnect = "pubsub:reactive:reconnect-receive:" + suffix;
		String chReconnectLater = "pubsub:reactive:reconnect-receive-later:" + suffix;
		String aliveChannel = "pubsub:reactive:alive:" + suffix;

		ReactiveRedisMessageListenerContainer container = new ReactiveRedisMessageListenerContainer(proxiedFactory);
		try {
			// Warm up the shared pubSubConnection, then unsubscribe so there are zero active subscribers
			CompletableFuture<Void> warmupReady = new CompletableFuture<>();
			Disposable warmupSub = container.receive(List.of(new ChannelTopic(warmupChannel)), new SubscriptionListener() {
				@Override
				public void onChannelSubscribed(byte[] ch, long count) {
					if (warmupChannel.equals(new String(ch, StandardCharsets.UTF_8))) {
						warmupReady.complete(null);
					}
				}
			}).subscribe();
			warmupReady.get(5, TimeUnit.SECONDS);
			warmupSub.dispose();
			await().atMost(Duration.ofSeconds(5))
					.until(() -> numSub(warmupChannel) == 0L && countWrapperListeners(container) == 0);

			// 1) Pause S->C replies so first receive() and receiveLater() on brand-new channels time out after hitting Redis
			proxy.setPauseServerToClient(true);

			container.receive(new ChannelTopic(chReceive)) //
					.as(StepVerifier::create) //
					.expectError(QueryTimeoutException.class) //
					.verify(Duration.ofSeconds(5));

			container.receiveLater(new ChannelTopic(chReceiveLater)) //
					.as(StepVerifier::create) //
					.expectError(QueryTimeoutException.class) //
					.verify(Duration.ofSeconds(5));

			await().atMost(Duration.ofSeconds(5)).until(() -> numSub(chReceive) == 1L && numSub(chReceiveLater) == 1L);
			assertThat(countWrapperListeners(container)).isZero();

			// Checkpoint 1: when delayed ACKs arrive, orphan cleanup unsubscribes both channels to 0 and evicts tracking
			proxy.setPauseServerToClient(false);
			await().atMost(Duration.ofSeconds(5))
					.until(() -> numSub(chReceive) == 0L && numSub(chReceiveLater) == 0L && countRolledBackTargets(container) == 0);

			// Attach an active canary channel to observe Lettuce auto-reconnect completion
			BlockingQueue<String> aliveMessages = new LinkedBlockingDeque<>();
			CompletableFuture<Void> aliveReady = new CompletableFuture<>();
			Disposable aliveSub = container.receive(List.of(new ChannelTopic(aliveChannel)), new SubscriptionListener() {
				@Override
				public void onChannelSubscribed(byte[] ch, long count) {
					if (aliveChannel.equals(new String(ch, StandardCharsets.UTF_8))) {
						aliveReady.complete(null);
					}
				}
			}).doOnNext(msg -> aliveMessages.add(msg.getMessage())).subscribe();

			try {
				aliveReady.get(5, TimeUnit.SECONDS);
				await().atMost(Duration.ofSeconds(5)).until(() -> numSub(aliveChannel) == 1L);

				// Checkpoint 2a: disconnect & reconnect must not resurrect chReceive / chReceiveLater
				proxy.disconnectActiveConnections();
				await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
					publish(aliveChannel, "reconnected-1");
					assertThat(aliveMessages.poll(200, TimeUnit.MILLISECONDS)).isEqualTo("reconnected-1");
				});
				assertThat(numSub(chReceive)).isZero();
				assertThat(numSub(chReceiveLater)).isZero();

				// Checkpoint 2b: even if late ACKs populate PubSubEndpoint while C->S is blocked (preventing pre-disconnect
				// UNSUBSCRIBE), forced disconnect + Lettuce auto-reconnect must self-heal replayed subscriptions to 0
				proxy.setPauseServerToClient(true);
				container.receive(new ChannelTopic(chReconnect)) //
						.as(StepVerifier::create) //
						.expectError(QueryTimeoutException.class) //
						.verify(Duration.ofSeconds(5));
				container.receiveLater(new ChannelTopic(chReconnectLater)) //
						.as(StepVerifier::create) //
						.expectError(QueryTimeoutException.class) //
						.verify(Duration.ofSeconds(5));

				await().atMost(Duration.ofSeconds(5))
						.until(() -> numSub(chReconnect) == 1L && numSub(chReconnectLater) == 1L);

				proxy.setPauseClientToServer(true);
				proxy.setPauseServerToClient(false);
				Thread.sleep(COMMAND_TIMEOUT.toMillis() + 150);
				assertThat(numSub(chReconnect)).isEqualTo(1L);
				assertThat(numSub(chReconnectLater)).isEqualTo(1L);

				proxy.disconnectActiveConnections();
				proxy.setPauseClientToServer(false);

				await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
					publish(aliveChannel, "reconnected-2");
					assertThat(aliveMessages.poll(200, TimeUnit.MILLISECONDS)).isEqualTo("reconnected-2");
				});
				await().atMost(Duration.ofSeconds(10)).until(
						() -> numSub(chReconnect) == 0L && numSub(chReconnectLater) == 0L && countRolledBackTargets(container) == 0);
				assertThat(numSub(aliveChannel)).isEqualTo(1L);
			} finally {
				aliveSub.dispose();
				await().atMost(Duration.ofSeconds(5))
						.until(() -> numSub(aliveChannel) == 0L && countWrapperListeners(container) == 0);
			}
		} finally {
			container.destroy();
		}
	}

	@Test // GH-3508
	@SuppressWarnings("unchecked")
	void reactiveOrphanCleanupPausedBeforeSendShouldPreserveConcurrentNewSubscriptionAndDeliverMessages()
			throws Exception {

		String suffix = UUID.randomUUID().toString();
		String warmupChannel = "pubsub:reactive:race-warmup:" + suffix;
		String channel = "pubsub:reactive:cleanup-vs-sub:" + suffix;

		AtomicBoolean pauseNextUnsubscribe = new AtomicBoolean();
		CountDownLatch cleanupPrepared = new CountDownLatch(1);
		CountDownLatch allowCleanupSend = new CountDownLatch(1);

		LettuceConnectionProvider realProvider = (LettuceConnectionProvider) ReflectionTestUtils.getField(proxiedFactory,
				"reactiveConnectionProvider");
		LettuceConnectionProvider interceptingProvider = createUnsubscribePausingProvider(realProvider,
				pauseNextUnsubscribe, cleanupPrepared, allowCleanupSend);
		ReflectionTestUtils.setField(proxiedFactory, "reactiveConnectionProvider", interceptingProvider);

		ReactiveRedisMessageListenerContainer container = new ReactiveRedisMessageListenerContainer(proxiedFactory);
		try {
			// Warm up the shared pubSubConnection, then dispose so there are zero active subscribers
			Disposable warmupSub = container.receiveLater(new ChannelTopic(warmupChannel)).block(Duration.ofSeconds(5))
					.subscribe();
			warmupSub.dispose();
			await().atMost(Duration.ofSeconds(5)).until(() -> numSub(warmupChannel) == 0L);

			// First subscription times out on client after SUBSCRIBE reaches Redis
			proxy.setPauseServerToClient(true);
			container.receiveLater(new ChannelTopic(channel)) //
					.as(StepVerifier::create) //
					.expectError(QueryTimeoutException.class) //
					.verify(Duration.ofSeconds(5));
			await().atMost(Duration.ofSeconds(5)).until(() -> numSub(channel) == 1L);

			// Unpause S->C while arming the pre-send pause on the orphan cleanup UNSUBSCRIBE
			pauseNextUnsubscribe.set(true);
			proxy.setPauseServerToClient(false);
			assertThat(cleanupPrepared.await(5, TimeUnit.SECONDS)).isTrue();

			// Start a new subscriber on the same channel while cleanup UNSUBSCRIBE is prepared but not yet sent
			BlockingQueue<String> received = new LinkedBlockingDeque<>();
			CompletableFuture<Disposable> newSubFuture = CompletableFuture
					.supplyAsync(() -> container.receiveLater(new ChannelTopic(channel)).block(Duration.ofSeconds(5))
							.doOnNext(msg -> received.add(msg.getMessage())).subscribe());

			await().atMost(Duration.ofSeconds(5)).until(() -> localTargetSubscriberCount(container, channel) == 1L);
			allowCleanupSend.countDown();

			Disposable newSub = newSubFuture.get(5, TimeUnit.SECONDS);
			try {
				assertThat(localTargetSubscriberCount(container, channel)).isEqualTo(1L);
				assertThat(countRolledBackTargets(container)).isZero();
				assertThat(numSub(channel)).isEqualTo(1L);

				publish(channel, "live-after-race");
				assertThat(received.poll(5, TimeUnit.SECONDS)).isEqualTo("live-after-race");
			} finally {
				newSub.dispose();
				await().atMost(Duration.ofSeconds(5)).until(() -> numSub(channel) == 0L);
			}
		} finally {
			ReflectionTestUtils.setField(proxiedFactory, "reactiveConnectionProvider", realProvider);
			container.destroy();
		}
	}

	@Test // GH-3508
	void reactiveRegularUnsubscribePausedBeforeSendShouldNotUndoConcurrentResubscribe() throws Exception {

		String suffix = UUID.randomUUID().toString();
		String channel = "pubsub:reactive:reg-unsub-vs-sub:" + suffix;

		AtomicBoolean pauseNextUnsubscribe = new AtomicBoolean();
		CountDownLatch unsubPrepared = new CountDownLatch(1);
		CountDownLatch allowUnsubSend = new CountDownLatch(1);

		LettuceConnectionProvider realProvider = (LettuceConnectionProvider) ReflectionTestUtils.getField(proxiedFactory,
				"reactiveConnectionProvider");
		LettuceConnectionProvider interceptingProvider = createUnsubscribePausingProvider(realProvider,
				pauseNextUnsubscribe, unsubPrepared, allowUnsubSend);
		ReflectionTestUtils.setField(proxiedFactory, "reactiveConnectionProvider", interceptingProvider);

		ReactiveRedisMessageListenerContainer container = new ReactiveRedisMessageListenerContainer(proxiedFactory);
		try {
			Disposable subA = container.receiveLater(new ChannelTopic(channel)).block(Duration.ofSeconds(5)).subscribe();
			await().atMost(Duration.ofSeconds(5)).until(() -> numSub(channel) == 1L);

			pauseNextUnsubscribe.set(true);
			subA.dispose();
			assertThat(unsubPrepared.await(5, TimeUnit.SECONDS)).isTrue();

			BlockingQueue<String> received = new LinkedBlockingDeque<>();
			CompletableFuture<Disposable> subBFuture = CompletableFuture
					.supplyAsync(() -> container.receiveLater(new ChannelTopic(channel)).block(Duration.ofSeconds(5))
							.doOnNext(msg -> received.add(msg.getMessage())).subscribe());

			await().atMost(Duration.ofSeconds(5)).until(() -> localTargetSubscriberCount(container, channel) == 1L);
			allowUnsubSend.countDown();

			Disposable subB = subBFuture.get(5, TimeUnit.SECONDS);
			try {
				assertThat(localTargetSubscriberCount(container, channel)).isEqualTo(1L);
				assertThat(numSub(channel)).isEqualTo(1L);

				publish(channel, "for-subB-after-unsub");
				assertThat(received.poll(5, TimeUnit.SECONDS)).isEqualTo("for-subB-after-unsub");
			} finally {
				subB.dispose();
				await().atMost(Duration.ofSeconds(5)).until(() -> numSub(channel) == 0L);
			}
		} finally {
			ReflectionTestUtils.setField(proxiedFactory, "reactiveConnectionProvider", realProvider);
			container.destroy();
		}
	}

	@Test // GH-3508
	void pooledImperativeInitialSubscribeTimeoutShouldDiscardConnectionAndKeepPoolUsableWithZeroNumsub()
			throws Exception {

		String suffix = UUID.randomUUID().toString();
		String failedChannel = "pubsub:pool:imperative:failed:" + suffix;
		String healthyChannel = "pubsub:pool:imperative:healthy:" + suffix;

		LettuceConnectionFactory pooledFactory = createSingleSlotPooledFactory();
		try {
			RedisConnection connA = pooledFactory.getConnection();
			try {
				proxy.setPauseServerToClientOnSubscribe(true);
				assertThatExceptionOfType(QueryTimeoutException.class).isThrownBy(
						() -> connA.subscribe((msg, pat) -> {}, failedChannel.getBytes(StandardCharsets.UTF_8)));
				await().atMost(Duration.ofSeconds(5)).until(() -> numSub(failedChannel) == 0L);
			} finally {
				proxy.setPauseServerToClient(false);
				connA.close();
			}

			AtomicInteger staleNotifications = new AtomicInteger();
			CompletableFuture<Void> healthySubscribed = new CompletableFuture<>();
			BlockingQueue<String> healthyMessages = new LinkedBlockingDeque<>();

			interface CombinedListener extends MessageListener, SubscriptionListener {}
			CombinedListener listenerB = new CombinedListener() {
				@Override
				public void onMessage(org.springframework.data.redis.connection.Message message, byte[] pattern) {
					healthyMessages.add(new String(message.getBody(), StandardCharsets.UTF_8));
				}

				@Override
				public void onChannelSubscribed(byte[] ch, long count) {
					String name = new String(ch, StandardCharsets.UTF_8);
					if (failedChannel.equals(name)) {
						staleNotifications.incrementAndGet();
					} else if (healthyChannel.equals(name)) {
						healthySubscribed.complete(null);
					}
				}
			};

			RedisConnection connB = pooledFactory.getConnection();
			try {
				connB.subscribe(listenerB, healthyChannel.getBytes(StandardCharsets.UTF_8));
				healthySubscribed.get(5, TimeUnit.SECONDS);
				assertThat(staleNotifications).hasValue(0);
				assertThat(numSub(failedChannel)).isZero();
				assertThat(numSub(healthyChannel)).isEqualTo(1L);

				publish(healthyChannel, "clean-pool");
				assertThat(healthyMessages.poll(5, TimeUnit.SECONDS)).isEqualTo("clean-pool");
			} finally {
				if (connB.getSubscription() != null) {
					connB.getSubscription().close();
				}
				connB.close();
			}
		} finally {
			pooledFactory.destroy();
		}
	}

	@Test // GH-3508
	void pooledReactiveCloseLaterShouldCleanupActiveSubscriptionBeforeReborrow() throws Exception {

		String suffix = UUID.randomUUID().toString();
		String channelA = "pubsub:pool:reactive:a:" + suffix;
		String channelB = "pubsub:pool:reactive:b:" + suffix;

		LettuceConnectionFactory pooledFactory = createSingleSlotPooledFactory();
		try {
			AtomicInteger staleCallbacksOnA = new AtomicInteger();
			ReactiveRedisConnection connA = pooledFactory.getReactiveConnection();
			ReactiveSubscription subA = connA.pubSubCommands().createSubscription(new SubscriptionListener() {
				@Override
				public void onChannelSubscribed(byte[] channel, long count) {
					if (channelB.equals(new String(channel, StandardCharsets.UTF_8))) {
						staleCallbacksOnA.incrementAndGet();
					}
				}
			}).block(Duration.ofSeconds(5));

			subA.subscribe(ByteBuffer.wrap(channelA.getBytes(StandardCharsets.UTF_8))).block(Duration.ofSeconds(5));
			await().atMost(Duration.ofSeconds(5)).until(() -> numSub(channelA) == 1L);

			// Close reactive connection directly without subA.cancel(); must clean up channelA and detach subA's listener
			connA.closeLater().block(Duration.ofSeconds(5));
			await().atMost(Duration.ofSeconds(5)).until(() -> numSub(channelA) == 0L);

			ReactiveRedisConnection connB = pooledFactory.getReactiveConnection();
			try {
				ReactiveSubscription subB = connB.pubSubCommands()
						.createSubscription(SubscriptionListener.NO_OP_SUBSCRIPTION_LISTENER).block(Duration.ofSeconds(5));
				BlockingQueue<String> messagesB = new LinkedBlockingDeque<>();
				Disposable receiveB = subB.receive().doOnNext(
						msg -> messagesB.add(StandardCharsets.UTF_8.decode(msg.getMessage().duplicate()).toString()))
						.subscribe();

				subB.subscribe(ByteBuffer.wrap(channelB.getBytes(StandardCharsets.UTF_8))).block(Duration.ofSeconds(5));
				await().atMost(Duration.ofSeconds(5)).until(() -> numSub(channelB) == 1L);
				assertThat(numSub(channelA)).isZero();
				assertThat(staleCallbacksOnA).hasValue(0);

				publish(channelB, "clean-reactive-pool");
				assertThat(messagesB.poll(5, TimeUnit.SECONDS)).isEqualTo("clean-reactive-pool");
				receiveB.dispose();
			} finally {
				connB.closeLater().block(Duration.ofSeconds(5));
			}
		} finally {
			pooledFactory.destroy();
		}
	}

	private LettuceConnectionFactory createSingleSlotPooledFactory() {

		GenericObjectPoolConfig<StatefulConnection<?, ?>> poolConfig = new GenericObjectPoolConfig<>();
		poolConfig.setMaxTotal(1);
		poolConfig.setMaxIdle(1);

		LettucePoolingClientConfiguration clientConfig = LettucePoolingClientConfiguration.builder()
				.clientResources(LettuceTestClientResources.getSharedClientResources())
				.clientOptions(ClientOptions.builder().autoReconnect(true).build()) //
				.commandTimeout(COMMAND_TIMEOUT) //
				.shutdownTimeout(Duration.ZERO) //
				.shutdownQuietPeriod(Duration.ZERO) //
				.poolConfig(poolConfig) //
				.build();

		LettuceConnectionFactory factory = new LettuceConnectionFactory(
				new RedisStandaloneConfiguration("127.0.0.1", proxy.getPort()), clientConfig);
		factory.afterPropertiesSet();
		factory.start();
		return factory;
	}

	private RedisMessageListenerContainer createStartedContainer() {

		RedisMessageListenerContainer container = new RedisMessageListenerContainer();
		container.setConnectionFactory(proxiedFactory);
		container.setRecoveryInterval(50);
		container.setBeanName("resiliency-container");
		container.afterPropertiesSet();
		container.start();
		return container;
	}

	private long numSub(String channel) {
		return controlConnection.sync().pubsubNumsub(channel.getBytes(StandardCharsets.UTF_8)).values().stream()
				.findFirst().orElse(0L);
	}

	private void publish(String channel, String payload) {
		controlConnection.sync().publish(channel.getBytes(StandardCharsets.UTF_8), payload.getBytes(StandardCharsets.UTF_8));
	}

	@SuppressWarnings("unchecked")
	private int countWrapperListeners(ReactiveRedisMessageListenerContainer container) {

		ReactiveRedisConnection reactiveConnection = (ReactiveRedisConnection) ReflectionTestUtils.getField(container,
				"connection");
		if (reactiveConnection == null) {
			return 0;
		}
		Object asyncConnect = ReflectionTestUtils.getField(reactiveConnection, "pubSubConnection");
		StatefulRedisPubSubConnection<?, ?> pubSubConn = (StatefulRedisPubSubConnection<?, ?>) ReflectionTestUtils
				.getField(asyncConnect, "connection");
		if (pubSubConn == null) {
			return 0;
		}
		Object endpoint = ReflectionTestUtils.getField(pubSubConn, "endpoint");
		List<?> listeners = (List<?>) ReflectionTestUtils.getField(endpoint, "listeners");
		return (int) listeners.stream()
				.filter(l -> l.getClass().getSimpleName().equals("LettuceByteBufferPubSubListenerWrapper")).count();
	}

	private int countRolledBackTargets(ReactiveRedisMessageListenerContainer container) {

		ReactiveRedisConnection reactiveConnection = (ReactiveRedisConnection) ReflectionTestUtils.getField(container,
				"connection");
		if (reactiveConnection == null) {
			return 0;
		}
		Object pubSub = ReflectionTestUtils.getField(reactiveConnection, "pubSub");
		if (pubSub == null) {
			return 0;
		}
		Map<?, ?> activeChannelCleanups = (Map<?, ?>) ReflectionTestUtils.getField(pubSub, "activeChannelCleanups");
		Map<?, ?> activePatternCleanups = (Map<?, ?>) ReflectionTestUtils.getField(pubSub, "activePatternCleanups");
		return activeChannelCleanups.size() + activePatternCleanups.size();
	}

	private long localTargetSubscriberCount(ReactiveRedisMessageListenerContainer container, String channel) {

		ReactiveRedisConnection reactiveConnection = (ReactiveRedisConnection) ReflectionTestUtils.getField(container,
				"connection");
		Object pubSub = ReflectionTestUtils.getField(reactiveConnection, "pubSub");
		Map<?, ?> channels = (Map<?, ?>) ReflectionTestUtils.getField(pubSub, "channels");
		Object target = channels.get(new ByteArrayWrapper(channel.getBytes(StandardCharsets.UTF_8)));
		if (target == null) {
			return 0L;
		}
		return (long) ReflectionTestUtils.getField(target, "subscribers");
	}

	@SuppressWarnings("unchecked")
	private static LettuceConnectionProvider createUnsubscribePausingProvider(LettuceConnectionProvider delegate,
			AtomicBoolean pauseNextUnsubscribe, CountDownLatch cleanupPrepared, CountDownLatch allowCleanupSend) {

		AtomicReference<StatefulConnection<?, ?>> leasedReal = new AtomicReference<>();

		return new LettuceConnectionProvider() {

			@Override
			public <T extends StatefulConnection<?, ?>> T getConnection(Class<T> connectionType) {
				return delegate.getConnection(connectionType);
			}

			@Override
			public <T extends StatefulConnection<?, ?>> CompletionStage<T> getConnectionAsync(Class<T> connectionType) {
				return delegate.getConnectionAsync(connectionType).thenApply(conn -> {
					if (!StatefulRedisPubSubConnection.class.isAssignableFrom(connectionType)) {
						return conn;
					}
					StatefulRedisPubSubConnection<ByteBuffer, ByteBuffer> realConn = (StatefulRedisPubSubConnection<ByteBuffer, ByteBuffer>) conn;
					RedisPubSubReactiveCommands<ByteBuffer, ByteBuffer> realReactive = realConn.reactive();
					RedisPubSubReactiveCommands<ByteBuffer, ByteBuffer> wrappedReactive = mock(
							RedisPubSubReactiveCommands.class, delegatesTo(realReactive));
					doAnswer(inv -> {
						Mono<Void> realMono = realReactive.unsubscribe((ByteBuffer[]) inv.getRawArguments()[0]);
						if (!pauseNextUnsubscribe.compareAndSet(true, false)) {
							return realMono;
						}
						return Mono.<Void> defer(() -> {
							cleanupPrepared.countDown();
							try {
								assertThat(allowCleanupSend.await(5, TimeUnit.SECONDS)).isTrue();
							} catch (InterruptedException ex) {
								Thread.currentThread().interrupt();
								return Mono.error(ex);
							}
							return realMono;
						}).subscribeOn(Schedulers.boundedElastic());
					}).when(wrappedReactive).unsubscribe(any(ByteBuffer[].class));

					StatefulRedisPubSubConnection<ByteBuffer, ByteBuffer> wrappedConn = mock(
							StatefulRedisPubSubConnection.class, delegatesTo(realConn));
					doReturn(wrappedReactive).when(wrappedConn).reactive();
					leasedReal.set(realConn);
					return connectionType.cast(wrappedConn);
				});
			}

			@Override
			public void release(StatefulConnection<?, ?> connection) {
				StatefulConnection<?, ?> real = leasedReal.getAndSet(null);
				delegate.release(real != null ? real : connection);
			}

			@Override
			public CompletableFuture<Void> releaseAsync(StatefulConnection<?, ?> connection) {
				StatefulConnection<?, ?> real = leasedReal.getAndSet(null);
				return delegate.releaseAsync(real != null ? real : connection);
			}
		};
	}

	/**
	 * Minimal fault-injecting TCP proxy forwarding loopback traffic to Redis.
	 */
	static class RedisTcpProxy implements Closeable {

		private final String targetHost;
		private final int targetPort;
		private final ServerSocket serverSocket;
		private final ExecutorService executor = Executors.newCachedThreadPool();
		private final List<ProxySession> sessions = new CopyOnWriteArrayList<>();
		private final Object pauseMonitor = new Object();

		private volatile boolean closed;
		private volatile boolean pauseClientToServer;
		private volatile boolean pauseServerToClient;
		private volatile boolean pauseServerToClientOnSubscribe;

		RedisTcpProxy(String targetHost, int targetPort) throws IOException {
			this.targetHost = targetHost;
			this.targetPort = targetPort;
			this.serverSocket = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
		}

		int getPort() {
			return serverSocket.getLocalPort();
		}

		void start() {
			executor.execute(() -> {
				while (!closed) {
					try {
						Socket client = serverSocket.accept();
						Socket upstream = new Socket(targetHost, targetPort);
						ProxySession session = new ProxySession(client, upstream);
						sessions.add(session);
						session.start();
					} catch (IOException ex) {
						if (!closed) {
							break;
						}
					}
				}
			});
		}

		void setPauseClientToServer(boolean pauseClientToServer) {
			this.pauseClientToServer = pauseClientToServer;
			if (!pauseClientToServer) {
				synchronized (pauseMonitor) {
					pauseMonitor.notifyAll();
				}
			}
		}

		void setPauseServerToClient(boolean pauseServerToClient) {
			this.pauseServerToClient = pauseServerToClient;
			if (!pauseServerToClient) {
				synchronized (pauseMonitor) {
					pauseMonitor.notifyAll();
				}
			}
		}

		void setPauseServerToClientOnSubscribe(boolean pauseServerToClientOnSubscribe) {
			this.pauseServerToClientOnSubscribe = pauseServerToClientOnSubscribe;
		}

		void disconnectActiveConnections() {
			for (ProxySession session : sessions) {
				session.close();
			}
			sessions.clear();
		}

		@Override
		public void close() throws IOException {
			closed = true;
			pauseServerToClientOnSubscribe = false;
			setPauseClientToServer(false);
			setPauseServerToClient(false);
			disconnectActiveConnections();
			serverSocket.close();
			executor.shutdownNow();
		}

		private class ProxySession implements Closeable {

			private final Socket client;
			private final Socket upstream;
			private volatile boolean sessionClosed;

			ProxySession(Socket client, Socket upstream) {
				this.client = client;
				this.upstream = upstream;
			}

			void start() {
				executor.execute(() -> pump(client, upstream, true));
				executor.execute(() -> pump(upstream, client, false));
			}

			private void pump(Socket source, Socket dest, boolean clientToServer) {
				byte[] buffer = new byte[4096];
				try (InputStream in = source.getInputStream(); OutputStream out = dest.getOutputStream()) {
					int read;
					while (!closed && !sessionClosed && (read = in.read(buffer)) != -1) {
						while (!closed && !sessionClosed && (clientToServer ? pauseClientToServer : pauseServerToClient)) {
							synchronized (pauseMonitor) {
								if (!closed && !sessionClosed && (clientToServer ? pauseClientToServer : pauseServerToClient)) {
									pauseMonitor.wait(50);
								}
							}
						}
						if (closed || sessionClosed) {
							break;
						}
						if (clientToServer && pauseServerToClientOnSubscribe
								&& new String(buffer, 0, read, StandardCharsets.US_ASCII).contains("\r\nSUBSCRIBE\r\n")) {
							pauseServerToClientOnSubscribe = false;
							setPauseServerToClient(true);
						}
						out.write(buffer, 0, read);
						out.flush();
					}
				} catch (IOException | InterruptedException ignored) {
					Thread.currentThread().interrupt();
				} finally {
					close();
				}
			}

			@Override
			public void close() {
				sessionClosed = true;
				try {
					client.close();
				} catch (IOException ignored) {}
				try {
					upstream.close();
				} catch (IOException ignored) {}
			}
		}
	}
}
