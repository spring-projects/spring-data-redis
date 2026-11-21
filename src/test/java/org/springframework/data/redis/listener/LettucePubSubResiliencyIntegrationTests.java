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

import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import reactor.core.Disposable;

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
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.dao.QueryTimeoutException;
import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.ReactiveRedisConnection;
import org.springframework.data.redis.connection.ReactiveSubscription;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.SubscriptionListener;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
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

		void disconnectActiveConnections() {
			for (ProxySession session : sessions) {
				session.close();
			}
			sessions.clear();
		}

		@Override
		public void close() throws IOException {
			closed = true;
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
