/*
 * Copyright 2026 the original author or authors.
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

import static java.nio.charset.StandardCharsets.*;
import static org.assertj.core.api.Assertions.*;

import redis.clients.jedis.JedisClientConfig;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.RedisProtocol;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.util.RedisInputStream;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisClusterConfiguration;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisSentinelConfiguration;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.test.util.ReflectionTestUtils;

/**
 * Protocol compatibility tests for {@link JedisConnectionFactory}.
 *
 * @author Shuen
 */
class JedisConnectionFactoryProtocolUnitTests {

	@ParameterizedTest // GH-3511
	@ValueSource(booleans = { true, false })
	void defaultResp2DoesNotRequireHello(boolean unified) throws Exception {

		try (HelloRejectingServer server = new HelloRejectingServer()) {
			assertThat(ping(server, configuration().build(), unified, "test-password")).isEqualTo("PONG");
			assertThat(server.commands).contains("AUTH", "PING").doesNotContain("HELLO 2", "HELLO 3");
		}
	}

	@Test // GH-3511
	void springResp2SelectionDoesNotRequireHello() throws Exception {

		try (HelloRejectingServer server = new HelloRejectingServer()) {
			JedisClientConfiguration configuration = configuration().protocol(RedisProtocol.RESP2).build();

			assertThat(ping(server, configuration, true, "test-password")).isEqualTo("PONG");
			assertThat(server.commands).contains("AUTH", "PING").doesNotContain("HELLO 2", "HELLO 3");
		}
	}

	@Test // GH-3511
	void unsupportedSpringResp3SelectionDoesNotFallBack() throws Exception {

		try (HelloRejectingServer server = new HelloRejectingServer()) {
			JedisClientConfiguration configuration = configuration().protocol(RedisProtocol.RESP3).build();

			assertThatThrownBy(() -> ping(server, configuration, true, "test-password"))
					.isInstanceOf(DataAccessException.class).hasMessageContaining("Server does not support HELLO");
			assertThat(server.commands).contains("HELLO 3").doesNotContain("AUTH", "PING");
		}
	}

	@ParameterizedTest // GH-3511
	@ValueSource(strings = { "standalone", "cluster", "sentinel" })
	void springProtocolSelectionAppliesToAllConfigurations(String mode) {

		JedisClientConfiguration configuration = configuration().protocol(RedisProtocol.RESP3).build();
		JedisConnectionFactory factory = switch (mode) {
			case "cluster" ->
				new JedisConnectionFactory(new RedisClusterConfiguration(List.of("127.0.0.1:6379")), configuration);
			case "sentinel" -> new JedisConnectionFactory(
					new RedisSentinelConfiguration().master("mymaster").sentinel("127.0.0.1", 26379), configuration);
			default -> new JedisConnectionFactory(new RedisStandaloneConfiguration(), configuration);
		};
		factory.setEarlyStartup(false);
		try {
			factory.afterPropertiesSet();
			JedisClientConfig effective = (JedisClientConfig) ReflectionTestUtils.getField(factory, "clientConfig");
			assertThat(effective.getRedisProtocol()).isEqualTo(RedisProtocol.RESP3);
			if (mode.equals("sentinel")) {
				assertThat(factory.createSentinelClientConfig(factory.getSentinelConfiguration()).getRedisProtocol())
						.isEqualTo(RedisProtocol.RESP3);
			}
		} finally {
			factory.destroy();
		}
	}

	@Test // GH-3511
	void reinitializationDoesNotReusePreviousCustomizedProtocol() {

		AtomicBoolean first = new AtomicBoolean(true);
		JedisClientConfiguration configuration = configuration().customize(builder -> {
			if (first.getAndSet(false)) {
				builder.protocol(RedisProtocol.RESP3);
			}
		}).build();
		JedisConnectionFactory factory = new JedisConnectionFactory(new RedisStandaloneConfiguration(), configuration);
		factory.setEarlyStartup(false);
		try {
			factory.afterPropertiesSet();
			JedisClientConfig initial = (JedisClientConfig) ReflectionTestUtils.getField(factory, "clientConfig");
			assertThat(initial.getRedisProtocol()).isEqualTo(RedisProtocol.RESP3);
			factory.afterPropertiesSet();
			JedisClientConfig reinitialized = (JedisClientConfig) ReflectionTestUtils.getField(factory, "clientConfig");
			assertThat(reinitialized.getRedisProtocol()).isNull();
			assertThat(reinitialized.isAutoNegotiateProtocol()).isFalse();
		} finally {
			factory.destroy();
		}
	}

	@Test // GH-3511
	void customizerOverridesSpringProtocolSelection() throws Exception {

		try (HelloRejectingServer server = new HelloRejectingServer()) {
			JedisClientConfiguration configuration = configuration().protocol(RedisProtocol.RESP3)
					.customize(builder -> builder.protocol(RedisProtocol.RESP2)).build();

			assertThatThrownBy(() -> ping(server, configuration, true, "test-password"))
					.isInstanceOf(DataAccessException.class).hasMessageContaining("Server does not support HELLO");
			assertThat(server.commands).contains("HELLO 2").doesNotContain("HELLO 3", "AUTH", "PING");
		}
	}

	@Test // GH-3511
	void explicitResp2CustomizerStillRequiresHello() throws Exception {

		try (HelloRejectingServer server = new HelloRejectingServer()) {
			JedisClientConfiguration configuration = configuration()
					.customize(builder -> builder.protocol(RedisProtocol.RESP2)).build();

			assertThatThrownBy(() -> ping(server, configuration, true, "test-password"))
					.isInstanceOf(DataAccessException.class).hasMessageContaining("Server does not support HELLO");
			assertThat(server.commands).contains("HELLO 2").doesNotContain("AUTH", "PING");
		}
	}

	@Test // GH-3511
	void unsupportedExplicitResp3DoesNotFallBack() throws Exception {

		try (HelloRejectingServer server = new HelloRejectingServer()) {
			JedisClientConfiguration configuration = configuration()
					.customize(builder -> builder.protocol(RedisProtocol.RESP3)).build();

			assertThatThrownBy(() -> ping(server, configuration, true, "test-password"))
					.isInstanceOf(DataAccessException.class).hasMessageContaining("Server does not support HELLO");
			assertThat(server.commands).contains("HELLO 3").doesNotContain("AUTH", "PING");
		}
	}

	@Test // GH-3511
	void authenticationFailuresAreNotSuppressed() throws Exception {

		try (HelloRejectingServer server = new HelloRejectingServer()) {
			assertThatThrownBy(() -> ping(server, configuration().build(), true, "incorrect-password"))
					.isInstanceOf(DataAccessException.class).hasMessageContaining("WRONGPASS");
			assertThat(server.commands).contains("AUTH").doesNotContain("PING");
		}
	}

	@Test // GH-3511
	void customizerCanEnableAutomaticNegotiation() throws Exception {

		try (HelloRejectingServer server = new HelloRejectingServer()) {
			JedisClientConfiguration configuration = configuration()
					.customize(builder -> builder.protocol(null).autoNegotiateProtocol(true)).build();

			assertThat(ping(server, configuration, true, "test-password")).isEqualTo("PONG");
			assertThat(server.commands).containsSubsequence("HELLO 3", "AUTH", "HELLO 2", "PING");
		}
	}

	private static JedisClientConfiguration.JedisClientConfigurationBuilder configuration() {
		return JedisClientConfiguration.builder().connectTimeout(Duration.ofSeconds(3)).readTimeout(Duration.ofSeconds(3));
	}

	private static String ping(HelloRejectingServer server, JedisClientConfiguration configuration, boolean unified,
			String password) {

		RedisStandaloneConfiguration endpoint = new RedisStandaloneConfiguration("127.0.0.1",
				server.listener.getLocalPort());
		endpoint.setPassword(password);
		JedisConnectionFactory factory = new JedisConnectionFactory(endpoint, configuration);
		factory.setUseUnifiedJedis(unified);
		try {
			factory.afterPropertiesSet();
			try (RedisConnection connection = factory.getConnection()) {
				return connection.ping();
			}
		} finally {
			factory.destroy();
		}
	}

	/** RESP2 endpoint that authenticates clients but does not implement HELLO. */
	private static class HelloRejectingServer implements AutoCloseable {

		private final ServerSocket listener = new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"));
		private final ExecutorService executor = Executors.newSingleThreadExecutor();
		private final Future<?> task;
		private final List<String> commands = new CopyOnWriteArrayList<>();
		private volatile @Nullable Socket client;

		HelloRejectingServer() throws IOException {
			task = executor.submit(this::serve);
		}

		private void serve() {

			while (!listener.isClosed()) {
				try (Socket socket = listener.accept()) {
					client = socket;
					socket.setSoTimeout(5000);
					RedisInputStream input = new RedisInputStream(socket.getInputStream());
					boolean authenticated = false;
					while (!listener.isClosed()) {
						List<?> arguments;
						try {
							arguments = (List<?>) Protocol.read(input);
						} catch (JedisConnectionException disconnected) {
							break;
						}
						String command = new String((byte[]) arguments.get(0), UTF_8);
						commands.add(command.equals("HELLO") ? "HELLO " + new String((byte[]) arguments.get(1), UTF_8) : command);
						String response;
						if (command.equals("HELLO")) {
							response = "-ERR unknown command\r\n";
						} else if (command.equals("AUTH")) {
							authenticated = new String((byte[]) arguments.get(arguments.size() - 1), UTF_8).equals("test-password");
							response = authenticated ? "+OK\r\n" : "-WRONGPASS invalid username-password pair\r\n";
						} else if (!authenticated) {
							response = "-NOAUTH Authentication required\r\n";
						} else if (command.equals("PING")) {
							response = "+PONG\r\n";
						} else if (command.equals("CLIENT")) {
							response = "+OK\r\n";
						} else {
							throw new IllegalStateException("Unexpected command: " + command);
						}
						socket.getOutputStream().write(response.getBytes(UTF_8));
						socket.getOutputStream().flush();
					}
				} catch (IOException ex) {
					if (!listener.isClosed()) {
						throw new UncheckedIOException(ex);
					}
				}
			}
		}

		@Override
		public void close() throws Exception {

			try {
				listener.close();
				Socket socket = client;
				if (socket != null) {
					socket.close();
				}
				task.get(5, TimeUnit.SECONDS);
			} finally {
				executor.shutdownNow();
			}
		}
	}
}
