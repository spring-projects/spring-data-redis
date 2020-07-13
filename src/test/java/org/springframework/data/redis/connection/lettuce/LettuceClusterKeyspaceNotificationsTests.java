/*
 * Copyright 2019-2020 the original author or authors.
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
package org.springframework.data.redis.connection.lettuce;

import static org.assertj.core.api.Assertions.*;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.cluster.SlotHash;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import org.springframework.data.redis.ConnectionFactoryTracker;
import org.springframework.data.redis.connection.ClusterCommandExecutor;
import org.springframework.data.redis.connection.ClusterTopologyProvider;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.RedisClusterConnection;
import org.springframework.data.redis.connection.RedisConfiguration;
import org.springframework.data.redis.test.util.RedisClusterRule;
import org.springframework.lang.Nullable;

/**
 * Integration tests to listen for keyspace notifications.
 *
 * @author Mark Paluch
 */
public class LettuceClusterKeyspaceNotificationsTests {

	@ClassRule public static RedisClusterRule clusterRule = new RedisClusterRule();

	CustomLettuceConnectionFactory factory;
	String keyspaceConfig;

	// maps to 127.0.0.1:7381/slot hash 13477
	String key = "10923";

	@Before
	public void before() {

		factory = new CustomLettuceConnectionFactory(clusterRule.getConfiguration());
		factory.setClientResources(LettuceTestClientResources.getSharedClientResources());
		ConnectionFactoryTracker.add(factory);
		factory.afterPropertiesSet();

		// enable keyspace events on a specific node.
		withConnection("127.0.0.1", 7381, commands -> {

			keyspaceConfig = commands.configGet("*").get("notify-keyspace-events");
			commands.configSet("notify-keyspace-events", "KEx");
		});

		assertThat(SlotHash.getSlot(key)).isEqualTo(13477);
	}

	@After
	public void tearDown() {

		// Restore previous settings.
		withConnection("127.0.0.1", 7381, commands -> {
			commands.configSet("notify-keyspace-events", keyspaceConfig);
		});
	}

	@Test // DATAREDIS-976
	public void shouldListenForKeyspaceNotifications() throws Exception {

		CompletableFuture<String> expiry = new CompletableFuture<>();

		RedisClusterConnection connection = factory.getClusterConnection();

		connection.pSubscribe((message, pattern) -> {
			expiry.complete(new String(message.getBody()) + ":" + new String(message.getChannel()));
		}, "__keyspace*@*".getBytes());

		withConnection("127.0.0.1", 7381, commands -> {
			commands.set(key, "foo", SetArgs.Builder.px(1));
		});

		assertThat(expiry.get(2, TimeUnit.SECONDS)).isEqualTo("expired:__keyspace@0__:10923");

		connection.getSubscription().close();
		connection.close();
	}

	private void withConnection(String hostname, int port, Consumer<RedisCommands<String, String>> commandsConsumer) {

		RedisClient client = RedisClient.create(LettuceTestClientResources.getSharedClientResources(),
				RedisURI.create(hostname, port));

		StatefulRedisConnection<String, String> connection = client.connect();
		commandsConsumer.accept(connection.sync());

		connection.close();
		client.shutdownAsync();
	}

	static class CustomLettuceConnectionFactory extends LettuceConnectionFactory {

		CustomLettuceConnectionFactory(RedisConfiguration redisConfiguration) {
			super(redisConfiguration);
		}

		@Override
		protected LettuceClusterConnection doCreateLettuceClusterConnection(
				StatefulRedisClusterConnection<byte[], byte[]> sharedConnection, LettuceConnectionProvider connectionProvider,
				ClusterTopologyProvider topologyProvider, ClusterCommandExecutor clusterCommandExecutor,
				Duration commandTimeout) {
			return new CustomLettuceClusterConnection(sharedConnection, connectionProvider, topologyProvider,
					clusterCommandExecutor, commandTimeout);
		}
	}

	static class CustomLettuceClusterConnection extends LettuceClusterConnection {

		CustomLettuceClusterConnection(@Nullable StatefulRedisClusterConnection<byte[], byte[]> sharedConnection,
				LettuceConnectionProvider connectionProvider, ClusterTopologyProvider clusterTopologyProvider,
				ClusterCommandExecutor executor, Duration timeout) {
			super(sharedConnection, connectionProvider, clusterTopologyProvider, executor, timeout);
		}

		@Override
		protected LettuceSubscription doCreateSubscription(MessageListener listener,
				StatefulRedisPubSubConnection<byte[], byte[]> connection, LettuceConnectionProvider connectionProvider) {
			return new CustomLettuceSubscription(listener, (StatefulRedisClusterPubSubConnection<byte[], byte[]>) connection,
					connectionProvider);
		}
	}

	/**
	 * Customized {@link LettuceSubscription}. Enables
	 * {@link StatefulRedisClusterPubSubConnection#setNodeMessagePropagation(boolean)} and uses
	 * {@link io.lettuce.core.cluster.api.sync.NodeSelection} to subscribe to all master nodes.
	 */
	static class CustomLettuceSubscription extends LettuceSubscription {

		private final StatefulRedisClusterPubSubConnection<byte[], byte[]> connection;

		CustomLettuceSubscription(MessageListener listener, StatefulRedisClusterPubSubConnection<byte[], byte[]> connection,
				LettuceConnectionProvider connectionProvider) {
			super(listener, connection, connectionProvider);
			this.connection = connection;

			// Must be enabled for keyspace notification propagation
			this.connection.setNodeMessagePropagation(true);
		}

		@Override
		protected void doPsubscribe(byte[]... patterns) {
			connection.sync().all().commands().psubscribe(patterns);
		}

		@Override
		protected void doPUnsubscribe(boolean all, byte[]... patterns) {
			connection.sync().all().commands().punsubscribe();
		}

		@Override
		protected void doSubscribe(byte[]... channels) {
			connection.sync().all().commands().subscribe(channels);
		}

		@Override
		protected void doUnsubscribe(boolean all, byte[]... channels) {
			connection.sync().all().commands().unsubscribe();
		}
	}
}
