/*
 * Copyright 2016-2023 the original author or authors.
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

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;
import io.lettuce.core.codec.StringCodec;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.assertj.core.api.Assumptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.data.redis.connection.lettuce.LettuceReactiveRedisConnection.ByteBufferCodec;
import org.springframework.data.redis.test.condition.RedisDetector;
import org.springframework.data.redis.test.extension.LettuceExtension;
import org.springframework.data.redis.test.extension.parametrized.MethodSource;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@MethodSource("parameters")
public abstract class LettuceReactiveCommandsTestSupport {

	static final String KEY_1 = "key-1";
	static final String KEY_2 = "key-2";
	static final String KEY_3 = "key-3";
	static final String SAME_SLOT_KEY_1 = "{key}-1";
	static final String SAME_SLOT_KEY_2 = "{key}-2";
	static final String SAME_SLOT_KEY_3 = "{key}-3";
	static final String VALUE_1 = "value-1";
	static final String VALUE_2 = "value-2";
	static final String VALUE_3 = "value-3";

	static final byte[] SAME_SLOT_KEY_1_BYTES = SAME_SLOT_KEY_1.getBytes(StandardCharsets.UTF_8);
	static final byte[] SAME_SLOT_KEY_2_BYTES = SAME_SLOT_KEY_2.getBytes(StandardCharsets.UTF_8);
	static final byte[] SAME_SLOT_KEY_3_BYTES = SAME_SLOT_KEY_3.getBytes(StandardCharsets.UTF_8);
	static final byte[] KEY_1_BYTES = KEY_1.getBytes(StandardCharsets.UTF_8);
	static final byte[] KEY_2_BYTES = KEY_2.getBytes(StandardCharsets.UTF_8);
	static final byte[] KEY_3_BYTES = KEY_3.getBytes(StandardCharsets.UTF_8);
	static final byte[] VALUE_1_BYTES = VALUE_1.getBytes(StandardCharsets.UTF_8);
	static final byte[] VALUE_2_BYTES = VALUE_2.getBytes(StandardCharsets.UTF_8);
	static final byte[] VALUE_3_BYTES = VALUE_3.getBytes(StandardCharsets.UTF_8);

	static final ByteBuffer KEY_1_BBUFFER = ByteBuffer.wrap(KEY_1_BYTES);
	static final ByteBuffer SAME_SLOT_KEY_1_BBUFFER = ByteBuffer.wrap(SAME_SLOT_KEY_1_BYTES);
	static final ByteBuffer VALUE_1_BBUFFER = ByteBuffer.wrap(VALUE_1_BYTES);

	static final ByteBuffer KEY_2_BBUFFER = ByteBuffer.wrap(KEY_2_BYTES);
	static final ByteBuffer SAME_SLOT_KEY_2_BBUFFER = ByteBuffer.wrap(SAME_SLOT_KEY_2_BYTES);
	static final ByteBuffer VALUE_2_BBUFFER = ByteBuffer.wrap(VALUE_2_BYTES);

	static final ByteBuffer KEY_3_BBUFFER = ByteBuffer.wrap(KEY_3_BYTES);
	static final ByteBuffer SAME_SLOT_KEY_3_BBUFFER = ByteBuffer.wrap(SAME_SLOT_KEY_3_BYTES);
	static final ByteBuffer VALUE_3_BBUFFER = ByteBuffer.wrap(VALUE_3_BYTES);

	public final LettuceConnectionProvider connectionProvider;
	public final LettuceConnectionProvider nativeConnectionProvider;
	public final LettuceConnectionProvider nativeBinaryConnectionProvider;

	LettuceReactiveRedisConnection connection;
	RedisClusterCommands<String, String> nativeCommands;
	RedisClusterCommands<ByteBuffer, ByteBuffer> nativeBinaryCommands;

	public LettuceReactiveCommandsTestSupport(Fixture fixture) {
		this.connectionProvider = fixture.connectionProvider;
		this.nativeConnectionProvider = fixture.nativeConnectionProvider;
		this.nativeBinaryConnectionProvider = fixture.nativeBinaryConnectionProvider;
	}

	public static List<Fixture> parameters() {

		LettuceExtension extension = new LettuceExtension();

		List<Fixture> parameters = new ArrayList<>();

		StandaloneConnectionProvider standaloneProvider = new StandaloneConnectionProvider(
				extension.getInstance(RedisClient.class),
				LettuceReactiveRedisConnection.CODEC);
		StandaloneConnectionProvider nativeConnectionProvider = new StandaloneConnectionProvider(
				extension.getInstance(RedisClient.class),
				StringCodec.UTF8);
		StandaloneConnectionProvider nativeBinaryConnectionProvider = new StandaloneConnectionProvider(
				extension.getInstance(RedisClient.class), ByteBufferCodec.INSTANCE);

		parameters.add(
				new Fixture(standaloneProvider, nativeConnectionProvider, nativeBinaryConnectionProvider, "Standalone"));
		parameters.add(new Fixture(
				new LettucePoolingConnectionProvider(standaloneProvider, LettucePoolingClientConfiguration.builder().build()),
				nativeConnectionProvider, nativeBinaryConnectionProvider, "Pooling"));


		ClusterConnectionProvider clusterProvider = new ClusterConnectionProvider(
				extension.getInstance(RedisClusterClient.class),
					LettuceReactiveRedisConnection.CODEC);
		ClusterConnectionProvider nativeClusterConnectionProvider = new ClusterConnectionProvider(
				extension.getInstance(RedisClusterClient.class),
					StringCodec.UTF8);
			ClusterConnectionProvider nativeBinaryClusterConnectionProvider = new ClusterConnectionProvider(
					extension.getInstance(RedisClusterClient.class), ByteBufferCodec.INSTANCE);

			parameters.add(new Fixture(clusterProvider, nativeClusterConnectionProvider,
					nativeBinaryClusterConnectionProvider, "Cluster"));

		return parameters;
	}

	static class Fixture {

		final LettuceConnectionProvider connectionProvider;
		final LettuceConnectionProvider nativeConnectionProvider;
		final LettuceConnectionProvider nativeBinaryConnectionProvider;
		final String label;

		Fixture(LettuceConnectionProvider connectionProvider, LettuceConnectionProvider nativeConnectionProvider,
				LettuceConnectionProvider nativeBinaryConnectionProvider, String label) {
			this.connectionProvider = connectionProvider;
			this.nativeConnectionProvider = nativeConnectionProvider;
			this.nativeBinaryConnectionProvider = nativeBinaryConnectionProvider;
			this.label = label;
		}

		@Override
		public String toString() {
			return label;
		}
	}

	@BeforeEach
	public void setUp() {

		if (nativeConnectionProvider instanceof StandaloneConnectionProvider) {

			nativeCommands = nativeConnectionProvider.getConnection(StatefulRedisConnection.class).sync();
			nativeBinaryCommands = nativeBinaryConnectionProvider.getConnection(StatefulRedisConnection.class).sync();
			this.connection = new LettuceReactiveRedisConnection(connectionProvider);
		} else {

			Assumptions.assumeThat(RedisDetector.isClusterAvailable()).isTrue();

			ClusterConnectionProvider clusterConnectionProvider = (ClusterConnectionProvider) nativeConnectionProvider;
			nativeCommands = nativeConnectionProvider.getConnection(StatefulRedisClusterConnection.class).sync();
			nativeBinaryCommands = nativeBinaryConnectionProvider.getConnection(StatefulRedisClusterConnection.class).sync();
			this.connection = new LettuceReactiveRedisClusterConnection(connectionProvider,
					clusterConnectionProvider.getRedisClient());
		}
	}

	@AfterEach
	public void tearDown() {

		if (nativeCommands != null) {
			flushAll();

			if (nativeCommands instanceof RedisCommands) {
				nativeConnectionProvider.release(((RedisCommands) nativeCommands).getStatefulConnection());
			}

			if (nativeCommands instanceof RedisAdvancedClusterCommands) {
				nativeConnectionProvider.release(((RedisAdvancedClusterCommands) nativeCommands).getStatefulConnection());
			}
		}

		if (connection != null) {
			connection.close();
		}
	}

	private void flushAll() {
		nativeCommands.flushall();
	}

}
