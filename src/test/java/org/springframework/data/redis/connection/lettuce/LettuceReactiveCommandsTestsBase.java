/*
 * Copyright 2016-2018 the original author or authors.
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

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;
import io.lettuce.core.codec.StringCodec;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.springframework.data.redis.connection.lettuce.LettuceReactiveRedisConnection.ByteBufferCodec;
import org.springframework.data.redis.test.util.LettuceRedisClientProvider;
import org.springframework.data.redis.test.util.LettuceRedisClusterClientProvider;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@RunWith(Parameterized.class)
public abstract class LettuceReactiveCommandsTestsBase {

	static final String KEY_1 = "key-1";
	static final String KEY_2 = "key-2";
	static final String KEY_3 = "key-3";
	static final String SAME_SLOT_KEY_1 = "{key}-1";
	static final String SAME_SLOT_KEY_2 = "{key}-2";
	static final String SAME_SLOT_KEY_3 = "{key}-3";
	static final String VALUE_1 = "value-1";
	static final String VALUE_2 = "value-2";
	static final String VALUE_3 = "value-3";

	static final byte[] SAME_SLOT_KEY_1_BYTES = SAME_SLOT_KEY_1.getBytes(Charset.forName("UTF-8"));
	static final byte[] SAME_SLOT_KEY_2_BYTES = SAME_SLOT_KEY_2.getBytes(Charset.forName("UTF-8"));
	static final byte[] SAME_SLOT_KEY_3_BYTES = SAME_SLOT_KEY_3.getBytes(Charset.forName("UTF-8"));
	static final byte[] KEY_1_BYTES = KEY_1.getBytes(Charset.forName("UTF-8"));
	static final byte[] KEY_2_BYTES = KEY_2.getBytes(Charset.forName("UTF-8"));
	static final byte[] KEY_3_BYTES = KEY_3.getBytes(Charset.forName("UTF-8"));
	static final byte[] VALUE_1_BYTES = VALUE_1.getBytes(Charset.forName("UTF-8"));
	static final byte[] VALUE_2_BYTES = VALUE_2.getBytes(Charset.forName("UTF-8"));
	static final byte[] VALUE_3_BYTES = VALUE_3.getBytes(Charset.forName("UTF-8"));

	static final ByteBuffer KEY_1_BBUFFER = ByteBuffer.wrap(KEY_1_BYTES);
	static final ByteBuffer SAME_SLOT_KEY_1_BBUFFER = ByteBuffer.wrap(SAME_SLOT_KEY_1_BYTES);
	static final ByteBuffer VALUE_1_BBUFFER = ByteBuffer.wrap(VALUE_1_BYTES);

	static final ByteBuffer KEY_2_BBUFFER = ByteBuffer.wrap(KEY_2_BYTES);
	static final ByteBuffer SAME_SLOT_KEY_2_BBUFFER = ByteBuffer.wrap(SAME_SLOT_KEY_2_BYTES);
	static final ByteBuffer VALUE_2_BBUFFER = ByteBuffer.wrap(VALUE_2_BYTES);

	static final ByteBuffer KEY_3_BBUFFER = ByteBuffer.wrap(KEY_3_BYTES);
	static final ByteBuffer SAME_SLOT_KEY_3_BBUFFER = ByteBuffer.wrap(SAME_SLOT_KEY_3_BYTES);
	static final ByteBuffer VALUE_3_BBUFFER = ByteBuffer.wrap(VALUE_3_BYTES);

	@Parameterized.Parameter(value = 0) public LettuceConnectionProvider connectionProvider;
	@Parameterized.Parameter(value = 1) public LettuceConnectionProvider nativeConnectionProvider;
	@Parameterized.Parameter(value = 2) public LettuceConnectionProvider nativeBinaryConnectionProvider;
	@Parameterized.Parameter(value = 3) public Object displayName;

	LettuceReactiveRedisConnection connection;
	RedisClusterCommands<String, String> nativeCommands;
	RedisClusterCommands<ByteBuffer, ByteBuffer> nativeBinaryCommands;

	@Parameterized.Parameters(name = "{2}")
	public static List<Object[]> parameters() {

		LettuceRedisClientProvider standalone = LettuceRedisClientProvider.local();
		LettuceRedisClusterClientProvider cluster = LettuceRedisClusterClientProvider.local();

		List<Object[]> parameters = new ArrayList<>();

		StandaloneConnectionProvider standaloneProvider = new StandaloneConnectionProvider(standalone.getClient(),
				LettuceReactiveRedisConnection.CODEC);
		StandaloneConnectionProvider nativeConnectionProvider = new StandaloneConnectionProvider(standalone.getClient(),
				StringCodec.UTF8);
		StandaloneConnectionProvider nativeBinaryConnectionProvider = new StandaloneConnectionProvider(
				standalone.getClient(), ByteBufferCodec.INSTANCE);

		parameters.add(
				new Object[] { standaloneProvider, nativeConnectionProvider, nativeBinaryConnectionProvider, "Standalone" });
		parameters.add(new Object[] {
				new LettucePoolingConnectionProvider(standaloneProvider, LettucePoolingClientConfiguration.builder().build()),
				nativeConnectionProvider, nativeBinaryConnectionProvider, "Standalone/Pooled" });

		if (cluster.test()) {

			ClusterConnectionProvider clusterProvider = new ClusterConnectionProvider(cluster.getClient(),
					LettuceReactiveRedisConnection.CODEC);
			ClusterConnectionProvider nativeClusterConnectionProvider = new ClusterConnectionProvider(cluster.getClient(),
					StringCodec.UTF8);
			ClusterConnectionProvider nativeBinaryClusterConnectionProvider = new ClusterConnectionProvider(
					cluster.getClient(), ByteBufferCodec.INSTANCE);

			parameters.add(new Object[] { clusterProvider, nativeClusterConnectionProvider,
					nativeBinaryClusterConnectionProvider, "Cluster" });
		}

		return parameters;
	}

	@Before
	public void setUp() {

		if (nativeConnectionProvider instanceof StandaloneConnectionProvider) {
			nativeCommands = nativeConnectionProvider.getConnection(StatefulRedisConnection.class).sync();
			nativeBinaryCommands = nativeBinaryConnectionProvider.getConnection(StatefulRedisConnection.class).sync();
			this.connection = new LettuceReactiveRedisConnection(connectionProvider);

		} else {
			ClusterConnectionProvider clusterConnectionProvider = (ClusterConnectionProvider) nativeConnectionProvider;
			nativeCommands = nativeConnectionProvider.getConnection(StatefulRedisClusterConnection.class).sync();
			nativeBinaryCommands = nativeBinaryConnectionProvider.getConnection(StatefulRedisClusterConnection.class).sync();
			this.connection = new LettuceReactiveRedisClusterConnection(connectionProvider,
					clusterConnectionProvider.getRedisClient());
		}
	}

	@After
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
