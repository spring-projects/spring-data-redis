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
package org.springframework.data.redis.connection.jedis;

import static org.assertj.core.api.Assertions.*;

import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

import redis.clients.jedis.UnifiedJedis;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

/**
 * Integration test of {@link JedisConnection} using {@link redis.clients.jedis.UnifiedJedis}.
 *
 * @author Tihomir Mateev
 * @author Mark Paluch
 * @author Tiefang Hu
 * @author Moritz Halbritter
 */
@ExtendWith(SpringExtension.class)
@ContextConfiguration(inheritLocations = false)
public class UnifiedJedisConnectionIntegrationTests extends JedisConnectionIntegrationTests {

	@Test
	@Override
	@Disabled("MOVE test requires the use of SELECT")
	public void testMove() {}

	@Test
	@Override
	@Disabled("SELECT not supported by UnifiedJedis")
	public void testSelect() {
		super.testSelect();
	}

	@Test
	@Override
	@Disabled("setClientName not supported by UnifiedJedis")
	void shouldSetClientName() {
		super.shouldSetClientName();
	}

	@Test
	@Override
	void testNativeConnectionIsJedis() {
		assertThat(byteConnection.getNativeConnection()).isInstanceOf(UnifiedJedis.class);
	}

	@Test // GH-3392
	void readBetweenWatchAndMultiShouldNotBeIncludedInExecResults() {

		connection.set("watched-key", "initial");
		connection.watch("watched-key".getBytes());
		assertThat(connection.isQueueing()).isFalse();

		String watchedValue = connection.get("watched-key");

		connection.multi();
		assertThat(connection.isQueueing()).isTrue();
		connection.set("watched-key", "updated");
		connection.set("other-key", "value");

		List<Object> results = connection.exec();

		assertThat(watchedValue).isEqualTo("initial");
		assertThat(results).containsExactly(true, true);
		assertThat(connection.isQueueing()).isFalse();
	}

	@Test // GH-3392
	void readBetweenWatchAndMultiShouldKeepWatchingTheKey() {

		connection.set("watched-key", "initial");
		connection.watch("watched-key".getBytes());

		// the read runs on the connection pinned by WATCH and must not drop its key registration
		assertThat(connection.get("watched-key")).isEqualTo("initial");

		connection.multi();
		connection.set("watched-key", "updated");

		RedisConnection other = connectionFactory.getConnection();

		try {
			other.stringCommands().set("watched-key".getBytes(), "modified-elsewhere".getBytes());
		} finally {
			other.close();
		}

		assertThat(connection.exec()).isNull();
		assertThat(connection.get("watched-key")).isEqualTo("modified-elsewhere");
	}

	@Test // GH-3392
	void writeBetweenWatchAndMultiShouldBeAppliedImmediately() {

		connection.set("watched-key", "initial");
		connection.watch("watched-key".getBytes());

		// SET is a status command, it must be applied right away and must not be registered as a transaction result
		connection.set("other-key", "from-watch-only");

		RedisConnection other = connectionFactory.getConnection();

		try {
			assertThat(other.stringCommands().get("other-key".getBytes())).isEqualTo("from-watch-only".getBytes());
		} finally {
			other.close();
		}

		connection.multi();
		connection.set("watched-key", "updated");

		assertThat(connection.exec()).containsExactly(true);
		assertThat(connection.get("watched-key")).isEqualTo("updated");
		assertThat(connection.get("other-key")).isEqualTo("from-watch-only");
	}

	@Test // GH-3392
	void unwatchBetweenWatchAndMultiShouldReleaseTheWatchedConnection() {

		connection.set("watched-key", "initial");
		connection.watch("watched-key".getBytes());

		assertThat(((JedisConnection) byteConnection).isWatchOnly()).isTrue();
		assertThat(connection.get("watched-key")).isEqualTo("initial");

		connection.unwatch();

		assertThat(((JedisConnection) byteConnection).isWatchOnly()).isFalse();
		assertThat(connection.isQueueing()).isFalse();
		assertThat(connection.get("watched-key")).isEqualTo("initial");

		// modifying the previously watched key must no longer abort the transaction
		RedisConnection other = connectionFactory.getConnection();

		try {
			other.stringCommands().set("watched-key".getBytes(), "modified-elsewhere".getBytes());
		} finally {
			other.close();
		}

		connection.multi();
		connection.set("watched-key", "updated");

		assertThat(connection.exec()).containsExactly(true);
		assertThat(connection.get("watched-key")).isEqualTo("updated");
	}

	@Test // GH-3392
	void rawCommandBetweenWatchAndMultiShouldUseTheWatchedConnection() {

		byte[] key = "watched-key".getBytes();
		connection.set("watched-key", "initial");
		connection.watch(key);

		Object watchedValue = byteConnection.execute("GET", key);

		connection.multi();
		connection.set("watched-key", "updated");

		List<Object> results = connection.exec();

		assertThat((byte[]) watchedValue).isEqualTo("initial".getBytes());
		assertThat(results).containsExactly(true);
	}

	@Test // GH-3392
	void watchShouldBeRejectedWhilePipelined() {

		byteConnection.openPipeline();

		assertThatExceptionOfType(InvalidDataAccessApiUsageException.class)
				.isThrownBy(() -> connection.watch("watched-key".getBytes()));
	}

	@ParameterizedTest(name = "{0}") // GH-3392
	@MethodSource("scanOperations")
	void scanShouldBeRejectedBetweenWatchAndMulti(String command, Consumer<RedisConnection> scanOperation) {

		connection.watch("watched-key".getBytes());

		assertThatExceptionOfType(InvalidDataAccessApiUsageException.class)
				.isThrownBy(() -> scanOperation.accept(byteConnection));
	}

	static Stream<Arguments> scanOperations() {

		byte[] key = "watched-key".getBytes();

		return Stream.of(Arguments.of("SCAN", (Consumer<RedisConnection>) it -> it.scan(ScanOptions.NONE)),
				Arguments.of("HSCAN", (Consumer<RedisConnection>) it -> it.hScan(key, ScanOptions.NONE)),
				Arguments.of("SSCAN", (Consumer<RedisConnection>) it -> it.sScan(key, ScanOptions.NONE)),
				Arguments.of("ZSCAN", (Consumer<RedisConnection>) it -> it.zScan(key, ScanOptions.NONE)));
	}

}
