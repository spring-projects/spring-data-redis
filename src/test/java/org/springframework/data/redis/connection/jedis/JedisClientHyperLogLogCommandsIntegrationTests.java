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

import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.test.condition.EnabledOnRedisAvailable;
import org.springframework.data.redis.test.extension.JedisExtension;

import static org.assertj.core.api.Assertions.*;

/**
 * Integration tests for {@link JedisClientHyperLogLogCommands}. Tests all methods in direct, transaction, and pipelined
 * modes.
 *
 * @author Tihomir Mateev
 * @since 4.1
 */
@EnabledOnRedisAvailable
@ExtendWith(JedisExtension.class)
class JedisClientHyperLogLogCommandsIntegrationTests {

	private JedisClientConnectionFactory factory;
	private JedisClientConnection connection;

	@BeforeEach
	void setUp() {
		RedisStandaloneConfiguration config = new RedisStandaloneConfiguration(SettingsUtils.getHost(),
				SettingsUtils.getPort());
		factory = new JedisClientConnectionFactory(config);
		factory.afterPropertiesSet();
		connection = (JedisClientConnection) factory.getConnection();
	}

	@AfterEach
	void tearDown() {
		if (connection != null) {
			connection.serverCommands().flushDb();
			connection.close();
		}
		if (factory != null) {
			factory.destroy();
		}
	}

	// ============ HyperLogLog Operations ============
	@Test
	void hyperLogLogOperationsShouldWork() {
		// Test pfAdd - add elements to HyperLogLog
		Long addResult1 = connection.hyperLogLogCommands().pfAdd("hll1".getBytes(), "a".getBytes(), "b".getBytes(),
				"c".getBytes());
		assertThat(addResult1).isEqualTo(1L); // 1 means HLL was modified

		// Add more elements
		Long addResult2 = connection.hyperLogLogCommands().pfAdd("hll1".getBytes(), "d".getBytes(), "e".getBytes());
		assertThat(addResult2).isEqualTo(1L);

		// Add duplicate elements - should not modify HLL
		Long addResult3 = connection.hyperLogLogCommands().pfAdd("hll1".getBytes(), "a".getBytes(), "b".getBytes());
		assertThat(addResult3).isEqualTo(0L); // 0 means HLL was not modified

		// Test pfCount - count unique elements in single HLL
		Long countResult = connection.hyperLogLogCommands().pfCount("hll1".getBytes());
		assertThat(countResult).isEqualTo(5L); // Approximate count of unique elements

		// Create another HLL
		connection.hyperLogLogCommands().pfAdd("hll2".getBytes(), "c".getBytes(), "d".getBytes(), "f".getBytes(),
				"g".getBytes());

		// Test pfCount - count unique elements across multiple HLLs
		Long countMultiResult = connection.hyperLogLogCommands().pfCount("hll1".getBytes(), "hll2".getBytes());
		assertThat(countMultiResult).isGreaterThanOrEqualTo(6L); // Union of unique elements

		// Test pfMerge - merge multiple HLLs into destination
		connection.hyperLogLogCommands().pfMerge("merged".getBytes(), "hll1".getBytes(), "hll2".getBytes());
		Long mergedCount = connection.hyperLogLogCommands().pfCount("merged".getBytes());
		assertThat(mergedCount).isGreaterThanOrEqualTo(6L); // Should contain union of all unique elements
	}

	@Test
	void transactionShouldExecuteAtomically() {
		// Set up initial state
		connection.hyperLogLogCommands().pfAdd("txHll1".getBytes(), "a".getBytes(), "b".getBytes());
		connection.hyperLogLogCommands().pfAdd("txHll2".getBytes(), "c".getBytes(), "d".getBytes());

		// Execute multiple HyperLogLog operations in a transaction
		connection.multi();
		connection.hyperLogLogCommands().pfAdd("txHll1".getBytes(), "e".getBytes());
		connection.hyperLogLogCommands().pfCount("txHll1".getBytes());
		connection.hyperLogLogCommands().pfMerge("txMerged".getBytes(), "txHll1".getBytes(), "txHll2".getBytes());
		connection.hyperLogLogCommands().pfCount("txMerged".getBytes());
		List<Object> results = connection.exec();

		// Verify all commands executed
		assertThat(results).hasSize(4);
		assertThat(results.get(0)).isEqualTo(1L); // pfAdd result
		assertThat(results.get(1)).isEqualTo(3L); // pfCount result
		// pfMerge returns void, so result is null
		assertThat((Long) results.get(3)).isGreaterThanOrEqualTo(4L); // pfCount result after merge
	}

	@Test
	void pipelineShouldExecuteMultipleCommands() {
		// Set up initial state
		connection.hyperLogLogCommands().pfAdd("pipeHll1".getBytes(), "a".getBytes(), "b".getBytes());
		connection.hyperLogLogCommands().pfAdd("pipeHll2".getBytes(), "c".getBytes(), "d".getBytes());

		// Execute multiple HyperLogLog operations in pipeline
		connection.openPipeline();
		connection.hyperLogLogCommands().pfAdd("pipeHll1".getBytes(), "e".getBytes(), "f".getBytes());
		connection.hyperLogLogCommands().pfCount("pipeHll1".getBytes());
		connection.hyperLogLogCommands().pfCount("pipeHll1".getBytes(), "pipeHll2".getBytes());
		connection.hyperLogLogCommands().pfMerge("pipeMerged".getBytes(), "pipeHll1".getBytes(), "pipeHll2".getBytes());
		connection.hyperLogLogCommands().pfCount("pipeMerged".getBytes());
		List<Object> results = connection.closePipeline();

		// Verify all command results
		assertThat(results).hasSize(5);
		assertThat(results.get(0)).isEqualTo(1L); // pfAdd result
		assertThat(results.get(1)).isEqualTo(4L); // pfCount result for hll1
		assertThat((Long) results.get(2)).isGreaterThanOrEqualTo(5L); // pfCount result for hll1 + hll2
		// pfMerge returns void, so result is null
		assertThat((Long) results.get(4)).isGreaterThanOrEqualTo(5L); // pfCount result after merge
	}
}
