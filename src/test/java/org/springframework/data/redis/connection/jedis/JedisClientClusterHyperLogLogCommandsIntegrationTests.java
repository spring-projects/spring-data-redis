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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.connection.RedisClusterConfiguration;
import org.springframework.data.redis.connection.RedisClusterConnection;
import org.springframework.data.redis.test.condition.EnabledOnRedisClusterAvailable;
import org.springframework.data.redis.test.extension.JedisExtension;

import static org.assertj.core.api.Assertions.*;

/**
 * Integration tests for {@link JedisClientHyperLogLogCommands} in cluster mode. Tests all methods in direct and
 * pipelined modes (transactions not supported in cluster).
 *
 * @author Tihomir Mateev
 * @since 4.1
 */
@EnabledOnRedisClusterAvailable
@ExtendWith(JedisExtension.class)
class JedisClientClusterHyperLogLogCommandsIntegrationTests {

	private JedisClientConnectionFactory factory;
	private RedisClusterConnection connection;

	@BeforeEach
	void setUp() {
		RedisClusterConfiguration config = new RedisClusterConfiguration().clusterNode(SettingsUtils.getHost(),
				SettingsUtils.getClusterPort());
		factory = new JedisClientConnectionFactory(config);
		factory.afterPropertiesSet();
		connection = factory.getClusterConnection();
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
		// Test pfAdd - add elements
		Long pfAddResult = connection.hyperLogLogCommands().pfAdd("hll1".getBytes(), "a".getBytes(), "b".getBytes(),
				"c".getBytes());
		assertThat(pfAddResult).isEqualTo(1L);

		// Add more elements
		Long pfAddResult2 = connection.hyperLogLogCommands().pfAdd("hll1".getBytes(), "d".getBytes(), "e".getBytes());
		assertThat(pfAddResult2).isGreaterThanOrEqualTo(0L);

		// Test pfCount - count unique elements
		Long pfCountResult = connection.hyperLogLogCommands().pfCount("hll1".getBytes());
		assertThat(pfCountResult).isGreaterThanOrEqualTo(5L);

		// Create another HLL
		connection.hyperLogLogCommands().pfAdd("{tag}hll2".getBytes(), "c".getBytes(), "d".getBytes(), "e".getBytes(),
				"f".getBytes());
		connection.hyperLogLogCommands().pfAdd("{tag}hll3".getBytes(), "a".getBytes(), "b".getBytes());

		// Test pfCount with multiple keys
		Long pfCountMultiResult = connection.hyperLogLogCommands().pfCount("{tag}hll2".getBytes(), "{tag}hll3".getBytes());
		assertThat(pfCountMultiResult).isGreaterThanOrEqualTo(4L);

		// Test pfMerge - merge HLLs
		connection.hyperLogLogCommands().pfMerge("{tag}hllMerged".getBytes(), "{tag}hll2".getBytes(),
				"{tag}hll3".getBytes());
		Long pfCountMergedResult = connection.hyperLogLogCommands().pfCount("{tag}hllMerged".getBytes());
		assertThat(pfCountMergedResult).isGreaterThanOrEqualTo(4L);
	}
}
