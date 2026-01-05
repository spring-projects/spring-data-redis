/*
 * Copyright 2016-present the original author or authors.
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

import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;

import org.springframework.data.redis.test.condition.EnabledOnRedisClusterAvailable;
import org.springframework.data.redis.test.extension.LettuceExtension;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@EnabledOnRedisClusterAvailable
@ExtendWith(LettuceExtension.class)
public abstract class LettuceReactiveClusterTestSupport {

	RedisClusterCommands<String, String> nativeCommands;
	LettuceReactiveRedisClusterConnection connection;

	@BeforeEach
	public void before(RedisClusterClient clusterClient) {

		nativeCommands = clusterClient.connect().sync();
		connection = new LettuceReactiveRedisClusterConnection(
				new ClusterConnectionProvider(clusterClient, LettuceReactiveRedisConnection.CODEC), clusterClient);
	}

	@AfterEach
	public void tearDown() {

		if (nativeCommands != null) {
			nativeCommands.flushall();

			if (nativeCommands instanceof RedisCommands redisCommands) {
				redisCommands.getStatefulConnection().close();
			}

			if (nativeCommands instanceof RedisAdvancedClusterCommands redisAdvancedClusterCommands) {
				redisAdvancedClusterCommands.getStatefulConnection().close();
			}
		}

		if (connection != null) {
			connection.close();
		}
	}
}
