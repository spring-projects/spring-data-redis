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

import redis.clients.jedis.ConnectionPool;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.RedisClusterClient;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;

/**
 * Jedis Cluster connection tests via {@link RedisClusterClient}.
 *
 * @author Mark Paluch
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class JedisRedisClusterClientConnectionTests extends JedisClusterConnectionTestSupport {

	private final RedisClusterClient nativeConnection;

	public JedisRedisClusterClientConnectionTests(RedisClusterClient nativeConnection) {
		super(nativeConnection, new JedisClusterConnection(nativeConnection));
		this.nativeConnection = nativeConnection;
	}

	@BeforeEach
	void beforeEach() {

		for (ConnectionPool pool : nativeConnection.getClusterNodes().values()) {
			try (Jedis jedis = new Jedis(pool.getResource())) {
				jedis.flushAll();
			} catch (Exception ignore) {
				// ignore since we cannot remove data from replicas
			}
		}
	}

}
