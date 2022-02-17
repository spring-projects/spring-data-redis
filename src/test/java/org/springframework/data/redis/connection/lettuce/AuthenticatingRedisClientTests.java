/*
 * Copyright 2013-2022 the original author or authors.
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
import io.lettuce.core.RedisException;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;

import java.util.concurrent.TimeUnit;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.data.redis.test.condition.EnabledOnRedisAvailable;

/**
 * Integration test of {@link AuthenticatingRedisClient}.
 *
 * @author Jennifer Hickey
 * @author Thomas Darimont
 * @author Christoph Strobl
 */
@EnabledOnRedisAvailable(6382)
class AuthenticatingRedisClientTests {

	private RedisClient client;

	@BeforeEach
	void setUp() {
		client = new AuthenticatingRedisClient("localhost", 6382, "foobared");
	}

	@AfterEach
	void tearDown() {
		if (client != null) {
			client.shutdown();
		}
	}

	@Test
	void connect() {
		StatefulRedisConnection<String, String> conn = client.connect();
		conn.sync().ping();
		conn.close();
	}

	@Test
	void connectWithInvalidPassword() {

		if (client != null) {
			client.shutdown();
		}

		RedisClient badClient = new AuthenticatingRedisClient("localhost", 6382, "notthepassword");
		Assertions.assertThatExceptionOfType(RedisException.class).isThrownBy(badClient::connect);
		badClient.shutdown(0, 0, TimeUnit.MILLISECONDS);
	}

	@Test
	void codecConnect() {
		StatefulRedisConnection<byte[], byte[]> conn = client.connect(LettuceConnection.CODEC);
		conn.sync().ping();
		conn.close();
	}

	@Test
	void connectAsync() {
		StatefulRedisConnection<String, String> conn = client.connect();
		conn.sync().ping();
		conn.close();
	}

	@Test
	void codecConnectAsync() {
		StatefulRedisConnection<byte[], byte[]> conn = client.connect(LettuceConnection.CODEC);
		conn.sync().ping();
		conn.close();
	}

	@Test
	void connectPubSub() {
		StatefulRedisPubSubConnection<String, String> conn = client.connectPubSub();
		conn.sync().ping();
		conn.close();
	}

	@Test
	void codecConnectPubSub() {
		StatefulRedisPubSubConnection<byte[], byte[]> conn = client.connectPubSub(LettuceConnection.CODEC);
		conn.sync().ping();
		conn.close();
	}

}
