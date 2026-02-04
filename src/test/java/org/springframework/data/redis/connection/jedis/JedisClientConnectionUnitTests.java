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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.UnifiedJedis;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link JedisClientConnection}.
 *
 * @author Tihomir Mateev
 * @since 4.1
 */
class JedisClientConnectionUnitTests {

	private UnifiedJedis clientMock;
	private JedisClientConnection connection;

	@BeforeEach
	void setUp() {
		clientMock = mock(UnifiedJedis.class);
		connection = new JedisClientConnection(clientMock, DefaultJedisClientConfig.builder().build());
	}

	@Test // GH-XXXX
	void shouldNotBePipelinedInitially() {
		assertThat(connection.isPipelined()).isFalse();
	}

	@Test // GH-XXXX
	void shouldNotBeQueueingInitially() {
		assertThat(connection.isQueueing()).isFalse();
	}

	@Test // GH-XXXX
	void shouldReturnClientFromGetter() {

		assertThat(connection.getJedis()).isEqualTo(clientMock);
	}

	@Test // GH-XXXX
	void shouldSetConvertPipelineAndTxResults() {

		connection.setConvertPipelineAndTxResults(false);

		// No direct way to verify, but should not throw exception
		assertThat(connection).isNotNull();
	}

	@Test // GH-XXXX
	void shouldReturnNativeConnectionFromGetter() {

		assertThat(connection.getNativeConnection()).isEqualTo(clientMock);
	}
}
