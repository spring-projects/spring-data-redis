/*
 * Copyright 2014-2021 the original author or authors.
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
import static org.mockito.Mockito.*;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.sentinel.api.StatefulRedisSentinelConnection;
import io.lettuce.core.sentinel.api.sync.RedisSentinelCommands;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import org.springframework.data.redis.connection.RedisNode;
import org.springframework.data.redis.connection.RedisNode.RedisNodeBuilder;
import org.springframework.data.redis.connection.RedisServer;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class LettuceSentinelConnectionUnitTests {

	private static final String MASTER_ID = "mymaster";

	private @Mock RedisClient redisClientMock;

	private @Mock StatefulRedisSentinelConnection<String, String> connectionMock;
	private @Mock RedisSentinelCommands<String, String> sentinelCommandsMock;

	private @Mock RedisFuture<List<Map<String, String>>> redisFutureMock;

	private LettuceSentinelConnection connection;

	@BeforeEach
	void setUp() {

		when(redisClientMock.connectSentinel()).thenReturn(connectionMock);
		when(connectionMock.sync()).thenReturn(sentinelCommandsMock);
		this.connection = new LettuceSentinelConnection(redisClientMock);
	}

	@Test // DATAREDIS-348
	void shouldConnectAfterCreation() {
		verify(redisClientMock, times(1)).connectSentinel();
	}

	@Test // DATAREDIS-348
	void failoverShouldBeSentCorrectly() {

		connection.failover(new RedisNodeBuilder().withName(MASTER_ID).build());
		verify(sentinelCommandsMock, times(1)).failover(eq(MASTER_ID));
	}

	@Test // DATAREDIS-348
	void failoverShouldThrowExceptionIfMasterNodeIsNull() {
		assertThatIllegalArgumentException().isThrownBy(() -> connection.failover(null));
	}

	@Test // DATAREDIS-348
	void failoverShouldThrowExceptionIfMasterNodeNameIsEmpty() {
		assertThatIllegalArgumentException().isThrownBy(() -> connection.failover(new RedisNodeBuilder().build()));
	}

	@Test // DATAREDIS-348
	void mastersShouldReadMastersCorrectly() {

		when(sentinelCommandsMock.masters()).thenReturn(Collections.<Map<String, String>> emptyList());
		connection.masters();
		verify(sentinelCommandsMock, times(1)).masters();
	}

	@Test // DATAREDIS-348
	void shouldReadSlavesCorrectly() {

		when(sentinelCommandsMock.slaves(MASTER_ID)).thenReturn(Collections.<Map<String, String>> emptyList());
		connection.slaves(MASTER_ID);
		verify(sentinelCommandsMock, times(1)).slaves(eq(MASTER_ID));
	}

	@Test // DATAREDIS-348
	void shouldReadSlavesCorrectlyWhenGivenNamedNode() {

		when(sentinelCommandsMock.slaves(MASTER_ID)).thenReturn(Collections.<Map<String, String>> emptyList());
		connection.slaves(new RedisNodeBuilder().withName(MASTER_ID).build());
		verify(sentinelCommandsMock, times(1)).slaves(eq(MASTER_ID));
	}

	@Test // DATAREDIS-348
	void readSlavesShouldThrowExceptionWhenGivenEmptyMasterName() {
		assertThatIllegalArgumentException().isThrownBy(() -> connection.slaves(""));
	}

	@Test // DATAREDIS-348
	void readSlavesShouldThrowExceptionWhenGivenNull() {
		assertThatIllegalArgumentException().isThrownBy(() -> connection.slaves((RedisNode) null));
	}

	@Test // DATAREDIS-348
	void readSlavesShouldThrowExceptionWhenNodeWithoutName() {
		assertThatIllegalArgumentException().isThrownBy(() -> connection.slaves(new RedisNodeBuilder().build()));
	}

	@Test // DATAREDIS-348
	void shouldRemoveMasterCorrectlyWhenGivenNamedNode() {

		connection.remove(new RedisNodeBuilder().withName(MASTER_ID).build());
		verify(sentinelCommandsMock, times(1)).remove(eq(MASTER_ID));
	}

	@Test // DATAREDIS-348
	void removeShouldThrowExceptionWhenGivenEmptyMasterName() {
		assertThatIllegalArgumentException().isThrownBy(() -> connection.remove(""));
	}

	@Test // DATAREDIS-348
	void removeShouldThrowExceptionWhenGivenNull() {
		assertThatIllegalArgumentException().isThrownBy(() -> connection.remove((RedisNode) null));
	}

	@Test // DATAREDIS-348
	void removeShouldThrowExceptionWhenNodeWithoutName() {
		assertThatIllegalArgumentException().isThrownBy(() -> connection.remove(new RedisNodeBuilder().build()));
	}

	@Test // DATAREDIS-348
	void monitorShouldBeSentCorrectly() {

		RedisServer server = new RedisServer("127.0.0.1", 6382);
		server.setName("anothermaster");
		server.setQuorum(3L);

		connection.monitor(server);
		verify(sentinelCommandsMock, times(1)).monitor(eq("anothermaster"), eq("127.0.0.1"), eq(6382), eq(3));
	}
}
