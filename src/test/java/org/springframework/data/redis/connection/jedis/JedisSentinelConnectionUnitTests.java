/*
 * Copyright 2014-present the original author or authors.
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
import static org.mockito.Mockito.*;

import redis.clients.jedis.Jedis;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.springframework.data.redis.connection.RedisNode;
import org.springframework.data.redis.connection.RedisNode.RedisNodeBuilder;
import org.springframework.data.redis.connection.RedisServer;

/**
 * @author Christoph Strobl
 */
@ExtendWith(MockitoExtension.class)
class JedisSentinelConnectionUnitTests {

	private @Mock Jedis jedisMock;

	private JedisSentinelConnection connection;

	@BeforeEach
	void setUp() {
		this.connection = new JedisSentinelConnection(jedisMock);
	}

	@Test // DATAREDIS-330
	void shouldConnectAfterCreation() {
		verify(jedisMock, times(1)).connect();
	}

	@SuppressWarnings("resource")
	@Test // DATAREDIS-330
	void shouldNotConnectIfAlreadyConnected() {

		Jedis yetAnotherJedisMock = mock(Jedis.class);
		when(yetAnotherJedisMock.isConnected()).thenReturn(true);

		new JedisSentinelConnection(yetAnotherJedisMock);

		verify(yetAnotherJedisMock, never()).connect();
	}

	@Test // DATAREDIS-330
	void failoverShouldBeSentCorrectly() {

		connection.failover(new RedisNodeBuilder().withName("mymaster").build());
		verify(jedisMock, times(1)).sentinelFailover(eq("mymaster"));
	}

	@Test // DATAREDIS-330
	void failoverShouldThrowExceptionIfMasterNodeIsNull() {
		assertThatIllegalArgumentException().isThrownBy(() -> connection.failover(null));
	}

	@Test // DATAREDIS-330
	void failoverShouldThrowExceptionIfMasterNodeNameIsEmpty() {
		assertThatIllegalArgumentException().isThrownBy(() -> connection.failover(new RedisNodeBuilder().build()));
	}

	@Test // DATAREDIS-330
	void mastersShouldReadMastersCorrectly() {

		connection.masters();
		verify(jedisMock, times(1)).sentinelMasters();
	}

	@Test // DATAREDIS-330
	void shouldReadReplicasCorrectly() {

		connection.replicas("mymaster");
		verify(jedisMock, times(1)).sentinelReplicas(eq("mymaster"));
	}

	@Test // DATAREDIS-330
	void shouldReadReplicasCorrectlyWhenGivenNamedNode() {

		connection.replicas(new RedisNodeBuilder().withName("mymaster").build());
		verify(jedisMock, times(1)).sentinelReplicas(eq("mymaster"));
	}

	@Test // DATAREDIS-330
	void readReplicasShouldThrowExceptionWhenGivenEmptyMasterName() {
		assertThatIllegalArgumentException().isThrownBy(() -> connection.replicas(""));
	}

	@Test // DATAREDIS-330
	void readReplicasShouldThrowExceptionWhenGivenNull() {
		assertThatIllegalArgumentException().isThrownBy(() -> connection.replicas((RedisNode) null));
	}

	@Test // DATAREDIS-330
	void readReplicasShouldThrowExceptionWhenNodeWithoutName() {
		assertThatIllegalArgumentException().isThrownBy(() -> connection.replicas(new RedisNodeBuilder().build()));
	}

	@Test // DATAREDIS-330
	void shouldRemoveMasterCorrectlyWhenGivenNamedNode() {

		connection.remove(new RedisNodeBuilder().withName("mymaster").build());
		verify(jedisMock, times(1)).sentinelRemove(eq("mymaster"));
	}

	@Test // DATAREDIS-330
	void removeShouldThrowExceptionWhenGivenEmptyMasterName() {
		assertThatIllegalArgumentException().isThrownBy(() -> connection.remove(""));
	}

	@Test // DATAREDIS-330
	void removeShouldThrowExceptionWhenGivenNull() {
		assertThatIllegalArgumentException().isThrownBy(() -> connection.remove((RedisNode) null));
	}

	@Test // DATAREDIS-330
	void removeShouldThrowExceptionWhenNodeWithoutName() {
		assertThatIllegalArgumentException().isThrownBy(() -> connection.remove(new RedisNodeBuilder().build()));
	}

	@Test // DATAREDIS-330
	void monitorShouldBeSentCorrectly() {

		RedisServer server = new RedisServer("127.0.0.1", 6382);
		server.setName("anothermaster");
		server.setQuorum(3L);

		connection.monitor(server);
		verify(jedisMock, times(1)).sentinelMonitor(eq("anothermaster"), eq("127.0.0.1"), eq(6382), eq(3));
	}

}
