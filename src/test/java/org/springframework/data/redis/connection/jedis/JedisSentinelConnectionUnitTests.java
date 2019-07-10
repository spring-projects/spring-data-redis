/*
 * Copyright 2014-2019 the original author or authors.
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

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import org.springframework.data.redis.connection.RedisNode;
import org.springframework.data.redis.connection.RedisNode.RedisNodeBuilder;
import org.springframework.data.redis.connection.RedisServer;

/**
 * @author Christoph Strobl
 */
@RunWith(MockitoJUnitRunner.class)
public class JedisSentinelConnectionUnitTests {

	private @Mock Jedis jedisMock;

	private JedisSentinelConnection connection;

	@Before
	public void setUp() {
		this.connection = new JedisSentinelConnection(jedisMock);
	}

	@Test // DATAREDIS-330
	public void shouldConnectAfterCreation() {
		verify(jedisMock, times(1)).connect();
	}

	@SuppressWarnings("resource")
	@Test // DATAREDIS-330
	public void shouldNotConnectIfAlreadyConnected() {

		Jedis yetAnotherJedisMock = mock(Jedis.class);
		when(yetAnotherJedisMock.isConnected()).thenReturn(true);

		new JedisSentinelConnection(yetAnotherJedisMock);

		verify(yetAnotherJedisMock, never()).connect();
	}

	@Test // DATAREDIS-330
	public void failoverShouldBeSentCorrectly() {

		connection.failover(new RedisNodeBuilder().withName("mymaster").build());
		verify(jedisMock, times(1)).sentinelFailover(eq("mymaster"));
	}

	@Test // DATAREDIS-330
	public void failoverShouldThrowExceptionIfMasterNodeIsNull() {
		assertThatIllegalArgumentException().isThrownBy(() -> connection.failover(null));
	}

	@Test // DATAREDIS-330
	public void failoverShouldThrowExceptionIfMasterNodeNameIsEmpty() {
		assertThatIllegalArgumentException().isThrownBy(() -> connection.failover(new RedisNodeBuilder().build()));
	}

	@Test // DATAREDIS-330
	public void mastersShouldReadMastersCorrectly() {

		connection.masters();
		verify(jedisMock, times(1)).sentinelMasters();
	}

	@Test // DATAREDIS-330
	public void shouldReadSlavesCorrectly() {

		connection.slaves("mymaster");
		verify(jedisMock, times(1)).sentinelSlaves(eq("mymaster"));
	}

	@Test // DATAREDIS-330
	public void shouldReadSlavesCorrectlyWhenGivenNamedNode() {

		connection.slaves(new RedisNodeBuilder().withName("mymaster").build());
		verify(jedisMock, times(1)).sentinelSlaves(eq("mymaster"));
	}

	@Test // DATAREDIS-330
	public void readSlavesShouldThrowExceptionWhenGivenEmptyMasterName() {
		assertThatIllegalArgumentException().isThrownBy(() -> connection.slaves(""));
	}

	@Test // DATAREDIS-330
	public void readSlavesShouldThrowExceptionWhenGivenNull() {
		assertThatIllegalArgumentException().isThrownBy(() -> connection.slaves((RedisNode) null));
	}

	@Test // DATAREDIS-330
	public void readSlavesShouldThrowExceptionWhenNodeWithoutName() {
		assertThatIllegalArgumentException().isThrownBy(() -> connection.slaves(new RedisNodeBuilder().build()));
	}

	@Test // DATAREDIS-330
	public void shouldRemoveMasterCorrectlyWhenGivenNamedNode() {

		connection.remove(new RedisNodeBuilder().withName("mymaster").build());
		verify(jedisMock, times(1)).sentinelRemove(eq("mymaster"));
	}

	@Test // DATAREDIS-330
	public void removeShouldThrowExceptionWhenGivenEmptyMasterName() {
		assertThatIllegalArgumentException().isThrownBy(() -> connection.remove(""));
	}

	@Test // DATAREDIS-330
	public void removeShouldThrowExceptionWhenGivenNull() {
		assertThatIllegalArgumentException().isThrownBy(() -> connection.remove((RedisNode) null));
	}

	@Test // DATAREDIS-330
	public void removeShouldThrowExceptionWhenNodeWithoutName() {
		assertThatIllegalArgumentException().isThrownBy(() -> connection.remove(new RedisNodeBuilder().build()));
	}

	@Test // DATAREDIS-330
	public void monitorShouldBeSentCorrectly() {

		RedisServer server = new RedisServer("127.0.0.1", 6382);
		server.setName("anothermaster");
		server.setQuorum(3L);

		connection.monitor(server);
		verify(jedisMock, times(1)).sentinelMonitor(eq("anothermaster"), eq("127.0.0.1"), eq(6382), eq(3));
	}

}
