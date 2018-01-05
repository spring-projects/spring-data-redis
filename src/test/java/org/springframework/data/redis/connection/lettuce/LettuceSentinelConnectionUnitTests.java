/*
 * Copyright 2014-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.redis.connection.lettuce;

import static org.mockito.Mockito.*;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.sentinel.api.StatefulRedisSentinelConnection;
import io.lettuce.core.sentinel.api.sync.RedisSentinelCommands;

import java.util.Collections;
import java.util.List;
import java.util.Map;

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
 * @author Mark Paluch
 */
@RunWith(MockitoJUnitRunner.class)
public class LettuceSentinelConnectionUnitTests {

	public static final String MASTER_ID = "mymaster";

	private @Mock RedisClient redisClientMock;

	private @Mock StatefulRedisSentinelConnection<String, String> connectionMock;
	private @Mock RedisSentinelCommands<String, String> sentinelCommandsMock;

	private @Mock RedisFuture<List<Map<String, String>>> redisFutureMock;

	private LettuceSentinelConnection connection;

	@Before
	public void setUp() {

		when(redisClientMock.connectSentinel()).thenReturn(connectionMock);
		when(connectionMock.sync()).thenReturn(sentinelCommandsMock);
		this.connection = new LettuceSentinelConnection(redisClientMock);
	}

	@Test // DATAREDIS-348
	public void shouldConnectAfterCreation() {
		verify(redisClientMock, times(1)).connectSentinel();
	}

	@Test // DATAREDIS-348
	public void failoverShouldBeSentCorrectly() {

		connection.failover(new RedisNodeBuilder().withName(MASTER_ID).build());
		verify(sentinelCommandsMock, times(1)).failover(eq(MASTER_ID));
	}

	@Test(expected = IllegalArgumentException.class) // DATAREDIS-348
	public void failoverShouldThrowExceptionIfMasterNodeIsNull() {
		connection.failover(null);
	}

	@Test(expected = IllegalArgumentException.class) // DATAREDIS-348
	public void failoverShouldThrowExceptionIfMasterNodeNameIsEmpty() {
		connection.failover(new RedisNodeBuilder().build());
	}

	@Test // DATAREDIS-348
	public void mastersShouldReadMastersCorrectly() {

		when(sentinelCommandsMock.masters()).thenReturn(Collections.<Map<String, String>> emptyList());
		connection.masters();
		verify(sentinelCommandsMock, times(1)).masters();
	}

	@Test // DATAREDIS-348
	public void shouldReadSlavesCorrectly() {

		when(sentinelCommandsMock.slaves(MASTER_ID)).thenReturn(Collections.<Map<String, String>> emptyList());
		connection.slaves(MASTER_ID);
		verify(sentinelCommandsMock, times(1)).slaves(eq(MASTER_ID));
	}

	@Test // DATAREDIS-348
	public void shouldReadSlavesCorrectlyWhenGivenNamedNode() {

		when(sentinelCommandsMock.slaves(MASTER_ID)).thenReturn(Collections.<Map<String, String>> emptyList());
		connection.slaves(new RedisNodeBuilder().withName(MASTER_ID).build());
		verify(sentinelCommandsMock, times(1)).slaves(eq(MASTER_ID));
	}

	@Test(expected = IllegalArgumentException.class) // DATAREDIS-348
	public void readSlavesShouldThrowExceptionWhenGivenEmptyMasterName() {
		connection.slaves("");
	}

	@Test(expected = IllegalArgumentException.class) // DATAREDIS-348
	public void readSlavesShouldThrowExceptionWhenGivenNull() {
		connection.slaves((RedisNode) null);
	}

	@Test(expected = IllegalArgumentException.class) // DATAREDIS-348
	public void readSlavesShouldThrowExceptionWhenNodeWithoutName() {
		connection.slaves(new RedisNodeBuilder().build());
	}

	@Test // DATAREDIS-348
	public void shouldRemoveMasterCorrectlyWhenGivenNamedNode() {

		connection.remove(new RedisNodeBuilder().withName(MASTER_ID).build());
		verify(sentinelCommandsMock, times(1)).remove(eq(MASTER_ID));
	}

	@Test(expected = IllegalArgumentException.class) // DATAREDIS-348
	public void removeShouldThrowExceptionWhenGivenEmptyMasterName() {
		connection.remove("");
	}

	@Test(expected = IllegalArgumentException.class) // DATAREDIS-348
	public void removeShouldThrowExceptionWhenGivenNull() {
		connection.remove((RedisNode) null);
	}

	@Test(expected = IllegalArgumentException.class) // DATAREDIS-348
	public void removeShouldThrowExceptionWhenNodeWithoutName() {
		connection.remove(new RedisNodeBuilder().build());
	}

	@Test // DATAREDIS-348
	public void monitorShouldBeSentCorrectly() {

		RedisServer server = new RedisServer("127.0.0.1", 6382);
		server.setName("anothermaster");
		server.setQuorum(3L);

		connection.monitor(server);
		verify(sentinelCommandsMock, times(1)).monitor(eq("anothermaster"), eq("127.0.0.1"), eq(6382), eq(3));
	}
}
