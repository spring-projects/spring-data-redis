/*
 * Copyright 2014 the original author or authors.
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

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.data.redis.connection.RedisNode;
import org.springframework.data.redis.connection.RedisNode.RedisNodeBuilder;
import org.springframework.data.redis.connection.RedisServer;

import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisFuture;
import com.lambdaworks.redis.RedisSentinelAsyncConnection;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@RunWith(MockitoJUnitRunner.class)
public class LettuceSentinelConnectionUnitTests {

	public static final String MASTER_ID = "mymaster";

	private @Mock RedisClient redisClientMock;

	private @Mock RedisSentinelAsyncConnection<String, String> connectionMock;

	private @Mock RedisFuture<List<Map<String, String>>> redisFutureMock;

	private LettuceSentinelConnection connection;

	@Before
	public void setUp() {

		when(redisClientMock.connectSentinelAsync()).thenReturn(connectionMock);
		this.connection = new LettuceSentinelConnection(redisClientMock);
	}

	/**
	 * @see DATAREDIS-348
	 */
	@Test
	public void shouldConnectAfterCreation() {
		verify(redisClientMock, times(1)).connectSentinelAsync();
	}

	/**
	 * @see DATAREDIS-348
	 */
	@Test
	public void failoverShouldBeSentCorrectly() {

		connection.failover(new RedisNodeBuilder().withName(MASTER_ID).build());
		verify(connectionMock, times(1)).failover(eq(MASTER_ID));
	}

	/**
	 * @see DATAREDIS-348
	 */
	@Test(expected = IllegalArgumentException.class)
	public void failoverShouldThrowExceptionIfMasterNodeIsNull() {
		connection.failover(null);
	}

	/**
	 * @see DATAREDIS-348
	 */
	@Test(expected = IllegalArgumentException.class)
	public void failoverShouldThrowExceptionIfMasterNodeNameIsEmpty() {
		connection.failover(new RedisNodeBuilder().build());
	}

	/**
	 * @see DATAREDIS-348
	 */
	@Test
	public void mastersShouldReadMastersCorrectly() {

		when(connectionMock.masters()).thenReturn(redisFutureMock);
		connection.masters();
		verify(connectionMock, times(1)).masters();
	}

	/**
	 * @see DATAREDIS-348
	 */
	@Test
	public void shouldReadSlavesCorrectly() {

		when(connectionMock.slaves(MASTER_ID)).thenReturn(redisFutureMock);
		connection.slaves(MASTER_ID);
		verify(connectionMock, times(1)).slaves(eq(MASTER_ID));
	}

	/**
	 * @see DATAREDIS-348
	 */
	@Test
	public void shouldReadSlavesCorrectlyWhenGivenNamedNode() {

		when(connectionMock.slaves(MASTER_ID)).thenReturn(redisFutureMock);
		connection.slaves(new RedisNodeBuilder().withName(MASTER_ID).build());
		verify(connectionMock, times(1)).slaves(eq(MASTER_ID));
	}

	/**
	 * @see DATAREDIS-348
	 */
	@Test(expected = IllegalArgumentException.class)
	public void readSlavesShouldThrowExceptionWhenGivenEmptyMasterName() {
		connection.slaves("");
	}

	/**
	 * @see DATAREDIS-348
	 */
	@Test(expected = IllegalArgumentException.class)
	public void readSlavesShouldThrowExceptionWhenGivenNull() {
		connection.slaves((RedisNode) null);
	}

	/**
	 * @see DATAREDIS-348
	 */
	@Test(expected = IllegalArgumentException.class)
	public void readSlavesShouldThrowExceptionWhenNodeWithoutName() {
		connection.slaves(new RedisNodeBuilder().build());
	}

	/**
	 * @see DATAREDIS-348
	 */
	@Test
	public void shouldRemoveMasterCorrectlyWhenGivenNamedNode() {

		connection.remove(new RedisNodeBuilder().withName(MASTER_ID).build());
		verify(connectionMock, times(1)).remove(eq(MASTER_ID));
	}

	/**
	 * @see DATAREDIS-348
	 */
	@Test(expected = IllegalArgumentException.class)
	public void removeShouldThrowExceptionWhenGivenEmptyMasterName() {
		connection.remove("");
	}

	/**
	 * @see DATAREDIS-348
	 */
	@Test(expected = IllegalArgumentException.class)
	public void removeShouldThrowExceptionWhenGivenNull() {
		connection.remove((RedisNode) null);
	}

	/**
	 * @see DATAREDIS-348
	 */
	@Test(expected = IllegalArgumentException.class)
	public void removeShouldThrowExceptionWhenNodeWithoutName() {
		connection.remove(new RedisNodeBuilder().build());
	}

	/**
	 * @see DATAREDIS-348
	 */
	@Test
	public void monitorShouldBeSentCorrectly() {

		RedisServer server = new RedisServer("127.0.0.1", 6382);
		server.setName("anothermaster");
		server.setQuorum(3L);

		connection.monitor(server);
		verify(connectionMock, times(1)).monitor(eq("anothermaster"), eq("127.0.0.1"), eq(6382), eq(3));
	}

}
