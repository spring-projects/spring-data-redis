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
package org.springframework.data.redis.connection.jedis;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.data.redis.connection.RedisNode;
import org.springframework.data.redis.connection.RedisNode.RedisNodeBuilder;
import org.springframework.data.redis.connection.RedisServer;

import redis.clients.jedis.Jedis;

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

	/**
	 * @see DATAREDIS-330
	 */
	@Test
	public void shouldConnectAfterCreation() {
		verify(jedisMock, times(1)).connect();
	}

	/**
	 * @see DATAREDIS-330
	 */
	@SuppressWarnings("resource")
	@Test
	public void shouldNotConnectIfAlreadyConnected() {

		Jedis yetAnotherJedisMock = mock(Jedis.class);
		when(yetAnotherJedisMock.isConnected()).thenReturn(true);

		new JedisSentinelConnection(yetAnotherJedisMock);

		verify(yetAnotherJedisMock, never()).connect();
	}

	/**
	 * @see DATAREDIS-330
	 */
	@Test
	public void failoverShouldBeSentCorrectly() {

		connection.failover(new RedisNodeBuilder().withName("mymaster").build());
		verify(jedisMock, times(1)).sentinelFailover(eq("mymaster"));
	}

	/**
	 * @see DATAREDIS-330
	 */
	@Test(expected = IllegalArgumentException.class)
	public void failoverShouldThrowExceptionIfMasterNodeIsNull() {
		connection.failover(null);
	}

	/**
	 * @see DATAREDIS-330
	 */
	@Test(expected = IllegalArgumentException.class)
	public void failoverShouldThrowExceptionIfMasterNodeNameIsEmpty() {
		connection.failover(new RedisNodeBuilder().build());
	}

	/**
	 * @see DATAREDIS-330
	 */
	@Test
	public void mastersShouldReadMastersCorrectly() {

		connection.masters();
		verify(jedisMock, times(1)).sentinelMasters();
	}

	/**
	 * @see DATAREDIS-330
	 */
	@Test
	public void shouldReadSlavesCorrectly() {

		connection.slaves("mymaster");
		verify(jedisMock, times(1)).sentinelSlaves(eq("mymaster"));
	}

	/**
	 * @see DATAREDIS-330
	 */
	@Test
	public void shouldReadSlavesCorrectlyWhenGivenNamedNode() {

		connection.slaves(new RedisNodeBuilder().withName("mymaster").build());
		verify(jedisMock, times(1)).sentinelSlaves(eq("mymaster"));
	}

	/**
	 * @see DATAREDIS-330
	 */
	@Test(expected = IllegalArgumentException.class)
	public void readSlavesShouldThrowExceptionWhenGivenEmptyMasterName() {
		connection.slaves("");
	}

	/**
	 * @see DATAREDIS-330
	 */
	@Test(expected = IllegalArgumentException.class)
	public void readSlavesShouldThrowExceptionWhenGivenNull() {
		connection.slaves((RedisNode) null);
	}

	/**
	 * @see DATAREDIS-330
	 */
	@Test(expected = IllegalArgumentException.class)
	public void readSlavesShouldThrowExceptionWhenNodeWithoutName() {
		connection.slaves(new RedisNodeBuilder().build());
	}

	/**
	 * @see DATAREDIS-330
	 */
	@Test
	public void shouldRemoveMasterCorrectlyWhenGivenNamedNode() {

		connection.remove(new RedisNodeBuilder().withName("mymaster").build());
		verify(jedisMock, times(1)).sentinelRemove(eq("mymaster"));
	}

	/**
	 * @see DATAREDIS-330
	 */
	@Test(expected = IllegalArgumentException.class)
	public void removeShouldThrowExceptionWhenGivenEmptyMasterName() {
		connection.remove("");
	}

	/**
	 * @see DATAREDIS-330
	 */
	@Test(expected = IllegalArgumentException.class)
	public void removeShouldThrowExceptionWhenGivenNull() {
		connection.remove((RedisNode) null);
	}

	/**
	 * @see DATAREDIS-330
	 */
	@Test(expected = IllegalArgumentException.class)
	public void removeShouldThrowExceptionWhenNodeWithoutName() {
		connection.remove(new RedisNodeBuilder().build());
	}

	/**
	 * @see DATAREDIS-330
	 */
	@Test
	public void monitorShouldBeSentCorrectly() {

		RedisServer server = new RedisServer("127.0.0.1", 6382);
		server.setName("anothermaster");
		server.setQuorum(3L);

		connection.monitor(server);
		verify(jedisMock, times(1)).sentinelMonitor(eq("anothermaster"), eq("127.0.0.1"), eq(6382), eq(3));
	}

}
