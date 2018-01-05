/*
 * Copyright 2015-2018 the original author or authors.
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
package org.springframework.data.redis.connection.jedis;

import static org.hamcrest.Matchers.*;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static org.springframework.data.redis.connection.ClusterTestVariables.*;
import static org.springframework.data.redis.test.util.MockitoUtils.*;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisClusterConnectionHandler;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.data.redis.ClusterStateFailureException;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.ClusterInfo;
import org.springframework.data.redis.connection.RedisClusterCommands.AddSlots;
import org.springframework.data.redis.connection.RedisClusterNode;
import org.springframework.data.redis.connection.jedis.JedisClusterConnection.JedisClusterTopologyProvider;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@RunWith(MockitoJUnitRunner.Silent.class)
public class JedisClusterConnectionUnitTests {

	private static final String CLUSTER_NODES_RESPONSE = "" //
			+ MASTER_NODE_1_ID + " " + CLUSTER_HOST + ":" + MASTER_NODE_1_PORT + " myself,master - 0 0 1 connected 0-5460"
			+ "\n" + MASTER_NODE_2_ID + " " + CLUSTER_HOST + ":" + MASTER_NODE_2_PORT
			+ " master - 0 1427718161587 2 connected 5461-10922" + "\n" + MASTER_NODE_2_ID + " " + CLUSTER_HOST + ":"
			+ MASTER_NODE_3_PORT + " master - 0 1427718161587 3 connected 10923-16383";

	static final String CLUSTER_INFO_RESPONSE = "cluster_state:ok" + "\n" + "cluster_slots_assigned:16384" + "\n"
			+ "cluster_slots_ok:16384" + "\n" + "cluster_slots_pfail:0" + "\n" + "cluster_slots_fail:0" + "\n"
			+ "cluster_known_nodes:4" + "\n" + "cluster_size:3" + "\n" + "cluster_current_epoch:30" + "\n"
			+ "cluster_my_epoch:2" + "\n" + "cluster_stats_messages_sent:2560260" + "\n"
			+ "cluster_stats_messages_received:2560086";

	JedisClusterConnection connection;

	@Spy StubJedisCluster clusterMock;
	@Mock JedisClusterConnectionHandler connectionHandlerMock;

	@Mock JedisPool node1PoolMock;
	@Mock JedisPool node2PoolMock;
	@Mock JedisPool node3PoolMock;

	@Mock Jedis con1Mock;
	@Mock Jedis con2Mock;
	@Mock Jedis con3Mock;

	Map<String, JedisPool> nodes = new LinkedHashMap<>();

	public @Rule ExpectedException expectedException = ExpectedException.none();

	@Before
	public void setUp() {

		nodes.put(CLUSTER_HOST + ":" + MASTER_NODE_1_PORT, node1PoolMock);
		nodes.put(CLUSTER_HOST + ":" + MASTER_NODE_2_PORT, node2PoolMock);
		nodes.put(CLUSTER_HOST + ":" + MASTER_NODE_3_PORT, node3PoolMock);

		when(clusterMock.getClusterNodes()).thenReturn(nodes);
		when(node1PoolMock.getResource()).thenReturn(con1Mock);
		when(node2PoolMock.getResource()).thenReturn(con2Mock);
		when(node3PoolMock.getResource()).thenReturn(con3Mock);

		when(con1Mock.clusterNodes()).thenReturn(CLUSTER_NODES_RESPONSE);
		clusterMock.setConnectionHandler(connectionHandlerMock);

		connection = new JedisClusterConnection(clusterMock);
	}

	@Test // DATAREDIS-315
	public void throwsExceptionWhenClusterCommandExecutorIsNull() {

		expectedException.expect(IllegalArgumentException.class);

		new JedisClusterConnection(clusterMock, null);
	}

	@Test // DATAREDIS-315
	public void clusterMeetShouldSendCommandsToExistingNodesCorrectly() {

		connection.clusterMeet(UNKNOWN_CLUSTER_NODE);

		verify(con1Mock, times(1)).clusterMeet(UNKNOWN_CLUSTER_NODE.getHost(), UNKNOWN_CLUSTER_NODE.getPort());
		verify(con2Mock, times(1)).clusterMeet(UNKNOWN_CLUSTER_NODE.getHost(), UNKNOWN_CLUSTER_NODE.getPort());
		verify(con2Mock, times(1)).clusterMeet(UNKNOWN_CLUSTER_NODE.getHost(), UNKNOWN_CLUSTER_NODE.getPort());
	}

	@Test // DATAREDIS-315
	public void clusterMeetShouldThrowExceptionWhenNodeIsNull() {

		expectedException.expect(IllegalArgumentException.class);

		connection.clusterMeet(null);
	}

	@Test // DATAREDIS-315
	public void clusterForgetShouldSendCommandsToRemainingNodesCorrectly() {

		connection.clusterForget(CLUSTER_NODE_2);

		verify(con1Mock, times(1)).clusterForget(CLUSTER_NODE_2.getId());
		verifyZeroInteractions(con2Mock);
		verify(con3Mock, times(1)).clusterForget(CLUSTER_NODE_2.getId());
	}

	@Test // DATAREDIS-315
	public void clusterReplicateShouldSendCommandsCorrectly() {

		connection.clusterReplicate(CLUSTER_NODE_1, CLUSTER_NODE_2);

		verify(con2Mock, times(1)).clusterReplicate(CLUSTER_NODE_1.getId());
		verify(con1Mock, times(1)).clusterNodes();
		verify(con1Mock, times(1)).close();
		verifyZeroInteractions(con1Mock);
	}

	@Test // DATAREDIS-315
	public void closeShouldNotCloseUnderlyingClusterPool() throws IOException {

		connection.close();

		verify(clusterMock, never()).close();
	}

	@Test // DATAREDIS-315
	public void isClosedShouldReturnConnectionStateCorrectly() {

		assertThat(connection.isClosed(), is(false));

		connection.close();

		assertThat(connection.isClosed(), is(true));
	}

	@Test // DATAREDIS-315
	public void clusterInfoShouldBeReturnedCorrectly() {

		when(con1Mock.clusterInfo()).thenReturn(CLUSTER_INFO_RESPONSE);
		when(con2Mock.clusterInfo()).thenReturn(CLUSTER_INFO_RESPONSE);
		when(con3Mock.clusterInfo()).thenReturn(CLUSTER_INFO_RESPONSE);

		ClusterInfo p = connection.clusterGetClusterInfo();
		assertThat(p.getSlotsAssigned(), is(16384L));

		verifyInvocationsAcross("clusterInfo", times(1), con1Mock, con2Mock, con3Mock);
	}

	@Test // DATAREDIS-315
	public void clusterSetSlotImportingShouldBeExecutedCorrectly() {

		connection.clusterSetSlot(CLUSTER_NODE_1, 100, AddSlots.IMPORTING);

		verify(con1Mock, times(1)).clusterSetSlotImporting(eq(100), eq(CLUSTER_NODE_1.getId()));
	}

	@Test // DATAREDIS-315
	public void clusterSetSlotMigratingShouldBeExecutedCorrectly() {

		connection.clusterSetSlot(CLUSTER_NODE_1, 100, AddSlots.MIGRATING);

		verify(con1Mock, times(1)).clusterSetSlotMigrating(eq(100), eq(CLUSTER_NODE_1.getId()));
	}

	@Test // DATAREDIS-315
	public void clusterSetSlotStableShouldBeExecutedCorrectly() {

		connection.clusterSetSlot(CLUSTER_NODE_1, 100, AddSlots.STABLE);

		verify(con1Mock, times(1)).clusterSetSlotStable(eq(100));
	}

	@Test // DATAREDIS-315
	public void clusterSetSlotNodeShouldBeExecutedCorrectly() {

		connection.clusterSetSlot(CLUSTER_NODE_1, 100, AddSlots.NODE);

		verify(con1Mock, times(1)).clusterSetSlotNode(eq(100), eq(CLUSTER_NODE_1.getId()));
	}

	@Test // DATAREDIS-315
	public void clusterSetSlotShouldBeExecutedOnTargetNodeWhenNodeIdNotSet() {

		connection.clusterSetSlot(new RedisClusterNode(CLUSTER_HOST, MASTER_NODE_2_PORT), 100, AddSlots.IMPORTING);

		verify(con2Mock, times(1)).clusterSetSlotImporting(eq(100), eq(CLUSTER_NODE_2.getId()));
	}

	@Test(expected = IllegalArgumentException.class) // DATAREDIS-315
	public void clusterSetSlotShouldThrowExceptionWhenModeIsNull() {
		connection.clusterSetSlot(CLUSTER_NODE_1, 100, null);
	}

	@Test // DATAREDIS-315
	public void clusterDeleteSlotsShouldBeExecutedCorrectly() {

		int[] slots = new int[] { 9000, 10000 };
		connection.clusterDeleteSlots(CLUSTER_NODE_2, slots);

		verify(con2Mock, times(1)).clusterDelSlots((int[]) anyVararg());
	}

	@Test(expected = IllegalArgumentException.class) // DATAREDIS-315
	public void clusterDeleteSlotShouldThrowExceptionWhenNodeIsNull() {
		connection.clusterDeleteSlots(null, new int[] { 1 });
	}

	@Test // DATAREDIS-315
	public void timeShouldBeExecutedOnArbitraryNode() {

		List<String> values = Arrays.asList("1449655759", "92217");
		when(con1Mock.time()).thenReturn(values);
		when(con2Mock.time()).thenReturn(values);
		when(con3Mock.time()).thenReturn(values);

		connection.time();

		verifyInvocationsAcross("time", times(1), con1Mock, con2Mock, con3Mock);
	}

	@Test // DATAREDIS-679
	public void shouldFailWithUnknownNode() {

		try {
			connection.serverCommands().dbSize(new RedisClusterNode(CLUSTER_HOST, SLAVEOF_NODE_1_PORT));
		} catch (IllegalArgumentException e) {
			assertThat(e.getMessage(),
					containsString("Node " + CLUSTER_HOST + ":" + SLAVEOF_NODE_1_PORT + " is unknown to cluster"));
		}
	}

	@Test // DATAREDIS-679
	public void shouldFailWithAbsentConnection() {

		nodes.remove(CLUSTER_HOST + ":" + MASTER_NODE_3_PORT);

		expectedException.expect(DataAccessResourceFailureException.class);
		expectedException.expectMessage("Node " + CLUSTER_HOST + ":" + MASTER_NODE_3_PORT + " is unknown to cluster");

		connection.serverCommands().dbSize(new RedisClusterNode(CLUSTER_HOST, MASTER_NODE_3_PORT));
	}

	@Test // DATAREDIS-679
	public void shouldReconfigureJedisWithDiscoveredNode() {

		nodes.remove(CLUSTER_HOST + ":" + MASTER_NODE_3_PORT);

		when(connectionHandlerMock.getConnectionFromNode(new HostAndPort(CLUSTER_HOST, MASTER_NODE_3_PORT)))
				.thenReturn(con3Mock);
		when(con3Mock.dbSize()).thenAnswer(new Answer<Long>() {

			@Override
			public Long answer(InvocationOnMock invocation) throws Throwable {

				// Required to return the resource properly after invocation.
				nodes.put(CLUSTER_HOST + ":" + MASTER_NODE_3_PORT, node3PoolMock);

				return 42L;
			}
		});

		Long result = connection.serverCommands().dbSize(new RedisClusterNode(CLUSTER_HOST, MASTER_NODE_3_PORT));

		assertThat(result, is(42L));
	}

	@Test // DATAREDIS-315
	public void timeShouldBeExecutedOnSingleNode() {

		when(con2Mock.time()).thenReturn(Arrays.asList("1449655759", "92217"));

		connection.time(CLUSTER_NODE_2);

		verify(con2Mock, times(1)).time();
		verify(con2Mock, times(1)).close();
		verify(con1Mock, times(1)).clusterNodes();
		verify(con1Mock, times(1)).close();
		verifyZeroInteractions(con1Mock, con3Mock);
	}

	@Test // DATAREDIS-315
	public void resetConfigStatsShouldBeExecutedOnAllNodes() {

		connection.resetConfigStats();

		verify(con1Mock, times(1)).configResetStat();
		verify(con2Mock, times(1)).configResetStat();
		verify(con3Mock, times(1)).configResetStat();
	}

	@Test // DATAREDIS-315
	public void resetConfigStatsShouldBeExecutedOnSingleNodeCorrectly() {

		connection.resetConfigStats(CLUSTER_NODE_2);

		verify(con2Mock, times(1)).configResetStat();
		verify(con2Mock, times(1)).close();
		verify(con1Mock, never()).configResetStat();
		verify(con3Mock, never()).configResetStat();
	}

	@Test // DATAREDIS-315
	public void clusterTopologyProviderShouldCollectErrorsWhenLoadingNodes() {

		expectedException.expect(ClusterStateFailureException.class);
		expectedException.expectMessage("127.0.0.1:7379 failed: o.O");
		expectedException.expectMessage("127.0.0.1:7380 failed: o.1");
		expectedException.expectMessage("127.0.0.1:7381 failed: o.2");

		when(con1Mock.clusterNodes()).thenThrow(new JedisConnectionException("o.O"));
		when(con2Mock.clusterNodes()).thenThrow(new JedisConnectionException("o.1"));
		when(con3Mock.clusterNodes()).thenThrow(new JedisConnectionException("o.2"));

		new JedisClusterTopologyProvider(clusterMock).getTopology();
	}

	@Test // DATAREDIS-603
	public void translatesUnknownExceptions() {

		IllegalArgumentException exception = new IllegalArgumentException("Aw, snap!");

		expectedException.expect(RedisSystemException.class);
		expectedException.expectMessage(exception.getMessage());
		expectedException.expectCause(is(exception));

		doThrow(exception).when(clusterMock).set("foo".getBytes(), "bar".getBytes());

		connection.set("foo".getBytes(), "bar".getBytes());
	}

	static class StubJedisCluster extends JedisCluster {

		JedisClusterConnectionHandler connectionHandler;

		public StubJedisCluster() {
			super(Collections.emptySet());
		}

		JedisClusterConnectionHandler getConnectionHandler() {
			return connectionHandler;
		}

		void setConnectionHandler(JedisClusterConnectionHandler connectionHandler) {
			this.connectionHandler = connectionHandler;
		}
	}
}
