/*
 * Copyright 2015 the original author or authors.
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

import static org.hamcrest.core.Is.*;
import static org.junit.Assert.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;
import static org.springframework.data.redis.connection.ClusterTestVariables.*;
import static org.springframework.data.redis.test.util.MockitoUtils.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.data.redis.ClusterStateFailureException;
import org.springframework.data.redis.connection.ClusterInfo;
import org.springframework.data.redis.connection.RedisClusterCommands.AddSlots;
import org.springframework.data.redis.connection.RedisClusterNode;
import org.springframework.data.redis.connection.jedis.JedisClusterConnection.JedisClusterTopologyProvider;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisConnectionException;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@RunWith(MockitoJUnitRunner.class)
public class JedisClusterConnectionUnitTests {

	private static final String CLUSTER_NODES_RESPONSE = "" //
			+ MASTER_NODE_1_ID + " " + CLUSTER_HOST + ":" + MASTER_NODE_1_PORT
			+ " myself,master - 0 0 1 connected 0-5460"
			+ "\n" + MASTER_NODE_2_ID + " " + CLUSTER_HOST + ":"
			+ MASTER_NODE_2_PORT
			+ " master - 0 1427718161587 2 connected 5461-10922" + "\n"
			+ MASTER_NODE_2_ID
			+ " " + CLUSTER_HOST + ":" + MASTER_NODE_3_PORT + " master - 0 1427718161587 3 connected 10923-16383";

	static final String CLUSTER_INFO_RESPONSE = "cluster_state:ok" + "\n"
			+ "cluster_slots_assigned:16384" + "\n" + "cluster_slots_ok:16384"
			+ "\n" + "cluster_slots_pfail:0" + "\n"
			+ "cluster_slots_fail:0" + "\n" + "cluster_known_nodes:4"
			+ "\n" + "cluster_size:3" + "\n"
			+ "cluster_current_epoch:30" + "\n" + "cluster_my_epoch:2"
			+ "\n" + "cluster_stats_messages_sent:2560260"
			+ "\n" + "cluster_stats_messages_received:2560086";

	JedisClusterConnection connection;

	@Mock JedisCluster clusterMock;

	@Mock JedisPool node1PoolMock;
	@Mock JedisPool node2PoolMock;
	@Mock JedisPool node3PoolMock;

	@Mock Jedis con1Mock;
	@Mock Jedis con2Mock;
	@Mock Jedis con3Mock;

	public @Rule ExpectedException expectedException = ExpectedException.none();

	@Before
	public void setUp() {

		Map<String, JedisPool> nodes = new LinkedHashMap<String, JedisPool>(3);
		nodes.put(CLUSTER_HOST + ":" + MASTER_NODE_1_PORT, node1PoolMock);
		nodes.put(CLUSTER_HOST + ":" + MASTER_NODE_2_PORT, node2PoolMock);
		nodes.put(CLUSTER_HOST + ":" + MASTER_NODE_3_PORT, node3PoolMock);

		when(clusterMock.getClusterNodes()).thenReturn(nodes);
		when(node1PoolMock.getResource()).thenReturn(con1Mock);
		when(node2PoolMock.getResource()).thenReturn(con2Mock);
		when(node3PoolMock.getResource()).thenReturn(con3Mock);

		when(con1Mock.clusterNodes()).thenReturn(CLUSTER_NODES_RESPONSE);

		connection = new JedisClusterConnection(clusterMock);
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void thowsExceptionWhenClusterCommandExecturorIsNull() {

		expectedException.expect(IllegalArgumentException.class);

		new JedisClusterConnection(clusterMock, null);
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void clusterMeetShouldSendCommandsToExistingNodesCorrectly() {

		connection.clusterMeet(UNKNOWN_CLUSTER_NODE);

		verify(con1Mock, times(1)).clusterMeet(UNKNOWN_CLUSTER_NODE.getHost(), UNKNOWN_CLUSTER_NODE.getPort());
		verify(con2Mock, times(1)).clusterMeet(UNKNOWN_CLUSTER_NODE.getHost(), UNKNOWN_CLUSTER_NODE.getPort());
		verify(con2Mock, times(1)).clusterMeet(UNKNOWN_CLUSTER_NODE.getHost(), UNKNOWN_CLUSTER_NODE.getPort());
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void clusterMeetShouldThrowExceptionWhenNodeIsNull() {

		expectedException.expect(IllegalArgumentException.class);

		connection.clusterMeet(null);
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void clusterForgetShouldSendCommandsToRemainingNodesCorrectly() {

		connection.clusterForget(CLUSTER_NODE_2);

		verify(con1Mock, times(1)).clusterForget(CLUSTER_NODE_2.getId());
		verifyZeroInteractions(con2Mock);
		verify(con3Mock, times(1)).clusterForget(CLUSTER_NODE_2.getId());
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void clusterReplicateShouldSendCommandsCorrectly() {

		connection.clusterReplicate(CLUSTER_NODE_1, CLUSTER_NODE_2);

		verify(con2Mock, times(1)).clusterReplicate(CLUSTER_NODE_1.getId());
		verify(con1Mock, times(1)).clusterNodes();
		verifyZeroInteractions(con1Mock);
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void closeShouldNotCloseUnderlyingClusterPool() throws IOException {

		connection.close();

		verify(clusterMock, never()).close();
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void isClosedShouldReturnConnectionStateCorrectly() {

		assertThat(connection.isClosed(), is(false));

		connection.close();

		assertThat(connection.isClosed(), is(true));
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void clusterInfoShouldBeReturnedCorrectly() {

		when(con1Mock.clusterInfo()).thenReturn(CLUSTER_INFO_RESPONSE);
		when(con2Mock.clusterInfo()).thenReturn(CLUSTER_INFO_RESPONSE);
		when(con3Mock.clusterInfo()).thenReturn(CLUSTER_INFO_RESPONSE);

		ClusterInfo p = connection.clusterGetClusterInfo();
		assertThat(p.getSlotsAssigned(), is(16384L));

		verifyInvocationsAcross("clusterInfo", times(1), con1Mock, con2Mock, con3Mock);
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void clusterSetSlotImportingShouldBeExecutedCorrectly() {

		connection.clusterSetSlot(CLUSTER_NODE_1, 100, AddSlots.IMPORTING);

		verify(con1Mock, times(1)).clusterSetSlotImporting(eq(100), eq(CLUSTER_NODE_1.getId()));
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void clusterSetSlotMigratingShouldBeExecutedCorrectly() {

		connection.clusterSetSlot(CLUSTER_NODE_1, 100, AddSlots.MIGRATING);

		verify(con1Mock, times(1)).clusterSetSlotMigrating(eq(100), eq(CLUSTER_NODE_1.getId()));
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void clusterSetSlotStableShouldBeExecutedCorrectly() {

		connection.clusterSetSlot(CLUSTER_NODE_1, 100, AddSlots.STABLE);

		verify(con1Mock, times(1)).clusterSetSlotStable(eq(100));
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void clusterSetSlotNodeShouldBeExecutedCorrectly() {

		connection.clusterSetSlot(CLUSTER_NODE_1, 100, AddSlots.NODE);

		verify(con1Mock, times(1)).clusterSetSlotNode(eq(100), eq(CLUSTER_NODE_1.getId()));
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void clusterSetSlotShouldBeExecutedOnTargetNodeWhenNodeIdNotSet() {

		connection.clusterSetSlot(new RedisClusterNode(CLUSTER_HOST, MASTER_NODE_2_PORT), 100, AddSlots.IMPORTING);

		verify(con2Mock, times(1)).clusterSetSlotImporting(eq(100), eq(CLUSTER_NODE_2.getId()));
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test(expected = IllegalArgumentException.class)
	public void clusterSetSlotShouldThrowExceptionWhenModeIsNull() {
		connection.clusterSetSlot(CLUSTER_NODE_1, 100, null);
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void clusterDeleteSlotsShouldBeExecutedCorrectly() {

		int[] slots = new int[] { 9000, 10000 };
		connection.clusterDeleteSlots(CLUSTER_NODE_2, slots);

		verify(con2Mock, times(1)).clusterDelSlots((int[]) anyVararg());
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test(expected = IllegalArgumentException.class)
	public void clusterDeleteSlotShouldThrowExceptionWhenNodeIsNull() {
		connection.clusterDeleteSlots(null, new int[] { 1 });
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void timeShouldBeExecutedOnArbitraryNode() {

		List<String> values = Arrays.asList("1449655759", "92217");
		when(con1Mock.time()).thenReturn(values);
		when(con2Mock.time()).thenReturn(values);
		when(con3Mock.time()).thenReturn(values);

		connection.time();

		verifyInvocationsAcross("time", times(1), con1Mock, con2Mock, con3Mock);
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void timeShouldBeExecutedOnSingleNode() {

		when(con2Mock.time()).thenReturn(Arrays.asList("1449655759", "92217"));

		connection.time(CLUSTER_NODE_2);

		verify(con2Mock, times(1)).time();
		verify(con1Mock, times(1)).clusterNodes();
		verifyZeroInteractions(con1Mock, con3Mock);
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void resetConfigStatsShouldBeExecutedOnAllNodes() {

		connection.resetConfigStats();

		verify(con1Mock, times(1)).configResetStat();
		verify(con2Mock, times(1)).configResetStat();
		verify(con3Mock, times(1)).configResetStat();
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void resetConfigStatsShouldBeExecutedOnSingleNodeCorrectly() {

		connection.resetConfigStats(CLUSTER_NODE_2);

		verify(con2Mock, times(1)).configResetStat();
		verify(con1Mock, never()).configResetStat();
		verify(con3Mock, never()).configResetStat();
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
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
}
