/*
 * Copyright 2015-2021 the original author or authors.
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
import static org.springframework.data.redis.connection.ClusterTestVariables.*;
import static org.springframework.data.redis.test.util.MockitoUtils.*;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisClusterConnectionHandler;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
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
 * @author Chen Guanqun
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class JedisClusterConnectionUnitTests {

	private static final String CLUSTER_NODES_RESPONSE = "" //
			+ MASTER_NODE_1_ID + " " + CLUSTER_HOST + ":" + MASTER_NODE_1_PORT + " myself,master - 0 0 1 connected 0-5460"
			+ "\n" + MASTER_NODE_2_ID + " " + CLUSTER_HOST + ":" + MASTER_NODE_2_PORT
			+ " master - 0 1427718161587 2 connected 5461-10922" + "\n" + MASTER_NODE_2_ID + " " + CLUSTER_HOST + ":"
			+ MASTER_NODE_3_PORT + " master - 0 1427718161587 3 connected 10923-16383";

	private static final String CLUSTER_INFO_RESPONSE = "cluster_state:ok" + "\n" + "cluster_slots_assigned:16384" + "\n"
			+ "cluster_slots_ok:16384" + "\n" + "cluster_slots_pfail:0" + "\n" + "cluster_slots_fail:0" + "\n"
			+ "cluster_known_nodes:4" + "\n" + "cluster_size:3" + "\n" + "cluster_current_epoch:30" + "\n"
			+ "cluster_my_epoch:2" + "\n" + "cluster_stats_messages_sent:2560260" + "\n"
			+ "cluster_stats_messages_received:2560086";

	private JedisClusterConnection connection;

	@Spy StubJedisCluster clusterMock;
	@Mock JedisClusterConnectionHandler connectionHandlerMock;

	@Mock JedisPool node1PoolMock;
	@Mock JedisPool node2PoolMock;
	@Mock JedisPool node3PoolMock;

	@Mock Jedis con1Mock;
	@Mock Jedis con2Mock;
	@Mock Jedis con3Mock;

	private Map<String, JedisPool> nodes = new LinkedHashMap<>();

	@BeforeEach
	void setUp() {

		nodes.put(CLUSTER_HOST + ":" + MASTER_NODE_1_PORT, node1PoolMock);
		nodes.put(CLUSTER_HOST + ":" + MASTER_NODE_2_PORT, node2PoolMock);
		nodes.put(CLUSTER_HOST + ":" + MASTER_NODE_3_PORT, node3PoolMock);

		when(clusterMock.getClusterNodes()).thenReturn(nodes);
		when(node1PoolMock.getResource()).thenReturn(con1Mock);
		when(node2PoolMock.getResource()).thenReturn(con2Mock);
		when(node3PoolMock.getResource()).thenReturn(con3Mock);

		when(con1Mock.clusterNodes()).thenReturn(CLUSTER_NODES_RESPONSE);
		when(con2Mock.clusterNodes()).thenReturn(CLUSTER_NODES_RESPONSE);
		when(con3Mock.clusterNodes()).thenReturn(CLUSTER_NODES_RESPONSE);
		clusterMock.setConnectionHandler(connectionHandlerMock);

		connection = new JedisClusterConnection(clusterMock);
	}

	@Test // DATAREDIS-315
	void throwsExceptionWhenClusterCommandExecutorIsNull() {
		assertThatIllegalArgumentException().isThrownBy(() -> new JedisClusterConnection(clusterMock, null));
	}

	@Test // DATAREDIS-315
	void clusterMeetShouldSendCommandsToExistingNodesCorrectly() {

		connection.clusterMeet(UNKNOWN_CLUSTER_NODE);

		verify(con1Mock, times(1)).clusterMeet(UNKNOWN_CLUSTER_NODE.getHost(), UNKNOWN_CLUSTER_NODE.getPort());
		verify(con2Mock, times(1)).clusterMeet(UNKNOWN_CLUSTER_NODE.getHost(), UNKNOWN_CLUSTER_NODE.getPort());
		verify(con2Mock, times(1)).clusterMeet(UNKNOWN_CLUSTER_NODE.getHost(), UNKNOWN_CLUSTER_NODE.getPort());
	}

	@Test // DATAREDIS-315
	void clusterMeetShouldThrowExceptionWhenNodeIsNull() {
		assertThatIllegalArgumentException().isThrownBy(() -> connection.clusterMeet(null));
	}

	@Test // DATAREDIS-315, DATAREDIS-890
	void clusterForgetShouldSendCommandsToRemainingNodesCorrectly() {

		connection.clusterForget(CLUSTER_NODE_2);

		verify(con1Mock, times(1)).clusterForget(CLUSTER_NODE_2.getId());
		verify(con3Mock, times(1)).clusterForget(CLUSTER_NODE_2.getId());
	}

	@Test // DATAREDIS-315, DATAREDIS-890
	void clusterReplicateShouldSendCommandsCorrectly() {

		connection.clusterReplicate(CLUSTER_NODE_1, CLUSTER_NODE_2);

		verify(con2Mock, times(1)).clusterReplicate(CLUSTER_NODE_1.getId());
		verify(con1Mock, atMost(1)).clusterNodes();
		verify(con1Mock, atMost(1)).close();
		verifyNoMoreInteractions(con1Mock);
	}

	@Test // DATAREDIS-315
	void closeShouldNotCloseUnderlyingClusterPool() throws IOException {

		connection.close();

		verify(clusterMock, never()).close();
	}

	@Test // DATAREDIS-315
	void isClosedShouldReturnConnectionStateCorrectly() {

		assertThat(connection.isClosed()).isFalse();

		connection.close();

		assertThat(connection.isClosed()).isTrue();
	}

	@Test // DATAREDIS-315
	void clusterInfoShouldBeReturnedCorrectly() {

		when(con1Mock.clusterInfo()).thenReturn(CLUSTER_INFO_RESPONSE);
		when(con2Mock.clusterInfo()).thenReturn(CLUSTER_INFO_RESPONSE);
		when(con3Mock.clusterInfo()).thenReturn(CLUSTER_INFO_RESPONSE);

		ClusterInfo p = connection.clusterGetClusterInfo();
		assertThat(p.getSlotsAssigned()).isEqualTo(16384L);

		verifyInvocationsAcross("clusterInfo", times(1), con1Mock, con2Mock, con3Mock);
	}

	@Test // DATAREDIS-315
	void clusterSetSlotImportingShouldBeExecutedCorrectly() {

		connection.clusterSetSlot(CLUSTER_NODE_1, 100, AddSlots.IMPORTING);

		verify(con1Mock, times(1)).clusterSetSlotImporting(eq(100), eq(CLUSTER_NODE_1.getId()));
	}

	@Test // DATAREDIS-315
	void clusterSetSlotMigratingShouldBeExecutedCorrectly() {

		connection.clusterSetSlot(CLUSTER_NODE_1, 100, AddSlots.MIGRATING);

		verify(con1Mock, times(1)).clusterSetSlotMigrating(eq(100), eq(CLUSTER_NODE_1.getId()));
	}

	@Test // DATAREDIS-315
	void clusterSetSlotStableShouldBeExecutedCorrectly() {

		connection.clusterSetSlot(CLUSTER_NODE_1, 100, AddSlots.STABLE);

		verify(con1Mock, times(1)).clusterSetSlotStable(eq(100));
	}

	@Test // DATAREDIS-315
	void clusterSetSlotNodeShouldBeExecutedCorrectly() {

		connection.clusterSetSlot(CLUSTER_NODE_1, 100, AddSlots.NODE);

		verify(con1Mock, times(1)).clusterSetSlotNode(eq(100), eq(CLUSTER_NODE_1.getId()));
	}

	@Test // DATAREDIS-315
	void clusterSetSlotShouldBeExecutedOnTargetNodeWhenNodeIdNotSet() {

		connection.clusterSetSlot(new RedisClusterNode(CLUSTER_HOST, MASTER_NODE_2_PORT), 100, AddSlots.IMPORTING);

		verify(con2Mock, times(1)).clusterSetSlotImporting(eq(100), eq(CLUSTER_NODE_2.getId()));
	}

	@Test // DATAREDIS-315
	void clusterSetSlotShouldThrowExceptionWhenModeIsNull() {
		assertThatIllegalArgumentException().isThrownBy(() -> connection.clusterSetSlot(CLUSTER_NODE_1, 100, null));
	}

	@Test // DATAREDIS-315
	void clusterDeleteSlotsShouldBeExecutedCorrectly() {

		int[] slots = new int[] { 9000, 10000 };
		connection.clusterDeleteSlots(CLUSTER_NODE_2, slots);

		verify(con2Mock, times(1)).clusterDelSlots((int[]) any());
	}

	@Test // DATAREDIS-315
	void clusterDeleteSlotShouldThrowExceptionWhenNodeIsNull() {
		assertThatIllegalArgumentException().isThrownBy(() -> connection.clusterDeleteSlots(null, new int[] { 1 }));
	}

	@Test // DATAREDIS-315
	void timeShouldBeExecutedOnArbitraryNode() {

		List<String> values = Arrays.asList("1449655759", "92217");
		when(con1Mock.time()).thenReturn(values);
		when(con2Mock.time()).thenReturn(values);
		when(con3Mock.time()).thenReturn(values);

		connection.time();

		verifyInvocationsAcross("time", times(1), con1Mock, con2Mock, con3Mock);
	}

	@Test // DATAREDIS-679
	void shouldFailWithUnknownNode() {

		try {
			connection.serverCommands().dbSize(new RedisClusterNode(CLUSTER_HOST, SLAVEOF_NODE_1_PORT));
		} catch (IllegalArgumentException e) {
			assertThat(e.getMessage())
					.contains("Node " + CLUSTER_HOST + ":" + SLAVEOF_NODE_1_PORT + " is unknown to cluster");
		}
	}

	@Test // DATAREDIS-679
	void shouldFailWithAbsentConnection() {

		nodes.remove(CLUSTER_HOST + ":" + MASTER_NODE_3_PORT);

		assertThatExceptionOfType(DataAccessResourceFailureException.class)
				.isThrownBy(() -> connection.serverCommands().dbSize(new RedisClusterNode(CLUSTER_HOST, MASTER_NODE_3_PORT)))
				.withMessageContaining("Node " + CLUSTER_HOST + ":" + MASTER_NODE_3_PORT + " is unknown to cluster");
	}

	@Test // DATAREDIS-679
	void shouldReconfigureJedisWithDiscoveredNode() {

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

		assertThat(result).isEqualTo(42L);
	}

	@Test // DATAREDIS-315, DATAREDIS-890
	void timeShouldBeExecutedOnSingleNode() {

		when(con2Mock.time()).thenReturn(Arrays.asList("1449655759", "92217"));

		connection.time(CLUSTER_NODE_2);

		verify(con2Mock, times(1)).time();
		verify(con2Mock, atLeast(1)).close();
		verify(con1Mock, atMost(1)).clusterNodes();
		verify(con1Mock, atMost(1)).close();

	}

	@Test // DATAREDIS-315
	void resetConfigStatsShouldBeExecutedOnAllNodes() {

		connection.resetConfigStats();

		verify(con1Mock, times(1)).configResetStat();
		verify(con2Mock, times(1)).configResetStat();
		verify(con3Mock, times(1)).configResetStat();
	}

	@Test // DATAREDIS-315, DATAREDIS-890
	void resetConfigStatsShouldBeExecutedOnSingleNodeCorrectly() {

		connection.resetConfigStats(CLUSTER_NODE_2);

		verify(con2Mock, times(1)).configResetStat();
		verify(con2Mock, atLeast(1)).close();
		verify(con1Mock, never()).configResetStat();
		verify(con3Mock, never()).configResetStat();
	}

	@Test // GH-1992
	void rewriteConfigShouldBeExecutedOnAllNodes() {

		connection.rewriteConfig();

		verify(con1Mock, times(1)).configRewrite();
		verify(con2Mock, times(1)).configRewrite();
		verify(con3Mock, times(1)).configRewrite();
	}

	@Test // GH-1992
	void rewriteConfigShouldBeExecutedOnSingleNodeCorrectly() {

		connection.rewriteConfig(CLUSTER_NODE_2);

		verify(con2Mock, times(1)).configRewrite();
		verify(con2Mock, atLeast(1)).close();
		verify(con1Mock, never()).configRewrite();
		verify(con3Mock, never()).configRewrite();
	}

	@Test // DATAREDIS-315
	void clusterTopologyProviderShouldCollectErrorsWhenLoadingNodes() {

		when(con1Mock.clusterNodes()).thenThrow(new JedisConnectionException("o.O"));
		when(con2Mock.clusterNodes()).thenThrow(new JedisConnectionException("o.1"));
		when(con3Mock.clusterNodes()).thenThrow(new JedisConnectionException("o.2"));

		assertThatExceptionOfType(ClusterStateFailureException.class)
				.isThrownBy(() -> new JedisClusterTopologyProvider(clusterMock).getTopology())
				.withMessageContaining("127.0.0.1:7379 failed: o.O").withMessageContaining("127.0.0.1:7380 failed: o.1");
	}

	@Test // DATAREDIS-603
	void translatesUnknownExceptions() {

		IllegalArgumentException exception = new IllegalArgumentException("Aw, snap!");

		doThrow(exception).when(clusterMock).set("foo".getBytes(), "bar".getBytes());

		assertThatExceptionOfType(RedisSystemException.class)
				.isThrownBy(() -> connection.set("foo".getBytes(), "bar".getBytes()))
				.withMessageContaining(exception.getMessage()).withCause(exception);
	}

	@Test // DATAREDIS-794
	void clusterTopologyProviderShouldUseCachedTopology() {

		when(clusterMock.getClusterNodes()).thenReturn(Collections.singletonMap("mock", node1PoolMock));
		when(con1Mock.clusterNodes()).thenReturn(CLUSTER_NODES_RESPONSE);

		JedisClusterTopologyProvider provider = new JedisClusterTopologyProvider(clusterMock, Duration.ofSeconds(5));
		provider.getTopology();
		provider.getTopology();

		verify(con1Mock).clusterNodes();
	}

	@Test // DATAREDIS-794
	void clusterTopologyProviderShouldRequestTopology() {

		when(clusterMock.getClusterNodes()).thenReturn(Collections.singletonMap("mock", node1PoolMock));
		when(con1Mock.clusterNodes()).thenReturn(CLUSTER_NODES_RESPONSE);

		JedisClusterTopologyProvider provider = new JedisClusterTopologyProvider(clusterMock, Duration.ZERO);
		provider.getTopology();
		provider.getTopology();

		verify(con1Mock, times(2)).clusterNodes();
	}

	@Test // GH-1985
	void nodeWithoutHostShouldRejectConnectionAttempt() {

		reset(con1Mock, con2Mock, con3Mock);

		when(con1Mock.clusterNodes())
				.thenReturn("ef570f86c7b1a953846668debc177a3a16733420 :6379 fail,master - 0 0 1 connected");
		when(con2Mock.clusterNodes())
				.thenReturn("ef570f86c7b1a953846668debc177a3a16733420 :6379 fail,master - 0 0 1 connected");
		when(con3Mock.clusterNodes())
				.thenReturn("ef570f86c7b1a953846668debc177a3a16733420 :6379 fail,master - 0 0 1 connected");

		JedisClusterConnection connection = new JedisClusterConnection(clusterMock);

		assertThatThrownBy(() -> connection.ping(new RedisClusterNode("ef570f86c7b1a953846668debc177a3a16733420")))
				.isInstanceOf(DataAccessResourceFailureException.class)
				.hasMessageContaining("ef570f86c7b1a953846668debc177a3a16733420");
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
