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
package org.springframework.data.redis.connection.lettuce;

import static org.hamcrest.core.AnyOf.*;
import static org.hamcrest.core.Is.*;
import static org.hamcrest.core.IsNull.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static org.springframework.data.redis.connection.ClusterTestVariables.*;
import static org.springframework.data.redis.test.util.MockitoUtils.*;

import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;
import io.lettuce.core.cluster.models.partitions.Partitions;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode.NodeFlag;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.data.redis.connection.ClusterCommandExecutor;
import org.springframework.data.redis.connection.ClusterNodeResourceProvider;
import org.springframework.data.redis.connection.RedisClusterCommands.AddSlots;
import org.springframework.data.redis.connection.RedisClusterNode;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@RunWith(MockitoJUnitRunner.Silent.class)
public class LettuceClusterConnectionUnitTests {

	static final byte[] KEY_1_BYTES = KEY_1.getBytes();

	static final byte[] VALUE_1_BYTES = VALUE_1.getBytes();

	static final byte[] KEY_2_BYTES = KEY_2.getBytes();

	static final byte[] KEY_3_BYTES = KEY_3.getBytes();

	@Mock RedisClusterClient clusterMock;

	@Mock LettuceConnectionProvider connectionProviderMock;
	@Mock ClusterCommandExecutor executorMock;
	@Mock ClusterNodeResourceProvider resourceProvider;
	@Mock StatefulRedisClusterConnection<byte[], byte[]> sharedConnectionMock;
	@Mock RedisClusterAsyncCommands<byte[], byte[]> dedicatedConnectionMock;
	@Mock RedisClusterCommands<byte[], byte[]> clusterConnection1Mock;
	@Mock RedisClusterCommands<byte[], byte[]> clusterConnection2Mock;
	@Mock RedisClusterCommands<byte[], byte[]> clusterConnection3Mock;

	LettuceClusterConnection connection;

	@Before
	public void setUp() {

		Partitions partitions = new Partitions();

		io.lettuce.core.cluster.models.partitions.RedisClusterNode partition1 = new io.lettuce.core.cluster.models.partitions.RedisClusterNode();
		partition1.setNodeId(CLUSTER_NODE_1.getId());
		partition1.setConnected(true);
		partition1.setFlags(Collections.singleton(NodeFlag.MASTER));
		partition1.setUri(RedisURI.create("redis://" + CLUSTER_HOST + ":" + MASTER_NODE_1_PORT));

		io.lettuce.core.cluster.models.partitions.RedisClusterNode partition2 = new io.lettuce.core.cluster.models.partitions.RedisClusterNode();
		partition2.setNodeId(CLUSTER_NODE_2.getId());
		partition2.setConnected(true);
		partition2.setFlags(Collections.singleton(NodeFlag.MASTER));
		partition2.setUri(RedisURI.create("redis://" + CLUSTER_HOST + ":" + MASTER_NODE_2_PORT));

		io.lettuce.core.cluster.models.partitions.RedisClusterNode partition3 = new io.lettuce.core.cluster.models.partitions.RedisClusterNode();
		partition3.setNodeId(CLUSTER_NODE_3.getId());
		partition3.setConnected(true);
		partition3.setFlags(Collections.singleton(NodeFlag.MASTER));
		partition3.setUri(RedisURI.create("redis://" + CLUSTER_HOST + ":" + MASTER_NODE_3_PORT));

		partitions.addPartition(partition1);
		partitions.addPartition(partition2);
		partitions.addPartition(partition3);

		when(resourceProvider.getResourceForSpecificNode(CLUSTER_NODE_1)).thenReturn(clusterConnection1Mock);
		when(resourceProvider.getResourceForSpecificNode(CLUSTER_NODE_2)).thenReturn(clusterConnection2Mock);
		when(resourceProvider.getResourceForSpecificNode(CLUSTER_NODE_3)).thenReturn(clusterConnection3Mock);

		when(clusterMock.getPartitions()).thenReturn(partitions);

		ClusterCommandExecutor executor = new ClusterCommandExecutor(new LettuceClusterTopologyProvider(clusterMock),
				resourceProvider, LettuceClusterConnection.exceptionConverter);

		connection = new LettuceClusterConnection(clusterMock, executor) {

			@Override
			protected RedisClusterAsyncCommands<byte[], byte[]> getAsyncDedicatedConnection() {
				return dedicatedConnectionMock;
			}

			@Override
			public List<RedisClusterNode> clusterGetNodes() {
				return Arrays.asList(CLUSTER_NODE_1, CLUSTER_NODE_2, CLUSTER_NODE_3);
			}
		};
	}

	@Test(expected = IllegalArgumentException.class) // DATAREDIS-315
	public void thowsExceptionWhenClusterCommandExecturorIsNull() {
		new LettuceClusterConnection(clusterMock, null);
	}

	@Test // DATAREDIS-315
	public void clusterMeetShouldSendCommandsToExistingNodesCorrectly() {

		connection.clusterMeet(UNKNOWN_CLUSTER_NODE);

		verify(clusterConnection1Mock, times(1)).clusterMeet(UNKNOWN_CLUSTER_NODE.getHost(),
				UNKNOWN_CLUSTER_NODE.getPort());
		verify(clusterConnection2Mock, times(1)).clusterMeet(UNKNOWN_CLUSTER_NODE.getHost(),
				UNKNOWN_CLUSTER_NODE.getPort());
		verify(clusterConnection3Mock, times(1)).clusterMeet(UNKNOWN_CLUSTER_NODE.getHost(),
				UNKNOWN_CLUSTER_NODE.getPort());
	}

	@Test(expected = IllegalArgumentException.class) // DATAREDIS-315
	public void clusterMeetShouldThrowExceptionWhenNodeIsNull() {
		connection.clusterMeet(null);
	}

	@Test // DATAREDIS-315
	public void clusterForgetShouldSendCommandsToRemainingNodesCorrectly() {

		connection.clusterForget(CLUSTER_NODE_2);

		verify(clusterConnection1Mock, times(1)).clusterForget(CLUSTER_NODE_2.getId());
		verifyZeroInteractions(clusterConnection2Mock);
		verify(clusterConnection3Mock, times(1)).clusterForget(CLUSTER_NODE_2.getId());
	}

	@Test // DATAREDIS-315
	public void clusterReplicateShouldSendCommandsCorrectly() {

		connection.clusterReplicate(CLUSTER_NODE_1, CLUSTER_NODE_2);

		verify(clusterConnection2Mock, times(1)).clusterReplicate(CLUSTER_NODE_1.getId());
		verifyZeroInteractions(clusterConnection1Mock);
	}

	@Test // DATAREDIS-315
	public void closeShouldNotCloseUnderlyingClusterPool() throws IOException {

		connection.close();

		verify(clusterMock, never()).shutdown();
	}

	@Test // DATAREDIS-315
	public void isClosedShouldReturnConnectionStateCorrectly() {

		assertThat(connection.isClosed(), is(false));

		connection.close();

		assertThat(connection.isClosed(), is(true));
	}

	@Test // DATAREDIS-315
	public void keysShouldBeRunOnAllClusterNodes() {

		when(clusterConnection1Mock.keys(any(byte[].class))).thenReturn(Collections.<byte[]> emptyList());
		when(clusterConnection2Mock.keys(any(byte[].class))).thenReturn(Collections.<byte[]> emptyList());
		when(clusterConnection3Mock.keys(any(byte[].class))).thenReturn(Collections.<byte[]> emptyList());

		byte[] pattern = LettuceConverters.toBytes("*");

		connection.keys(pattern);

		verify(clusterConnection1Mock, times(1)).keys(pattern);
		verify(clusterConnection2Mock, times(1)).keys(pattern);
		verify(clusterConnection3Mock, times(1)).keys(pattern);
	}

	@Test // DATAREDIS-315
	public void keysShouldOnlyBeRunOnDedicatedNodeWhenPinned() {

		when(clusterConnection2Mock.keys(any(byte[].class))).thenReturn(Collections.<byte[]> emptyList());

		byte[] pattern = LettuceConverters.toBytes("*");

		connection.keys(CLUSTER_NODE_2, pattern);

		verify(clusterConnection1Mock, never()).keys(pattern);
		verify(clusterConnection2Mock, times(1)).keys(pattern);
		verify(clusterConnection3Mock, never()).keys(pattern);
	}

	@Test // DATAREDIS-315
	public void randomKeyShouldReturnAnyKeyFromRandomNode() {

		when(clusterConnection1Mock.randomkey()).thenReturn(KEY_1_BYTES);
		when(clusterConnection2Mock.randomkey()).thenReturn(KEY_2_BYTES);
		when(clusterConnection3Mock.randomkey()).thenReturn(KEY_3_BYTES);

		assertThat(connection.randomKey(), anyOf(is(KEY_1_BYTES), is(KEY_2_BYTES), is(KEY_3_BYTES)));
		verifyInvocationsAcross("randomkey", times(1), clusterConnection1Mock, clusterConnection2Mock,
				clusterConnection3Mock);
	}

	@Test // DATAREDIS-315
	public void randomKeyShouldReturnKeyWhenAvailableOnAnyNode() {

		when(clusterConnection3Mock.randomkey()).thenReturn(KEY_3_BYTES);

		for (int i = 0; i < 100; i++) {
			assertThat(connection.randomKey(), is(KEY_3_BYTES));
		}
	}

	@Test // DATAREDIS-315
	public void randomKeyShouldReturnNullWhenNoKeysPresentOnAllNodes() {

		when(clusterConnection1Mock.randomkey()).thenReturn(null);
		when(clusterConnection2Mock.randomkey()).thenReturn(null);
		when(clusterConnection3Mock.randomkey()).thenReturn(null);

		assertThat(connection.randomKey(), nullValue());
	}

	@Test // DATAREDIS-315
	public void clusterSetSlotImportingShouldBeExecutedCorrectly() {

		connection.clusterSetSlot(CLUSTER_NODE_1, 100, AddSlots.IMPORTING);

		verify(clusterConnection1Mock, times(1)).clusterSetSlotImporting(eq(100), eq(CLUSTER_NODE_1.getId()));
	}

	@Test // DATAREDIS-315
	public void clusterSetSlotMigratingShouldBeExecutedCorrectly() {

		connection.clusterSetSlot(CLUSTER_NODE_1, 100, AddSlots.MIGRATING);

		verify(clusterConnection1Mock, times(1)).clusterSetSlotMigrating(eq(100), eq(CLUSTER_NODE_1.getId()));
	}

	@Test // DATAREDIS-315
	public void clusterSetSlotStableShouldBeExecutedCorrectly() {

		connection.clusterSetSlot(CLUSTER_NODE_1, 100, AddSlots.STABLE);

		verify(clusterConnection1Mock, times(1)).clusterSetSlotStable(eq(100));
	}

	@Test // DATAREDIS-315
	public void clusterSetSlotNodeShouldBeExecutedCorrectly() {

		connection.clusterSetSlot(CLUSTER_NODE_1, 100, AddSlots.NODE);

		verify(clusterConnection1Mock, times(1)).clusterSetSlotNode(eq(100), eq(CLUSTER_NODE_1.getId()));
	}

	@Test // DATAREDIS-315
	public void clusterSetSlotShouldBeExecutedOnTargetNodeWhenNodeIdNotSet() {

		connection.clusterSetSlot(new RedisClusterNode(CLUSTER_HOST, MASTER_NODE_2_PORT), 100, AddSlots.IMPORTING);

		verify(clusterConnection2Mock, times(1)).clusterSetSlotImporting(eq(100), eq(CLUSTER_NODE_2.getId()));
	}

	@Test(expected = IllegalArgumentException.class) // DATAREDIS-315
	public void clusterSetSlotShouldThrowExceptionWhenModeIsNull() {
		connection.clusterSetSlot(CLUSTER_NODE_1, 100, null);
	}

	@Test // DATAREDIS-315
	public void clusterDeleteSlotsShouldBeExecutedCorrectly() {

		int[] slots = new int[] { 9000, 10000 };
		connection.clusterDeleteSlots(CLUSTER_NODE_2, slots);

		verify(clusterConnection2Mock, times(1)).clusterDelSlots((int[]) any());
	}

	@Test(expected = IllegalArgumentException.class) // DATAREDIS-315
	public void clusterDeleteSlotShouldThrowExceptionWhenNodeIsNull() {
		connection.clusterDeleteSlots(null, new int[] { 1 });
	}

	@Test // DATAREDIS-315
	public void timeShouldBeExecutedOnArbitraryNode() {

		List<byte[]> values = Arrays.asList("1449655759".getBytes(), "92217".getBytes());
		when(clusterConnection1Mock.time()).thenReturn(values);
		when(clusterConnection2Mock.time()).thenReturn(values);
		when(clusterConnection3Mock.time()).thenReturn(values);

		connection.time();

		verifyInvocationsAcross("time", times(1), clusterConnection1Mock, clusterConnection2Mock, clusterConnection3Mock);
	}

	@Test // DATAREDIS-315
	public void timeShouldBeExecutedOnSingleNode() {

		when(clusterConnection2Mock.time()).thenReturn(Arrays.asList("1449655759".getBytes(), "92217".getBytes()));

		connection.time(CLUSTER_NODE_2);

		verify(clusterConnection2Mock, times(1)).time();
		verifyZeroInteractions(clusterConnection1Mock, clusterConnection3Mock);
	}

	@Test // DATAREDIS-315
	public void resetConfigStatsShouldBeExecutedOnAllNodes() {

		connection.resetConfigStats();

		verify(clusterConnection1Mock, times(1)).configResetstat();
		verify(clusterConnection2Mock, times(1)).configResetstat();
		verify(clusterConnection3Mock, times(1)).configResetstat();
	}

	@Test // DATAREDIS-315
	public void resetConfigStatsShouldBeExecutedOnSingleNodeCorrectly() {

		connection.resetConfigStats(CLUSTER_NODE_2);

		verify(clusterConnection2Mock, times(1)).configResetstat();
		verify(clusterConnection1Mock, never()).configResetstat();
		verify(clusterConnection1Mock, never()).configResetstat();
	}

	@Test // DATAREDIS-731, DATAREDIS-545
	public void shouldExecuteOnSharedConnection() {

		RedisAdvancedClusterCommands<byte[], byte[]> sync = mock(RedisAdvancedClusterCommands.class);

		when(sharedConnectionMock.sync()).thenReturn(sync);

		LettuceClusterConnection connection = new LettuceClusterConnection(sharedConnectionMock, connectionProviderMock,
				clusterMock, executorMock, Duration.ZERO);

		connection.keyCommands().del(KEY_1_BYTES);

		verify(sync).del(KEY_1_BYTES);
		verifyNoMoreInteractions(connectionProviderMock);
	}

	@Test // DATAREDIS-731, DATAREDIS-545
	public void shouldExecuteOnDedicatedConnection() {

		RedisCommands<byte[], byte[]> sync = mock(RedisCommands.class);
		StatefulRedisConnection<byte[], byte[]> dedicatedConnection = mock(StatefulRedisConnection.class);

		when(connectionProviderMock.getConnection(StatefulConnection.class)).thenReturn(dedicatedConnection);
		when(dedicatedConnection.sync()).thenReturn(sync);

		LettuceClusterConnection connection = new LettuceClusterConnection(sharedConnectionMock, connectionProviderMock,
				clusterMock, executorMock, Duration.ZERO);

		connection.listCommands().bLPop(1, KEY_1_BYTES);

		verify(sync).blpop(1, KEY_1_BYTES);
		verify(connectionProviderMock).getConnection(StatefulConnection.class);
		verifyNoMoreInteractions(connectionProviderMock);
		verifyZeroInteractions(sharedConnectionMock);
	}
}
