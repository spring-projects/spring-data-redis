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
package org.springframework.data.redis.connection.lettuce;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.springframework.data.redis.connection.ClusterTestVariables.*;
import static org.springframework.data.redis.test.util.MockitoUtils.*;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;
import io.lettuce.core.cluster.models.partitions.Partitions;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode.NodeFlag;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import org.springframework.data.redis.connection.ClusterCommandExecutor;
import org.springframework.data.redis.connection.ClusterNodeResourceProvider;
import org.springframework.data.redis.connection.ClusterTopologyProvider;
import org.springframework.data.redis.connection.RedisClusterCommands.AddSlots;
import org.springframework.data.redis.connection.RedisClusterNode;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class LettuceClusterConnectionUnitTests {

	private static final byte[] KEY_1_BYTES = KEY_1.getBytes();

	static final byte[] VALUE_1_BYTES = VALUE_1.getBytes();

	private static final byte[] KEY_2_BYTES = KEY_2.getBytes();

	private static final byte[] KEY_3_BYTES = KEY_3.getBytes();

	@Mock RedisClusterClient clusterMock;
	@Mock ClusterTopologyProvider topologyProviderMock;

	@Mock LettuceConnectionProvider connectionProviderMock;
	@Mock ClusterCommandExecutor executorMock;
	@Mock ClusterNodeResourceProvider resourceProvider;
	@Mock StatefulRedisClusterConnection<byte[], byte[]> sharedConnectionMock;
	@Mock RedisClusterAsyncCommands<byte[], byte[]> dedicatedConnectionMock;
	@Mock RedisClusterCommands<byte[], byte[]> clusterConnection1Mock;
	@Mock RedisClusterCommands<byte[], byte[]> clusterConnection2Mock;
	@Mock RedisClusterCommands<byte[], byte[]> clusterConnection3Mock;

	private LettuceClusterConnection connection;

	@BeforeEach
	void setUp() {

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
		partitions.updateCache();

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

	@Test // DATAREDIS-315
	void thowsExceptionWhenClusterCommandExecturorIsNull() {
		assertThatIllegalArgumentException().isThrownBy(() -> new LettuceClusterConnection(clusterMock, null));
	}

	@Test // DATAREDIS-315
	void clusterMeetShouldSendCommandsToExistingNodesCorrectly() {

		connection.clusterMeet(UNKNOWN_CLUSTER_NODE);

		verify(clusterConnection1Mock, times(1)).clusterMeet(UNKNOWN_CLUSTER_NODE.getHost(),
				UNKNOWN_CLUSTER_NODE.getPort());
		verify(clusterConnection2Mock, times(1)).clusterMeet(UNKNOWN_CLUSTER_NODE.getHost(),
				UNKNOWN_CLUSTER_NODE.getPort());
		verify(clusterConnection3Mock, times(1)).clusterMeet(UNKNOWN_CLUSTER_NODE.getHost(),
				UNKNOWN_CLUSTER_NODE.getPort());
	}

	@Test // DATAREDIS-315
	void clusterMeetShouldThrowExceptionWhenNodeIsNull() {
		assertThatIllegalArgumentException().isThrownBy(() -> connection.clusterMeet(null));
	}

	@Test // DATAREDIS-315
	void clusterForgetShouldSendCommandsToRemainingNodesCorrectly() {

		connection.clusterForget(CLUSTER_NODE_2);

		verify(clusterConnection1Mock, times(1)).clusterForget(CLUSTER_NODE_2.getId());
		verifyZeroInteractions(clusterConnection2Mock);
		verify(clusterConnection3Mock, times(1)).clusterForget(CLUSTER_NODE_2.getId());
	}

	@Test // DATAREDIS-315
	void clusterReplicateShouldSendCommandsCorrectly() {

		connection.clusterReplicate(CLUSTER_NODE_1, CLUSTER_NODE_2);

		verify(clusterConnection2Mock, times(1)).clusterReplicate(CLUSTER_NODE_1.getId());
		verifyZeroInteractions(clusterConnection1Mock);
	}

	@Test // DATAREDIS-315
	void closeShouldNotCloseUnderlyingClusterPool() throws IOException {

		connection.close();

		verify(clusterMock, never()).shutdown();
	}

	@Test // DATAREDIS-315
	void isClosedShouldReturnConnectionStateCorrectly() {

		assertThat(connection.isClosed()).isFalse();

		connection.close();

		assertThat(connection.isClosed()).isTrue();
	}

	@Test // DATAREDIS-315
	void keysShouldBeRunOnAllClusterNodes() {

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
	void keysShouldOnlyBeRunOnDedicatedNodeWhenPinned() {

		when(clusterConnection2Mock.keys(any(byte[].class))).thenReturn(Collections.<byte[]> emptyList());

		byte[] pattern = LettuceConverters.toBytes("*");

		connection.keys(CLUSTER_NODE_2, pattern);

		verify(clusterConnection1Mock, never()).keys(pattern);
		verify(clusterConnection2Mock, times(1)).keys(pattern);
		verify(clusterConnection3Mock, never()).keys(pattern);
	}

	@Test // DATAREDIS-315
	void randomKeyShouldReturnAnyKeyFromRandomNode() {

		when(clusterConnection1Mock.randomkey()).thenReturn(KEY_1_BYTES);
		when(clusterConnection2Mock.randomkey()).thenReturn(KEY_2_BYTES);
		when(clusterConnection3Mock.randomkey()).thenReturn(KEY_3_BYTES);

		assertThat(connection.randomKey()).isIn(KEY_1_BYTES, KEY_2_BYTES, KEY_3_BYTES);
		verifyInvocationsAcross("randomkey", times(1), clusterConnection1Mock, clusterConnection2Mock,
				clusterConnection3Mock);
	}

	@Test // DATAREDIS-315
	void randomKeyShouldReturnKeyWhenAvailableOnAnyNode() {

		when(clusterConnection3Mock.randomkey()).thenReturn(KEY_3_BYTES);

		for (int i = 0; i < 100; i++) {
			assertThat(connection.randomKey()).isEqualTo(KEY_3_BYTES);
		}
	}

	@Test // DATAREDIS-315
	void randomKeyShouldReturnNullWhenNoKeysPresentOnAllNodes() {

		when(clusterConnection1Mock.randomkey()).thenReturn(null);
		when(clusterConnection2Mock.randomkey()).thenReturn(null);
		when(clusterConnection3Mock.randomkey()).thenReturn(null);

		assertThat(connection.randomKey()).isNull();
	}

	@Test // DATAREDIS-315
	void clusterSetSlotImportingShouldBeExecutedCorrectly() {

		connection.clusterSetSlot(CLUSTER_NODE_1, 100, AddSlots.IMPORTING);

		verify(clusterConnection1Mock, times(1)).clusterSetSlotImporting(eq(100), eq(CLUSTER_NODE_1.getId()));
	}

	@Test // DATAREDIS-315
	void clusterSetSlotMigratingShouldBeExecutedCorrectly() {

		connection.clusterSetSlot(CLUSTER_NODE_1, 100, AddSlots.MIGRATING);

		verify(clusterConnection1Mock, times(1)).clusterSetSlotMigrating(eq(100), eq(CLUSTER_NODE_1.getId()));
	}

	@Test // DATAREDIS-315
	void clusterSetSlotStableShouldBeExecutedCorrectly() {

		connection.clusterSetSlot(CLUSTER_NODE_1, 100, AddSlots.STABLE);

		verify(clusterConnection1Mock, times(1)).clusterSetSlotStable(eq(100));
	}

	@Test // DATAREDIS-315
	void clusterSetSlotNodeShouldBeExecutedCorrectly() {

		connection.clusterSetSlot(CLUSTER_NODE_1, 100, AddSlots.NODE);

		verify(clusterConnection1Mock, times(1)).clusterSetSlotNode(eq(100), eq(CLUSTER_NODE_1.getId()));
	}

	@Test // DATAREDIS-315
	void clusterSetSlotShouldBeExecutedOnTargetNodeWhenNodeIdNotSet() {

		connection.clusterSetSlot(new RedisClusterNode(CLUSTER_HOST, MASTER_NODE_2_PORT), 100, AddSlots.IMPORTING);

		verify(clusterConnection2Mock, times(1)).clusterSetSlotImporting(eq(100), eq(CLUSTER_NODE_2.getId()));
	}

	@Test // DATAREDIS-315
	void clusterSetSlotShouldThrowExceptionWhenModeIsNull() {
		assertThatIllegalArgumentException().isThrownBy(() -> connection.clusterSetSlot(CLUSTER_NODE_1, 100, null));
	}

	@Test // DATAREDIS-315
	void clusterDeleteSlotsShouldBeExecutedCorrectly() {

		int[] slots = new int[] { 9000, 10000 };
		connection.clusterDeleteSlots(CLUSTER_NODE_2, slots);

		verify(clusterConnection2Mock, times(1)).clusterDelSlots((int[]) any());
	}

	@Test // DATAREDIS-315
	void clusterDeleteSlotShouldThrowExceptionWhenNodeIsNull() {
		assertThatIllegalArgumentException().isThrownBy(() -> connection.clusterDeleteSlots(null, new int[] { 1 }));
	}

	@Test // DATAREDIS-315
	void timeShouldBeExecutedOnArbitraryNode() {

		List<byte[]> values = Arrays.asList("1449655759".getBytes(), "92217".getBytes());
		when(dedicatedConnectionMock.time()).thenReturn(new LettuceServerCommands.CompletedRedisFuture<>(values));

		connection.time();

		verify(dedicatedConnectionMock).time();
	}

	@Test // DATAREDIS-315
	void timeShouldBeExecutedOnSingleNode() {

		when(clusterConnection2Mock.time()).thenReturn(Arrays.asList("1449655759".getBytes(), "92217".getBytes()));

		connection.time(CLUSTER_NODE_2);

		verify(clusterConnection2Mock, times(1)).time();
		verifyZeroInteractions(clusterConnection1Mock, clusterConnection3Mock);
	}

	@Test // DATAREDIS-315
	void resetConfigStatsShouldBeExecutedOnAllNodes() {

		connection.resetConfigStats();

		verify(clusterConnection1Mock, times(1)).configResetstat();
		verify(clusterConnection2Mock, times(1)).configResetstat();
		verify(clusterConnection3Mock, times(1)).configResetstat();
	}

	@Test // DATAREDIS-315
	void resetConfigStatsShouldBeExecutedOnSingleNodeCorrectly() {

		connection.resetConfigStats(CLUSTER_NODE_2);

		verify(clusterConnection2Mock, times(1)).configResetstat();
		verify(clusterConnection1Mock, never()).configResetstat();
		verify(clusterConnection1Mock, never()).configResetstat();
	}

	@Test // DATAREDIS-731, DATAREDIS-545
	void shouldExecuteOnSharedConnection() {

		RedisAdvancedClusterAsyncCommands<byte[], byte[]> async = mock(RedisAdvancedClusterAsyncCommands.class);

		when(sharedConnectionMock.async()).thenReturn(async);
		when(async.del(any())).thenReturn(mock(RedisFuture.class));

		LettuceClusterConnection connection = new LettuceClusterConnection(sharedConnectionMock, connectionProviderMock,
				topologyProviderMock, executorMock, Duration.ZERO);

		connection.keyCommands().del(KEY_1_BYTES);

		verify(async).del(KEY_1_BYTES);
		verifyNoMoreInteractions(connectionProviderMock);
	}

	@Test // DATAREDIS-731, DATAREDIS-545
	void shouldExecuteOnDedicatedConnection() {

		RedisAsyncCommands<byte[], byte[]> async = mock(RedisAsyncCommands.class);
		StatefulRedisConnection<byte[], byte[]> dedicatedConnection = mock(StatefulRedisConnection.class);

		when(connectionProviderMock.getConnection(StatefulConnection.class)).thenReturn(dedicatedConnection);
		when(dedicatedConnection.async()).thenReturn(async);
		when(async.blpop(anyLong(), any())).thenReturn(mock(RedisFuture.class));

		LettuceClusterConnection connection = new LettuceClusterConnection(sharedConnectionMock, connectionProviderMock,
				topologyProviderMock, executorMock, Duration.ZERO);

		connection.listCommands().bLPop(1, KEY_1_BYTES);

		verify(async).blpop(1, KEY_1_BYTES);
		verify(connectionProviderMock).getConnection(StatefulConnection.class);
		verifyNoMoreInteractions(connectionProviderMock);
		verifyZeroInteractions(sharedConnectionMock);
	}
}
