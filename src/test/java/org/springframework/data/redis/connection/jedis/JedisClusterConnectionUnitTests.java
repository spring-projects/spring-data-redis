/*
 * Copyright 2015 the original author or authors.
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

import static org.hamcrest.core.AnyOf.*;
import static org.hamcrest.core.Is.*;
import static org.hamcrest.core.IsCollectionContaining.*;
import static org.hamcrest.core.IsNull.*;
import static org.junit.Assert.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.hamcrest.core.IsInstanceOf;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.connection.ClusterCommandExecutionFailureException;
import org.springframework.data.redis.connection.RedisNode;
import org.springframework.data.redis.connection.jedis.JedisClusterConnection.JedisCommandCallback;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisException;

/**
 * @author Christoph Strobl
 */
@RunWith(MockitoJUnitRunner.class)
public class JedisClusterConnectionUnitTests {

	static final String KEY_1 = "key-1";
	static final byte[] KEY_1_BYTES = KEY_1.getBytes();

	static final String VALUE_1 = "value-1";
	static final byte[] VALUE_1_BYTES = VALUE_1.getBytes();

	static final String KEY_2 = "key-2";
	static final byte[] KEY_2_BYTES = KEY_2.getBytes();

	static final String KEY_3 = "key-3";
	static final byte[] KEY_3_BYTES = KEY_3.getBytes();

	static final String CLUSTER_NODE_1_HOST = "127.0.0.1";
	static final String CLUSTER_NODE_2_HOST = "127.0.0.1";
	static final String CLUSTER_NODE_3_HOST = "127.0.0.1";

	static final int CLUSTER_NODE_1_PORT = 6379;
	static final int CLUSTER_NODE_2_PORT = 6380;
	static final int CLUSTER_NODE_3_PORT = 6381;

	static final RedisNode CLUSTER_NODE_1 = new RedisNode(CLUSTER_NODE_1_HOST, CLUSTER_NODE_1_PORT)
			.withId("ef570f86c7b1a953846668debc177a3a16733420");
	static final RedisNode CLUSTER_NODE_2 = new RedisNode(CLUSTER_NODE_2_HOST, CLUSTER_NODE_2_PORT)
			.withId("0f2ee5df45d18c50aca07228cc18b1da96fd5e84");
	static final RedisNode CLUSTER_NODE_3 = new RedisNode(CLUSTER_NODE_3_HOST, CLUSTER_NODE_3_PORT)
			.withId("3b9b8192a874fa8f1f09dbc0ee20afab5738eee7");

	static final RedisNode UNKNOWN_CLUSTER_NODE = new RedisNode("8.8.8.8", 6379);

	@Mock JedisCluster clusterMock;

	@Mock JedisPool connectionPool1Mock;
	@Mock JedisPool connectionPool2Mock;
	@Mock JedisPool connectionPool3Mock;

	@Mock Jedis clusterConnection1Mock;
	@Mock Jedis clusterConnection2Mock;
	@Mock Jedis clusterConnection3Mock;

	JedisClusterConnection connection;

	@Before
	public void setUp() {

		Map<String, JedisPool> connectionPoolMap = new HashMap<String, JedisPool>();

		when(connectionPool1Mock.getResource()).thenReturn(clusterConnection1Mock);
		when(connectionPool2Mock.getResource()).thenReturn(clusterConnection2Mock);
		when(connectionPool3Mock.getResource()).thenReturn(clusterConnection3Mock);

		connectionPoolMap.put(CLUSTER_NODE_1_HOST + ":" + CLUSTER_NODE_1_PORT, connectionPool1Mock);
		connectionPoolMap.put(CLUSTER_NODE_2_HOST + ":" + CLUSTER_NODE_2_PORT, connectionPool2Mock);
		connectionPoolMap.put(CLUSTER_NODE_3_HOST + ":" + CLUSTER_NODE_3_PORT, connectionPool3Mock);

		when(clusterMock.getClusterNodes()).thenReturn(connectionPoolMap);

		connection = new JedisClusterConnection(clusterMock);
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void getResourcePoolForSpecificNodeShouldReturnNodeWhenExists() {
		assertThat(connection.getResourcePoolForSpecificNode(CLUSTER_NODE_1), is(connectionPool1Mock));
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void getResourcePoolForSpecificNodeShouldReturnNullWhenNodeDoesNotExist() {
		assertThat(connection.getResourcePoolForSpecificNode(UNKNOWN_CLUSTER_NODE), nullValue());
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test(expected = IllegalArgumentException.class)
	public void getResourcePoolForSpecificNodeShouldThrowExceptionWhenNodeIsNull() {
		connection.getResourcePoolForSpecificNode(null);
	}

	/**
	 * @see DATAREDIS-315
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void runCommandOnSingleNodeShouldBeExecutedCorrectly() {

		JedisCommandCallback callback = mock(JedisCommandCallback.class);

		connection.runCommandOnSingleNode(callback, CLUSTER_NODE_2);

		verify(callback, times(1)).doInJedis(eq(clusterConnection2Mock));
	}

	/**
	 * @see DATAREDIS-315
	 */
	@SuppressWarnings("unchecked")
	@Test(expected = IllegalArgumentException.class)
	public void runCommandOnSingleNodeShouldThrowExceptionWhenNodeIsNull() {
		connection.runCommandOnSingleNode(mock(JedisCommandCallback.class), null);
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test(expected = IllegalArgumentException.class)
	public void runCommandOnSingleNodeShouldThrowExceptionWhenCommandCallbackIsNull() {
		connection.runCommandOnSingleNode(null, CLUSTER_NODE_1);
	}

	/**
	 * @see DATAREDIS-315
	 */
	@SuppressWarnings("unchecked")
	@Test(expected = IllegalArgumentException.class)
	public void runCommandOnSingleNodeShouldThrowExceptionWhenNodeIsUnknown() {
		connection.runCommandOnSingleNode(mock(JedisCommandCallback.class), UNKNOWN_CLUSTER_NODE);
	}

	/**
	 * @see DATAREDIS-315
	 */
	@SuppressWarnings({ "unchecked" })
	@Test
	public void runCommandOnSingleNodeShouldReturnResourceToPoolCorrectly() {

		connection.runCommandOnSingleNode(mock(JedisCommandCallback.class), CLUSTER_NODE_1);

		verify(connectionPool1Mock, times(1)).returnResource(clusterConnection1Mock);
	}

	/**
	 * @see DATAREDIS-315
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void runCommandOnSingleNodeShouldReturnResourceToPoolCorrectlyWhenExceptionRaisedDuringExecution() {

		JedisCommandCallback callback = mock(JedisCommandCallback.class);
		when(callback.doInJedis(any(Jedis.class))).thenThrow(new JedisException("(error) CLUSTER..."));

		try {
			connection.runCommandOnSingleNode(callback, CLUSTER_NODE_1);
		} catch (Exception e) {
			// just catch this one as we want to check resource pool
		}

		verify(connectionPool1Mock, times(1)).returnResource(clusterConnection1Mock);
	}

	/**
	 * @see DATAREDIS-315
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void runCommandOnSingleNodeShouldReturnBrokenResourceToPoolCorrectlyWhenExceptionIndicatesConnectionProblems() {

		JedisCommandCallback callback = mock(JedisCommandCallback.class);
		when(callback.doInJedis(any(Jedis.class))).thenThrow(new RedisConnectionFailureException("unable to connect"));

		try {
			connection.runCommandOnSingleNode(callback, CLUSTER_NODE_1);
		} catch (Exception e) {
			// just catch this one as we want to check resource pool
		}

		verify(connectionPool1Mock, times(1)).returnBrokenResource(clusterConnection1Mock);
	}

	/**
	 * @see DATAREDIS-315
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void runCommandAsyncOnNodesShouldExecuteCommandOnEveryKnwonClusterNode() {

		JedisCommandCallback callback = mock(JedisCommandCallback.class);

		connection.runCommandAsyncOnNodes(callback, Arrays.asList(CLUSTER_NODE_1, CLUSTER_NODE_2));

		verify(callback, times(1)).doInJedis(clusterConnection1Mock);
		verify(callback, times(1)).doInJedis(clusterConnection2Mock);
	}

	/**
	 * @see DATAREDIS-315
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void runCommandAsyncOnNodesShouldCompleteAndCollectErrorsOfAllNodes() {

		JedisCommandCallback callback = mock(JedisCommandCallback.class);
		when(callback.doInJedis(clusterConnection1Mock)).thenReturn("1");
		when(callback.doInJedis(clusterConnection2Mock)).thenThrow(new JedisException("(error) CLUSTER..."));
		when(callback.doInJedis(clusterConnection3Mock)).thenReturn("3");

		try {
			connection.runCommandAsyncOnNodes(callback, Arrays.asList(CLUSTER_NODE_1, CLUSTER_NODE_2, CLUSTER_NODE_3));
		} catch (ClusterCommandExecutionFailureException e) {

			assertThat(e.getCauses().size(), is(1));
			assertThat(e.getCauses().iterator().next(), IsInstanceOf.instanceOf(DataAccessException.class));
		}

		verify(callback, times(1)).doInJedis(clusterConnection1Mock);
		verify(callback, times(1)).doInJedis(clusterConnection2Mock);
		verify(callback, times(1)).doInJedis(clusterConnection3Mock);
	}

	/**
	 * @see DATAREDIS-315
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void runCommandAsyncOnNodesShouldCollectResultsCorrectly() {

		JedisCommandCallback callback = mock(JedisCommandCallback.class);
		when(callback.doInJedis(clusterConnection1Mock)).thenReturn("1");
		when(callback.doInJedis(clusterConnection2Mock)).thenReturn("2");
		when(callback.doInJedis(clusterConnection3Mock)).thenReturn("3");

		Map<RedisNode, String> result = connection.runCommandAsyncOnNodes(callback,
				Arrays.asList(CLUSTER_NODE_1, CLUSTER_NODE_2, CLUSTER_NODE_3));

		assertThat(result.keySet(), hasItems(CLUSTER_NODE_1, CLUSTER_NODE_2, CLUSTER_NODE_3));
		assertThat(result.values(), hasItems("1", "2", "3"));
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void clusterMeetShouldSendCommandsToExistingNodesCorrectly() {

		connection.clusterMeet(UNKNOWN_CLUSTER_NODE);

		verify(clusterConnection1Mock, times(1))
				.clusterMeet(UNKNOWN_CLUSTER_NODE.getHost(), UNKNOWN_CLUSTER_NODE.getPort());
		verify(clusterConnection2Mock, times(1))
				.clusterMeet(UNKNOWN_CLUSTER_NODE.getHost(), UNKNOWN_CLUSTER_NODE.getPort());
		verify(clusterConnection3Mock, times(1))
				.clusterMeet(UNKNOWN_CLUSTER_NODE.getHost(), UNKNOWN_CLUSTER_NODE.getPort());
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test(expected = IllegalArgumentException.class)
	public void clusterMeetShouldThrowExceptionWhenNodeIsNull() {
		connection.clusterMeet(null);
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void clusterForgetShouldSendCommandsToRemainingNodesCorrectly() {

		connection.clusterForget(CLUSTER_NODE_2);

		verify(clusterConnection1Mock, times(1)).clusterForget(CLUSTER_NODE_2.getId());
		verifyZeroInteractions(clusterConnection2Mock);
		verify(clusterConnection3Mock, times(1)).clusterForget(CLUSTER_NODE_2.getId());
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void clusterReplicateShouldSendCommandsCorrectly() {

		connection.clusterReplicate(CLUSTER_NODE_1, CLUSTER_NODE_2);

		verify(clusterConnection2Mock, times(1)).clusterReplicate(CLUSTER_NODE_1.getId());
		verifyZeroInteractions(clusterConnection1Mock);
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void closeShouldDelegateToClusterClose() throws IOException {

		connection.close();

		verify(clusterMock, times(1)).close();
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void isClosedShouldRetrunConnectionStateCorrectly() {

		assertThat(connection.isClosed(), is(false));

		connection.close();

		assertThat(connection.isClosed(), is(true));
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void getNativeConnectionShouldRetrunJedisCluster() {
		assertThat(connection.getNativeConnection(), is((Object) clusterMock));
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void delShouldRemoveSingleKey() {

		connection.del(KEY_1_BYTES);

		verify(clusterMock, times(1)).del(KEY_1_BYTES);
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void delShouldRemoveMultipleKeys() {

		connection.del(KEY_1_BYTES, KEY_2_BYTES);

		verify(clusterMock, times(1)).del(KEY_1_BYTES);
		verify(clusterMock, times(1)).del(KEY_2_BYTES);
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void typeShouldRetrieveTypeCorrectly() {

		connection.type(KEY_1_BYTES);

		verify(clusterMock, times(1)).type(KEY_1_BYTES);
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void keysShouldBeRunOnAllClusterNodes() {

		when(clusterConnection1Mock.keys(any(byte[].class))).thenReturn(Collections.<byte[]> emptySet());
		when(clusterConnection2Mock.keys(any(byte[].class))).thenReturn(Collections.<byte[]> emptySet());
		when(clusterConnection3Mock.keys(any(byte[].class))).thenReturn(Collections.<byte[]> emptySet());

		byte[] pattern = JedisConverters.toBytes("*");

		connection.keys(pattern);

		verify(clusterConnection1Mock, times(1)).keys(pattern);
		verify(clusterConnection2Mock, times(1)).keys(pattern);
		verify(clusterConnection3Mock, times(1)).keys(pattern);
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void keysShouldOnlyBeRunOnDedicatedNodeWhenPinned() {

		when(clusterConnection2Mock.keys(any(byte[].class))).thenReturn(Collections.<byte[]> emptySet());

		byte[] pattern = JedisConverters.toBytes("*");

		connection.keys(pattern, CLUSTER_NODE_2);

		verify(clusterConnection1Mock, never()).keys(pattern);
		verify(clusterConnection2Mock, times(1)).keys(pattern);
		verify(clusterConnection3Mock, never()).keys(pattern);
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void randomKeyShouldReturnAnyKeyFromRandomNode() {

		when(clusterConnection1Mock.randomBinaryKey()).thenReturn(KEY_1_BYTES);
		when(clusterConnection2Mock.randomBinaryKey()).thenReturn(KEY_2_BYTES);
		when(clusterConnection3Mock.randomBinaryKey()).thenReturn(KEY_3_BYTES);

		assertThat(connection.randomKey(), anyOf(is(KEY_1_BYTES), is(KEY_2_BYTES), is(KEY_3_BYTES)));
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void randomKeyShouldReturnKeyWhenAvailableOnAnyNode() {

		when(clusterConnection3Mock.randomBinaryKey()).thenReturn(KEY_3_BYTES);

		for (int i = 0; i < 100; i++) {
			assertThat(connection.randomKey(), is(KEY_3_BYTES));
		}
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void randomKeyShouldReturnNullWhenNoKeysPresentOnAllNodes() {

		when(clusterConnection1Mock.randomBinaryKey()).thenReturn(null);
		when(clusterConnection2Mock.randomBinaryKey()).thenReturn(null);
		when(clusterConnection3Mock.randomBinaryKey()).thenReturn(null);

		assertThat(connection.randomKey(), nullValue());
	}

}
