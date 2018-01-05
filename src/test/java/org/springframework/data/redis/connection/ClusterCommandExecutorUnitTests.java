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
package org.springframework.data.redis.connection;

import static org.hamcrest.core.Is.*;
import static org.hamcrest.core.IsCollectionContaining.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static org.springframework.data.redis.test.util.MockitoUtils.*;

import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;

import org.hamcrest.core.IsInstanceOf;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.ClusterRedirectException;
import org.springframework.data.redis.PassThroughExceptionTranslationStrategy;
import org.springframework.data.redis.TooManyClusterRedirectionsException;
import org.springframework.data.redis.connection.ClusterCommandExecutor.ClusterCommandCallback;
import org.springframework.data.redis.connection.ClusterCommandExecutor.MultiKeyClusterCommandCallback;
import org.springframework.data.redis.connection.ClusterCommandExecutor.MultiNodeResult;
import org.springframework.data.redis.connection.RedisClusterNode.LinkState;
import org.springframework.data.redis.connection.RedisClusterNode.SlotRange;
import org.springframework.data.redis.connection.RedisNode.NodeType;
import org.springframework.scheduling.concurrent.ConcurrentTaskExecutor;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@RunWith(MockitoJUnitRunner.class)
public class ClusterCommandExecutorUnitTests {

	static final String CLUSTER_NODE_1_HOST = "127.0.0.1";
	static final String CLUSTER_NODE_2_HOST = "127.0.0.1";
	static final String CLUSTER_NODE_3_HOST = "127.0.0.1";

	static final int CLUSTER_NODE_1_PORT = 7379;
	static final int CLUSTER_NODE_2_PORT = 7380;
	static final int CLUSTER_NODE_3_PORT = 7381;

	static final RedisClusterNode CLUSTER_NODE_1 = RedisClusterNode.newRedisClusterNode()
			.listeningAt(CLUSTER_NODE_1_HOST, CLUSTER_NODE_1_PORT).serving(new SlotRange(0, 5460))
			.withId("ef570f86c7b1a953846668debc177a3a16733420").promotedAs(NodeType.MASTER).linkState(LinkState.CONNECTED)
			.build();
	static final RedisClusterNode CLUSTER_NODE_2 = RedisClusterNode.newRedisClusterNode()
			.listeningAt(CLUSTER_NODE_2_HOST, CLUSTER_NODE_2_PORT).serving(new SlotRange(5461, 10922))
			.withId("0f2ee5df45d18c50aca07228cc18b1da96fd5e84").promotedAs(NodeType.MASTER).linkState(LinkState.CONNECTED)
			.build();
	static final RedisClusterNode CLUSTER_NODE_3 = RedisClusterNode.newRedisClusterNode()
			.listeningAt(CLUSTER_NODE_3_HOST, CLUSTER_NODE_3_PORT).serving(new SlotRange(10923, 16383))
			.withId("3b9b8192a874fa8f1f09dbc0ee20afab5738eee7").promotedAs(NodeType.MASTER).linkState(LinkState.CONNECTED)
			.build();
	static final RedisClusterNode CLUSTER_NODE_2_LOOKUP = RedisClusterNode.newRedisClusterNode()
			.withId("0f2ee5df45d18c50aca07228cc18b1da96fd5e84").build();

	static final RedisClusterNode UNKNOWN_CLUSTER_NODE = new RedisClusterNode("8.8.8.8", 7379, SlotRange.empty());

	private ClusterCommandExecutor executor;

	private static final ConnectionCommandCallback<String> COMMAND_CALLBACK = Connection::theWheelWeavesAsTheWheelWills;

	private static final Converter<Exception, DataAccessException> exceptionConverter = source -> {

		if (source instanceof MovedException) {
			return new ClusterRedirectException(1000, ((MovedException) source).host, ((MovedException) source).port, source);
		}

		return new InvalidDataAccessApiUsageException(source.getMessage(), source);
	};

	private static final MultiKeyConnectionCommandCallback<String> MULTIKEY_CALLBACK = Connection::bloodAndAshes;

	@Mock Connection con1;
	@Mock Connection con2;
	@Mock Connection con3;

	@Before
	public void setUp() {

		this.executor = new ClusterCommandExecutor(new MockClusterNodeProvider(), new MockClusterResourceProvider(),
				new PassThroughExceptionTranslationStrategy(exceptionConverter));
	}

	@After
	public void tearDown() throws Exception {
		this.executor.destroy();
	}

	@Test // DATAREDIS-315
	public void executeCommandOnSingleNodeShouldBeExecutedCorrectly() {

		executor.executeCommandOnSingleNode(COMMAND_CALLBACK, CLUSTER_NODE_2);

		verify(con2, times(1)).theWheelWeavesAsTheWheelWills();
	}

	@Test // DATAREDIS-315
	public void executeCommandOnSingleNodeByHostAndPortShouldBeExecutedCorrectly() {

		executor.executeCommandOnSingleNode(COMMAND_CALLBACK,
				new RedisClusterNode(CLUSTER_NODE_2_HOST, CLUSTER_NODE_2_PORT));

		verify(con2, times(1)).theWheelWeavesAsTheWheelWills();
	}

	@Test // DATAREDIS-315
	public void executeCommandOnSingleNodeByNodeIdShouldBeExecutedCorrectly() {

		executor.executeCommandOnSingleNode(COMMAND_CALLBACK, new RedisClusterNode(CLUSTER_NODE_2.id));

		verify(con2, times(1)).theWheelWeavesAsTheWheelWills();
	}

	@Test(expected = IllegalArgumentException.class) // DATAREDIS-315
	public void executeCommandOnSingleNodeShouldThrowExceptionWhenNodeIsNull() {
		executor.executeCommandOnSingleNode(COMMAND_CALLBACK, null);
	}

	@Test(expected = IllegalArgumentException.class) // DATAREDIS-315
	public void executeCommandOnSingleNodeShouldThrowExceptionWhenCommandCallbackIsNull() {
		executor.executeCommandOnSingleNode(null, CLUSTER_NODE_1);
	}

	@Test(expected = IllegalArgumentException.class) // DATAREDIS-315
	public void executeCommandOnSingleNodeShouldThrowExceptionWhenNodeIsUnknown() {
		executor.executeCommandOnSingleNode(COMMAND_CALLBACK, UNKNOWN_CLUSTER_NODE);
	}

	@Test // DATAREDIS-315
	public void executeCommandAsyncOnNodesShouldExecuteCommandOnGivenNodes() {

		ClusterCommandExecutor executor = new ClusterCommandExecutor(new MockClusterNodeProvider(),
				new MockClusterResourceProvider(), new PassThroughExceptionTranslationStrategy(exceptionConverter),
				new ConcurrentTaskExecutor(new SyncTaskExecutor()));

		executor.executeCommandAsyncOnNodes(COMMAND_CALLBACK, Arrays.asList(CLUSTER_NODE_1, CLUSTER_NODE_2));

		verify(con1, times(1)).theWheelWeavesAsTheWheelWills();
		verify(con2, times(1)).theWheelWeavesAsTheWheelWills();
		verify(con3, never()).theWheelWeavesAsTheWheelWills();
	}

	@Test // DATAREDIS-315
	public void executeCommandAsyncOnNodesShouldExecuteCommandOnGivenNodesByHostAndPort() {

		ClusterCommandExecutor executor = new ClusterCommandExecutor(new MockClusterNodeProvider(),
				new MockClusterResourceProvider(), new PassThroughExceptionTranslationStrategy(exceptionConverter),
				new ConcurrentTaskExecutor(new SyncTaskExecutor()));

		executor.executeCommandAsyncOnNodes(COMMAND_CALLBACK,
				Arrays.asList(new RedisClusterNode(CLUSTER_NODE_1_HOST, CLUSTER_NODE_1_PORT),
						new RedisClusterNode(CLUSTER_NODE_2_HOST, CLUSTER_NODE_2_PORT)));

		verify(con1, times(1)).theWheelWeavesAsTheWheelWills();
		verify(con2, times(1)).theWheelWeavesAsTheWheelWills();
		verify(con3, never()).theWheelWeavesAsTheWheelWills();
	}

	@Test // DATAREDIS-315
	public void executeCommandAsyncOnNodesShouldExecuteCommandOnGivenNodesByNodeId() {

		ClusterCommandExecutor executor = new ClusterCommandExecutor(new MockClusterNodeProvider(),
				new MockClusterResourceProvider(), new PassThroughExceptionTranslationStrategy(exceptionConverter),
				new ConcurrentTaskExecutor(new SyncTaskExecutor()));

		executor.executeCommandAsyncOnNodes(COMMAND_CALLBACK,
				Arrays.asList(new RedisClusterNode(CLUSTER_NODE_1.id), CLUSTER_NODE_2_LOOKUP));

		verify(con1, times(1)).theWheelWeavesAsTheWheelWills();
		verify(con2, times(1)).theWheelWeavesAsTheWheelWills();
		verify(con3, never()).theWheelWeavesAsTheWheelWills();
	}

	@Test(expected = IllegalArgumentException.class) // DATAREDIS-315
	public void executeCommandAsyncOnNodesShouldFailOnGivenUnknownNodes() {

		ClusterCommandExecutor executor = new ClusterCommandExecutor(new MockClusterNodeProvider(),
				new MockClusterResourceProvider(), new PassThroughExceptionTranslationStrategy(exceptionConverter),
				new ConcurrentTaskExecutor(new SyncTaskExecutor()));

		executor.executeCommandAsyncOnNodes(COMMAND_CALLBACK,
				Arrays.asList(new RedisClusterNode("unknown"), CLUSTER_NODE_2_LOOKUP));
	}

	@Test // DATAREDIS-315
	public void executeCommandOnAllNodesShouldExecuteCommandOnEveryKnownClusterNode() {

		ClusterCommandExecutor executor = new ClusterCommandExecutor(new MockClusterNodeProvider(),
				new MockClusterResourceProvider(), new PassThroughExceptionTranslationStrategy(exceptionConverter),
				new ConcurrentTaskExecutor(new SyncTaskExecutor()));

		executor.executeCommandOnAllNodes(COMMAND_CALLBACK);

		verify(con1, times(1)).theWheelWeavesAsTheWheelWills();
		verify(con2, times(1)).theWheelWeavesAsTheWheelWills();
		verify(con3, times(1)).theWheelWeavesAsTheWheelWills();
	}

	@Test // DATAREDIS-315
	public void executeCommandAsyncOnNodesShouldCompleteAndCollectErrorsOfAllNodes() {

		when(con1.theWheelWeavesAsTheWheelWills()).thenReturn("rand");
		when(con2.theWheelWeavesAsTheWheelWills()).thenThrow(new IllegalStateException("(error) mat lost the dagger..."));
		when(con3.theWheelWeavesAsTheWheelWills()).thenReturn("perrin");

		try {
			executor.executeCommandOnAllNodes(COMMAND_CALLBACK);
		} catch (ClusterCommandExecutionFailureException e) {

			assertThat(e.getCauses().size(), is(1));
			assertThat(e.getCauses().iterator().next(), IsInstanceOf.instanceOf(DataAccessException.class));
		}

		verify(con1, times(1)).theWheelWeavesAsTheWheelWills();
		verify(con2, times(1)).theWheelWeavesAsTheWheelWills();
		verify(con3, times(1)).theWheelWeavesAsTheWheelWills();
	}

	@Test // DATAREDIS-315
	public void executeCommandAsyncOnNodesShouldCollectResultsCorrectly() {

		when(con1.theWheelWeavesAsTheWheelWills()).thenReturn("rand");
		when(con2.theWheelWeavesAsTheWheelWills()).thenReturn("mat");
		when(con3.theWheelWeavesAsTheWheelWills()).thenReturn("perrin");

		MultiNodeResult<String> result = executor.executeCommandOnAllNodes(COMMAND_CALLBACK);

		assertThat(result.resultsAsList(), hasItems("rand", "mat", "perrin"));
	}

	@Test // DATAREDIS-315, DATAREDIS-467
	public void executeMultikeyCommandShouldRunCommandAcrossCluster() {

		// key-1 and key-9 map both to node1
		ArgumentCaptor<byte[]> captor = ArgumentCaptor.forClass(byte[].class);
		when(con1.bloodAndAshes(captor.capture())).thenReturn("rand").thenReturn("egwene");

		when(con2.bloodAndAshes(any(byte[].class))).thenReturn("mat");
		when(con3.bloodAndAshes(any(byte[].class))).thenReturn("perrin");

		MultiNodeResult<String> result = executor.executeMultiKeyCommand(MULTIKEY_CALLBACK,
				new HashSet<>(
				Arrays.asList("key-1".getBytes(), "key-2".getBytes(), "key-3".getBytes(), "key-9".getBytes())));

		assertThat(result.resultsAsList(), hasItems("rand", "mat", "perrin", "egwene"));

		// check that 2 keys have been routed to node1
		assertThat(captor.getAllValues().size(), is(2));
	}

	@Test // DATAREDIS-315
	public void executeCommandOnSingleNodeAndFollowRedirect() {

		when(con1.theWheelWeavesAsTheWheelWills()).thenThrow(new MovedException(CLUSTER_NODE_3_HOST, CLUSTER_NODE_3_PORT));

		executor.executeCommandOnSingleNode(COMMAND_CALLBACK, CLUSTER_NODE_1);

		verify(con1, times(1)).theWheelWeavesAsTheWheelWills();
		verify(con3, times(1)).theWheelWeavesAsTheWheelWills();
		verify(con2, never()).theWheelWeavesAsTheWheelWills();
	}

	@Test // DATAREDIS-315
	public void executeCommandOnSingleNodeAndFollowRedirectButStopsAfterMaxRedirects() {

		when(con1.theWheelWeavesAsTheWheelWills()).thenThrow(new MovedException(CLUSTER_NODE_3_HOST, CLUSTER_NODE_3_PORT));
		when(con3.theWheelWeavesAsTheWheelWills()).thenThrow(new MovedException(CLUSTER_NODE_2_HOST, CLUSTER_NODE_2_PORT));
		when(con2.theWheelWeavesAsTheWheelWills()).thenThrow(new MovedException(CLUSTER_NODE_1_HOST, CLUSTER_NODE_1_PORT));

		try {
			executor.setMaxRedirects(4);
			executor.executeCommandOnSingleNode(COMMAND_CALLBACK, CLUSTER_NODE_1);
		} catch (Exception e) {
			assertThat(e, IsInstanceOf.instanceOf(TooManyClusterRedirectionsException.class));
		}

		verify(con1, times(2)).theWheelWeavesAsTheWheelWills();
		verify(con3, times(2)).theWheelWeavesAsTheWheelWills();
		verify(con2, times(1)).theWheelWeavesAsTheWheelWills();
	}

	@Test // DATAREDIS-315
	public void executeCommandOnArbitraryNodeShouldPickARandomNode() {

		executor.executeCommandOnArbitraryNode(COMMAND_CALLBACK);

		verifyInvocationsAcross("theWheelWeavesAsTheWheelWills", times(1), con1, con2, con3);
	}

	class MockClusterNodeProvider implements ClusterTopologyProvider {

		@Override
		public ClusterTopology getTopology() {
			return new ClusterTopology(
					new LinkedHashSet<>(Arrays.asList(CLUSTER_NODE_1, CLUSTER_NODE_2, CLUSTER_NODE_3)));
		}

	}

	class MockClusterResourceProvider implements ClusterNodeResourceProvider {

		@Override
		public Connection getResourceForSpecificNode(RedisClusterNode node) {

			if (CLUSTER_NODE_1.equals(node)) {
				return con1;
			}
			if (CLUSTER_NODE_2.equals(node)) {
				return con2;
			}
			if (CLUSTER_NODE_3.equals(node)) {
				return con3;
			}

			return null;
		}

		@Override
		public void returnResourceForSpecificNode(RedisClusterNode node, Object resource) {
			// TODO Auto-generated method stub
		}

	}

	static interface ConnectionCommandCallback<S> extends ClusterCommandCallback<Connection, S> {

	}

	static interface MultiKeyConnectionCommandCallback<S> extends MultiKeyClusterCommandCallback<Connection, S> {

	}

	static interface Connection {

		String theWheelWeavesAsTheWheelWills();

		String bloodAndAshes(byte[] key);
	}

	static class MovedException extends RuntimeException {

		String host;
		int port;

		public MovedException(String host, int port) {
			this.host = host;
			this.port = port;
		}

	}

}
