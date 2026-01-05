/*
 * Copyright 2015-present the original author or authors.
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
package org.springframework.data.redis.connection;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.springframework.data.redis.test.util.MockitoUtils.*;

import edu.umd.cs.mtc.MultithreadedTestCase;
import edu.umd.cs.mtc.TestFramework;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.core.task.support.TaskExecutorAdapter;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.ClusterRedirectException;
import org.springframework.data.redis.PassThroughExceptionTranslationStrategy;
import org.springframework.data.redis.TooManyClusterRedirectionsException;
import org.springframework.data.redis.connection.ClusterCommandExecutor.ClusterCommandCallback;
import org.springframework.data.redis.connection.ClusterCommandExecutor.MultiKeyClusterCommandCallback;
import org.springframework.data.redis.connection.ClusterCommandExecutor.MultiNodeResult;
import org.springframework.data.redis.connection.ClusterCommandExecutor.NodeExecution;
import org.springframework.data.redis.connection.ClusterCommandExecutor.NodeResult;
import org.springframework.data.redis.connection.RedisClusterNode.LinkState;
import org.springframework.data.redis.connection.RedisClusterNode.SlotRange;
import org.springframework.data.redis.connection.RedisNode.NodeType;

/**
 * Unit Tests for {@link ClusterCommandExecutor}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author John Blum
 */
@ExtendWith(MockitoExtension.class)
class ClusterCommandExecutorUnitTests {

	private static final String CLUSTER_NODE_1_HOST = "127.0.0.1";
	private static final String CLUSTER_NODE_2_HOST = "127.0.0.1";
	private static final String CLUSTER_NODE_3_HOST = "127.0.0.1";

	private static final int CLUSTER_NODE_1_PORT = 7379;
	private static final int CLUSTER_NODE_2_PORT = 7380;
	private static final int CLUSTER_NODE_3_PORT = 7381;

	private static final RedisClusterNode CLUSTER_NODE_1 = RedisClusterNode.newRedisClusterNode()
			.listeningAt(CLUSTER_NODE_1_HOST, CLUSTER_NODE_1_PORT) //
			.serving(new SlotRange(0, 5460)) //
			.withId("ef570f86c7b1a953846668debc177a3a16733420") //
			.promotedAs(NodeType.MASTER) //
			.linkState(LinkState.CONNECTED) //
			.withName("ClusterNodeX") //
			.build();

	private static final RedisClusterNode CLUSTER_NODE_2 = RedisClusterNode.newRedisClusterNode()
			.listeningAt(CLUSTER_NODE_2_HOST, CLUSTER_NODE_2_PORT) //
			.serving(new SlotRange(5461, 10922)) //
			.withId("0f2ee5df45d18c50aca07228cc18b1da96fd5e84") //
			.promotedAs(NodeType.MASTER) //
			.linkState(LinkState.CONNECTED) //
			.withName("ClusterNodeY") //
			.build();

	private static final RedisClusterNode CLUSTER_NODE_3 = RedisClusterNode.newRedisClusterNode()
			.listeningAt(CLUSTER_NODE_3_HOST, CLUSTER_NODE_3_PORT) //
			.serving(new SlotRange(10923, 16383)) //
			.withId("3b9b8192a874fa8f1f09dbc0ee20afab5738eee7") //
			.promotedAs(NodeType.MASTER) //
			.linkState(LinkState.CONNECTED) //
			.withName("ClusterNodeZ") //
			.build();

	private static final RedisClusterNode CLUSTER_NODE_2_LOOKUP = RedisClusterNode.newRedisClusterNode()
			.withId("0f2ee5df45d18c50aca07228cc18b1da96fd5e84") //
			.build();

	private static final RedisClusterNode UNKNOWN_CLUSTER_NODE = new RedisClusterNode("8.8.8.8", 7379, SlotRange.empty());

	private ClusterCommandExecutor executor;

	private static final ConnectionCommandCallback<String> COMMAND_CALLBACK = Connection::theWheelWeavesAsTheWheelWills;

	private static final Converter<Exception, DataAccessException> exceptionConverter = source -> {

		if (source instanceof MovedException movedException) {
			return new ClusterRedirectException(1000, movedException.host, movedException.port, source);
		}

		return new InvalidDataAccessApiUsageException(source.getMessage(), source);
	};

	private static final MultiKeyConnectionCommandCallback<String> MULTIKEY_CALLBACK = Connection::bloodAndAshes;

	@Mock Connection connection1;
	@Mock Connection connection2;
	@Mock Connection connection3;

	@BeforeEach
	void setUp() {

		this.executor = new ClusterCommandExecutor(new MockClusterNodeProvider(), new MockClusterNodeResourceProvider(),
				new PassThroughExceptionTranslationStrategy(exceptionConverter),
				new TaskExecutorAdapter(new SyncTaskExecutor()));
	}

	@AfterEach
	void tearDown() throws Exception {
		this.executor.destroy();
	}

	@Test // DATAREDIS-315
	void executeCommandOnSingleNodeShouldBeExecutedCorrectly() {

		executor.executeCommandOnSingleNode(COMMAND_CALLBACK, CLUSTER_NODE_2);

		verify(connection2).theWheelWeavesAsTheWheelWills();
	}

	@Test // DATAREDIS-315
	void executeCommandOnSingleNodeByHostAndPortShouldBeExecutedCorrectly() {

		executor.executeCommandOnSingleNode(COMMAND_CALLBACK,
				new RedisClusterNode(CLUSTER_NODE_2_HOST, CLUSTER_NODE_2_PORT));

		verify(connection2).theWheelWeavesAsTheWheelWills();
	}

	@Test // DATAREDIS-315
	@SuppressWarnings("ConstantConditions")
	void executeCommandOnSingleNodeByNodeIdShouldBeExecutedCorrectly() {

		executor.executeCommandOnSingleNode(COMMAND_CALLBACK, new RedisClusterNode(CLUSTER_NODE_2.id));

		verify(connection2).theWheelWeavesAsTheWheelWills();
	}

	@Test // DATAREDIS-315
	@SuppressWarnings("ConstantConditions")
	void executeCommandOnSingleNodeShouldThrowExceptionWhenNodeIsNull() {
		assertThatIllegalArgumentException().isThrownBy(() -> executor.executeCommandOnSingleNode(COMMAND_CALLBACK, null));
	}

	@Test // DATAREDIS-315
	@SuppressWarnings("ConstantConditions")
	void executeCommandOnSingleNodeShouldThrowExceptionWhenCommandCallbackIsNull() {
		assertThatIllegalArgumentException().isThrownBy(() -> executor.executeCommandOnSingleNode(null, CLUSTER_NODE_1));
	}

	@Test // DATAREDIS-315
	void executeCommandOnSingleNodeShouldThrowExceptionWhenNodeIsUnknown() {
		assertThatIllegalArgumentException()
				.isThrownBy(() -> executor.executeCommandOnSingleNode(COMMAND_CALLBACK, UNKNOWN_CLUSTER_NODE));
	}

	@Test // DATAREDIS-315
	void executeCommandAsyncOnNodesShouldExecuteCommandOnGivenNodes() {

		executor.executeCommandAsyncOnNodes(COMMAND_CALLBACK, Arrays.asList(CLUSTER_NODE_1, CLUSTER_NODE_2));

		verify(connection1).theWheelWeavesAsTheWheelWills();
		verify(connection2).theWheelWeavesAsTheWheelWills();
		verify(connection3, never()).theWheelWeavesAsTheWheelWills();
	}

	@Test // DATAREDIS-315
	void executeCommandAsyncOnNodesShouldExecuteCommandOnGivenNodesByHostAndPort() {

		executor.executeCommandAsyncOnNodes(COMMAND_CALLBACK,
				Arrays.asList(new RedisClusterNode(CLUSTER_NODE_1_HOST, CLUSTER_NODE_1_PORT),
						new RedisClusterNode(CLUSTER_NODE_2_HOST, CLUSTER_NODE_2_PORT)));

		verify(connection1).theWheelWeavesAsTheWheelWills();
		verify(connection2).theWheelWeavesAsTheWheelWills();
		verify(connection3, never()).theWheelWeavesAsTheWheelWills();
	}

	@Test // DATAREDIS-315
	@SuppressWarnings("ConstantConditions")
	void executeCommandAsyncOnNodesShouldExecuteCommandOnGivenNodesByNodeId() {

		executor.executeCommandAsyncOnNodes(COMMAND_CALLBACK,
				Arrays.asList(new RedisClusterNode(CLUSTER_NODE_1.id), CLUSTER_NODE_2_LOOKUP));

		verify(connection1).theWheelWeavesAsTheWheelWills();
		verify(connection2).theWheelWeavesAsTheWheelWills();
		verify(connection3, never()).theWheelWeavesAsTheWheelWills();
	}

	@Test // DATAREDIS-315
	void executeCommandAsyncOnNodesShouldFailOnGivenUnknownNodes() {

		assertThatIllegalArgumentException().isThrownBy(() -> executor.executeCommandAsyncOnNodes(COMMAND_CALLBACK,
				Arrays.asList(new RedisClusterNode("unknown"), CLUSTER_NODE_2_LOOKUP)));
	}

	@Test // DATAREDIS-315
	void executeCommandOnAllNodesShouldExecuteCommandOnEveryKnownClusterNode() {

		executor.executeCommandOnAllNodes(COMMAND_CALLBACK);

		verify(connection1).theWheelWeavesAsTheWheelWills();
		verify(connection2).theWheelWeavesAsTheWheelWills();
		verify(connection3).theWheelWeavesAsTheWheelWills();
	}

	@Test // DATAREDIS-315
	void executeCommandAsyncOnNodesShouldCompleteAndCollectErrorsOfAllNodes() {

		when(connection1.theWheelWeavesAsTheWheelWills()).thenReturn("rand");
		when(connection2.theWheelWeavesAsTheWheelWills())
				.thenThrow(new IllegalStateException("(error) mat lost the dagger..."));
		when(connection3.theWheelWeavesAsTheWheelWills()).thenReturn("perrin");

		try {
			executor.executeCommandOnAllNodes(COMMAND_CALLBACK);
		} catch (ClusterCommandExecutionFailureException ex) {

			assertThat(ex.getSuppressed()).hasSize(1);
			assertThat(ex.getSuppressed()[0]).isInstanceOf(DataAccessException.class);
		}

		verify(connection1).theWheelWeavesAsTheWheelWills();
		verify(connection2).theWheelWeavesAsTheWheelWills();
		verify(connection3).theWheelWeavesAsTheWheelWills();
	}

	@Test // DATAREDIS-315
	void executeCommandAsyncOnNodesShouldCollectResultsCorrectly() {

		when(connection1.theWheelWeavesAsTheWheelWills()).thenReturn("rand");
		when(connection2.theWheelWeavesAsTheWheelWills()).thenReturn("mat");
		when(connection3.theWheelWeavesAsTheWheelWills()).thenReturn("perrin");

		MultiNodeResult<String> result = executor.executeCommandOnAllNodes(COMMAND_CALLBACK);

		assertThat(result.resultsAsList()).contains("rand", "mat", "perrin");
	}

	@Test // DATAREDIS-315, DATAREDIS-467
	void executeMultikeyCommandShouldRunCommandAcrossCluster() {

		// key-1 and key-9 map both to node1
		ArgumentCaptor<byte[]> captor = ArgumentCaptor.forClass(byte[].class);

		when(connection1.bloodAndAshes(captor.capture())).thenReturn("rand").thenReturn("egwene");
		when(connection2.bloodAndAshes(any(byte[].class))).thenReturn("mat");
		when(connection3.bloodAndAshes(any(byte[].class))).thenReturn("perrin");

		MultiNodeResult<String> result = executor.executeMultiKeyCommand(MULTIKEY_CALLBACK,
				new HashSet<>(Arrays.asList("key-1".getBytes(), "key-2".getBytes(), "key-3".getBytes(), "key-9".getBytes())));

		assertThat(result.resultsAsList()).contains("rand", "mat", "perrin", "egwene");

		// check that 2 keys have been routed to node1
		assertThat(captor.getAllValues().size()).isEqualTo(2);
	}

	@Test // DATAREDIS-315
	void executeCommandOnSingleNodeAndFollowRedirect() {

		when(connection1.theWheelWeavesAsTheWheelWills())
				.thenThrow(new MovedException(CLUSTER_NODE_3_HOST, CLUSTER_NODE_3_PORT));

		executor.executeCommandOnSingleNode(COMMAND_CALLBACK, CLUSTER_NODE_1);

		verify(connection1).theWheelWeavesAsTheWheelWills();
		verify(connection3).theWheelWeavesAsTheWheelWills();
		verify(connection2, never()).theWheelWeavesAsTheWheelWills();
	}

	@Test // DATAREDIS-315
	void executeCommandOnSingleNodeAndFollowRedirectButStopsAfterMaxRedirects() {

		when(connection1.theWheelWeavesAsTheWheelWills())
				.thenThrow(new MovedException(CLUSTER_NODE_3_HOST, CLUSTER_NODE_3_PORT));
		when(connection3.theWheelWeavesAsTheWheelWills())
				.thenThrow(new MovedException(CLUSTER_NODE_2_HOST, CLUSTER_NODE_2_PORT));
		when(connection2.theWheelWeavesAsTheWheelWills())
				.thenThrow(new MovedException(CLUSTER_NODE_1_HOST, CLUSTER_NODE_1_PORT));

		executor.setMaxRedirects(4);

		assertThatExceptionOfType(TooManyClusterRedirectionsException.class).isThrownBy(() -> {
			executor.executeCommandOnSingleNode(COMMAND_CALLBACK, CLUSTER_NODE_1);
		});

		verify(connection1, times(2)).theWheelWeavesAsTheWheelWills();
		verify(connection3, times(2)).theWheelWeavesAsTheWheelWills();
		verify(connection2).theWheelWeavesAsTheWheelWills();
	}

	@Test // DATAREDIS-315
	void executeCommandOnArbitraryNodeShouldPickARandomNode() {

		executor.executeCommandOnArbitraryNode(COMMAND_CALLBACK);

		verifyInvocationsAcross("theWheelWeavesAsTheWheelWills", connection1, connection2, connection3);
	}

	@Test // GH-2518
	void collectResultsCompletesSuccessfullyAfterTimeouts() {

		Map<NodeExecution, Future<NodeResult<Object>>> futures = new HashMap<>();

		NodeResult<Object> nodeOneA = new NodeResult<>(CLUSTER_NODE_1, "A");
		NodeResult<Object> nodeTwoB = new NodeResult<>(CLUSTER_NODE_2, "B");
		NodeResult<Object> nodeThreeC = new NodeResult<>(CLUSTER_NODE_3, "C");

		doWithScheduler(scheduler -> {

			futures.put(new NodeExecution(CLUSTER_NODE_1), scheduler.schedule(() -> nodeOneA, 15, TimeUnit.MILLISECONDS));
			futures.put(new NodeExecution(CLUSTER_NODE_2), scheduler.schedule(() -> nodeTwoB, 15, TimeUnit.MILLISECONDS));
			futures.put(new NodeExecution(CLUSTER_NODE_3), scheduler.schedule(() -> nodeThreeC, 15, TimeUnit.MILLISECONDS));

			MultiNodeResult<Object> results = this.executor.collectResults(futures);

			assertThat(results).isNotNull();
			assertThat(results.getResults()).containsExactlyInAnyOrder(nodeOneA, nodeTwoB, nodeThreeC);
		});
	}

	@Test // GH-2518
	void collectResultsFailsWithExecutionException() {

		Map<NodeExecution, Future<NodeResult<Object>>> futures = new HashMap<>();
		NodeResult<Object> nodeOneA = new NodeResult<>(CLUSTER_NODE_1, "A");

		futures.put(new NodeExecution(CLUSTER_NODE_1), CompletableFuture.completedFuture(nodeOneA));
		futures.put(new NodeExecution(CLUSTER_NODE_2),
				CompletableFuture.failedFuture(new IllegalArgumentException("MockError")));

		assertThatExceptionOfType(ClusterCommandExecutionFailureException.class)
				.isThrownBy(() -> this.executor.collectResults(futures)) //
				.withMessage("MockError") //
				.withRootCauseInstanceOf(IllegalArgumentException.class);
	}

	@Test // GH-2518
	void collectResultsFailsWithInterruptedException() throws Throwable {
		TestFramework.runOnce(new CollectResultsInterruptedMultithreadedTestCase(this.executor));
	}

	@Test // GH-2518
	@SuppressWarnings("ConstantConditions")
	void collectResultsCapturesFirstExecutionExceptionOnly() {

		AtomicInteger count = new AtomicInteger(0);
		AtomicInteger exceptionCount = new AtomicInteger(0);

		Map<NodeExecution, Future<NodeResult<Object>>> futures = new HashMap<>();

		CompletableFuture<NodeResult<Object>> doneLater = new CompletableFuture<>() {

			@Override
			public NodeResult<Object> get(long timeout, TimeUnit unit) throws TimeoutException {

				if (count.incrementAndGet() % 2 == 0) {
					return null;
				}
				throw new TimeoutException();
			}
		};

		CompletableFuture<NodeResult<Object>> alwaysFail = new CompletableFuture<>() {

			@Override
			public NodeResult<Object> get(long timeout, TimeUnit unit) throws ExecutionException {

				throw new ExecutionException("TestError",
						new IllegalStateException("MockError" + exceptionCount.getAndIncrement()));
			}
		};

		futures.put(new NodeExecution(CLUSTER_NODE_1), doneLater);
		futures.put(new NodeExecution(CLUSTER_NODE_2), alwaysFail);

		assertThatExceptionOfType(ClusterCommandExecutionFailureException.class)
				.isThrownBy(() -> this.executor.collectResults(futures)) //
				.withMessage("MockError0") //
				.withCauseInstanceOf(InvalidDataAccessApiUsageException.class)
				.withRootCauseInstanceOf(IllegalStateException.class);

		assertThat(exceptionCount).hasValue(1);
	}

	/**
	 * Performs the given action within the scope of a running {@link ScheduledExecutorService}. The scheduler is only
	 * valid during the callback and shut down after this method returns.
	 *
	 * @param callback the action to invoke.
	 */
	private void doWithScheduler(Consumer<ScheduledExecutorService> callback) {

		ScheduledExecutorService scheduler = new ScheduledThreadPoolExecutor(3);
		try {
			callback.accept(scheduler);
		} finally {
			scheduler.shutdown();
		}
	}

	static class MockClusterNodeProvider implements ClusterTopologyProvider {

		@Override
		public ClusterTopology getTopology() {
			return new ClusterTopology(Set.of(CLUSTER_NODE_1, CLUSTER_NODE_2, CLUSTER_NODE_3));
		}
	}

	class MockClusterNodeResourceProvider implements ClusterNodeResourceProvider {

		@Override
		@SuppressWarnings("all")
		public Connection getResourceForSpecificNode(RedisClusterNode clusterNode) {

			return CLUSTER_NODE_1.equals(clusterNode) ? connection1
					: CLUSTER_NODE_2.equals(clusterNode) ? connection2 : CLUSTER_NODE_3.equals(clusterNode) ? connection3 : null;
		}

		@Override
		public void returnResourceForSpecificNode(RedisClusterNode node, Object resource) {}
	}

	interface ConnectionCommandCallback<S> extends ClusterCommandCallback<Connection, S> {}

	interface MultiKeyConnectionCommandCallback<S> extends MultiKeyClusterCommandCallback<Connection, S> {}

	interface Connection {

		String theWheelWeavesAsTheWheelWills();

		String bloodAndAshes(byte[] key);
	}

	static class MovedException extends RuntimeException {

		String host;
		int port;

		MovedException(String host, int port) {
			this.host = host;
			this.port = port;
		}
	}

	@SuppressWarnings("all")
	static class CollectResultsInterruptedMultithreadedTestCase extends MultithreadedTestCase {

		private static final CountDownLatch latch = new CountDownLatch(1);

		private static final Comparator<NodeExecution> NODE_COMPARATOR = Comparator
				.comparing(nodeExecution -> nodeExecution.getNode().getName());

		private final ClusterCommandExecutor clusterCommandExecutor;

		private final Map<NodeExecution, Future<NodeResult<Object>>> futureNodeResults;

		private Future<NodeResult<Object>> mockNodeOneFutureResult;
		private Future<NodeResult<Object>> mockNodeTwoFutureResult;

		private volatile Thread collectResultsThread;

		private CollectResultsInterruptedMultithreadedTestCase(ClusterCommandExecutor clusterCommandExecutor) {
			this.clusterCommandExecutor = clusterCommandExecutor;
			this.futureNodeResults = new ConcurrentSkipListMap<>(NODE_COMPARATOR);
		}

		@Override
		public void initialize() {

			this.mockNodeOneFutureResult = this.futureNodeResults.computeIfAbsent(new NodeExecution(CLUSTER_NODE_1),
					nodeExecution -> new CompletableFuture<>());

			this.mockNodeTwoFutureResult = this.futureNodeResults.computeIfAbsent(new NodeExecution(CLUSTER_NODE_2),
					nodeExecution -> new CompletableFuture<>() {
						@Override
						public NodeResult<Object> get(long timeout, TimeUnit unit)
								throws InterruptedException, TimeoutException, ExecutionException {
							latch.await();
							return super.get(timeout, unit);
						}
					});
		}

		public void thread1() {

			assertTick(0);

			this.collectResultsThread = Thread.currentThread();
			this.collectResultsThread.setName("CollectResults Thread");

			assertThatExceptionOfType(ClusterCommandExecutionFailureException.class)
					.isThrownBy(() -> this.clusterCommandExecutor.collectResults(this.futureNodeResults));

			assertThat(this.collectResultsThread.isInterrupted()).isTrue();
		}

		public void thread2() {

			assertTick(0);

			Thread.currentThread().setName("Interrupting Thread");

			waitForTick(1);

			assertThat(this.collectResultsThread).isNotNull();
			assertThat(this.collectResultsThread.getName()).isEqualTo("CollectResults Thread");

			this.collectResultsThread.interrupt();
		}
	}
}
