/*
 * Copyright 2026-present the original author or authors.
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

import redis.clients.jedis.AbstractPipeline;
import redis.clients.jedis.AbstractTransaction;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.UnifiedJedis;

import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.InvalidDataAccessApiUsageException;

/**
 * Unit tests for {@link UnifiedJedisConnection}.
 *
 * @author Tihomir Mateev
 */
@ExtendWith(MockitoExtension.class)
class UnifiedJedisConnectionUnitTests {

	@Mock
	private UnifiedJedis unifiedJedisMock;

	@Mock
	private AbstractTransaction transactionMock;

	@Mock
	private AbstractPipeline pipelineMock;

	private UnifiedJedisConnection connection;

	@BeforeEach
	void setUp() {
		connection = new UnifiedJedisConnection(unifiedJedisMock);
	}

	@Nested
	class ConstructorTests {

		@Test
		void shouldThrowExceptionWhenJedisIsNull() {
			assertThatIllegalArgumentException()
					.isThrownBy(() -> new UnifiedJedisConnection(null))
					.withMessageContaining("must not be null");
		}

		@Test
		void shouldCreateConnectionSuccessfully() {
			UnifiedJedisConnection conn = new UnifiedJedisConnection(unifiedJedisMock);
			assertThat(conn).isNotNull();
			assertThat(conn.isClosed()).isFalse();
		}
	}

	@Nested
	class CloseTests {

		@Test
		void shouldMarkConnectionAsClosedAfterClose() {
			connection.close();
			assertThat(connection.isClosed()).isTrue();
		}

		@Test
		void shouldCleanupPipelineOnClose() {
			// Set up pipeline via reflection since pipeline field is protected
			connection.pipeline = pipelineMock;

			connection.close();

			verify(pipelineMock).close();
			assertThat(connection.isClosed()).isTrue();
		}

		@Test
		void shouldCleanupTransactionOnClose() {
			connection.transaction = transactionMock;

			connection.close();

			verify(transactionMock).discard();
			verify(transactionMock).close();
			assertThat(connection.isClosed()).isTrue();
		}

		@Test
		void shouldHandleExceptionDuringPipelineCleanup() {
			connection.pipeline = pipelineMock;
			doThrow(new RuntimeException("Pipeline close error")).when(pipelineMock).close();

			// Should not throw
			assertThatNoException().isThrownBy(() -> connection.close());
			assertThat(connection.isClosed()).isTrue();
		}

		@Test
		void shouldHandleExceptionDuringTransactionDiscard() {
			connection.transaction = transactionMock;
			doThrow(new RuntimeException("Discard error")).when(transactionMock).discard();

			// Should not throw
			assertThatNoException().isThrownBy(() -> connection.close());
			verify(transactionMock).close();
			assertThat(connection.isClosed()).isTrue();
		}

		@Test
		void shouldHandleExceptionDuringTransactionClose() {
			connection.transaction = transactionMock;
			doThrow(new RuntimeException("Close error")).when(transactionMock).close();

			// Should not throw
			assertThatNoException().isThrownBy(() -> connection.close());
			assertThat(connection.isClosed()).isTrue();
		}
	}

	@Nested
	class NativeConnectionTests {

		@Test
		void shouldReturnUnifiedJedisAsNativeConnection() {
			assertThat(connection.getNativeConnection()).isSameAs(unifiedJedisMock);
		}

		@Test
		void shouldReturnUnifiedJedisViaGetJedis() {
			assertThat(connection.getJedis()).isSameAs(unifiedJedisMock);
		}
	}

	@Nested
	class SelectTests {

		@Test
		void shouldThrowExceptionOnSelect() {
			assertThatExceptionOfType(InvalidDataAccessApiUsageException.class)
					.isThrownBy(() -> connection.select(1))
					.withMessageContaining("SELECT is not supported with pooled connections");
		}
	}

	@Nested
	class SetClientNameTests {

		@Test
		void shouldThrowExceptionOnSetClientName() {
			assertThatExceptionOfType(InvalidDataAccessApiUsageException.class)
					.isThrownBy(() -> connection.setClientName("test".getBytes()))
					.withMessageContaining("setClientName is not supported with pooled connections");
		}
	}

	@Nested
	class WatchTests {

		@Test
		void shouldCreateTransactionOnFirstWatch() {
			when(unifiedJedisMock.transaction(false)).thenReturn(transactionMock);

			connection.watch("key1".getBytes());

			verify(unifiedJedisMock).transaction(false);
			verify(transactionMock).watch("key1".getBytes());
		}

		@Test
		void shouldReuseTransactionOnSubsequentWatch() {
			when(unifiedJedisMock.transaction(false)).thenReturn(transactionMock);

			connection.watch("key1".getBytes());
			connection.watch("key2".getBytes());

			// transaction(false) should only be called once
			verify(unifiedJedisMock, times(1)).transaction(false);
			verify(transactionMock).watch("key1".getBytes());
			verify(transactionMock).watch("key2".getBytes());
		}

		@Test
		void shouldThrowExceptionWhenWatchCalledDuringMulti() {
			when(unifiedJedisMock.multi()).thenReturn(transactionMock);

			connection.multi();

			assertThatExceptionOfType(InvalidDataAccessApiUsageException.class)
					.isThrownBy(() -> connection.watch("key".getBytes()))
					.withMessageContaining("WATCH is not supported when a transaction is active");
		}
	}

	@Nested
	class UnwatchTests {

		@Test
		void shouldDoNothingWhenNoTransactionActive() {
			// Should not throw
			assertThatNoException().isThrownBy(() -> connection.unwatch());
		}

		@Test
		void shouldUnwatchAndCloseTransactionWhenNotInMulti() {
			when(unifiedJedisMock.transaction(false)).thenReturn(transactionMock);

			connection.watch("key".getBytes());
			connection.unwatch();

			verify(transactionMock).unwatch();
			verify(transactionMock).close();
		}

		@Test
		void shouldUnwatchButNotCloseWhenInMulti() {
			when(unifiedJedisMock.transaction(false)).thenReturn(transactionMock);

			connection.watch("key".getBytes());
			connection.multi(); // This sets isMultiExecuted = true
			connection.unwatch();

			verify(transactionMock).unwatch();
			// close should NOT be called because we're in MULTI state
			verify(transactionMock, never()).close();
		}

		@Test
		void shouldHandleExceptionDuringUnwatchClose() {
			when(unifiedJedisMock.transaction(false)).thenReturn(transactionMock);
			doThrow(new RuntimeException("Close error")).when(transactionMock).close();

			connection.watch("key".getBytes());

			// Should not throw
			assertThatNoException().isThrownBy(() -> connection.unwatch());
		}
	}

	@Nested
	class MultiTests {

		@Test
		void shouldCreateNewTransactionOnMulti() {
			when(unifiedJedisMock.multi()).thenReturn(transactionMock);

			connection.multi();

			verify(unifiedJedisMock).multi();
		}

		@Test
		void shouldSendMultiOnExistingTransactionFromWatch() {
			when(unifiedJedisMock.transaction(false)).thenReturn(transactionMock);

			connection.watch("key".getBytes());
			connection.multi();

			verify(transactionMock).multi();
			verify(unifiedJedisMock, never()).multi(); // Should not create new transaction
		}

		@Test
		void shouldBeIdempotentWhenMultiCalledTwice() {
			when(unifiedJedisMock.multi()).thenReturn(transactionMock);

			connection.multi();
			connection.multi(); // Second call should be no-op

			verify(unifiedJedisMock, times(1)).multi();
		}

		@Test
		void shouldThrowExceptionWhenPipelineIsOpen() {
			connection.pipeline = pipelineMock;

			assertThatExceptionOfType(InvalidDataAccessApiUsageException.class)
					.isThrownBy(() -> connection.multi())
					.withMessageContaining("Cannot use Transaction while a pipeline is open");
		}
	}

	@Nested
	class ExecTests {

		@Test
		void shouldThrowExceptionWhenNoTransactionActive() {
			// exec() throws when no transaction is active - the exception type depends on internal state
			assertThatException().isThrownBy(() -> connection.exec());
		}

		@Test
		void shouldExecuteTransactionAndCloseIt() {
			when(unifiedJedisMock.multi()).thenReturn(transactionMock);
			when(transactionMock.exec()).thenReturn(Collections.emptyList());

			connection.multi();
			connection.exec();

			verify(transactionMock).exec();
			verify(transactionMock).close();
		}

		@Test
		void shouldCloseTransactionEvenWhenExecFails() {
			when(unifiedJedisMock.multi()).thenReturn(transactionMock);
			when(transactionMock.exec()).thenThrow(new RuntimeException("Exec failed"));

			connection.multi();

			assertThatException().isThrownBy(() -> connection.exec());

			verify(transactionMock).close();
		}
	}

	@Nested
	class DiscardTests {

		@Test
		void shouldThrowExceptionWhenNoTransactionActive() {
			// discard() throws when no transaction is active
			assertThatException().isThrownBy(() -> connection.discard());
		}

		@Test
		void shouldDiscardTransactionAndCloseIt() {
			when(unifiedJedisMock.multi()).thenReturn(transactionMock);

			connection.multi();
			connection.discard();

			verify(transactionMock).discard();
			verify(transactionMock).close();
		}

		@Test
		void shouldCloseTransactionEvenWhenDiscardFails() {
			when(unifiedJedisMock.multi()).thenReturn(transactionMock);
			doThrow(new RuntimeException("Discard failed")).when(transactionMock).discard();

			connection.multi();

			assertThatException().isThrownBy(() -> connection.discard());

			verify(transactionMock).close();
		}
	}

	@Nested
	class ClosePipelineTests {

		@Test
		void shouldReturnEmptyListWhenNoPipelineActive() {
			List<?> result = connection.closePipeline();
			assertThat(result).isEmpty();
		}

		@Test
		void shouldClosePipeline() {
			Pipeline pipeline = mock(Pipeline.class);
			connection.pipeline = pipeline;

			connection.closePipeline();

			verify(pipeline).close();
		}

		@Test
		void shouldClosePipelineEvenWhenSyncFails() {
			Pipeline pipeline = mock(Pipeline.class);
			doThrow(new RuntimeException("Sync failed")).when(pipeline).sync();
			connection.pipeline = pipeline;

			assertThatException().isThrownBy(() -> connection.closePipeline());

			verify(pipeline).close();
		}
	}

	@Nested
	class OpenPipelineTests {

		@Test
		void shouldCreatePipelineWhenOpened() {
			Pipeline pipelineMock = mock(Pipeline.class);
			when(unifiedJedisMock.pipelined()).thenReturn(pipelineMock);

			connection.openPipeline();

			assertThat(connection.isPipelined()).isTrue();
		}

		@Test
		void shouldThrowExceptionWhenOpeningPipelineDuringTransaction() {
			when(unifiedJedisMock.multi()).thenReturn(transactionMock);

			connection.multi();

			assertThatExceptionOfType(InvalidDataAccessApiUsageException.class)
					.isThrownBy(() -> connection.openPipeline());
		}
	}

	@Nested
	class StateTests {

		@Test
		void shouldReportNotQueueingInitially() {
			assertThat(connection.isQueueing()).isFalse();
		}

		@Test
		void shouldReportQueueingAfterMulti() {
			when(unifiedJedisMock.multi()).thenReturn(transactionMock);

			connection.multi();

			assertThat(connection.isQueueing()).isTrue();
		}

		@Test
		void shouldReportNotQueueingAfterExec() {
			when(unifiedJedisMock.multi()).thenReturn(transactionMock);
			when(transactionMock.exec()).thenReturn(Collections.emptyList());

			connection.multi();
			connection.exec();

			assertThat(connection.isQueueing()).isFalse();
		}

		@Test
		void shouldReportNotQueueingAfterDiscard() {
			when(unifiedJedisMock.multi()).thenReturn(transactionMock);

			connection.multi();
			connection.discard();

			assertThat(connection.isQueueing()).isFalse();
		}

		@Test
		void shouldReportNotPipelinedInitially() {
			assertThat(connection.isPipelined()).isFalse();
		}

		@Test
		void shouldReportPipelinedAfterOpenPipeline() {
			Pipeline pipelineMock = mock(Pipeline.class);
			when(unifiedJedisMock.pipelined()).thenReturn(pipelineMock);

			connection.openPipeline();

			assertThat(connection.isPipelined()).isTrue();
		}

		@Test
		void shouldReportNotPipelinedAfterClosePipeline() {
			Pipeline pipelineMock = mock(Pipeline.class);
			when(unifiedJedisMock.pipelined()).thenReturn(pipelineMock);

			connection.openPipeline();
			connection.closePipeline();

			assertThat(connection.isPipelined()).isFalse();
		}
	}

	@Nested
	class WatchThenMultiThenExecTests {

		@Test
		void shouldExecuteFullWatchMultiExecFlow() {
			when(unifiedJedisMock.transaction(false)).thenReturn(transactionMock);
			when(transactionMock.exec()).thenReturn(Collections.emptyList());

			connection.watch("key".getBytes());
			connection.multi();
			connection.exec();

			verify(unifiedJedisMock).transaction(false);
			verify(transactionMock).watch("key".getBytes());
			verify(transactionMock).multi();
			verify(transactionMock).exec();
			verify(transactionMock).close();
		}

		@Test
		void shouldExecuteFullWatchMultiDiscardFlow() {
			when(unifiedJedisMock.transaction(false)).thenReturn(transactionMock);

			connection.watch("key".getBytes());
			connection.multi();
			connection.discard();

			verify(unifiedJedisMock).transaction(false);
			verify(transactionMock).watch("key".getBytes());
			verify(transactionMock).multi();
			verify(transactionMock).discard();
			verify(transactionMock).close();
		}

		@Test
		void shouldHandleWatchThenUnwatch() {
			when(unifiedJedisMock.transaction(false)).thenReturn(transactionMock);

			connection.watch("key".getBytes());
			connection.unwatch();

			verify(transactionMock).watch("key".getBytes());
			verify(transactionMock).unwatch();
			verify(transactionMock).close();
		}
	}
}

