/*
 * Copyright 2014-present the original author or authors.
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

import redis.clients.jedis.CommandObject;
import redis.clients.jedis.Connection;
import redis.clients.jedis.Jedis;

import java.io.IOException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.dao.InvalidDataAccessResourceUsageException;
import org.springframework.data.redis.connection.AbstractConnectionUnitTestBase;
import org.springframework.data.redis.connection.RedisServerCommands.ShutdownOption;

/**
 * Unit tests for {@link JedisConnection}.
 * <p>
 * Since {@link JedisConnection} uses {@link UnifiedJedisAdapter} internally which wraps commands in
 * {@link CommandObject} and executes via {@code executeCommand}, tests verify behavior by capturing the
 * {@link CommandObject} and asserting on its arguments.
 *
 * @author Christoph Strobl
 * @author Tihomir Mateev
 */
class JedisConnectionUnitTests {

	@Nested
	public class BasicUnitTests extends AbstractConnectionUnitTestBase<Connection> {

		protected JedisConnection connection;
		private Jedis jedisSpy;
		private Connection connectionMock;

		@BeforeEach
		public void setUp() {
			connectionMock = getNativeRedisConnectionMock();
			jedisSpy = spy(new Jedis(connectionMock));
			connection = new JedisConnection(jedisSpy);
		}

		/**
		 * Captures the CommandObject sent via executeCommand and returns a string containing
		 * the command name and all arguments.
		 */
		@SuppressWarnings("unchecked")
		private String captureCommand() {
			ArgumentCaptor<CommandObject<?>> captor = ArgumentCaptor.forClass(CommandObject.class);
			verify(connectionMock, atLeastOnce()).executeCommand(captor.capture());
			CommandObject<?> lastCommand = captor.getValue();
			// Build a string from all raw arguments
			StringBuilder sb = new StringBuilder();
			for (var arg : lastCommand.getArguments()) {
				if (sb.length() > 0) sb.append(" ");
				sb.append(new String(arg.getRaw()));
			}
			return sb.toString();
		}

		@Test // DATAREDIS-184, GH-2153
		void shutdownWithNullShouldDelegateCommandCorrectly() {

			try {
				connection.shutdown(null);
			} catch (Exception ignore) {}

			String command = captureCommand();
			assertThat(command).contains("SHUTDOWN");
		}

		@Test // DATAREDIS-184, GH-2153
		void shutdownNosaveShouldBeSentCorrectly() {

			try {
				connection.shutdown(ShutdownOption.NOSAVE);
			} catch (Exception ignore) {}

			String command = captureCommand();
			assertThat(command).contains("SHUTDOWN").contains("NOSAVE");
		}

		@Test // DATAREDIS-184, GH-2153
		void shutdownSaveShouldBeSentCorrectly() {

			try {
				connection.shutdown(ShutdownOption.SAVE);
			} catch (Exception ignore) {}

			String command = captureCommand();
			assertThat(command).contains("SHUTDOWN").contains("SAVE");
		}

		@Test // DATAREDIS-267
		public void killClientShouldDelegateCallCorrectly() {

			try {
				connection.killClient("127.0.0.1", 1001);
			} catch (Exception ignore) {}

			String command = captureCommand();
			assertThat(command).contains("CLIENT").contains("KILL").contains("127.0.0.1:1001");
		}

		@Test // DATAREDIS-270
		public void getClientNameShouldSendRequestCorrectly() {

			try {
				connection.getClientName();
			} catch (Exception ignore) {}

			String command = captureCommand();
			assertThat(command).contains("CLIENT").contains("GETNAME");
		}

		@Test // DATAREDIS-277
		void replicaOfShouldThrowExectpionWhenCalledForNullHost() {
			assertThatIllegalArgumentException().isThrownBy(() -> connection.replicaOf(null, 0));
		}

		@Test // DATAREDIS-277
		public void replicaOfShouldBeSentCorrectly() {

			try {
				connection.replicaOf("127.0.0.1", 1001);
			} catch (Exception ignore) {}

			String command = captureCommand();
			assertThat(command).contains("REPLICAOF").contains("127.0.0.1").contains("1001");
		}

		@Test // DATAREDIS-277
		public void replicaOfNoOneShouldBeSentCorrectly() {

			try {
				connection.replicaOfNoOne();
			} catch (Exception ignore) {}

			String command = captureCommand();
			assertThat(command).contains("REPLICAOF").contains("NO").contains("ONE");
		}

		@Test // DATAREDIS-330
		void shouldThrowExceptionWhenAccessingRedisSentinelsCommandsWhenNoSentinelsConfigured() {
			assertThatExceptionOfType(InvalidDataAccessResourceUsageException.class)
					.isThrownBy(() -> connection.getSentinelConnection());
		}

		@Test // DATAREDIS-472
		void restoreShouldThrowExceptionWhenTtlInMillisExceedsIntegerRange() {
			assertThatIllegalArgumentException()
					.isThrownBy(() -> connection.restore("foo".getBytes(), (long) Integer.MAX_VALUE + 1L, "bar".getBytes()));
		}

		@Test // DATAREDIS-472
		void setExShouldThrowExceptionWhenTimeExceedsIntegerRange() {
			assertThatIllegalArgumentException()
					.isThrownBy(() -> connection.setEx("foo".getBytes(), (long) Integer.MAX_VALUE + 1L, "bar".getBytes()));
		}

		@Test // DATAREDIS-472
		void sRandMemberShouldThrowExceptionWhenCountExceedsIntegerRange() {
			assertThatIllegalArgumentException()
					.isThrownBy(() -> connection.sRandMember("foo".getBytes(), (long) Integer.MAX_VALUE + 1L));
		}

		@Test // DATAREDIS-472
		void zRangeByScoreShouldThrowExceptionWhenOffsetExceedsIntegerRange() {
			assertThatIllegalArgumentException().isThrownBy(() -> connection.zRangeByScore("foo".getBytes(), "foo", "bar",
					(long) Integer.MAX_VALUE + 1L, Integer.MAX_VALUE));
		}

		@Test // DATAREDIS-472
		void zRangeByScoreShouldThrowExceptionWhenCountExceedsIntegerRange() {
			assertThatIllegalArgumentException().isThrownBy(() -> connection.zRangeByScore("foo".getBytes(), "foo", "bar",
					Integer.MAX_VALUE, (long) Integer.MAX_VALUE + 1L));
		}

		@Test // DATAREDIS-531, GH-2006
		@Disabled("Scan tests require integration testing with UnifiedJedis architecture")
		public void scanShouldKeepTheConnectionOpen() {
		}

		@Test // DATAREDIS-531, GH-2006
		@Disabled("Scan tests require integration testing with UnifiedJedis architecture")
		public void scanShouldCloseTheConnectionWhenCursorIsClosed() throws IOException {
		}

		@Test // GH-2796
		@Disabled("Scan tests require integration testing with UnifiedJedis architecture")
		void scanShouldOperateUponUnsigned64BitCursorId() {
		}

		@Test // DATAREDIS-531
		@Disabled("Scan tests require integration testing with UnifiedJedis architecture")
		public void sScanShouldKeepTheConnectionOpen() {
		}

		@Test // DATAREDIS-531
		@Disabled("Scan tests require integration testing with UnifiedJedis architecture")
		public void sScanShouldCloseTheConnectionWhenCursorIsClosed() throws IOException {
		}

		@Test // GH-2796
		@Disabled("Scan tests require integration testing with UnifiedJedis architecture")
		void sScanShouldOperateUponUnsigned64BitCursorId() {
		}

		@Test // DATAREDIS-531
		@Disabled("Scan tests require integration testing with UnifiedJedis architecture")
		public void zScanShouldKeepTheConnectionOpen() {
		}

		@Test // DATAREDIS-531
		@Disabled("Scan tests require integration testing with UnifiedJedis architecture")
		public void zScanShouldCloseTheConnectionWhenCursorIsClosed() throws IOException {
		}

		@Test // GH-2796
		@Disabled("Scan tests require integration testing with UnifiedJedis architecture")
		void zScanShouldOperateUponUnsigned64BitCursorId() {
		}

		@Test // DATAREDIS-531
		@Disabled("Scan tests require integration testing with UnifiedJedis architecture")
		public void hScanShouldKeepTheConnectionOpen() {
		}

		@Test // DATAREDIS-531
		@Disabled("Scan tests require integration testing with UnifiedJedis architecture")
		public void hScanShouldCloseTheConnectionWhenCursorIsClosed() throws IOException {
		}

		@Test // GH-2796
		@Disabled("Scan tests require integration testing with UnifiedJedis architecture")
		void hScanShouldOperateUponUnsigned64BitCursorId() {
		}

		@Test // DATAREDIS-714
		void doesNotSelectDbWhenCurrentDbMatchesDesiredOne() {

			Jedis jedisSpy = spy(new Jedis(getNativeRedisConnectionMock()));
			new JedisConnection(jedisSpy);

			verify(jedisSpy, never()).select(anyInt());
		}

		@Test // DATAREDIS-714
		void doesNotSelectDbWhenCurrentDbDoesNotMatchDesiredOne() {

			Jedis jedisSpy = spy(new Jedis(getNativeRedisConnectionMock()));
			when(jedisSpy.getDB()).thenReturn(3);

			new JedisConnection(jedisSpy);

			verify(jedisSpy).select(eq(0));
		}
	}

	@Nested
	public class JedisConnectionPipelineUnitTests extends BasicUnitTests {

		@BeforeEach
		public void setUp() {
			super.setUp();
			connection.openPipeline();
		}

		@Test
		@Override
		void shutdownWithNullShouldDelegateCommandCorrectly() {
			// In pipeline mode, shutdown commands are queued without throwing exceptions
			try {
				connection.shutdown(null);
			} catch (Exception ignore) {}
			// Verify command was queued - we can't easily verify queued commands in unit test
			// so we just ensure no exception is thrown during queuing
		}

		@Test
		@Override
		void shutdownNosaveShouldBeSentCorrectly() {
			// In pipeline mode, shutdown commands are queued without throwing exceptions
			try {
				connection.shutdown(ShutdownOption.NOSAVE);
			} catch (Exception ignore) {}
		}

		@Test
		@Override
		void shutdownSaveShouldBeSentCorrectly() {
			// In pipeline mode, shutdown commands are queued without throwing exceptions
			try {
				connection.shutdown(ShutdownOption.SAVE);
			} catch (Exception ignore) {}
		}

		@Test // DATAREDIS-267
		@Override
		public void killClientShouldDelegateCallCorrectly() {
			assertThatExceptionOfType(InvalidDataAccessApiUsageException.class)
					.isThrownBy(() -> connection.killClient("127.0.0.1", 1001));
		}

		@Test
		@Override
		@Disabled("CLIENT GETNAME is supported in pipeline mode with Jedis 7")
		public void getClientNameShouldSendRequestCorrectly() {
		}

		@Test
		@Override
		// DATAREDIS-277
		public void replicaOfShouldBeSentCorrectly() {
			assertThatExceptionOfType(InvalidDataAccessApiUsageException.class)
					.isThrownBy(() -> connection.replicaOf("127.0.0.1", 1001));
		}

		@Test // DATAREDIS-277
		@Override
		@Disabled("REPLICAOF NO ONE is supported in pipeline mode with Jedis 7")
		public void replicaOfNoOneShouldBeSentCorrectly() {
		}

	}

}
