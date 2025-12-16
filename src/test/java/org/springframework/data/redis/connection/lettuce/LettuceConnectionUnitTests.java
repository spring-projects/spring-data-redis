/*
 * Copyright 2014-2025 the original author or authors.
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

import io.lettuce.core.*;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.output.ScanOutput;
import io.lettuce.core.output.StatusOutput;
import io.lettuce.core.protocol.AsyncCommand;
import io.lettuce.core.protocol.Command;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.CommandType;

import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import org.springframework.dao.InvalidDataAccessResourceUsageException;
import org.springframework.data.redis.connection.AbstractConnectionUnitTestBase;
import org.springframework.data.redis.connection.RedisServerCommands.ShutdownOption;
import org.springframework.data.redis.connection.RedisStreamCommands.TrimOptions;
import org.springframework.data.redis.connection.RedisStreamCommands.XAddOptions;
import org.springframework.data.redis.connection.RedisStreamCommands.XClaimOptions;
import org.springframework.data.redis.connection.RedisStreamCommands.XTrimOptions;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.zset.Tuple;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.KeyScanOptions;
import org.springframework.test.util.ReflectionTestUtils;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 */
class LettuceConnectionUnitTests {

	protected LettuceConnection connection;
	private RedisClient clientMock;
	StatefulRedisConnection<byte[], byte[]> statefulConnectionMock;
	RedisAsyncCommands<byte[], byte[]> asyncCommandsMock;
	RedisCommands<byte[], byte[]> commandsMock;

	@BeforeEach
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void setUp() throws InvocationTargetException, IllegalAccessException {

		clientMock = mock(RedisClient.class);
		statefulConnectionMock = mock(StatefulRedisConnection.class);
		when(clientMock.connect((RedisCodec) any())).thenReturn(statefulConnectionMock);

		asyncCommandsMock = Mockito.mock(RedisAsyncCommands.class, invocation -> {

			if (invocation.getMethod().getReturnType().equals(RedisFuture.class)) {

				Command<?, ?, ?> cmd = new Command<>(CommandType.PING, new StatusOutput<>(StringCodec.UTF8));
				AsyncCommand<?, ?, ?> async = new AsyncCommand<>(cmd);
				async.complete();

				return async;
			}
			return null;
		});
		commandsMock = Mockito.mock(RedisCommands.class);

		when(statefulConnectionMock.async()).thenReturn(asyncCommandsMock);
		when(statefulConnectionMock.sync()).thenReturn(commandsMock);
		connection = new LettuceConnection(0, clientMock);
	}

	@Nested
	@SuppressWarnings({ "rawtypes", "deprecation" })
	class BasicUnitTests extends AbstractConnectionUnitTestBase<RedisAsyncCommands> {

		@Test // DATAREDIS-184
		public void shutdownWithNullOptionsIsCalledCorrectly() {

			connection.shutdown(null);
			verify(asyncCommandsMock).shutdown(true);
		}

		@Test // DATAREDIS-184
		public void shutdownWithNosaveOptionIsCalledCorrectly() {

			connection.shutdown(ShutdownOption.NOSAVE);
			verify(asyncCommandsMock).shutdown(false);
		}

		@Test // DATAREDIS-184
		public void shutdownWithSaveOptionIsCalledCorrectly() {

			connection.shutdown(ShutdownOption.SAVE);
			verify(asyncCommandsMock).shutdown(true);
		}

		@Test // DATAREDIS-267
		public void killClientShouldDelegateCallCorrectly() {

			String ipPort = "127.0.0.1:1001";
			connection.killClient("127.0.0.1", 1001);
			verify(asyncCommandsMock).clientKill(eq(ipPort));
		}

		@Test // DATAREDIS-270
		public void getClientNameShouldSendRequestCorrectly() {

			connection.getClientName();
			verify(asyncCommandsMock).clientGetname();
		}

		@Test // DATAREDIS-277
		void replicaOfShouldThrowExectpionWhenCalledForNullHost() {
			assertThatIllegalArgumentException().isThrownBy(() -> connection.replicaOf(null, 0));
		}

		@Test // DATAREDIS-277
		public void replicaOfShouldBeSentCorrectly() {

			connection.replicaOf("127.0.0.1", 1001);
			verify(asyncCommandsMock).slaveof(eq("127.0.0.1"), eq(1001));
		}

		@Test // DATAREDIS-277
		public void replicaOfNoOneShouldBeSentCorrectly() {

			connection.replicaOfNoOne();
			verify(asyncCommandsMock).slaveofNoOne();
		}

		@Test // DATAREDIS-348
		void shouldThrowExceptionWhenAccessingRedisSentinelsCommandsWhenNoSentinelsConfigured() {
			assertThatExceptionOfType(InvalidDataAccessResourceUsageException.class)
					.isThrownBy(() -> connection.getSentinelConnection());
		}

		@Test // DATAREDIS-431, GH-2984
		void dbIndexShouldBeSetWhenObtainingConnection() {

			connection = new LettuceConnection(null, 0, clientMock, 0);
			connection.select(1);
			connection.getNativeConnection();

			verify(asyncCommandsMock).dispatch(eq(CommandType.SELECT), any(), any());
			verifyNoInteractions(commandsMock);
		}

		@Test // DATAREDIS-603
		void translatesUnknownExceptions() {

			IllegalArgumentException exception = new IllegalArgumentException("Aw, snap");

			when(asyncCommandsMock.set(any(), any())).thenThrow(exception);
			connection = new LettuceConnection(null, 0, clientMock, 1);

			assertThatThrownBy(() -> connection.set("foo".getBytes(), "bar".getBytes())).hasRootCause(exception);
		}

		@Test // DATAREDIS-603
		void translatesPipelineUnknownExceptions() {

			IllegalArgumentException exception = new IllegalArgumentException("Aw, snap");

			when(asyncCommandsMock.set(any(byte[].class), any(byte[].class))).thenThrow(exception);
			connection = new LettuceConnection(null, 0, clientMock, 1);
			connection.openPipeline();

			assertThatThrownBy(() -> connection.set("foo".getBytes(), "bar".getBytes())).hasRootCause(exception);
		}

		@Test // DATAREDIS-1122
		void xaddShouldPassOptionsToConverter() {

			MapRecord<byte[], byte[], byte[]> record = MapRecord.create("key".getBytes(), Collections.emptyMap());

			connection.streamCommands().xAdd(record, XAddOptions.maxlen(100));
			ArgumentCaptor<XAddArgs> args = ArgumentCaptor.forClass(XAddArgs.class);
			verify(asyncCommandsMock).xadd(any(), args.capture(), anyMap());

			assertThat(args.getValue()).isNotNull();
		}

		@Test // GH-3232
		void xtrimShouldPassOptionsToConverter() {

			connection.streamCommands().xTrim("key".getBytes(), XTrimOptions.trim(TrimOptions.maxLen(100)));
			ArgumentCaptor<XTrimArgs> args = ArgumentCaptor.forClass(XTrimArgs.class);
			verify(asyncCommandsMock).xtrim(any(), args.capture());

			assertThat(args.getValue()).isNotNull();
		}

		@Test // DATAREDIS-1226
		void xClaimShouldNotAddJustIdFlagToArgs() {

			connection.streamCommands().xClaim("key".getBytes(), "group", "owner",
					XClaimOptions.minIdle(Duration.ofMillis(100)).ids("1-1"));
			ArgumentCaptor<XClaimArgs> args = ArgumentCaptor.forClass(XClaimArgs.class);

			verify(asyncCommandsMock).xclaim(any(), any(), args.capture(), any());

			assertThat(ReflectionTestUtils.getField(args.getValue(), "justid")).isEqualTo(false);
		}

		@Test // DATAREDIS-1226
		void xClaimJustIdShouldAddJustIdFlagToArgs() {

			connection.streamCommands().xClaimJustId("key".getBytes(), "group", "owner",
					XClaimOptions.minIdle(Duration.ofMillis(100)).ids("1-1"));
			ArgumentCaptor<XClaimArgs> args = ArgumentCaptor.forClass(XClaimArgs.class);

			verify(asyncCommandsMock).xclaim(any(), any(), args.capture(), any());

			assertThat(ReflectionTestUtils.getField(args.getValue(), "justid")).isEqualTo(true);
		}

		@Test // GH-1979
		void executeShouldPassThruCustomCommands() {

			Command<byte[], byte[], String> command = new Command<>(new LettuceConnection.CustomCommandType("FOO.BAR"),
					new StatusOutput<>(ByteArrayCodec.INSTANCE));
			AsyncCommand<byte[], byte[], String> future = new AsyncCommand<>(command);
			future.complete();

			when(asyncCommandsMock.dispatch(any(), any(), any())).thenReturn((RedisFuture) future);

			connection.execute("foo.bar", command.getOutput());

			verify(asyncCommandsMock).dispatch(eq(command.getType()), eq(command.getOutput()), any(CommandArgs.class));
		}

		@Test // GH-2796
		void scanShouldOperateUponUnsigned64BitCursorId() {

			String cursorId = "9286422431637962824";
			KeyScanCursor<byte[]> sc = new KeyScanCursor<>() {
				@Override
				public List<byte[]> getKeys() {
					return List.of("spring".getBytes());
				}
			};
			sc.setCursor(cursorId);
			sc.setFinished(false);

			Command<byte[], byte[], KeyScanCursor<byte[]>> command = new Command<>(
					new LettuceConnection.CustomCommandType("SCAN"), new ScanOutput<>(ByteArrayCodec.INSTANCE, sc) {
						@Override
						protected void setOutput(ByteBuffer bytes) {

						}
					});
			AsyncCommand<byte[], byte[], KeyScanCursor<byte[]>> future = new AsyncCommand<>(command);
			future.complete();

			when(asyncCommandsMock.scan(any(ScanCursor.class), any(ScanArgs.class))).thenReturn(future, future);

			Cursor<byte[]> cursor = connection.scan(KeyScanOptions.NONE);
			cursor.next(); // initial
			assertThat(cursor.getCursorId()).isEqualTo(Long.parseUnsignedLong(cursorId));

			cursor.next(); // fetch next
			ArgumentCaptor<ScanCursor> captor = ArgumentCaptor.forClass(ScanCursor.class);
			verify(asyncCommandsMock, times(2)).scan(captor.capture(), any(ScanArgs.class));
			assertThat(captor.getAllValues()).map(ScanCursor::getCursor).containsExactly("0", cursorId);
		}

		@Test // GH-2796
		void sScanShouldOperateUponUnsigned64BitCursorId() {

			String cursorId = "9286422431637962824";
			ValueScanCursor<byte[]> sc = new ValueScanCursor<>() {
				@Override
				public List<byte[]> getValues() {
					return List.of("spring".getBytes());
				}
			};
			sc.setCursor(cursorId);
			sc.setFinished(false);

			Command<byte[], byte[], ValueScanCursor<byte[]>> command = new Command<>(
					new LettuceConnection.CustomCommandType("SSCAN"), new ScanOutput<>(ByteArrayCodec.INSTANCE, sc) {
						@Override
						protected void setOutput(ByteBuffer bytes) {

						}
					});
			AsyncCommand<byte[], byte[], ValueScanCursor<byte[]>> future = new AsyncCommand<>(command);
			future.complete();

			when(asyncCommandsMock.sscan(any(byte[].class), any(ScanCursor.class), any(ScanArgs.class))).thenReturn(future,
					future);

			Cursor<byte[]> cursor = connection.setCommands().sScan("key".getBytes(), KeyScanOptions.NONE);
			cursor.next(); // initial
			assertThat(cursor.getId()).isEqualTo(Cursor.CursorId.of(cursorId));

			cursor.next(); // fetch next
			ArgumentCaptor<ScanCursor> captor = ArgumentCaptor.forClass(ScanCursor.class);
			verify(asyncCommandsMock, times(2)).sscan(any(byte[].class), captor.capture(), any(ScanArgs.class));
			assertThat(captor.getAllValues()).map(ScanCursor::getCursor).containsExactly("0", cursorId);
		}

		@Test // GH-2796
		void zScanShouldOperateUponUnsigned64BitCursorId() {

			String cursorId = "9286422431637962824";
			ScoredValueScanCursor<byte[]> sc = new ScoredValueScanCursor<>() {
				@Override
				public List<ScoredValue<byte[]>> getValues() {
					return List.of(ScoredValue.just(10D, "spring".getBytes()));
				}
			};
			sc.setCursor(cursorId);
			sc.setFinished(false);

			Command<byte[], byte[], ScoredValueScanCursor<byte[]>> command = new Command<>(
					new LettuceConnection.CustomCommandType("ZSCAN"), new ScanOutput<>(ByteArrayCodec.INSTANCE, sc) {
						@Override
						protected void setOutput(ByteBuffer bytes) {

						}
					});
			AsyncCommand<byte[], byte[], ScoredValueScanCursor<byte[]>> future = new AsyncCommand<>(command);
			future.complete();

			when(asyncCommandsMock.zscan(any(byte[].class), any(ScanCursor.class), any(ScanArgs.class))).thenReturn(future,
					future);

			Cursor<Tuple> cursor = connection.zSetCommands().zScan("key".getBytes(), KeyScanOptions.NONE);
			cursor.next(); // initial
			assertThat(cursor.getCursorId()).isEqualTo(Long.parseUnsignedLong(cursorId));

			cursor.next(); // fetch next
			ArgumentCaptor<ScanCursor> captor = ArgumentCaptor.forClass(ScanCursor.class);
			verify(asyncCommandsMock, times(2)).zscan(any(byte[].class), captor.capture(), any(ScanArgs.class));
			assertThat(captor.getAllValues()).map(ScanCursor::getCursor).containsExactly("0", cursorId);
		}

		@Test // GH-2796
		void hScanShouldOperateUponUnsigned64BitCursorId() {

			String cursorId = "9286422431637962824";
			MapScanCursor<byte[], byte[]> sc = new MapScanCursor<>() {
				@Override
				public Map<byte[], byte[]> getMap() {
					return Map.of("spring".getBytes(), "data".getBytes());
				}
			};
			sc.setCursor(cursorId);
			sc.setFinished(false);

			Command<byte[], byte[], MapScanCursor<byte[], byte[]>> command = new Command<>(
					new LettuceConnection.CustomCommandType("HSCAN"), new ScanOutput<>(ByteArrayCodec.INSTANCE, sc) {
						@Override
						protected void setOutput(ByteBuffer bytes) {

						}
					});
			AsyncCommand<byte[], byte[], MapScanCursor<byte[], byte[]>> future = new AsyncCommand<>(command);
			future.complete();

			when(asyncCommandsMock.hscan(any(byte[].class), any(ScanCursor.class), any(ScanArgs.class))).thenReturn(future,
					future);

			Cursor<Entry<byte[], byte[]>> cursor = connection.hashCommands().hScan("key".getBytes(), KeyScanOptions.NONE);
			cursor.next(); // initial
			assertThat(cursor.getCursorId()).isEqualTo(Long.parseUnsignedLong(cursorId));

			cursor.next(); // fetch next
			ArgumentCaptor<ScanCursor> captor = ArgumentCaptor.forClass(ScanCursor.class);
			verify(asyncCommandsMock, times(2)).hscan(any(byte[].class), captor.capture(), any(ScanArgs.class));
			assertThat(captor.getAllValues()).map(ScanCursor::getCursor).containsExactly("0", cursorId);
		}

	}

	@Nested
	class LettucePipelineConnectionUnitTests extends BasicUnitTests {

		@BeforeEach
		public void setUp() throws InvocationTargetException, IllegalAccessException {
			connection.openPipeline();
		}

		@Test // DATAREDIS-528
		@Disabled("SHUTDOWN not supported in pipeline")
		public void shutdownWithSaveOptionIsCalledCorrectly() {

		}

		@Test // DATAREDIS-528
		@Disabled("SHUTDOWN not supported in pipeline")
		public void shutdownWithNosaveOptionIsCalledCorrectly() {

		}

		@Test // DATAREDIS-528
		public void replicaOfShouldBeSentCorrectly() {

			connection.replicaOf("127.0.0.1", 1001);
			verify(asyncCommandsMock).slaveof(eq("127.0.0.1"), eq(1001));
		}

		@Test // DATAREDIS-528
		@Disabled("SHUTDOWN not supported in pipeline")
		public void shutdownWithNullOptionsIsCalledCorrectly() {

		}

		@Test // DATAREDIS-528
		public void killClientShouldDelegateCallCorrectly() {

			String ipPort = "127.0.0.1:1001";
			connection.killClient("127.0.0.1", 1001);
			verify(asyncCommandsMock).clientKill(eq(ipPort));
		}

		@Test // DATAREDIS-528
		public void replicaOfNoOneShouldBeSentCorrectly() {

			connection.replicaOfNoOne();
			verify(asyncCommandsMock).slaveofNoOne();
		}

		@Test // DATAREDIS-528
		public void getClientNameShouldSendRequestCorrectly() {

			connection.getClientName();
			verify(asyncCommandsMock).clientGetname();
		}

		@Test
		@Disabled("scan not supported in pipeline")
		void scanShouldOperateUponUnsigned64BitCursorId() {

		}

		@Test
		@Disabled("scan not supported in pipeline")
		void sScanShouldOperateUponUnsigned64BitCursorId() {

		}

		@Test
		@Disabled("scan not supported in pipeline")
		void zScanShouldOperateUponUnsigned64BitCursorId() {

		}

		@Test
		@Disabled("scan not supported in pipeline")
		void hScanShouldOperateUponUnsigned64BitCursorId() {

		}
	}
}
