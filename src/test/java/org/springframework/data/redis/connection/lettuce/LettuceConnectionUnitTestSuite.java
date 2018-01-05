/*
 * Copyright 2014-2018 the original author or authors.
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

import static org.hamcrest.Matchers.*;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.any;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.protocol.RedisCommand;

import java.lang.reflect.InvocationTargetException;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.mockito.Mockito;
import org.springframework.dao.InvalidDataAccessResourceUsageException;
import org.springframework.data.redis.connection.AbstractConnectionUnitTestBase;
import org.springframework.data.redis.connection.RedisServerCommands.ShutdownOption;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionUnitTestSuite.LettuceConnectionUnitTests;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionUnitTestSuite.LettucePipelineConnectionUnitTests;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({ LettuceConnectionUnitTests.class, LettucePipelineConnectionUnitTests.class })
public class LettuceConnectionUnitTestSuite {

	@SuppressWarnings("rawtypes")
	public static class LettuceConnectionUnitTests extends AbstractConnectionUnitTestBase<RedisAsyncCommands> {

		protected LettuceConnection connection;
		private RedisClient clientMock;
		protected StatefulRedisConnection<byte[], byte[]> statefulConnectionMock;
		protected RedisAsyncCommands<byte[], byte[]> asyncCommandsMock;
		protected RedisCommands syncCommandsMock;

		@SuppressWarnings({ "unchecked" })
		@Before
		public void setUp() throws InvocationTargetException, IllegalAccessException {

			clientMock = mock(RedisClient.class);
			statefulConnectionMock = mock(StatefulRedisConnection.class);
			when(clientMock.connect((RedisCodec) any())).thenReturn(statefulConnectionMock);

			asyncCommandsMock = getNativeRedisConnectionMock();
			syncCommandsMock = mock(RedisCommands.class);

			when(statefulConnectionMock.async()).thenReturn(getNativeRedisConnectionMock());
			when(statefulConnectionMock.sync()).thenReturn(syncCommandsMock);
			connection = new LettuceConnection(0, clientMock);
		}

		@Test // DATAREDIS-184
		public void shutdownWithNullOptionsIsCalledCorrectly() {

			connection.shutdown(null);
			verify(syncCommandsMock, times(1)).shutdown(true);
		}

		@Test // DATAREDIS-184
		public void shutdownWithNosaveOptionIsCalledCorrectly() {

			connection.shutdown(ShutdownOption.NOSAVE);
			verify(syncCommandsMock, times(1)).shutdown(false);
		}

		@Test // DATAREDIS-184
		public void shutdownWithSaveOptionIsCalledCorrectly() {

			connection.shutdown(ShutdownOption.SAVE);
			verify(syncCommandsMock, times(1)).shutdown(true);
		}

		@Test // DATAREDIS-267
		public void killClientShouldDelegateCallCorrectly() {

			String ipPort = "127.0.0.1:1001";
			connection.killClient("127.0.0.1", 1001);
			verify(syncCommandsMock, times(1)).clientKill(eq(ipPort));
		}

		@Test // DATAREDIS-270
		public void getClientNameShouldSendRequestCorrectly() {

			connection.getClientName();
			verify(syncCommandsMock, times(1)).clientGetname();
		}

		@Test(expected = IllegalArgumentException.class) // DATAREDIS-277
		public void slaveOfShouldThrowExectpionWhenCalledForNullHost() {
			connection.slaveOf(null, 0);
		}

		@Test // DATAREDIS-277
		public void slaveOfShouldBeSentCorrectly() {

			connection.slaveOf("127.0.0.1", 1001);
			verify(syncCommandsMock, times(1)).slaveof(eq("127.0.0.1"), eq(1001));
		}

		@Test // DATAREDIS-277
		public void slaveOfNoOneShouldBeSentCorrectly() {

			connection.slaveOfNoOne();
			verify(syncCommandsMock, times(1)).slaveofNoOne();
		}

		@Test(expected = InvalidDataAccessResourceUsageException.class) // DATAREDIS-348
		public void shouldThrowExceptionWhenAccessingRedisSentinelsCommandsWhenNoSentinelsConfigured() {
			connection.getSentinelConnection();
		}

		@Test // DATAREDIS-431
		public void dbIndexShouldBeSetWhenObtainingConnection() {

			connection = new LettuceConnection(null, 0, clientMock, null, 1);
			connection.getNativeConnection();

			verify(syncCommandsMock, times(1)).select(1);
		}

		@Test // DATAREDIS-603
		public void translatesUnknownExceptions() {

			IllegalArgumentException exception = new IllegalArgumentException("Aw, snap!");

			when(syncCommandsMock.set(any(), any())).thenThrow(exception);
			connection = new LettuceConnection(null, 0, clientMock, null, 1);

			try {
				connection.set("foo".getBytes(), "bar".getBytes());
			} catch (Exception e) {

				assertThat(e.getMessage(), containsString(exception.getMessage()));
				assertThat(e.getCause(), is((Throwable) exception));
			}
		}

		@Test // DATAREDIS-603
		public void translatesPipelineUnknownExceptions() throws Exception {

			IllegalArgumentException exception = new IllegalArgumentException("Aw, snap!");

			RedisCommand future = mock(RedisCommand.class, Mockito.withSettings().extraInterfaces(RedisFuture.class));

			when(((RedisFuture) future).get()).thenThrow(exception);
			when(asyncCommandsMock.set(any(byte[].class), any(byte[].class))).thenReturn((RedisFuture) future);
			connection = new LettuceConnection(null, 0, clientMock, null, 1);
			connection.openPipeline();

			try {
				connection.set("foo".getBytes(), "bar".getBytes());
			} catch (Exception e) {
				assertThat(e.getMessage(), containsString(exception.getMessage()));
				assertThat(e.getCause(), is((Throwable) exception));
			}
		}
	}

	public static class LettucePipelineConnectionUnitTests extends LettuceConnectionUnitTests {

		@Override
		@Before
		public void setUp() throws InvocationTargetException, IllegalAccessException {
			super.setUp();
			this.connection.openPipeline();
		}

		@Test // DATAREDIS-528
		public void shutdownWithSaveOptionIsCalledCorrectly() {

			connection.shutdown(ShutdownOption.SAVE);
			verify(asyncCommandsMock, times(1)).shutdown(true);
		}

		@Test // DATAREDIS-528
		public void shutdownWithNosaveOptionIsCalledCorrectly() {

			connection.shutdown(ShutdownOption.NOSAVE);
			verify(asyncCommandsMock, times(1)).shutdown(false);
		}

		@Test // DATAREDIS-528
		public void slaveOfShouldBeSentCorrectly() {

			connection.slaveOf("127.0.0.1", 1001);
			verify(asyncCommandsMock, times(1)).slaveof(eq("127.0.0.1"), eq(1001));
		}

		@Test // DATAREDIS-528
		public void shutdownWithNullOptionsIsCalledCorrectly() {

			connection.shutdown(null);
			verify(asyncCommandsMock, times(1)).shutdown(true);
		}

		@Test // DATAREDIS-528
		public void killClientShouldDelegateCallCorrectly() {

			String ipPort = "127.0.0.1:1001";
			connection.killClient("127.0.0.1", 1001);
			verify(asyncCommandsMock, times(1)).clientKill(eq(ipPort));
		}

		@Test // DATAREDIS-528
		public void slaveOfNoOneShouldBeSentCorrectly() {

			connection.slaveOfNoOne();
			verify(asyncCommandsMock, times(1)).slaveofNoOne();
		}

		@Test // DATAREDIS-528
		public void getClientNameShouldSendRequestCorrectly() {

			connection.getClientName();
			verify(asyncCommandsMock, times(1)).clientGetname();
		}
	}
}
