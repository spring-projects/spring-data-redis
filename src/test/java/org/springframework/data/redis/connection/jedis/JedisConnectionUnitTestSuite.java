/*
 * Copyright 2014 the original author or authors.
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

import static org.hamcrest.core.IsEqual.*;
import static org.junit.Assert.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;
import org.mockito.ArgumentCaptor;
import org.springframework.dao.InvalidDataAccessResourceUsageException;
import org.springframework.data.redis.connection.AbstractConnectionUnitTestBase;
import org.springframework.data.redis.connection.RedisServerCommands.ShutdownOption;
import org.springframework.data.redis.connection.jedis.JedisConnectionUnitTestSuite.JedisConnectionPipelineUnitTests;
import org.springframework.data.redis.connection.jedis.JedisConnectionUnitTestSuite.JedisConnectionUnitTests;
import org.springframework.data.redis.core.TimeoutUtils;

import redis.clients.jedis.Client;
import redis.clients.jedis.Jedis;

/**
 * @author Christoph Strobl
 */
@RunWith(Suite.class)
@SuiteClasses({ JedisConnectionUnitTests.class, JedisConnectionPipelineUnitTests.class })
public class JedisConnectionUnitTestSuite {

	public static class JedisConnectionUnitTests extends AbstractConnectionUnitTestBase<Client> {

		protected JedisConnection connection;
		private Jedis jedisSpy;

		@Before
		public void setUp() {

			jedisSpy = spy(new MockedClientJedis("http://localhost:1234", getNativeRedisConnectionMock()));
			connection = new JedisConnection(jedisSpy);
		}

		/**
		 * @see DATAREDIS-184
		 */
		@Test
		public void shutdownWithNullShouldDelegateCommandCorrectly() {

			connection.shutdown(null);

			verifyNativeConnectionInvocation().shutdown();
		}

		/**
		 * @see DATAREDIS-184
		 */
		@Test
		public void shutdownNosaveShouldBeSentCorrectlyUsingLuaScript() {

			connection.shutdown(ShutdownOption.NOSAVE);

			ArgumentCaptor<byte[]> captor = ArgumentCaptor.forClass(byte[].class);
			verifyNativeConnectionInvocation().eval(captor.capture(), any(byte[].class), any(byte[][].class));

			assertThat(captor.getValue(), equalTo("return redis.call('SHUTDOWN','NOSAVE')".getBytes()));
		}

		/**
		 * @see DATAREDIS-184
		 */
		@Test
		public void shutdownSaveShouldBeSentCorrectlyUsingLuaScript() {

			connection.shutdown(ShutdownOption.SAVE);

			ArgumentCaptor<byte[]> captor = ArgumentCaptor.forClass(byte[].class);
			verifyNativeConnectionInvocation().eval(captor.capture(), any(byte[].class), any(byte[][].class));

			assertThat(captor.getValue(), equalTo("return redis.call('SHUTDOWN','SAVE')".getBytes()));
		}

		/**
		 * @see DATAREDIS-286
		 */
		@Test
		public void pExpireHavingIntOverflowShouldUseRedisServerTimeAsReferenceForPExpireAt() {

			long msec = Long.valueOf((long) Integer.MAX_VALUE + 1);
			long expected = msec + TimeoutUtils.toMillis(1, TimeUnit.SECONDS);

			/* redis time as list containing [0] = seconds, [1] = microseconds
			 * @see http://redis.io/commands/time
			 */
			when(jedisSpy.time()).thenReturn(Arrays.asList("1", "0"));

			connection.pExpire("foo".getBytes(), msec);
			verifyNativeConnectionInvocation().pexpireAt(any(byte[].class), eq(expected));
		}

		/**
		 * @see DATAREDIS-267
		 */
		@Test
		public void killClientShouldDelegateCallCorrectly() {

			connection.killClient("127.0.0.1", 1001);
			verifyNativeConnectionInvocation().clientKill(eq("127.0.0.1:1001"));
		}

		/**
		 * @see DATAREDIS-270
		 */
		@Test
		public void getClientNameShouldSendRequestCorrectly() {

			connection.getClientName();
			verifyNativeConnectionInvocation().clientGetname();
		}

		/**
		 * @see DATAREDIS-277
		 */
		@Test(expected = IllegalArgumentException.class)
		public void slaveOfShouldThrowExectpionWhenCalledForNullHost() {
			connection.slaveOf(null, 0);
		}

		/**
		 * @see DATAREDIS-277
		 */
		@Test
		public void slaveOfShouldBeSentCorrectly() {

			connection.slaveOf("127.0.0.1", 1001);
			verifyNativeConnectionInvocation().slaveof(eq("127.0.0.1"), eq(1001));
		}

		/**
		 * @see DATAREDIS-277
		 */
		@Test
		public void slaveOfNoOneShouldBeSentCorrectly() {

			connection.slaveOfNoOne();
			verifyNativeConnectionInvocation().slaveofNoOne();
		}

		/**
		 * @see DATAREDIS-330
		 */
		@Test(expected = InvalidDataAccessResourceUsageException.class)
		public void shouldThrowExceptionWhenAccessingRedisSentinelsCommandsWhenNoSentinelsConfigured() {
			connection.getSentinelConnection();
		}

	}

	public static class JedisConnectionPipelineUnitTests extends JedisConnectionUnitTests {

		@Before
		public void setUp() {
			super.setUp();
			connection.openPipeline();
		}

		/**
		 * @see DATAREDIS-184
		 */
		@Override
		@Test(expected = UnsupportedOperationException.class)
		public void shutdownNosaveShouldBeSentCorrectlyUsingLuaScript() {
			super.shutdownNosaveShouldBeSentCorrectlyUsingLuaScript();
		}

		/**
		 * @see DATAREDIS-184
		 */
		@Override
		@Test(expected = UnsupportedOperationException.class)
		public void shutdownSaveShouldBeSentCorrectlyUsingLuaScript() {
			super.shutdownSaveShouldBeSentCorrectlyUsingLuaScript();
		}

		/**
		 * @see DATAREDIS-267
		 */
		@Test(expected = UnsupportedOperationException.class)
		public void killClientShouldDelegateCallCorrectly() {
			super.killClientShouldDelegateCallCorrectly();
		}

		/**
		 * @see DATAREDIS-270
		 */
		@Override
		@Test(expected = UnsupportedOperationException.class)
		public void getClientNameShouldSendRequestCorrectly() {
			super.getClientNameShouldSendRequestCorrectly();
		}

		/**
		 * @see DATAREDIS-277
		 */
		@Override
		@Test(expected = UnsupportedOperationException.class)
		public void slaveOfShouldBeSentCorrectly() {
			super.slaveOfShouldBeSentCorrectly();
		}

		/**
		 * @see DATAREDIS-277
		 */
		@Test(expected = UnsupportedOperationException.class)
		public void slaveOfNoOneShouldBeSentCorrectly() {
			super.slaveOfNoOneShouldBeSentCorrectly();
		}

	}

	/**
	 * {@link Jedis} extension allowing to use mocked object as {@link Client}.
	 */
	private static class MockedClientJedis extends Jedis {

		public MockedClientJedis(String host, Client client) {
			super(host);
			this.client = client;
		}

	}
}
