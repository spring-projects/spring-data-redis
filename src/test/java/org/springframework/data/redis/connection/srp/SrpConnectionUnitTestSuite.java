/*
 * Copyright 2014-2017 the original author or authors.
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
package org.springframework.data.redis.connection.srp;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.springframework.data.redis.connection.AbstractConnectionUnitTestBase;
import org.springframework.data.redis.connection.RedisServerCommands.ShutdownOption;
import org.springframework.data.redis.connection.srp.SrpConnectionUnitTestSuite.SrpConnectionPiplineUnitTests;
import org.springframework.data.redis.connection.srp.SrpConnectionUnitTestSuite.SrpConnectionUnitTests;

import redis.client.RedisClient;
import redis.client.RedisClient.Pipeline;

import com.google.common.base.Charsets;

/**
 * @author Christoph Strobl
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({ SrpConnectionUnitTests.class, SrpConnectionPiplineUnitTests.class })
public class SrpConnectionUnitTestSuite {

	public static class SrpConnectionUnitTests extends AbstractConnectionUnitTestBase<RedisClient> {

		protected SrpConnection connection;

		@Before
		public void setUp() {
			connection = new SrpConnection(getNativeRedisConnectionMock());
		}

		@Test // DATAREDIS-184
		public void shutdownWithNullOpionsIsCalledCorrectly() {

			connection.shutdown(null);
			verifyNativeConnectionInvocation().shutdown("SAVE".getBytes(Charsets.UTF_8), null);
		}

		@Test // DATAREDIS-184
		public void shutdownWithSaveIsCalledCorrectly() {

			connection.shutdown(ShutdownOption.SAVE);
			verifyNativeConnectionInvocation().shutdown("SAVE".getBytes(Charsets.UTF_8), null);
		}

		@Test // DATAREDIS-184
		public void shutdownWithNosaveIsCalledCorrectly() {

			connection.shutdown(ShutdownOption.NOSAVE);
			verifyNativeConnectionInvocation().shutdown("NOSAVE".getBytes(Charsets.UTF_8), null);
		}

		@Test // DATAREDIS-267
		public void killClientShouldDelegateCallCorrectly() {

			String ipPort = "127.0.0.1:1001";
			connection.killClient("127.0.0.1", 1001);
			verifyNativeConnectionInvocation().client_kill(eq(ipPort));
		}

		@Test // DATAREDIS-270
		public void getClientNameShouldSendRequestCorrectly() {

			connection.getClientName();
			verifyNativeConnectionInvocation().client_getname();
		}

		@Test(expected = IllegalArgumentException.class) // DATAREDIS-277
		public void slaveOfShouldThrowExectpionWhenCalledForNullHost() {
			connection.slaveOf(null, 0);
		}

		@Test // DATAREDIS-277
		public void slaveOfShouldBeSentCorrectly() {

			connection.slaveOf("127.0.0.1", 1001);
			verifyNativeConnectionInvocation().slaveof(eq("127.0.0.1"), eq(1001));
		}

		@Test // DATAREDIS-277
		public void slaveOfNoOneShouldBeSentCorrectly() {

			connection.slaveOfNoOne();
			verifyNativeConnectionInvocation().slaveof(eq("NO"), eq("ONE"));
		}
	}

	public static class SrpConnectionPiplineUnitTests extends AbstractConnectionUnitTestBase<Pipeline> {

		protected SrpConnection connection;

		@Before
		public void setUp() {

			RedisClient clientMock = mock(RedisClient.class);
			connection = new SrpConnection(clientMock);
			when(clientMock.pipeline()).thenReturn(getNativeRedisConnectionMock());

			connection.openPipeline();
		}

		@Test // DATAREDIS-184
		public void shutdownWithNullOpionsIsCalledCorrectly() {

			connection.shutdown(null);
			verifyNativeConnectionInvocation().shutdown("SAVE".getBytes(Charsets.UTF_8), null);
		}

		@Test // DATAREDIS-184
		public void shutdownWithSaveIsCalledCorrectly() {

			connection.shutdown(ShutdownOption.SAVE);
			verifyNativeConnectionInvocation().shutdown("SAVE".getBytes(Charsets.UTF_8), null);
		}

		@Test // DATAREDIS-184
		public void shutdownWithNosaveIsCalledCorrectly() {

			connection.shutdown(ShutdownOption.NOSAVE);
			verifyNativeConnectionInvocation().shutdown("NOSAVE".getBytes(Charsets.UTF_8), null);
		}

		@Test // DATAREDIS-270
		public void getClientNameShouldSendRequestCorrectly() {

			connection.getClientName();
			verifyNativeConnectionInvocation().client_getname();
		}

	}

}
