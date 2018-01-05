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
package org.springframework.data.redis.connection.jedis;

import static org.hamcrest.core.IsEqual.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import redis.clients.jedis.Client;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import java.io.IOException;
import java.util.Collections;
import java.util.Map.Entry;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;
import org.mockito.ArgumentCaptor;
import org.springframework.dao.InvalidDataAccessResourceUsageException;
import org.springframework.data.redis.connection.AbstractConnectionUnitTestBase;
import org.springframework.data.redis.connection.RedisServerCommands.ShutdownOption;
import org.springframework.data.redis.connection.RedisZSetCommands.Tuple;
import org.springframework.data.redis.connection.jedis.JedisConnectionUnitTestSuite.JedisConnectionPipelineUnitTests;
import org.springframework.data.redis.connection.jedis.JedisConnectionUnitTestSuite.JedisConnectionUnitTests;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;

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

		@Test // DATAREDIS-184
		public void shutdownWithNullShouldDelegateCommandCorrectly() {

			connection.shutdown(null);

			verifyNativeConnectionInvocation().shutdown();
		}

		@Test // DATAREDIS-184
		public void shutdownNosaveShouldBeSentCorrectlyUsingLuaScript() {

			connection.shutdown(ShutdownOption.NOSAVE);

			ArgumentCaptor<byte[]> captor = ArgumentCaptor.forClass(byte[].class);
			verifyNativeConnectionInvocation().eval(captor.capture(), any(byte[].class), any(byte[][].class));

			assertThat(captor.getValue(), equalTo("return redis.call('SHUTDOWN','NOSAVE')".getBytes()));
		}

		@Test // DATAREDIS-184
		public void shutdownSaveShouldBeSentCorrectlyUsingLuaScript() {

			connection.shutdown(ShutdownOption.SAVE);

			ArgumentCaptor<byte[]> captor = ArgumentCaptor.forClass(byte[].class);
			verifyNativeConnectionInvocation().eval(captor.capture(), any(byte[].class), any(byte[][].class));

			assertThat(captor.getValue(), equalTo("return redis.call('SHUTDOWN','SAVE')".getBytes()));
		}

		@Test // DATAREDIS-267
		public void killClientShouldDelegateCallCorrectly() {

			connection.killClient("127.0.0.1", 1001);
			verifyNativeConnectionInvocation().clientKill(eq("127.0.0.1:1001"));
		}

		@Test // DATAREDIS-270
		public void getClientNameShouldSendRequestCorrectly() {

			connection.getClientName();
			verifyNativeConnectionInvocation().clientGetname();
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
			verifyNativeConnectionInvocation().slaveofNoOne();
		}

		@Test(expected = InvalidDataAccessResourceUsageException.class) // DATAREDIS-330
		public void shouldThrowExceptionWhenAccessingRedisSentinelsCommandsWhenNoSentinelsConfigured() {
			connection.getSentinelConnection();
		}

		@Test(expected = IllegalArgumentException.class) // DATAREDIS-472
		public void restoreShouldThrowExceptionWhenTtlInMillisExceedsIntegerRange() {
			connection.restore("foo".getBytes(), new Long(Integer.MAX_VALUE) + 1L, "bar".getBytes());
		}

		@Test(expected = IllegalArgumentException.class) // DATAREDIS-472
		public void setExShouldThrowExceptionWhenTimeExceedsIntegerRange() {
			connection.setEx("foo".getBytes(), new Long(Integer.MAX_VALUE) + 1L, "bar".getBytes());
		}

		@Test(expected = IllegalArgumentException.class) // DATAREDIS-472
		public void getRangeShouldThrowExceptionWhenStartExceedsIntegerRange() {
			connection.getRange("foo".getBytes(), new Long(Integer.MAX_VALUE) + 1L, Integer.MAX_VALUE);
		}

		@Test(expected = IllegalArgumentException.class) // DATAREDIS-472
		public void getRangeShouldThrowExceptionWhenEndExceedsIntegerRange() {
			connection.getRange("foo".getBytes(), Integer.MAX_VALUE, new Long(Integer.MAX_VALUE) + 1L);
		}

		@Test(expected = IllegalArgumentException.class) // DATAREDIS-472
		public void sRandMemberShouldThrowExceptionWhenCountExceedsIntegerRange() {
			connection.sRandMember("foo".getBytes(), new Long(Integer.MAX_VALUE) + 1L);
		}

		@Test(expected = IllegalArgumentException.class) // DATAREDIS-472
		public void zRangeByScoreShouldThrowExceptionWhenOffsetExceedsIntegerRange() {
			connection.zRangeByScore("foo".getBytes(), "foo", "bar", new Long(Integer.MAX_VALUE) + 1L, Integer.MAX_VALUE);
		}

		@Test(expected = IllegalArgumentException.class) // DATAREDIS-472
		public void zRangeByScoreShouldThrowExceptionWhenCountExceedsIntegerRange() {
			connection.zRangeByScore("foo".getBytes(), "foo", "bar", Integer.MAX_VALUE, new Long(Integer.MAX_VALUE) + 1L);
		}

		@Test // DATAREDIS-531
		public void scanShouldKeepTheConnectionOpen() {

			doReturn(new ScanResult<>("0", Collections.<String> emptyList())).when(jedisSpy).scan(anyString(),
					any(ScanParams.class));

			connection.scan(ScanOptions.NONE);

			verify(jedisSpy, never()).quit();
		}

		@Test // DATAREDIS-531
		public void scanShouldCloseTheConnectionWhenCursorIsClosed() throws IOException {

			doReturn(new ScanResult<>("0", Collections.<String> emptyList())).when(jedisSpy).scan(anyString(),
					any(ScanParams.class));

			Cursor<byte[]> cursor = connection.scan(ScanOptions.NONE);
			cursor.close();

			verify(jedisSpy, times(1)).quit();
		}

		@Test // DATAREDIS-531
		public void sScanShouldKeepTheConnectionOpen() {

			doReturn(new ScanResult<>("0", Collections.<String> emptyList())).when(jedisSpy).sscan(any(byte[].class),
					any(byte[].class), any(ScanParams.class));

			connection.sScan("foo".getBytes(), ScanOptions.NONE);

			verify(jedisSpy, never()).quit();
		}

		@Test // DATAREDIS-531
		public void sScanShouldCloseTheConnectionWhenCursorIsClosed() throws IOException {

			doReturn(new ScanResult<>("0", Collections.<String> emptyList())).when(jedisSpy).sscan(any(byte[].class),
					any(byte[].class), any(ScanParams.class));

			Cursor<byte[]> cursor = connection.sScan("foo".getBytes(), ScanOptions.NONE);
			cursor.close();

			verify(jedisSpy, times(1)).quit();
		}

		@Test // DATAREDIS-531
		public void zScanShouldKeepTheConnectionOpen() {

			doReturn(new ScanResult<>("0", Collections.<String> emptyList())).when(jedisSpy).zscan(any(byte[].class),
					any(byte[].class), any(ScanParams.class));

			connection.zScan("foo".getBytes(), ScanOptions.NONE);

			verify(jedisSpy, never()).quit();
		}

		@Test // DATAREDIS-531
		public void zScanShouldCloseTheConnectionWhenCursorIsClosed() throws IOException {

			doReturn(new ScanResult<>("0", Collections.<String> emptyList())).when(jedisSpy).zscan(any(byte[].class),
					any(byte[].class), any(ScanParams.class));

			Cursor<Tuple> cursor = connection.zScan("foo".getBytes(), ScanOptions.NONE);
			cursor.close();

			verify(jedisSpy, times(1)).quit();
		}

		@Test // DATAREDIS-531
		public void hScanShouldKeepTheConnectionOpen() {

			doReturn(new ScanResult<>("0", Collections.<String> emptyList())).when(jedisSpy).hscan(any(byte[].class),
					any(byte[].class), any(ScanParams.class));

			connection.hScan("foo".getBytes(), ScanOptions.NONE);

			verify(jedisSpy, never()).quit();
		}

		@Test // DATAREDIS-531
		public void hScanShouldCloseTheConnectionWhenCursorIsClosed() throws IOException {

			doReturn(new ScanResult<>("0", Collections.<String> emptyList())).when(jedisSpy).hscan(any(byte[].class),
					any(byte[].class), any(ScanParams.class));

			Cursor<Entry<byte[], byte[]>> cursor = connection.hScan("foo".getBytes(), ScanOptions.NONE);
			cursor.close();

			verify(jedisSpy, times(1)).quit();
		}

		@Test // DATAREDIS-714
		public void doesNotSelectDbWhenCurrentDbMatchesDesiredOne() {

			Jedis jedisSpy = spy(new MockedClientJedis("http://localhost:1234", getNativeRedisConnectionMock()));
			new JedisConnection(jedisSpy);

			verify(jedisSpy, never()).select(anyInt());
		}

		@Test // DATAREDIS-714
		public void doesNotSelectDbWhenCurrentDbDoesNotMatchDesiredOne() {

			Jedis jedisSpy = spy(new MockedClientJedis("http://localhost:1234", getNativeRedisConnectionMock()));
			when(jedisSpy.getDB()).thenReturn(3L);

			new JedisConnection(jedisSpy);

			verify(jedisSpy).select(eq(0));
		}
	}

	public static class JedisConnectionPipelineUnitTests extends JedisConnectionUnitTests {

		@Before
		public void setUp() {
			super.setUp();
			connection.openPipeline();
		}

		@Override
		@Test(expected = UnsupportedOperationException.class) // DATAREDIS-184
		public void shutdownNosaveShouldBeSentCorrectlyUsingLuaScript() {
			super.shutdownNosaveShouldBeSentCorrectlyUsingLuaScript();
		}

		@Override
		@Test(expected = UnsupportedOperationException.class) // DATAREDIS-184
		public void shutdownSaveShouldBeSentCorrectlyUsingLuaScript() {
			super.shutdownSaveShouldBeSentCorrectlyUsingLuaScript();
		}

		@Test(expected = UnsupportedOperationException.class) // DATAREDIS-267
		public void killClientShouldDelegateCallCorrectly() {
			super.killClientShouldDelegateCallCorrectly();
		}

		@Override
		@Test(expected = UnsupportedOperationException.class) // DATAREDIS-270
		public void getClientNameShouldSendRequestCorrectly() {
			super.getClientNameShouldSendRequestCorrectly();
		}

		@Override
		@Test(expected = UnsupportedOperationException.class) // DATAREDIS-277
		public void slaveOfShouldBeSentCorrectly() {
			super.slaveOfShouldBeSentCorrectly();
		}

		@Test(expected = UnsupportedOperationException.class) // DATAREDIS-277
		public void slaveOfNoOneShouldBeSentCorrectly() {
			super.slaveOfNoOneShouldBeSentCorrectly();
		}

		@Test(expected = UnsupportedOperationException.class) // DATAREDIS-531
		public void scanShouldKeepTheConnectionOpen() {
			super.scanShouldKeepTheConnectionOpen();
		}

		@Test(expected = UnsupportedOperationException.class) // DATAREDIS-531
		public void scanShouldCloseTheConnectionWhenCursorIsClosed() throws IOException {
			super.scanShouldCloseTheConnectionWhenCursorIsClosed();
		}

		@Test(expected = UnsupportedOperationException.class) // DATAREDIS-531
		public void sScanShouldKeepTheConnectionOpen() {
			super.sScanShouldKeepTheConnectionOpen();
		}

		@Test(expected = UnsupportedOperationException.class) // DATAREDIS-531
		public void sScanShouldCloseTheConnectionWhenCursorIsClosed() throws IOException {
			super.sScanShouldCloseTheConnectionWhenCursorIsClosed();
		}

		@Test(expected = UnsupportedOperationException.class) // DATAREDIS-531
		public void zScanShouldKeepTheConnectionOpen() {
			super.zScanShouldKeepTheConnectionOpen();
		}

		@Test(expected = UnsupportedOperationException.class) // DATAREDIS-531
		public void zScanShouldCloseTheConnectionWhenCursorIsClosed() throws IOException {
			super.zScanShouldCloseTheConnectionWhenCursorIsClosed();
		}

		@Test(expected = UnsupportedOperationException.class) // DATAREDIS-531
		public void hScanShouldKeepTheConnectionOpen() {
			super.hScanShouldKeepTheConnectionOpen();
		}

		@Test(expected = UnsupportedOperationException.class) // DATAREDIS-531
		public void hScanShouldCloseTheConnectionWhenCursorIsClosed() throws IOException {
			super.hScanShouldCloseTheConnectionWhenCursorIsClosed();
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
