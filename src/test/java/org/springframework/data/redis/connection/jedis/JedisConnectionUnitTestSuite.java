/*
 * Copyright 2014-2019 the original author or authors.
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

			assertThat(captor.getValue()).isEqualTo("return redis.call('SHUTDOWN','NOSAVE')".getBytes());
		}

		@Test // DATAREDIS-184
		public void shutdownSaveShouldBeSentCorrectlyUsingLuaScript() {

			connection.shutdown(ShutdownOption.SAVE);

			ArgumentCaptor<byte[]> captor = ArgumentCaptor.forClass(byte[].class);
			verifyNativeConnectionInvocation().eval(captor.capture(), any(byte[].class), any(byte[][].class));

			assertThat(captor.getValue()).isEqualTo("return redis.call('SHUTDOWN','SAVE')".getBytes());
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

		@Test // DATAREDIS-277
		public void slaveOfShouldThrowExectpionWhenCalledForNullHost() {
			assertThatIllegalArgumentException().isThrownBy(() -> connection.slaveOf(null, 0));
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

		@Test // DATAREDIS-330
		public void shouldThrowExceptionWhenAccessingRedisSentinelsCommandsWhenNoSentinelsConfigured() {
			assertThatExceptionOfType(InvalidDataAccessResourceUsageException.class)
					.isThrownBy(() -> connection.getSentinelConnection());
		}

		@Test // DATAREDIS-472
		public void restoreShouldThrowExceptionWhenTtlInMillisExceedsIntegerRange() {
			assertThatIllegalArgumentException()
					.isThrownBy(() -> connection.restore("foo".getBytes(), (long) Integer.MAX_VALUE + 1L, "bar".getBytes()));
		}

		@Test // DATAREDIS-472
		public void setExShouldThrowExceptionWhenTimeExceedsIntegerRange() {
			assertThatIllegalArgumentException()
					.isThrownBy(() -> connection.setEx("foo".getBytes(), (long) Integer.MAX_VALUE + 1L, "bar".getBytes()));
		}

		@Test // DATAREDIS-472
		public void sRandMemberShouldThrowExceptionWhenCountExceedsIntegerRange() {
			assertThatIllegalArgumentException()
					.isThrownBy(() -> connection.sRandMember("foo".getBytes(), (long) Integer.MAX_VALUE + 1L));
		}

		@Test // DATAREDIS-472
		public void zRangeByScoreShouldThrowExceptionWhenOffsetExceedsIntegerRange() {
			assertThatIllegalArgumentException().isThrownBy(() -> connection.zRangeByScore("foo".getBytes(), "foo", "bar",
					(long) Integer.MAX_VALUE + 1L, Integer.MAX_VALUE));
		}

		@Test // DATAREDIS-472
		public void zRangeByScoreShouldThrowExceptionWhenCountExceedsIntegerRange() {
			assertThatIllegalArgumentException().isThrownBy(() -> connection.zRangeByScore("foo".getBytes(), "foo", "bar",
					Integer.MAX_VALUE, (long) Integer.MAX_VALUE + 1L));
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
			when(jedisSpy.getDB()).thenReturn(3);

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

		@Test
		@Override
		// DATAREDIS-184
		public void shutdownNosaveShouldBeSentCorrectlyUsingLuaScript() {
			assertThatExceptionOfType(UnsupportedOperationException.class)
					.isThrownBy(() -> super.shutdownNosaveShouldBeSentCorrectlyUsingLuaScript());
		}

		@Test
		@Override
		// DATAREDIS-184
		public void shutdownSaveShouldBeSentCorrectlyUsingLuaScript() {
			assertThatExceptionOfType(UnsupportedOperationException.class)
					.isThrownBy(() -> super.shutdownSaveShouldBeSentCorrectlyUsingLuaScript());
		}

		@Test // DATAREDIS-267
		public void killClientShouldDelegateCallCorrectly() {
			assertThatExceptionOfType(UnsupportedOperationException.class)
					.isThrownBy(() -> super.killClientShouldDelegateCallCorrectly());
		}

		@Test
		@Override
		// DATAREDIS-270
		public void getClientNameShouldSendRequestCorrectly() {
			assertThatExceptionOfType(UnsupportedOperationException.class)
					.isThrownBy(() -> super.getClientNameShouldSendRequestCorrectly());
		}

		@Test
		@Override
		// DATAREDIS-277
		public void slaveOfShouldBeSentCorrectly() {
			assertThatExceptionOfType(UnsupportedOperationException.class)
					.isThrownBy(() -> super.slaveOfShouldBeSentCorrectly());
		}

		@Test // DATAREDIS-277
		public void slaveOfNoOneShouldBeSentCorrectly() {
			assertThatExceptionOfType(UnsupportedOperationException.class)
					.isThrownBy(() -> super.slaveOfNoOneShouldBeSentCorrectly());
		}

		@Test // DATAREDIS-531
		public void scanShouldKeepTheConnectionOpen() {
			assertThatExceptionOfType(UnsupportedOperationException.class)
					.isThrownBy(() -> super.scanShouldKeepTheConnectionOpen());
		}

		@Test // DATAREDIS-531
		public void scanShouldCloseTheConnectionWhenCursorIsClosed() {
			assertThatExceptionOfType(UnsupportedOperationException.class)
					.isThrownBy(() -> super.scanShouldCloseTheConnectionWhenCursorIsClosed());
		}

		@Test // DATAREDIS-531
		public void sScanShouldKeepTheConnectionOpen() {
			assertThatExceptionOfType(UnsupportedOperationException.class)
					.isThrownBy(() -> super.sScanShouldKeepTheConnectionOpen());
		}

		@Test // DATAREDIS-531
		public void sScanShouldCloseTheConnectionWhenCursorIsClosed() {
			assertThatExceptionOfType(UnsupportedOperationException.class)
					.isThrownBy(() -> super.sScanShouldCloseTheConnectionWhenCursorIsClosed());
		}

		@Test // DATAREDIS-531
		public void zScanShouldKeepTheConnectionOpen() {
			assertThatExceptionOfType(UnsupportedOperationException.class)
					.isThrownBy(() -> super.zScanShouldKeepTheConnectionOpen());
		}

		@Test // DATAREDIS-531
		public void zScanShouldCloseTheConnectionWhenCursorIsClosed() {
			assertThatExceptionOfType(UnsupportedOperationException.class)
					.isThrownBy(() -> super.zScanShouldCloseTheConnectionWhenCursorIsClosed());
		}

		@Test // DATAREDIS-531
		public void hScanShouldKeepTheConnectionOpen() {
			assertThatExceptionOfType(UnsupportedOperationException.class)
					.isThrownBy(() -> super.hScanShouldKeepTheConnectionOpen());
		}

		@Test // DATAREDIS-531
		public void hScanShouldCloseTheConnectionWhenCursorIsClosed() {
			assertThatExceptionOfType(UnsupportedOperationException.class)
					.isThrownBy(() -> super.hScanShouldCloseTheConnectionWhenCursorIsClosed());
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
