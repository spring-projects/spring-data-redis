/*
 * Copyright 2011-2016 the original author or authors.
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

import static org.hamcrest.core.Is.*;
import static org.hamcrest.core.IsEqual.*;
import static org.hamcrest.core.IsNull.*;
import static org.junit.Assert.*;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.data.redis.ConnectionFactoryTracker;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.connection.DefaultStringRedisConnection;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.data.repository.util.QueryExecutionConverters;

import com.lambdaworks.redis.RedisException;
import com.lambdaworks.redis.api.async.RedisAsyncCommands;

/**
 * Integration test of {@link LettuceConnectionFactory}
 * 
 * @author Jennifer Hickey
 * @author Thomas Darimont
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public class LettuceConnectionFactoryTests {

	private LettuceConnectionFactory factory;

	private StringRedisConnection connection;

	@Before
	public void setUp() {

		factory = new LettuceConnectionFactory(SettingsUtils.getHost(), SettingsUtils.getPort());
		factory.setClientResources(LettuceTestClientResources.getSharedClientResources());
		factory.afterPropertiesSet();
		factory.setShutdownTimeout(0);
		connection = new DefaultStringRedisConnection(factory.getConnection());
	}

	@After
	public void tearDown() {
		factory.destroy();

		if (connection != null) {
			connection.close();
		}
	}

	@AfterClass
	public static void cleanUp() {
		ConnectionFactoryTracker.cleanUp();
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void testGetNewConnectionOnError() throws Exception {
		factory.setValidateConnection(true);
		connection.lPush("alist", "baz");
		RedisAsyncCommands nativeConn = (RedisAsyncCommands) connection.getNativeConnection();
		nativeConn.getStatefulConnection().close();
		// Give some time for async channel close
		Thread.sleep(500);
		connection.bLPop(1, "alist".getBytes());
		try {
			connection.get("test3");
			fail("Expected exception using natively closed conn");
		} catch (RedisSystemException e) {
			// expected, shared conn is closed
		}
		DefaultStringRedisConnection conn2 = new DefaultStringRedisConnection(factory.getConnection());
		assertNotSame(nativeConn, conn2.getNativeConnection());
		conn2.set("anotherkey", "anothervalue");
		assertEquals("anothervalue", conn2.get("anotherkey"));
		conn2.close();
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void testConnectionErrorNoValidate() throws Exception {
		connection.lPush("ablist", "baz");
		((RedisAsyncCommands) connection.getNativeConnection()).getStatefulConnection().close();
		// Give some time for async channel close
		Thread.sleep(500);
		DefaultStringRedisConnection conn2 = new DefaultStringRedisConnection(factory.getConnection());
		try {
			conn2.set("anotherkey", "anothervalue");
			fail("Expected exception using natively closed conn");
		} catch (RedisSystemException e) {
			// expected, as we are re-using the natively closed conn
		} finally {
			conn2.close();
		}
	}

	@Test
	public void testValidateNoError() {
		factory.setValidateConnection(true);
		RedisConnection conn2 = factory.getConnection();
		assertSame(connection.getNativeConnection(), conn2.getNativeConnection());
	}

	@Test
	public void testSelectDb() {

		LettuceConnectionFactory factory2 = new LettuceConnectionFactory(SettingsUtils.getHost(), SettingsUtils.getPort());
		factory2.setClientResources(LettuceTestClientResources.getSharedClientResources());
		factory2.setShutdownTimeout(0);
		factory2.setDatabase(1);
		factory2.afterPropertiesSet();

		ConnectionFactoryTracker.add(factory2);

		StringRedisConnection connection2 = new DefaultStringRedisConnection(factory2.getConnection());
		connection2.flushDb();
		// put an item in database 0
		connection.set("sometestkey", "sometestvalue");
		try {
			// there should still be nothing in database 1
			assertEquals(Long.valueOf(0), connection2.dbSize());
		} finally {
			connection2.close();
			factory2.destroy();
		}
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testDisableSharedConnection() throws Exception {
		factory.setShareNativeConnection(false);
		RedisConnection conn2 = factory.getConnection();
		assertNotSame(connection.getNativeConnection(), conn2.getNativeConnection());
		// Give some time for native connection to asynchronously initialize, else close doesn't work
		Thread.sleep(100);
		conn2.close();
		assertTrue(conn2.isClosed());
		// Give some time for native connection to asynchronously close
		Thread.sleep(100);
		try {
			((RedisAsyncCommands<byte[], byte[]>) conn2.getNativeConnection()).ping();
			fail("The native connection should be closed");
		} catch (RedisException e) {
			// expected
		}
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testResetConnection() {
		RedisAsyncCommands<byte[], byte[]> nativeConn = (RedisAsyncCommands<byte[], byte[]>) connection
				.getNativeConnection();
		factory.resetConnection();
		assertNotSame(nativeConn, factory.getConnection().getNativeConnection());
		nativeConn.getStatefulConnection().close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testInitConnection() {
		RedisAsyncCommands<byte[], byte[]> nativeConn = (RedisAsyncCommands<byte[], byte[]>) connection
				.getNativeConnection();
		factory.initConnection();
		RedisConnection newConnection = factory.getConnection();
		assertNotSame(nativeConn, newConnection.getNativeConnection());
		newConnection.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testResetAndInitConnection() {
		RedisAsyncCommands<byte[], byte[]> nativeConn = (RedisAsyncCommands<byte[], byte[]>) connection
				.getNativeConnection();
		factory.resetConnection();
		factory.initConnection();
		RedisConnection newConnection = factory.getConnection();
		assertNotSame(nativeConn, newConnection.getNativeConnection());
		newConnection.close();
	}

	@Test
	public void testGetConnectionException() {
		factory.resetConnection();
		factory.setHostName("fakeHost");
		factory.afterPropertiesSet();
		try {
			factory.getConnection();
			fail("Expected connection failure exception");
		} catch (RedisConnectionFailureException e) {}
	}

	@Test
	public void testGetConnectionNotSharedBadHostname() {
		factory.setShareNativeConnection(false);
		factory.setHostName("fakeHost");
		factory.afterPropertiesSet();
		factory.getConnection();
	}

	@Test
	public void testGetSharedConnectionNotShared() {
		factory.setShareNativeConnection(false);
		factory.setHostName("fakeHost");
		factory.afterPropertiesSet();
		assertNull(factory.getSharedConnection());
	}

	@Test
	public void testCreateFactoryWithPool() {
		DefaultLettucePool pool = new DefaultLettucePool(SettingsUtils.getHost(), SettingsUtils.getPort());
		pool.setClientResources(LettuceTestClientResources.getSharedClientResources());
		pool.afterPropertiesSet();
		LettuceConnectionFactory factory2 = new LettuceConnectionFactory(pool);
		factory2.setShutdownTimeout(0);
		factory2.afterPropertiesSet();

		ConnectionFactoryTracker.add(factory2);

		RedisConnection conn2 = factory2.getConnection();
		conn2.close();
		factory2.destroy();
		pool.destroy();
	}

	@Ignore("Uncomment this test to manually check connection reuse in a pool scenario")
	@Test
	public void testLotsOfConnections() throws InterruptedException {
		// Running a netstat here should show only the 8 conns from the pool (plus 2 from setUp and 1 from factory2
		// afterPropertiesSet for shared conn)
		DefaultLettucePool pool = new DefaultLettucePool(SettingsUtils.getHost(), SettingsUtils.getPort());
		pool.afterPropertiesSet();
		final LettuceConnectionFactory factory2 = new LettuceConnectionFactory(pool);
		factory2.afterPropertiesSet();

		ConnectionFactoryTracker.add(factory2);

		for (int i = 1; i < 1000; i++) {
			Thread th = new Thread(new Runnable() {
				public void run() {
					factory2.getConnection().bRPop(50000, "foo".getBytes());
				}
			});
			th.start();
		}
		Thread.sleep(234234234);
	}

	@Ignore("Redis must have requirepass set to run this test")
	@Test
	public void testConnectWithPassword() {
		factory.setPassword("foo");
		factory.afterPropertiesSet();
		RedisConnection conn = factory.getConnection();
		// Test shared and dedicated conns
		conn.ping();
		conn.bLPop(1, "key".getBytes());
		conn.close();
	}

	/**
	 * @see DATAREDIS-431
	 */
	@Test
	public void dbIndexShouldBePropagatedCorrectly() {

		LettuceConnectionFactory factory = new LettuceConnectionFactory();
		factory.setClientResources(LettuceTestClientResources.getSharedClientResources());
		factory.setDatabase(2);
		factory.afterPropertiesSet();

		ConnectionFactoryTracker.add(factory);

		StringRedisConnection connectionToDbIndex2 = new DefaultStringRedisConnection(factory.getConnection());

		try {

			String key = "key-in-db-2";
			connectionToDbIndex2.set(key, "the wheel of time");

			assertThat(connection.get(key), nullValue());
			assertThat(connectionToDbIndex2.get(key), notNullValue());
		} finally {
			connectionToDbIndex2.close();
		}
	}

	/**
	 * @see DATAREDIS-462
	 */
	@Test
	public void factoryWorksWithoutClientResources() {

		LettuceConnectionFactory factory = new LettuceConnectionFactory();
		factory.setShutdownTimeout(0);
		factory.afterPropertiesSet();

		ConnectionFactoryTracker.add(factory);

		StringRedisConnection connection = new DefaultStringRedisConnection(factory.getConnection());

		try {
			assertThat(connection.ping(), is(equalTo("PONG")));
		} finally {
			connection.close();
		}
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void factoryShouldReturnReactiveConnectionWhenCorrectly() {

		LettuceConnectionFactory factory = new LettuceConnectionFactory();
		factory.afterPropertiesSet();

		ConnectionFactoryTracker.add(factory);

		assertThat(factory.getReactiveConnection()
				.execute(cmd -> QueryExecutionConverters.RxJava1ObservableToMonoConverter.INSTANCE.convert(cmd.ping()))
				.blockFirst(), is("PONG"));
	}
}
