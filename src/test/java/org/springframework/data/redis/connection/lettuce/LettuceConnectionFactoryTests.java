/*
 * Copyright 2011-2013 the original author or authors.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;

import org.apache.commons.pool.impl.GenericObjectPool.Config;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.connection.DefaultStringRedisConnection;
import org.springframework.data.redis.connection.PoolException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.StringRedisConnection;

import com.lambdaworks.redis.RedisAsyncConnection;
import com.lambdaworks.redis.RedisException;

/**
 * Integration test of {@link LettuceConnectionFactory}
 *
 * @author Jennifer Hickey
 *
 */
public class LettuceConnectionFactoryTests {

	private LettuceConnectionFactory factory;

	private StringRedisConnection connection;

	@Before
	public void setUp() {
		factory = new LettuceConnectionFactory(SettingsUtils.getHost(), SettingsUtils.getPort());
		factory.afterPropertiesSet();
		connection = new DefaultStringRedisConnection(factory.getConnection());
	}

	@After
	public void tearDown() {
		factory.destroy();
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void testGetNewConnectionOnError() throws Exception {
		factory.setValidateConnection(true);
		connection.lPush("alist", "baz");
		RedisAsyncConnection nativeConn = (RedisAsyncConnection) connection.getNativeConnection();
		nativeConn.close();
		// Give some time for async channel close
		Thread.sleep(500);
		connection.bLPop(1, "alist".getBytes());
		try {
			connection.get("test3");
			fail("Expected exception using natively closed conn");
		} catch (RedisSystemException e) {
			// expected, shared conn is closed
		}
		DefaultStringRedisConnection conn2 = new DefaultStringRedisConnection(
				factory.getConnection());
		assertNotSame(nativeConn, conn2.getNativeConnection());
		conn2.set("anotherkey", "anothervalue");
		assertEquals("anothervalue", conn2.get("anotherkey"));
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void testConnectionErrorNoValidate() throws Exception {
		connection.lPush("ablist", "baz");
		((RedisAsyncConnection) connection.getNativeConnection()).close();
		// Give some time for async channel close
		Thread.sleep(500);
		DefaultStringRedisConnection conn2 = new DefaultStringRedisConnection(
				factory.getConnection());
		try {
			conn2.set("anotherkey", "anothervalue");
			fail("Expected exception using natively closed conn");
		} catch (RedisSystemException e) {
			// expected, as we are re-using the natively closed conn
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
		LettuceConnectionFactory factory2 = new LettuceConnectionFactory(SettingsUtils.getHost(),
				SettingsUtils.getPort());
		factory2.setDatabase(1);
		factory2.afterPropertiesSet();
		StringRedisConnection connection2 = new DefaultStringRedisConnection(
				factory2.getConnection());
		connection2.flushDb();
		// put an item in database 0
		connection.set("sometestkey", "sometestvalue");
		try {
			// there should still be nothing in database 1
			assertEquals(Long.valueOf(0), connection2.dbSize());
		} finally {
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
			((RedisAsyncConnection<byte[], byte[]>) conn2.getNativeConnection()).ping();
			fail("The native connection should be closed");
		} catch (RedisException e) {
			// expected
		}
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testResetConnection() {
		RedisAsyncConnection<byte[], byte[]> nativeConn = (RedisAsyncConnection<byte[], byte[]>) connection
				.getNativeConnection();
		factory.resetConnection();
		assertNotSame(nativeConn, factory.getConnection().getNativeConnection());
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testInitConnection() {
		RedisAsyncConnection<byte[], byte[]> nativeConn = (RedisAsyncConnection<byte[], byte[]>) connection
				.getNativeConnection();
		factory.initConnection();
		assertNotSame(nativeConn, factory.getConnection().getNativeConnection());
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testResetAndInitConnection() {
		RedisAsyncConnection<byte[], byte[]> nativeConn = (RedisAsyncConnection<byte[], byte[]>) connection
				.getNativeConnection();
		factory.resetConnection();
		factory.initConnection();
		assertNotSame(nativeConn, factory.getConnection().getNativeConnection());
	}

	@Test(expected=RedisConnectionFailureException.class)
	public void testInitConnectionException() {
		factory.setHostName("fakeHost");
		factory.afterPropertiesSet();
	}

	@Test(expected=RedisConnectionFailureException.class)
	public void testGetConnectionNotSharedException() {
		factory.setShareNativeConnection(false);
		factory.setHostName("fakeHost");
		factory.afterPropertiesSet();
		factory.getConnection();
	}

	@Test(expected=RedisConnectionFailureException.class)
	public void testGetConnectionSharedException() {
		factory.setShareNativeConnection(false);
		factory.setHostName("fakeHost");
		factory.afterPropertiesSet();
		factory.setShareNativeConnection(true);
		factory.getConnection();
	}

	@Test
	public void testGetSharedConnectionNotShared() {
		factory.setShareNativeConnection(false);
		factory.setHostName("fakeHost");
		factory.afterPropertiesSet();
		assertNull(factory.getSharedConnection());
	}

	@Test(expected=RedisConnectionFailureException.class)
	public void testGetSharedConnectionSharedException() {
		factory.setShareNativeConnection(false);
		factory.setHostName("fakeHost");
		factory.afterPropertiesSet();
		factory.setShareNativeConnection(true);
		factory.getSharedConnection();
	}

	@Test
	public void testCreateLettuceConnectorWithPool() {
		Config poolConfig = new Config();
		poolConfig.maxActive = 1;
		poolConfig.maxWait = 1;
		DefaultLettucePool pool = new DefaultLettucePool(SettingsUtils.getHost(), SettingsUtils.getPort(), poolConfig);
		pool.afterPropertiesSet();
		LettuceConnectionFactory factory2 = new LettuceConnectionFactory(pool);
		factory2.afterPropertiesSet();
		factory2.createLettuceConnector(false);
		try {
			// We know we are using the pool if we try to get another connection
			// when maxActive is set to 1
			factory2.createLettuceConnector(false);
			fail("Expected Exception retrieving connection from pool");
		} catch (PoolException e) {
		}
	}

	@Ignore("Uncomment this test to manually check connection reuse in a pool scenario")
	@Test
	public void testLotsOfConnections() throws InterruptedException {
		// Running a netstat here should show only the 8 conns from the pool (plus 2 from setUp and 1 from factory2 afterPropertiesSet for shared conn)
		final LettuceConnectionFactory factory2 = new LettuceConnectionFactory(new DefaultLettucePool(SettingsUtils.getHost(), SettingsUtils.getPort()));
		factory2.afterPropertiesSet();
		for(int i=1;i< 1000;i++) {
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
	}
}
