/*
 * Copyright 2011-2019 the original author or authors.
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
import static org.junit.Assume.*;

import io.lettuce.core.EpollProvider;
import io.lettuce.core.KqueueProvider;
import io.lettuce.core.ReadFrom;
import io.lettuce.core.RedisException;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.reactive.BaseRedisReactiveCommands;
import reactor.test.StepVerifier;

import java.io.File;
import java.time.Duration;
import java.util.function.Consumer;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
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
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.RedisStaticMasterReplicaConfiguration;
import org.springframework.data.redis.connection.StringRedisConnection;

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
		assertThat(conn2.getNativeConnection()).isNotSameAs(nativeConn);
		conn2.set("anotherkey", "anothervalue");
		assertThat(conn2.get("anotherkey")).isEqualTo("anothervalue");
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
		assertThat(conn2.getNativeConnection()).isSameAs(connection.getNativeConnection());
	}

	@Test // DATAREDIS-973
	public void testSelectDb() {

		// put an item in database 0
		connection.set("sometestkey", "sometestvalue");

		LettuceConnectionFactory sharingConnectionFactory = newConnectionFactory(cf -> cf.setDatabase(1));
		runSelectDbTest(sharingConnectionFactory);

		LettuceConnectionFactory nonSharingConnectionFactory = newConnectionFactory(cf -> {
			cf.setDatabase(1);
			cf.setShareNativeConnection(false);
		});
		runSelectDbTest(nonSharingConnectionFactory);
	}

	@Test // DATAREDIS-973
	public void testSelectDbReactive() {

		LettuceConnectionFactory sharingConnectionFactory = newConnectionFactory(cf -> cf.setDatabase(1));
		runSelectDbReactiveTest(sharingConnectionFactory);

		LettuceConnectionFactory nonSharingConnectionFactory = newConnectionFactory(cf -> {
			cf.setDatabase(1);
			cf.setShareNativeConnection(false);
		});
		runSelectDbTest(nonSharingConnectionFactory);
	}

	private void runSelectDbTest(LettuceConnectionFactory factory2) {

		ConnectionFactoryTracker.add(factory2);

		StringRedisConnection separateDatabase = new DefaultStringRedisConnection(factory2.getConnection());
		separateDatabase.flushDb();

		// put an item in database 0
		connection.set("sometestkey", "sometestvalue");

		try {
			// there should still be nothing in database 1
			assertThat(separateDatabase.dbSize()).isEqualTo(Long.valueOf(0));
		} finally {
			separateDatabase.close();
			factory2.destroy();
		}
	}

	private void runSelectDbReactiveTest(LettuceConnectionFactory factory2) {

		ConnectionFactoryTracker.add(factory2);

		LettuceReactiveRedisConnection separateDatabase = factory2.getReactiveConnection();

		separateDatabase.serverCommands().flushDb() //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		// put an item in database 0
		connection.set("sometestkey", "sometestvalue");

		try {
			// there should still be nothing in database 1
			separateDatabase.serverCommands().dbSize() //
					.as(StepVerifier::create).expectNext(0L) //
					.verifyComplete();
		} finally {
			separateDatabase.close();
			factory2.destroy();
		}
	}

	private static LettuceConnectionFactory newConnectionFactory(Consumer<LettuceConnectionFactory> customizer) {

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(SettingsUtils.getHost(),
				SettingsUtils.getPort());
		connectionFactory.setClientResources(LettuceTestClientResources.getSharedClientResources());
		connectionFactory.setShutdownTimeout(0);
		customizer.accept(connectionFactory);
		connectionFactory.afterPropertiesSet();

		return connectionFactory;
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testDisableSharedConnection() throws Exception {
		factory.setShareNativeConnection(false);
		RedisConnection conn2 = factory.getConnection();
		assertThat(conn2.getNativeConnection()).isNotSameAs(connection.getNativeConnection());
		// Give some time for native connection to asynchronously initialize, else close doesn't work
		Thread.sleep(100);
		conn2.close();
		assertThat(conn2.isClosed()).isTrue();
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
		assertThat(factory.getConnection().getNativeConnection()).isNotSameAs(nativeConn);
		nativeConn.getStatefulConnection().close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testInitConnection() {
		RedisAsyncCommands<byte[], byte[]> nativeConn = (RedisAsyncCommands<byte[], byte[]>) connection
				.getNativeConnection();
		factory.initConnection();
		RedisConnection newConnection = factory.getConnection();
		assertThat(newConnection.getNativeConnection()).isNotSameAs(nativeConn);
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
		assertThat(newConnection.getNativeConnection()).isNotSameAs(nativeConn);
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
		assertThat(factory.getSharedConnection()).isNull();
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
			Thread th = new Thread(() -> factory2.getConnection().bRPop(50000, "foo".getBytes()));
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

	@Test // DATAREDIS-431
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

			assertThat(connection.get(key)).isNull();
			assertThat(connectionToDbIndex2.get(key)).isNotNull();
		} finally {
			connectionToDbIndex2.close();
		}
	}

	@Test // DATAREDIS-462
	public void factoryWorksWithoutClientResources() {

		LettuceConnectionFactory factory = new LettuceConnectionFactory();
		factory.setShutdownTimeout(0);
		factory.afterPropertiesSet();

		ConnectionFactoryTracker.add(factory);

		StringRedisConnection connection = new DefaultStringRedisConnection(factory.getConnection());

		try {
			assertThat(connection.ping()).isEqualTo("PONG");
		} finally {
			connection.close();
		}
	}

	@Test // DATAREDIS-525
	public void factoryShouldReturnReactiveConnectionWhenCorrectly() {

		LettuceConnectionFactory factory = new LettuceConnectionFactory();
		factory.setClientResources(LettuceTestClientResources.getSharedClientResources());
		factory.afterPropertiesSet();

		ConnectionFactoryTracker.add(factory);

		assertThat(factory.getReactiveConnection().execute(BaseRedisReactiveCommands::ping).blockFirst()).isEqualTo("PONG");
	}

	@Test // DATAREDIS-667
	public void factoryCreatesPooledConnections() {

		GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();

		LettuceClientConfiguration configuration = LettucePoolingClientConfiguration.builder().poolConfig(poolConfig)
				.clientResources(LettuceTestClientResources.getSharedClientResources()).shutdownTimeout(Duration.ZERO).build();

		LettuceConnectionFactory factory = new LettuceConnectionFactory(new RedisStandaloneConfiguration(), configuration);
		factory.setShareNativeConnection(false);
		factory.afterPropertiesSet();

		ConnectionFactoryTracker.add(factory);

		RedisConnection initial = factory.getConnection();
		Object initialNativeConnection = initial.getNativeConnection();

		initial.close();

		RedisConnection subsequent = factory.getConnection();
		Object subsequentNativeConnection = subsequent.getNativeConnection();

		subsequent.close();

		assertThat(initialNativeConnection).isEqualTo(subsequentNativeConnection);

		factory.destroy();
	}

	@Test // DATAREDIS-687
	public void connectsThroughRedisSocket() {

		assumeTrue(EpollProvider.isAvailable() || KqueueProvider.isAvailable());
		assumeTrue(new File(SettingsUtils.getSocket()).exists());

		LettuceClientConfiguration configuration = LettuceTestClientConfiguration.create();

		LettuceConnectionFactory factory = new LettuceConnectionFactory(SettingsUtils.socketConfiguration(), configuration);
		factory.setShareNativeConnection(false);
		factory.afterPropertiesSet();

		RedisConnection connection = factory.getConnection();
		assertThat(connection.ping()).isEqualTo("PONG");

		connection.close();
		factory.destroy();
	}

	@Test // DATAREDIS-762, DATAREDIS-869
	public void factoryUsesElastiCacheMasterReplicaConnections() {

		assumeTrue(String.format("No replicas connected to %s:%s.", SettingsUtils.getHost(), SettingsUtils.getPort()),
				connection.info("replication").getProperty("connected_slaves", "0").compareTo("0") > 0);

		LettuceClientConfiguration configuration = LettuceTestClientConfiguration.builder().readFrom(ReadFrom.SLAVE)
				.build();

		RedisStaticMasterReplicaConfiguration elastiCache = new RedisStaticMasterReplicaConfiguration(
				SettingsUtils.getHost()).node(SettingsUtils.getHost(), SettingsUtils.getPort() + 1);

		LettuceConnectionFactory factory = new LettuceConnectionFactory(elastiCache, configuration);
		factory.afterPropertiesSet();

		RedisConnection connection = factory.getConnection();

		try {
			assertThat(connection.ping()).isEqualTo("PONG");
			assertThat(connection.info().getProperty("role")).isEqualTo("slave");
		} finally {
			connection.close();
		}

		factory.destroy();
	}

	@Test // DATAREDIS-762, DATAREDIS-869
	public void factoryUsesElastiCacheMasterWithoutMaster() {

		assumeTrue(String.format("No replicas connected to %s:%s.", SettingsUtils.getHost(), SettingsUtils.getPort()),
				connection.info("replication").getProperty("connected_slaves", "0").compareTo("0") > 0);

		LettuceClientConfiguration configuration = LettuceTestClientConfiguration.builder().readFrom(ReadFrom.MASTER)
				.build();

		RedisStaticMasterReplicaConfiguration elastiCache = new RedisStaticMasterReplicaConfiguration(
				SettingsUtils.getHost(), SettingsUtils.getPort() + 1);

		LettuceConnectionFactory factory = new LettuceConnectionFactory(elastiCache, configuration);
		factory.afterPropertiesSet();

		RedisConnection connection = factory.getConnection();

		try {
			connection.ping();
			fail("Expected RedisException: Master is currently unknown");
		} catch (RedisSystemException e) {

			assertThat(e.getCause()).isInstanceOf(RedisException.class);
			assertThat(e.getCause().getMessage()).contains("Master is currently unknown");
		} finally {
			connection.close();
		}

		factory.destroy();
	}

	@Test // DATAREDIS-580, DATAREDIS-869
	public void factoryUsesMasterReplicaConnections() {

		assumeTrue(String.format("No replicas connected to %s:%s.", SettingsUtils.getHost(), SettingsUtils.getPort()),
				connection.info("replication").getProperty("connected_slaves", "0").compareTo("0") > 0);

		LettuceClientConfiguration configuration = LettuceTestClientConfiguration.builder().readFrom(ReadFrom.SLAVE)
				.build();

		LettuceConnectionFactory factory = new LettuceConnectionFactory(SettingsUtils.standaloneConfiguration(),
				configuration);
		factory.afterPropertiesSet();

		RedisConnection connection = factory.getConnection();

		try {
			assertThat(connection.ping()).isEqualTo("PONG");
			assertThat(connection.info().getProperty("role")).isEqualTo("slave");
		} finally {
			connection.close();
		}

		factory.destroy();
	}

	@Test // DATAREDIS-576
	public void connectionAppliesClientName() {

		LettuceClientConfiguration configuration = LettuceClientConfiguration.builder()
				.clientResources(LettuceTestClientResources.getSharedClientResources()).clientName("clientName").build();

		LettuceConnectionFactory factory = new LettuceConnectionFactory(new RedisStandaloneConfiguration(), configuration);
		factory.setShareNativeConnection(false);
		factory.afterPropertiesSet();

		ConnectionFactoryTracker.add(factory);

		RedisConnection connection = factory.getConnection();

		assertThat(connection.getClientName()).isEqualTo("clientName");
		connection.close();
	}

	@Test // DATAREDIS-576
	public void getClientNameShouldEqualWithFactorySetting() {

		LettuceConnectionFactory factory = new LettuceConnectionFactory(new RedisStandaloneConfiguration());
		factory.setClientResources(LettuceTestClientResources.getSharedClientResources());
		factory.setClientName("clientName");
		factory.afterPropertiesSet();

		ConnectionFactoryTracker.add(factory);

		RedisConnection connection = factory.getConnection();
		assertThat(connection.getClientName()).isEqualTo("clientName");

		connection.close();
	}
}
