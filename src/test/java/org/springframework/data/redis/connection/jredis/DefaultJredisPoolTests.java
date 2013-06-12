/*
 * Copyright 2013 the original author or authors.
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
package org.springframework.data.redis.connection.jredis;

import org.jredis.ClientRuntimeException;
import org.jredis.JRedis;
import org.jredis.RedisException;
import org.jredis.connector.ConnectionSpec;
import org.jredis.connector.NotConnectedException;
import org.jredis.ri.alphazero.connection.DefaultConnectionSpec;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.data.redis.SettingsUtils;
import org.apache.commons.pool.impl.GenericObjectPool.Config;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertNotSame;

import static org.junit.Assert.assertNotNull;

/**
 * Integration test of {@link DefaultJredisPool}
 * 
 * @author Jennifer Hickey
 * 
 */
public class DefaultJredisPoolTests {

	private ConnectionSpec connectionSpec;

	private JredisPool pool;

	@Before
	public void setUp() {
		this.connectionSpec = DefaultConnectionSpec.newSpec(SettingsUtils.getHost(), SettingsUtils.getPort(),
				0, null);
	}

	@After
	public void tearDown() {
		if (this.pool != null) {
			this.pool.destroy();
		}
	}

	@Test
	public void testGetResource() throws RedisException {
		this.pool = new DefaultJredisPool(connectionSpec);
		JRedis client = pool.getResource();
		assertNotNull(client);
		client.ping();
	}

	@Test
	public void testGetResourcePoolExhausted() {
		Config poolConfig = new Config();
		poolConfig.maxActive = 1;
		poolConfig.maxWait = 1;
		this.pool = new DefaultJredisPool(connectionSpec, poolConfig);
		JRedis client = pool.getResource();
		assertNotNull(client);
		try {
			pool.getResource();
			fail("ClientRuntimeException should be thrown when pool exhausted");
		} catch (ClientRuntimeException e) {

		}
	}

	@Test
	public void testGetResourceValidate() {
		Config poolConfig = new Config();
		poolConfig.testOnBorrow = true;
		this.pool = new DefaultJredisPool(connectionSpec, poolConfig);
		JRedis client = pool.getResource();
		assertNotNull(client);
	}

	@Test(expected = ClientRuntimeException.class)
	public void testGetResourceCreationUnsuccessful() {
		// Config poolConfig = new Config();
		// poolConfig.testOnBorrow = true;
		this.pool = new DefaultJredisPool(DefaultConnectionSpec.newSpec(SettingsUtils.getHost(), 3333, 0,
				null));
		pool.getResource();
	}

	@Test
	public void testReturnResource() throws RedisException {
		Config poolConfig = new Config();
		poolConfig.maxActive = 1;
		poolConfig.maxWait = 1;
		this.pool = new DefaultJredisPool(connectionSpec);
		JRedis client = pool.getResource();
		assertNotNull(client);
		pool.returnResource(client);
		assertNotNull(pool.getResource());
	}

	@Test
	public void testReturnBrokenResource() throws RedisException {
		Config poolConfig = new Config();
		poolConfig.maxActive = 1;
		poolConfig.maxWait = 1;
		this.pool = new DefaultJredisPool(connectionSpec, poolConfig);
		JRedis client = pool.getResource();
		assertNotNull(client);
		pool.returnBrokenResource(client);
		JRedis client2 = pool.getResource();
		assertNotSame(client, client2);
		try {
			client.ping();
			fail("Broken resouce connection should be closed");
		} catch (NotConnectedException e) {
		}
	}

	@Test
	public void testCreateWithHostAndPort() {
		this.pool = new DefaultJredisPool(SettingsUtils.getHost(), SettingsUtils.getPort());
		assertNotNull(pool.getResource());
	}

	@Test
	public void testCreateWithHostPortAndDbIndex() {
		this.pool = new DefaultJredisPool(SettingsUtils.getHost(), SettingsUtils.getPort(), 1, null, 0);
		assertNotNull(pool.getResource());
	}

}
