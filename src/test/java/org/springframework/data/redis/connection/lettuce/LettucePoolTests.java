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
package org.springframework.data.redis.connection.lettuce;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.fail;

import org.apache.commons.pool.impl.GenericObjectPool.Config;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.connection.PoolException;

import com.lambdaworks.redis.RedisAsyncConnection;
import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisException;

/**
 * Unit test of {@link LettucePool}
 * 
 * @author Jennifer Hickey
 * 
 */
public class LettucePoolTests {

	private RedisClient client;

	private LettucePool pool;

	@Before
	public void setUp() {
		this.client = new RedisClient(SettingsUtils.getHost(), SettingsUtils.getPort());
	}

	@After
	public void tearDown() {
		if (this.pool != null) {
			this.pool.destroy();
		}
	}

	@Test
	public void testGetResource() {
		this.pool = new LettucePool(client);
		RedisAsyncConnection<byte[], byte[]> client = pool.getResource();
		assertNotNull(client);
		client.ping();
	}

	@Test
	public void testGetResourcePoolExhausted() {
		Config poolConfig = new Config();
		poolConfig.maxActive = 1;
		poolConfig.maxWait = 1;
		this.pool = new LettucePool(client, poolConfig, 0);
		RedisAsyncConnection<byte[], byte[]> client = pool.getResource();
		assertNotNull(client);
		try {
			pool.getResource();
			fail("PoolException should be thrown when pool exhausted");
		} catch (PoolException e) {
		}
	}

	@Test
	public void testGetResourceValidate() {
		Config poolConfig = new Config();
		poolConfig.testOnBorrow = true;
		this.pool = new LettucePool(client, poolConfig, 0);
		RedisAsyncConnection<byte[], byte[]> client = pool.getResource();
		assertNotNull(client);
	}

	@Test(expected = PoolException.class)
	public void testGetResourceCreationUnsuccessful() {
		this.pool = new LettucePool(new RedisClient(SettingsUtils.getHost(), 3333));
		pool.getResource();
	}

	@Test
	public void testReturnResource() {
		Config poolConfig = new Config();
		poolConfig.maxActive = 1;
		poolConfig.maxWait = 1;
		this.pool = new LettucePool(client);
		RedisAsyncConnection<byte[], byte[]> client = pool.getResource();
		assertNotNull(client);
		pool.returnResource(client);
		assertNotNull(pool.getResource());
	}

	@Test
	public void testReturnBrokenResource() {
		Config poolConfig = new Config();
		poolConfig.maxActive = 1;
		poolConfig.maxWait = 1;
		this.pool = new LettucePool(client, poolConfig, 0);
		RedisAsyncConnection<byte[], byte[]> client = pool.getResource();
		assertNotNull(client);
		pool.returnBrokenResource(client);
		RedisAsyncConnection<byte[], byte[]> client2 = pool.getResource();
		assertNotSame(client, client2);
		try {
			client.ping();
			fail("Broken resouce connection should be closed");
		} catch (RedisException e) {
		}
	}

	@Test
	public void testCreateWithHostAndPort() {
		this.pool = new LettucePool(SettingsUtils.getHost(), SettingsUtils.getPort());
		assertNotNull(pool.getResource());
	}

	@Test
	public void testCreateWithDbIndex() {
		this.pool = new LettucePool(SettingsUtils.getHost(), SettingsUtils.getPort(), 1, 65000);
		assertNotNull(pool.getResource());
	}

	@Test(expected = PoolException.class)
	public void testCreateWithDbIndexInvalid() {
		this.pool = new LettucePool(SettingsUtils.getHost(), SettingsUtils.getPort(), 17, 65000);
		pool.getResource();
	}
}
