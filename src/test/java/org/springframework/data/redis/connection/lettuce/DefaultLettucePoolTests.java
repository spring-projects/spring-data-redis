/*
 * Copyright 2013-2016 the original author or authors.
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
import static org.junit.Assert.*;
import static org.springframework.test.util.ReflectionTestUtils.*;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.connection.PoolConfig;
import org.springframework.data.redis.connection.PoolException;
import org.springframework.data.redis.connection.RedisSentinelConfiguration;

import com.lambdaworks.redis.RedisAsyncConnection;
import com.lambdaworks.redis.RedisException;
import com.lambdaworks.redis.RedisURI;

/**
 * Unit test of {@link DefaultLettucePool}
 * 
 * @author Jennifer Hickey
 * @author Thomas Darimont
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public class DefaultLettucePoolTests {

	private DefaultLettucePool pool;

	@After
	public void tearDown() {
		if (this.pool != null) {

			if (this.pool.getClient() != null) {
				this.pool.getClient().shutdown(0, 0, TimeUnit.MILLISECONDS);
			}

			this.pool.destroy();
		}
	}

	@Test
	public void testGetResource() {
		this.pool = new DefaultLettucePool(SettingsUtils.getHost(), SettingsUtils.getPort());
		this.pool.setClientResources(LettuceTestClientResources.getSharedClientResources());
		pool.afterPropertiesSet();
		RedisAsyncConnection<byte[], byte[]> client = pool.getResource();
		assertNotNull(client);
		client.ping();
		client.close();
	}

	@Test
	public void testGetResourcePoolExhausted() {
		GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
		poolConfig.setMaxTotal(1);
		poolConfig.setMaxWaitMillis(1);
		this.pool = new DefaultLettucePool(SettingsUtils.getHost(), SettingsUtils.getPort(), poolConfig);
		this.pool.setClientResources(LettuceTestClientResources.getSharedClientResources());
		pool.afterPropertiesSet();
		RedisAsyncConnection<byte[], byte[]> client = pool.getResource();
		assertNotNull(client);
		try {
			pool.getResource();
			fail("PoolException should be thrown when pool exhausted");
		} catch (PoolException e) {} finally {
			client.close();
		}
	}

	@Test
	public void testGetResourceValidate() {
		PoolConfig poolConfig = new PoolConfig();
		poolConfig.setTestOnBorrow(true);
		this.pool = new DefaultLettucePool(SettingsUtils.getHost(), SettingsUtils.getPort(), poolConfig);
		this.pool.setClientResources(LettuceTestClientResources.getSharedClientResources());
		pool.afterPropertiesSet();
		RedisAsyncConnection<byte[], byte[]> client = pool.getResource();
		assertNotNull(client);
		client.close();
	}

	@Test(expected = PoolException.class)
	public void testGetResourceCreationUnsuccessful() throws Exception {
		this.pool = new DefaultLettucePool(SettingsUtils.getHost(), 3333);
		this.pool.setClientResources(LettuceTestClientResources.getSharedClientResources());
		pool.afterPropertiesSet();
		pool.getResource();
	}

	@Test
	public void testReturnResource() {
		GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
		poolConfig.setMaxTotal(1);
		poolConfig.setMaxWaitMillis(1);
		this.pool = new DefaultLettucePool(SettingsUtils.getHost(), SettingsUtils.getPort(), poolConfig);
		this.pool.setClientResources(LettuceTestClientResources.getSharedClientResources());
		pool.afterPropertiesSet();
		RedisAsyncConnection<byte[], byte[]> client = pool.getResource();
		assertNotNull(client);
		pool.returnResource(client);
		assertNotNull(pool.getResource());
		client.close();
	}

	@Test
	public void testReturnBrokenResource() {
		GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
		poolConfig.setMaxTotal(1);
		poolConfig.setMaxWaitMillis(1);
		this.pool = new DefaultLettucePool(SettingsUtils.getHost(), SettingsUtils.getPort(), poolConfig);
		this.pool.setClientResources(LettuceTestClientResources.getSharedClientResources());
		pool.afterPropertiesSet();
		RedisAsyncConnection<byte[], byte[]> client = pool.getResource();
		assertNotNull(client);
		pool.returnBrokenResource(client);
		RedisAsyncConnection<byte[], byte[]> client2 = pool.getResource();
		assertNotSame(client, client2);
		try {
			client.ping();
			fail("Broken resouce connection should be closed");
		} catch (RedisException e) {} finally {
			client.close();
			client2.close();
		}
	}

	@Test
	public void testCreateWithDbIndex() {
		this.pool = new DefaultLettucePool(SettingsUtils.getHost(), SettingsUtils.getPort());
		this.pool.setClientResources(LettuceTestClientResources.getSharedClientResources());
		pool.setDatabase(1);
		pool.afterPropertiesSet();
		assertNotNull(pool.getResource());
	}

	@Test(expected = PoolException.class)
	public void testCreateWithDbIndexInvalid() {
		this.pool = new DefaultLettucePool(SettingsUtils.getHost(), SettingsUtils.getPort());
		this.pool.setClientResources(LettuceTestClientResources.getSharedClientResources());
		pool.setDatabase(17);
		pool.afterPropertiesSet();
		pool.getResource();
	}

	@Test(expected = PoolException.class)
	public void testCreateWithPasswordNoPassword() {
		this.pool = new DefaultLettucePool(SettingsUtils.getHost(), SettingsUtils.getPort());
		this.pool.setClientResources(LettuceTestClientResources.getSharedClientResources());
		pool.setPassword("notthepassword");
		pool.afterPropertiesSet();
		pool.getResource();
	}

	@Ignore("Redis must have requirepass set to run this test")
	@Test
	public void testCreatePassword() {
		this.pool = new DefaultLettucePool(SettingsUtils.getHost(), SettingsUtils.getPort());
		this.pool.setClientResources(LettuceTestClientResources.getSharedClientResources());
		pool.setPassword("foo");
		pool.afterPropertiesSet();
		RedisAsyncConnection<byte[], byte[]> conn = pool.getResource();
		conn.ping();
		conn.close();
	}

	@Ignore("Redis must have requirepass set to run this test")
	@Test(expected = PoolException.class)
	public void testCreateInvalidPassword() {
		this.pool = new DefaultLettucePool(SettingsUtils.getHost(), SettingsUtils.getPort());
		this.pool.setClientResources(LettuceTestClientResources.getSharedClientResources());
		pool.setPassword("bad");
		pool.afterPropertiesSet();
		pool.getResource();
	}

	/**
	 * @see DATAREDIS-524
	 */
	@Test
	public void testCreateSentinelWithPassword() {

		pool = new DefaultLettucePool(new RedisSentinelConfiguration("mymaster", Collections.singleton("host:1234")));
		pool.setClientResources(LettuceTestClientResources.getSharedClientResources());
		pool.setPassword("foo");
		pool.afterPropertiesSet();

		RedisURI redisURI = (RedisURI) getField(pool.getClient(), "redisURI");

		assertThat(redisURI.getPassword(), is(equalTo(pool.getPassword().toCharArray())));
	}

	/**
	 * @see DATAREDIS-462
	 */
	@Test
	public void poolWorksWithoutClientResources() {

		this.pool = new DefaultLettucePool(SettingsUtils.getHost(), SettingsUtils.getPort());
		pool.setDatabase(1);
		pool.afterPropertiesSet();
		assertNotNull(pool.getResource());
	}
}
