/*
 * Copyright 2013-2018 the original author or authors.
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

import io.lettuce.core.RedisException;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.connection.PoolException;
import org.springframework.data.redis.connection.RedisSentinelConfiguration;

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

		if (pool != null) {

			if (pool.getClient() != null) {
				pool.getClient().shutdown(0, 0, TimeUnit.MILLISECONDS);
			}

			pool.destroy();
		}
	}

	@Test
	public void testGetResource() {

		pool = new DefaultLettucePool(SettingsUtils.getHost(), SettingsUtils.getPort());
		pool.setClientResources(LettuceTestClientResources.getSharedClientResources());
		pool.afterPropertiesSet();
		StatefulRedisConnection<byte[], byte[]> client = (StatefulRedisConnection<byte[], byte[]>) pool.getResource();
		assertNotNull(client);
		client.sync().ping();
		client.close();
	}

	@Test
	public void testGetResourcePoolExhausted() {

		GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
		poolConfig.setMaxTotal(1);
		poolConfig.setMaxWaitMillis(1);
		pool = new DefaultLettucePool(SettingsUtils.getHost(), SettingsUtils.getPort(), poolConfig);
		pool.setClientResources(LettuceTestClientResources.getSharedClientResources());
		pool.afterPropertiesSet();
		StatefulRedisConnection<byte[], byte[]> client = (StatefulRedisConnection<byte[], byte[]>) pool.getResource();
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

		GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
		poolConfig.setTestOnBorrow(true);
		pool = new DefaultLettucePool(SettingsUtils.getHost(), SettingsUtils.getPort(), poolConfig);
		pool.setClientResources(LettuceTestClientResources.getSharedClientResources());
		pool.afterPropertiesSet();
		StatefulRedisConnection<byte[], byte[]> client = (StatefulRedisConnection<byte[], byte[]>) pool.getResource();
		assertNotNull(client);
		client.close();
	}

	@Test(expected = PoolException.class)
	public void testGetResourceCreationUnsuccessful() throws Exception {

		pool = new DefaultLettucePool(SettingsUtils.getHost(), 3333);
		pool.setClientResources(LettuceTestClientResources.getSharedClientResources());
		pool.afterPropertiesSet();
		pool.getResource();
	}

	@Test
	public void testReturnResource() {

		GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
		poolConfig.setMaxTotal(1);
		poolConfig.setMaxWaitMillis(1);
		pool = new DefaultLettucePool(SettingsUtils.getHost(), SettingsUtils.getPort(), poolConfig);
		pool.setClientResources(LettuceTestClientResources.getSharedClientResources());
		pool.afterPropertiesSet();
		StatefulRedisConnection<byte[], byte[]> client = (StatefulRedisConnection<byte[], byte[]>) pool.getResource();
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
		pool = new DefaultLettucePool(SettingsUtils.getHost(), SettingsUtils.getPort(), poolConfig);
		pool.setClientResources(LettuceTestClientResources.getSharedClientResources());
		pool.afterPropertiesSet();
		StatefulRedisConnection<byte[], byte[]> client = (StatefulRedisConnection<byte[], byte[]>) pool.getResource();
		assertNotNull(client);
		pool.returnBrokenResource(client);
		StatefulRedisConnection<byte[], byte[]> client2 = (StatefulRedisConnection<byte[], byte[]>) pool.getResource();
		assertNotSame(client, client2);
		try {
			client.sync().ping();
			fail("Broken resouce connection should be closed");
		} catch (RedisException e) {} finally {
			client.close();
			client2.close();
		}
	}

	@Test
	public void testCreateWithDbIndex() {

		pool = new DefaultLettucePool(SettingsUtils.getHost(), SettingsUtils.getPort());
		pool.setClientResources(LettuceTestClientResources.getSharedClientResources());
		pool.setDatabase(1);
		pool.afterPropertiesSet();
		assertNotNull(pool.getResource());
	}

	@Test(expected = PoolException.class)
	public void testCreateWithDbIndexInvalid() {

		pool = new DefaultLettucePool(SettingsUtils.getHost(), SettingsUtils.getPort());
		pool.setClientResources(LettuceTestClientResources.getSharedClientResources());
		pool.setDatabase(17);
		pool.afterPropertiesSet();
		pool.getResource();
	}

	@Test(expected = PoolException.class)
	public void testCreateWithPasswordNoPassword() {

		pool = new DefaultLettucePool(SettingsUtils.getHost(), SettingsUtils.getPort());
		pool.setClientResources(LettuceTestClientResources.getSharedClientResources());
		pool.setPassword("notthepassword");
		pool.afterPropertiesSet();
		pool.getResource();
	}

	@Ignore("Redis must have requirepass set to run this test")
	@Test
	public void testCreatePassword() {

		pool = new DefaultLettucePool(SettingsUtils.getHost(), SettingsUtils.getPort());
		pool.setClientResources(LettuceTestClientResources.getSharedClientResources());
		pool.setPassword("foo");
		pool.afterPropertiesSet();
		StatefulRedisConnection<byte[], byte[]> client = (StatefulRedisConnection<byte[], byte[]>) pool.getResource();
		client.sync().ping();
		client.sync().getStatefulConnection().close();
	}

	@Ignore("Redis must have requirepass set to run this test")
	@Test(expected = PoolException.class)
	public void testCreateInvalidPassword() {

		pool = new DefaultLettucePool(SettingsUtils.getHost(), SettingsUtils.getPort());
		pool.setClientResources(LettuceTestClientResources.getSharedClientResources());
		pool.setPassword("bad");
		pool.afterPropertiesSet();
		pool.getResource();
	}

	@Test // DATAREDIS-524
	public void testCreateSentinelWithPassword() {

		pool = new DefaultLettucePool(new RedisSentinelConfiguration("mymaster", Collections.singleton("host:1234")));
		pool.setClientResources(LettuceTestClientResources.getSharedClientResources());
		pool.setPassword("foo");
		pool.afterPropertiesSet();

		RedisURI redisUri = (RedisURI) getField(pool.getClient(), "redisURI");

		assertThat(redisUri.getPassword(), is(equalTo(pool.getPassword().toCharArray())));
	}

	@Test // DATAREDIS-462
	public void poolWorksWithoutClientResources() {

		pool = new DefaultLettucePool(SettingsUtils.getHost(), SettingsUtils.getPort());
		pool.setDatabase(1);
		pool.afterPropertiesSet();
		assertNotNull(pool.getResource());
	}
}
