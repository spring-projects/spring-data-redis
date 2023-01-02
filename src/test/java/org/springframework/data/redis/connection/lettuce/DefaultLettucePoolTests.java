/*
 * Copyright 2013-2023 the original author or authors.
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
import static org.springframework.test.util.ReflectionTestUtils.*;

import io.lettuce.core.RedisException;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.connection.PoolException;
import org.springframework.data.redis.connection.RedisSentinelConfiguration;
import org.springframework.data.redis.test.extension.LettuceTestClientResources;

/**
 * Unit test of {@link DefaultLettucePool}
 *
 * @author Jennifer Hickey
 * @author Thomas Darimont
 * @author Christoph Strobl
 * @author Mark Paluch
 */
class DefaultLettucePoolTests {

	private DefaultLettucePool pool;

	@AfterEach
	void tearDown() {

		if (pool != null) {

			if (pool.getClient() != null) {
				pool.getClient().shutdown(0, 0, TimeUnit.MILLISECONDS);
			}

			pool.destroy();
		}
	}

	@Test
	void testGetResource() {

		pool = new DefaultLettucePool(SettingsUtils.getHost(), SettingsUtils.getPort());
		pool.setClientResources(LettuceTestClientResources.getSharedClientResources());
		pool.afterPropertiesSet();
		StatefulRedisConnection<byte[], byte[]> client = (StatefulRedisConnection<byte[], byte[]>) pool.getResource();
		assertThat(client).isNotNull();
		client.sync().ping();
		client.close();
	}

	@Test
	void testGetResourcePoolExhausted() {

		GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
		poolConfig.setMaxTotal(1);
		poolConfig.setMaxWaitMillis(1);
		pool = new DefaultLettucePool(SettingsUtils.getHost(), SettingsUtils.getPort(), poolConfig);
		pool.setClientResources(LettuceTestClientResources.getSharedClientResources());
		pool.afterPropertiesSet();
		StatefulRedisConnection<byte[], byte[]> client = (StatefulRedisConnection<byte[], byte[]>) pool.getResource();
		assertThat(client).isNotNull();
		try {
			pool.getResource();
			fail("PoolException should be thrown when pool exhausted");
		} catch (PoolException e) {} finally {
			client.close();
		}
	}

	@Test
	void testGetResourceValidate() {

		GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
		poolConfig.setTestOnBorrow(true);
		pool = new DefaultLettucePool(SettingsUtils.getHost(), SettingsUtils.getPort(), poolConfig);
		pool.setClientResources(LettuceTestClientResources.getSharedClientResources());
		pool.afterPropertiesSet();
		StatefulRedisConnection<byte[], byte[]> client = (StatefulRedisConnection<byte[], byte[]>) pool.getResource();
		assertThat(client).isNotNull();
		client.close();
	}

	@Test
	void testGetResourceCreationUnsuccessful() throws Exception {

		pool = new DefaultLettucePool(SettingsUtils.getHost(), 3333);
		pool.setClientResources(LettuceTestClientResources.getSharedClientResources());
		pool.afterPropertiesSet();
		assertThatExceptionOfType(PoolException.class).isThrownBy(() -> pool.getResource());
	}

	@Test
	void testReturnResource() {

		GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
		poolConfig.setMaxTotal(1);
		poolConfig.setMaxWaitMillis(1);
		pool = new DefaultLettucePool(SettingsUtils.getHost(), SettingsUtils.getPort(), poolConfig);
		pool.setClientResources(LettuceTestClientResources.getSharedClientResources());
		pool.afterPropertiesSet();
		StatefulRedisConnection<byte[], byte[]> client = (StatefulRedisConnection<byte[], byte[]>) pool.getResource();
		assertThat(client).isNotNull();
		pool.returnResource(client);
		assertThat(pool.getResource()).isNotNull();
		client.close();
	}

	@Test
	void testReturnBrokenResource() {

		GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
		poolConfig.setMaxTotal(1);
		poolConfig.setMaxWaitMillis(1);
		pool = new DefaultLettucePool(SettingsUtils.getHost(), SettingsUtils.getPort(), poolConfig);
		pool.setClientResources(LettuceTestClientResources.getSharedClientResources());
		pool.afterPropertiesSet();
		StatefulRedisConnection<byte[], byte[]> client = (StatefulRedisConnection<byte[], byte[]>) pool.getResource();
		assertThat(client).isNotNull();
		pool.returnBrokenResource(client);
		StatefulRedisConnection<byte[], byte[]> client2 = (StatefulRedisConnection<byte[], byte[]>) pool.getResource();
		assertThat(client2).isNotSameAs(client);
		try {
			client.sync().ping();
			fail("Broken resouce connection should be closed");
		} catch (RedisException e) {} finally {
			client.close();
			client2.close();
		}
	}

	@Test
	void testCreateWithDbIndex() {

		pool = new DefaultLettucePool(SettingsUtils.getHost(), SettingsUtils.getPort());
		pool.setClientResources(LettuceTestClientResources.getSharedClientResources());
		pool.setDatabase(1);
		pool.afterPropertiesSet();
		assertThat(pool.getResource()).isNotNull();
	}

	@Test
	void testCreateWithDbIndexInvalid() {

		pool = new DefaultLettucePool(SettingsUtils.getHost(), SettingsUtils.getPort());
		pool.setClientResources(LettuceTestClientResources.getSharedClientResources());
		pool.setDatabase(17);
		pool.afterPropertiesSet();
		assertThatExceptionOfType(PoolException.class).isThrownBy(() -> pool.getResource());
	}

	@Test // DATAREDIS-524
	void testCreateSentinelWithPassword() {

		pool = new DefaultLettucePool(new RedisSentinelConfiguration("mymaster", Collections.singleton("host:1234")));
		pool.setClientResources(LettuceTestClientResources.getSharedClientResources());
		pool.setPassword("foo");
		pool.afterPropertiesSet();

		RedisURI redisUri = (RedisURI) getField(pool.getClient(), "redisURI");

		assertThat(redisUri.getPassword()).isEqualTo(pool.getPassword().toCharArray());
	}

	@Test // DATAREDIS-462
	void poolWorksWithoutClientResources() {

		pool = new DefaultLettucePool(SettingsUtils.getHost(), SettingsUtils.getPort());
		pool.setDatabase(1);
		pool.afterPropertiesSet();
		assertThat(pool.getResource()).isNotNull();
	}
}
