/*
 * Copyright 2015 the original author or authors.
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

import static org.hamcrest.core.Is.*;
import static org.hamcrest.core.IsCollectionContaining.*;
import static org.hamcrest.core.IsNull.*;
import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;

/**
 * @author Christoph Strobl
 */
public class JedisClusterConnectionTests {

	static final List<HostAndPort> CLUSTER_NODES = Arrays.asList(new HostAndPort("127.0.0.1", 6379), new HostAndPort(
			"127.0.0.1", 6380), new HostAndPort("127.0.0.1", 6381));

	static final byte[] KEY_1_BYTES = JedisConverters.toBytes("key1");
	static final byte[] KEY_2_BYTES = JedisConverters.toBytes("key2");

	static final byte[] VALUE_1_BYTES = JedisConverters.toBytes("value1");
	static final byte[] VALUE_2_BYTES = JedisConverters.toBytes("value2");

	JedisCluster cluster;
	JedisClusterConnection clusterConnection;

	@BeforeClass
	public static void before() {

		Jedis jedis = new Jedis("127.0.0.1", 6379);
		String mode = JedisConverters.toProperties(jedis.info()).getProperty("redis_mode");
		jedis.close();

		Assume.assumeThat(mode, is("cluster"));
	}

	@Before
	public void setUp() throws IOException {

		cluster = new JedisCluster(new HashSet<HostAndPort>(CLUSTER_NODES));
		clusterConnection = new JedisClusterConnection(this.cluster);
	}

	@After
	public void tearDown() {

		for (JedisPool pool : cluster.getClusterNodes().values()) {
			pool.getResource().flushDB();
		}
		cluster.close();
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void shouldAllowSettingAndGettingValues() {

		clusterConnection.set(KEY_1_BYTES, VALUE_1_BYTES);
		assertThat(clusterConnection.get(KEY_1_BYTES), is(VALUE_1_BYTES));
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void delShouldRemoveSingleKeyCorrectly() {

		clusterConnection.set(KEY_1_BYTES, VALUE_1_BYTES);
		clusterConnection.del(KEY_1_BYTES);

		assertThat(clusterConnection.get(KEY_1_BYTES), nullValue());
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void delShouldRemoveMultipleKeysCorrectly() {

		clusterConnection.set(KEY_1_BYTES, VALUE_1_BYTES);
		clusterConnection.set(KEY_2_BYTES, VALUE_2_BYTES);

		clusterConnection.del(KEY_1_BYTES);
		clusterConnection.del(KEY_2_BYTES);

		assertThat(clusterConnection.get(KEY_1_BYTES), nullValue());
		assertThat(clusterConnection.get(KEY_2_BYTES), nullValue());
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void keysShouldReturnAllKeys() {

		clusterConnection.set(KEY_1_BYTES, VALUE_1_BYTES);
		clusterConnection.set(KEY_2_BYTES, VALUE_2_BYTES);

		assertThat(clusterConnection.keys(JedisConverters.toBytes("*")), hasItems(KEY_1_BYTES, KEY_2_BYTES));
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void randomKeyShouldReturnCorrectlyWhenKeysAvailable() {

		clusterConnection.set(KEY_1_BYTES, VALUE_1_BYTES);
		clusterConnection.set(KEY_2_BYTES, VALUE_2_BYTES);

		assertThat(clusterConnection.randomKey(), notNullValue());
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void randomKeyShouldReturnNullWhenNoKeysAvailable() {
		assertThat(clusterConnection.randomKey(), nullValue());
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void expireShouldBeSetCorreclty() {

		clusterConnection.set(KEY_1_BYTES, VALUE_1_BYTES);
		clusterConnection.expire(KEY_1_BYTES, 5);

		assertThat(cluster.ttl(JedisConverters.toString(KEY_1_BYTES)) > 1, is(true));
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void pExpireShouldBeSetCorreclty() {

		clusterConnection.set(KEY_1_BYTES, VALUE_1_BYTES);
		clusterConnection.pExpire(KEY_1_BYTES, 5000);

		assertThat(cluster.ttl(JedisConverters.toString(KEY_1_BYTES)) > 1, is(true));
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void expireAtShouldBeSetCorrectly() {

		clusterConnection.set(KEY_1_BYTES, VALUE_1_BYTES);
		clusterConnection.expireAt(KEY_1_BYTES, System.currentTimeMillis() / 1000 + 5000);

		assertThat(cluster.ttl(JedisConverters.toString(KEY_1_BYTES)) > 1, is(true));
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void pExpireAtShouldBeSetCorrectly() {

		clusterConnection.set(KEY_1_BYTES, VALUE_1_BYTES);
		clusterConnection.pExpireAt(KEY_1_BYTES, System.currentTimeMillis() + 5000);

		assertThat(cluster.ttl(JedisConverters.toString(KEY_1_BYTES)) > 1, is(true));
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void dbSizeShouldReturnCummulatedDbSize() {

		clusterConnection.set(KEY_1_BYTES, VALUE_1_BYTES);
		clusterConnection.set(KEY_2_BYTES, VALUE_2_BYTES);

		assertThat(clusterConnection.dbSize(), is(2L));
	}
}
