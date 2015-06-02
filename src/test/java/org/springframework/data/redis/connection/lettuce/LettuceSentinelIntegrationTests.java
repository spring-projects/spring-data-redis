/*
 * Copyright 2014 the original author or authors.
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

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.util.Collection;
import java.util.List;

import org.junit.*;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.connection.*;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.test.util.RedisSentinelRule;
import org.springframework.test.annotation.IfProfileValue;

/**
 * @author Christoph Strobl
 * @author Thomas Darimont
 * @author Mark Paluch
 */
public class LettuceSentinelIntegrationTests extends AbstractConnectionIntegrationTests {
	
	private static final String MASTER_NAME = "mymaster";
	private static final RedisServer SENTINEL_0 = new RedisServer("127.0.0.1", 26379);
	private static final RedisServer SENTINEL_1 = new RedisServer("127.0.0.1", 26380);
	
	private static final RedisServer SLAVE_0 = new RedisServer("127.0.0.1", 6380);
	private static final RedisServer SLAVE_1 = new RedisServer("127.0.0.1", 6381);
	
	private static final RedisSentinelConfiguration SENTINEL_CONFIG = new RedisSentinelConfiguration() //
			.master(MASTER_NAME)
			.sentinel(SENTINEL_0)
			.sentinel(SENTINEL_1);

	public @Rule RedisSentinelRule sentinelRule = RedisSentinelRule.forConfig(SENTINEL_CONFIG).oneActive();

	private static LettuceConnectionFactory lettuceConnectionFactory;

	@BeforeClass
	public static void beforeClass(){
		LettuceConnectionFactory lettuceConnectionFactory = new LettuceConnectionFactory(SENTINEL_CONFIG);
		lettuceConnectionFactory.setShareNativeConnection(false);
		lettuceConnectionFactory.afterPropertiesSet();
		LettuceSentinelIntegrationTests.lettuceConnectionFactory = lettuceConnectionFactory;
	}

	@AfterClass
	public static void afterClass(){
		LettuceSentinelIntegrationTests.lettuceConnectionFactory.destroy();
	}

	@Before
	public void setUp() {
		connectionFactory = lettuceConnectionFactory;
		super.setUp();
	}

	@After
	public void tearDown() {
		super.tearDown();
	}

	/**
	 * @see DATAREDIS-348
	 */
	@Test
	public void shouldReadMastersCorrectly() {

		List<RedisServer> servers = (List<RedisServer>) connectionFactory.getSentinelConnection().masters();
		assertThat(servers.size(), is(1));
		assertThat(servers.get(0).getName(),is(MASTER_NAME));
	}
	
	/**
	 * @see DATAREDIS-348
	 */
	@Test
	public void shouldReadSlavesOfMastersCorrectly() {

		RedisSentinelConnection sentinelConnection = connectionFactory.getSentinelConnection();
		
		List<RedisServer> servers = (List<RedisServer>) sentinelConnection.masters();
		assertThat(servers.size(), is(1));
		
		Collection<RedisServer> slaves = sentinelConnection.slaves(servers.get(0));
		assertThat(slaves.size(), is(2));
		assertThat(slaves, hasItems(SLAVE_0, SLAVE_1));
	}

}
