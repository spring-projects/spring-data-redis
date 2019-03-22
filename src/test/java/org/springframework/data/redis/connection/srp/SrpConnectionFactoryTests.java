/*
 * Copyright 2013-2019 the original author or authors.
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
package org.springframework.data.redis.connection.srp;

import org.junit.Ignore;
import org.junit.Test;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.connection.RedisConnection;
import static org.junit.Assert.fail;

/**
 * Integration test of {@link SrpConnectionFactory}
 * 
 * @author Jennifer Hickey
 */
public class SrpConnectionFactoryTests {

	@Test
	public void testConnect() {
		SrpConnectionFactory factory = new SrpConnectionFactory(SettingsUtils.getHost(), SettingsUtils.getPort());
		factory.afterPropertiesSet();
		RedisConnection connection = factory.getConnection();
		connection.ping();
	}

	@Test
	public void testConnectInvalidHost() {
		SrpConnectionFactory factory = new SrpConnectionFactory();
		factory.setHostName("fakeHost");
		factory.afterPropertiesSet();
		try {
			factory.getConnection();
			fail("Expected a connection failure exception");
		} catch (RedisConnectionFailureException e) {}
	}

	@Ignore("Redis must have requirepass set to run this test")
	@Test
	public void testConnectWithPassword() {
		SrpConnectionFactory factory = new SrpConnectionFactory(SettingsUtils.getHost(), SettingsUtils.getPort());
		factory.setPassword("foob");
		factory.afterPropertiesSet();
		RedisConnection connection = factory.getConnection();
		connection.ping();
		RedisConnection connection2 = factory.getConnection();
		connection2.ping();
	}
}
