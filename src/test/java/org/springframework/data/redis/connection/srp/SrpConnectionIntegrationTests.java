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

package org.springframework.data.redis.connection.srp;

import org.junit.Test;
import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.connection.AbstractConnectionIntegrationTests;
import org.springframework.data.redis.connection.RedisConnectionFactory;

import redis.client.RedisClient;

/**
 * @author Costin Leau
 */
public class SrpConnectionIntegrationTests extends AbstractConnectionIntegrationTests {
	SrpConnectionFactory factory;

	public SrpConnectionIntegrationTests() {
		factory = new SrpConnectionFactory();
		factory.setPort(SettingsUtils.getPort());
		factory.setHostName(SettingsUtils.getHost());

		factory.afterPropertiesSet();
	}


	protected RedisConnectionFactory getConnectionFactory() {
		return factory;
	}

	@Test
	public void testRaw() throws Exception {
		RedisClient rc = (RedisClient) factory.getConnection().getNativeConnection();

		System.out.println(rc.dbsize());
		System.out.println(rc.exists("foobar"));
		rc.set("foobar", "barfoo");
		System.out.println(rc.get("foobar"));
	}

	public void testNullCollections() throws Exception {
	}
}
