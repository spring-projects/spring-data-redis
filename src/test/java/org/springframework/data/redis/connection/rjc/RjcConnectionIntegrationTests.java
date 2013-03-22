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
package org.springframework.data.redis.connection.rjc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.List;
import java.util.UUID;

import org.junit.Ignore;
import org.junit.Test;
import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.connection.AbstractConnectionIntegrationTests;
import org.springframework.data.redis.connection.RedisConnectionFactory;

/**
 * @author Costin Leau
 */
public class RjcConnectionIntegrationTests extends AbstractConnectionIntegrationTests {

	RjcConnectionFactory factory;

	public RjcConnectionIntegrationTests() {
		factory = new RjcConnectionFactory();
		factory.setPort(SettingsUtils.getPort());
		factory.setHostName(SettingsUtils.getHost());

		factory.setUsePool(true);
		factory.afterPropertiesSet();
	}

	
	protected RedisConnectionFactory getConnectionFactory() {
		return factory;
	}

	@Test
	public void testMultiExec() throws Exception {
		byte[] key = "key".getBytes();
		byte[] value = "value".getBytes();

		connection.multi();
		connection.set(key, value);
		assertNull(connection.get(key));
		List<Object> results = connection.exec();
		assertEquals(2, results.size());
		assertEquals("OK", (String) results.get(0));
		assertEquals(new String(value), new String(RjcUtils.encode((String)results.get(1))));
	}

	// override test to address the encoding issue (the bytes[] in raw format differ)
	@Test
	public void testExecuteNativeWithPipeline() throws Exception {
		String key1 = getClass() + "#ExecuteNativeWithPipeline#1";
		String value1 = UUID.randomUUID().toString();
		String key2 = getClass() + "#ExecuteNativeWithPipeline#2";
		String value2 = UUID.randomUUID().toString();

		connection.openPipeline();
		connection.execute("SET", key1, value1);
		connection.execute("SET", key2, value2);
		connection.execute("GET", key1);
		connection.execute("GET", key2);
		List<Object> result = connection.closePipeline();
		assertEquals(4, result.size());
	}

	@Ignore("DATAREDIS-134 string ops do not work with encoded values")
	public void testSortStore() {
	}
}
