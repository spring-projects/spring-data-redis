/*
 * Copyright 2002-2010 the original author or authors.
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

package org.springframework.datastore.redis.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Test;
import org.springframework.dao.InvalidDataAccessApiUsageException;

import redis.clients.jedis.JedisException;

public abstract class AbstractClientIntegrationTests {

	protected RedisClient client;
	
	@After
	public void tearDown() throws IOException {
		client.disconnect();
	}
	@Test
	public void save() {
		String status = client.save();
		assertEquals("OK", status);
	}

	@Test
	public void bgsave() {
		try {
			String status = client.bgsave();
			assertEquals("Background saving started", status);
		} catch (InvalidDataAccessApiUsageException e) {
			assertEquals("ERR Background save already in progress",
					e.getMessage());
		}
	}

	@Test
	public void bgrewriteaof() {
		String status = client.bgrewriteaof();
		assertEquals("Background append only file rewriting started", status);
	}

	@Test
	public void lastsave() throws InterruptedException {
		int before = client.lastsave();
		String st = "";
		while (!st.equals("OK")) {
			try {
				Thread.sleep(1000);
				st = client.save();
			} catch (JedisException e) {

			}
		}
		int after = client.lastsave();
		assertTrue((after - before) > 0);
	}
	
	
	
	@Test
	public void info() {			
		Map<String,String> infoResponse = client.info();
		Assert.assertNotNull(infoResponse);
		Assert.assertTrue(infoResponse.containsKey("redis_version"));
		//Map<String, String> infoResponse = client.info();
		//Assert.assertTrue("Expected non empty map of info about the server.", infoResponse.size() > 0);
		//Assert.assertTrue("Expected key 'redis_version' in map of info about the server.",
		//				infoResponse.containsKey("redis_version"));
	}

	@Test
	public void setAndGet() {		
		client.set("foo", "blah blah"); 	
		String value = client.get("foo");
		Assert.assertEquals("blah blah", value);
	}
	
	@Test
	public void conversions() {
		Person p = new Person("Joe", "Trader", 33);
		
	}
	

}
