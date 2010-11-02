/*
 * Copyright 2010 the original author or authors.
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

package org.springframework.datastore.redis.core.jedis;

import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.springframework.datastore.redis.core.AbstractClientIntegrationTests;
import org.springframework.datastore.redis.core.RedisClientFactory;
import org.springframework.datastore.redis.core.RedisTemplate;
import org.springframework.datastore.redis.util.RedisSet;

public class JedisRedisClientIntegrationTests extends
		AbstractClientIntegrationTests {

	RedisClientFactory clientFactory;
	@Before
	public void setUp() {
		clientFactory = new JedisClientFactory();
		clientFactory.setPassword("foobared");
		client = clientFactory.createClient();
		client.flushAll();
	}

	@Test
	public void setAdd() {
		client.sadd("s1", "1");
		client.sadd("s1", "2");
		client.sadd("s1", "3");
		client.sadd("s2", "2");
		client.sadd("s2", "3");
		Set<String> intersection = client.sinter("s1", "s2");
		System.out.println(intersection);
		
		
	}
	
	@Test
	public void setIntersectionTests() {
		RedisTemplate template = new RedisTemplate(clientFactory);
		RedisSet s1 = new RedisSet(template, "s1");
		s1.add("1"); s1.add("2"); s1.add("3");		
		RedisSet s2 = new RedisSet(template, "s2");
		s2.add("2"); s2.add("3");
		Set s3 = s1.intersection("s3", s1, s2);		
		for (Object object : s3) {
			System.out.println(object);
		}
		
	}

}
