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

package org.springframework.datastore.redis.core;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.datastore.redis.connection.jredis.JRedisClientFactory;

public class RedisTemplateIntegrationTests {

	RedisTemplate template;
	@Before
	public void setUp() {
		template = new RedisTemplate(new JRedisClientFactory());
	}
	
	@Test
	public void conversions() {
		Person p = new Person("Joe", "Trader", 33);
		template.convertAndSet("trader:1", p);
		Person samePerson = template.getAndConvert("trader:1", Person.class);
		Assert.assertEquals(p, samePerson);		
	}
}
