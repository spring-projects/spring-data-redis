/*
 * Copyright 2013 the original author or authors.
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
package org.springframework.data.redis.core;

import java.util.Arrays;
import java.util.Set;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.IfProfileValue;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Integration test of {@link DefaultSetOperations}
 * 
 * @author Jennifer Hickey
 * 
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("RedisTemplateTests-context.xml")
public class DefaultSetOperationsTests {
	
	@Autowired
	private RedisTemplate<String, String> redisTemplate;
	
	private SetOperations<String, String> setOps;
	
	@Before
	public void setUp() {
		setOps = redisTemplate.opsForSet();
	}

	@After
	public void tearDown() {
		redisTemplate.getConnectionFactory().getConnection().flushDb();
	}
	
	@Test
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testRandomMembers() {
		setOps.add("test", "foo");
		setOps.add("test", "bar");
		setOps.add("test", "baz");
		Set<String> members = setOps.randomMembers("test", 2);
		assertEquals(2, members.size());
		assertTrue(Arrays.asList(new String[] {"foo", "bar", "baz"}).containsAll(members));
	}
}
