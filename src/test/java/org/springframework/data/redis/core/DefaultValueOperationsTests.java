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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.IfProfileValue;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Integration test of {@link DefaultValueOperations}
 * 
 * @author Jennifer Hickey
 * 
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("RedisTemplateTests-context.xml")
public class DefaultValueOperationsTests {

	@Autowired
	private RedisTemplate<String, String> redisTemplate;

	private ValueOperations<String, String> valueOps;

	@Before
	public void setUp() {
		valueOps = redisTemplate.opsForValue();
	}

	@After
	public void tearDown() {
		redisTemplate.getConnectionFactory().getConnection().flushDb();
	}

	@Test
	public void testIncrementLong() throws Exception {
		String key = "test.template.inc";
		valueOps.set(key, "10");
		assertEquals(Long.valueOf(0), valueOps.increment(key, -10));
		assertEquals(0, Integer.valueOf(valueOps.get(key)).intValue());
		valueOps.increment(key, -10);
		assertEquals(-10, Integer.valueOf(valueOps.get(key)).intValue());
	}

	@Test
	@IfProfileValue(name = "redisVersion", value = "2.6")
	public void testIncrementDouble() {
		String key = "test.template.inc";
		valueOps.set(key, "10.5");
		assertEquals(Double.valueOf(11.9), valueOps.increment(key, 1.4));
		assertEquals("11.9", valueOps.get(key));
		valueOps.increment(key, -10d);
		assertEquals("1.9", valueOps.get(key));
	}

	@Test
	public void testMultiSetIfAbsent() {
		Map<String,String> keysAndValues = new HashMap<String,String>();
		keysAndValues.put("foo", "bar");
		keysAndValues.put("baz", "test");
		assertTrue(valueOps.multiSetIfAbsent(keysAndValues));
		assertEquals(new HashSet<String>(keysAndValues.values()),
				new HashSet<String>(valueOps.multiGet(keysAndValues.keySet())));
	}

	@Test
	public void testMultiSetIfAbsentFailure() {
		valueOps.set("foo", "alreadyset");
		Map<String,String> keysAndValues = new HashMap<String,String>();
		keysAndValues.put("foo", "bar");
		keysAndValues.put("baz", "test");
		assertFalse(valueOps.multiSetIfAbsent(keysAndValues));
	}
}
