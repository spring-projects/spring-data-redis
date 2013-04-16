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
package org.springframework.data.redis.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.List;

import org.junit.AfterClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.ConnectionFactoryTracker;
import org.springframework.data.redis.connection.ConnectionUtils;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.support.collections.CollectionTestParams;
import org.springframework.data.redis.support.collections.ObjectFactory;

/**
 * @author Costin Leau
 */
@RunWith(Parameterized.class)
public class TemplateTest {
	private ObjectFactory<Object> objFactory;
	private RedisTemplate template;

	public TemplateTest(ObjectFactory<Object> objFactory, RedisTemplate template) {
		this.objFactory = objFactory;
		this.template = template;
		ConnectionFactoryTracker.add(template.getConnectionFactory());
	}

	@AfterClass
	public static void cleanUp() {
		ConnectionFactoryTracker.cleanUp();
	}

	@Parameters
	public static Collection<Object[]> testParams() {
		return CollectionTestParams.testParams();
	}

	@Test(expected = IllegalArgumentException.class)
	public void testTemplateNotInitialized() throws Exception {
		RedisTemplate tpl = new RedisTemplate();
		tpl.setConnectionFactory(template.getConnectionFactory());
		tpl.exec();
	}

	@Test
	public void testKeys() throws Exception {
		assertTrue(template.keys("*") != null);
	}

	@Test
	public void testIncrement() throws Exception {
		// disable in case of Rjc
		if (ConnectionUtils.isRjc(template.getConnectionFactory())) {
			return;
		}

		StringRedisTemplate sr = new StringRedisTemplate(template.getConnectionFactory());
		String key = "test.template.inc";
		ValueOperations<String, String> valueOps = sr.opsForValue();
		valueOps.set(key, "10");
		valueOps.increment(key, -10);
		assertEquals(0, Integer.valueOf(valueOps.get(key)).intValue());
		valueOps.increment(key, -10);
		assertEquals(-10, Integer.valueOf(valueOps.get(key)).intValue());
	}

	//@Test
	public void testGetNonExistingKey() throws Exception {
		List<Object> res = (List<Object>) template.execute(new RedisCallback<List<Object>>() {

			public List<Object> doInRedis(RedisConnection connection) throws DataAccessException {
				connection.hGet("non-existing-key".getBytes(), "some-value".getBytes());
				return connection.closePipeline();
			}
		}, true, true);
	}
}
