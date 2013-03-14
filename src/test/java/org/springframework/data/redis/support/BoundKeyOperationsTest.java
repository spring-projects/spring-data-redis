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
package org.springframework.data.redis.support;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.springframework.data.redis.ConnectionFactoryTracker;
import org.springframework.data.redis.connection.jredis.JredisConnectionFactory;
import org.springframework.data.redis.core.BoundKeyOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.support.collections.ObjectFactory;

/**
 * @author Costin Leau
 * @author Jennifer Hickey
 */
@RunWith(Parameterized.class)
public class BoundKeyOperationsTest {
	private BoundKeyOperations<Object> keyOps;
	private ObjectFactory<Object> objFactory;
	private RedisTemplate template;

	public BoundKeyOperationsTest(BoundKeyOperations<Object> keyOps, ObjectFactory<Object> objFactory,
			RedisTemplate template) {
		this.objFactory = objFactory;
		this.keyOps = keyOps;
		this.template = template;
		ConnectionFactoryTracker.add(template.getConnectionFactory());
	}

	@After
	public void stop() {
	}

	@AfterClass
	public static void cleanUp() {
		ConnectionFactoryTracker.cleanUp();
	}

	@Parameters
	public static Collection<Object[]> testParams() {
		return BoundKeyParams.testParams();
	}

	@Test
	public void testRename() throws Exception {
		Object key = keyOps.getKey();
		assertNotNull(key);
		Object newName = objFactory.instance();
		keyOps.rename(newName);
		assertEquals(newName, keyOps.getKey());
		keyOps.rename(key);
		assertEquals(key, keyOps.getKey());
	}

	@Test
	public void testExpire() throws Exception {
		assertEquals(Long.valueOf(-1), keyOps.getExpire());
		if (keyOps.expire(10, TimeUnit.SECONDS)) {
			long expire = keyOps.getExpire().longValue();
			assertTrue(expire <= 10 && expire > 5);
		}
	}

	@Test
	public void testPersist() throws Exception {
		assumeTrue(!isJredis());
		keyOps.persist();
		assertEquals(Long.valueOf(-1), keyOps.getExpire());
		if (keyOps.expire(10, TimeUnit.SECONDS)) {
			assertTrue(keyOps.getExpire().longValue() > 0);
		}
		keyOps.persist();
		assertEquals(-1, keyOps.getExpire().longValue());
	}

	private boolean isJredis() {
		return template.getConnectionFactory() instanceof JredisConnectionFactory;
	}
}