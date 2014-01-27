/*
 * Copyright 2011-2014 the original author or authors.
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

import static org.junit.Assert.*;
import static org.junit.Assume.*;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.springframework.data.redis.ConnectionFactoryTracker;
import org.springframework.data.redis.ObjectFactory;
import org.springframework.data.redis.connection.ConnectionUtils;
import org.springframework.data.redis.core.BoundKeyOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.support.atomic.RedisAtomicInteger;
import org.springframework.data.redis.support.atomic.RedisAtomicLong;

/**
 * @author Costin Leau
 * @author Jennifer Hickey
 * @author Thomas Darimont
 */
@RunWith(Parameterized.class)
public class BoundKeyOperationsTest {
	private BoundKeyOperations keyOps;
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
	public void stop() {}

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
		// DATAREDIS-188 Infinite loop renaming a non-existent Collection when using Lettuce
		assumeTrue(!ConnectionUtils.isLettuce(template.getConnectionFactory()));
		Object key = keyOps.getKey();
		assertNotNull(key);
		// RedisAtomicInteger/Long need to be reset, as they may be created
		// at start of test run and underlying key wiped out by other tests
		try {
			keyOps.getClass().getMethod("set", int.class).invoke(keyOps, 0);
		} catch (NoSuchMethodException e) {}
		try {
			keyOps.getClass().getMethod("set", long.class).invoke(keyOps, 0l);
		} catch (NoSuchMethodException e) {}
		Object newName = objFactory.instance();
		keyOps.rename(newName);
		assertEquals(newName, keyOps.getKey());
		keyOps.rename(key);
		assertEquals(key, keyOps.getKey());
	}

	/**
	 * @see DATAREDIS-251
	 */
	@Test
	public void testExpire() throws Exception {

		populateBoundKey();

		assertEquals(keyOps.getClass().getName() + " -> " + keyOps.getKey(), Long.valueOf(-1), keyOps.getExpire());
		if (keyOps.expire(10, TimeUnit.SECONDS)) {
			long expire = keyOps.getExpire().longValue();
			assertTrue(expire <= 10 && expire > 5);
		}
	}

	/**
	 * @see DATAREDIS-251
	 */
	@Test
	public void testPersist() throws Exception {
		assumeTrue(!ConnectionUtils.isJredis(template.getConnectionFactory()));

		populateBoundKey();

		keyOps.persist();

		assertEquals(keyOps.getClass().getName() + " -> " + keyOps.getKey(), Long.valueOf(-1), keyOps.getExpire());
		if (keyOps.expire(10, TimeUnit.SECONDS)) {
			assertTrue(keyOps.getExpire().longValue() > 0);
		}
		keyOps.persist();
		assertEquals(keyOps.getClass().getName() + " -> " + keyOps.getKey(), -1, keyOps.getExpire().longValue());
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private void populateBoundKey() {

		if (keyOps instanceof List || keyOps instanceof Set) {
			((Collection) keyOps).add("dummy");
		} else if (keyOps instanceof Map) {
			((Map) keyOps).put("dummy", "dummy");
		} else if (keyOps instanceof RedisAtomicInteger) {
			((RedisAtomicInteger) keyOps).set(42);
		} else if (keyOps instanceof RedisAtomicLong) {
			((RedisAtomicLong) keyOps).set(42L);
		}
	}
}
