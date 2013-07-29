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

package org.springframework.data.redis.cache;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.springframework.cache.Cache;
import org.springframework.cache.Cache.ValueWrapper;
import org.springframework.cache.CacheManager;
import org.springframework.data.redis.ConnectionFactoryTracker;
import org.springframework.data.redis.ObjectFactory;
import org.springframework.data.redis.connection.ConnectionUtils;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.support.collections.CollectionTestParams;

/**
 * @author Costin Leau
 * @author Jennifer Hickey
 */
@SuppressWarnings("rawtypes")
@RunWith(Parameterized.class)
public class RedisCacheTest extends AbstractNativeCacheTest<RedisTemplate> {

	private ObjectFactory<Object> objFactory;
	private RedisTemplate template;


	public RedisCacheTest(ObjectFactory<Object> objFactory, RedisTemplate template) {
		this.objFactory = objFactory;
		this.template = template;
		ConnectionFactoryTracker.add(template.getConnectionFactory());
	}

	@Parameters
	public static Collection<Object[]> testParams() {
		return CollectionTestParams.testParams();
	}


	@SuppressWarnings("unchecked")
	protected Cache createCache(RedisTemplate nativeCache) {
		return new RedisCache(CACHE_NAME, CACHE_NAME.concat(":").getBytes(), nativeCache,
				TimeUnit.MINUTES.toSeconds(10));
	}


	protected RedisTemplate createNativeCache() throws Exception {
		return template;
	}

	@Before
	public void setUp() throws Exception {
		assumeTrue(!ConnectionUtils.isJredis(template.getConnectionFactory()));
		ConnectionFactoryTracker.add(template.getConnectionFactory());
		super.setUp();
	}

	@AfterClass
	public static void cleanUp() {
		ConnectionFactoryTracker.cleanUp();
	}


	protected Object getObject() {
		return objFactory.instance();
	}

	@Test
	public void testConcurrentRead() throws Exception {
		final Object key1 = getObject();
		final Object value1 = getObject();

		final Object key2 = getObject();
		final Object value2 = getObject();

		final AtomicBoolean failed = new AtomicBoolean(true);
		cache.put(key1, value1);
		cache.put(key2, value2);

		Thread th = new Thread(new Runnable() {
			public void run() {
				cache.clear();
				cache.put(value1, key1);
				cache.put(value2, key2);
				failed.set(key1.equals(cache.get(value1)));

			}
		}, "concurrent-cache-access");
		th.start();
		th.join();

		assertFalse(failed.get());

		final Object key3 = getObject();
		final Object value3 = getObject();

		cache.put(key3, value3);
		cache.put(value3, key3);

		assertNull(cache.get(key1));
		assertNull(cache.get(key2));
		ValueWrapper valueWrapper = cache.get(value1);
		assertNotNull(valueWrapper);
		assertEquals(key1, valueWrapper.get());
	}

	@Test
	public void testCacheName() throws Exception {
		CacheManager redisCM = new RedisCacheManager(template);
		String cacheName = "s2gx11";
		Cache cache = redisCM.getCache(cacheName);
		assertNotNull(cache);
		assertTrue(redisCM.getCacheNames().contains(cacheName));
	}

	@Test
	public void testGetWhileClear() throws InterruptedException {
		int numTries = 10;
		final AtomicBoolean monitorStateException = new AtomicBoolean(false);
		final CountDownLatch latch = new CountDownLatch(numTries);
		Runnable clearCache = new Runnable() {
			public void run() {
				cache.clear();
			}
		};
		Runnable putCache = new Runnable() {
			public void run() {
				try {
					cache.put("foo", "bar");
				}catch(IllegalMonitorStateException e) {
					monitorStateException.set(true);
				} finally {
					latch.countDown();
				}
			}
		};
		for (int i = 0; i < numTries; i++) {
			new Thread(clearCache).start();
			new Thread(putCache).start();
		}
		latch.await();
		assertFalse(monitorStateException.get());
	}
}
