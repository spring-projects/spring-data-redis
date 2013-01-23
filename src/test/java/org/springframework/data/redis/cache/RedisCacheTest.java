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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.data.redis.ConnectionFactoryTracker;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.support.collections.CollectionTestParams;
import org.springframework.data.redis.support.collections.ObjectFactory;

/**
 * @author Costin Leau
 */
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

	
	protected Cache createCache(RedisTemplate nativeCache) {
		return new RedisCache(CACHE_NAME, CACHE_NAME.concat(":").getBytes(), nativeCache, TimeUnit.MINUTES.toSeconds(5));
	}

	
	protected RedisTemplate createNativeCache() throws Exception {
		return template;
	}

	@Before
	public void setUp() throws Exception {
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

		cache.put(key1, value1);
		cache.put(key2, value2);

		Thread th = new Thread(new Runnable() {

			public void run() {
				cache.clear();
				cache.put(value1, key1);
				cache.put(value2, key2);
			}
		}, "concurrent-cache-access");

		th.start();
		th.join();

		final Object key3 = getObject();
		final Object value3 = getObject();

		cache.put(key3, value3);
		cache.put(value3, key3);

		assertNull(cache.get(key1));
		assertNull(cache.get(key2));
		assertEquals(key1, cache.get(value1).get());
	}
	
	@Test
	public void testCacheName() throws Exception {
		CacheManager redisCM = new RedisCacheManager(template);
		String cacheName = "s2gx11";
		Cache cache = redisCM.getCache(cacheName);
		assertNotNull(cache);
		assertTrue(redisCM.getCacheNames().contains(cacheName));
	}
}