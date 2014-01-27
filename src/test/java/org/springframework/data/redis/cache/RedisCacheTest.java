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

package org.springframework.data.redis.cache;

import static org.hamcrest.core.IsInstanceOf.*;
import static org.hamcrest.core.IsNull.*;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.*;
import static org.springframework.data.redis.matcher.RedisTestMatchers.isEqual;

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
import org.springframework.data.redis.core.AbstractOperationsTestParams;
import org.springframework.data.redis.core.RedisTemplate;

/**
 * @author Costin Leau
 * @author Jennifer Hickey
 * @author Christoph Strobl
 */
@SuppressWarnings("rawtypes")
@RunWith(Parameterized.class)
public class RedisCacheTest extends AbstractNativeCacheTest<RedisTemplate> {

	private ObjectFactory<Object> keyFactory;
	private ObjectFactory<Object> valueFactory;
	private RedisTemplate template;

	public RedisCacheTest(RedisTemplate template, ObjectFactory<Object> keyFactory, ObjectFactory<Object> valueFactory) {
		this.keyFactory = keyFactory;
		this.valueFactory = valueFactory;
		this.template = template;
		ConnectionFactoryTracker.add(template.getConnectionFactory());
	}

	@Parameters
	public static Collection<Object[]> testParams() {
		return AbstractOperationsTestParams.testParams();
	}

	@SuppressWarnings("unchecked")
	protected Cache createCache(RedisTemplate nativeCache) {
		return new RedisCache(CACHE_NAME, CACHE_NAME.concat(":").getBytes(), nativeCache, TimeUnit.MINUTES.toSeconds(10));
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

	protected Object getValue() {
		return valueFactory.instance();
	}

	protected Object getKey() {
		return keyFactory.instance();
	}

	@Test
	public void testConcurrentRead() throws Exception {
		final Object key1 = getKey();
		final Object value1 = getValue();

		final Object k1 = getKey();
		final Object v1 = getValue();

		final Object key2 = getKey();
		final Object value2 = getValue();

		final Object k2 = getKey();
		final Object v2 = getValue();

		final AtomicBoolean failed = new AtomicBoolean(true);
		cache.put(key1, value1);
		cache.put(key2, value2);

		Thread th = new Thread(new Runnable() {
			public void run() {
				cache.clear();
				cache.put(k1, v1);
				cache.put(k2, v2);
				failed.set(v1.equals(cache.get(k1)));

			}
		}, "concurrent-cache-access");
		th.start();
		th.join();

		assertFalse(failed.get());

		final Object key3 = getKey();
		final Object key4 = getKey();
		final Object value3 = getValue();
		final Object value4 = getValue();

		cache.put(key3, value3);
		cache.put(key4, value4);

		assertNull(cache.get(key1));
		assertNull(cache.get(key2));
		ValueWrapper valueWrapper = cache.get(k1);
		assertNotNull(valueWrapper);
		assertThat(valueWrapper.get(), isEqual(v1));
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
		final Object key1 = getKey();
		final Object value1 = getValue();
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
					cache.put(key1, value1);
				} catch (IllegalMonitorStateException e) {
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

	/**
	 * @see DATAREDIS-243
	 */
	@Test
	public void testCacheGetShouldReturnCachedInstance() {
		assumeThat(cache, instanceOf(RedisCache.class));

		Object key = getKey();
		Object value = getValue();
		cache.put(key, value);

		assertThat(value, isEqual(((RedisCache) cache).get(key, Object.class)));
	}

	/**
	 * @see DATAREDIS-243
	 */
	@Test
	public void testCacheGetShouldRetunInstanceOfCorrectType() {
		assumeThat(cache, instanceOf(RedisCache.class));

		Object key = getKey();
		Object value = getValue();
		cache.put(key, value);

		RedisCache redisCache = (RedisCache) cache;
		assertThat(redisCache.get(key, value.getClass()), instanceOf(value.getClass()));
	}

	/**
	 * @see DATAREDIS-243
	 */
	@Test(expected = ClassCastException.class)
	public void testCacheGetShouldThrowExceptionOnInvalidType() {
		assumeThat(cache, instanceOf(RedisCache.class));

		Object key = getKey();
		Object value = getValue();
		cache.put(key, value);

		RedisCache redisCache = (RedisCache) cache;
		@SuppressWarnings("unused")
		Cache retrievedObject = redisCache.get(key, Cache.class);
	}

	/**
	 * @see DATAREDIS-243
	 */
	@Test
	public void testCacheGetShouldReturnNullIfNoCachedValueFound() {
		assumeThat(cache, instanceOf(RedisCache.class));

		Object key = getKey();
		Object value = getValue();
		cache.put(key, value);

		RedisCache redisCache = (RedisCache) cache;

		Object invalidKey = template.getKeySerializer() == null ? "spring-data-redis".getBytes() : "spring-data-redis";
		assertThat(redisCache.get(invalidKey, value.getClass()), nullValue());
	}
}
