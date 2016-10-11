/*
 * Copyright 2011-2016 the original author or authors.
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

import static edu.umd.cs.mtc.TestFramework.*;
import static org.assertj.core.api.Assertions.*;
import static org.hamcrest.core.IsInstanceOf.*;
import static org.junit.Assume.*;

import java.util.Collection;
import java.util.concurrent.Callable;
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
import org.springframework.data.redis.ConnectionFactoryTracker;
import org.springframework.data.redis.ObjectFactory;
import org.springframework.data.redis.StringObjectFactory;
import org.springframework.data.redis.core.AbstractOperationsTestParams;
import org.springframework.data.redis.core.RedisTemplate;

import edu.umd.cs.mtc.MultithreadedTestCase;

/**
 * @author Costin Leau
 * @author Jennifer Hickey
 * @author Christoph Strobl
 * @author Mark Paluch
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
	protected RedisCache createCache(RedisTemplate nativeCache) {
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

		assertThat(failed.get()).isFalse();

		final Object key3 = getKey();
		final Object key4 = getKey();
		final Object value3 = getValue();
		final Object value4 = getValue();

		cache.put(key3, value3);
		cache.put(key4, value4);

		assertThat(cache.get(key1)).isNull();
		assertThat(cache.get(key2)).isNull();
		ValueWrapper valueWrapper = cache.get(k1);
		assertThat(valueWrapper).isNotNull();
		assertThat(valueWrapper.get()).isEqualTo(v1);
	}

	@Test
	public void testCacheName() throws Exception {

		RedisCacheManager redisCM = new RedisCacheManager(template);
		redisCM.afterPropertiesSet();

		String cacheName = "s2gx11";
		Cache cache = redisCM.getCache(cacheName);
		assertThat(cache).isNotNull();
		assertThat(redisCM.getCacheNames().contains(cacheName)).isTrue();
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
		assertThat(monitorStateException.get()).isFalse();
	}

	@Test // DATAREDIS-243
	public void testCacheGetShouldReturnCachedInstance() {
		assumeThat(cache, instanceOf(RedisCache.class));

		Object key = getKey();
		Object value = getValue();
		cache.put(key, value);

		assertThat(((RedisCache) cache).get(key, Object.class)).isEqualTo(value);
	}

	@Test // DATAREDIS-243
	public void testCacheGetShouldRetunInstanceOfCorrectType() {
		assumeThat(cache, instanceOf(RedisCache.class));

		Object key = getKey();
		Object value = getValue();
		cache.put(key, value);

		RedisCache redisCache = (RedisCache) cache;
		assertThat(redisCache.get(key, value.getClass())).isInstanceOf(value.getClass());
	}

	@Test(expected = ClassCastException.class) // DATAREDIS-243
	public void testCacheGetShouldThrowExceptionOnInvalidType() {
		assumeThat(cache, instanceOf(RedisCache.class));

		Object key = getKey();
		Object value = getValue();
		cache.put(key, value);

		RedisCache redisCache = (RedisCache) cache;
		@SuppressWarnings("unused")
		Cache retrievedObject = redisCache.get(key, Cache.class);
	}

	@Test // DATAREDIS-243
	public void testCacheGetShouldReturnNullIfNoCachedValueFound() {
		assumeThat(cache, instanceOf(RedisCache.class));

		Object key = getKey();
		Object value = getValue();
		cache.put(key, value);

		RedisCache redisCache = (RedisCache) cache;

		Object invalidKey = template.getKeySerializer() == null ? "spring-data-redis".getBytes() : "spring-data-redis";
		assertThat(redisCache.get(invalidKey, value.getClass())).isNull();
	}

	@Test // DATAREDIS-344, DATAREDIS-416
	public void putIfAbsentShouldSetValueOnlyIfNotPresent() {

		assumeThat(cache, instanceOf(RedisCache.class));

		RedisCache redisCache = (RedisCache) cache;

		Object key = getKey();
		template.delete(key);

		Object value = getValue();

		assertThat(redisCache.putIfAbsent(key, value)).isNull();

		ValueWrapper wrapper = redisCache.putIfAbsent(key, value);

		if (!(value instanceof Number)) {
			assertThat(wrapper.get()).isNotSameAs(value);
		}

		assertThat(wrapper.get()).isEqualTo(value);
	}

	@Test // DATAREDIS-510
	public void cachePutWithNullShouldNotAddStuffToRedis() {

		Object key = getKey();
		Object value = getValue();

		cache.put(key, null);

		assertThat(cache.get(key)).isNull();
	}

	@Test // DATAREDIS-510
	public void cachePutWithNullShouldRemoveKeyIfExists() {

		Object key = getKey();
		Object value = getValue();

		cache.put(key, value);

		assertThat(cache.get(key).get()).isEqualTo(value);

		cache.put(key, null);

		assertThat(cache.get(key)).isNull();
	}

	@Test // DATAREDIS-443, DATAREDIS-452
	public void testCacheGetSynchronized() throws Throwable {

		assumeThat(cache, instanceOf(RedisCache.class));
		assumeThat(valueFactory, instanceOf(StringObjectFactory.class));

		runOnce(new CacheGetWithValueLoaderIsThreadSafe((RedisCache) cache));
	}

	@SuppressWarnings("unused")
	private static class CacheGetWithValueLoaderIsThreadSafe extends MultithreadedTestCase {

		RedisCache redisCache;
		TestCacheLoader<String> cacheLoader;

		public CacheGetWithValueLoaderIsThreadSafe(RedisCache redisCache) {

			this.redisCache = redisCache;

			cacheLoader = new TestCacheLoader<String>("test") {

				@Override
				public String call() throws Exception {

					waitForTick(2);
					return super.call();
				}
			};
		}

		public void thread1() {

			assertTick(0);
			assertThat(redisCache.get("key", cacheLoader)).isEqualTo("test");
		}

		public void thread2() {

			waitForTick(1);
			assertThat(redisCache.get("key", new TestCacheLoader<String>("illegal value"))).isEqualTo("test");
			assertTick(2);
		}
	}

	private static class TestCacheLoader<T> implements Callable<T> {

		private final T value;

		public TestCacheLoader(T value) {
			this.value = value;
		}

		@Override
		public T call() throws Exception {
			return value;
		}
	}
}
