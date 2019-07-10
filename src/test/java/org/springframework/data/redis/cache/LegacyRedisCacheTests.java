/*
 * Copyright 2011-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
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
import static org.assertj.core.api.Assumptions.*;

import edu.umd.cs.mtc.MultithreadedTestCase;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.AfterClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import org.springframework.cache.Cache;
import org.springframework.cache.Cache.ValueRetrievalException;
import org.springframework.cache.Cache.ValueWrapper;
import org.springframework.data.redis.ConnectionFactoryTracker;
import org.springframework.data.redis.ObjectFactory;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.AbstractOperationsTestParams;
import org.springframework.data.redis.core.RedisTemplate;

/**
 * Tests moved over from 1.x line RedisCache implementation. Just removed somme of the limitations/assumtions previously
 * required.
 *
 * @author Costin Leau
 * @author Jennifer Hickey
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@SuppressWarnings("rawtypes")
@RunWith(Parameterized.class)
public class LegacyRedisCacheTests {

	final static String CACHE_NAME = "testCache";
	ObjectFactory<Object> keyFactory;
	ObjectFactory<Object> valueFactory;
	RedisConnectionFactory connectionFactory;
	final boolean allowCacheNullValues;

	RedisCache cache;

	public LegacyRedisCacheTests(RedisTemplate template, ObjectFactory<Object> keyFactory,
			ObjectFactory<Object> valueFactory, boolean allowCacheNullValues) {

		this.connectionFactory = template.getConnectionFactory();
		this.keyFactory = keyFactory;
		this.valueFactory = valueFactory;
		this.allowCacheNullValues = allowCacheNullValues;
		ConnectionFactoryTracker.add(connectionFactory);

		cache = createCache();
	}

	@Parameters
	public static Collection<Object[]> testParams() {

		Collection<Object[]> params = AbstractOperationsTestParams.testParams();

		Collection<Object[]> target = new ArrayList<>();
		for (Object[] source : params) {

			Object[] cacheNullDisabled = Arrays.copyOf(source, source.length + 1);
			Object[] cacheNullEnabled = Arrays.copyOf(source, source.length + 1);

			cacheNullDisabled[source.length] = false;
			cacheNullEnabled[source.length] = true;

			target.add(cacheNullDisabled);
			target.add(cacheNullEnabled);
		}

		return target;
	}

	@AfterClass
	public static void cleanUp() {
		ConnectionFactoryTracker.cleanUp();
	}

	@SuppressWarnings("unchecked")
	private RedisCache createCache() {

		RedisCacheConfiguration cacheConfiguration = RedisCacheConfiguration.defaultCacheConfig()
				.entryTtl(Duration.ofSeconds(10));
		if (!allowCacheNullValues) {
			cacheConfiguration = cacheConfiguration.disableCachingNullValues();
		}

		return new RedisCache(CACHE_NAME, new DefaultRedisCacheWriter(connectionFactory), cacheConfiguration);
	}

	protected Object getValue() {
		return valueFactory.instance();
	}

	protected Object getKey() {
		return keyFactory.instance();
	}

	@Test
	public void testCachePut() throws Exception {
		Object key = getKey();
		Object value = getValue();

		assertThat(value).isNotNull();
		assertThat(cache.get(key)).isNull();
		cache.put(key, value);
		ValueWrapper valueWrapper = cache.get(key);
		if (valueWrapper != null) {
			assertThat(valueWrapper.get()).isEqualTo(value);
		}
	}

	@Test
	public void testCacheClear() throws Exception {
		Object key1 = getKey();
		Object value1 = getValue();

		Object key2 = getKey();
		Object value2 = getValue();

		assertThat(cache.get(key1)).isNull();
		cache.put(key1, value1);
		assertThat(cache.get(key2)).isNull();
		cache.put(key2, value2);
		cache.clear();
		assertThat(cache.get(key2)).isNull();
		assertThat(cache.get(key1)).isNull();
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

		Thread th = new Thread(() -> {
			cache.clear();
			cache.put(k1, v1);
			cache.put(k2, v2);
			failed.set(v1.equals(cache.get(k1)));

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
	public void testGetWhileClear() throws InterruptedException {

		final Object key1 = getKey();
		final Object value1 = getValue();
		int numTries = 10;
		final AtomicBoolean monitorStateException = new AtomicBoolean(false);
		final CountDownLatch latch = new CountDownLatch(numTries);
		Runnable clearCache = cache::clear;
		Runnable putCache = () -> {
			try {
				cache.put(key1, value1);
			} catch (IllegalMonitorStateException e) {
				monitorStateException.set(true);
			} finally {
				latch.countDown();
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

		Object key = getKey();
		Object value = getValue();
		cache.put(key, value);

		assertThat(value).isEqualTo(cache.get(key, Object.class));
	}

	@Test // DATAREDIS-243
	public void testCacheGetShouldRetunInstanceOfCorrectType() {

		Object key = getKey();
		Object value = getValue();
		cache.put(key, value);

		assertThat(cache.get(key, value.getClass())).isInstanceOf(value.getClass());
	}

	@Test(expected = IllegalStateException.class) // DATAREDIS-243
	public void testCacheGetShouldThrowExceptionOnInvalidType() {

		Object key = getKey();
		Object value = getValue();
		cache.put(key, value);

		@SuppressWarnings("unused")
		Cache retrievedObject = cache.get(key, Cache.class);
	}

	@Test // DATAREDIS-243
	public void testCacheGetShouldReturnNullIfNoCachedValueFound() {

		Object key = getKey();
		Object value = getValue();
		cache.put(key, value);

		Object invalidKey = "spring-data-redis".getBytes();
		assertThat(cache.get(invalidKey, value.getClass())).isNull();
	}

	@Test // DATAREDIS-344, DATAREDIS-416
	public void putIfAbsentShouldSetValueOnlyIfNotPresent() {

		Object key = getKey();

		Object value = getValue();

		assertThat(cache.putIfAbsent(key, value)).isNull();

		ValueWrapper wrapper = cache.putIfAbsent(key, value);

		if (!(value instanceof Number)) {
			assertThat(wrapper.get()).isNotSameAs(value);
		}

		assertThat(wrapper.get()).isEqualTo(value);
	}

	@Test(expected = IllegalArgumentException.class) // DATAREDIS-510, DATAREDIS-606
	public void cachePutWithNullShouldNotAddStuffToRedis() {

		assumeThat(allowCacheNullValues).as("Only suitable when cache does NOT allow null values.").isFalse();

		Object key = getKey();

		cache.put(key, null);
	}

	@Test // DATAREDIS-510, DATAREDIS-606
	public void cachePutWithNullShouldErrorAndLeaveExistingKeyUntouched() {

		assumeThat(allowCacheNullValues).as("Only suitable when cache does NOT allow null values.").isFalse();

		Object key = getKey();
		Object value = getValue();

		cache.put(key, value);

		assertThat(cache.get(key).get()).isEqualTo(value);

		try {
			cache.put(key, null);
		} catch (IllegalArgumentException e) {
			// forget this one.
		}

		assertThat(cache.get(key).get()).isEqualTo(value);
	}

	@Test // DATAREDIS-443, DATAREDIS-452
	public void testCacheGetSynchronized() throws Throwable {
		runOnce(new CacheGetWithValueLoaderIsThreadSafe(cache));
	}

	@Test // DATAREDIS-553
	public void cachePutWithNullShouldAddStuffToRedisWhenCachingNullIsEnabled() {

		assumeThat(allowCacheNullValues).as("Only suitable when cache does allow null values.").isTrue();

		Object key = getKey();
		Object value = getValue();

		cache.put(key, null);

		assertThat(cache.get(key, String.class)).isNull();
	}

	@Test // DATAREDIS-553
	public void testCacheGetSynchronizedNullAllowingNull() {

		assumeThat(allowCacheNullValues).as("Only suitable when cache does allow null values.").isTrue();

		Object key = getKey();
		Object value = cache.get(key, () -> null);

		assertThat(value).isNull();
		assertThat(cache.get(key).get()).isNull();
	}

	@Test(expected = IllegalArgumentException.class) // DATAREDIS-553, DATAREDIS-606
	public void testCacheGetSynchronizedNullNotAllowingNull() {

		assumeThat(allowCacheNullValues).as("Only suitable when cache does NOT allow null values.").isFalse();

		Object key = getKey();
		Object value = cache.get(key, () -> null);
	}

	@Test(expected = ValueRetrievalException.class)
	public void testCacheGetSynchronizedThrowsExceptionInValueLoader() {

		Object key = getKey();
		Object value = cache.get(key, () -> {
			throw new RuntimeException("doh!");
		});
	}

	@Test // DATAREDIS-553
	public void testCacheGetSynchronizedNullWithStoredNull() {

		assumeThat(allowCacheNullValues).as("Only suitable when cache does allow null values.").isTrue();

		Object key = getKey();
		cache.put(key, null);

		Object cachedValue = cache.get(key, () -> null);

		assertThat(cachedValue).isNull();
	}

	@SuppressWarnings("unused")
	private static class CacheGetWithValueLoaderIsThreadSafe extends MultithreadedTestCase {

		Cache redisCache;
		TestCacheLoader<String> cacheLoader;

		public CacheGetWithValueLoaderIsThreadSafe(Cache redisCache) {

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
			assertThat(redisCache.get("key", new TestCacheLoader<>("illegal value"))).isEqualTo("test");
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
