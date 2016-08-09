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

import static org.assertj.core.api.Assertions.*;

import org.junit.Before;
import org.junit.Test;
import org.springframework.cache.Cache;
import org.springframework.cache.Cache.ValueWrapper;

/**
 * Test for native cache implementations.
 * 
 * @author Costin Leau
 */
public abstract class AbstractNativeCacheTest<T> {

	private T nativeCache;
	protected Cache cache;
	protected final static String CACHE_NAME = "testCache";
	private final boolean allowCacheNullValues;

	protected AbstractNativeCacheTest(boolean allowCacheNullValues) {
		this.allowCacheNullValues = allowCacheNullValues;
	}

	@Before
	public void setUp() throws Exception {
		nativeCache = createNativeCache();
		cache = createCache(nativeCache, allowCacheNullValues);
		cache.clear();
	}

	protected abstract T createNativeCache() throws Exception;

	protected abstract Cache createCache(T nativeCache, boolean allowCacheNullValues);

	protected abstract Object getKey();

	protected abstract Object getValue();

	protected boolean getAllowCacheNullValues() {
		return allowCacheNullValues;
	}

	@Test
	public void testCacheName() throws Exception {
		assertThat(cache.getName()).isEqualTo(CACHE_NAME);
	}

	@Test
	public void testNativeCache() throws Exception {
		assertThat(cache.getNativeCache()).isSameAs(nativeCache);
	}

	@Test
	public void testCachePut() throws Exception {
		Object key = getKey();
		Object value = getValue();

		assertThat(value).isNotNull();
		assertThat(cache.get(key)).isNull();
		cache.put(key, value);
		ValueWrapper valueWrapper = cache.get(key);
		assertThat(valueWrapper.get()).isEqualTo(value);
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
}
