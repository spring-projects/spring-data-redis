/*
 * Copyright 2018-2025 the original author or authors.
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

import org.springframework.util.Assert;

/**
 * {@link CacheKeyPrefix} is a callback hook for creating custom prefixes prepended to the actual {@literal key} stored
 * in Redis.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author John Blum
 * @since 2.0.4
 */
@FunctionalInterface
public interface CacheKeyPrefix {

	/**
	 * Default separator.
	 *
	 * @since 2.3
	 */
	String SEPARATOR = "::";

	/**
	 * Compute the {@link String prefix} for the actual {@literal cache key} stored in Redis.
	 *
	 * @param cacheName {@link String name} of the cache in which the key is stored; will never be {@literal null}.
	 * @return the computed {@link String prefix} of the {@literal cache key} stored in Redis; never {@literal null}.
	 */
	String compute(String cacheName);

	/**
	 * Creates a default {@link CacheKeyPrefix} scheme that prefixes cache keys with the {@link String name} of the cache
	 * followed by double colons. For example, a cache named {@literal myCache} will prefix all cache keys with
	 * {@literal myCache::}.
	 *
	 * @return the default {@link CacheKeyPrefix} scheme.
	 */
	static CacheKeyPrefix simple() {
		return name -> name + SEPARATOR;
	}

	/**
	 * Creates a {@link CacheKeyPrefix} scheme that prefixes cache keys with the given {@link String prefix}. The
	 * {@link String prefix} is prepended to the {@link String cacheName} followed by double colons. For example, a prefix
	 * {@literal redis-} with a cache named {@literal  myCache} results in {@literal  redis-myCache::}.
	 *
	 * @param prefix must not be {@literal null}.
	 * @return the default {@link CacheKeyPrefix} scheme.
	 * @since 2.3
	 */
	static CacheKeyPrefix prefixed(String prefix) {

		Assert.notNull(prefix, "Prefix must not be null");

		return name -> prefix + name + SEPARATOR;
	}
}
