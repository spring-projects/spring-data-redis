/*
 * Copyright 2018-2020 the original author or authors.
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
 * {@link CacheKeyPrefix} provides a hook for creating custom prefixes prepended to the actual {@literal key} stored in
 * Redis.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
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
	 * Compute the prefix for the actual {@literal key} stored in Redis.
	 *
	 * @param cacheName will never be {@literal null}.
	 * @return never {@literal null}.
	 */
	String compute(String cacheName);

	/**
	 * Creates a default {@link CacheKeyPrefix} scheme that prefixes cache keys with {@code cacheName} followed by double
	 * colons. A cache named {@code myCache} will prefix all cache keys with {@code myCache::}.
	 *
	 * @return the default {@link CacheKeyPrefix} scheme.
	 */
	static CacheKeyPrefix simple() {
		return name -> name + SEPARATOR;
	}

	/**
	 * Creates a {@link CacheKeyPrefix} scheme that prefixes cache keys with the given {@code prefix} prepended to the
	 * {@code cacheName} followed by double colons. A cache named {@code myCache} with prefix {@code redis-} will result
	 * in {@code redis-myCache::}.
	 *
	 * @param prefix must not be {@literal null}.
	 * @return the default {@link CacheKeyPrefix} scheme.
	 * @since 2.3
	 */
	static CacheKeyPrefix prefixed(String prefix) {

		Assert.notNull(prefix, "Prefix must not be null!");
		return name -> prefix + name + SEPARATOR;
	}
}
