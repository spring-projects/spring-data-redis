/*
 * Copyright 2020 the original author or authors.
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

/**
 * The statistics collector supports capturing of relevant {@link RedisCache} operations such as
 * {@literal hits & misses}.
 *
 * @author Christoph Strobl
 * @since 2.4
 */
public interface CacheStatisticsCollector extends CacheStatisticsProvider {

	/**
	 * Increase the counter for {@literal put operations} of the given cache.
	 * 
	 * @param cacheName must not be {@literal null}.
	 */
	void incPuts(String cacheName);

	/**
	 * Increase the counter for {@literal get operations} of the given cache.
	 * 
	 * @param cacheName must not be {@literal null}.
	 */
	void incGets(String cacheName);

	/**
	 * Increase the counter for {@literal get operations with result} of the given cache.
	 * 
	 * @param cacheName must not be {@literal null}.
	 */
	void incHits(String cacheName);

	/**
	 * Increase the counter for {@literal get operations without result} of the given cache.
	 * 
	 * @param cacheName must not be {@literal null}.
	 */
	void incMisses(String cacheName);

	/**
	 * Increase the counter for {@literal delete operations} of the given cache.
	 * 
	 * @param cacheName must not be {@literal null}.
	 */
	default void incDeletes(String cacheName) {
		incDeletesBy(cacheName, 1);
	}

	/**
	 * Increase the counter for {@literal delete operations} of the given cache by the given value.
	 * 
	 * @param cacheName must not be {@literal null}.
	 */
	void incDeletesBy(String cacheName, int value);

	/**
	 * Increase the gauge for {@literal sync lock duration} of the cache by the given nanoseconds.
	 * 
	 * @param cacheName must not be {@literal null}.
	 */
	void incLockTime(String cacheName, long durationNS);

	/**
	 * Reset the all counters and gauges of for the given cache.
	 * 
	 * @param cacheName must not be {@literal null}.
	 */
	void reset(String cacheName);

	/**
	 * @return a {@link CacheStatisticsCollector} that performs no action.
	 */
	static CacheStatisticsCollector none() {
		return NoOpCacheStatisticsCollector.INSTANCE;
	}

	/**
	 * @return a default {@link CacheStatisticsCollector} implementation.
	 */
	static CacheStatisticsCollector instance/*clearlyNeedsBetterNaming*/() {
		return new DefaultCacheStatisticsCollector();
	}
}
