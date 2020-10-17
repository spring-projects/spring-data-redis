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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Default {@link CacheStatisticsCollector} implementation holding synchronized per cache
 * {@link MutableCacheStatistics}.
 *
 * @author Christoph Strobl
 * @since 2.4
 */
class DefaultCacheStatisticsCollector implements CacheStatisticsCollector {

	private final Map<String, MutableCacheStatistics> stats = new ConcurrentHashMap<>();

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.cache.CacheStatisticsCollector.incPuts(java.lang.String)
	 */
	@Override
	public void incPuts(String cacheName) {
		statsFor(cacheName).incPuts();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.cache.CacheStatisticsCollector.incGets(java.lang.String)
	 */
	@Override
	public void incGets(String cacheName) {
		statsFor(cacheName).incGets();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.cache.CacheStatisticsCollector.incHits(java.lang.String)
	 */
	@Override
	public void incHits(String cacheName) {
		statsFor(cacheName).incHits();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.cache.CacheStatisticsCollector.incMisses(java.lang.String)
	 */
	@Override
	public void incMisses(String cacheName) {
		statsFor(cacheName).incMisses();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.cache.CacheStatisticsCollector.incDeletesBy(java.lang.String, int)
	 */
	@Override
	public void incDeletesBy(String cacheName, int value) {
		statsFor(cacheName).incDeletes(value);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.cache.CacheStatisticsCollector.incLockTime(java.lang.String)
	 */
	@Override
	public void incLockTime(String name, long durationNS) {
		statsFor(name).incLockWaitTime(durationNS);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.cache.CacheStatisticsCollector.reset(java.lang.String)
	 */
	@Override
	public void reset(String cacheName) {
		statsFor(cacheName).reset();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.cache.CacheStatisticsCollector.getCacheStatistics(java.lang.String)
	 */
	@Override
	public CacheStatistics getCacheStatistics(String cacheName) {
		return statsFor(cacheName).captureSnapshot();
	}

	private MutableCacheStatistics statsFor(String cacheName) {
		return stats.computeIfAbsent(cacheName, MutableCacheStatistics::new);
	}
}
