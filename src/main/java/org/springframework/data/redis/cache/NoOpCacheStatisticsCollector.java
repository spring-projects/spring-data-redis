/*
 * Copyright 2020-2025 the original author or authors.
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

import java.time.Instant;
import java.util.concurrent.TimeUnit;

import org.jspecify.annotations.Nullable;
import org.springframework.util.ObjectUtils;

/**
 * {@link CacheStatisticsCollector} implementation that does not capture anything but throws an
 * {@link IllegalStateException} when {@link #getCacheStatistics(String) obtaining} {@link CacheStatistics} for a cache.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.4
 */
enum NoOpCacheStatisticsCollector implements CacheStatisticsCollector {

	INSTANCE;

	@Override
	public void incPuts(String cacheName) {}

	@Override
	public void incGets(String cacheName) {}

	@Override
	public void incHits(String cacheName) {}

	@Override
	public void incMisses(String cacheName) {}

	@Override
	public void incDeletesBy(String cacheName, int value) {}

	@Override
	public void incLockTime(String name, long durationNS) {}

	@Override
	public void reset(String cacheName) {}

	@Override
	public CacheStatistics getCacheStatistics(String cacheName) {
		return new EmptyStatistics(cacheName);
	}

	private static class EmptyStatistics implements CacheStatistics {

		private final String cacheName;

		EmptyStatistics(String cacheName) {
			this.cacheName = cacheName;
		}

		@Override
		public String getCacheName() {
			return cacheName;
		}

		@Override
		public long getPuts() {
			return 0;
		}

		@Override
		public long getGets() {
			return 0;
		}

		@Override
		public long getHits() {
			return 0;
		}

		@Override
		public long getMisses() {
			return 0;
		}

		@Override
		public long getDeletes() {
			return 0;
		}

		@Override
		public long getLockWaitDuration(TimeUnit unit) {
			return 0;
		}

		@Override
		public Instant getSince() {
			return Instant.EPOCH;
		}

		@Override
		public Instant getLastReset() {
			return getSince();
		}

		@Override
		public boolean equals(@Nullable Object o) {

			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}

			EmptyStatistics that = (EmptyStatistics) o;
			return ObjectUtils.nullSafeEquals(cacheName, that.cacheName);
		}

		@Override
		public int hashCode() {
			return ObjectUtils.nullSafeHashCode(cacheName);
		}
	}
}
