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

import java.time.Instant;
import java.util.concurrent.TimeUnit;

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

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.cache.CacheStatisticsCollector.incPuts(java.lang.String)
	 */
	@Override
	public void incPuts(String cacheName) {}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.cache.CacheStatisticsCollector.incGets(java.lang.String)
	 */
	@Override
	public void incGets(String cacheName) {}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.cache.CacheStatisticsCollector.incHits(java.lang.String)
	 */
	@Override
	public void incHits(String cacheName) {}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.cache.CacheStatisticsCollector.incMisses(java.lang.String)
	 */
	@Override
	public void incMisses(String cacheName) {}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.cache.CacheStatisticsCollector.incDeletesBy(java.lang.String)
	 */
	@Override
	public void incDeletesBy(String cacheName, int value) {}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.cache.CacheStatisticsCollector.incLockTime(java.lang.String)
	 */
	@Override
	public void incLockTime(String name, long durationNS) {}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.cache.CacheStatisticsCollector.reset(java.lang.String)
	 */
	@Override
	public void reset(String cacheName) {}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.cache.CacheStatisticsCollector.getCacheStatistics(java.lang.String)
	 */
	@Override
	public CacheStatistics getCacheStatistics(String cacheName) {
		return new EmptyStatistics(cacheName);
	}

	private static class EmptyStatistics implements CacheStatistics {

		private final String cacheName;

		EmptyStatistics(String cacheName) {
			this.cacheName = cacheName;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.cache.CacheStatistics#getCacheName()
		 */
		@Override
		public String getCacheName() {
			return cacheName;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.cache.CacheStatistics#getPuts()
		 */
		@Override
		public long getPuts() {
			return 0;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.cache.CacheStatistics#getGets()
		 */
		@Override
		public long getGets() {
			return 0;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.cache.CacheStatistics#getHits()
		 */
		@Override
		public long getHits() {
			return 0;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.cache.CacheStatistics#getMisses()
		 */
		@Override
		public long getMisses() {
			return 0;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.cache.CacheStatistics#getDeletes()
		 */
		@Override
		public long getDeletes() {
			return 0;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.cache.CacheStatistics#getLockWaitDuration(java.util.concurrent.TimeUnit)
		 */
		@Override
		public long getLockWaitDuration(TimeUnit unit) {
			return 0;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.cache.CacheStatistics#getSince()
		 */
		@Override
		public Instant getSince() {
			return Instant.EPOCH;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.cache.CacheStatistics#getLastReset()
		 */
		@Override
		public Instant getLastReset() {
			return getSince();
		}

		/*
		 * (non-Javadoc)
		 * @see java.lang.Object#equals(java.lang.Object)
		 */
		@Override
		public boolean equals(Object o) {

			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}

			EmptyStatistics that = (EmptyStatistics) o;
			return ObjectUtils.nullSafeEquals(cacheName, that.cacheName);
		}

		/*
		 * (non-Javadoc)
		 * @see java.lang.Object#hashCode()
		 */
		@Override
		public int hashCode() {
			return ObjectUtils.nullSafeHashCode(cacheName);
		}
	}
}
