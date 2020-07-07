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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import org.springframework.util.Assert;

/**
 * Default mutable {@link CacheStatistics} implementation.
 *
 * @author Mark Paluch
 * @since 2.4
 */
class DefaultCacheStatistics implements CacheStatistics {

	private final LongAdder stores = new LongAdder();
	private final LongAdder retrievals = new LongAdder();
	private final LongAdder hits = new LongAdder();
	private final LongAdder misses = new LongAdder();
	private final LongAdder removals = new LongAdder();
	private final LongAdder lockWaitTimeNs = new LongAdder();

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.cache.CacheStatistics#getStores()
	 */
	@Override
	public long getStores() {
		return stores.sum();
	}

	void incrementStores() {
		stores.increment();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.cache.CacheStatistics#getRetrievals()
	 */
	@Override
	public long getRetrievals() {
		return retrievals.sum();
	}

	void incrementRetrievals() {
		retrievals.increment();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.cache.CacheStatistics#getHits()
	 */
	@Override
	public long getHits() {
		return hits.sum();
	}

	void incrementHits() {
		hits.increment();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.cache.CacheStatistics#getMisses()
	 */
	@Override
	public long getMisses() {
		return misses.sum();
	}

	void incrementMisses() {
		misses.increment();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.cache.CacheStatistics#getRemovals()
	 */
	@Override
	public long getRemovals() {
		return removals.sum();
	}

	void incrementRemovals() {
		incrementRemovals(1);
	}

	/**
	 * @param x number of removals to add.
	 */
	void incrementRemovals(int x) {
		removals.add(x);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.cache.CacheStatistics#getLockWaitDuration(java.util.concurrent.TimeUnit)
	 */
	@Override
	public long getLockWaitDuration(TimeUnit unit) {

		Assert.notNull(unit, "TimeUnit must not be null");

		return unit.convert(lockWaitTimeNs.sum(), TimeUnit.NANOSECONDS);
	}

	void incrementLockWaitTime(long waitTimeNs) {
		lockWaitTimeNs.add(waitTimeNs);
	}
}
