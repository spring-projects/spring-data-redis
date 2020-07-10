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
import java.util.concurrent.atomic.LongAdder;

import org.springframework.util.Assert;

/**
 * Default mutable {@link CacheStatistics} implementation.
 *
 * @author Mark Paluch
 * @since 2.4
 */
class MutableCacheStatistics implements CacheStatistics {

	private final String cacheName;

	private final Instant aliveSince = Instant.now();
	private Instant lastReset = aliveSince;

	private final LongAdder puts = new LongAdder();
	private final LongAdder gets = new LongAdder();
	private final LongAdder hits = new LongAdder();
	private final LongAdder misses = new LongAdder();
	private final LongAdder deletes = new LongAdder();
	private final LongAdder lockWaitTimeNs = new LongAdder();

	MutableCacheStatistics(String cacheName) {
		this.cacheName = cacheName;
	}

	/*
	 * (non-Javadoc)
	 * org.springframework.data.redis.cache.CacheStatistics#getCacheName()
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
		return puts.sum();
	}

	void incPuts() {
		puts.increment();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.cache.CacheStatistics#getRetrievals()
	 */
	@Override
	public long getGets() {
		return gets.sum();
	}

	void incGets() {
		gets.increment();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.cache.CacheStatistics#getHits()
	 */
	@Override
	public long getHits() {
		return hits.sum();
	}

	void incHits() {
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

	void incMisses() {
		misses.increment();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.cache.CacheStatistics#getRemovals()
	 */
	@Override
	public long getDeletes() {
		return deletes.sum();
	}

	void incDeletes() {
		incDeletes(1);
	}

	/**
	 * @param x number of removals to add.
	 */
	void incDeletes(int x) {
		deletes.add(x);
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

	@Override
	public Instant getSince() {
		return this.aliveSince;
	}

	@Override
	public Instant getLastReset() {
		return lastReset;
	}

	void incLockWaitTime(long waitTimeNs) {
		lockWaitTimeNs.add(waitTimeNs);
	}

	void reset() {

		lastReset = Instant.now();

		puts.reset();
		gets.reset();
		hits.reset();
		misses.reset();
		deletes.reset();
		lockWaitTimeNs.reset();
	}

	CacheStatistics captureSnapshot() {
		return new Snapshot(this);
	}

	/**
	 * {@link CacheStatistics} value object holding snapshot data.
	 */
	private static class Snapshot implements CacheStatistics {

		private final String cacheName;
		private final long puts;
		private final long gets;
		private final long hits;
		private final long misses;
		private final long deletes;
		private final long lockWaitTimeNS;
		private final long pending;
		private final Instant time;
		private final Instant since;
		private final Instant lastReset;

		Snapshot(CacheStatistics statistics) {

			cacheName = statistics.getCacheName();
			gets = statistics.getGets();
			hits = statistics.getHits();
			misses = statistics.getMisses();
			puts = statistics.getPuts();
			deletes = statistics.getDeletes();
			pending = gets - (hits + misses);

			lockWaitTimeNS = statistics.getLockWaitDuration(TimeUnit.NANOSECONDS);

			time = Instant.now();
			since = Instant.from(statistics.getSince());
			lastReset = Instant.from(statistics.getLastReset());
		}

		/*
		 * (non-Javadoc)
		 * org.springframework.data.redis.cache.CacheStatistics#getCacheName()
		 */
		@Override
		public String getCacheName() {
			return cacheName;
		}

		/*
		 * (non-Javadoc)
		 * org.springframework.data.redis.cache.CacheStatistics#getPuts()
		 */
		@Override
		public long getPuts() {
			return puts;
		}

		/*
		 * (non-Javadoc)
		 * org.springframework.data.redis.cache.CacheStatistics#getGets()
		 */
		@Override
		public long getGets() {
			return gets;
		}

		/*
		 * (non-Javadoc)
		 * org.springframework.data.redis.cache.CacheStatistics#getHits()
		 */
		@Override
		public long getHits() {
			return hits;
		}

		/*
		 * (non-Javadoc)
		 * org.springframework.data.redis.cache.CacheStatistics#getMisses()
		 */
		@Override
		public long getMisses() {
			return misses;
		}

		/*
		 * (non-Javadoc)
		 * org.springframework.data.redis.cache.CacheStatistics#getPending()
		 */
		@Override
		public long getPending() {
			return pending;
		}

		/*
		 * (non-Javadoc)
		 * org.springframework.data.redis.cache.CacheStatistics#getDeletes()
		 */
		@Override
		public long getDeletes() {
			return deletes;
		}

		/*
		 * (non-Javadoc)
		 * org.springframework.data.redis.cache.CacheStatistics#getCacheName(java.util.concurrent.TimeUnit)
		 */
		@Override
		public long getLockWaitDuration(TimeUnit unit) {
			return unit.convert(lockWaitTimeNS, TimeUnit.NANOSECONDS);
		}

		/*
		 * (non-Javadoc)
		 * org.springframework.data.redis.cache.CacheStatistics#getSince()
		 */
		@Override
		public Instant getSince() {
			return since;
		}

		/*
		 * (non-Javadoc)
		 * org.springframework.data.redis.cache.CacheStatistics#getLastReset()
		 */
		@Override
		public Instant getLastReset() {
			return lastReset;
		}

		/*
		 * (non-Javadoc)
		 * org.springframework.data.redis.cache.CacheStatistics#getTime()
		 */
		@Override
		public Instant getTime() {
			return time;
		}
	}
}
