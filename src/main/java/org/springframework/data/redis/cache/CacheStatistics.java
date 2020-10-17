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

/**
 * Cache statistics for a {@link RedisCache}. <br />
 * <strong>NOTE:</strong> {@link CacheStatistics} only serve local (in memory) data and do not collect any server
 * statistics.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.4
 */
public interface CacheStatistics {

	/**
	 * @return the name of the {@link RedisCache}.
	 */
	String getCacheName();

	/**
	 * @return number of put operations on the cache.
	 */
	long getPuts();

	/**
	 * @return the total number of get operations including both {@link #getHits() hits} and {@link #getMisses() misses}.
	 */
	long getGets();

	/**
	 * @return the number of cache get hits.
	 */
	long getHits();

	/**
	 * @return number of cache get misses.
	 */
	long getMisses();

	/**
	 * @return the number of {@link #getGets() gets} that have not yet been answered (neither {@link #getHits() hit} nor
	 *         {@link #getMisses() miss}).
	 */
	default long getPending() {
		return getGets() - (getHits() + getMisses());
	}

	/**
	 * @return number of cache removals.
	 */
	long getDeletes();

	/**
	 * @param unit the time unit to report the lock wait duration.
	 * @return lock duration using the given {@link TimeUnit} if the cache is configured to use locking.
	 */
	long getLockWaitDuration(TimeUnit unit);

	/**
	 * @return initial point in time when started statistics capturing.
	 */
	Instant getSince();

	/**
	 * @return instantaneous point in time of last statistics counter reset. Equals {@link #getSince()} if never reset.
	 */
	Instant getLastReset();

	/**
	 * @return the statistics time.
	 */
	default Instant getTime() {
		return Instant.now();
	}
}
