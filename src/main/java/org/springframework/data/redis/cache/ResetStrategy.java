/*
 * Copyright 2026-present the original author or authors.
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
 * Interface defining a strategy for resetting the cache.
 *
 * @author Christoph Strobl
 * @since 4.1
 */
public interface ResetStrategy {


	/**
	 * Default reset strategy iterating through each registered {@link RedisCache} and calling {@link RedisCache#clear()}.
	 * Actual clearing may be performed in an asynchronous or deferred fashion, with subsequent lookups possibly still
	 * seeing the entries.
	 */
	static ResetStrategy clear() {
		return ResetStrategies.DefaultResetStrategies.CLEAR;
	}

	/**
	 * Reset strategy iterating through each registered {@link RedisCache} and calling {@link RedisCache#invalidate()}.
	 * This strategy expects all entries to be immediately invisible for subsequent lookups.
	 */
	static ResetStrategy invalidate() {
		return ResetStrategies.DefaultResetStrategies.INVALIDATE;
	}

	/**
	 * Flush the Redis database using the {@code FLUSHDB} command. This is the fastest way to reset all caches, but it
	 * will also wipe all keys in the database, which may not be desirable if the database holds keys that are not used by
	 * a cache or the database is shared with other applications keys. This strategy does not acquire or wait for any
	 * locks to be released.
	 */
	static ResetStrategy flushDb() {
		return ResetStrategies.FlushingResetStrategy.FLUSHING;
	}


}
