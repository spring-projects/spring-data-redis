/*
 * Copyright 2017-2023 the original author or authors.
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

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * {@link RedisCacheWriter} provides low-level access to Redis commands ({@code SET, SETNX, GET, EXPIRE,...})
 * used for caching.
 * <p>
 * The {@link RedisCacheWriter} may be shared by multiple cache implementations and is responsible for reading/writing
 * binary data from/to Redis. The implementation honors potential cache lock flags that might be set.
 * <p>
 * The default {@link RedisCacheWriter} implementation can be customized with {@link BatchStrategy}
 * to tune performance behavior.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author John Blum
 * @since 2.0
 */
public interface RedisCacheWriter extends CacheStatisticsProvider {

	/**
	 * Create new {@link RedisCacheWriter} without locking behavior.
	 *
	 * @param connectionFactory must not be {@literal null}.
	 * @return new instance of {@link DefaultRedisCacheWriter}.
	 */
	static RedisCacheWriter nonLockingRedisCacheWriter(RedisConnectionFactory connectionFactory) {
		return nonLockingRedisCacheWriter(connectionFactory, BatchStrategies.keys());
	}

	/**
	 * Create new {@link RedisCacheWriter} without locking behavior.
	 *
	 * @param connectionFactory must not be {@literal null}.
	 * @param batchStrategy must not be {@literal null}.
	 * @return new instance of {@link DefaultRedisCacheWriter}.
	 * @since 2.6
	 */
	static RedisCacheWriter nonLockingRedisCacheWriter(RedisConnectionFactory connectionFactory,
			BatchStrategy batchStrategy) {

		Assert.notNull(connectionFactory, "ConnectionFactory must not be null");
		Assert.notNull(batchStrategy, "BatchStrategy must not be null");

		return new DefaultRedisCacheWriter(connectionFactory, batchStrategy);
	}

	/**
	 * Create new {@link RedisCacheWriter} with locking behavior.
	 *
	 * @param connectionFactory must not be {@literal null}.
	 * @return new instance of {@link DefaultRedisCacheWriter}.
	 */
	static RedisCacheWriter lockingRedisCacheWriter(RedisConnectionFactory connectionFactory) {
		return lockingRedisCacheWriter(connectionFactory, BatchStrategies.keys());
	}

	/**
	 * Create new {@link RedisCacheWriter} with locking behavior.
	 *
	 * @param connectionFactory must not be {@literal null}.
	 * @param batchStrategy must not be {@literal null}.
	 * @return new instance of {@link DefaultRedisCacheWriter}.
	 * @since 2.6
	 */
	static RedisCacheWriter lockingRedisCacheWriter(RedisConnectionFactory connectionFactory,
			BatchStrategy batchStrategy) {

		return lockingRedisCacheWriter(connectionFactory, Duration.ofMillis(50), TtlFunction.persistent(), batchStrategy);
	}

	/**
	 * Create new {@link RedisCacheWriter} with locking behavior.
	 *
	 * @param connectionFactory must not be {@literal null}.
	 * @param sleepTime sleep time between lock access attempts, must not be {@literal null}.
	 * @param lockTtlFunction TTL function to compute the Lock TTL. The function is called with contextual keys
	 * and values (such as the cache name on cleanup or the actual key/value on put requests);
	 * must not be {@literal null}.
	 * @param batchStrategy must not be {@literal null}.
	 * @return new instance of {@link DefaultRedisCacheWriter}.
	 * @since 3.2
	 */
	static RedisCacheWriter lockingRedisCacheWriter(RedisConnectionFactory connectionFactory, Duration sleepTime,
			TtlFunction lockTtlFunction, BatchStrategy batchStrategy) {

		Assert.notNull(connectionFactory, "ConnectionFactory must not be null");

		return new DefaultRedisCacheWriter(connectionFactory, sleepTime, lockTtlFunction, CacheStatisticsCollector.none(),
				batchStrategy);
	}

	/**
	 * Get the binary value representation from Redis stored for the given key.
	 *
	 * @param name must not be {@literal null}.
	 * @param key must not be {@literal null}.
	 * @return {@literal null} if key does not exist.
	 * @see #get(String, byte[], Duration)
	 */
	@Nullable
	byte[] get(String name, byte[] key);

	/**
	 * Get the binary value representation from Redis stored for the given key and set
	 * the given {@link Duration TTL expiration} for the cache entry.
	 *
	 * @param name must not be {@literal null}.
	 * @param key must not be {@literal null}.
	 * @param ttl {@link Duration} specifying the {@literal expiration timeout} for the cache entry.
	 * @return {@literal null} if key does not exist or has {@literal expired}.
	 */
	@Nullable
	default byte[] get(String name, byte[] key, @Nullable Duration ttl) {
		return get(name, key);
	}

	/**
	 * Determines whether the asynchronous {@link #retrieve(String, byte[])}
	 * and {@link #retrieve(String, byte[], Duration)} cache operations are supported by the implementation.
	 * <p>
	 * The main factor for whether the {@literal retrieve} operation can be supported will primarily be determined
	 * by the Redis driver in use at runtime.
	 * <p>
	 * Returns {@literal false} by default. This will have an effect of {@link RedisCache#retrieve(Object)}
	 * and {@link RedisCache#retrieve(Object, Supplier)} throwing an {@link UnsupportedOperationException}.
	 *
	 * @return {@literal true} if asynchronous {@literal retrieve} operations are supported by the implementation.
	 * @since 3.2
	 */
	default boolean supportsAsyncRetrieve() {
		return false;
	}

	/**
	 * Asynchronously retrieves the {@link CompletableFuture value} to which the {@link RedisCache}
	 * maps the given {@link byte[] key}.
	 * <p>
	 * This operation is non-blocking.
	 *
	 * @param name {@link String} with the name of the {@link RedisCache}.
	 * @param key {@link byte[] key} mapped to the {@link CompletableFuture value} in the {@link RedisCache}.
	 * @return the {@link CompletableFuture value} to which the {@link RedisCache} maps the given {@link byte[] key}.
	 * @see #retrieve(String, byte[], Duration)
	 * @since 3.2
	 */
	default CompletableFuture<byte[]> retrieve(String name, byte[] key) {
		return retrieve(name, key, null);
	}

	/**
	 * Asynchronously retrieves the {@link CompletableFuture value} to which the {@link RedisCache} maps
	 * the given {@link byte[] key} setting the {@link Duration TTL expiration} for the cache entry.
	 * <p>
	 * This operation is non-blocking.
	 *
	 * @param name {@link String} with the name of the {@link RedisCache}.
	 * @param key {@link byte[] key} mapped to the {@link CompletableFuture value} in the {@link RedisCache}.
	 * @param ttl {@link Duration} specifying the {@literal expiration timeout} for the cache entry.
	 * @return the {@link CompletableFuture value} to which the {@link RedisCache} maps the given {@link byte[] key}.
	 * @since 3.2
	 */
	CompletableFuture<byte[]> retrieve(String name, byte[] key, @Nullable Duration ttl);

	/**
	 * Write the given key/value pair to Redis and set the expiration time if defined.
	 *
	 * @param name The cache name must not be {@literal null}.
	 * @param key The key for the cache entry. Must not be {@literal null}.
	 * @param value The value stored for the key. Must not be {@literal null}.
	 * @param ttl Optional expiration time. Can be {@literal null}.
	 */
	void put(String name, byte[] key, byte[] value, @Nullable Duration ttl);

	/**
	 * Store the given key/value pair asynchronously to Redis and set the expiration time if defined.
	 * <p>
	 * This operation is non-blocking.
	 *
	 * @param name The cache name must not be {@literal null}.
	 * @param key The key for the cache entry. Must not be {@literal null}.
	 * @param value The value stored for the key. Must not be {@literal null}.
	 * @param ttl Optional expiration time. Can be {@literal null}.
	 * @since 3.2
	 */
	CompletableFuture<Void> store(String name, byte[] key, byte[] value, @Nullable Duration ttl);

	/**
	 * Write the given value to Redis if the key does not already exist.
	 *
	 * @param name The cache name must not be {@literal null}.
	 * @param key The key for the cache entry. Must not be {@literal null}.
	 * @param value The value stored for the key. Must not be {@literal null}.
	 * @param ttl Optional expiration time. Can be {@literal null}.
	 * @return {@literal null} if the value has been written, the value stored for the key if it already exists.
	 */
	@Nullable
	byte[] putIfAbsent(String name, byte[] key, byte[] value, @Nullable Duration ttl);

	/**
	 * Remove the given key from Redis.
	 *
	 * @param name The cache name must not be {@literal null}.
	 * @param key The key for the cache entry. Must not be {@literal null}.
	 */
	void remove(String name, byte[] key);

	/**
	 * Remove all keys following the given pattern.
	 *
	 * @param name The cache name must not be {@literal null}.
	 * @param pattern The pattern for the keys to remove. Must not be {@literal null}.
	 */
	void clean(String name, byte[] pattern);

	/**
	 * Reset all statistics counters and gauges for this cache.
	 *
	 * @since 2.4
	 */
	void clearStatistics(String name);

	/**
	 * Obtain a {@link RedisCacheWriter} using the given {@link CacheStatisticsCollector} to collect metrics.
	 *
	 * @param cacheStatisticsCollector must not be {@literal null}.
	 * @return new instance of {@link RedisCacheWriter}.
	 */
	RedisCacheWriter withStatisticsCollector(CacheStatisticsCollector cacheStatisticsCollector);

	/**
	 * Function to compute the time to live from the cache {@code key} and {@code value}.
	 *
	 * @author Mark Paluch
	 * @since 3.2
	 */
	@FunctionalInterface
	interface TtlFunction {

		Duration NO_EXPIRATION = Duration.ZERO;

		/**
		 * Creates a {@literal Singleton} {@link TtlFunction} using the given {@link Duration}.
		 *
		 * @param duration the time to live. Can be {@link Duration#ZERO} for persistent values (i.e. cache entry
		 * does not expire).
		 * @return a singleton {@link TtlFunction} using {@link Duration}.
		 */
		static TtlFunction just(Duration duration) {

			Assert.notNull(duration, "TTL Duration must not be null");

			return new FixedDurationTtlFunction(duration);
		}

		/**
		 * Returns a {@link TtlFunction} to create persistent entires that do not expire.
		 *
		 * @return a {@link TtlFunction} to create persistent entires that do not expire.
		 */
		static TtlFunction persistent() {
			return just(NO_EXPIRATION);
		}

		/**
		 * Compute a {@link Duration time-to-live (TTL)} using the cache {@code key} and {@code value}.
		 * <p>
		 * The {@link Duration time-to-live (TTL)} is computed on each write operation. Redis uses millisecond granularity
		 * for timeouts. Any more granular values (e.g. micros or nanos) are not considered and will be truncated due to
		 * rounding. Returning {@link Duration#ZERO}, or a value less than {@code Duration.ofMillis(1)}, results in a
		 * persistent value that does not expire.
		 *
		 * @param key the cache key.
		 * @param value the cache value. Can be {@code null} if the cache supports {@code null} value caching.
		 * @return the computed {@link Duration time-to-live (TTL)}. Can be {@link Duration#ZERO} for persistent values
		 *         (i.e. cache entry does not expire).
		 */
		Duration getTimeToLive(Object key, @Nullable Object value);

	}
}
