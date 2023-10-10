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

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

import org.springframework.dao.PessimisticLockingFailureException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStringCommands.SetOption;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * {@link RedisCacheWriter} implementation capable of reading/writing binary data from/to Redis in {@literal standalone}
 * and {@literal cluster} environments, and uses a given {@link RedisConnectionFactory} to obtain the actual
 * {@link RedisConnection}.
 * <p>
 * {@link DefaultRedisCacheWriter} can be used in
 * {@link RedisCacheWriter#lockingRedisCacheWriter(RedisConnectionFactory) locking}
 * or {@link RedisCacheWriter#nonLockingRedisCacheWriter(RedisConnectionFactory) non-locking} mode. While
 * {@literal non-locking} aims for maximum performance it may result in overlapping, non-atomic, command execution for
 * operations spanning multiple Redis interactions like {@code putIfAbsent}. The {@literal locking} counterpart prevents
 * command overlap by setting an explicit lock key and checking against presence of this key which leads to additional
 * requests and potential command wait times.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author André Prata
 * @author John Blum
 * @since 2.0
 */
class DefaultRedisCacheWriter implements RedisCacheWriter {

	private final BatchStrategy batchStrategy;

	private final CacheStatisticsCollector statistics;

	private final Duration sleepTime;

	private final RedisConnectionFactory connectionFactory;

	private final TtlFunction lockTtl;

	/**
	 * @param connectionFactory must not be {@literal null}.
	 * @param batchStrategy must not be {@literal null}.
	 */
	DefaultRedisCacheWriter(RedisConnectionFactory connectionFactory, BatchStrategy batchStrategy) {
		this(connectionFactory, Duration.ZERO, batchStrategy);
	}

	/**
	 * @param connectionFactory must not be {@literal null}.
	 * @param sleepTime sleep time between lock request attempts. Must not be {@literal null}. Use {@link Duration#ZERO}
	 *          to disable locking.
	 * @param batchStrategy must not be {@literal null}.
	 */
	DefaultRedisCacheWriter(RedisConnectionFactory connectionFactory, Duration sleepTime, BatchStrategy batchStrategy) {
		this(connectionFactory, sleepTime, TtlFunction.persistent(), CacheStatisticsCollector.none(), batchStrategy);
	}

	/**
	 * @param connectionFactory must not be {@literal null}.
	 * @param sleepTime sleep time between lock request attempts. Must not be {@literal null}. Use {@link Duration#ZERO}
	 *          to disable locking.
	 * @param lockTtl Lock TTL function must not be {@literal null}.
	 * @param cacheStatisticsCollector must not be {@literal null}.
	 * @param batchStrategy must not be {@literal null}.
	 */
	DefaultRedisCacheWriter(RedisConnectionFactory connectionFactory, Duration sleepTime, TtlFunction lockTtl,
			CacheStatisticsCollector cacheStatisticsCollector, BatchStrategy batchStrategy) {

		Assert.notNull(connectionFactory, "ConnectionFactory must not be null");
		Assert.notNull(sleepTime, "SleepTime must not be null");
		Assert.notNull(lockTtl, "Lock TTL Function must not be null");
		Assert.notNull(cacheStatisticsCollector, "CacheStatisticsCollector must not be null");
		Assert.notNull(batchStrategy, "BatchStrategy must not be null");

		this.connectionFactory = connectionFactory;
		this.sleepTime = sleepTime;
		this.lockTtl = lockTtl;
		this.statistics = cacheStatisticsCollector;
		this.batchStrategy = batchStrategy;
	}

	@Override
	public byte[] get(String name, byte[] key) {
		return get(name, key, null);
	}

	@Override
	public byte[] get(String name, byte[] key, @Nullable Duration ttl) {

		Assert.notNull(name, "Name must not be null");
		Assert.notNull(key, "Key must not be null");

		byte[] result = shouldExpireWithin(ttl)
			? execute(name, connection -> connection.stringCommands().getEx(key, Expiration.from(ttl)))
			: execute(name, connection -> connection.stringCommands().get(key));

		statistics.incGets(name);

		if (result != null) {
			statistics.incHits(name);
		} else {
			statistics.incMisses(name);
		}

		return result;
	}

	@Override
	public void put(String name, byte[] key, byte[] value, @Nullable Duration ttl) {

		Assert.notNull(name, "Name must not be null");
		Assert.notNull(key, "Key must not be null");
		Assert.notNull(value, "Value must not be null");

		execute(name, connection -> {

			if (shouldExpireWithin(ttl)) {
				connection.stringCommands()
						.set(key, value, Expiration.from(ttl.toMillis(), TimeUnit.MILLISECONDS), SetOption.upsert());
			} else {
				connection.stringCommands().set(key, value);
			}

			return "OK";
		});

		statistics.incPuts(name);
	}

	@Override
	public byte[] putIfAbsent(String name, byte[] key, byte[] value, @Nullable Duration ttl) {

		Assert.notNull(name, "Name must not be null");
		Assert.notNull(key, "Key must not be null");
		Assert.notNull(value, "Value must not be null");

		return execute(name, connection -> {

			if (isLockingCacheWriter()) {
				doLock(name, key, value, connection);
			}

			try {

				boolean put;

				if (shouldExpireWithin(ttl)) {
					put = isTrue(connection.stringCommands().set(key, value, Expiration.from(ttl), SetOption.ifAbsent()));
				} else {
					put = isTrue(connection.stringCommands().setNX(key, value));
				}

				if (put) {
					statistics.incPuts(name);
					return null;
				}

				return connection.stringCommands().get(key);

			} finally {
				if (isLockingCacheWriter()) {
					doUnlock(name, connection);
				}
			}
		});
	}

	@Override
	public void remove(String name, byte[] key) {

		Assert.notNull(name, "Name must not be null");
		Assert.notNull(key, "Key must not be null");

		execute(name, connection -> connection.keyCommands().del(key));
		statistics.incDeletes(name);
	}

	@Override
	public void clean(String name, byte[] pattern) {

		Assert.notNull(name, "Name must not be null");
		Assert.notNull(pattern, "Pattern must not be null");

		execute(name, connection -> {

			boolean wasLocked = false;

			try {
				if (isLockingCacheWriter()) {
					doLock(name, name, pattern, connection);
					wasLocked = true;
				}

				long deleteCount = batchStrategy.cleanCache(connection, name, pattern);

				while (deleteCount > Integer.MAX_VALUE) {
					statistics.incDeletesBy(name, Integer.MAX_VALUE);
					deleteCount -= Integer.MAX_VALUE;
				}

				statistics.incDeletesBy(name, (int) deleteCount);

			} finally {
				if (wasLocked && isLockingCacheWriter()) {
					doUnlock(name, connection);
				}
			}

			return "OK";
		});
	}

	@Override
	public CacheStatistics getCacheStatistics(String cacheName) {
		return statistics.getCacheStatistics(cacheName);
	}

	@Override
	public void clearStatistics(String name) {
		statistics.reset(name);
	}

	@Override
	public RedisCacheWriter withStatisticsCollector(CacheStatisticsCollector cacheStatisticsCollector) {
		return new DefaultRedisCacheWriter(connectionFactory, sleepTime, lockTtl, cacheStatisticsCollector,
				this.batchStrategy);
	}

	/**
	 * Explicitly set a write lock on a cache.
	 *
	 * @param name the name of the cache to lock.
	 */
	void lock(String name) {
		execute(name, connection -> doLock(name, name, null, connection));
	}

	@Nullable
	private Boolean doLock(String name, Object contextualKey, @Nullable Object contextualValue,
			RedisConnection connection) {

		Expiration expiration = Expiration.from(this.lockTtl.getTimeToLive(contextualKey, contextualValue));

		return connection.stringCommands()
				.set(createCacheLockKey(name), new byte[0], expiration, SetOption.SET_IF_ABSENT);
	}

	/**
	 * Explicitly remove a write lock from a cache.
	 *
	 * @param name the name of the cache to unlock.
	 */
	void unlock(String name) {
		executeLockFree(connection -> doUnlock(name, connection));
	}

	@Nullable
	private Long doUnlock(String name, RedisConnection connection) {
		return connection.keyCommands().del(createCacheLockKey(name));
	}

	boolean doCheckLock(String name, RedisConnection connection) {
		return isTrue(connection.keyCommands().exists(createCacheLockKey(name)));
	}

	/**
	 * @return {@literal true} if {@link RedisCacheWriter} uses locks.
	 */
	private boolean isLockingCacheWriter() {
		return !sleepTime.isZero() && !sleepTime.isNegative();
	}

	private <T> T execute(String name, Function<RedisConnection, T> callback) {

		try (RedisConnection connection = connectionFactory.getConnection()) {
			checkAndPotentiallyWaitUntilUnlocked(name, connection);
			return callback.apply(connection);
		}
	}

	private void executeLockFree(Consumer<RedisConnection> callback) {

		try (RedisConnection connection = connectionFactory.getConnection()) {
			callback.accept(connection);
		}
	}

	private void checkAndPotentiallyWaitUntilUnlocked(String name, RedisConnection connection) {

		if (!isLockingCacheWriter()) {
			return;
		}

		long lockWaitTimeNs = System.nanoTime();

		try {
			while (doCheckLock(name, connection)) {
				Thread.sleep(sleepTime.toMillis());
			}
		} catch (InterruptedException cause) {

			// Re-interrupt current thread, to allow other participants to react.
			Thread.currentThread().interrupt();

			String message = String.format("Interrupted while waiting to unlock cache %s", name);

			throw new PessimisticLockingFailureException(message, cause);
		} finally {
			statistics.incLockTime(name, System.nanoTime() - lockWaitTimeNs);
		}
	}

	private static byte[] createCacheLockKey(String name) {
		return (name + "~lock").getBytes(StandardCharsets.UTF_8);
	}

	private boolean isTrue(@Nullable Boolean value) {
		return Boolean.TRUE.equals(value);
	}

	private static boolean shouldExpireWithin(@Nullable Duration ttl) {
		return ttl != null && !ttl.isZero() && !ttl.isNegative();
	}
}
