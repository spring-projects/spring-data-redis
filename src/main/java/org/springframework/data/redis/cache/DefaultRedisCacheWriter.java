/*
 * Copyright 2017-2025 the original author or authors.
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

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.jspecify.annotations.Nullable;

import org.springframework.dao.PessimisticLockingFailureException;
import org.springframework.data.redis.connection.ReactiveKeyCommands;
import org.springframework.data.redis.connection.ReactiveRedisConnection;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.connection.ReactiveStringCommands;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStringCommands;
import org.springframework.data.redis.connection.RedisStringCommands.SetOption;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.data.redis.util.ByteUtils;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.ObjectUtils;

/**
 * {@link RedisCacheWriter} implementation capable of reading/writing binary data from/to Redis in {@literal standalone}
 * and {@literal cluster} environments, and uses a given {@link RedisConnectionFactory} to obtain the actual
 * {@link RedisConnection}.
 * <p>
 * {@link DefaultRedisCacheWriter} can be used in
 * {@link RedisCacheWriter#lockingRedisCacheWriter(RedisConnectionFactory) locking} or
 * {@link RedisCacheWriter#nonLockingRedisCacheWriter(RedisConnectionFactory) non-locking} mode. While
 * {@literal non-locking} aims for maximum performance it may result in overlapping, non-atomic, command execution for
 * operations spanning multiple Redis interactions like {@code putIfAbsent}. The {@literal locking} counterpart prevents
 * command overlap by setting an explicit lock key and checking against presence of this key which leads to additional
 * requests and potential command wait times.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author André Prata
 * @author John Blum
 * @author ChanYoung Joung
 * @since 2.0
 */
class DefaultRedisCacheWriter implements RedisCacheWriter {

	private static final boolean REACTIVE_REDIS_CONNECTION_FACTORY_PRESENT = ClassUtils
			.isPresent("org.springframework.data.redis.connection.ReactiveRedisConnectionFactory", null);

	private final BatchStrategy batchStrategy;

	private final CacheStatisticsCollector statistics;

	private final Duration sleepTime;

	private final RedisConnectionFactory connectionFactory;

	private final TtlFunction lockTtl;

	private final AsyncCacheWriter asyncCacheWriter;

	private final boolean asynchronousWrites;

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
		this(connectionFactory, sleepTime, TtlFunction.persistent(), CacheStatisticsCollector.none(), batchStrategy, true);
	}

	DefaultRedisCacheWriter(RedisConnectionFactory connectionFactory, Duration sleepTime, TtlFunction lockTtl,
			CacheStatisticsCollector cacheStatisticsCollector, BatchStrategy batchStrategy, boolean asynchronousWrites) {

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

		if (REACTIVE_REDIS_CONNECTION_FACTORY_PRESENT && this.connectionFactory instanceof ReactiveRedisConnectionFactory) {
			this.asyncCacheWriter = new AsynchronousCacheWriterDelegate();
			this.asynchronousWrites = asynchronousWrites;
		} else {
			asyncCacheWriter = UnsupportedAsyncCacheWriter.INSTANCE;
			this.asynchronousWrites = false;
		}
	}

	/**
	 * Create a new {@code DefaultRedisCacheWriter} applying configuration through {@code configurerConsumer}.
	 *
	 * @param connectionFactory the connection factory to use.
	 * @param configurerConsumer configuration consumer.
	 * @return a new {@code DefaultRedisCacheWriter}.
	 * @since 4.0
	 */
	public static DefaultRedisCacheWriter create(RedisConnectionFactory connectionFactory,
			Consumer<RedisCacheWriterConfigurer> configurerConsumer) {

		Assert.notNull(connectionFactory, "RedisConnectionFactory must not be null");
		Assert.notNull(configurerConsumer, "RedisCacheWriterConfigurer function must not be null");

		DefaultRedisCacheWriterConfigurer config = new DefaultRedisCacheWriterConfigurer();
		configurerConsumer.accept(config);

		return new DefaultRedisCacheWriter(connectionFactory, config.lockSleepTime, config.lockTtlFunction,
				config.cacheStatisticsCollector, config.batchStrategy, !config.immediateWrites);
	}

	static class DefaultRedisCacheWriterConfigurer
			implements RedisCacheWriterConfigurer, CacheLockingConfigurer, CacheLockingConfiguration {

		CacheStatisticsCollector cacheStatisticsCollector = CacheStatisticsCollector.none();
		BatchStrategy batchStrategy = BatchStrategies.keys();
		Duration lockSleepTime = Duration.ZERO;
		TtlFunction lockTtlFunction = TtlFunction.persistent();
		boolean immediateWrites = false;

		@Override
		public RedisCacheWriterConfigurer collectStatistics(CacheStatisticsCollector cacheStatisticsCollector) {

			Assert.notNull(cacheStatisticsCollector, "CacheStatisticsCollector must not be null");
			this.cacheStatisticsCollector = cacheStatisticsCollector;

			return this;
		}

		@Override
		public RedisCacheWriterConfigurer batchStrategy(BatchStrategy batchStrategy) {

			Assert.notNull(batchStrategy, "BatchStrategy must not be null");
			this.batchStrategy = batchStrategy;

			return this;
		}

		@Override
		public RedisCacheWriterConfigurer cacheLocking(Consumer<CacheLockingConfigurer> configurerConsumer) {

			Assert.notNull(configurerConsumer, "CacheLockingConfigurer function must not be null");
			configurerConsumer.accept(this);

			return this;
		}

		@Override
		public RedisCacheWriterConfigurer immediateWrites(boolean enableImmediateWrites) {

			this.immediateWrites = enableImmediateWrites;
			return this;
		}

		@Override
		public void disable() {
			this.lockSleepTime = Duration.ZERO;
		}

		@Override
		public void enable(Consumer<CacheLockingConfiguration> configurationConsumer) {

			Assert.notNull(configurationConsumer, "CacheLockingConfigurer function must not be null");

			if (this.lockSleepTime.isZero() || this.lockSleepTime.isNegative()) {
				this.lockSleepTime = Duration.ofMillis(50);
			}
			configurationConsumer.accept(this);
		}

		@Override
		public CacheLockingConfiguration sleepTime(Duration sleepTime) {

			Assert.notNull(sleepTime, "Lock sleep time must not be null");
			Assert.isTrue(!sleepTime.isZero() && !sleepTime.isNegative(),
					"Lock sleep time must not be null zero or negative");

			this.lockSleepTime = sleepTime;

			return this;
		}

		@Override
		public CacheLockingConfiguration lockTimeout(TtlFunction ttlFunction) {

			Assert.notNull(ttlFunction, "TTL function must not be null");
			this.lockTtlFunction = ttlFunction;

			return this;
		}

	}

	@Override
	public byte @Nullable [] get(String name, byte[] key) {
		return get(name, key, null);
	}

	@Override
	public byte @Nullable [] get(String name, byte[] key, @Nullable Duration ttl) {

		Assert.notNull(name, "Name must not be null");
		Assert.notNull(key, "Key must not be null");

		return execute(name, connection -> doGet(connection, name, key, ttl));
	}

	@SuppressWarnings("NullAway")
	private byte @Nullable [] doGet(RedisConnection connection, String name, byte[] key, @Nullable Duration ttl) {

		RedisStringCommands commands = connection.stringCommands();
		byte[] result = shouldExpireWithin(ttl) ? commands.getEx(key, Expiration.from(ttl)) : commands.get(key);

		statistics.incGets(name);

		if (result != null) {
			statistics.incHits(name);
		} else {
			statistics.incMisses(name);
		}

		return result;
	}

	@Override
	public byte[] get(String name, byte[] key, Supplier<byte[]> valueLoader, @Nullable Duration ttl,
			boolean timeToIdleEnabled) {

		Assert.notNull(name, "Name must not be null");
		Assert.notNull(key, "Key must not be null");

		boolean withTtl = shouldExpireWithin(ttl);

		// double-checked locking optimization
		if (isLockingCacheWriter()) {
			byte[] bytes = get(name, key, timeToIdleEnabled && withTtl ? ttl : null);
			if (bytes != null) {
				return bytes;
			}
		}

		return execute(name, connection -> {

			if (isLockingCacheWriter()) {
				doLock(name, key, null, connection);
			}

			try {

				byte[] result = doGet(connection, name, key, timeToIdleEnabled && withTtl ? ttl : null);

				if (result != null) {
					return result;
				}

				byte[] value = valueLoader.get();
				doPut(connection, name, key, value, ttl);
				return value;
			} finally {
				if (isLockingCacheWriter()) {
					doUnlock(name, connection);
				}
			}
		});
	}

	@Override
	public boolean supportsAsyncRetrieve() {
		return asyncCacheWriter.isSupported();
	}

	private boolean writeAsynchronously() {
		return supportsAsyncRetrieve() && asynchronousWrites;
	}

	@Override
	public CompletableFuture<byte[]> retrieve(String name, byte[] key, @Nullable Duration ttl) {

		Assert.notNull(name, "Name must not be null");
		Assert.notNull(key, "Key must not be null");

		return asyncCacheWriter.retrieve(name, key, ttl) //
				.thenApply(cachedValue -> {

					statistics.incGets(name);

					if (cachedValue != null) {
						statistics.incHits(name);
					} else {
						statistics.incMisses(name);
					}

					return cachedValue;
				});
	}

	@Override
	public void put(String name, byte[] key, byte[] value, @Nullable Duration ttl) {

		Assert.notNull(name, "Name must not be null");
		Assert.notNull(key, "Key must not be null");
		Assert.notNull(value, "Value must not be null");

		if (writeAsynchronously()) {
			asyncCacheWriter.store(name, key, value, ttl).thenRun(() -> statistics.incPuts(name));
		} else {
			execute(name, connection -> {
				doPut(connection, name, key, value, ttl);
				return "OK";
			});
		}
	}

	@SuppressWarnings("NullAway")
	private void doPut(RedisConnection connection, String name, byte[] key, byte[] value, @Nullable Duration ttl) {

		if (shouldExpireWithin(ttl)) {
			connection.stringCommands().set(key, value, Expiration.from(ttl.toMillis(), TimeUnit.MILLISECONDS),
					SetOption.upsert());
		} else {
			connection.stringCommands().set(key, value);
		}

		statistics.incPuts(name);
	}

	@Override
	public CompletableFuture<Void> store(String name, byte[] key, byte[] value, @Nullable Duration ttl) {

		Assert.notNull(name, "Name must not be null");
		Assert.notNull(key, "Key must not be null");
		Assert.notNull(value, "Value must not be null");

		return asyncCacheWriter.store(name, key, value, ttl) //
				.thenRun(() -> statistics.incPuts(name));
	}

	@Override
	@SuppressWarnings("NullAway")
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
					put = ObjectUtils.nullSafeEquals(
							connection.stringCommands().set(key, value, Expiration.from(ttl), SetOption.ifAbsent()), true);
				} else {
					put = ObjectUtils.nullSafeEquals(connection.stringCommands().setNX(key, value), true);
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

		if (writeAsynchronously()) {
			asyncCacheWriter.remove(name, key).thenRun(() -> statistics.incDeletes(name));
		} else {
			removeIfPresent(name, key);
		}
	}

	@Override
	public boolean removeIfPresent(String name, byte[] key) {

		Long removals = execute(name, connection -> connection.keyCommands().del(key));
		statistics.incDeletes(name);

		return removals > 0;
	}

	@Override
	public void clean(String name, byte[] pattern) {

		Assert.notNull(name, "Name must not be null");
		Assert.notNull(pattern, "Pattern must not be null");

		if (writeAsynchronously()) {
			asyncCacheWriter.clean(name, pattern, batchStrategy)
					.thenAccept(deleteCount -> statistics.incDeletesBy(name, deleteCount.intValue()));
			return;
		}

		invalidate(name, pattern);
	}

	@Override
	public boolean invalidate(String name, byte[] pattern) {

		Assert.notNull(name, "Name must not be null");
		Assert.notNull(pattern, "Pattern must not be null");

		return execute(name, connection -> {

			try {
				if (isLockingCacheWriter()) {
					doLock(name, name, pattern, connection);
				}

				long deleteCount = batchStrategy.cleanCache(connection, name, pattern);

				while (deleteCount > Integer.MAX_VALUE) {
					statistics.incDeletesBy(name, Integer.MAX_VALUE);
					deleteCount -= Integer.MAX_VALUE;
				}

				statistics.incDeletesBy(name, (int) deleteCount);

				return deleteCount > 0;
			} finally {
				if (isLockingCacheWriter()) {
					doUnlock(name, connection);
				}
			}
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
				this.batchStrategy, this.asynchronousWrites);
	}

	/**
	 * Explicitly set a write lock on a cache.
	 *
	 * @param name the name of the cache to lock.
	 */
	void lock(String name) {
		executeWithoutResult(name, connection -> doLock(name, name, null, connection));
	}

	void doLock(String name, Object contextualKey, @Nullable Object contextualValue, RedisConnection connection) {

		RedisStringCommands commands = connection.stringCommands();
		Expiration expiration = Expiration.from(this.lockTtl.getTimeToLive(contextualKey, contextualValue));
		byte[] cacheLockKey = createCacheLockKey(name);

		while (!ObjectUtils.nullSafeEquals(commands.set(cacheLockKey, new byte[0], expiration, SetOption.SET_IF_ABSENT),
				true)) {
			checkAndPotentiallyWaitUntilUnlocked(name, connection);
		}
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
	Long doUnlock(String name, RedisConnection connection) {
		return connection.keyCommands().del(createCacheLockKey(name));
	}

	private <T> T execute(String name, Function<RedisConnection, T> callback) {

		try (RedisConnection connection = this.connectionFactory.getConnection()) {
			checkAndPotentiallyWaitUntilUnlocked(name, connection);
			return callback.apply(connection);
		}
	}

	private void executeWithoutResult(String name, Consumer<RedisConnection> callback) {

		try (RedisConnection connection = this.connectionFactory.getConnection()) {
			checkAndPotentiallyWaitUntilUnlocked(name, connection);
			callback.accept(connection);
		}
	}

	private <T> T executeLockFree(Function<RedisConnection, T> callback) {

		try (RedisConnection connection = this.connectionFactory.getConnection()) {
			return callback.apply(connection);
		}
	}

	/**
	 * Determines whether this {@link RedisCacheWriter} uses locks during caching operations.
	 *
	 * @return {@literal true} if {@link RedisCacheWriter} uses locks.
	 */
	private boolean isLockingCacheWriter() {
		return !this.sleepTime.isZero() && !this.sleepTime.isNegative();
	}

	private void checkAndPotentiallyWaitUntilUnlocked(String name, RedisConnection connection) {

		if (!isLockingCacheWriter()) {
			return;
		}

		long lockWaitTimeNs = System.nanoTime();

		try {
			while (doCheckLock(name, connection)) {
				Thread.sleep(this.sleepTime.toMillis());
			}
		} catch (InterruptedException ex) {

			// Re-interrupt current Thread to allow other participants to react.
			Thread.currentThread().interrupt();

			throw new PessimisticLockingFailureException("Interrupted while waiting to unlock cache %s".formatted(name), ex);
		} finally {
			this.statistics.incLockTime(name, System.nanoTime() - lockWaitTimeNs);
		}
	}

	boolean doCheckLock(String name, RedisConnection connection) {
		return ObjectUtils.nullSafeEquals(connection.keyCommands().exists(createCacheLockKey(name)), true);
	}

	byte[] createCacheLockKey(String name) {
		return (name + "~lock").getBytes(StandardCharsets.UTF_8);
	}

	private static boolean shouldExpireWithin(@Nullable Duration ttl) {
		return ttl != null && !ttl.isZero() && !ttl.isNegative();
	}

	/**
	 * Interface for asynchronous cache retrieval.
	 *
	 * @since 3.2
	 */
	interface AsyncCacheWriter {

		/**
		 * @return {@code true} if async cache operations are supported; {@code false} otherwise.
		 */
		boolean isSupported();

		/**
		 * Retrieve a cache entry asynchronously.
		 *
		 * @param name the cache name from which to retrieve the cache entry.
		 * @param key the cache entry key.
		 * @param ttl optional TTL to set for Time-to-Idle eviction.
		 * @return a future that completes either with a value if the value exists or completing with {@literal null} if the
		 *         cache does not contain an entry.
		 */
		CompletableFuture<byte[]> retrieve(String name, byte[] key, @Nullable Duration ttl);

		/**
		 * Store a cache entry asynchronously.
		 *
		 * @param name the cache name which to store the cache entry to.
		 * @param key the key for the cache entry. Must not be {@literal null}.
		 * @param value the value stored for the key. Must not be {@literal null}.
		 * @param ttl optional expiration time. Can be {@literal null}.
		 * @return a future that signals completion.
		 */
		CompletableFuture<Void> store(String name, byte[] key, byte[] value, @Nullable Duration ttl);

		/**
		 * Remove a cache entry asynchronously.
		 *
		 * @param name the cache name which to store the cache entry to.
		 * @param key the key for the cache entry. Must not be {@literal null}.
		 * @return a future that signals completion.
		 */
		CompletableFuture<Void> remove(String name, byte[] key);

		/**
		 * Clear the cache asynchronously.
		 *
		 * @param name the cache name which to store the cache entry to.
		 * @param pattern {@link String pattern} used to match Redis keys to clear.
		 * @param batchStrategy strategy to use.
		 * @return a future that signals completion emitting the number of removed keys.
		 */
		CompletableFuture<Long> clean(String name, byte[] pattern, BatchStrategy batchStrategy);

	}

	/**
	 * Unsupported variant of a {@link AsyncCacheWriter}.
	 *
	 * @since 3.2
	 */
	enum UnsupportedAsyncCacheWriter implements AsyncCacheWriter {

		INSTANCE;

		@Override
		public boolean isSupported() {
			return false;
		}

		@Override
		public CompletableFuture<byte[]> retrieve(String name, byte[] key, @Nullable Duration ttl) {
			throw new UnsupportedOperationException("async retrieve not supported");
		}

		@Override
		public CompletableFuture<Void> store(String name, byte[] key, byte[] value, @Nullable Duration ttl) {
			throw new UnsupportedOperationException("async store not supported");
		}

		@Override
		public CompletableFuture<Void> remove(String name, byte[] key) {
			throw new UnsupportedOperationException("async remove not supported");
		}

		@Override
		public CompletableFuture<Long> clean(String name, byte[] pattern, BatchStrategy batchStrategy) {
			throw new UnsupportedOperationException("async clean not supported");
		}

	}

	/**
	 * Delegate implementing {@link AsyncCacheWriter} to provide asynchronous cache retrieval and storage operations using
	 * {@link ReactiveRedisConnectionFactory}.
	 *
	 * @since 3.2
	 */
	class AsynchronousCacheWriterDelegate implements AsyncCacheWriter {

		private static final int DEFAULT_SCAN_BATCH_SIZE = 64;
		private final int cleanBatchSize;

		public AsynchronousCacheWriterDelegate() {
			this.cleanBatchSize = batchStrategy instanceof BatchStrategies.Scan scan ? scan.batchSize
					: DEFAULT_SCAN_BATCH_SIZE;
		}

		@Override
		public boolean isSupported() {
			return true;
		}

		@Override
		@SuppressWarnings("NullAway")
		public CompletableFuture<byte[]> retrieve(String name, byte[] key, @Nullable Duration ttl) {

			return doWithConnection(connection -> {

				ByteBuffer wrappedKey = ByteBuffer.wrap(key);
				Mono<?> cacheLockCheck = isLockingCacheWriter() ? waitForLock(connection, name) : Mono.empty();
				ReactiveStringCommands stringCommands = connection.stringCommands();

				Mono<ByteBuffer> get = shouldExpireWithin(ttl) ? stringCommands.getEx(wrappedKey, Expiration.from(ttl))
						: stringCommands.get(wrappedKey);

				return cacheLockCheck.then(get).map(ByteUtils::getBytes);
			});
		}

		@Override
		public CompletableFuture<Void> store(String name, byte[] key, byte[] value, @Nullable Duration ttl) {

			return doWithConnection(connection -> {

				Mono<?> mono = doWithLocking(name, key, value, connection, () -> doStore(key, value, ttl, connection));

				return mono.then();
			});
		}

		@SuppressWarnings("NullAway")
		private Mono<Boolean> doStore(byte[] cacheKey, byte[] value, @Nullable Duration ttl,
				ReactiveRedisConnection connection) {

			ByteBuffer wrappedKey = ByteBuffer.wrap(cacheKey);
			ByteBuffer wrappedValue = ByteBuffer.wrap(value);

			if (shouldExpireWithin(ttl)) {
				return connection.stringCommands().set(wrappedKey, wrappedValue,
						Expiration.from(ttl.toMillis(), TimeUnit.MILLISECONDS), SetOption.upsert());
			} else {
				return connection.stringCommands().set(wrappedKey, wrappedValue);
			}
		}

		@Override
		public CompletableFuture<Void> remove(String name, byte[] key) {

			return doWithConnection(connection -> {
				return doWithLocking(name, key, null, connection, () -> doRemove(key, connection)).then();
			});
		}

		@Override
		public CompletableFuture<Long> clean(String name, byte[] pattern, BatchStrategy batchStrategy) {

			return doWithConnection(connection -> {
				return doWithLocking(name, pattern, null, connection, () -> doClean(pattern, connection));
			});
		}

		private Mono<Long> doClean(byte[] pattern, ReactiveRedisConnection connection) {

			ReactiveKeyCommands commands = connection.keyCommands();

			Flux<ByteBuffer> keys;

			if (batchStrategy instanceof BatchStrategies.Keys) {
				keys = commands.keys(ByteBuffer.wrap(pattern)).flatMapMany(Flux::fromIterable);
			} else {
				keys = commands.scan(ScanOptions.scanOptions().count(cleanBatchSize).match(pattern).build());
			}

			return keys
					.buffer(cleanBatchSize) //
					.flatMap(commands::mUnlink) //
					.collect(Collectors.summingLong(Long::longValue));
		}

		@SuppressWarnings("NullAway")
		private Mono<Long> doRemove(byte[] cacheKey, ReactiveRedisConnection connection) {

			ByteBuffer wrappedKey = ByteBuffer.wrap(cacheKey);

			return connection.keyCommands().unlink(wrappedKey);
		}

		private <T> Mono<T> doWithLocking(String name, byte[] key, byte @Nullable [] value,
				ReactiveRedisConnection connection, Supplier<Mono<T>> action) {

			if (isLockingCacheWriter()) {
				return Mono.usingWhen(doLock(name, key, value, connection), unused -> action.get(),
						unused -> doUnlock(name, connection));
			}

			return action.get();
		}

		private Mono<Object> doLock(String name, Object contextualKey, @Nullable Object contextualValue,
				ReactiveRedisConnection connection) {

			ByteBuffer key = ByteBuffer.wrap(createCacheLockKey(name));
			ByteBuffer value = ByteBuffer.wrap(new byte[0]);
			Expiration expiration = Expiration.from(lockTtl.getTimeToLive(contextualKey, contextualValue));

			return connection.stringCommands().set(key, value, expiration, SetOption.SET_IF_ABSENT) //
					// Ensure we emit an object, otherwise, the Mono.usingWhen operator doesn't run the inner resource function.
					.thenReturn(Boolean.TRUE);
		}

		private Mono<Void> doUnlock(String name, ReactiveRedisConnection connection) {
			return connection.keyCommands().del(ByteBuffer.wrap(createCacheLockKey(name))).then();
		}

		private Mono<Void> waitForLock(ReactiveRedisConnection connection, String cacheName) {

			AtomicLong lockWaitTimeNs = new AtomicLong();
			byte[] cacheLockKey = createCacheLockKey(cacheName);

			Flux<Long> wait = Flux.interval(Duration.ZERO, sleepTime);
			Mono<Boolean> exists = connection.keyCommands().exists(ByteBuffer.wrap(cacheLockKey)).filter(it -> !it);

			return wait.doOnSubscribe(subscription -> lockWaitTimeNs.set(System.nanoTime())) //
					.flatMap(it -> exists) //
					.doFinally(signalType -> statistics.incLockTime(cacheName, System.nanoTime() - lockWaitTimeNs.get())) //
					.next() //
					.then();
		}

		private <T> CompletableFuture<T> doWithConnection(
				Function<ReactiveRedisConnection, Mono<T>> callback) {

			ReactiveRedisConnectionFactory cf = (ReactiveRedisConnectionFactory) connectionFactory;

			return Mono.usingWhen(Mono.fromSupplier(cf::getReactiveConnection), //
					callback::apply, //
					ReactiveRedisConnection::closeLater) //
					.toFuture();
		}

	}

}
