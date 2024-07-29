/*
 * Copyright 2017-2024 the original author or authors.
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

import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.redis.cache.RedisCacheWriter.*;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.junit.jupiter.api.BeforeEach;

import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStringCommands.SetOption;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.data.redis.test.condition.EnabledOnRedisDriver;
import org.springframework.data.redis.test.condition.EnabledOnRedisDriver.DriverQualifier;
import org.springframework.data.redis.test.condition.RedisDriver;
import org.springframework.data.redis.test.extension.parametrized.MethodSource;
import org.springframework.data.redis.test.extension.parametrized.ParameterizedRedisTest;
import org.springframework.lang.Nullable;

/**
 * Integration tests for {@link DefaultRedisCacheWriter}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author ChanYoung Joung
 */
@MethodSource("testParams")
public class DefaultRedisCacheWriterTests {

	private static final String CACHE_NAME = "default-redis-cache-writer-tests";

	private String key = "key-1";
	private String cacheKey = CACHE_NAME + "::" + key;

	private byte[] binaryCacheKey = cacheKey.getBytes(StandardCharsets.UTF_8);
	private byte[] binaryCacheValue = "value".getBytes(StandardCharsets.UTF_8);

	private final @DriverQualifier RedisConnectionFactory connectionFactory;

	public DefaultRedisCacheWriterTests(RedisConnectionFactory connectionFactory) {
		this.connectionFactory = connectionFactory;
	}

	public static Collection<Object[]> testParams() {
		return CacheTestParams.justConnectionFactories();
	}

	@BeforeEach
	void setUp() {
		doWithConnection(RedisConnection::flushAll);
	}

	@ParameterizedRedisTest // DATAREDIS-481, DATAREDIS-1082
	void putShouldAddEternalEntry() {

		RedisCacheWriter writer = nonLockingRedisCacheWriter(connectionFactory)
				.withStatisticsCollector(CacheStatisticsCollector.create());

		writer.put(CACHE_NAME, binaryCacheKey, binaryCacheValue, Duration.ZERO);

		doWithConnection(connection -> {
			assertThat(connection.get(binaryCacheKey)).isEqualTo(binaryCacheValue);
			assertThat(connection.ttl(binaryCacheKey)).isEqualTo(-1);
		});

		assertThat(writer.getCacheStatistics(CACHE_NAME).getPuts()).isOne();
		assertThat(writer.getCacheStatistics(CACHE_NAME).getLockWaitDuration(TimeUnit.NANOSECONDS)).isZero();
	}

	@ParameterizedRedisTest // DATAREDIS-481
	void putShouldAddExpiringEntry() {

		nonLockingRedisCacheWriter(connectionFactory).put(CACHE_NAME, binaryCacheKey, binaryCacheValue,
				Duration.ofSeconds(1));

		doWithConnection(connection -> {
			assertThat(connection.get(binaryCacheKey)).isEqualTo(binaryCacheValue);
			assertThat(connection.ttl(binaryCacheKey)).isGreaterThan(0);
		});
	}

	@ParameterizedRedisTest // DATAREDIS-481
	void putShouldOverwriteExistingEternalEntry() {

		doWithConnection(connection -> connection.set(binaryCacheKey, "foo".getBytes()));

		nonLockingRedisCacheWriter(connectionFactory).put(CACHE_NAME, binaryCacheKey, binaryCacheValue, Duration.ZERO);

		doWithConnection(connection -> {
			assertThat(connection.get(binaryCacheKey)).isEqualTo(binaryCacheValue);
			assertThat(connection.ttl(binaryCacheKey)).isEqualTo(-1);
		});
	}

	@ParameterizedRedisTest // DATAREDIS-481
	void putShouldOverwriteExistingExpiringEntryAndResetTtl() {

		doWithConnection(connection -> connection.set(binaryCacheKey, "foo".getBytes(),
				Expiration.from(1, TimeUnit.MINUTES), SetOption.upsert()));

		nonLockingRedisCacheWriter(connectionFactory).put(CACHE_NAME, binaryCacheKey, binaryCacheValue,
				Duration.ofSeconds(5));

		doWithConnection(connection -> {
			assertThat(connection.get(binaryCacheKey)).isEqualTo(binaryCacheValue);
			assertThat(connection.ttl(binaryCacheKey)).isGreaterThan(3).isLessThan(6);
		});
	}

	@ParameterizedRedisTest // DATAREDIS-481, DATAREDIS-1082
	void getShouldReturnValue() {

		doWithConnection(connection -> connection.set(binaryCacheKey, binaryCacheValue));

		RedisCacheWriter writer = nonLockingRedisCacheWriter(connectionFactory)
				.withStatisticsCollector(CacheStatisticsCollector.create());

		assertThat(writer.get(CACHE_NAME, binaryCacheKey)).isEqualTo(binaryCacheValue);
		assertThat(writer.getCacheStatistics(CACHE_NAME).getGets()).isOne();
		assertThat(writer.getCacheStatistics(CACHE_NAME).getHits()).isOne();
		assertThat(writer.getCacheStatistics(CACHE_NAME).getMisses()).isZero();
	}

	@ParameterizedRedisTest // DATAREDIS-481
	void getShouldReturnNullWhenKeyDoesNotExist() {
		assertThat(nonLockingRedisCacheWriter(connectionFactory).get(CACHE_NAME, binaryCacheKey)).isNull();
	}

	@ParameterizedRedisTest // GH-2650
	@EnabledOnRedisDriver(RedisDriver.LETTUCE)
	void cacheHitRetrieveShouldIncrementStatistics() throws ExecutionException, InterruptedException {

		doWithConnection(connection -> connection.set(binaryCacheKey, binaryCacheValue));

		RedisCacheWriter writer = nonLockingRedisCacheWriter(connectionFactory)
				.withStatisticsCollector(CacheStatisticsCollector.create());

		writer.retrieve(CACHE_NAME, binaryCacheKey).get();

		assertThat(writer.getCacheStatistics(CACHE_NAME).getGets()).isOne();
		assertThat(writer.getCacheStatistics(CACHE_NAME).getHits()).isOne();
	}

	@ParameterizedRedisTest // GH-2650
	@EnabledOnRedisDriver(RedisDriver.LETTUCE)
	void storeShouldIncrementStatistics() throws ExecutionException, InterruptedException {

		RedisCacheWriter writer = nonLockingRedisCacheWriter(connectionFactory)
				.withStatisticsCollector(CacheStatisticsCollector.create());

		writer.store(CACHE_NAME, binaryCacheKey, binaryCacheValue, null).get();

		assertThat(writer.getCacheStatistics(CACHE_NAME).getPuts()).isOne();
	}

	@ParameterizedRedisTest // GH-2650
	@EnabledOnRedisDriver(RedisDriver.LETTUCE)
	void cacheMissRetrieveWithLoaderAsyncShouldIncrementStatistics() throws ExecutionException, InterruptedException {

		RedisCacheWriter writer = nonLockingRedisCacheWriter(connectionFactory)
				.withStatisticsCollector(CacheStatisticsCollector.create());

		writer.retrieve(CACHE_NAME, binaryCacheKey).get();

		assertThat(writer.getCacheStatistics(CACHE_NAME).getGets()).isOne();
		assertThat(writer.getCacheStatistics(CACHE_NAME).getMisses()).isOne();
	}

	@ParameterizedRedisTest // DATAREDIS-481, DATAREDIS-1082
	void putIfAbsentShouldAddEternalEntryWhenKeyDoesNotExist() {

		RedisCacheWriter writer = nonLockingRedisCacheWriter(connectionFactory)
				.withStatisticsCollector(CacheStatisticsCollector.create());

		assertThat(writer.putIfAbsent(CACHE_NAME, binaryCacheKey, binaryCacheValue, Duration.ZERO)).isNull();

		doWithConnection(connection -> {
			assertThat(connection.get(binaryCacheKey)).isEqualTo(binaryCacheValue);
		});

		assertThat(writer.getCacheStatistics(CACHE_NAME).getPuts()).isOne();
	}

	@ParameterizedRedisTest // DATAREDIS-481, DATAREDIS-1082
	void putIfAbsentShouldNotAddEternalEntryWhenKeyAlreadyExist() {

		doWithConnection(connection -> connection.set(binaryCacheKey, binaryCacheValue));

		RedisCacheWriter writer = nonLockingRedisCacheWriter(connectionFactory)
				.withStatisticsCollector(CacheStatisticsCollector.create());

		assertThat(writer.putIfAbsent(CACHE_NAME, binaryCacheKey, "foo".getBytes(), Duration.ZERO))
				.isEqualTo(binaryCacheValue);

		doWithConnection(connection -> {
			assertThat(connection.get(binaryCacheKey)).isEqualTo(binaryCacheValue);
		});

		assertThat(writer.getCacheStatistics(CACHE_NAME).getPuts()).isZero();
	}

	@ParameterizedRedisTest // DATAREDIS-481, DATAREDIS-1082
	void putIfAbsentShouldAddExpiringEntryWhenKeyDoesNotExist() {

		RedisCacheWriter writer = nonLockingRedisCacheWriter(connectionFactory)
				.withStatisticsCollector(CacheStatisticsCollector.create());

		assertThat(writer.putIfAbsent(CACHE_NAME, binaryCacheKey, binaryCacheValue, Duration.ofSeconds(5))).isNull();

		doWithConnection(connection -> {
			assertThat(connection.ttl(binaryCacheKey)).isGreaterThan(3).isLessThan(6);
		});

		assertThat(writer.getCacheStatistics(CACHE_NAME).getPuts()).isOne();
	}

	@ParameterizedRedisTest // GH-2890
	void getWithValueLoaderShouldStoreCacheValue() {

		RedisCacheWriter writer = nonLockingRedisCacheWriter(connectionFactory)
				.withStatisticsCollector(CacheStatisticsCollector.create());

		writer.get(CACHE_NAME, binaryCacheKey, () -> binaryCacheValue, Duration.ofSeconds(5), true);

		doWithConnection(connection -> {
			assertThat(connection.ttl(binaryCacheKey)).isGreaterThan(3).isLessThan(6);
		});

		assertThat(writer.getCacheStatistics(CACHE_NAME).getMisses()).isOne();
		assertThat(writer.getCacheStatistics(CACHE_NAME).getPuts()).isOne();
	}

	@ParameterizedRedisTest // DATAREDIS-481, DATAREDIS-1082
	void removeShouldDeleteEntry() {

		doWithConnection(connection -> connection.set(binaryCacheKey, binaryCacheValue));

		RedisCacheWriter writer = nonLockingRedisCacheWriter(connectionFactory)
				.withStatisticsCollector(CacheStatisticsCollector.create());

		writer.remove(CACHE_NAME, binaryCacheKey);

		doWithConnection(connection -> assertThat(connection.exists(binaryCacheKey)).isFalse());

		assertThat(writer.getCacheStatistics(CACHE_NAME).getDeletes()).isOne();
	}

	@ParameterizedRedisTest // DATAREDIS-418, DATAREDIS-1082
	void cleanShouldRemoveAllKeysByPattern() {

		doWithConnection(connection -> {
			connection.set(binaryCacheKey, binaryCacheValue);
			connection.set("foo".getBytes(), "bar".getBytes());
		});

		RedisCacheWriter writer = nonLockingRedisCacheWriter(connectionFactory)
				.withStatisticsCollector(CacheStatisticsCollector.create());

		writer.clean(CACHE_NAME, (CACHE_NAME + "::*").getBytes(Charset.forName("UTF-8")));

		doWithConnection(connection -> {
			assertThat(connection.exists(binaryCacheKey)).isFalse();
			assertThat(connection.exists("foo".getBytes())).isTrue();
		});

		assertThat(writer.getCacheStatistics(CACHE_NAME).getDeletes()).isOne();
	}

	@ParameterizedRedisTest // DATAREDIS-481
	void nonLockingCacheWriterShouldIgnoreExistingLock() {

		((DefaultRedisCacheWriter) lockingRedisCacheWriter(connectionFactory)).lock(CACHE_NAME);

		nonLockingRedisCacheWriter(connectionFactory).put(CACHE_NAME, binaryCacheKey, binaryCacheValue, Duration.ZERO);

		doWithConnection(connection -> {
			assertThat(connection.exists(binaryCacheKey)).isTrue();
		});
	}

	@ParameterizedRedisTest // DATAREDIS-481
	void lockingCacheWriterShouldIgnoreExistingLockOnDifferenceCache() {

		((DefaultRedisCacheWriter) lockingRedisCacheWriter(connectionFactory)).lock(CACHE_NAME);

		lockingRedisCacheWriter(connectionFactory).put(CACHE_NAME + "-no-the-other-cache", binaryCacheKey, binaryCacheValue,
				Duration.ZERO);

		doWithConnection(connection -> {
			assertThat(connection.exists(binaryCacheKey)).isTrue();
		});
	}

	@ParameterizedRedisTest // DATAREDIS-481, DATAREDIS-1082
	void lockingCacheWriterShouldWaitForLockRelease() throws InterruptedException {

		DefaultRedisCacheWriter writer = (DefaultRedisCacheWriter) lockingRedisCacheWriter(connectionFactory)
				.withStatisticsCollector(CacheStatisticsCollector.create());
		writer.lock(CACHE_NAME);

		CountDownLatch beforeWrite = new CountDownLatch(1);
		CountDownLatch afterWrite = new CountDownLatch(1);

		Thread th = new Thread(() -> {
			beforeWrite.countDown();
			writer.put(CACHE_NAME, binaryCacheKey, binaryCacheValue, Duration.ZERO);
			afterWrite.countDown();
		});

		th.start();

		try {

			beforeWrite.await();

			Thread.sleep(200);

			doWithConnection(connection -> {
				assertThat(connection.exists(binaryCacheKey)).isFalse();
			});

			writer.unlock(CACHE_NAME);
			afterWrite.await();

			doWithConnection(connection -> {
				assertThat(connection.exists(binaryCacheKey)).isTrue();
			});

			assertThat(writer.getCacheStatistics(CACHE_NAME).getLockWaitDuration(TimeUnit.NANOSECONDS)).isGreaterThan(0);

		} finally {
			th.interrupt();
		}
	}

	@ParameterizedRedisTest // DATAREDIS-481
	void lockingCacheWriterShouldExitWhenInterruptedWaitForLockRelease() throws InterruptedException {

		DefaultRedisCacheWriter cw = (DefaultRedisCacheWriter) lockingRedisCacheWriter(connectionFactory);
		cw.lock(CACHE_NAME);

		CountDownLatch beforeWrite = new CountDownLatch(1);
		CountDownLatch afterWrite = new CountDownLatch(1);
		AtomicReference<Exception> exceptionRef = new AtomicReference<>();

		Thread th = new Thread(() -> {

			DefaultRedisCacheWriter writer = new DefaultRedisCacheWriter(connectionFactory, Duration.ofMillis(50),
					BatchStrategies.keys()) {

				@Override
				boolean doCheckLock(String name, RedisConnection connection) {
					beforeWrite.countDown();
					return super.doCheckLock(name, connection);
				}
			};

			try {
				writer.put(CACHE_NAME, binaryCacheKey, binaryCacheValue, Duration.ZERO);
			} catch (Exception ex) {
				exceptionRef.set(ex);
			} finally {
				afterWrite.countDown();
			}
		});

		th.start();
		beforeWrite.await();

		th.interrupt();

		afterWrite.await();

		assertThat(exceptionRef.get()).hasRootCauseInstanceOf(InterruptedException.class);
	}

	@ParameterizedRedisTest // GH-2300
	void lockingCacheWriterShouldUsePersistentLocks() {

		DefaultRedisCacheWriter writer = (DefaultRedisCacheWriter) lockingRedisCacheWriter(connectionFactory,
				Duration.ofSeconds(1), TtlFunction.persistent(), BatchStrategies.keys());

		writer.lock(CACHE_NAME);

		doWithConnection(conn -> {
			Long ttl = conn.ttl("default-redis-cache-writer-tests~lock".getBytes());
			assertThat(ttl).isEqualTo(-1);
		});
	}

	@ParameterizedRedisTest // GH-2300
	void lockingCacheWriterShouldApplyLockTtl() {

		DefaultRedisCacheWriter writer = (DefaultRedisCacheWriter) lockingRedisCacheWriter(connectionFactory,
				Duration.ofSeconds(1), TtlFunction.just(Duration.ofSeconds(60)), BatchStrategies.keys());

		writer.lock(CACHE_NAME);

		doWithConnection(conn -> {
			Long ttl = conn.ttl("default-redis-cache-writer-tests~lock".getBytes());
			assertThat(ttl).isGreaterThan(30).isLessThan(70);
		});
	}

	@ParameterizedRedisTest // DATAREDIS-1082
	void noOpStatisticsCollectorReturnsEmptyStatsInstance() {

		DefaultRedisCacheWriter cw = (DefaultRedisCacheWriter) lockingRedisCacheWriter(connectionFactory);
		CacheStatistics stats = cw.getCacheStatistics(CACHE_NAME);

		cw.putIfAbsent(CACHE_NAME, binaryCacheKey, binaryCacheValue, Duration.ofSeconds(5));

		assertThat(stats).isNotNull();
		assertThat(stats.getPuts()).isZero();
	}

	@ParameterizedRedisTest // GH-1686
	void doLockShouldGetLock() throws InterruptedException {

		int threadCount = 3;
		CountDownLatch beforeWrite = new CountDownLatch(threadCount);
		CountDownLatch afterWrite = new CountDownLatch(threadCount);
		AtomicLong concurrency = new AtomicLong();

		DefaultRedisCacheWriter cw = new DefaultRedisCacheWriter(connectionFactory, Duration.ofMillis(10),
				BatchStrategies.keys()) {

			boolean doLock(String name, Object contextualKey, @Nullable Object contextualValue, RedisConnection connection) {

				boolean doLock = super.doLock(name, contextualKey, contextualValue, connection);

				// any concurrent access (aka not waiting until the lock is acquired) will result in a concurrency greater 1
				assertThat(concurrency.incrementAndGet()).isOne();
				return doLock;
			}

			@Nullable
			@Override
			Long doUnlock(String name, RedisConnection connection) {
				try {
					return super.doUnlock(name, connection);
				} finally {
					concurrency.decrementAndGet();
				}
			}
		};

		cw.lock(CACHE_NAME);

		// introduce concurrency
		List<CompletableFuture<?>> completions = new ArrayList<>();
		for (int i = 0; i < threadCount; i++) {

			CompletableFuture<?> completion = new CompletableFuture<>();
			completions.add(completion);

			Thread th = new Thread(() -> {
				beforeWrite.countDown();
				try {
					cw.putIfAbsent(CACHE_NAME, binaryCacheKey, binaryCacheValue, Duration.ZERO);
					completion.complete(null);
				} catch (Throwable e) {
					completion.completeExceptionally(e);
				}
				afterWrite.countDown();
			});

			th.start();
		}

		assertThat(beforeWrite.await(5, TimeUnit.SECONDS)).isTrue();
		Thread.sleep(100);

		cw.unlock(CACHE_NAME);
		assertThat(afterWrite.await(5, TimeUnit.SECONDS)).isTrue();

		for (CompletableFuture<?> completion : completions) {
			assertThat(completion).isCompleted().isCompletedWithValue(null);
		}

		doWithConnection(conn -> {
			assertThat(conn.exists("default-redis-cache-writer-tests~lock".getBytes())).isFalse();
		});
	}

	private void doWithConnection(Consumer<RedisConnection> callback) {

		try (RedisConnection connection = connectionFactory.getConnection()) {
			callback.accept(connection);
		}
	}
}
