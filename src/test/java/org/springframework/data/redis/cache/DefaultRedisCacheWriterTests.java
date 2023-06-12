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

import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.redis.cache.RedisCacheWriter.*;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.junit.jupiter.api.BeforeEach;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStringCommands.SetOption;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.connection.jedis.extension.JedisConnectionFactoryExtension;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.data.redis.test.extension.RedisStanalone;
import org.springframework.data.redis.test.extension.parametrized.MethodSource;
import org.springframework.data.redis.test.extension.parametrized.ParameterizedRedisTest;

/**
 * Integration tests for {@link DefaultRedisCacheWriter}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@MethodSource("testParams")
public class DefaultRedisCacheWriterTests {

	private static final String CACHE_NAME = "default-redis-cache-writer-tests";

	private String key = "key-1";
	private String cacheKey = CACHE_NAME + "::" + key;

	private byte[] binaryCacheKey = cacheKey.getBytes(StandardCharsets.UTF_8);
	private byte[] binaryCacheValue = "value".getBytes(StandardCharsets.UTF_8);

	private RedisConnectionFactory connectionFactory;

	public DefaultRedisCacheWriterTests(RedisConnectionFactory connectionFactory) {
		this.connectionFactory = connectionFactory;
	}

	public static Collection<Object[]> testParams() {
		return CacheTestParams.justConnectionFactories();
	}

	@BeforeEach
	void setUp() {

		JedisConnectionFactory connectionFactory =
				JedisConnectionFactoryExtension.getConnectionFactory(RedisStanalone.class);

		this.connectionFactory = connectionFactory;

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

		lockingRedisCacheWriter(connectionFactory).put(CACHE_NAME + "-no-the-other-cache", binaryCacheKey,
				binaryCacheValue, Duration.ZERO);

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
			} catch (Exception cause) {
				exceptionRef.set(cause);
			} finally {
				afterWrite.countDown();
			}
		});

		th.start();
		beforeWrite.await();

		th.interrupt();

		afterWrite.await();

		assertThat(exceptionRef.get()).hasMessageContaining("Interrupted while waiting to unlock")
				.hasCauseInstanceOf(InterruptedException.class);
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

	private void doWithConnection(Consumer<RedisConnection> callback) {

		try (RedisConnection connection = connectionFactory.getConnection()) {
			callback.accept(connection);
		}
	}
}
