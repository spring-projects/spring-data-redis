/*
 * Copyright 2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.springframework.data.redis.ConnectionFactoryTracker;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStringCommands.SetOption;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.types.Expiration;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@RunWith(Parameterized.class)
public class DefaultRedisCacheWriterTests {

	static final String CACHE_NAME = "default-redis-cache-writer-tests";

	String key = "key-1";
	String cacheKey = CACHE_NAME + "::" + key;

	byte[] binaryCacheKey = cacheKey.getBytes(StandardCharsets.UTF_8);
	byte[] binaryCacheValue = "value".getBytes(StandardCharsets.UTF_8);

	RedisConnectionFactory connectionFactory;

	public DefaultRedisCacheWriterTests(RedisConnectionFactory connectionFactory) {

		this.connectionFactory = connectionFactory;

		ConnectionFactoryTracker.add(connectionFactory);
	}

	@Parameters(name = "{index}: {0}")
	public static Collection<Object[]> testParams() {
		return CacheTestParams.justConnectionFactories();
	}

	@AfterClass
	public static void cleanUpResources() {
		ConnectionFactoryTracker.cleanUp();
	}

	@Before
	public void setUp() {

		JedisConnectionFactory cf = new JedisConnectionFactory();
		cf.afterPropertiesSet();

		connectionFactory = cf;

		doWithConnection(RedisConnection::flushAll);
	}

	@Test // DATAREDIS-481
	public void putShouldAddEternalEntry() {

		nonLockingRedisCacheWriter(connectionFactory).put(CACHE_NAME, binaryCacheKey, binaryCacheValue, Duration.ZERO);

		doWithConnection(connection -> {
			assertThat(connection.get(binaryCacheKey)).isEqualTo(binaryCacheValue);
			assertThat(connection.ttl(binaryCacheKey)).isEqualTo(-1);
		});
	}

	@Test // DATAREDIS-481
	public void putShouldAddExpiringEntry() {

		nonLockingRedisCacheWriter(connectionFactory).put(CACHE_NAME, binaryCacheKey, binaryCacheValue,
				Duration.ofSeconds(1));

		doWithConnection(connection -> {
			assertThat(connection.get(binaryCacheKey)).isEqualTo(binaryCacheValue);
			assertThat(connection.ttl(binaryCacheKey)).isGreaterThan(0);
		});
	}

	@Test // DATAREDIS-481
	public void putShouldOverwriteExistingEternalEntry() {

		doWithConnection(connection -> connection.set(binaryCacheKey, "foo".getBytes()));

		nonLockingRedisCacheWriter(connectionFactory).put(CACHE_NAME, binaryCacheKey, binaryCacheValue, Duration.ZERO);

		doWithConnection(connection -> {
			assertThat(connection.get(binaryCacheKey)).isEqualTo(binaryCacheValue);
			assertThat(connection.ttl(binaryCacheKey)).isEqualTo(-1);
		});
	}

	@Test // DATAREDIS-481
	public void putShouldOverwriteExistingExpiringEntryAndResetTtl() {

		doWithConnection(connection -> connection.set(binaryCacheKey, "foo".getBytes(),
				Expiration.from(1, TimeUnit.MINUTES), SetOption.upsert()));

		nonLockingRedisCacheWriter(connectionFactory).put(CACHE_NAME, binaryCacheKey, binaryCacheValue,
				Duration.ofSeconds(5));

		doWithConnection(connection -> {
			assertThat(connection.get(binaryCacheKey)).isEqualTo(binaryCacheValue);
			assertThat(connection.ttl(binaryCacheKey)).isGreaterThan(3).isLessThan(6);
		});
	}

	@Test // DATAREDIS-481
	public void getShouldReturnValue() {

		doWithConnection(connection -> connection.set(binaryCacheKey, binaryCacheValue));

		assertThat(nonLockingRedisCacheWriter(connectionFactory).get(CACHE_NAME, binaryCacheKey))
				.isEqualTo(binaryCacheValue);
	}

	@Test // DATAREDIS-481
	public void getShouldReturnNullWhenKeyDoesNotExist() {
		assertThat(nonLockingRedisCacheWriter(connectionFactory).get(CACHE_NAME, binaryCacheKey)).isNull();
	}

	@Test // DATAREDIS-481
	public void putIfAbsentShouldAddEternalEntryWhenKeyDoesNotExist() {

		assertThat(nonLockingRedisCacheWriter(connectionFactory).putIfAbsent(CACHE_NAME, binaryCacheKey, binaryCacheValue,
				Duration.ZERO)).isNull();

		doWithConnection(connection -> {
			assertThat(connection.get(binaryCacheKey)).isEqualTo(binaryCacheValue);
		});
	}

	@Test // DATAREDIS-481
	public void putIfAbsentShouldNotAddEternalEntryWhenKeyAlreadyExist() {

		doWithConnection(connection -> connection.set(binaryCacheKey, binaryCacheValue));

		assertThat(nonLockingRedisCacheWriter(connectionFactory).putIfAbsent(CACHE_NAME, binaryCacheKey, "foo".getBytes(),
				Duration.ZERO)).isEqualTo(binaryCacheValue);

		doWithConnection(connection -> {
			assertThat(connection.get(binaryCacheKey)).isEqualTo(binaryCacheValue);
		});
	}

	@Test // DATAREDIS-481
	public void putIfAbsentShouldAddExpiringEntryWhenKeyDoesNotExist() {

		assertThat(nonLockingRedisCacheWriter(connectionFactory).putIfAbsent(CACHE_NAME, binaryCacheKey, binaryCacheValue,
				Duration.ofSeconds(5))).isNull();

		doWithConnection(connection -> {
			assertThat(connection.ttl(binaryCacheKey)).isGreaterThan(3).isLessThan(6);
		});
	}

	@Test // DATAREDIS-481
	public void removeShouldDeleteEntry() {

		doWithConnection(connection -> connection.set(binaryCacheKey, binaryCacheValue));

		nonLockingRedisCacheWriter(connectionFactory).remove(CACHE_NAME, binaryCacheKey);

		doWithConnection(connection -> assertThat(connection.exists(binaryCacheKey)).isFalse());
	}

	@Test // DATAREDIS-418
	public void cleanShouldRemoveAllKeysByPattern() {

		doWithConnection(connection -> {
			connection.set(binaryCacheKey, binaryCacheValue);
			connection.set("foo".getBytes(), "bar".getBytes());
		});

		nonLockingRedisCacheWriter(connectionFactory).clean(CACHE_NAME,
				(CACHE_NAME + "::*").getBytes(Charset.forName("UTF-8")));

		doWithConnection(connection -> {
			assertThat(connection.exists(binaryCacheKey)).isFalse();
			assertThat(connection.exists("foo".getBytes())).isTrue();
		});
	}

	@Test // DATAREDIS-481
	public void nonLockingCacheWriterShouldIgnoreExistingLock() {

		((DefaultRedisCacheWriter) lockingRedisCacheWriter(connectionFactory)).lock(CACHE_NAME);

		nonLockingRedisCacheWriter(connectionFactory).put(CACHE_NAME, binaryCacheKey, binaryCacheValue, Duration.ZERO);

		doWithConnection(connection -> {
			assertThat(connection.exists(binaryCacheKey)).isTrue();
		});
	}

	@Test // DATAREDIS-481
	public void lockingCacheWriterShouldIgnoreExistingLockOnDifferenceCache() {

		((DefaultRedisCacheWriter) lockingRedisCacheWriter(connectionFactory)).lock(CACHE_NAME);

		lockingRedisCacheWriter(connectionFactory).put(CACHE_NAME + "-no-the-other-cache", binaryCacheKey, binaryCacheValue,
				Duration.ZERO);

		doWithConnection(connection -> {
			assertThat(connection.exists(binaryCacheKey)).isTrue();
		});
	}

	@Test // DATAREDIS-481
	public void lockingCacheWriterShouldWaitForLockRelease() throws InterruptedException {

		DefaultRedisCacheWriter cw = (DefaultRedisCacheWriter) lockingRedisCacheWriter(connectionFactory);
		cw.lock(CACHE_NAME);

		CountDownLatch beforeWrite = new CountDownLatch(1);
		CountDownLatch afterWrite = new CountDownLatch(1);

		Thread th = new Thread(() -> {

			RedisCacheWriter writer = lockingRedisCacheWriter(connectionFactory);
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

			cw.unlock(CACHE_NAME);

			afterWrite.await();
			doWithConnection(connection -> {
				assertThat(connection.exists(binaryCacheKey)).isTrue();
			});
		} finally {
			th.interrupt();
		}
	}

	@Test // DATAREDIS-481
	public void lockingCacheWriterShouldExitWhenInterruptedWaitForLockRelease() throws InterruptedException {

		DefaultRedisCacheWriter cw = (DefaultRedisCacheWriter) lockingRedisCacheWriter(connectionFactory);
		cw.lock(CACHE_NAME);

		CountDownLatch beforeWrite = new CountDownLatch(1);
		CountDownLatch afterWrite = new CountDownLatch(1);
		AtomicReference<Exception> exceptionRef = new AtomicReference<>();

		Thread th = new Thread(() -> {

			DefaultRedisCacheWriter writer = new DefaultRedisCacheWriter(connectionFactory, Duration.ofMillis(50)) {

				@Override
				boolean doCheckLock(String name, RedisConnection connection) {
					beforeWrite.countDown();
					return super.doCheckLock(name, connection);
				}
			};

			try {
				writer.put(CACHE_NAME, binaryCacheKey, binaryCacheValue, Duration.ZERO);
			} catch (Exception e) {
				exceptionRef.set(e);
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

	private void doWithConnection(Consumer<RedisConnection> callback) {

		RedisConnection connection = connectionFactory.getConnection();
		try {
			callback.accept(connection);
		} finally {
			connection.close();
		}
	}
}
