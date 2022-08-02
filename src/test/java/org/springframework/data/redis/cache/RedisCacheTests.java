/*
 * Copyright 2017-2022 the original author or authors.
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
import static org.assertj.core.api.Assumptions.*;

import io.netty.util.concurrent.DefaultThreadFactory;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import org.junit.jupiter.api.BeforeEach;

import org.springframework.cache.Cache.ValueWrapper;
import org.springframework.cache.interceptor.SimpleKey;
import org.springframework.cache.interceptor.SimpleKeyGenerator;
import org.springframework.cache.support.NullValue;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.serializer.RedisSerializationContext.SerializationPair;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.test.extension.parametrized.MethodSource;
import org.springframework.data.redis.test.extension.parametrized.ParameterizedRedisTest;
import org.springframework.lang.Nullable;

/**
 * Tests for {@link RedisCache} with {@link DefaultRedisCacheWriter} using different {@link RedisSerializer} and
 * {@link RedisConnectionFactory} pairs.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Piotr Mionskowski
 * @author Jos Roseboom
 */
@MethodSource("testParams")
public class RedisCacheTests {

	private String key = "key-1";
	private String cacheKey = "cache::" + key;
	private byte[] binaryCacheKey = cacheKey.getBytes(StandardCharsets.UTF_8);

	private Person sample = new Person("calmity", new Date());
	private byte[] binarySample;

	private byte[] binaryNullValue = RedisSerializer.java().serialize(NullValue.INSTANCE);

	private RedisConnectionFactory connectionFactory;
	private RedisSerializer serializer;
	private RedisCache cache;

	public RedisCacheTests(RedisConnectionFactory connectionFactory, RedisSerializer serializer) {

		this.connectionFactory = connectionFactory;
		this.serializer = serializer;
		this.binarySample = serializer.serialize(sample);
	}

	public static Collection<Object[]> testParams() {
		return CacheTestParams.connectionFactoriesAndSerializers();
	}

	@BeforeEach
	void setUp() {

		doWithConnection(RedisConnection::flushAll);

		cache = new RedisCache("cache", RedisCacheWriter.nonLockingRedisCacheWriter(connectionFactory),
				RedisCacheConfiguration.defaultCacheConfig().serializeValuesWith(SerializationPair.fromSerializer(serializer)));
	}

	@ParameterizedRedisTest // DATAREDIS-481
	void putShouldAddEntry() {

		cache.put("key-1", sample);

		doWithConnection(connection -> {
			assertThat(connection.exists(binaryCacheKey)).isTrue();
		});
	}

	@ParameterizedRedisTest // GH-2379
	void cacheShouldBeClearedByPattern() {

		cache.put(key, sample);

		final String keyPattern = "*" + key.substring(1);
		cache.clearByPattern(keyPattern);

		doWithConnection(connection -> {
			assertThat(connection.exists(binaryCacheKey)).isFalse();
		});
	}

	@ParameterizedRedisTest // GH-2379
	void cacheShouldNotBeClearedIfNoPatternMatch() {

		cache.put(key, sample);

		final String keyPattern = "*" + key.substring(1) + "tail";
		cache.clearByPattern(keyPattern);

		doWithConnection(connection -> {
			assertThat(connection.exists(binaryCacheKey)).isTrue();
		});
	}

	@ParameterizedRedisTest // DATAREDIS-481
	void putNullShouldAddEntryForNullValue() {

		cache.put("key-1", null);

		doWithConnection(connection -> {
			assertThat(connection.exists(binaryCacheKey)).isTrue();
			assertThat(connection.get(binaryCacheKey)).isEqualTo(binaryNullValue);
		});
	}

	@ParameterizedRedisTest // DATAREDIS-481
	void putIfAbsentShouldAddEntryIfNotExists() {

		cache.putIfAbsent("key-1", sample);

		doWithConnection(connection -> {
			assertThat(connection.exists(binaryCacheKey)).isTrue();
			assertThat(connection.get(binaryCacheKey)).isEqualTo(binarySample);
		});
	}

	@ParameterizedRedisTest // DATAREDIS-481
	void putIfAbsentWithNullShouldAddNullValueEntryIfNotExists() {

		assertThat(cache.putIfAbsent("key-1", null)).isNull();

		doWithConnection(connection -> {
			assertThat(connection.exists(binaryCacheKey)).isTrue();
			assertThat(connection.get(binaryCacheKey)).isEqualTo(binaryNullValue);
		});
	}

	@ParameterizedRedisTest // DATAREDIS-481
	void putIfAbsentShouldReturnExistingIfExists() {

		doWithConnection(connection -> connection.set(binaryCacheKey, binarySample));

		ValueWrapper result = cache.putIfAbsent("key-1", "this-should-not-be-set");

		assertThat(result).isNotNull();
		assertThat(result.get()).isEqualTo(sample);

		doWithConnection(connection -> {
			assertThat(connection.get(binaryCacheKey)).isEqualTo(binarySample);
		});
	}

	@ParameterizedRedisTest // DATAREDIS-481
	void putIfAbsentShouldReturnExistingNullValueIfExists() {

		doWithConnection(connection -> connection.set(binaryCacheKey, binaryNullValue));

		ValueWrapper result = cache.putIfAbsent("key-1", "this-should-not-be-set");

		assertThat(result).isNotNull();
		assertThat(result.get()).isNull();

		doWithConnection(connection -> {
			assertThat(connection.get(binaryCacheKey)).isEqualTo(binaryNullValue);
		});
	}

	@ParameterizedRedisTest // DATAREDIS-481
	void getShouldRetrieveEntry() {

		doWithConnection(connection -> {
			connection.set(binaryCacheKey, binarySample);
		});

		ValueWrapper result = cache.get(key);
		assertThat(result).isNotNull();
		assertThat(result.get()).isEqualTo(sample);
	}

	@ParameterizedRedisTest // DATAREDIS-481
	void shouldReadAndWriteSimpleCacheKey() {

		SimpleKey key = new SimpleKey("param-1", "param-2");

		cache.put(key, sample);

		ValueWrapper result = cache.get(key);
		assertThat(result).isNotNull();
		assertThat(result.get()).isEqualTo(sample);
	}

	@ParameterizedRedisTest // DATAREDIS-481
	void shouldRejectNonInvalidKey() {

		InvalidKey key = new InvalidKey(sample.getFirstame(), sample.getBirthdate());

		assertThatIllegalStateException().isThrownBy(() -> cache.put(key, sample));
	}

	@ParameterizedRedisTest // DATAREDIS-481
	void shouldAllowComplexKeyWithToStringMethod() {

		ComplexKey key = new ComplexKey(sample.getFirstame(), sample.getBirthdate());

		cache.put(key, sample);

		ValueWrapper result = cache.get(key);
		assertThat(result).isNotNull();
		assertThat(result.get()).isEqualTo(sample);
	}

	@ParameterizedRedisTest // DATAREDIS-481
	void getShouldReturnNullWhenKeyDoesNotExist() {
		assertThat(cache.get(key)).isNull();
	}

	@ParameterizedRedisTest // DATAREDIS-481
	void getShouldReturnValueWrapperHoldingNullIfNullValueStored() {

		doWithConnection(connection -> {
			connection.set(binaryCacheKey, binaryNullValue);
		});

		ValueWrapper result = cache.get(key);
		assertThat(result).isNotNull();
		assertThat(result.get()).isEqualTo(null);
	}

	@ParameterizedRedisTest // DATAREDIS-481
	void evictShouldRemoveKey() {

		doWithConnection(connection -> {
			connection.set(binaryCacheKey, binaryNullValue);
			connection.set("other".getBytes(), "value".getBytes());
		});

		cache.evict(key);

		doWithConnection(connection -> {
			assertThat(connection.exists(binaryCacheKey)).isFalse();
			assertThat(connection.exists("other".getBytes())).isTrue();
		});
	}

	@ParameterizedRedisTest // GH-2028
	void clearShouldClearCache() {

		doWithConnection(connection -> {
			connection.set(binaryCacheKey, binaryNullValue);
			connection.set("other".getBytes(), "value".getBytes());
		});

		cache.clear();

		doWithConnection(connection -> {
			assertThat(connection.exists(binaryCacheKey)).isFalse();
			assertThat(connection.exists("other".getBytes())).isTrue();
		});
	}

	@ParameterizedRedisTest // GH-1721
	void clearWithScanShouldClearCache() {

		// SCAN not supported via Jedis Cluster.
		if (connectionFactory instanceof JedisConnectionFactory) {
			assumeThat(((JedisConnectionFactory) connectionFactory).isRedisClusterAware()).isFalse();
		}

		RedisCache cache = new RedisCache("cache",
				RedisCacheWriter.nonLockingRedisCacheWriter(connectionFactory, BatchStrategies.scan(25)),
				RedisCacheConfiguration.defaultCacheConfig().serializeValuesWith(SerializationPair.fromSerializer(serializer)));

		doWithConnection(connection -> {
			connection.set(binaryCacheKey, binaryNullValue);
			connection.set("cache::foo".getBytes(), binaryNullValue);
			connection.set("other".getBytes(), "value".getBytes());
		});

		cache.clear();

		doWithConnection(connection -> {
			assertThat(connection.exists(binaryCacheKey)).isFalse();
			assertThat(connection.exists("cache::foo".getBytes())).isFalse();
			assertThat(connection.exists("other".getBytes())).isTrue();
		});
	}

	@ParameterizedRedisTest // DATAREDIS-481
	void getWithCallableShouldResolveValueIfNotPresent() {

		assertThat(cache.get(key, () -> sample)).isEqualTo(sample);

		doWithConnection(connection -> {
			assertThat(connection.exists(binaryCacheKey)).isTrue();
			assertThat(connection.get(binaryCacheKey)).isEqualTo(binarySample);
		});
	}

	@ParameterizedRedisTest // DATAREDIS-481
	void getWithCallableShouldNotResolveValueIfPresent() {

		doWithConnection(connection -> connection.set(binaryCacheKey, binaryNullValue));

		cache.get(key, () -> {
			throw new IllegalStateException("Why call the value loader when we've got a cache entry");
		});

		doWithConnection(connection -> {
			assertThat(connection.exists(binaryCacheKey)).isTrue();
			assertThat(connection.get(binaryCacheKey)).isEqualTo(binaryNullValue);
		});
	}

	@ParameterizedRedisTest // DATAREDIS-715
	void computePrefixCreatesCacheKeyCorrectly() {

		RedisCache cacheWithCustomPrefix = new RedisCache("cache",
				RedisCacheWriter.nonLockingRedisCacheWriter(connectionFactory),
				RedisCacheConfiguration.defaultCacheConfig().serializeValuesWith(SerializationPair.fromSerializer(serializer))
						.computePrefixWith(cacheName -> "_" + cacheName + "_"));

		cacheWithCustomPrefix.put("key-1", sample);

		doWithConnection(connection -> {

			assertThat(connection.stringCommands().get("_cache_key-1".getBytes(StandardCharsets.UTF_8)))
					.isEqualTo(binarySample);
		});
	}

	@ParameterizedRedisTest // DATAREDIS-1041
	void prefixCacheNameCreatesCacheKeyCorrectly() {

		RedisCache cacheWithCustomPrefix = new RedisCache("cache",
				RedisCacheWriter.nonLockingRedisCacheWriter(connectionFactory),
				RedisCacheConfiguration.defaultCacheConfig().serializeValuesWith(SerializationPair.fromSerializer(serializer))
						.prefixCacheNameWith("redis::"));

		cacheWithCustomPrefix.put("key-1", sample);

		doWithConnection(connection -> {

			assertThat(connection.stringCommands().get("redis::cache::key-1".getBytes(StandardCharsets.UTF_8)))
					.isEqualTo(binarySample);
		});
	}

	@ParameterizedRedisTest // DATAREDIS-715
	void fetchKeyWithComputedPrefixReturnsExpectedResult() {

		doWithConnection(connection -> connection.set("_cache_key-1".getBytes(StandardCharsets.UTF_8), binarySample));

		RedisCache cacheWithCustomPrefix = new RedisCache("cache",
				RedisCacheWriter.nonLockingRedisCacheWriter(connectionFactory),
				RedisCacheConfiguration.defaultCacheConfig().serializeValuesWith(SerializationPair.fromSerializer(serializer))
						.computePrefixWith(cacheName -> "_" + cacheName + "_"));

		ValueWrapper result = cacheWithCustomPrefix.get(key);

		assertThat(result).isNotNull();
		assertThat(result.get()).isEqualTo(sample);
	}

	@ParameterizedRedisTest // DATAREDIS-1032
	void cacheShouldAllowListKeyCacheKeysOfSimpleTypes() {

		Object key = SimpleKeyGenerator.generateKey(Collections.singletonList("my-cache-key-in-a-list"));
		cache.put(key, sample);

		ValueWrapper target = cache
				.get(SimpleKeyGenerator.generateKey(Collections.singletonList("my-cache-key-in-a-list")));
		assertThat(target.get()).isEqualTo(sample);
	}

	@ParameterizedRedisTest // DATAREDIS-1032
	void cacheShouldAllowArrayKeyCacheKeysOfSimpleTypes() {

		Object key = SimpleKeyGenerator.generateKey("my-cache-key-in-an-array");
		cache.put(key, sample);

		ValueWrapper target = cache.get(SimpleKeyGenerator.generateKey("my-cache-key-in-an-array"));
		assertThat(target.get()).isEqualTo(sample);
	}

	@ParameterizedRedisTest // DATAREDIS-1032
	void cacheShouldAllowListCacheKeysOfComplexTypes() {

		Object key = SimpleKeyGenerator
				.generateKey(Collections.singletonList(new ComplexKey(sample.getFirstame(), sample.getBirthdate())));
		cache.put(key, sample);

		ValueWrapper target = cache.get(SimpleKeyGenerator
				.generateKey(Collections.singletonList(new ComplexKey(sample.getFirstame(), sample.getBirthdate()))));
		assertThat(target.get()).isEqualTo(sample);
	}

	@ParameterizedRedisTest // DATAREDIS-1032
	void cacheShouldAllowMapCacheKeys() {

		Object key = SimpleKeyGenerator
				.generateKey(Collections.singletonMap("map-key", new ComplexKey(sample.getFirstame(), sample.getBirthdate())));
		cache.put(key, sample);

		ValueWrapper target = cache.get(SimpleKeyGenerator
				.generateKey(Collections.singletonMap("map-key", new ComplexKey(sample.getFirstame(), sample.getBirthdate()))));
		assertThat(target.get()).isEqualTo(sample);
	}

	@ParameterizedRedisTest // DATAREDIS-1032
	void cacheShouldFailOnNonConvertibleCacheKey() {

		Object key = SimpleKeyGenerator
				.generateKey(Collections.singletonList(new InvalidKey(sample.getFirstame(), sample.getBirthdate())));
		assertThatExceptionOfType(IllegalStateException.class).isThrownBy(() -> cache.put(key, sample));
	}

	@ParameterizedRedisTest // GH-2079
	void multipleThreadsLoadValueOnce() throws InterruptedException {

		int threadCount = 2;

		CountDownLatch prepare = new CountDownLatch(threadCount);
		CountDownLatch prepareForReturn = new CountDownLatch(1);
		CountDownLatch finished = new CountDownLatch(threadCount);
		AtomicInteger retrievals = new AtomicInteger();
		AtomicReference<byte[]> storage = new AtomicReference<>();

		cache = new RedisCache("foo", new RedisCacheWriter() {
			@Override
			public void put(String name, byte[] key, byte[] value, @Nullable Duration ttl) {
				storage.set(value);
			}

			@Override
			public byte[] get(String name, byte[] key) {

				prepare.countDown();
				try {
					prepareForReturn.await(1, TimeUnit.MINUTES);
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}

				return storage.get();
			}

			@Override
			public byte[] putIfAbsent(String name, byte[] key, byte[] value, @Nullable Duration ttl) {
				return new byte[0];
			}

			@Override
			public void remove(String name, byte[] key) {

			}

			@Override
			public void clean(String name, byte[] pattern) {

			}

			@Override
			public void clearStatistics(String name) {

			}

			@Override
			public RedisCacheWriter withStatisticsCollector(CacheStatisticsCollector cacheStatisticsCollector) {
				return null;
			}

			@Override
			public CacheStatistics getCacheStatistics(String cacheName) {
				return null;
			}
		}, RedisCacheConfiguration.defaultCacheConfig());

		ThreadPoolExecutor tpe = new ThreadPoolExecutor(threadCount, threadCount, 1, TimeUnit.MINUTES,
				new LinkedBlockingDeque<>(), new DefaultThreadFactory("RedisCacheTests"));

		IntStream.range(0, threadCount).forEach(it -> tpe.submit(() -> {
			cache.get("foo", retrievals::incrementAndGet);
			finished.countDown();
		}));

		// wait until all Threads have arrived in RedisCacheWriter.get(…)
		prepare.await();

		// let all threads continue
		prepareForReturn.countDown();

		// wait until ThreadPoolExecutor has completed.
		finished.await();
		tpe.shutdown();

		assertThat(retrievals).hasValue(1);
	}

	void doWithConnection(Consumer<RedisConnection> callback) {
        RedisConnection connection = connectionFactory.getConnection();
        try {
            callback.accept(connection);
        } finally {
            connection.close();
        }
    }

	@Data
	@NoArgsConstructor
	@AllArgsConstructor
	static class Person implements Serializable {
		String firstame;
		Date birthdate;
	}

	@RequiredArgsConstructor // toString not overridden
	static class InvalidKey implements Serializable {
		final String firstame;
		final Date birthdate;
	}

	@Data
	@RequiredArgsConstructor
	static class ComplexKey implements Serializable {
		final String firstame;
		final Date birthdate;
	}
}
