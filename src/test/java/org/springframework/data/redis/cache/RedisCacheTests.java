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

import static org.assertj.core.api.Assertions.*;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;

import org.assertj.core.api.ThrowingConsumer;
import org.awaitility.Awaitility;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.MethodSource;

import org.springframework.cache.Cache.ValueWrapper;
import org.springframework.cache.interceptor.SimpleKey;
import org.springframework.cache.interceptor.SimpleKeyGenerator;
import org.springframework.cache.support.NullValue;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceConnection;
import org.springframework.data.redis.serializer.RedisSerializationContext.SerializationPair;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.test.condition.EnabledOnCommand;
import org.springframework.data.redis.test.condition.EnabledOnRedisDriver;
import org.springframework.data.redis.test.condition.EnabledOnRedisDriver.DriverQualifier;
import org.springframework.data.redis.test.condition.RedisDriver;

/**
 * Tests for {@link RedisCache} with {@link DefaultRedisCacheWriter} using different {@link RedisSerializer} and
 * {@link RedisConnectionFactory} pairs.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Piotr Mionskowski
 * @author Jos Roseboom
 * @author John Blum
 */
@ParameterizedClass
@MethodSource("testParams")
public class RedisCacheTests {

	private String key = "key-1";
	private String cacheKey = "cache::" + key;
	private byte[] binaryCacheKey = cacheKey.getBytes(StandardCharsets.UTF_8);

	private Person sample = new Person("calmity", new Date());
	private byte[] binarySample;

	private byte[] binaryNullValue = RedisSerializer.java().serialize(NullValue.INSTANCE);

	private final @DriverQualifier RedisConnectionFactory connectionFactory;
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

		this.cache = new RedisCache("cache", usingRedisCacheWriter(), usingRedisCacheConfiguration());
	}

	@Test // DATAREDIS-481
	void putShouldAddEntry() {

		cache.put("key-1", sample);

		doWithConnection(connection -> assertThat(connection.exists(binaryCacheKey)).isTrue());
	}

	@Test // GH-2379
	void cacheShouldBeClearedByPattern() {

		cache.put(key, sample);

		String keyPattern = "*" + key.substring(1);
		cache.clear(keyPattern);

		maybeWaitForNonBlockingClean();

		doWithConnection(connection -> assertThat(connection.exists(binaryCacheKey)).isFalse());
	}

	@Test // GH-2379
	void cacheShouldNotBeClearedIfNoPatternMatch() {

		cache.put(key, sample);

		String keyPattern = "*" + key.substring(1) + "tail";
		cache.clear(keyPattern);

		maybeWaitForNonBlockingClean();

		doWithConnection(connection -> assertThat(connection.exists(binaryCacheKey)).isTrue());
	}

	@Test
	// DATAREDIS-481
	void putNullShouldAddEntryForNullValue() {

		cache.put("key-1", null);

		doWithConnection(connection -> {
			assertThat(connection.exists(binaryCacheKey)).isTrue();
			assertThat(connection.get(binaryCacheKey)).isEqualTo(binaryNullValue);
		});
	}

	@Test // DATAREDIS-481
	void putIfAbsentShouldAddEntryIfNotExists() {

		cache.putIfAbsent("key-1", sample);

		doWithConnection(connection -> {
			assertThat(connection.exists(binaryCacheKey)).isTrue();
			assertThat(connection.get(binaryCacheKey)).isEqualTo(binarySample);
		});
	}

	@Test // DATAREDIS-481
	void putIfAbsentWithNullShouldAddNullValueEntryIfNotExists() {

		assertThat(cache.putIfAbsent("key-1", null)).isNull();

		doWithConnection(connection -> {
			assertThat(connection.exists(binaryCacheKey)).isTrue();
			assertThat(connection.get(binaryCacheKey)).isEqualTo(binaryNullValue);
		});
	}

	@Test // DATAREDIS-481
	void putIfAbsentShouldReturnExistingIfExists() {

		doWithConnection(connection -> connection.set(binaryCacheKey, binarySample));

		ValueWrapper result = cache.putIfAbsent("key-1", "this-should-not-be-set");

		assertThat(result).isNotNull();
		assertThat(result.get()).isEqualTo(sample);

		doWithConnection(connection -> assertThat(connection.get(binaryCacheKey)).isEqualTo(binarySample));
	}

	@Test // DATAREDIS-481
	void putIfAbsentShouldReturnExistingNullValueIfExists() {

		doWithConnection(connection -> connection.set(binaryCacheKey, binaryNullValue));

		ValueWrapper result = cache.putIfAbsent("key-1", "this-should-not-be-set");

		assertThat(result).isNotNull();
		assertThat(result.get()).isNull();

		doWithConnection(connection -> assertThat(connection.get(binaryCacheKey)).isEqualTo(binaryNullValue));
	}

	@Test // DATAREDIS-481
	void getShouldRetrieveEntry() {

		doWithConnection(connection -> connection.set(binaryCacheKey, binarySample));

		ValueWrapper result = cache.get(key);
		assertThat(result).isNotNull();
		assertThat(result.get()).isEqualTo(sample);
	}

	@Test // DATAREDIS-481
	void shouldReadAndWriteSimpleCacheKey() {

		SimpleKey key = new SimpleKey("param-1", "param-2");

		cache.put(key, sample);

		ValueWrapper result = cache.get(key);
		assertThat(result).isNotNull();
		assertThat(result.get()).isEqualTo(sample);
	}

	@Test // DATAREDIS-481
	void shouldRejectNonInvalidKey() {

		InvalidKey key = new InvalidKey(sample.getFirstname(), sample.getBirthdate());

		assertThatIllegalStateException().isThrownBy(() -> cache.put(key, sample));
	}

	@Test // DATAREDIS-481
	void shouldAllowComplexKeyWithToStringMethod() {

		ComplexKey key = new ComplexKey(sample.getFirstname(), sample.getBirthdate());

		cache.put(key, sample);

		ValueWrapper result = cache.get(key);

		assertThat(result).isNotNull();
		assertThat(result.get()).isEqualTo(sample);
	}

	@Test // DATAREDIS-481
	void getShouldReturnNullWhenKeyDoesNotExist() {
		assertThat(cache.get(key)).isNull();
	}

	@Test // DATAREDIS-481
	void getShouldReturnValueWrapperHoldingNullIfNullValueStored() {

		doWithConnection(connection -> connection.set(binaryCacheKey, binaryNullValue));

		ValueWrapper result = cache.get(key);

		assertThat(result).isNotNull();
		assertThat(result.get()).isEqualTo(null);
	}

	@Test // GH-2890
	void getWithValueLoaderShouldStoreNull() {

		doWithConnection(connection -> connection.set(binaryCacheKey, binaryNullValue));

		Object result = cache.get(key, () -> {
			throw new IllegalStateException();
		});

		assertThat(result).isNull();
	}

	@Test // GH-2890
	void getWithValueLoaderShouldRetrieveValue() {

		AtomicLong counter = new AtomicLong();
		Object result = cache.get(key, () -> {
			counter.incrementAndGet();
			return sample;
		});

		assertThat(result).isEqualTo(sample);
		result = cache.get(key, () -> {
			counter.incrementAndGet();
			return sample;
		});

		assertThat(result).isEqualTo(sample);
		assertThat(counter).hasValue(1);
	}

	@Test // DATAREDIS-481
	void evictShouldRemoveKey() {

		doWithConnection(connection -> {
			connection.set(binaryCacheKey, binaryNullValue);
			connection.set("other".getBytes(), "value".getBytes());
		});

		cache.evict(key);

		Awaitility.await().until(() -> cache.get(key) == null);

		doWithConnection(connection -> {
			assertThat(connection.exists(binaryCacheKey)).isFalse();
			assertThat(connection.exists("other".getBytes())).isTrue();
		});
	}

	@Test // GH-3236
	void evictShouldRemoveKeyIfPresent() {

		doWithConnection(connection -> {
			connection.set(binaryCacheKey, binaryNullValue);
			connection.set("other".getBytes(), "value".getBytes());
		});

		assertThat(cache.evictIfPresent(key)).isTrue();
		assertThat(cache.evictIfPresent(key)).isFalse();

		doWithConnection(connection -> {
			assertThat(connection.exists(binaryCacheKey)).isFalse();
			assertThat(connection.exists("other".getBytes())).isTrue();
		});
	}

	@Test // GH-2028
	void clearShouldClearCache() {

		doWithConnection(connection -> {
			connection.set(binaryCacheKey, binaryNullValue);
			connection.set("other".getBytes(), "value".getBytes());
		});

		cache.clear();

		maybeWaitForNonBlockingClean();

		doWithConnection(connection -> {
			assertThat(connection.exists(binaryCacheKey)).isFalse();
			assertThat(connection.exists("other".getBytes())).isTrue();
		});
	}

	@Test // GH-2028
	void clearShouldInvalidateCache() {

		doWithConnection(connection -> {
			connection.set(binaryCacheKey, binaryNullValue);
			connection.set("other".getBytes(), "value".getBytes());
		});

		assertThat(cache.invalidate()).isTrue();

		doWithConnection(connection -> {
			assertThat(connection.exists(binaryCacheKey)).isFalse();
			assertThat(connection.exists("other".getBytes())).isTrue();
		});
	}

	@Test // GH-1721
	@EnabledOnRedisDriver(RedisDriver.LETTUCE) // SCAN not supported via Jedis Cluster.
	void clearWithScanShouldClearCache() {

		RedisCache cache = new RedisCache("cache",
				RedisCacheWriter.nonLockingRedisCacheWriter(connectionFactory, BatchStrategies.scan(25)),
				RedisCacheConfiguration.defaultCacheConfig().serializeValuesWith(SerializationPair.fromSerializer(serializer)));

		doWithConnection(connection -> {
			connection.set(binaryCacheKey, binaryNullValue);
			connection.set("cache::foo".getBytes(), binaryNullValue);
			connection.set("other".getBytes(), "value".getBytes());
		});

		cache.clear();

		maybeWaitForNonBlockingClean();

		doWithConnection(connection -> {
			assertThat(connection.exists(binaryCacheKey)).isFalse();
			assertThat(connection.exists("cache::foo".getBytes())).isFalse();
			assertThat(connection.exists("other".getBytes())).isTrue();
		});
	}

	@Test // DATAREDIS-481
	void getWithCallableShouldResolveValueIfNotPresent() {

		assertThat(cache.get(key, () -> sample)).isEqualTo(sample);

		doWithConnection(connection -> {
			assertThat(connection.exists(binaryCacheKey)).isTrue();
			assertThat(connection.get(binaryCacheKey)).isEqualTo(binarySample);
		});
	}

	@Test // DATAREDIS-481
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

	@Test // DATAREDIS-715
	void computePrefixCreatesCacheKeyCorrectly() {

		RedisCache cacheWithCustomPrefix = new RedisCache("cache",
				RedisCacheWriter.nonLockingRedisCacheWriter(connectionFactory),
				RedisCacheConfiguration.defaultCacheConfig().serializeValuesWith(SerializationPair.fromSerializer(serializer))
						.computePrefixWith(cacheName -> "_" + cacheName + "_"));

		cacheWithCustomPrefix.put("key-1", sample);

		doWithConnection(
				connection -> assertThat(connection.stringCommands().get("_cache_key-1".getBytes(StandardCharsets.UTF_8)))
						.isEqualTo(binarySample));
	}

	@Test // DATAREDIS-1041
	void prefixCacheNameCreatesCacheKeyCorrectly() {

		RedisCache cacheWithCustomPrefix = new RedisCache("cache",
				RedisCacheWriter.nonLockingRedisCacheWriter(connectionFactory), RedisCacheConfiguration.defaultCacheConfig()
						.serializeValuesWith(SerializationPair.fromSerializer(serializer)).prefixCacheNameWith("redis::"));

		cacheWithCustomPrefix.put("key-1", sample);

		doWithConnection(connection -> assertThat(
				connection.stringCommands().get("redis::cache::key-1".getBytes(StandardCharsets.UTF_8)))
				.isEqualTo(binarySample));
	}

	@Test // DATAREDIS-715
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

	@Test // DATAREDIS-1032
	void cacheShouldAllowListKeyCacheKeysOfSimpleTypes() {

		Object key = SimpleKeyGenerator.generateKey(Collections.singletonList("my-cache-key-in-a-list"));
		cache.put(key, sample);

		ValueWrapper target = cache
				.get(SimpleKeyGenerator.generateKey(Collections.singletonList("my-cache-key-in-a-list")));

		assertThat(target.get()).isEqualTo(sample);
	}

	@Test // DATAREDIS-1032
	void cacheShouldAllowArrayKeyCacheKeysOfSimpleTypes() {

		Object key = SimpleKeyGenerator.generateKey("my-cache-key-in-an-array");
		cache.put(key, sample);

		ValueWrapper target = cache.get(SimpleKeyGenerator.generateKey("my-cache-key-in-an-array"));

		assertThat(target.get()).isEqualTo(sample);
	}

	@Test // DATAREDIS-1032
	void cacheShouldAllowListCacheKeysOfComplexTypes() {

		Object key = SimpleKeyGenerator
				.generateKey(Collections.singletonList(new ComplexKey(sample.getFirstname(), sample.getBirthdate())));
		cache.put(key, sample);

		ValueWrapper target = cache.get(SimpleKeyGenerator
				.generateKey(Collections.singletonList(new ComplexKey(sample.getFirstname(), sample.getBirthdate()))));

		assertThat(target.get()).isEqualTo(sample);
	}

	@Test // DATAREDIS-1032
	void cacheShouldAllowMapCacheKeys() {

		Object key = SimpleKeyGenerator
				.generateKey(Collections.singletonMap("map-key", new ComplexKey(sample.getFirstname(), sample.getBirthdate())));
		cache.put(key, sample);

		ValueWrapper target = cache.get(SimpleKeyGenerator.generateKey(
				Collections.singletonMap("map-key", new ComplexKey(sample.getFirstname(), sample.getBirthdate()))));

		assertThat(target.get()).isEqualTo(sample);
	}

	@Test // DATAREDIS-1032
	void cacheShouldFailOnNonConvertibleCacheKey() {

		Object key = SimpleKeyGenerator
				.generateKey(Collections.singletonList(new InvalidKey(sample.getFirstname(), sample.getBirthdate())));

		assertThatIllegalStateException().isThrownBy(() -> cache.put(key, sample));
	}

	@EnabledOnCommand("GETEX")
	@Test // GH-2351
	void cacheGetWithTimeToIdleExpirationWhenEntryNotExpiredShouldReturnValue() {

		doWithConnection(connection -> connection.stringCommands().set(this.binaryCacheKey, this.binarySample));

		RedisCache cache = new RedisCache("cache", usingRedisCacheWriter(),
				usingRedisCacheConfiguration(withTtiExpiration()));

		assertThat(unwrap(cache.get(this.key))).isEqualTo(this.sample);

		doWithConnection(connection -> {

			assertThat(connection.keyCommands().ttl(this.binaryCacheKey)).isGreaterThan(1);
		});
	}

	@EnabledOnCommand("GETEX")
	@Test // GH-2351
	void cacheGetWithTimeToIdleExpirationAfterEntryExpiresShouldReturnNull() {

		doWithConnection(connection -> connection.stringCommands().set(this.binaryCacheKey, this.binarySample));

		RedisCache cache = new RedisCache("cache", usingRedisCacheWriter(),
				usingRedisCacheConfiguration(withTtiExpiration()));

		assertThat(unwrap(cache.get(this.key))).isEqualTo(this.sample);

		doWithConnection(connection -> {
			assertThat(connection.keyCommands().ttl(this.binaryCacheKey)).isGreaterThan(1);
		});
	}

	@Test // GH-2650
	@EnabledOnRedisDriver(RedisDriver.JEDIS)
	void retrieveCacheValueUsingJedis() {

		assertThatExceptionOfType(UnsupportedOperationException.class)
				.isThrownBy(() -> this.cache.retrieve(this.binaryCacheKey)).withMessageContaining("RedisCache");
	}

	@Test // GH-2650
	@EnabledOnRedisDriver(RedisDriver.JEDIS)
	void retrieveLoadedValueUsingJedis() {

		assertThatExceptionOfType(UnsupportedOperationException.class)
				.isThrownBy(() -> this.cache.retrieve(this.binaryCacheKey, () -> usingCompletedFuture("TEST")))
				.withMessageContaining("RedisCache");
	}

	@Test // GH-2650
	@EnabledOnRedisDriver(RedisDriver.LETTUCE)
	void retrieveReturnsCachedValue() throws Exception {

		doWithConnection(connection -> connection.stringCommands().set(this.binaryCacheKey, this.binarySample));

		RedisCache cache = new RedisCache("cache", usingLockingRedisCacheWriter(),
				usingRedisCacheConfiguration().disableCachingNullValues());

		CompletableFuture<ValueWrapper> value = cache.retrieve(this.key);

		assertThat(value).isNotNull();
		assertThat(value.get(5, TimeUnit.SECONDS)).isNotNull();
		assertThat(value.get().get()).isEqualTo(this.sample);
		assertThat(value).isDone();

		doWithConnection(connection -> {
			assertThat(connection.keyCommands().ttl(this.binaryCacheKey)).isEqualTo(-1);
		});
	}

	@Test // GH-2890
	@EnabledOnRedisDriver(RedisDriver.LETTUCE)
	void retrieveAppliesTimeToIdle() throws ExecutionException, InterruptedException {

		doWithConnection(connection -> connection.stringCommands().set(this.binaryCacheKey, this.binarySample));

		RedisCache cache = new RedisCache("cache", usingRedisCacheWriter(),
				usingRedisCacheConfiguration(withTtiExpiration()));

		CompletableFuture<ValueWrapper> value = cache.retrieve(this.key);

		assertThat(value).isNotNull();
		assertThat(value.get().get()).isEqualTo(this.sample);
		assertThat(value).isDone();

		doWithConnection(connection -> {
			assertThat(connection.keyCommands().ttl(this.binaryCacheKey)).isGreaterThan(1);
		});
	}

	@Test // GH-2650
	@EnabledOnRedisDriver(RedisDriver.LETTUCE)
	void retrieveReturnsCachedNullableValue() throws Exception {

		doWithConnection(connection -> connection.stringCommands().set(this.binaryCacheKey, this.binarySample));

		RedisCache cache = new RedisCache("cache", usingLockingRedisCacheWriter(), usingRedisCacheConfiguration());

		CompletableFuture<ValueWrapper> value = cache.retrieve(this.key);

		assertThat(value).isNotNull();
		assertThat(value.get().get()).isEqualTo(this.sample);
		assertThat(value).isDone();
	}

	@Test // GH-2783
	@EnabledOnRedisDriver(RedisDriver.LETTUCE)
	void retrieveReturnsCachedNullValue() throws Exception {

		doWithConnection(connection -> connection.set(binaryCacheKey, binaryNullValue));

		CompletableFuture<ValueWrapper> value = (CompletableFuture<ValueWrapper>) cache.retrieve(this.key);
		ValueWrapper wrapper = value.get(5, TimeUnit.SECONDS);

		assertThat(wrapper).isNotNull();
		assertThat(wrapper.get()).isNull();
	}

	@Test // GH-2650
	@EnabledOnRedisDriver(RedisDriver.LETTUCE)
	void retrieveReturnsCachedValueWhenLockIsReleased() throws Exception {

		String testValue = "TestValue";

		byte[] binaryCacheValue = this.serializer.serialize(testValue);

		doWithConnection(connection -> connection.stringCommands().set(this.binaryCacheKey, binaryCacheValue));

		RedisCache cache = new RedisCache("cache", usingLockingRedisCacheWriter(Duration.ofMillis(5L)),
				usingRedisCacheConfiguration());

		DefaultRedisCacheWriter cacheWriter = (DefaultRedisCacheWriter) cache.getCacheWriter();

		cacheWriter.lock("cache");

		CompletableFuture<ValueWrapper> value = cache.retrieve(this.key);
		assertThat(value).isNotDone();

		cacheWriter.unlock("cache");

		assertThat(value.get(15L, TimeUnit.MILLISECONDS).get()).isEqualTo(testValue);
		assertThat(value).isDone();
	}

	@Test // GH-2650
	@EnabledOnRedisDriver(RedisDriver.LETTUCE)
	void retrieveReturnsLoadedValue() throws Exception {

		AtomicBoolean loaded = new AtomicBoolean(false);
		Person jon = new Person("Jon", Date.from(Instant.now()));
		CompletableFuture<Person> valueLoader = CompletableFuture.completedFuture(jon);

		RedisCache cache = new RedisCache("cache", usingLockingRedisCacheWriter(), usingRedisCacheConfiguration());

		Supplier<CompletableFuture<Person>> valueLoaderSupplier = () -> {
			loaded.set(true);
			return valueLoader;
		};

		CompletableFuture<Person> value = cache.retrieve(this.key, valueLoaderSupplier);

		assertThat(value.get()).isEqualTo(jon);
		assertThat(loaded.get()).isTrue();
		assertThat(value).isDone();
	}

	@Test // GH-2650
	@EnabledOnRedisDriver(RedisDriver.LETTUCE)
	void retrieveStoresLoadedValue() throws Exception {

		Person jon = new Person("Jon", Date.from(Instant.now()));
		Supplier<CompletableFuture<Person>> valueLoaderSupplier = () -> CompletableFuture.completedFuture(jon);

		RedisCache cache = new RedisCache("cache", usingLockingRedisCacheWriter(), usingRedisCacheConfiguration());

		cache.retrieve(this.key, valueLoaderSupplier).get();

		doWithConnection(
				connection -> assertThat(connection.keyCommands().exists("cache::key-1".getBytes(StandardCharsets.UTF_8)))
						.isTrue());
	}

	@Test // GH-2650
	@EnabledOnRedisDriver(RedisDriver.LETTUCE)
	void retrieveReturnsNull() throws Exception {

		doWithConnection(connection -> connection.stringCommands().set(this.binaryCacheKey, this.binaryNullValue));

		RedisCache cache = new RedisCache("cache", usingLockingRedisCacheWriter(), usingRedisCacheConfiguration());

		CompletableFuture<ValueWrapper> value = cache.retrieve(this.key);

		assertThat(value).isNotNull();
		assertThat(value.get(5, TimeUnit.SECONDS).get()).isNull();
		assertThat(value).isDone();

		doWithConnection(connection -> connection.keyCommands().del(this.binaryCacheKey));

		value = cache.retrieve(this.key);

		assertThat(value).isNotNull();
		assertThat(value.get(5, TimeUnit.SECONDS)).isNull();
	}

	private <T> CompletableFuture<T> usingCompletedFuture(T value) {
		return CompletableFuture.completedFuture(value);
	}

	private RedisCacheConfiguration usingRedisCacheConfiguration() {
		return usingRedisCacheConfiguration(Function.identity());
	}

	private RedisCacheConfiguration usingRedisCacheConfiguration(
			Function<RedisCacheConfiguration, RedisCacheConfiguration> customizer) {

		return customizer.apply(RedisCacheConfiguration.defaultCacheConfig()
				.serializeValuesWith(SerializationPair.fromSerializer(this.serializer)));
	}

	private RedisCacheWriter usingRedisCacheWriter() {
		return usingNonLockingRedisCacheWriter();
	}

	private RedisCacheWriter usingLockingRedisCacheWriter() {
		return RedisCacheWriter.lockingRedisCacheWriter(this.connectionFactory);
	}

	private RedisCacheWriter usingLockingRedisCacheWriter(Duration sleepTime) {
		return RedisCacheWriter.lockingRedisCacheWriter(this.connectionFactory, sleepTime,
				RedisCacheWriter.TtlFunction.persistent(), BatchStrategies.keys());
	}

	private RedisCacheWriter usingNonLockingRedisCacheWriter() {
		return RedisCacheWriter.nonLockingRedisCacheWriter(this.connectionFactory);
	}

	private @Nullable Object unwrap(@Nullable Object value) {
		return value instanceof ValueWrapper wrapper ? wrapper.get() : value;
	}

	private Function<RedisCacheConfiguration, RedisCacheConfiguration> withTtiExpiration() {

		Function<RedisCacheConfiguration, RedisCacheConfiguration> entryTtlFunction = cacheConfiguration -> cacheConfiguration
				.entryTtl(Duration.ofSeconds(10));

		return entryTtlFunction.andThen(RedisCacheConfiguration::enableTimeToIdle);
	}

	void doWithConnection(ThrowingConsumer<RedisConnection> callback) {
		try (RedisConnection connection = connectionFactory.getConnection()) {
			callback.accept(connection);
		}
	}

	private void maybeWaitForNonBlockingClean() {
		doWithConnection(redisConnection -> {
			// Cache interface specifies non-blocking behavior, so we've need to wait for Lettuce async to avoid races between
			// the process and our assertion here.
			if (redisConnection instanceof LettuceConnection) {
				Thread.sleep(250);
			}
		});
	}

	static class Person implements Serializable {

		private String firstname;
		private Date birthdate;

		public Person() {}

		public Person(String firstname, Date birthdate) {
			this.firstname = firstname;
			this.birthdate = birthdate;
		}

		public String getFirstname() {
			return this.firstname;
		}

		public void setFirstname(String firstname) {
			this.firstname = firstname;
		}

		public Date getBirthdate() {
			return this.birthdate;
		}

		public void setBirthdate(Date birthdate) {
			this.birthdate = birthdate;
		}

		@Override
		public boolean equals(Object obj) {

			if (this == obj) {
				return true;
			}

			if (!(obj instanceof Person that)) {
				return false;
			}

			return Objects.equals(this.getFirstname(), that.getFirstname())
					&& Objects.equals(this.getBirthdate(), that.getBirthdate());
		}

		@Override
		public int hashCode() {
			return Objects.hash(getFirstname(), getBirthdate());
		}

		@Override
		public String toString() {
			return "RedisCacheTests.Person(firstname=" + this.getFirstname() + ", birthdate=" + this.getBirthdate() + ")";
		}
	}

	// toString not overridden
	static class InvalidKey implements Serializable {

		final String firstname;
		final Date birthdate;

		public InvalidKey(String firstname, Date birthdate) {
			this.firstname = firstname;
			this.birthdate = birthdate;
		}
	}

	static class ComplexKey implements Serializable {

		final String firstname;
		final Date birthdate;

		public ComplexKey(String firstname, Date birthdate) {
			this.firstname = firstname;
			this.birthdate = birthdate;
		}

		public String getFirstname() {
			return this.firstname;
		}

		public Date getBirthdate() {
			return this.birthdate;
		}

		@Override
		public boolean equals(final Object obj) {

			if (this == obj) {
				return true;
			}

			if (!(obj instanceof ComplexKey that)) {
				return false;
			}

			return Objects.equals(this.getFirstname(), that.getFirstname())
					&& Objects.equals(this.getBirthdate(), that.getBirthdate());
		}

		@Override
		public int hashCode() {
			return Objects.hash(getFirstname(), getBirthdate());
		}

		@Override
		public String toString() {
			return "RedisCacheTests.ComplexKey(firstame=" + this.getFirstname() + ", birthdate=" + this.getBirthdate() + ")";
		}
	}
}
