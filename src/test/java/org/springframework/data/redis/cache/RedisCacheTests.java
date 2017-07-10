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

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.Date;
import java.util.function.Consumer;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.springframework.cache.Cache.ValueWrapper;
import org.springframework.cache.interceptor.SimpleKey;
import org.springframework.cache.support.NullValue;
import org.springframework.data.redis.ConnectionFactoryTracker;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.serializer.JdkSerializationRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializationContext.SerializationPair;
import org.springframework.data.redis.serializer.RedisSerializer;

/**
 * Tests for {@link RedisCache} with {@link DefaultRedisCacheWriter} using different {@link RedisSerializer} and
 * {@link RedisConnectionFactory} pairs.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@RunWith(Parameterized.class)
public class RedisCacheTests {

	String key = "key-1";
	String cacheKey = "cache::" + key;
	byte[] binaryCacheKey = cacheKey.getBytes(Charset.forName("UTF-8"));

	Person sample = new Person("calmity", new Date());
	byte[] binarySample;

	byte[] binaryNullValue = new JdkSerializationRedisSerializer().serialize(NullValue.INSTANCE);

	RedisConnectionFactory connectionFactory;
	RedisSerializer serializer;
	RedisCache cache;

	public RedisCacheTests(RedisConnectionFactory connectionFactory, RedisSerializer serializer) {

		this.connectionFactory = connectionFactory;
		this.serializer = serializer;
		this.binarySample = serializer.serialize(sample);

		ConnectionFactoryTracker.add(connectionFactory);
	}

	@Parameters(name = "{index}: {0} & {1}")
	public static Collection<Object[]> testParams() {
		return CacheTestParams.connectionFactoriesAndSerializers();
	}

	@AfterClass
	public static void cleanUpResources() {
		ConnectionFactoryTracker.cleanUp();
	}

	@Before
	public void setUp() {

		doWithConnection(RedisConnection::flushAll);

		cache = new RedisCache("cache", new DefaultRedisCacheWriter(connectionFactory),
				RedisCacheConfiguration.defaultCacheConfig().serializeValuesWith(SerializationPair.fromSerializer(serializer)));
	}

	@Test // DATAREDIS-481
	public void putShouldAddEntry() {

		cache.put("key-1", sample);

		doWithConnection(connection -> {
			assertThat(connection.exists(binaryCacheKey)).isTrue();
		});
	}

	@Test // DATAREDIS-481
	public void putNullShouldAddEntryForNullValue() {

		cache.put("key-1", null);

		doWithConnection(connection -> {
			assertThat(connection.exists(binaryCacheKey)).isTrue();
			assertThat(connection.get(binaryCacheKey)).isEqualTo(binaryNullValue);
		});
	}

	@Test // DATAREDIS-481
	public void putIfAbsentShouldAddEntryIfNotExists() {

		cache.putIfAbsent("key-1", sample);

		doWithConnection(connection -> {
			assertThat(connection.exists(binaryCacheKey)).isTrue();
			assertThat(connection.get(binaryCacheKey)).isEqualTo(binarySample);
		});
	}

	@Test // DATAREDIS-481
	public void putIfAbsentWithNullShouldAddNullValueEntryIfNotExists() {

		assertThat(cache.putIfAbsent("key-1", null)).isNull();

		doWithConnection(connection -> {
			assertThat(connection.exists(binaryCacheKey)).isTrue();
			assertThat(connection.get(binaryCacheKey)).isEqualTo(binaryNullValue);
		});
	}

	@Test // DATAREDIS-481
	public void putIfAbsentShouldReturnExistingIfExists() {

		doWithConnection(connection -> connection.set(binaryCacheKey, binarySample));

		ValueWrapper result = cache.putIfAbsent("key-1", "this-should-not-be-set");

		assertThat(result).isNotNull();
		assertThat(result.get()).isEqualTo(sample);

		doWithConnection(connection -> {
			assertThat(connection.get(binaryCacheKey)).isEqualTo(binarySample);
		});
	}

	@Test // DATAREDIS-481
	public void putIfAbsentShouldReturnExistingNullValueIfExists() {

		doWithConnection(connection -> connection.set(binaryCacheKey, binaryNullValue));

		ValueWrapper result = cache.putIfAbsent("key-1", "this-should-not-be-set");

		assertThat(result).isNotNull();
		assertThat(result.get()).isNull();

		doWithConnection(connection -> {
			assertThat(connection.get(binaryCacheKey)).isEqualTo(binaryNullValue);
		});
	}

	@Test // DATAREDIS-481
	public void getShouldRetrieveEntry() {

		doWithConnection(connection -> {
			connection.set(binaryCacheKey, binarySample);
		});

		ValueWrapper result = cache.get(key);
		assertThat(result).isNotNull();
		assertThat(result.get()).isEqualTo(sample);
	}

	@Test // DATAREDIS-481
	public void shouldReadAndWriteSimpleCacheKey() {

		SimpleKey key = new SimpleKey("param-1", "param-2");

		cache.put(key, sample);

		ValueWrapper result = cache.get(key);
		assertThat(result).isNotNull();
		assertThat(result.get()).isEqualTo(sample);
	}

	@Test(expected = IllegalStateException.class) // DATAREDIS-481
	public void shouldRejectNonInvalidKey() {

		InvalidKey key = new InvalidKey(sample.getFirstame(), sample.getBirthdate());

		cache.put(key, sample);
	}

	@Test // DATAREDIS-481
	public void shouldAllowComplexKeyWithToStringMethod() {

		ComplexKey key = new ComplexKey(sample.getFirstame(), sample.getBirthdate());

		cache.put(key, sample);

		ValueWrapper result = cache.get(key);
		assertThat(result).isNotNull();
		assertThat(result.get()).isEqualTo(sample);
	}

	@Test // DATAREDIS-481
	public void getShouldReturnNullWhenKeyDoesNotExist() {
		assertThat(cache.get(key)).isNull();
	}

	@Test // DATAREDIS-481
	public void getShouldReturnValueWrapperHoldingNullIfNullValueStored() {

		doWithConnection(connection -> {
			connection.set(binaryCacheKey, binaryNullValue);
		});

		ValueWrapper result = cache.get(key);
		assertThat(result).isNotNull();
		assertThat(result.get()).isEqualTo(null);
	}

	@Test // DATAREDIS-481
	public void evictShouldRemoveKey() {

		doWithConnection(connection -> {
			connection.set(binaryCacheKey, binaryNullValue);
		});

		cache.evict(key);

		doWithConnection(connection -> {
			assertThat(connection.exists(binaryCacheKey)).isFalse();
		});
	}

	@Test // DATAREDIS-481
	public void getWithCallableShouldResolveValueIfNotPresent() {

		assertThat(cache.get(key, () -> sample)).isEqualTo(sample);

		doWithConnection(connection -> {
			assertThat(connection.exists(binaryCacheKey)).isTrue();
			assertThat(connection.get(binaryCacheKey)).isEqualTo(binarySample);
		});
	}

	@Test // DATAREDIS-481
	public void getWithCallableShouldNotResolveValueIfPresent() {

		doWithConnection(connection -> connection.set(binaryCacheKey, binaryNullValue));

		cache.get(key, () -> {
			throw new IllegalStateException("Why call the value loader when we've got a cache entry?");
		});

		doWithConnection(connection -> {
			assertThat(connection.exists(binaryCacheKey)).isTrue();
			assertThat(connection.get(binaryCacheKey)).isEqualTo(binaryNullValue);
		});
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
