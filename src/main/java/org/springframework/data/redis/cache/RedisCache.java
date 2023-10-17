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

import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringJoiner;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import org.springframework.cache.Cache;
import org.springframework.cache.support.AbstractValueAdaptingCache;
import org.springframework.cache.support.NullValue;
import org.springframework.cache.support.SimpleValueWrapper;
import org.springframework.core.convert.ConversionFailedException;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.TypeDescriptor;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.util.ByteUtils;
import org.springframework.data.redis.util.RedisAssertions;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;
import org.springframework.util.ReflectionUtils;

/**
 * {@link AbstractValueAdaptingCache Cache} implementation using Redis as the underlying store for cache data.
 * <p>
 * Use {@link RedisCacheManager} to create {@link RedisCache} instances.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Piotr Mionskowski
 * @author Jos Roseboom
 * @author John Blum
 * @since 2.0
 */
@SuppressWarnings("unused")
public class RedisCache extends AbstractValueAdaptingCache {

	static final byte[] BINARY_NULL_VALUE = RedisSerializer.java().serialize(NullValue.INSTANCE);

	static final String CACHE_RETRIEVAL_UNSUPPORTED_OPERATION_EXCEPTION_MESSAGE =
			"The Redis driver configured with RedisCache through RedisCacheWriter does not support CompletableFuture-based retrieval";

	private final Lock lock = new ReentrantLock();

	private final RedisCacheConfiguration cacheConfiguration;

	private final RedisCacheWriter cacheWriter;

	private final String name;

	/**
	 * Create a new {@link RedisCache} with the given {@link String name} and {@link RedisCacheConfiguration},
	 * using the {@link RedisCacheWriter} to execute Redis commands supporting the cache operations.
	 *
	 * @param name {@link String name} for this {@link Cache}; must not be {@literal null}.
	 * @param cacheWriter {@link RedisCacheWriter} used to perform {@link RedisCache} operations
	 * by executing the necessary Redis commands; must not be {@literal null}.
	 * @param cacheConfiguration {@link RedisCacheConfiguration} applied to this {@link RedisCache} on creation;
	 * must not be {@literal null}.
	 * @throws IllegalArgumentException if either the given {@link RedisCacheWriter} or {@link RedisCacheConfiguration}
	 * are {@literal null} or the given {@link String} name for this {@link RedisCache} is {@literal null}.
	 */
	protected RedisCache(String name, RedisCacheWriter cacheWriter, RedisCacheConfiguration cacheConfiguration) {

		super(RedisAssertions.requireNonNull(cacheConfiguration, "CacheConfiguration must not be null")
				.getAllowCacheNullValues());

		Assert.notNull(name, "Name must not be null");
		Assert.notNull(cacheWriter, "CacheWriter must not be null");

		this.name = name;
		this.cacheWriter = cacheWriter;
		this.cacheConfiguration = cacheConfiguration;
	}

	/**
	 * Get the {@link RedisCacheConfiguration} used to configure this {@link RedisCache} on initialization.
	 *
	 * @return an immutable {@link RedisCacheConfiguration} used to configure this {@link RedisCache} on initialization.
	 */
	public RedisCacheConfiguration getCacheConfiguration() {
		return this.cacheConfiguration;
	}

	/**
	 * Gets the configured {@link RedisCacheWriter} used to adapt Redis for cache operations.
	 *
	 * @return the configured {@link RedisCacheWriter} used to adapt Redis for cache operations.
	 */
	protected RedisCacheWriter getCacheWriter() {
		return this.cacheWriter;
	}

	/**
	 * Gets the configured {@link ConversionService} used to convert {@link Object cache keys} to a {@link String}
	 * when accessing entries in the cache.
	 *
	 * @return the configured {@link ConversionService} used to convert {@link Object cache keys} to a {@link String}
	 * when accessing entries in the cache.
	 * @see RedisCacheConfiguration#getConversionService()
	 * @see #getCacheConfiguration()
	 */
	protected ConversionService getConversionService() {
		return getCacheConfiguration().getConversionService();
	}

	@Override
	public String getName() {
		return this.name;
	}

	@Override
	public RedisCacheWriter getNativeCache() {
		return getCacheWriter();
	}

	/**
	 * Return the {@link CacheStatistics} snapshot for this cache instance.
	 * <p>
	 * Statistics are accumulated per cache instance and not from the backing Redis data store.
	 *
	 * @return {@link CacheStatistics} object for this {@link RedisCache}.
	 * @since 2.4
	 */
	public CacheStatistics getStatistics() {
		return getCacheWriter().getCacheStatistics(getName());
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T> T get(Object key, Callable<T> valueLoader) {

		ValueWrapper result = get(key);

		return result != null ? (T) result.get() : getSynchronized(key, valueLoader);
	}

	@Nullable
	@SuppressWarnings("unchecked")
	private <T> T getSynchronized(Object key, Callable<T> valueLoader) {

		lock.lock();

		try {
			ValueWrapper result = get(key);
			return result != null ? (T) result.get() : loadCacheValue(key, valueLoader);
		} finally {
			lock.unlock();
		}
	}

	/**
	 * Loads the {@link Object} using the given {@link Callable valueLoader} and {@link #put(Object, Object) puts}
	 * the {@link Object loaded value} in the cache.
	 *
	 * @param <T> {@link Class type} of the loaded {@link Object cache value}.
	 * @param key {@link Object key} mapped to the loaded {@link Object cache value}.
	 * @param valueLoader {@link Callable} object used to load the {@link Object value} for the given {@link Object key}.
	 * @return the loaded {@link Object value}.
	 */
	protected <T> T loadCacheValue(Object key, Callable<T> valueLoader) {

		T value;

		try {
			value = valueLoader.call();
		} catch (Exception cause) {
			throw new ValueRetrievalException(key, valueLoader, cause);
		}

		put(key, value);

		return value;
	}

	@Override
	protected Object lookup(Object key) {

		byte[] binaryKey = createAndConvertCacheKey(key);

		byte[] binaryValue = getCacheConfiguration().isTimeToIdleEnabled()
				? getCacheWriter().get(getName(), binaryKey, getTimeToLive(key))
				: getCacheWriter().get(getName(), binaryKey);

		return binaryValue != null ? deserializeCacheValue(binaryValue) : null;
	}

	private Duration getTimeToLive(Object key) {
		return getTimeToLive(key, null);
	}

	private Duration getTimeToLive(Object key, @Nullable Object value) {
		return getCacheConfiguration().getTtlFunction().getTimeToLive(key, value);
	}

	@Override
	public void put(Object key, @Nullable Object value) {

		Object cacheValue = processAndCheckValue(value);

		byte[] binaryKey = createAndConvertCacheKey(key);
		byte[] binaryValue = serializeCacheValue(cacheValue);

		Duration timeToLive = getTimeToLive(key, value);

		getCacheWriter().put(getName(), binaryKey, binaryValue, timeToLive);
	}

	@Override
	public ValueWrapper putIfAbsent(Object key, @Nullable Object value) {

		Object cacheValue = preProcessCacheValue(value);

		if (nullCacheValueIsNotAllowed(cacheValue)) {
			return get(key);
		}

		Duration timeToLive = getTimeToLive(key, value);

		byte[] binaryKey = createAndConvertCacheKey(key);
		byte[] binaryValue = serializeCacheValue(cacheValue);
		byte[] result = getCacheWriter().putIfAbsent(getName(), binaryKey, binaryValue, timeToLive);

		return result != null ? new SimpleValueWrapper(fromStoreValue(deserializeCacheValue(result))) : null;
	}

	@Override
	public void clear() {
		clear("*");
	}

	/**
	 * Clear keys that match the given {@link String keyPattern}.
	 * <p>
	 * Useful when cache keys are formatted in a style where Redis patterns can be used for matching these.
	 *
	 * @param keyPattern {@link String pattern} used to match Redis keys to clear.
	 * @since 3.0
	 */
	public void clear(String keyPattern) {
		getCacheWriter().clean(getName(), createAndConvertCacheKey(keyPattern));
	}

	/**
	 * Reset all statistics counters and gauges for this cache.
	 *
	 * @since 2.4
	 */
	public void clearStatistics() {
		getCacheWriter().clearStatistics(getName());
	}

	@Override
	public void evict(Object key) {
		getCacheWriter().remove(getName(), createAndConvertCacheKey(key));
	}

	@Override
	public CompletableFuture<?> retrieve(Object key) {

		if (!getCacheWriter().supportsAsyncRetrieve()) {
			throw new UnsupportedOperationException(CACHE_RETRIEVAL_UNSUPPORTED_OPERATION_EXCEPTION_MESSAGE);
		}

		return retrieveValue(key).thenApply(this::nullSafeDeserializedStoreValue);
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T> CompletableFuture<T> retrieve(Object key, Supplier<CompletableFuture<T>> valueLoader) {

		if (!getCacheWriter().supportsAsyncRetrieve()) {
			throw new UnsupportedOperationException(CACHE_RETRIEVAL_UNSUPPORTED_OPERATION_EXCEPTION_MESSAGE);
		}

		return retrieveValue(key).thenCompose(bytes -> {

			if (bytes != null) {
				return CompletableFuture.completedFuture((T) nullSafeDeserializedStoreValue(bytes));
			}

			return valueLoader.get().thenCompose(value -> {

				Object cacheValue = processAndCheckValue(value);

				byte[] binaryKey = createAndConvertCacheKey(key);
				byte[] binaryValue = serializeCacheValue(cacheValue);

				Duration timeToLive = getTimeToLive(key, cacheValue);

				return getCacheWriter().store(getName(), binaryKey, binaryValue, timeToLive)
						.thenApply(v -> value);
			});
		});
	}

	private Object processAndCheckValue(@Nullable Object value) {

		Object cacheValue = preProcessCacheValue(value);

		if (nullCacheValueIsNotAllowed(cacheValue)) {

			String message = String.format("Cache '%s' does not allow 'null' values; Avoid storing null"
					+ " via '@Cacheable(unless=\"#result == null\")' or configure RedisCache to allow 'null'"
					+ " via RedisCacheConfiguration", getName());

			throw new IllegalArgumentException(message);
		}

		return cacheValue;
	}

	/**
	 * Customization hook called before passing object to
	 * {@link org.springframework.data.redis.serializer.RedisSerializer}.
	 *
	 * @param value can be {@literal null}.
	 * @return preprocessed value. Can be {@literal null}.
	 */
	@Nullable
	protected Object preProcessCacheValue(@Nullable Object value) {
		return value != null ? value : isAllowNullValues() ? NullValue.INSTANCE : null;
	}

	/**
	 * Serialize the given {@link String cache key}.
	 *
	 * @param cacheKey {@link String cache key} to serialize; must not be {@literal null}.
	 * @return an array of bytes from the given, serialized {@link String cache key}; never {@literal null}.
	 * @see RedisCacheConfiguration#getKeySerializationPair()
	 */
	protected byte[] serializeCacheKey(String cacheKey) {
		return ByteUtils.getBytes(getCacheConfiguration().getKeySerializationPair().write(cacheKey));
	}

	/**
	 * Serialize the {@link Object value} to cache as an array of bytes.
	 *
	 * @param value {@link Object} to serialize and cache; must not be {@literal null}.
	 * @return an array of bytes from the serialized {@link Object value}; never {@literal null}.
	 * @see RedisCacheConfiguration#getValueSerializationPair()
	 */
	protected byte[] serializeCacheValue(Object value) {

		if (isAllowNullValues() && value instanceof NullValue) {
			return BINARY_NULL_VALUE;
		}

		return ByteUtils.getBytes(getCacheConfiguration().getValueSerializationPair().write(value));
	}

	/**
	 * Deserialize the given the array of bytes to the actual {@link Object cache value}.
	 *
	 * @param value array of bytes to deserialize; must not be {@literal null}.
	 * @return an {@link Object} deserialized from the array of bytes using the configured value
	 *         {@link RedisSerializationContext.SerializationPair}; can be {@literal null}.
	 * @see RedisCacheConfiguration#getValueSerializationPair()
	 */
	@Nullable
	protected Object deserializeCacheValue(byte[] value) {

		if (isAllowNullValues() && ObjectUtils.nullSafeEquals(value, BINARY_NULL_VALUE)) {
			return NullValue.INSTANCE;
		}

		return getCacheConfiguration().getValueSerializationPair().read(ByteBuffer.wrap(value));
	}

	/**
	 * Customization hook for creating cache key before it gets serialized.
	 *
	 * @param key will never be {@literal null}.
	 * @return never {@literal null}.
	 */
	protected String createCacheKey(Object key) {

		String convertedKey = convertKey(key);

		return getCacheConfiguration().usePrefix() ? prefixCacheKey(convertedKey) : convertedKey;
	}

	/**
	 * Convert {@code key} to a {@link String} used in cache key creation.
	 *
	 * @param key will never be {@literal null}.
	 * @return never {@literal null}.
	 * @throws IllegalStateException if {@code key} cannot be converted to {@link String}.
	 */
	protected String convertKey(Object key) {

		if (key instanceof String stringKey) {
			return stringKey;
		}

		TypeDescriptor source = TypeDescriptor.forObject(key);

		ConversionService conversionService = getConversionService();

		if (conversionService.canConvert(source, TypeDescriptor.valueOf(String.class))) {
			try {
				return conversionService.convert(key, String.class);
			} catch (ConversionFailedException cause) {

				// May fail if the given key is a collection
				if (isCollectionLikeOrMap(source)) {
					return convertCollectionLikeOrMapKey(key, source);
				}

				throw cause;
			}
		}

		if (hasToStringMethod(key)) {
			return key.toString();
		}

		String message = String.format("Cannot convert cache key %s to String; Please register a suitable Converter"
				+ " via 'RedisCacheConfiguration.configureKeyConverters(...)' or override '%s.toString()'",
				source, key.getClass().getName());

		throw new IllegalStateException(message);
	}

	private CompletableFuture<byte[]> retrieveValue(Object key) {
		return getCacheWriter().retrieve(getName(), createAndConvertCacheKey(key));
	}

	@Nullable
	private Object nullSafeDeserializedStoreValue(@Nullable byte[] value) {
		return value != null ? fromStoreValue(deserializeCacheValue(value)) : null;
	}

	private boolean hasToStringMethod(Object target) {
		return hasToStringMethod(target.getClass());
	}

	private boolean hasToStringMethod(Class<?> type) {

		Method toString = ReflectionUtils.findMethod(type, "toString");

		return toString != null && !Object.class.equals(toString.getDeclaringClass());
	}

	private boolean isCollectionLikeOrMap(TypeDescriptor source) {
		return source.isArray() || source.isCollection() || source.isMap();
	}

	private String convertCollectionLikeOrMapKey(Object key, TypeDescriptor source) {

		if (source.isMap()) {

			int count = 0;

			StringBuilder target = new StringBuilder("{");

			for (Entry<?, ?> entry : ((Map<?, ?>) key).entrySet()) {
				target.append(convertKey(entry.getKey())).append("=").append(convertKey(entry.getValue()));
				target.append(++count > 1 ? ", " : "");
			}

			target.append("}");

			return target.toString();

		} else if (source.isCollection() || source.isArray()) {

			StringJoiner stringJoiner = new StringJoiner(",");

			Collection<?> collection = source.isCollection() ? (Collection<?>) key
					: Arrays.asList(ObjectUtils.toObjectArray(key));

			for (Object collectedKey : collection) {
				stringJoiner.add(convertKey(collectedKey));
			}

			return "[" + stringJoiner + "]";
		}

		throw new IllegalArgumentException(String.format("Cannot convert cache key [%s] to String", key));
	}

	private byte[] createAndConvertCacheKey(Object key) {
		return serializeCacheKey(createCacheKey(key));
	}

	private String prefixCacheKey(String key) {
		// allow contextual cache names by computing the key prefix on every call.
		return getCacheConfiguration().getKeyPrefixFor(getName()) + key;
	}

	private boolean nullCacheValueIsNotAllowed(@Nullable Object cacheValue) {
		return cacheValue == null && !isAllowNullValues();
	}
}
