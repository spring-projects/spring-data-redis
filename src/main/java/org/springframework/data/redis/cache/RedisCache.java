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
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringJoiner;
import java.util.concurrent.Callable;

import org.springframework.cache.Cache;
import org.springframework.cache.support.AbstractValueAdaptingCache;
import org.springframework.cache.support.NullValue;
import org.springframework.cache.support.SimpleValueWrapper;
import org.springframework.core.convert.ConversionFailedException;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.TypeDescriptor;
import org.springframework.data.redis.cache.RedisCacheConfiguration.TtlFunction;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.util.ByteUtils;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;
import org.springframework.util.ReflectionUtils;

/**
 * {@link org.springframework.cache.Cache} implementation using for Redis as the underlying store for cache data.
 * <p>
 * Use {@link RedisCacheManager} to create {@link RedisCache} instances.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Piotr Mionskowski
 * @author Jos Roseboom
 * @author John Blum
 * @see org.springframework.cache.support.AbstractValueAdaptingCache
 * @since 2.0
 */
@SuppressWarnings("unused")
public class RedisCache extends AbstractValueAdaptingCache {

	private static final byte[] BINARY_NULL_VALUE = RedisSerializer.java().serialize(NullValue.INSTANCE);

	private final RedisCacheConfiguration cacheConfiguration;

	private final RedisCacheWriter cacheWriter;

	private final String name;

	private final TtlFunction ttlFunction;

	/**
	 * Create a new {@link RedisCache}.
	 *
	 * @param name {@link String name} for this {@link Cache}; must not be {@literal null}.
	 * @param cacheWriter {@link RedisCacheWriter} used to perform {@link RedisCache} operations
	 * by executing appropriate Redis commands; must not be {@literal null}.
	 * @param cacheConfiguration {@link RedisCacheConfiguration} applied to this {@link RedisCache on creation;
	 * must not be {@literal null}.
	 * @throws IllegalArgumentException if either the given {@link RedisCacheWriter} or {@link RedisCacheConfiguration}
	 * are {@literal null} or the given {@link String} name for this {@link RedisCache} is {@literal null}.
	 */
	protected RedisCache(String name, RedisCacheWriter cacheWriter, RedisCacheConfiguration cacheConfiguration) {

		super(cacheConfiguration.getAllowCacheNullValues());

		Assert.notNull(name, "Name must not be null");
		Assert.notNull(cacheWriter, "CacheWriter must not be null");
		Assert.notNull(cacheConfiguration, "CacheConfiguration must not be null");

		this.name = name;
		this.cacheWriter = cacheWriter;
		this.cacheConfiguration = cacheConfiguration;
		this.ttlFunction = cacheConfiguration.getTtlFunction();
	}


	/**
	 * Get {@link RedisCacheConfiguration} used.
	 *
	 * @return immutable {@link RedisCacheConfiguration}. Never {@literal null}.
	 */
	public RedisCacheConfiguration getCacheConfiguration() {
		return this.cacheConfiguration;
	}

	protected RedisCacheWriter getCacheWriter() {
		return this.cacheWriter;
	}

	protected ConversionService getConversionService() {
		return getCacheConfiguration().getConversionService();
	}

	@Override
	public String getName() {
		return this.name;
	}

	@Override
	public RedisCacheWriter getNativeCache() {
		return this.cacheWriter;
	}

	/**
	 * Return the {@link CacheStatistics} snapshot for this cache instance. Statistics are accumulated per cache instance
	 * and not from the backing Redis data store.
	 *
	 * @return statistics object for this {@link RedisCache}.
	 * @since 2.4
	 */
	public CacheStatistics getStatistics() {
		return getCacheWriter().getCacheStatistics(getName());
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T> T get(Object key, Callable<T> valueLoader) {

		ValueWrapper result = get(key);

		return result != null ? (T) result.get()
			: getSynchronized(key, valueLoader);
	}

	@SuppressWarnings("unchecked")
	@Nullable
	private synchronized <T> T getSynchronized(Object key, Callable<T> valueLoader) {

		ValueWrapper result = get(key);

		return result != null ? (T) result.get()
			: loadCacheValue(key, valueLoader);
	}

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

		byte[] value = getCacheWriter().get(getName(), createAndConvertCacheKey(key));

		return value != null ? deserializeCacheValue(value) : null;
	}

	@Override
	public void put(Object key, @Nullable Object value) {

		Object cacheValue = preProcessCacheValue(value);

		if (!isAllowNullValues() && cacheValue == null) {

			String message = String.format("Cache '%s' does not allow 'null' values; Avoid storing null"
					+ " via '@Cacheable(unless=\"#result == null\")' or configure RedisCache to allow 'null'"
					+ " via RedisCacheConfiguration",
				getName());

			throw new IllegalArgumentException(message);
		}

		getCacheWriter().put(getName(), createAndConvertCacheKey(key), serializeCacheValue(cacheValue),
				ttlFunction.getTimeToLive(key, value));
	}

	@Override
	public ValueWrapper putIfAbsent(Object key, @Nullable Object value) {

		Object cacheValue = preProcessCacheValue(value);

		if (!isAllowNullValues() && cacheValue == null) {
			return get(key);
		}

		byte[] result = getCacheWriter().putIfAbsent(getName(), createAndConvertCacheKey(key),
				serializeCacheValue(cacheValue), ttlFunction.getTimeToLive(key, value));

		return result != null ? new SimpleValueWrapper(fromStoreValue(deserializeCacheValue(result))) : null;
	}

	@Override
	public void clear() {
		clear("*");
	}

	/**
	 * Clear keys that match the provided {@link String keyPattern}.
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

	/**
	 * Customization hook called before passing object to
	 * {@link org.springframework.data.redis.serializer.RedisSerializer}.
	 *
	 * @param value can be {@literal null}.
	 * @return preprocessed value. Can be {@literal null}.
	 */
	@Nullable
	protected Object preProcessCacheValue(@Nullable Object value) {

		return value != null ? value
			: isAllowNullValues() ? NullValue.INSTANCE
			: null;
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
	 * {@link RedisSerializationContext.SerializationPair}; can be {@literal null}.
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
	 * Convert {@code key} to a {@link String} representation used for cache key creation.
	 *
	 * @param key will never be {@literal null}.
	 * @return never {@literal null}.
	 * @throws IllegalStateException if {@code key} cannot be converted to {@link String}.
	 */
	protected String convertKey(Object key) {

		if (key instanceof String) {
			return (String) key;
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
}
