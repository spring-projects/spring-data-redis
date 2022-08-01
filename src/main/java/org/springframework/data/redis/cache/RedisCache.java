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

import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringJoiner;
import java.util.concurrent.Callable;

import org.springframework.cache.support.AbstractValueAdaptingCache;
import org.springframework.cache.support.NullValue;
import org.springframework.cache.support.SimpleValueWrapper;
import org.springframework.core.convert.ConversionFailedException;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.TypeDescriptor;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.util.ByteUtils;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;
import org.springframework.util.ReflectionUtils;

/**
 * {@link org.springframework.cache.Cache} implementation using for Redis as underlying store.
 * <p>
 * Use {@link RedisCacheManager} to create {@link RedisCache} instances.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Piotr Mionskowski
 * @author Jos Roseboom
 * @see RedisCacheConfiguration
 * @see RedisCacheWriter
 * @since 2.0
 */
public class RedisCache extends AbstractValueAdaptingCache {

	private static final byte[] BINARY_NULL_VALUE = RedisSerializer.java().serialize(NullValue.INSTANCE);

	private final String name;
	private final RedisCacheWriter cacheWriter;
	private final RedisCacheConfiguration cacheConfig;
	private final ConversionService conversionService;

	/**
	 * Create new {@link RedisCache}.
	 *
	 * @param name must not be {@literal null}.
	 * @param cacheWriter must not be {@literal null}.
	 * @param cacheConfig must not be {@literal null}.
	 */
	protected RedisCache(String name, RedisCacheWriter cacheWriter, RedisCacheConfiguration cacheConfig) {

		super(cacheConfig.getAllowCacheNullValues());

		Assert.notNull(name, "Name must not be null");
		Assert.notNull(cacheWriter, "CacheWriter must not be null");
		Assert.notNull(cacheConfig, "CacheConfig must not be null");

		this.name = name;
		this.cacheWriter = cacheWriter;
		this.cacheConfig = cacheConfig;
		this.conversionService = cacheConfig.getConversionService();
	}

	@Override
	protected Object lookup(Object key) {

		byte[] value = cacheWriter.get(name, createAndConvertCacheKey(key));

		if (value == null) {
			return null;
		}

		return deserializeCacheValue(value);
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public RedisCacheWriter getNativeCache() {
		return this.cacheWriter;
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T> T get(Object key, Callable<T> valueLoader) {

		ValueWrapper result = get(key);

		if (result != null) {
			return (T) result.get();
		}

		return getSynchronized(key, valueLoader);
	}

	@SuppressWarnings("unchecked")
	private synchronized <T> T getSynchronized(Object key, Callable<T> valueLoader) {

		ValueWrapper result = get(key);

		if (result != null) {
			return (T) result.get();
		}

		T value;
		try {
			value = valueLoader.call();
		} catch (Exception e) {
			throw new ValueRetrievalException(key, valueLoader, e);
		}
		put(key, value);
		return value;
	}

	@Override
	public void put(Object key, @Nullable Object value) {

		Object cacheValue = preProcessCacheValue(value);

		if (!isAllowNullValues() && cacheValue == null) {

			throw new IllegalArgumentException(String.format(
					"Cache '%s' does not allow 'null' values; Avoid storing null via '@Cacheable(unless=\"#result == null\")' or configure RedisCache to allow 'null' via RedisCacheConfiguration",
					name));
		}

		cacheWriter.put(name, createAndConvertCacheKey(key), serializeCacheValue(cacheValue), cacheConfig.getTtl());
	}

	@Override
	public ValueWrapper putIfAbsent(Object key, @Nullable Object value) {

		Object cacheValue = preProcessCacheValue(value);

		if (!isAllowNullValues() && cacheValue == null) {
			return get(key);
		}

		byte[] result = cacheWriter.putIfAbsent(name, createAndConvertCacheKey(key), serializeCacheValue(cacheValue),
				cacheConfig.getTtl());

		if (result == null) {
			return null;
		}

		return new SimpleValueWrapper(fromStoreValue(deserializeCacheValue(result)));
	}

	@Override
	public void evict(Object key) {
		cacheWriter.remove(name, createAndConvertCacheKey(key));
	}

	@Override
	public void clear() {
		clearByPattern("*");
	}

	/**
	 * <p>Clear keys that match the provided pattern.</p>
	 * <br />
	 * <p>Useful when the cache keys consists of multiple parameters. For example:
	 * a cache key consists of a brand id and a country id. Now country 42 has relevant data updated. We want to clear all
	 * the caches that involve country 42, regardless the brand. That can be done by clearByPattern("*42")</p>
	 * @param keyPattern the pattern of the key
	 */
	public void clearByPattern(String keyPattern) {
		byte[] pattern = conversionService.convert(createCacheKey(keyPattern), byte[].class);
		cacheWriter.clean(name, pattern);
	}

	/**
	 * Return the {@link CacheStatistics} snapshot for this cache instance. Statistics are accumulated per cache instance
	 * and not from the backing Redis data store.
	 *
	 * @return statistics object for this {@link RedisCache}.
	 * @since 2.4
	 */
	public CacheStatistics getStatistics() {
		return cacheWriter.getCacheStatistics(getName());
	}

	/**
	 * Reset all statistics counters and gauges for this cache.
	 *
	 * @since 2.4
	 */
	public void clearStatistics() {
		cacheWriter.clearStatistics(getName());
	}

	/**
	 * Get {@link RedisCacheConfiguration} used.
	 *
	 * @return immutable {@link RedisCacheConfiguration}. Never {@literal null}.
	 */
	public RedisCacheConfiguration getCacheConfiguration() {
		return cacheConfig;
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

		if (value != null) {
			return value;
		}

		return isAllowNullValues() ? NullValue.INSTANCE : null;
	}

	/**
	 * Serialize the key.
	 *
	 * @param cacheKey must not be {@literal null}.
	 * @return never {@literal null}.
	 */
	protected byte[] serializeCacheKey(String cacheKey) {
		return ByteUtils.getBytes(cacheConfig.getKeySerializationPair().write(cacheKey));
	}

	/**
	 * Serialize the value to cache.
	 *
	 * @param value must not be {@literal null}.
	 * @return never {@literal null}.
	 */
	protected byte[] serializeCacheValue(Object value) {

		if (isAllowNullValues() && value instanceof NullValue) {
			return BINARY_NULL_VALUE;
		}

		return ByteUtils.getBytes(cacheConfig.getValueSerializationPair().write(value));
	}

	/**
	 * Deserialize the given value to the actual cache value.
	 *
	 * @param value must not be {@literal null}.
	 * @return can be {@literal null}.
	 */
	@Nullable
	protected Object deserializeCacheValue(byte[] value) {

		if (isAllowNullValues() && ObjectUtils.nullSafeEquals(value, BINARY_NULL_VALUE)) {
			return NullValue.INSTANCE;
		}

		return cacheConfig.getValueSerializationPair().read(ByteBuffer.wrap(value));
	}

	/**
	 * Customization hook for creating cache key before it gets serialized.
	 *
	 * @param key will never be {@literal null}.
	 * @return never {@literal null}.
	 */
	protected String createCacheKey(Object key) {

		String convertedKey = convertKey(key);

		if (!cacheConfig.usePrefix()) {
			return convertedKey;
		}

		return prefixCacheKey(convertedKey);
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

		if (conversionService.canConvert(source, TypeDescriptor.valueOf(String.class))) {
			try {
				return conversionService.convert(key, String.class);
			} catch (ConversionFailedException e) {

				// may fail if the given key is a collection
				if (isCollectionLikeOrMap(source)) {
					return convertCollectionLikeOrMapKey(key, source);
				}

				throw e;
			}
		}

		Method toString = ReflectionUtils.findMethod(key.getClass(), "toString");

		if (toString != null && !Object.class.equals(toString.getDeclaringClass())) {
			return key.toString();
		}

		throw new IllegalStateException(String.format(
				"Cannot convert cache key %s to String; Please register a suitable Converter via 'RedisCacheConfiguration.configureKeyConverters(...)' or override '%s.toString()'",
				source, key.getClass().getSimpleName()));
	}

	private String convertCollectionLikeOrMapKey(Object key, TypeDescriptor source) {

		if (source.isMap()) {

			StringBuilder target = new StringBuilder("{");

			for (Entry<?, ?> entry : ((Map<?, ?>) key).entrySet()) {
				target.append(convertKey(entry.getKey())).append("=").append(convertKey(entry.getValue()));
			}
			target.append("}");

			return target.toString();
		} else if (source.isCollection() || source.isArray()) {

			StringJoiner sj = new StringJoiner(",");

			Collection<?> collection = source.isCollection() ? (Collection<?>) key
					: Arrays.asList(ObjectUtils.toObjectArray(key));

			for (Object val : collection) {
				sj.add(convertKey(val));
			}
			return "[" + sj.toString() + "]";
		}

		throw new IllegalArgumentException(String.format("Cannot convert cache key %s to String", key));
	}

	private boolean isCollectionLikeOrMap(TypeDescriptor source) {
		return source.isArray() || source.isCollection() || source.isMap();
	}

	private byte[] createAndConvertCacheKey(Object key) {
		return serializeCacheKey(createCacheKey(key));
	}

	private String prefixCacheKey(String key) {

		// allow contextual cache names by computing the key prefix on every call.
		return cacheConfig.getKeyPrefixFor(name) + key;
	}

}
