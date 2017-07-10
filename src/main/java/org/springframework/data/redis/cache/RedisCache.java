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

import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.concurrent.Callable;

import org.springframework.cache.support.AbstractValueAdaptingCache;
import org.springframework.cache.support.NullValue;
import org.springframework.cache.support.SimpleValueWrapper;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.TypeDescriptor;
import org.springframework.data.redis.serializer.JdkSerializationRedisSerializer;
import org.springframework.data.redis.util.ByteUtils;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;
import org.springframework.util.ReflectionUtils;

/**
 * {@link org.springframework.cache.Cache} implementation using for Redis as underlying store.
 * <p />
 * Use {@link RedisCacheManager} to create {@link RedisCache} instances.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.0
 * @see RedisCacheConfiguration
 * @see RedisCacheWriter
 */
public class RedisCache extends AbstractValueAdaptingCache {

	private static final byte[] BINARY_NULL_VALUE = new JdkSerializationRedisSerializer().serialize(NullValue.INSTANCE);

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

		Assert.notNull(name, "Name must not be null!");
		Assert.notNull(cacheWriter, "CacheWriter must not be null!");
		Assert.notNull(cacheConfig, "CacheConfig must not be null!");

		this.name = name;
		this.cacheWriter = cacheWriter;
		this.cacheConfig = cacheConfig;
		this.conversionService = cacheConfig.getConversionService();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.cache.support.AbstractValueAdaptingCache#lookup(java.lang.Object)
	 */
	@Override
	protected Object lookup(Object key) {

		byte[] value = cacheWriter.get(name, createAndConvertCacheKey(key));

		if (value == null) {
			return null;
		}

		return deserializeCacheValue(value);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.cache.Cache#getName()
	 */
	@Override
	public String getName() {
		return name;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.cache.Cache#getNativeCache()
	 */
	@Override
	public RedisCacheWriter getNativeCache() {
		return this.cacheWriter;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.cache.Cache#get(java.lang.Object, java.util.concurrent.Callable)
	 */
	@Override
	@SuppressWarnings("unchecked")
	public synchronized <T> T get(Object key, Callable<T> valueLoader) {

		ValueWrapper result = get(key);

		if (result != null) {
			return (T) result.get();
		}

		T value = valueFromLoader(key, valueLoader);
		put(key, value);
		return value;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.cache.Cache#put(java.lang.Object, java.lang.Object)
	 */
	@Override
	public void put(Object key, Object value) {

		Object cacheValue = preProcessCacheValue(value);

		if (!isAllowNullValues() && cacheValue == null) {

			throw new IllegalArgumentException(String.format(
					"Cache '%s' does not allow 'null' values. Avoid storing null via '@Cacheable(unless=\"#result == null\")' or configure RedisCache to allow 'null' via RedisCacheConfiguration.",
					name));
		}

		cacheWriter.put(name, createAndConvertCacheKey(key), serializeCacheValue(cacheValue), cacheConfig.getTtl());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.cache.Cache#putIfAbsent(java.lang.Object, java.lang.Object)
	 */
	@Override
	public ValueWrapper putIfAbsent(Object key, Object value) {

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

	/*
	 * (non-Javadoc)
	 * @see org.springframework.cache.Cache#evict(java.lang.Object)
	 */
	@Override
	public void evict(Object key) {
		cacheWriter.remove(name, createAndConvertCacheKey(key));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.cache.Cache#clear()
	 */
	@Override
	public void clear() {

		byte[] pattern = conversionService.convert(createCacheKey("*"), byte[].class);
		cacheWriter.clean(name, pattern);
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
	protected Object preProcessCacheValue(Object value) {

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

		TypeDescriptor source = TypeDescriptor.forObject(key);
		if (conversionService.canConvert(source, TypeDescriptor.valueOf(String.class))) {
			return conversionService.convert(key, String.class);
		}

		Method toString = ReflectionUtils.findMethod(key.getClass(), "toString");

		if (toString != null && !Object.class.equals(toString.getDeclaringClass())) {
			return key.toString();
		}

		throw new IllegalStateException(
				"Cannot convert " + source + " to String. Register a Converter or override toString().");
	}

	private byte[] createAndConvertCacheKey(Object key) {
		return serializeCacheKey(createCacheKey(key));
	}

	private String prefixCacheKey(String key) {
		return cacheConfig.getKeyPrefix().orElseGet(() -> name + "::") + key;
	}

	private <T> T valueFromLoader(Object key, Callable<T> valueLoader) {

		try {
			return valueLoader.call();
		} catch (Exception e) {
			throw new ValueRetrievalException(key, valueLoader, e);
		}
	}
}
