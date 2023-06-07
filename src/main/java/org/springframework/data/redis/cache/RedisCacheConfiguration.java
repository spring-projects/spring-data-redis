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

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.function.Consumer;

import org.springframework.cache.Cache;
import org.springframework.cache.interceptor.SimpleKey;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.convert.converter.ConverterRegistry;
import org.springframework.data.redis.cache.RedisCacheWriter.TtlFunction;
import org.springframework.data.redis.serializer.RedisSerializationContext.SerializationPair;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.format.support.DefaultFormattingConversionService;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Immutable {@link RedisCacheConfiguration} used to customize {@link RedisCache} behaviour, such as caching
 * {@literal null} values, computing cache key prefixes and handling binary serialization.
 * <p>
 * Start with {@link RedisCacheConfiguration#defaultCacheConfig()} and customize {@link RedisCache} behaviour from that
 * point on.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author John Blum
 * @author Koy Zhuang
 * @since 2.0
 */
public class RedisCacheConfiguration {

	protected static final boolean DEFAULT_CACHE_NULL_VALUES = true;
	protected static final boolean DEFAULT_USE_PREFIX = true;
	protected static final boolean DO_NOT_CACHE_NULL_VALUES = false;
	protected static final boolean DO_NOT_USE_PREFIX = false;

	/**
	 * Default {@link RedisCacheConfiguration} using the following:
	 * <dl>
	 * <dt>key expiration</dt>
	 * <dd>eternal</dd>
	 * <dt>cache null values</dt>
	 * <dd>yes</dd>
	 * <dt>prefix cache keys</dt>
	 * <dd>yes</dd>
	 * <dt>default prefix</dt>
	 * <dd>[the actual cache name]</dd>
	 * <dt>key serializer</dt>
	 * <dd>{@link org.springframework.data.redis.serializer.StringRedisSerializer}</dd>
	 * <dt>value serializer</dt>
	 * <dd>{@link org.springframework.data.redis.serializer.JdkSerializationRedisSerializer}</dd>
	 * <dt>conversion service</dt>
	 * <dd>{@link DefaultFormattingConversionService} with {@link #registerDefaultConverters(ConverterRegistry) default}
	 * cache key converters</dd>
	 * </dl>
	 *
	 * @return new {@link RedisCacheConfiguration}.
	 */
	public static RedisCacheConfiguration defaultCacheConfig() {
		return defaultCacheConfig(null);
	}

	/**
	 * Create default {@link RedisCacheConfiguration} given {@link ClassLoader} using the following:
	 * <dl>
	 * <dt>key expiration</dt>
	 * <dd>eternal</dd>
	 * <dt>cache null values</dt>
	 * <dd>yes</dd>
	 * <dt>prefix cache keys</dt>
	 * <dd>yes</dd>
	 * <dt>default prefix</dt>
	 * <dd>[the actual cache name]</dd>
	 * <dt>key serializer</dt>
	 * <dd>{@link org.springframework.data.redis.serializer.StringRedisSerializer}</dd>
	 * <dt>value serializer</dt>
	 * <dd>{@link org.springframework.data.redis.serializer.JdkSerializationRedisSerializer}</dd>
	 * <dt>conversion service</dt>
	 * <dd>{@link DefaultFormattingConversionService} with {@link #registerDefaultConverters(ConverterRegistry) default}
	 * cache key converters</dd>
	 * </dl>
	 *
	 * @param classLoader the {@link ClassLoader} used for deserialization by the
	 *          {@link org.springframework.data.redis.serializer.JdkSerializationRedisSerializer}.
	 * @return new {@link RedisCacheConfiguration}.
	 * @since 2.1
	 */
	public static RedisCacheConfiguration defaultCacheConfig(@Nullable ClassLoader classLoader) {

		DefaultFormattingConversionService conversionService = new DefaultFormattingConversionService();

		registerDefaultConverters(conversionService);

		return new RedisCacheConfiguration((k, v) -> Duration.ZERO, DEFAULT_CACHE_NULL_VALUES, DEFAULT_USE_PREFIX,
				CacheKeyPrefix.simple(), SerializationPair.fromSerializer(RedisSerializer.string()),
				SerializationPair.fromSerializer(RedisSerializer.java(classLoader)), conversionService);
	}

	private final boolean cacheNullValues;
	private final boolean usePrefix;

	private final CacheKeyPrefix keyPrefix;

	private final ConversionService conversionService;

	private final TtlFunction ttlFunction;

	private final SerializationPair<String> keySerializationPair;
	private final SerializationPair<Object> valueSerializationPair;

	@SuppressWarnings("unchecked")
	private RedisCacheConfiguration(TtlFunction ttlFunction, Boolean cacheNullValues, Boolean usePrefix,
			CacheKeyPrefix keyPrefix, SerializationPair<String> keySerializationPair,
			SerializationPair<?> valueSerializationPair, ConversionService conversionService) {

		this.ttlFunction = ttlFunction;
		this.cacheNullValues = cacheNullValues;
		this.usePrefix = usePrefix;
		this.keyPrefix = keyPrefix;
		this.keySerializationPair = keySerializationPair;
		this.valueSerializationPair = (SerializationPair<Object>) valueSerializationPair;
		this.conversionService = conversionService;
	}

	/**
	 * Prefix the {@link RedisCache#getName() cache name} with the given value. <br />
	 * The generated cache key will be: {@code prefix + cache name + "::" + cache entry key}.
	 *
	 * @param prefix the prefix to prepend to the cache name.
	 * @return new {@link RedisCacheConfiguration}.
	 * @see #computePrefixWith(CacheKeyPrefix)
	 * @see CacheKeyPrefix#prefixed(String)
	 * @since 2.3
	 */
	public RedisCacheConfiguration prefixCacheNameWith(String prefix) {
		return computePrefixWith(CacheKeyPrefix.prefixed(prefix));
	}

	/**
	 * Use the given {@link CacheKeyPrefix} to compute the prefix for the actual Redis {@literal key} given the
	 * {@literal cache name} as function input.
	 *
	 * @param cacheKeyPrefix must not be {@literal null}.
	 * @return new {@link RedisCacheConfiguration}.
	 * @since 2.0.4
	 * @see CacheKeyPrefix
	 */
	public RedisCacheConfiguration computePrefixWith(CacheKeyPrefix cacheKeyPrefix) {

		Assert.notNull(cacheKeyPrefix, "Function for computing prefix must not be null");

		return new RedisCacheConfiguration(ttlFunction, cacheNullValues, DEFAULT_USE_PREFIX, cacheKeyPrefix,
				keySerializationPair, valueSerializationPair, conversionService);
	}

	/**
	 * Disable caching {@literal null} values. <br />
	 * <strong>NOTE</strong> any {@link org.springframework.cache.Cache#put(Object, Object)} operation involving
	 * {@literal null} value will error. Nothing will be written to Redis, nothing will be removed. An already existing
	 * key will still be there afterwards with the very same value as before.
	 *
	 * @return new {@link RedisCacheConfiguration}.
	 */
	public RedisCacheConfiguration disableCachingNullValues() {
		return new RedisCacheConfiguration(ttlFunction, DO_NOT_CACHE_NULL_VALUES, usePrefix, keyPrefix,
				keySerializationPair, valueSerializationPair, conversionService);
	}

	/**
	 * Disable using cache key prefixes. <br />
	 * <strong>NOTE</strong>: {@link Cache#clear()} might result in unintended removal of {@literal key}s in Redis. Make
	 * sure to use a dedicated Redis instance when disabling prefixes.
	 *
	 * @return new {@link RedisCacheConfiguration}.
	 */
	public RedisCacheConfiguration disableKeyPrefix() {

		return new RedisCacheConfiguration(ttlFunction, cacheNullValues, DO_NOT_USE_PREFIX, keyPrefix, keySerializationPair,
				valueSerializationPair, conversionService);
	}

	/**
	 * Set the constant ttl to apply for cache entries. Use {@link Duration#ZERO} to declare an eternal cache.
	 *
	 * @param ttl must not be {@literal null}.
	 * @return new {@link RedisCacheConfiguration}.
	 */
	public RedisCacheConfiguration entryTtl(Duration ttl) {

		Assert.notNull(ttl, "TTL duration must not be null");

		return entryTtl(TtlFunction.just(ttl));
	}

	/**
	 * Set the {@link TtlFunction TTL function} to compute the time to live for cache entries.
	 *
	 * @param ttlFunction the {@link TtlFunction} to compute the time to live for cache entries, must not be
	 *          {@literal null}.
	 * @return new {@link RedisCacheConfiguration}.
	 * @since 3.2
	 */
	public RedisCacheConfiguration entryTtl(TtlFunction ttlFunction) {

		Assert.notNull(ttlFunction, "TtlFunction must not be null");

		return new RedisCacheConfiguration(ttlFunction, cacheNullValues, usePrefix, keyPrefix, keySerializationPair,
				valueSerializationPair, conversionService);
	}

	/**
	 * Define the {@link SerializationPair} used for de-/serializing cache keys.
	 *
	 * @param keySerializationPair must not be {@literal null}.
	 * @return new {@link RedisCacheConfiguration}.
	 */
	public RedisCacheConfiguration serializeKeysWith(SerializationPair<String> keySerializationPair) {

		Assert.notNull(keySerializationPair, "KeySerializationPair must not be null");

		return new RedisCacheConfiguration(ttlFunction, cacheNullValues, usePrefix, keyPrefix, keySerializationPair,
				valueSerializationPair, conversionService);
	}

	/**
	 * Define the {@link SerializationPair} used for de-/serializing cache values.
	 *
	 * @param valueSerializationPair must not be {@literal null}.
	 * @return new {@link RedisCacheConfiguration}.
	 */
	public RedisCacheConfiguration serializeValuesWith(SerializationPair<?> valueSerializationPair) {

		Assert.notNull(valueSerializationPair, "ValueSerializationPair must not be null");

		return new RedisCacheConfiguration(ttlFunction, cacheNullValues, usePrefix, keyPrefix, keySerializationPair,
				valueSerializationPair, conversionService);
	}

	/**
	 * Define the {@link ConversionService} used for cache key to {@link String} conversion.
	 *
	 * @param conversionService must not be {@literal null}.
	 * @return new {@link RedisCacheConfiguration}.
	 */
	public RedisCacheConfiguration withConversionService(ConversionService conversionService) {

		Assert.notNull(conversionService, "ConversionService must not be null");

		return new RedisCacheConfiguration(ttlFunction, cacheNullValues, usePrefix, keyPrefix, keySerializationPair,
				valueSerializationPair, conversionService);
	}

	/**
	 * @return {@literal true} if caching {@literal null} is allowed.
	 */
	public boolean getAllowCacheNullValues() {
		return cacheNullValues;
	}

	/**
	 * @return {@literal true} if cache keys need to be prefixed with the {@link #getKeyPrefixFor(String)} if present or
	 *         the default which resolves to {@link Cache#getName()}.
	 */
	public boolean usePrefix() {
		return usePrefix;
	}

	/**
	 * @return The {@link ConversionService} used for cache key to {@link String} conversion. Never {@literal null}.
	 */
	public ConversionService getConversionService() {
		return conversionService;
	}

	/**
	 * Get the computed {@literal key} prefix for a given {@literal cacheName}.
	 *
	 * @return never {@literal null}.
	 * @since 2.0.4
	 */
	public String getKeyPrefixFor(String cacheName) {

		Assert.notNull(cacheName, "Cache name must not be null");

		return keyPrefix.compute(cacheName);
	}

	/**
	 * @return never {@literal null}.
	 */
	public SerializationPair<String> getKeySerializationPair() {
		return keySerializationPair;
	}

	/**
	 * @return never {@literal null}.
	 */
	public SerializationPair<Object> getValueSerializationPair() {
		return valueSerializationPair;
	}

	/**
	 * @return The constant expiration time (ttl) for cache entries. Never {@literal null}.
	 * @deprecated since 3.2, use {@link #getTtlFunction()} instead.
	 */
	@Deprecated(since = "3.2", forRemoval = true)
	public Duration getTtl() {
		return getTtlFunction().getTimeToLive(Object.class, null);
	}

	/**
	 * Returns the function to compute the time to live.
	 *
	 * @return the function to compute the time to live.
	 * @since 3.2
	 */
	public TtlFunction getTtlFunction() {
		return ttlFunction;
	}

	/**
	 * Adds a {@link Converter} to extract the {@link String} representation of a {@literal cache key} if no suitable
	 * {@link Object#toString()} method is present.
	 *
	 * @param cacheKeyConverter {@link Converter} used to convert a {@literal cache key} into a {@link String}.
	 * @throws IllegalStateException if {@link #getConversionService()} does not allow {@link Converter} registration.
	 * @see org.springframework.core.convert.converter.Converter
	 * @since 2.2
	 */
	public void addCacheKeyConverter(Converter<?, String> cacheKeyConverter) {
		configureKeyConverters(it -> it.addConverter(cacheKeyConverter));
	}

	/**
	 * Configure the underlying {@link ConversionService} used to extract the {@literal cache key}.
	 *
	 * @param registryConsumer {@link Consumer} used to register a {@link Converter} with the configured
	 *          {@link ConverterRegistry}; never {@literal null}.
	 * @throws IllegalStateException if {@link #getConversionService()} does not allow {@link Converter} registration.
	 * @see org.springframework.core.convert.converter.ConverterRegistry
	 * @since 2.2
	 */
	public void configureKeyConverters(Consumer<ConverterRegistry> registryConsumer) {

		if (!(getConversionService() instanceof ConverterRegistry)) {

			String message = "'%s' returned by getConversionService() does not allow Converter registration;"
					+ " Please make sure to provide a ConversionService that implements ConverterRegistry";

			throw new IllegalStateException(String.format(message, getConversionService().getClass().getName()));
		}

		registryConsumer.accept((ConverterRegistry) getConversionService());
	}

	/**
	 * Registers default cache {@link Converter key converters}.
	 * <p>
	 * The following converters get registered:
	 * <p>
	 * <ul>
	 * <li>{@link String} to {@link byte byte[]} using UTF-8 encoding.</li>
	 * <li>{@link SimpleKey} to {@link String}</li>
	 * </ul>
	 *
	 * @param registry {@link ConverterRegistry} in which the {@link Converter key converters} are registered; must not be
	 *          {@literal null}.
	 * @see org.springframework.core.convert.converter.ConverterRegistry
	 */
	public static void registerDefaultConverters(ConverterRegistry registry) {

		Assert.notNull(registry, "ConverterRegistry must not be null");

		registry.addConverter(String.class, byte[].class, source -> source.getBytes(StandardCharsets.UTF_8));
		registry.addConverter(SimpleKey.class, String.class, SimpleKey::toString);
	}

}
