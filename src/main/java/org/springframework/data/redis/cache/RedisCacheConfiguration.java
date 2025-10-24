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
 * Immutable {@link RedisCacheConfiguration} used to customize {@link RedisCache} behavior, such as caching
 * {@literal null} values, computing cache key prefixes and handling binary serialization.
 * <p>
 * Start with {@link RedisCacheConfiguration#defaultCacheConfig()} and customize {@link RedisCache} behavior using the
 * builder methods, such as {@link #entryTtl(Duration)}, {@link #serializeKeysWith(SerializationPair)} and
 * {@link #serializeValuesWith(SerializationPair)}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author John Blum
 * @since 2.0
 */
public class RedisCacheConfiguration {

	protected static final boolean DEFAULT_CACHE_NULL_VALUES = true;
	protected static final boolean DEFAULT_ENABLE_TIME_TO_IDLE_EXPIRATION = false;
	protected static final boolean DEFAULT_USE_PREFIX = true;
	protected static final boolean DO_NOT_CACHE_NULL_VALUES = false;
	protected static final boolean DO_NOT_USE_PREFIX = false;
	protected static final boolean USE_TIME_TO_IDLE_EXPIRATION = true;

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

		return new RedisCacheConfiguration(TtlFunction.persistent(), DEFAULT_CACHE_NULL_VALUES,
				DEFAULT_ENABLE_TIME_TO_IDLE_EXPIRATION, DEFAULT_USE_PREFIX, CacheKeyPrefix.simple(),
				SerializationPair.fromSerializer(RedisSerializer.string()),
				SerializationPair.fromSerializer(RedisSerializer.java(classLoader)), conversionService);
	}

	private final boolean cacheNullValues;
	private final boolean enableTimeToIdle;
	private final boolean usePrefix;

	private final CacheKeyPrefix keyPrefix;

	private final ConversionService conversionService;

	private final SerializationPair<String> keySerializationPair;
	private final SerializationPair<Object> valueSerializationPair;

	private final TtlFunction ttlFunction;

	@SuppressWarnings("unchecked")
	private RedisCacheConfiguration(TtlFunction ttlFunction, Boolean cacheNullValues, Boolean enableTimeToIdle,
			Boolean usePrefix, CacheKeyPrefix keyPrefix, SerializationPair<String> keySerializationPair,
			SerializationPair<?> valueSerializationPair, ConversionService conversionService) {

		this.ttlFunction = ttlFunction;
		this.cacheNullValues = cacheNullValues;
		this.enableTimeToIdle = enableTimeToIdle;
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

		Assert.notNull(cacheKeyPrefix, "Function used to compute prefix must not be null");

		return new RedisCacheConfiguration(getTtlFunction(), getAllowCacheNullValues(), isTimeToIdleEnabled(),
				DEFAULT_USE_PREFIX, cacheKeyPrefix, getKeySerializationPair(), getValueSerializationPair(),
				getConversionService());
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
		return new RedisCacheConfiguration(getTtlFunction(), DO_NOT_CACHE_NULL_VALUES, isTimeToIdleEnabled(), usePrefix(),
				getKeyPrefix(), getKeySerializationPair(), getValueSerializationPair(), getConversionService());
	}

	/**
	 * Disable using cache key prefixes. <br />
	 * <strong>NOTE</strong>: {@link Cache#clear()} might result in unintended removal of {@literal key}s in Redis. Make
	 * sure to use a dedicated Redis instance when disabling prefixes.
	 *
	 * @return new {@link RedisCacheConfiguration}.
	 */
	public RedisCacheConfiguration disableKeyPrefix() {
		return new RedisCacheConfiguration(getTtlFunction(), getAllowCacheNullValues(), isTimeToIdleEnabled(),
				DO_NOT_USE_PREFIX, getKeyPrefix(), getKeySerializationPair(), getValueSerializationPair(),
				getConversionService());
	}

	/**
	 * Enables {@literal time-to-idle (TTI) expiration} on {@link Cache} read operations, such as
	 * {@link Cache#get(Object)}.
	 * <p>
	 * Enabling this option applies the same {@link #getTtlFunction() TTL expiration policy} to {@link Cache} read
	 * operations as it does for {@link Cache} write operations. In effect, this will invoke the Redis {@literal GETEX}
	 * command in place of {@literal GET}.
	 * <p>
	 * Redis does not support the concept of {@literal TTI}, only {@literal TTL}. However, if {@literal TTL} expiration is
	 * applied to all {@link Cache} operations, both read and write alike, and {@link Cache} operations passed with
	 * expiration are used consistently across the application, then in effect, an application can achieve {@literal TTI}
	 * expiration-like behavior.
	 * <p>
	 * Requires Redis 6.2.0 or newer.
	 *
	 * @return this {@link RedisCacheConfiguration}.
	 * @see <a href="https://redis.io/commands/getex/">GETEX</a>
	 * @since 3.2.0
	 */
	public RedisCacheConfiguration enableTimeToIdle() {
		return new RedisCacheConfiguration(getTtlFunction(), getAllowCacheNullValues(), USE_TIME_TO_IDLE_EXPIRATION,
				usePrefix(), getKeyPrefix(), getKeySerializationPair(), getValueSerializationPair(), getConversionService());
	}

	/**
	 * Set the ttl to apply for cache entries. Use {@link Duration#ZERO} to declare an eternal cache.
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

		return new RedisCacheConfiguration(ttlFunction, getAllowCacheNullValues(), isTimeToIdleEnabled(), usePrefix(),
				getKeyPrefix(), getKeySerializationPair(), getValueSerializationPair(), getConversionService());
	}

	/**
	 * Define the {@link SerializationPair} used for de-/serializing cache keys.
	 *
	 * @param keySerializationPair must not be {@literal null}.
	 * @return new {@link RedisCacheConfiguration}.
	 */
	public RedisCacheConfiguration serializeKeysWith(SerializationPair<String> keySerializationPair) {

		Assert.notNull(keySerializationPair, "KeySerializationPair must not be null");

		return new RedisCacheConfiguration(getTtlFunction(), getAllowCacheNullValues(), isTimeToIdleEnabled(), usePrefix(),
				getKeyPrefix(), keySerializationPair, getValueSerializationPair(), getConversionService());
	}

	/**
	 * Define the {@link SerializationPair} used for de-/serializing cache values.
	 *
	 * @param valueSerializationPair must not be {@literal null}.
	 * @return new {@link RedisCacheConfiguration}.
	 */
	public RedisCacheConfiguration serializeValuesWith(SerializationPair<?> valueSerializationPair) {

		Assert.notNull(valueSerializationPair, "ValueSerializationPair must not be null");

		return new RedisCacheConfiguration(getTtlFunction(), getAllowCacheNullValues(), isTimeToIdleEnabled(), usePrefix(),
				getKeyPrefix(), getKeySerializationPair(), valueSerializationPair, getConversionService());
	}

	/**
	 * Define the {@link ConversionService} used for cache key to {@link String} conversion.
	 *
	 * @param conversionService must not be {@literal null}.
	 * @return new {@link RedisCacheConfiguration}.
	 */
	public RedisCacheConfiguration withConversionService(ConversionService conversionService) {

		Assert.notNull(conversionService, "ConversionService must not be null");

		return new RedisCacheConfiguration(getTtlFunction(), getAllowCacheNullValues(), isTimeToIdleEnabled(), usePrefix(),
				getKeyPrefix(), getKeySerializationPair(), getValueSerializationPair(), conversionService);
	}

	/**
	 * @return {@literal true} if caching {@literal null} is allowed.
	 */
	public boolean getAllowCacheNullValues() {
		return this.cacheNullValues;
	}

	/**
	 * Determines whether {@literal time-to-idle (TTI) expiration} has been enabled for caching.
	 * <p>
	 * Use {@link #enableTimeToIdle()} to opt-in and enable {@literal time-to-idle (TTI) expiration} for caching.
	 *
	 * @return {@literal true} if {@literal time-to-idle (TTI) expiration} was configured and enabled for caching.
	 *         Defaults to {@literal false}.
	 * @see <a href="https://redis.io/commands/getex/">GETEX</a>
	 * @since 3.2.0
	 */
	public boolean isTimeToIdleEnabled() {
		return this.enableTimeToIdle;
	}

	/**
	 * @return {@literal true} if cache keys need to be prefixed with the {@link #getKeyPrefixFor(String)} if present or
	 *         the default which resolves to {@link Cache#getName()}.
	 */
	public boolean usePrefix() {
		return this.usePrefix;
	}

	/**
	 * @return The {@link ConversionService} used for cache key to {@link String} conversion. Never {@literal null}.
	 */
	public ConversionService getConversionService() {
		return this.conversionService;
	}

	/**
	 * Gets the configured {@link CacheKeyPrefix}.
	 *
	 * @return the configured {@link CacheKeyPrefix}.
	 */
	public CacheKeyPrefix getKeyPrefix() {
		return this.keyPrefix;
	}

	/**
	 * Get the computed {@literal key} prefix for a given {@literal cacheName}.
	 *
	 * @return never {@literal null}.
	 * @since 2.0.4
	 */
	public String getKeyPrefixFor(String cacheName) {

		Assert.notNull(cacheName, "Cache name must not be null");

		return this.keyPrefix.compute(cacheName);
	}

	/**
	 * @return never {@literal null}.
	 */
	public SerializationPair<String> getKeySerializationPair() {
		return this.keySerializationPair;
	}

	/**
	 * @return never {@literal null}.
	 */
	public SerializationPair<Object> getValueSerializationPair() {
		return this.valueSerializationPair;
	}

	/**
	 * Returns a computed {@link Duration TTL expiration timeout} based on cache entry key/value if a {@link TtlFunction}
	 * was confiugred using {@link #entryTtl(TtlFunction)}.
	 * <p>
	 * Otherwise, returns the user-provided, fixed {@link Duration} if {@link #entryTtl(Duration)} was called during cache
	 * configuration.
	 *
	 * @return the configured {@link Duration TTL expiration}.
	 * @deprecated since 3.2. Use {@link #getTtlFunction()} instead.
	 */
	@Deprecated(since = "3.2")
	public Duration getTtl() {
		return getTtlFunction().getTimeToLive(Object.class, null);
	}

	/**
	 * Gets the {@link TtlFunction} used to compute a cache key {@literal time-to-live (TTL) expiration}.
	 *
	 * @return the {@link TtlFunction} used to compute expiration time (TTL) for cache entries; never {@literal null}.
	 */
	public TtlFunction getTtlFunction() {
		return this.ttlFunction;
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
			throw new IllegalStateException(("'%s' returned by getConversionService() does not allow Converter registration;"
					+ " Please make sure to provide a ConversionService that implements ConverterRegistry")
					.formatted(getConversionService().getClass().getName()));
		}

		registryConsumer.accept((ConverterRegistry) getConversionService());
	}

	/**
	 * Registers default cache {@link Converter key converters}.
	 * <p>
	 * The following converters get registered:
	 * <ul>
	 * <li>{@link String} to {@code byte byte[]} using UTF-8 encoding.</li>
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
