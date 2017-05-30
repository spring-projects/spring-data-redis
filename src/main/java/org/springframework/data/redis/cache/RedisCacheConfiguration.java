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

import java.time.Duration;
import java.util.Optional;

import org.springframework.cache.Cache;
import org.springframework.data.redis.serializer.JdkSerializationRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializationContext.SerializationPair;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.util.Assert;

/**
 * Immutable {@link RedisCacheConfiguration} helps customizing {@link RedisCache} behaviour such as caching
 * {@literal null} values, cache key prefixes and binary serialization. <br />
 * Start with {@link RedisCacheConfiguration#defaultCacheConfig()} and customize {@link RedisCache} behaviour from
 * there on.
 *
 * @author Christoph Strobl
 * @since 2.0
 */
public class RedisCacheConfiguration {

	private final Duration ttl;
	private final boolean cacheNullValues;
	private final String keyPrefix;
	private final boolean usePrefix;

	private final SerializationPair<String> keySerializationPair;
	private final SerializationPair<?> valueSerializationPair;

	private RedisCacheConfiguration(Duration ttl, Boolean cacheNullValues, Boolean usePrefix, String keyPrefix,
			SerializationPair<String> keySerializationPair, SerializationPair<?> valueSerializationPair) {

		this.ttl = ttl;
		this.cacheNullValues = cacheNullValues;
		this.usePrefix = usePrefix;
		this.keyPrefix = keyPrefix;
		this.keySerializationPair = keySerializationPair;
		this.valueSerializationPair = valueSerializationPair;
	}

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
	 * <dd>StringRedisSerializer.class</dd>
	 * <dt>value serializer</dt>
	 * <dd>JdkSerializationRedisSerializer.class</dd>
	 * </dl>
	 * 
	 * @return new {@link RedisCacheConfiguration}.
	 */
	public static RedisCacheConfiguration defaultCacheConfig() {

		return new RedisCacheConfiguration(Duration.ZERO, true, true, null,
				SerializationPair.fromSerializer(new StringRedisSerializer()),
				SerializationPair.fromSerializer(new JdkSerializationRedisSerializer()));
	}

	/**
	 * Set the ttl to apply for cache entries. Use {@link Duration#ZERO} to have an eternal cache.
	 *
	 * @param ttl must not be {@literal null}.
	 * @return new {@link RedisCacheConfiguration}.
	 */
	public RedisCacheConfiguration entryTtl(Duration ttl) {

		Assert.notNull(ttl, "Ttl must not be null!");

		return new RedisCacheConfiguration(ttl, cacheNullValues, usePrefix, keyPrefix, keySerializationPair,
				valueSerializationPair);
	}

	/**
	 * Use the given prefix instead of the default one.
	 *
	 * @param prefix must not be {@literal null}.
	 * @return new {@link RedisCacheConfiguration}.
	 */
	public RedisCacheConfiguration prefixKeysWith(String prefix) {

		Assert.notNull(prefix, "Prefix must not be null!");

		return new RedisCacheConfiguration(ttl, cacheNullValues, true, prefix, keySerializationPair,
				valueSerializationPair);
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
		return new RedisCacheConfiguration(ttl, false, usePrefix, keyPrefix, keySerializationPair, valueSerializationPair);
	}

	/**
	 * Disable using cache key prefixes. <br />
	 * <strong>NOTE</strong>: {@link Cache#clear()} might result in unintended removal of {@literal key}s in Redis. Make
	 * sure to use a dedicated Redis instance when disabling prefixes.
	 *
	 * @return new {@link RedisCacheConfiguration}.
	 */
	public RedisCacheConfiguration disableKeyPrefix() {

		return new RedisCacheConfiguration(ttl, cacheNullValues, false, keyPrefix, keySerializationPair,
				valueSerializationPair);
	}

	/**
	 * Define the {@link SerializationPair} used for de-/serializing cache keys.
	 *
	 * @param keySerializationPair must not be {@literal null}.
	 * @return new {@link RedisCacheConfiguration}.
	 */
	public RedisCacheConfiguration serializeKeysWith(SerializationPair<String> keySerializationPair) {

		Assert.notNull(keySerializationPair, "KeySerializationPair must not be null!");

		return new RedisCacheConfiguration(ttl, cacheNullValues, usePrefix, keyPrefix, keySerializationPair,
				valueSerializationPair);
	}

	/**
	 * Define the {@link SerializationPair} used for de-/serializing cache values.
	 *
	 * @param valueSerializationPair must not be {@literal null}.
	 * @return new {@link RedisCacheConfiguration}.
	 */
	public RedisCacheConfiguration serializeValuesWith(SerializationPair<?> valueSerializationPair) {

		Assert.notNull(valueSerializationPair, "ValueSerializationPair must not be null!");

		return new RedisCacheConfiguration(ttl, cacheNullValues, usePrefix, keyPrefix, keySerializationPair,
				valueSerializationPair);
	}

	/**
	 * @return never {@literal null}.
	 */
	public Optional<String> getKeyPrefix() {
		return Optional.ofNullable(keyPrefix);
	}

	/**
	 * @return {@literal true} if cache keys need to be prefixed with the {@link #getKeyPrefix()} if present or the
	 *         default which resolves to {@link Cache#getName()}.
	 */
	public boolean usePrefix() {
		return usePrefix;
	}

	/**
	 * @return {@literal true} if caching {@literal null} is allowed.
	 */
	public boolean getAllowCacheNullValues() {
		return cacheNullValues;
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
	public SerializationPair getValueSerializationPair() {
		return valueSerializationPair;
	}

	/**
	 * @return The expiration time (ttl) for cache entries. Never {@literal null}.
	 */
	public Duration getTtl() {
		return ttl != null ? ttl : Duration.ZERO;
	}

}
