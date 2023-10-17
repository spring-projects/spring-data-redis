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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.cache.transaction.AbstractTransactionSupportingCacheManager;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.util.RedisAssertions;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * {@link CacheManager} implementation for Redis backed by {@link RedisCache}.
 * <p>
 * This {@link CacheManager} creates {@link Cache caches} on first write, by default. Empty {@link Cache caches}
 * are not visible in Redis due to how Redis represents empty data structures.
 * <p>
 * {@link Cache Caches} requiring a different {@link RedisCacheConfiguration cache configuration} than the
 * {@link RedisCacheConfiguration#defaultCacheConfig() default cache configuration} can be specified via
 * {@link RedisCacheManagerBuilder#withInitialCacheConfigurations(Map)} or individually using
 * {@link RedisCacheManagerBuilder#withCacheConfiguration(String, RedisCacheConfiguration)}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Yanming Zhou
 * @author John Blum
 * @see org.springframework.cache.transaction.AbstractTransactionSupportingCacheManager
 * @see org.springframework.data.redis.connection.RedisConnectionFactory
 * @see org.springframework.data.redis.cache.RedisCacheConfiguration
 * @see org.springframework.data.redis.cache.RedisCacheWriter
 * @see org.springframework.data.redis.cache.RedisCache
 * @since 2.0
 */
public class RedisCacheManager extends AbstractTransactionSupportingCacheManager {

	protected static final boolean DEFAULT_ALLOW_RUNTIME_CACHE_CREATION = true;

	private final boolean allowRuntimeCacheCreation;

	private final RedisCacheConfiguration defaultCacheConfiguration;

	private final RedisCacheWriter cacheWriter;

	private final Map<String, RedisCacheConfiguration> initialCacheConfiguration;

	/**
	 * Creates a new {@link RedisCacheManager} initialized with the given {@link RedisCacheWriter} and default
	 * {@link RedisCacheConfiguration}.
	 * <p>
	 * Allows {@link RedisCache cache} creation at runtime.
	 *
	 * @param cacheWriter {@link RedisCacheWriter} used to perform {@link RedisCache} operations
	 * by executing appropriate Redis commands; must not be {@literal null}.
	 * @param defaultCacheConfiguration {@link RedisCacheConfiguration} applied to new {@link RedisCache Redis caches}
	 * by default when no cache-specific {@link RedisCacheConfiguration} is provided; must not be {@literal null}.
	 * @throws IllegalArgumentException if either the given {@link RedisCacheWriter} or {@link RedisCacheConfiguration}
	 * are {@literal null}.
	 * @see org.springframework.data.redis.cache.RedisCacheConfiguration
	 * @see org.springframework.data.redis.cache.RedisCacheWriter
	 */
	public RedisCacheManager(RedisCacheWriter cacheWriter, RedisCacheConfiguration defaultCacheConfiguration) {
		this(cacheWriter, defaultCacheConfiguration, DEFAULT_ALLOW_RUNTIME_CACHE_CREATION);
	}

	/**
	 * Creates a new {@link RedisCacheManager} initialized with the given {@link RedisCacheWriter}
	 * and default {@link RedisCacheConfiguration} along with whether to allow cache creation at runtime.
	 *
	 * @param cacheWriter {@link RedisCacheWriter} used to perform {@link RedisCache} operations
	 * by executing appropriate Redis commands; must not be {@literal null}.
	 * @param defaultCacheConfiguration {@link RedisCacheConfiguration} applied to new {@link RedisCache Redis caches}
	 * by default when no cache-specific {@link RedisCacheConfiguration} is provided; must not be {@literal null}.
	 * @param allowRuntimeCacheCreation boolean specifying whether to allow creation of undeclared caches at runtime;
	 * {@literal true} by default. Maybe just use {@link RedisCacheConfiguration#defaultCacheConfig()}.
	 * @throws IllegalArgumentException if either the given {@link RedisCacheWriter} or {@link RedisCacheConfiguration}
	 * are {@literal null}.
	 * @see org.springframework.data.redis.cache.RedisCacheConfiguration
	 * @see org.springframework.data.redis.cache.RedisCacheWriter
	 * @since 2.0.4
	 */
	private RedisCacheManager(RedisCacheWriter cacheWriter, RedisCacheConfiguration defaultCacheConfiguration,
			boolean allowRuntimeCacheCreation) {

		this.defaultCacheConfiguration = RedisAssertions.requireNonNull(defaultCacheConfiguration,
				"DefaultCacheConfiguration must not be null");

		this.cacheWriter = RedisAssertions.requireNonNull(cacheWriter, "CacheWriter must not be null");
		this.initialCacheConfiguration = new LinkedHashMap<>();
		this.allowRuntimeCacheCreation = allowRuntimeCacheCreation;
	}

	/**
	 * Creates a new {@link RedisCacheManager} initialized with the given {@link RedisCacheWriter} and a default
	 * {@link RedisCacheConfiguration} along with an optional, initial set of {@link String cache names}
	 * used to create {@link RedisCache Redis caches} on startup.
	 * <p>
	 * Allows {@link RedisCache cache} creation at runtime.
	 *
	 * @param cacheWriter {@link RedisCacheWriter} used to perform {@link RedisCache} operations
	 * by executing appropriate Redis commands; must not be {@literal null}.
	 * @param defaultCacheConfiguration {@link RedisCacheConfiguration} applied to new {@link RedisCache Redis caches}
	 * by default when no cache-specific {@link RedisCacheConfiguration} is provided; must not be {@literal null}.
	 * @param initialCacheNames optional set of {@link String cache names} used to create {@link RedisCache Redis caches}
	 * on startup. The default {@link RedisCacheConfiguration} will be applied to each cache.
	 * @throws IllegalArgumentException if either the given {@link RedisCacheWriter} or {@link RedisCacheConfiguration}
	 * are {@literal null}.
	 * @see org.springframework.data.redis.cache.RedisCacheConfiguration
	 * @see org.springframework.data.redis.cache.RedisCacheWriter
	 */
	public RedisCacheManager(RedisCacheWriter cacheWriter, RedisCacheConfiguration defaultCacheConfiguration,
			String... initialCacheNames) {

		this(cacheWriter, defaultCacheConfiguration, DEFAULT_ALLOW_RUNTIME_CACHE_CREATION, initialCacheNames);
	}

	/**
	 * Creates a new {@link RedisCacheManager} initialized with the given {@link RedisCacheWriter} and default
	 * {@link RedisCacheConfiguration} along with whether to allow cache creation at runtime.
	 * <p>
	 * Additionally, the optional, initial set of {@link String cache names} will be used to
	 * create {@link RedisCache Redis caches} on startup.
	 *
	 * @param cacheWriter {@link RedisCacheWriter} used to perform {@link RedisCache} operations
	 * by executing appropriate Redis commands; must not be {@literal null}.
	 * @param defaultCacheConfiguration {@link RedisCacheConfiguration} applied to new {@link RedisCache Redis caches}
	 * by default when no cache-specific {@link RedisCacheConfiguration} is provided; must not be {@literal null}.
	 * @param allowRuntimeCacheCreation boolean specifying whether to allow creation of undeclared caches at runtime;
	 * {@literal true} by default. Maybe just use {@link RedisCacheConfiguration#defaultCacheConfig()}.
	 * @param initialCacheNames optional set of {@link String cache names} used to create {@link RedisCache Redis caches}
	 * on startup. The default {@link RedisCacheConfiguration} will be applied to each cache.
	 * @throws IllegalArgumentException if either the given {@link RedisCacheWriter} or {@link RedisCacheConfiguration}
	 * are {@literal null}.
	 * @see org.springframework.data.redis.cache.RedisCacheConfiguration
	 * @see org.springframework.data.redis.cache.RedisCacheWriter
	 * @since 2.0.4
	 */
	public RedisCacheManager(RedisCacheWriter cacheWriter, RedisCacheConfiguration defaultCacheConfiguration,
			boolean allowRuntimeCacheCreation, String... initialCacheNames) {

		this(cacheWriter, defaultCacheConfiguration, allowRuntimeCacheCreation);

		for (String cacheName : initialCacheNames) {
			this.initialCacheConfiguration.put(cacheName, defaultCacheConfiguration);
		}
	}

	/**
	 * Creates new {@link RedisCacheManager} using given {@link RedisCacheWriter} and default
	 * {@link RedisCacheConfiguration}.
	 * <p>
	 * Additionally, an initial {@link RedisCache} will be created and configured using the associated
	 * {@link RedisCacheConfiguration} for each {@link String named} {@link RedisCache} in the given {@link Map}.
	 * <p>
	 * Allows {@link RedisCache cache} creation at runtime.
	 *
	 * @param cacheWriter {@link RedisCacheWriter} used to perform {@link RedisCache} operations
	 * by executing appropriate Redis commands; must not be {@literal null}.
	 * @param defaultCacheConfiguration {@link RedisCacheConfiguration} applied to new {@link RedisCache Redis caches}
	 * by default when no cache-specific {@link RedisCacheConfiguration} is provided; must not be {@literal null}.
	 * @param initialCacheConfigurations {@link Map} of declared, known {@link String cache names} along with associated
	 * {@link RedisCacheConfiguration} used to create and configure {@link RedisCache Reds caches} on startup;
	 * must not be {@literal null}.
	 * @throws IllegalArgumentException if either the given {@link RedisCacheWriter} or {@link RedisCacheConfiguration}
	 * are {@literal null}.
	 * @see org.springframework.data.redis.cache.RedisCacheConfiguration
	 * @see org.springframework.data.redis.cache.RedisCacheWriter
	 */
	public RedisCacheManager(RedisCacheWriter cacheWriter, RedisCacheConfiguration defaultCacheConfiguration,
			Map<String, RedisCacheConfiguration> initialCacheConfigurations) {

		this(cacheWriter, defaultCacheConfiguration, DEFAULT_ALLOW_RUNTIME_CACHE_CREATION, initialCacheConfigurations);
	}

	/**
	 * Creates a new {@link RedisCacheManager} initialized with the given {@link RedisCacheWriter} and a default
	 * {@link RedisCacheConfiguration}, and whether to allow {@link RedisCache} creation at runtime.
	 * <p>
	 * Additionally, an initial {@link RedisCache} will be created and configured using the associated
	 * {@link RedisCacheConfiguration} for each {@link String named} {@link RedisCache} in the given {@link Map}.
	 *
	 * @param cacheWriter {@link RedisCacheWriter} used to perform {@link RedisCache} operations
	 * by executing appropriate Redis commands; must not be {@literal null}.
	 * @param defaultCacheConfiguration {@link RedisCacheConfiguration} applied to new {@link RedisCache Redis caches}
	 * by default when no cache-specific {@link RedisCacheConfiguration} is provided; must not be {@literal null}.
	 * @param allowRuntimeCacheCreation boolean specifying whether to allow creation of undeclared caches at runtime;
	 * {@literal true} by default. Maybe just use {@link RedisCacheConfiguration#defaultCacheConfig()}.
	 * @param initialCacheConfigurations {@link Map} of declared, known {@link String cache names} along with the
	 * associated {@link RedisCacheConfiguration} used to create and configure {@link RedisCache Redis caches}
	 * on startup; must not be {@literal null}.
	 * @throws IllegalArgumentException if either the given {@link RedisCacheWriter} or {@link RedisCacheConfiguration}
	 * are {@literal null}.
	 * @see org.springframework.data.redis.cache.RedisCacheConfiguration
	 * @see org.springframework.data.redis.cache.RedisCacheWriter
	 * @since 2.0.4
	 */
	public RedisCacheManager(RedisCacheWriter cacheWriter, RedisCacheConfiguration defaultCacheConfiguration,
			boolean allowRuntimeCacheCreation, Map<String, RedisCacheConfiguration> initialCacheConfigurations) {

		this(cacheWriter, defaultCacheConfiguration, allowRuntimeCacheCreation);

		Assert.notNull(initialCacheConfigurations, "InitialCacheConfigurations must not be null");

		this.initialCacheConfiguration.putAll(initialCacheConfigurations);
	}

	/**
	 * @deprecated since 3.2. Use {@link RedisCacheManager#RedisCacheManager(RedisCacheWriter, RedisCacheConfiguration, boolean, Map)} instead.
	 */
	@Deprecated(since = "3.2")
	public RedisCacheManager(RedisCacheWriter cacheWriter, RedisCacheConfiguration defaultCacheConfiguration,
			Map<String, RedisCacheConfiguration> initialCacheConfigurations, boolean allowRuntimeCacheCreation) {

		this(cacheWriter, defaultCacheConfiguration, allowRuntimeCacheCreation, initialCacheConfigurations);
	}

	/**
	 * Factory method returning a {@literal Builder} used to construct and configure a {@link RedisCacheManager}.
	 *
	 * @return new {@link RedisCacheManagerBuilder}.
	 * @since 2.3
	 */
	public static RedisCacheManagerBuilder builder() {
		return new RedisCacheManagerBuilder();
	}

	/**
	 * Factory method returning a {@literal Builder} used to construct and configure a {@link RedisCacheManager}
	 * initialized with the given {@link RedisCacheWriter}.
	 *
	 * @param cacheWriter {@link RedisCacheWriter} used to perform {@link RedisCache} operations
	 * by executing appropriate Redis commands; must not be {@literal null}.
	 * @return new {@link RedisCacheManagerBuilder}.
	 * @throws IllegalArgumentException if the given {@link RedisCacheWriter} is {@literal null}.
	 * @see org.springframework.data.redis.cache.RedisCacheWriter
	 */
	public static RedisCacheManagerBuilder builder(RedisCacheWriter cacheWriter) {

		Assert.notNull(cacheWriter, "CacheWriter must not be null");

		return RedisCacheManagerBuilder.fromCacheWriter(cacheWriter);
	}

	/**
	 * Factory method returning a {@literal Builder} used to construct and configure a {@link RedisCacheManager}
	 * initialized with the given {@link RedisConnectionFactory}.
	 *
	 * @param connectionFactory {@link RedisConnectionFactory} used by the {@link RedisCacheManager}
	 * to acquire connections to Redis when performing {@link RedisCache} operations; must not be {@literal null}.
	 * @return new {@link RedisCacheManagerBuilder}.
	 * @throws IllegalArgumentException if the given {@link RedisConnectionFactory} is {@literal null}.
	 * @see org.springframework.data.redis.connection.RedisConnectionFactory
	 */
	public static RedisCacheManagerBuilder builder(RedisConnectionFactory connectionFactory) {

		Assert.notNull(connectionFactory, "ConnectionFactory must not be null");

		return RedisCacheManagerBuilder.fromConnectionFactory(connectionFactory);
	}

	/**
	 * Factory method used to construct a new {@link RedisCacheManager} initialized with the given
	 * {@link RedisConnectionFactory} and using {@link RedisCacheConfiguration#defaultCacheConfig() defaults} for caching.
	 * <dl>
	 * <dt>locking</dt>
	 * <dd>disabled</dd>
	 * <dt>batch strategy</dt>
	 * <dd>{@link BatchStrategies#keys()}</dd>
	 * <dt>cache configuration</dt>
	 * <dd>{@link RedisCacheConfiguration#defaultCacheConfig()}</dd>
	 * <dt>initial caches</dt>
	 * <dd>none</dd>
	 * <dt>transaction aware</dt>
	 * <dd>no</dd>
	 * <dt>in-flight cache creation</dt>
	 * <dd>enabled</dd>
	 * </dl>
	 *
	 * @param connectionFactory {@link RedisConnectionFactory} used by the {@link RedisCacheManager}
	 * to acquire connections to Redis when performing {@link RedisCache} operations; must not be {@literal null}.
	 * @return new {@link RedisCacheManager}.
	 * @throws IllegalArgumentException if the given {@link RedisConnectionFactory} is {@literal null}.
	 * @see org.springframework.data.redis.connection.RedisConnectionFactory
	 */
	public static RedisCacheManager create(RedisConnectionFactory connectionFactory) {

		Assert.notNull(connectionFactory, "ConnectionFactory must not be null");

		RedisCacheWriter cacheWriter = RedisCacheWriter.nonLockingRedisCacheWriter(connectionFactory);
		RedisCacheConfiguration cacheConfiguration = RedisCacheConfiguration.defaultCacheConfig();

		return new RedisCacheManager(cacheWriter, cacheConfiguration);
	}

	/**
	 * Determines whether {@link RedisCache Redis caches} are allowed to be created at runtime.
	 *
	 * @return a boolean value indicating whether {@link RedisCache Redis caches} are allowed to be created at runtime.
	 */
	public boolean isAllowRuntimeCacheCreation() {
		return this.allowRuntimeCacheCreation;
	}

	/**
	 * Return an {@link Collections#unmodifiableMap(Map) unmodifiable Map} containing {@link String caches name} mapped to
	 * the {@link RedisCache} {@link RedisCacheConfiguration configuration}.
	 *
	 * @return unmodifiable {@link Map} containing {@link String cache name}
	 * / {@link RedisCacheConfiguration configuration} pairs.
	 */
	public Map<String, RedisCacheConfiguration> getCacheConfigurations() {

		Map<String, RedisCacheConfiguration> cacheConfigurationMap = new HashMap<>(getCacheNames().size());

		getCacheNames().forEach(cacheName -> {
			RedisCache cache = (RedisCache) lookupCache(cacheName);
			RedisCacheConfiguration cacheConfiguration = cache != null ? cache.getCacheConfiguration() : null;
			cacheConfigurationMap.put(cacheName, cacheConfiguration);
		});

		return Collections.unmodifiableMap(cacheConfigurationMap);
	}

	/**
	 * Gets the default {@link RedisCacheConfiguration} applied to new {@link RedisCache} instances on creation when
	 * custom, non-specific {@link RedisCacheConfiguration} was not provided.
	 *
	 * @return the default {@link RedisCacheConfiguration}.
	 */
	protected RedisCacheConfiguration getDefaultCacheConfiguration() {
		return this.defaultCacheConfiguration;
	}

	/**
	 * Gets a {@link Map} of {@link String cache names} to {@link RedisCacheConfiguration} objects as the initial set
	 * of {@link RedisCache Redis caches} to create on startup.
	 *
	 * @return a {@link Map} of {@link String cache names} to {@link RedisCacheConfiguration} objects.
	 */
	protected Map<String, RedisCacheConfiguration> getInitialCacheConfiguration() {
		return Collections.unmodifiableMap(this.initialCacheConfiguration);
	}

	/**
	 * Returns a reference to the configured {@link RedisCacheWriter} used to perform {@link RedisCache} operations,
	 * such as reading from and writing to the cache.
	 *
	 * @return a reference to the configured {@link RedisCacheWriter}.
	 * @see org.springframework.data.redis.cache.RedisCacheWriter
	 */
	protected RedisCacheWriter getCacheWriter() {
		return this.cacheWriter;
	}

	@Override
	protected RedisCache getMissingCache(String name) {
		return isAllowRuntimeCacheCreation() ? createRedisCache(name, getDefaultCacheConfiguration()) : null;
	}

	/**
	 * Creates a new {@link RedisCache} with given {@link String name} and {@link RedisCacheConfiguration}.
	 *
	 * @param name {@link String name} for the {@link RedisCache}; must not be {@literal null}.
	 * @param cacheConfiguration {@link RedisCacheConfiguration} used to configure the {@link RedisCache};
	 * resolves to the {@link #getDefaultCacheConfiguration()} if {@literal null}.
	 * @return a new {@link RedisCache} instance; never {@literal null}.
	 */
	protected RedisCache createRedisCache(String name, @Nullable RedisCacheConfiguration cacheConfiguration) {
		return new RedisCache(name, getCacheWriter(), resolveCacheConfiguration(cacheConfiguration));
	}

	@Override
	protected Collection<RedisCache> loadCaches() {

		return getInitialCacheConfiguration().entrySet().stream()
				.map(entry -> createRedisCache(entry.getKey(), entry.getValue())).toList();
	}

	private RedisCacheConfiguration resolveCacheConfiguration(@Nullable RedisCacheConfiguration cacheConfiguration) {
		return cacheConfiguration != null ? cacheConfiguration : getDefaultCacheConfiguration();
	}

	/**
	 * {@literal Builder} for creating a {@link RedisCacheManager}.
	 *
	 * @author Christoph Strobl
	 * @author Mark Paluch
	 * @author Kezhu Wang
	 * @author John Blum
	 * @since 2.0
	 */
	public static class RedisCacheManagerBuilder {

		/**
		 * Factory method returning a new {@literal Builder} used to create and configure a {@link RedisCacheManager} using
		 * the given {@link RedisCacheWriter}.
		 *
		 * @param cacheWriter {@link RedisCacheWriter} used to perform {@link RedisCache} operations
		 * by executing appropriate Redis commands; must not be {@literal null}.
		 * @return new {@link RedisCacheManagerBuilder}.
		 * @throws IllegalArgumentException if the given {@link RedisCacheWriter} is {@literal null}.
		 * @see org.springframework.data.redis.cache.RedisCacheWriter
		 */
		public static RedisCacheManagerBuilder fromCacheWriter(RedisCacheWriter cacheWriter) {
			return new RedisCacheManagerBuilder(RedisAssertions.requireNonNull(cacheWriter, "CacheWriter must not be null"));
		}

		/**
		 * Factory method returning a new {@literal Builder} used to create and configure a {@link RedisCacheManager} using
		 * the given {@link RedisConnectionFactory}.
		 *
		 * @param connectionFactory {@link RedisConnectionFactory} used by the {@link RedisCacheManager}
		 * to acquire connections to Redis when performing {@link RedisCache} operations; must not be {@literal null}.
		 * @return new {@link RedisCacheManagerBuilder}.
		 * @throws IllegalArgumentException if the given {@link RedisConnectionFactory} is {@literal null}.
		 * @see org.springframework.data.redis.connection.RedisConnectionFactory
		 */
		public static RedisCacheManagerBuilder fromConnectionFactory(RedisConnectionFactory connectionFactory) {

			Assert.notNull(connectionFactory, "ConnectionFactory must not be null");

			RedisCacheWriter cacheWriter = RedisCacheWriter.nonLockingRedisCacheWriter(connectionFactory);

			return new RedisCacheManagerBuilder(cacheWriter);
		}

		private boolean allowRuntimeCacheCreation = true;
		private boolean enableTransactions;

		private CacheStatisticsCollector statisticsCollector = CacheStatisticsCollector.none();

		private final Map<String, RedisCacheConfiguration> initialCaches = new LinkedHashMap<>();

		private RedisCacheConfiguration defaultCacheConfiguration = RedisCacheConfiguration.defaultCacheConfig();

		private @Nullable RedisCacheWriter cacheWriter;

		private RedisCacheManagerBuilder() {}

		private RedisCacheManagerBuilder(RedisCacheWriter cacheWriter) {
			this.cacheWriter = cacheWriter;
		}

		/**
		 * Configure whether to allow cache creation at runtime.
		 *
		 * @param allowRuntimeCacheCreation boolean to allow creation of undeclared caches at runtime;
		 * {@literal true} by default.
		 * @return this {@link RedisCacheManagerBuilder}.
		 */
		public RedisCacheManagerBuilder allowCreateOnMissingCache(boolean allowRuntimeCacheCreation) {
			this.allowRuntimeCacheCreation = allowRuntimeCacheCreation;
			return this;
		}

		/**
		 * Disable {@link RedisCache} creation at runtime for non-configured, undeclared caches.
		 * <p>
		 * {@link RedisCacheManager#getMissingCache(String)} returns {@literal null} for any non-configured,
		 * undeclared {@link Cache} instead of a new {@link RedisCache} instance.
		 * This allows the {@link org.springframework.cache.support.CompositeCacheManager} to participate.
		 *
		 * @return this {@link RedisCacheManagerBuilder}.
		 * @see #allowCreateOnMissingCache(boolean)
		 * @see #enableCreateOnMissingCache()
		 * @since 2.0.4
		 */
		public RedisCacheManagerBuilder disableCreateOnMissingCache() {
			return allowCreateOnMissingCache(false);
		}

		/**
		 * Enables {@link RedisCache} creation at runtime for unconfigured, undeclared caches.
		 *
		 * @return this {@link RedisCacheManagerBuilder}.
		 * @see #allowCreateOnMissingCache(boolean)
		 * @see #disableCreateOnMissingCache()
		 * @since 2.0.4
		 */
		public RedisCacheManagerBuilder enableCreateOnMissingCache() {
			return allowCreateOnMissingCache(true);
		}

		/**
		 * Returns the default {@link RedisCacheConfiguration}.
		 *
		 * @return the default {@link RedisCacheConfiguration}.
		 */
		public RedisCacheConfiguration cacheDefaults() {
			return this.defaultCacheConfiguration;
		}

		/**
		 * Define a default {@link RedisCacheConfiguration} applied to dynamically created {@link RedisCache}s.
		 *
		 * @param defaultCacheConfiguration must not be {@literal null}.
		 * @return this {@link RedisCacheManagerBuilder}.
		 */
		public RedisCacheManagerBuilder cacheDefaults(RedisCacheConfiguration defaultCacheConfiguration) {

			Assert.notNull(defaultCacheConfiguration, "DefaultCacheConfiguration must not be null");

			this.defaultCacheConfiguration = defaultCacheConfiguration;

			return this;
		}

		/**
		 * Configure a {@link RedisCacheWriter}.
		 *
		 * @param cacheWriter must not be {@literal null}.
		 * @return this {@link RedisCacheManagerBuilder}.
		 * @since 2.3
		 */
		public RedisCacheManagerBuilder cacheWriter(RedisCacheWriter cacheWriter) {
			this.cacheWriter = RedisAssertions.requireNonNull(cacheWriter, "CacheWriter must not be null");
			return this;
		}

		/**
		 * Enables cache statistics.
		 *
		 * @return this {@link RedisCacheManagerBuilder}.
		 */
		public RedisCacheManagerBuilder enableStatistics() {
			this.statisticsCollector = CacheStatisticsCollector.create();
			return this;
		}

		/**
		 * Append a {@link Set} of cache names to be pre initialized with current {@link RedisCacheConfiguration}.
		 * <strong>NOTE:</strong> This calls depends on {@link #cacheDefaults(RedisCacheConfiguration)} using whatever
		 * default {@link RedisCacheConfiguration} is present at the time of invoking this method.
		 *
		 * @param cacheNames must not be {@literal null}.
		 * @return this {@link RedisCacheManagerBuilder}.
		 */
		public RedisCacheManagerBuilder initialCacheNames(Set<String> cacheNames) {

			RedisAssertions.requireNonNull(cacheNames, "CacheNames must not be null")
					.forEach(it -> withCacheConfiguration(it, defaultCacheConfiguration));

			return this;
		}

		/**
		 * Enable {@link RedisCache}s to synchronize cache put/evict operations with ongoing Spring-managed transactions.
		 *
		 * @return this {@link RedisCacheManagerBuilder}.
		 */
		public RedisCacheManagerBuilder transactionAware() {
			this.enableTransactions = true;
			return this;
		}

		/**
		 * Registers the given {@link String cache name} and {@link RedisCacheConfiguration} used to create
		 * and configure a {@link RedisCache} on startup.
		 *
		 * @param cacheName {@link String name} of the cache to register for creation on startup.
		 * @param cacheConfiguration {@link RedisCacheConfiguration} used to configure the new cache on startup.
		 * @return this {@link RedisCacheManagerBuilder}.
		 * @since 2.2
		 */
		public RedisCacheManagerBuilder withCacheConfiguration(String cacheName,
				RedisCacheConfiguration cacheConfiguration) {

			Assert.notNull(cacheName, "CacheName must not be null");
			Assert.notNull(cacheConfiguration, "CacheConfiguration must not be null");

			this.initialCaches.put(cacheName, cacheConfiguration);

			return this;
		}

		/**
		 * Append a {@link Map} of cache name/{@link RedisCacheConfiguration} pairs to be pre initialized.
		 *
		 * @param cacheConfigurations must not be {@literal null}.
		 * @return this {@link RedisCacheManagerBuilder}.
		 */
		public RedisCacheManagerBuilder withInitialCacheConfigurations(
				Map<String, RedisCacheConfiguration> cacheConfigurations) {

			RedisAssertions.requireNonNull(cacheConfigurations, "CacheConfigurations must not be null")
					.forEach((cacheName, cacheConfiguration) -> RedisAssertions.requireNonNull(cacheConfiguration,
						"RedisCacheConfiguration for cache [%s] must not be null", cacheName));

			this.initialCaches.putAll(cacheConfigurations);

			return this;
		}

		/**
		 * Get the {@link RedisCacheConfiguration} for a given cache by its name.
		 *
		 * @param cacheName must not be {@literal null}.
		 * @return {@link Optional#empty()} if no {@link RedisCacheConfiguration} set for the given cache name.
		 * @since 2.2
		 */
		public Optional<RedisCacheConfiguration> getCacheConfigurationFor(String cacheName) {
			return Optional.ofNullable(this.initialCaches.get(cacheName));
		}

		/**
		 * Get the {@link Set} of cache names for which the builder holds {@link RedisCacheConfiguration configuration}.
		 *
		 * @return an unmodifiable {@link Set} holding the name of caches
		 * for which a {@link RedisCacheConfiguration configuration} has been set.
		 * @since 2.2
		 */
		public Set<String> getConfiguredCaches() {
			return Collections.unmodifiableSet(this.initialCaches.keySet());
		}

		/**
		 * Create new instance of {@link RedisCacheManager} with configuration options applied.
		 *
		 * @return new instance of {@link RedisCacheManager}.
		 */
		public RedisCacheManager build() {

			Assert.state(cacheWriter != null, "CacheWriter must not be null;"
					+ " You can provide one via 'RedisCacheManagerBuilder#cacheWriter(RedisCacheWriter)'");

			RedisCacheWriter resolvedCacheWriter = !CacheStatisticsCollector.none().equals(this.statisticsCollector)
					? this.cacheWriter.withStatisticsCollector(this.statisticsCollector)
					: this.cacheWriter;

			RedisCacheManager cacheManager = newRedisCacheManager(resolvedCacheWriter);

			cacheManager.setTransactionAware(this.enableTransactions);

			return cacheManager;
		}

		private RedisCacheManager newRedisCacheManager(RedisCacheWriter cacheWriter) {
			return new RedisCacheManager(cacheWriter, cacheDefaults(), this.allowRuntimeCacheCreation, this.initialCaches);
		}
	}
}
