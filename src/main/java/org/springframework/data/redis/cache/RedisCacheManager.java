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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.cache.transaction.AbstractTransactionSupportingCacheManager;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

/**
 * @author Christoph Strobl
 * @since 2.0
 */
public class RedisCacheManager extends AbstractTransactionSupportingCacheManager {

	private final RedisCacheWriter cacheWriter;
	private final RedisCacheConfiguration defaultCacheConfig;
	private final Map<String, RedisCacheConfiguration> initialCacheConfiguration;

	public RedisCacheManager(RedisCacheWriter cacheWriter, RedisCacheConfiguration defaultCacheConfiguration) {

		Assert.notNull(cacheWriter, "CacheWriter must not be null!");
		Assert.notNull(defaultCacheConfiguration, "DefaultCacheConfiguration must not be null!");

		this.cacheWriter = cacheWriter;
		this.defaultCacheConfig = defaultCacheConfiguration;
		this.initialCacheConfiguration = new LinkedHashMap<>();
	}

	/**
	 * Creates new {@link RedisCacheManager} using given {@link RedisCacheWriter} and default
	 * {@link RedisCacheConfiguration}.
	 *
	 * @param cacheWriter must not be {@literal null}.
	 * @param defaultCacheConfiguration must not be {@literal null}. Maybe just use
	 *          {@link RedisCacheConfiguration#defaultCacheConfig()}.
	 * @param initialCacheNames optional set of known cache names that will be created with given
	 *          {@literal defaultCacheConfiguration}.
	 */
	public RedisCacheManager(RedisCacheWriter cacheWriter, RedisCacheConfiguration defaultCacheConfiguration,
							 String... initialCacheNames) {

		this(cacheWriter, defaultCacheConfiguration);

		for (String cacheName : initialCacheNames) {
			this.initialCacheConfiguration.put(cacheName, defaultCacheConfiguration);
		}
	}

	/**
	 * Creates new {@link RedisCacheManager} using given {@link RedisCacheWriter} and default
	 * {@link RedisCacheConfiguration}.
	 *
	 * @param cacheWriter must not be {@literal null}.
	 * @param defaultCacheConfiguration must not be {@literal null}. Maybe just use
	 *          {@link RedisCacheConfiguration#defaultCacheConfig()}.
	 * @param initialCacheConfigurations Map of known cache names along with the configuration to use for those caches.
	 *          Must not be {@literal null}.
	 */
	public RedisCacheManager(RedisCacheWriter cacheWriter, RedisCacheConfiguration defaultCacheConfiguration,
							 Map<String, RedisCacheConfiguration> initialCacheConfigurations) {

		this(cacheWriter, defaultCacheConfiguration);

		Assert.notNull(initialCacheConfigurations, "InitialCacheConfigurations must not be null!");
		this.initialCacheConfiguration.putAll(initialCacheConfigurations);
	}

	/**
	 * Create a new {@link RedisCacheManager} with defaults applied.
	 * <dl>
	 * <dt>locking</dt>
	 * <dd>disabled</dd>
	 * <dt>cache configuration</dt>
	 * <dd>{@link RedisCacheConfiguration#defaultCacheConfig()}</dd>
	 * <dt>initial caches</dt>
	 * <dd>none</dd>
	 * <dt>transaction aware</dt>
	 * <dd>no</dd>
	 * </dl>
	 *
	 * @param connectionFactory must not be {@literal null}.
	 * @return new instance of {@link RedisCacheManager}.
	 */
	public static RedisCacheManager defaultCacheManager(RedisConnectionFactory connectionFactory) {

		Assert.notNull(connectionFactory, "ConnectionFactory must not be null!");

		return new RedisCacheManager(new DefaultRedisCacheWriter(connectionFactory),
				RedisCacheConfiguration.defaultCacheConfig());
	}

	/**
	 * Entry point for builder style {@link RedisCacheManager} configuration.
	 *
	 * @param connectionFactory must not be {@literal null}.
	 * @return new {@link RedisCacheManagerConfigurator}.
	 */
	public static RedisCacheManagerConfigurator usingRawConnectionFactory(RedisConnectionFactory connectionFactory) {

		Assert.notNull(connectionFactory, "ConnectionFactory must not be null!");

		return RedisCacheManagerConfigurator.usingRawFactory(connectionFactory);
	}

	/**
	 * Entry point for builder style {@link RedisCacheManager} configuration.
	 *
	 * @param cacheWriter must not be {@literal null}.
	 * @return new {@link RedisCacheManagerConfigurator}.
	 */
	public static RedisCacheManagerConfigurator usingCacheWriter(RedisCacheWriter cacheWriter) {

		Assert.notNull(cacheWriter, "CacheWriter must not be null!");

		return RedisCacheManagerConfigurator.usingCacheWriter(cacheWriter);
	}

	@Override
	protected Collection<RedisCache> loadCaches() {

		List<RedisCache> caches = new LinkedList<>();
		for (Map.Entry<String, RedisCacheConfiguration> entry : initialCacheConfiguration.entrySet()) {
			caches.add(createRedisCache(entry.getKey(), entry.getValue()));
		}
		return caches;
	}

	@Override
	protected RedisCache getMissingCache(String name) {
		return createRedisCache(name, defaultCacheConfig);
	}

	/**
	 * @return unmodifiable {@link Map} containing cache name / configuration pairs. Never {@literal null}.
	 */
	public Map<String, RedisCacheConfiguration> getCacheConfigurations() {

		Map<String, RedisCacheConfiguration> configurationMap = new HashMap<>(getCacheNames().size());
		getCacheNames().forEach(it -> {

			RedisCache cache = RedisCache.class.cast(lookupCache(it));
			configurationMap.put(it, cache != null ? cache.getCacheConfiguration() : null);
		});
		return Collections.unmodifiableMap(configurationMap);
	}

	/**
	 * Configuration hook for creating {@link RedisCache} with given name and cacheConfig.
	 *
	 * @param name must not be {@literal null}.
	 * @param cacheConfig can be {@literal null}.
	 * @return never {@literal null}.
	 */
	protected RedisCache createRedisCache(String name, RedisCacheConfiguration cacheConfig) {
		return new RedisCache(name, cacheWriter, cacheConfig != null ? cacheConfig : defaultCacheConfig);
	}

	/**
	 * Configurator for creating {@link RedisCacheManager}.
	 *
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	public static class RedisCacheManagerConfigurator {

		private final RedisCacheConfiguration defaultCacheConfiguration;
		private final RedisCacheWriter cacheWriter;
		private final boolean enableTransactions;
		private final Map<String, RedisCacheConfiguration> intialCaches = new LinkedHashMap<>();

		private RedisCacheManagerConfigurator(RedisCacheWriter cacheWriter,
				RedisCacheConfiguration defaultCacheConfiguration, Map<String, RedisCacheConfiguration> intialCaches,
				boolean enableTransactions) {

			this.cacheWriter = cacheWriter;
			this.defaultCacheConfiguration = defaultCacheConfiguration;

			if (!CollectionUtils.isEmpty(intialCaches)) {
				this.intialCaches.putAll(intialCaches);
			}

			this.enableTransactions = enableTransactions;
		}

		/**
		 * Entry point for builder style {@link RedisCacheManager} configuration.
		 *
		 * @param connectionFactory must not be {@literal null}.
		 * @return new {@link RedisCacheManagerConfigurator}.
		 */
		public static RedisCacheManagerConfigurator usingRawFactory(RedisConnectionFactory connectionFactory) {

			Assert.notNull(connectionFactory, "ConnectionFactory must not be null!");

			return usingCacheWriter(new DefaultRedisCacheWriter(connectionFactory));
		}

		/**
		 * Entry point for builder style {@link RedisCacheManager} configuration.
		 *
		 * @param cacheWriter must not be {@literal null}.
		 * @return new {@link RedisCacheManagerConfigurator}.
		 */
		public static RedisCacheManagerConfigurator usingCacheWriter(RedisCacheWriter cacheWriter) {

			Assert.notNull(cacheWriter, "CacheWriter must not be null!");

			return new RedisCacheManagerConfigurator(cacheWriter, RedisCacheConfiguration.defaultCacheConfig(), null, false);
		}

		/**
		 * Define a default {@link RedisCacheConfiguration} applied to dynamically created {@link RedisCache}s.
		 *
		 * @param defaultCacheConfiguration must not be {@literal null}.
		 * @return new instance of {@link RedisCacheManagerConfigurator}.
		 */
		public RedisCacheManagerConfigurator withCacheDefaults(RedisCacheConfiguration defaultCacheConfiguration) {

			Assert.notNull(defaultCacheConfiguration, "DefaultCacheConfiguration must not be null!");

			return new RedisCacheManagerConfigurator(cacheWriter, defaultCacheConfiguration, intialCaches,
					enableTransactions);
		}

		/**
		 * Enable {@link RedisCache}s to synchronize cache put/evict operations with ongoing Spring-managed transactions.
		 *
		 * @return new instance of {@link RedisCacheManagerConfigurator}.
		 */
		public RedisCacheManagerConfigurator transactionAware() {
			return new RedisCacheManagerConfigurator(cacheWriter, defaultCacheConfiguration, intialCaches, true);
		}

		/**
		 * Append a {@link Set} of cache names to be pre initialized with current {@link RedisCacheConfiguration}.
		 * <strong>NOTE:</strong> This calls depends on {@link #withCacheDefaults(RedisCacheConfiguration)} using whatever
		 * default {@link RedisCacheConfiguration} is present at the time of invoking this method.
		 *
		 * @param cacheNames must not be {@literal null}.
		 * @return new instance of {@link RedisCacheManagerConfigurator}.
		 */
		public RedisCacheManagerConfigurator withInitialCacheNames(Set<String> cacheNames) {

			Assert.notNull(cacheNames, "CacheNames must not be null!");

			Map<String, RedisCacheConfiguration> cacheConfigMap = new LinkedHashMap<>(cacheNames.size());
			cacheNames.forEach(it -> cacheConfigMap.put(it, defaultCacheConfiguration));
			return withInitialCacheConfigurations(cacheConfigMap);
		}

		/**
		 * Append a {@link Map} of cache name/{@link RedisCacheConfiguration} pairs to be pre initialized.
		 *
		 * @param cacheConfigurations must not be {@literal null}.
		 * @return new instance of {@link RedisCacheManagerConfigurator}.
		 */
		public RedisCacheManagerConfigurator withInitialCacheConfigurations(
				Map<String, RedisCacheConfiguration> cacheConfigurations) {

			Assert.notNull(cacheConfigurations, "CacheConfigurations must not be null!");

			Map<String, RedisCacheConfiguration> cacheConfigMap = new LinkedHashMap<>(intialCaches);
			cacheConfigMap.putAll(cacheConfigurations);
			return new RedisCacheManagerConfigurator(cacheWriter, defaultCacheConfiguration, cacheConfigMap,
					enableTransactions);
		}

		/**
		 * Create new instance of {@link RedisCacheManager} with configuration options applied.
		 *
		 * @return new instance of {@link RedisCacheManager}.
		 */
		public RedisCacheManager createAndGet() {

			RedisCacheManager cm = new RedisCacheManager(cacheWriter, defaultCacheConfiguration, intialCaches) {

				boolean hasAlreadyBeenSet;

				@Override
				public void setTransactionAware(boolean transactionAware) {

					if (hasAlreadyBeenSet) {
						throw new IllegalStateException(
								String.format("CacheManager transaction awareness has already been set to %s.", isTransactionAware()));
					}

					super.setTransactionAware(transactionAware);
					hasAlreadyBeenSet = true;
				}
			};

			cm.setTransactionAware(enableTransactions);
			cm.afterPropertiesSet(); // is this a good idea? Still need to think about it.
			return cm;
		}
	}
}
