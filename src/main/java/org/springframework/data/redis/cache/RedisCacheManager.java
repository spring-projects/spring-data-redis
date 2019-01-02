/*
 * Copyright 2011-2019 the original author or authors.
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.cache.support.NullValue;
import org.springframework.cache.transaction.AbstractTransactionSupportingCacheManager;
import org.springframework.cache.transaction.TransactionAwareCacheDecorator;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

/**
 * {@link CacheManager} implementation for Redis. By default saves the keys directly, without appending a prefix (which
 * acts as a namespace). To avoid clashes, it is recommended to change this (by setting 'usePrefix' to 'true'). <br>
 * By default {@link RedisCache}s will be lazily initialized for each {@link #getCache(String)} request unless a set of
 * predefined cache names is provided. <br>
 * <br>
 * Setting {@link #setTransactionAware(boolean)} to {@code true} will force Caches to be decorated as
 * {@link TransactionAwareCacheDecorator} so values will only be written to the cache after successful commit of
 * surrounding transaction.
 * 
 * @author Costin Leau
 * @author Christoph Strobl
 * @author Thomas Darimont
 */
public class RedisCacheManager extends AbstractTransactionSupportingCacheManager {

	private final Log logger = LogFactory.getLog(RedisCacheManager.class);

	@SuppressWarnings("rawtypes") //
	private final RedisOperations redisOperations;

	private boolean usePrefix = false;
	private RedisCachePrefix cachePrefix = new DefaultRedisCachePrefix();
	private boolean loadRemoteCachesOnStartup = false;
	private boolean dynamic = true;

	// 0 - never expire
	private long defaultExpiration = 0;
	private Map<String, Long> expires = null;

	private Set<String> configuredCacheNames;

	private final boolean cacheNullValues;

	/**
	 * Construct a {@link RedisCacheManager}.
	 * 
	 * @param redisOperations
	 */
	@SuppressWarnings("rawtypes")
	public RedisCacheManager(RedisOperations redisOperations) {
		this(redisOperations, Collections.<String> emptyList());
	}

	/**
	 * Construct a static {@link RedisCacheManager}, managing caches for the specified cache names only.
	 * 
	 * @param redisOperations
	 * @param cacheNames
	 * @since 1.2
	 */
	@SuppressWarnings("rawtypes")
	public RedisCacheManager(RedisOperations redisOperations, Collection<String> cacheNames) {
		this(redisOperations, cacheNames, false);
	}

	/**
	 * Construct a static {@link RedisCacheManager}, managing caches for the specified cache names only. <br />
	 * <br />
	 * <strong>NOTE</strong> When enabling {@code cacheNullValues} please make sure the {@link RedisSerializer} used by
	 * {@link RedisOperations} is capable of serializing {@link NullValue}.
	 *
	 * @param redisOperations {@link RedisOperations} to work upon.
	 * @param cacheNames {@link Collection} of known cache names.
	 * @param cacheNullValues set to {@literal true} to allow caching {@literal null}.
	 * @since 1.8
	 */
	@SuppressWarnings("rawtypes")
	public RedisCacheManager(RedisOperations redisOperations, Collection<String> cacheNames, boolean cacheNullValues) {

		this.redisOperations = redisOperations;
		this.cacheNullValues = cacheNullValues;
		setCacheNames(cacheNames);
	}

	/**
	 * Specify the set of cache names for this CacheManager's 'static' mode. <br>
	 * The number of caches and their names will be fixed after a call to this method, with no creation of further cache
	 * regions at runtime. <br>
	 * Calling this with a {@code null} or empty collection argument resets the mode to 'dynamic', allowing for further
	 * creation of caches again.
	 */
	public void setCacheNames(Collection<String> cacheNames) {

		Set<String> newCacheNames = CollectionUtils.isEmpty(cacheNames) ? Collections.<String> emptySet()
				: new HashSet<String>(cacheNames);

		this.configuredCacheNames = newCacheNames;
		this.dynamic = newCacheNames.isEmpty();
	}

	public void setUsePrefix(boolean usePrefix) {
		this.usePrefix = usePrefix;
	}

	/**
	 * Sets the cachePrefix. Defaults to 'DefaultRedisCachePrefix').
	 * 
	 * @param cachePrefix the cachePrefix to set
	 */
	public void setCachePrefix(RedisCachePrefix cachePrefix) {
		this.cachePrefix = cachePrefix;
	}

	/**
	 * Sets the default expire time (in seconds).
	 * 
	 * @param defaultExpireTime time in seconds.
	 */
	public void setDefaultExpiration(long defaultExpireTime) {
		this.defaultExpiration = defaultExpireTime;
	}

	/**
	 * Sets the expire time (in seconds) for cache regions (by key).
	 * 
	 * @param expires time in seconds
	 */
	public void setExpires(Map<String, Long> expires) {
		this.expires = (expires != null ? new ConcurrentHashMap<String, Long>(expires) : null);
	}

	/**
	 * If set to {@code true} {@link RedisCacheManager} will try to retrieve cache names from redis server using
	 * {@literal KEYS} command and initialize {@link RedisCache} for each of them.
	 * 
	 * @param loadRemoteCachesOnStartup
	 * @since 1.2
	 */
	public void setLoadRemoteCachesOnStartup(boolean loadRemoteCachesOnStartup) {
		this.loadRemoteCachesOnStartup = loadRemoteCachesOnStartup;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.cache.support.AbstractCacheManager#loadCaches()
	 */
	@Override
	protected Collection<? extends Cache> loadCaches() {

		Assert.notNull(this.redisOperations, "A redis template is required in order to interact with data store");

		Set<Cache> caches = new LinkedHashSet<Cache>(
				loadRemoteCachesOnStartup ? loadAndInitRemoteCaches() : new ArrayList<Cache>());

		Set<String> cachesToLoad = new LinkedHashSet<String>(this.configuredCacheNames);
		cachesToLoad.addAll(this.getCacheNames());

		if (!CollectionUtils.isEmpty(cachesToLoad)) {

			for (String cacheName : cachesToLoad) {
				caches.add(createCache(cacheName));
			}
		}

		return caches;
	}

	/**
	 * Returns a new {@link Collection} of {@link Cache} from the given caches collection and adds the configured
	 * {@link Cache}s of they are not already present.
	 * 
	 * @param caches must not be {@literal null}
	 * @return
	 */
	protected Collection<? extends Cache> addConfiguredCachesIfNecessary(Collection<? extends Cache> caches) {

		Assert.notNull(caches, "Caches must not be null!");

		Collection<Cache> result = new ArrayList<Cache>(caches);

		for (String cacheName : getCacheNames()) {

			boolean configuredCacheAlreadyPresent = false;

			for (Cache cache : caches) {

				if (cache.getName().equals(cacheName)) {
					configuredCacheAlreadyPresent = true;
					break;
				}
			}

			if (!configuredCacheAlreadyPresent) {
				result.add(getCache(cacheName));
			}
		}

		return result;
	}

	/**
	 * Will no longer add the cache to the set of
	 *
	 * @param cacheName
	 * @return
	 * @deprecated since 1.8 - please use {@link #getCache(String)}.
	 */
	@Deprecated
	protected Cache createAndAddCache(String cacheName) {

		Cache cache = super.getCache(cacheName);
		return cache != null ? cache : createCache(cacheName);
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.cache.support.AbstractCacheManager#getMissingCache(java.lang.String)
	 */
	@Override
	protected Cache getMissingCache(String name) {
		return this.dynamic ? createCache(name) : null;
	}

	@SuppressWarnings("unchecked")
	protected RedisCache createCache(String cacheName) {
		long expiration = computeExpiration(cacheName);
		return new RedisCache(cacheName, (usePrefix ? cachePrefix.prefix(cacheName) : null), redisOperations, expiration,
				cacheNullValues);
	}

	protected long computeExpiration(String name) {
		Long expiration = null;
		if (expires != null) {
			expiration = expires.get(name);
		}
		return (expiration != null ? expiration.longValue() : defaultExpiration);
	}

	protected List<Cache> loadAndInitRemoteCaches() {

		List<Cache> caches = new ArrayList<Cache>();

		try {
			Set<String> cacheNames = loadRemoteCacheKeys();
			if (!CollectionUtils.isEmpty(cacheNames)) {
				for (String cacheName : cacheNames) {
					if (null == super.getCache(cacheName)) {
						caches.add(createCache(cacheName));
					}
				}
			}
		} catch (Exception e) {
			if (logger.isWarnEnabled()) {
				logger.warn("Failed to initialize cache with remote cache keys.", e);
			}
		}

		return caches;
	}

	@SuppressWarnings("unchecked")
	protected Set<String> loadRemoteCacheKeys() {
		return (Set<String>) redisOperations.execute(new RedisCallback<Set<String>>() {

			@Override
			public Set<String> doInRedis(RedisConnection connection) throws DataAccessException {

				// we are using the ~keys postfix as defined in RedisCache#setName
				Set<byte[]> keys = connection.keys(redisOperations.getKeySerializer().serialize("*~keys"));
				Set<String> cacheKeys = new LinkedHashSet<String>();

				if (!CollectionUtils.isEmpty(keys)) {
					for (byte[] key : keys) {
						cacheKeys.add(redisOperations.getKeySerializer().deserialize(key).toString().replace("~keys", ""));
					}
				}

				return cacheKeys;
			}
		});
	}

	@SuppressWarnings("rawtypes")
	protected RedisOperations getRedisOperations() {
		return redisOperations;
	}

	protected RedisCachePrefix getCachePrefix() {
		return cachePrefix;
	}

	protected boolean isUsePrefix() {
		return usePrefix;
	}

	/* (non-Javadoc)
	* @see
	org.springframework.cache.transaction.AbstractTransactionSupportingCacheManager#decorateCache(org.springframework.cache.Cache)
	*/
	@Override
	protected Cache decorateCache(Cache cache) {

		if (isCacheAlreadyDecorated(cache)) {
			return cache;
		}

		return super.decorateCache(cache);
	}

	protected boolean isCacheAlreadyDecorated(Cache cache) {
		return isTransactionAware() && cache instanceof TransactionAwareCacheDecorator;
	}
}
