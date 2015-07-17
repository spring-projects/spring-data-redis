/*
 * Copyright 2011-2015 the original author or authors.
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

import static org.springframework.util.Assert.*;
import static org.springframework.util.ObjectUtils.*;

import java.util.Arrays;
import java.util.Set;

import org.springframework.cache.Cache;
import org.springframework.cache.support.SimpleValueWrapper;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

/**
 * Cache implementation on top of Redis.
 * 
 * @author Costin Leau
 * @author Christoph Strobl
 * @author Thomas Darimont
 */
@SuppressWarnings("unchecked")
public class RedisCache implements Cache {

	@SuppressWarnings("rawtypes")//
	private final RedisOperations redisOperations;
	private final RedisCacheMetadata cacheMetadata;
	private final CacheValueAccessor cacheValueAccessor;

	/**
	 * Constructs a new <code>RedisCache</code> instance.
	 * 
	 * @param name cache name
	 * @param prefix
	 * @param redisOperations
	 * @param expiration
	 */
	public RedisCache(String name, byte[] prefix, RedisOperations<? extends Object, ? extends Object> redisOperations,
			long expiration) {

		hasText(name, "non-empty cache name is required");
		this.cacheMetadata = new RedisCacheMetadata(name, prefix);
		this.cacheMetadata.setDefaultExpiration(expiration);

		this.redisOperations = redisOperations;
		this.cacheValueAccessor = new CacheValueAccessor(redisOperations.getValueSerializer());
	}

	/**
	 * Return the value to which this cache maps the specified key, generically specifying a type that return value will
	 * be cast to.
	 * 
	 * @param key
	 * @param type
	 * @return
	 * @see DATAREDIS-243
	 */
	public <T> T get(Object key, Class<T> type) {

		ValueWrapper wrapper = get(key);
		return wrapper == null ? null : (T) wrapper.get();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.cache.Cache#get(java.lang.Object)
	 */
	@Override
	public ValueWrapper get(Object key) {
		return get(RedisCacheKey.builder(key).usePrefix(this.cacheMetadata.getKeyPrefix()).withKeySerializer(
				redisOperations.getKeySerializer()).build());
	}

	/**
	 * Return the value to which this cache maps the specified key.
	 * 
	 * @param cacheKey the key whose associated value is to be returned via its binary representation.
	 * @return the {@link RedisCacheElement} stored at given key or {@literal null} if no value found for key.
	 * @since 1.5
	 */
	public RedisCacheElement get(final RedisCacheKey cacheKey) {

		notNull(cacheKey, "CacheKey must not be null!");
		final RedisCacheElement element = new RedisCacheElement(cacheKey, null);
		byte[] bs = (byte[]) redisOperations.execute(new AbstractRedisCacheCallback<byte[]>(
				new RedisCacheElement(cacheKey, null), cacheMetadata) {

			@Override
			public byte[] doInRedis(RedisCacheElement element, RedisConnection connection)
					throws DataAccessException {

				return connection.get(element.getKeyBytes());
			}
		});
		Object value = redisOperations.getValueSerializer() != null ? redisOperations.getValueSerializer().deserialize(bs) : bs;
		return (bs == null ? null : new RedisCacheElement(element.getKey(), value));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.cache.Cache#put(java.lang.Object, java.lang.Object)
	 */
	@Override
	public void put(final Object key, final Object value) {

		put(new RedisCacheElement(RedisCacheKey.builder(key).usePrefix(cacheMetadata.getKeyPrefix()).withKeySerializer(
				redisOperations.getKeySerializer()).build(), value).expireAfter(cacheMetadata.getDefaultExpiration()));
	}

	/**
	 * Add the element by adding {@link RedisCacheElement#get()} at {@link RedisCacheElement#getKeyBytes()}. If the cache
	 * previously contained a mapping for this {@link RedisCacheElement#getKeyBytes()}, the old value is replaced by
	 * {@link RedisCacheElement#get()}.
	 * 
	 * @param element must not be {@literal null}.
	 * @since 1.5
	 */
	public void put(RedisCacheElement element) {

		notNull(element, "Element must not be null!");
		redisOperations.execute(new RedisCachePutCallback(element, cacheValueAccessor, cacheMetadata));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.cache.Cache#putIfAbsent(java.lang.Object, java.lang.Object)
	 */
	public ValueWrapper putIfAbsent(Object key, final Object value) {

		return putIfAbsent(new RedisCacheElement(RedisCacheKey.builder(key).usePrefix(cacheMetadata.getKeyPrefix())
				.withKeySerializer(redisOperations.getKeySerializer()).build(), value).expireAfter(cacheMetadata.getDefaultExpiration()));
	}

	/**
	 * Add the element as long as no element exists at {@link RedisCacheElement#getKeyBytes()}. If a value is present for
	 * {@link RedisCacheElement#getKeyBytes()} this one is returned.
	 * 
	 * @param element must not be {@literal null}.
	 * @return
	 * @since 1.5
	 */
	public ValueWrapper putIfAbsent(RedisCacheElement element) {

		notNull(element, "Element must not be null!");
		return toWrapper(redisOperations.execute(new RedisCachePutIfAbsentCallback(element, cacheValueAccessor, cacheMetadata)));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.cache.Cache#evict(java.lang.Object)
	 */
	public void evict(Object key) {
		evict(new RedisCacheElement(RedisCacheKey.builder(key).usePrefix(cacheMetadata.getKeyPrefix()).withKeySerializer(
				redisOperations.getKeySerializer()).build(), null));
	}

	/**
	 * @param element {@link RedisCacheElement#getKeyBytes()}
	 * @since 1.5
	 */
	public void evict(final RedisCacheElement element) {

		notNull(element, "Element must not be null!");
		redisOperations.execute(new RedisCacheEvictCallback(element, cacheMetadata));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.cache.Cache#clear()
	 */
	public void clear() {
		redisOperations.execute(cacheMetadata.usesKeyPrefix() ? new RedisCacheCleanByPrefixCallback(cacheMetadata)
				: new RedisCacheCleanByKeysCallback(cacheMetadata));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.cache.Cache#getName()
	 */
	public String getName() {
		return cacheMetadata.getCacheName();
	}

	/**
	 * {@inheritDoc} This implementation simply returns the RedisTemplate used for configuring the cache, giving access to
	 * the underlying Redis store.
	 */
	public Object getNativeCache() {
		return redisOperations;
	}

	private ValueWrapper toWrapper(Object value) {
		return (value != null ? new SimpleValueWrapper(value) : null);
	}

	/**
	 * Metadata required to maintain {@link RedisCache}. Keeps track of additional data structures required for processing
	 * cache operations.
	 * 
	 * @author Christoph Strobl
	 * @since 1.5
	 */
	static class RedisCacheMetadata {

		private final String cacheName;
		private final byte[] keyPrefix;
		private final byte[] setOfKnownKeys;
		private final byte[] cacheLockName;
		private long defaultExpiration = 0;

		/**
		 * @param cacheName must not be {@literal null} or empty.
		 * @param keyPrefix can be {@literal null}.
		 */
		public RedisCacheMetadata(String cacheName, byte[] keyPrefix) {

			hasText(cacheName, "CacheName must not be null or empty!");
			this.cacheName = cacheName;
			this.keyPrefix = keyPrefix;

			StringRedisSerializer stringSerializer = new StringRedisSerializer();

			// name of the set holding the keys
			this.setOfKnownKeys = usesKeyPrefix() ? new byte[] {} : stringSerializer.serialize(cacheName + "~keys");
			this.cacheLockName = stringSerializer.serialize(cacheName + "~lock");
		}

		/**
		 * @return true if the {@link RedisCache} uses a prefix for key ranges.
		 */
		public boolean usesKeyPrefix() {
			return (keyPrefix != null && keyPrefix.length > 0);
		}

		/**
		 * Get the binary representation of the key prefix.
		 * 
		 * @return never {@literal null}.
		 */
		public byte[] getKeyPrefix() {
			return this.keyPrefix;
		}

		/**
		 * Get the binary representation of the key identifying the data structure used to maintain known keys.
		 * 
		 * @return never {@literal null}.
		 */
		public byte[] getSetOfKnownKeysKey() {
			return setOfKnownKeys;
		}

		/**
		 * Get the binary representation of the key identifying the data structure used to lock the cache.
		 * 
		 * @return never {@literal null}.
		 */
		public byte[] getCacheLockKey() {
			return cacheLockName;
		}

		/**
		 * Get the name of the cache.
		 * 
		 * @return
		 */
		public String getCacheName() {
			return cacheName;
		}

		/**
		 * Set the default expiration time in seconds
		 * 
		 * @param defaultExpiration
		 */
		public void setDefaultExpiration(long seconds) {
			this.defaultExpiration = seconds;
		}

		/**
		 * Get the default expiration time in seconds.
		 * 
		 * @return
		 */
		public long getDefaultExpiration() {
			return defaultExpiration;
		}

	}

	/**
	 * @author Christoph Strobl
	 * @since 1.5
	 */
	static class CacheValueAccessor {

		@SuppressWarnings("rawtypes")//
		private final RedisSerializer valueSerializer;

		@SuppressWarnings("rawtypes")
		CacheValueAccessor(RedisSerializer valueRedisSerializer) {
			valueSerializer = valueRedisSerializer;
		}

		byte[] convertToBytesIfNecessary(Object value) {

			if (valueSerializer == null && value instanceof byte[]) {
				return (byte[]) value;
			}

			return valueSerializer.serialize(value);
		}

		Object deserializeIfNecessary(byte[] value) {

			if (valueSerializer != null) {
				return valueSerializer.deserialize(value);
			}

			return value;
		}
	}

	/**
	 * @author Christoph Strobl
	 * @since 1.5
	 * @param <T>
	 */
	static abstract class AbstractRedisCacheCallback<T> implements RedisCallback<T> {

		private long WAIT_FOR_LOCK_TIMEOUT = 300;
		private final RedisCacheElement element;
		private final RedisCacheMetadata cacheMetadata;

		public AbstractRedisCacheCallback(RedisCacheElement element, RedisCacheMetadata metadata) {
			this.element = element;
			this.cacheMetadata = metadata;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.core.RedisCallback#doInRedis(org.springframework.data.redis.connection.RedisConnection)
		 */
		@Override
		public T doInRedis(RedisConnection connection) throws DataAccessException {
			waitForLock(connection);
			return doInRedis(element, connection);
		}

		public abstract T doInRedis(RedisCacheElement element, RedisConnection connection) throws DataAccessException;

		protected void processKeyExpiration(RedisCacheElement element, RedisConnection connection) {
			if (!element.isEternal()) {
				connection.expire(element.getKeyBytes(), element.getTimeToLive());
			}
		}

		protected void maintainKnownKeys(RedisCacheElement element, RedisConnection connection) {

			if (!element.hasKeyPrefix()) {

				connection.zAdd(cacheMetadata.getSetOfKnownKeysKey(), 0, element.getKeyBytes());

				if (!element.isEternal()) {
					connection.expire(cacheMetadata.getSetOfKnownKeysKey(), element.getTimeToLive());
				}
			}
		}

		protected void cleanKnownKeys(RedisCacheElement element, RedisConnection connection) {

			if (!element.hasKeyPrefix()) {
				connection.zRem(cacheMetadata.getSetOfKnownKeysKey(), element.getKeyBytes());
			}
		}

		protected boolean waitForLock(RedisConnection connection) {

			boolean retry;
			boolean foundLock = false;
			do {
				retry = false;
				if (connection.exists(cacheMetadata.getCacheLockKey())) {
					foundLock = true;
					try {
						Thread.sleep(WAIT_FOR_LOCK_TIMEOUT);
					} catch (InterruptedException ex) {
						Thread.currentThread().interrupt();
					}
					retry = true;
				}
			} while (retry);

			return foundLock;
		}
	}

	/**
	 * @author Christoph Strobl
	 * @param <T>
	 * @since 1.5
	 */
	static abstract class LockingRedisCacheCallback<T> implements RedisCallback<T> {

		private final RedisCacheMetadata metadata;

		public LockingRedisCacheCallback(RedisCacheMetadata metadata) {
			this.metadata = metadata;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.core.RedisCallback#doInRedis(org.springframework.data.redis.connection.RedisConnection)
		 */
		@Override
		public T doInRedis(RedisConnection connection) throws DataAccessException {

			if (connection.exists(metadata.getCacheLockKey())) {
				return null;
			}
			try {
				connection.set(metadata.getCacheLockKey(), metadata.getCacheLockKey());
				return doInLock(connection);
			} finally {
				connection.del(metadata.getCacheLockKey());
			}
		}

		public abstract T doInLock(RedisConnection connection);
	}

	/**
	 * @author Christoph Strobl
	 * @since 1.5
	 */
	static class RedisCacheCleanByKeysCallback extends LockingRedisCacheCallback<Void> {

		private static final int PAGE_SIZE = 128;
		private final RedisCacheMetadata metadata;

		RedisCacheCleanByKeysCallback(RedisCacheMetadata metadata) {
			super(metadata);
			this.metadata = metadata;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.cache.RedisCache.LockingRedisCacheCallback#doInLock(org.springframework.data.redis.connection.RedisConnection)
		 */
		@Override
		public Void doInLock(RedisConnection connection) {

			int offset = 0;
			boolean finished = false;

			do {
				// need to paginate the keys
				Set<byte[]> keys = connection.zRange(metadata.getSetOfKnownKeysKey(), (offset) * PAGE_SIZE, (offset + 1)
						* PAGE_SIZE - 1);
				finished = keys.size() < PAGE_SIZE;
				offset++;
				if (!keys.isEmpty()) {
					connection.del(keys.toArray(new byte[keys.size()][]));
				}
			} while (!finished);

			connection.del(metadata.getSetOfKnownKeysKey());
			return null;
		}
	}

	/**
	 * @author Christoph Strobl
	 * @since 1.5
	 */
	static class RedisCacheCleanByPrefixCallback extends LockingRedisCacheCallback<Void> {

		private static final byte[] REMOVE_KEYS_BY_PATTERN_LUA = new StringRedisSerializer()
				.serialize("local keys = redis.call('KEYS', ARGV[1]); local keysCount = table.getn(keys); if(keysCount > 0) then for _, key in ipairs(keys) do redis.call('del', key); end; end; return keysCount;");
		private static final byte[] WILD_CARD = new StringRedisSerializer().serialize("*");
		private final RedisCacheMetadata metadata;

		public RedisCacheCleanByPrefixCallback(RedisCacheMetadata metadata) {
			super(metadata);
			this.metadata = metadata;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.cache.RedisCache.LockingRedisCacheCallback#doInLock(org.springframework.data.redis.connection.RedisConnection)
		 */
		@Override
		public Void doInLock(RedisConnection connection) throws DataAccessException {

			byte[] prefixToUse = Arrays.copyOf(metadata.getKeyPrefix(), metadata.getKeyPrefix().length + WILD_CARD.length);
			System.arraycopy(WILD_CARD, 0, prefixToUse, metadata.getKeyPrefix().length, WILD_CARD.length);

			connection.eval(REMOVE_KEYS_BY_PATTERN_LUA, ReturnType.INTEGER, 0, prefixToUse);

			return null;
		}
	}

	/**
	 * @author Christoph Strobl
	 * @since 1.5
	 */
	static class RedisCacheEvictCallback extends AbstractRedisCacheCallback<Void> {

		public RedisCacheEvictCallback(RedisCacheElement element, RedisCacheMetadata metadata) {
			super(element, metadata);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.cache.RedisCache.AbstractRedisCacheCallback#doInRedis(org.springframework.data.redis.cache.RedisCacheElement, org.springframework.data.redis.connection.RedisConnection)
		 */
		@Override
		public Void doInRedis(RedisCacheElement element, RedisConnection connection) throws DataAccessException {

			connection.del(element.getKeyBytes());
			cleanKnownKeys(element, connection);
			return null;
		}
	}

	/**
	 * @author Christoph Strobl
	 * @since 1.5
	 */
	static class RedisCachePutCallback extends AbstractRedisCacheCallback<Void> {

		private final byte[] valueBytes;

		public RedisCachePutCallback(RedisCacheElement element, CacheValueAccessor valueAccessor,
				RedisCacheMetadata metadata) {

			super(element, metadata);
			this.valueBytes = valueAccessor.convertToBytesIfNecessary(element.get());
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.cache.RedisCache.AbstractRedisPutCallback#doInRedis(org.springframework.data.redis.cache.RedisCache.RedisCacheElement, org.springframework.data.redis.connection.RedisConnection)
		 */
		@Override
		public Void doInRedis(RedisCacheElement element, RedisConnection connection) throws DataAccessException {

			connection.multi();

			connection.set(element.getKeyBytes(), valueBytes);

			processKeyExpiration(element, connection);
			maintainKnownKeys(element, connection);

			connection.exec();
			return null;
		}
	}

	/**
	 * @author Christoph Strobl
	 * @since 1.5
	 */
	static class RedisCachePutIfAbsentCallback extends AbstractRedisCacheCallback<Object> {

		private final CacheValueAccessor valueAccessor;

		public RedisCachePutIfAbsentCallback(RedisCacheElement element, CacheValueAccessor valueAccessor,
				RedisCacheMetadata metadata) {

			super(element, metadata);
			this.valueAccessor = valueAccessor;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.cache.RedisCache.AbstractRedisPutCallback#doInRedis(org.springframework.data.redis.cache.RedisCache.RedisCacheElement, org.springframework.data.redis.connection.RedisConnection)
		 */
		@Override
		public Object doInRedis(RedisCacheElement element, RedisConnection connection) throws DataAccessException {

			waitForLock(connection);
			Object resultValue = put(element, connection);

			if (nullSafeEquals(element.get(), resultValue)) {
				processKeyExpiration(element, connection);
				maintainKnownKeys(element, connection);
			}

			return resultValue;
		}

		private Object put(RedisCacheElement element, RedisConnection connection) {

			boolean valueWasSet = connection.setNX(element.getKeyBytes(),
					valueAccessor.convertToBytesIfNecessary(element.get()));
			return valueWasSet ? element.get() : valueAccessor.deserializeIfNecessary(connection.get(element.getKeyBytes()));
		}
	}

}
