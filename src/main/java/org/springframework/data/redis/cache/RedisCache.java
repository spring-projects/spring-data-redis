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

import java.util.Arrays;
import java.util.Set;

import org.springframework.cache.Cache;
import org.springframework.cache.support.SimpleValueWrapper;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

/**
 * Cache implementation on top of Redis.
 * 
 * @author Costin Leau
 * @author Christoph Strobl
 * @author Thomas Darimont
 */
@SuppressWarnings("unchecked")
public class RedisCache implements Cache {

	private static final byte[] REMOVE_KEYS_BY_PATTERN_LUA = "local keys = redis.call('KEYS', ARGV[1]); local keysCount = table.getn(keys); if(keysCount > 0) then for _, key in ipairs(keys) do redis.call('del', key); end; end; return keysCount;"
			.getBytes();
	private static final int PAGE_SIZE = 128;
	private final String name;
	@SuppressWarnings("rawtypes") private final RedisTemplate template;
	private final byte[] prefix;
	private final byte[] setOfKnownKeys;
	private final byte[] cacheLockName;
	private long WAIT_FOR_LOCK = 300;
	private final long expiration;

	/**
	 * Constructs a new <code>RedisCache</code> instance.
	 * 
	 * @param name cache name
	 * @param prefix
	 * @param template
	 * @param expiration
	 */
	public RedisCache(String name, byte[] prefix, RedisTemplate<? extends Object, ? extends Object> template,
			long expiration) {

		Assert.hasText(name, "non-empty cache name is required");
		this.name = name;
		this.template = template;
		this.prefix = prefix;
		this.expiration = expiration;

		StringRedisSerializer stringSerializer = new StringRedisSerializer();

		// name of the set holding the keys
		this.setOfKnownKeys = stringSerializer.serialize(name + "~keys");
		this.cacheLockName = stringSerializer.serialize(name + "~lock");
	}

	public String getName() {
		return name;
	}

	/**
	 * {@inheritDoc} This implementation simply returns the RedisTemplate used for configuring the cache, giving access to
	 * the underlying Redis store.
	 */
	public Object getNativeCache() {
		return template;
	}

	public ValueWrapper get(final Object key) {
		return (ValueWrapper) template.execute(new RedisCallback<ValueWrapper>() {

			public ValueWrapper doInRedis(RedisConnection connection) throws DataAccessException {
				waitForLock(connection);
				byte[] bs = connection.get(computeKey(key));
				Object value = template.getValueSerializer() != null ? template.getValueSerializer().deserialize(bs) : bs;
				return (bs == null ? null : new SimpleValueWrapper(value));
			}
		}, true);
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
	 * @see org.springframework.cache.Cache#put(java.lang.Object, java.lang.Object)
	 */
	public void put(final Object key, final Object value) {

		put(new RedisCacheElement(convertToBytesIfNecessary(template.getKeySerializer(), key), value).usePrefix(prefix)
				.expireAfter(expiration));
	}

	/**
	 * @param element
	 * @since 1.5
	 */
	public void put(final RedisCacheElement element) {
		template.execute(new RedisCachePutCallback(element), true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.cache.Cache#putIfAbsent(java.lang.Object, java.lang.Object)
	 */
	public ValueWrapper putIfAbsent(Object key, final Object value) {

		return putIfAbsent(new RedisCacheElement(convertToBytesIfNecessary(template.getKeySerializer(), key), value)
				.usePrefix(prefix).expireAfter(expiration));
	}

	/**
	 * @param element
	 * @return
	 * @since 1.5
	 */
	public ValueWrapper putIfAbsent(final RedisCacheElement element) {
		return toWrapper(template.execute(new RedisCachePutIfAbsentCallback(element), true));
	}

	private ValueWrapper toWrapper(Object value) {
		return (value != null ? new SimpleValueWrapper(value) : null);
	}

	public void evict(Object key) {
		final byte[] k = computeKey(key);

		template.execute(new RedisCallback<Object>() {
			public Object doInRedis(RedisConnection connection) throws DataAccessException {
				connection.del(k);
				// remove key from set
				if (shouldTrackKeys()) {
					connection.zRem(setOfKnownKeys, k);
				}
				return null;
			}
		}, true);
	}

	public void clear() {
		// need to del each key individually
		template.execute(new RedisCallback<Object>() {
			public Object doInRedis(RedisConnection connection) throws DataAccessException {
				// another clear is on-going
				if (connection.exists(cacheLockName)) {
					return null;
				}

				try {
					connection.set(cacheLockName, cacheLockName);

					int offset = 0;
					boolean finished = false;

					if (shouldTrackKeys()) {
						do {
							// need to paginate the keys
							Set<byte[]> keys = connection.zRange(setOfKnownKeys, (offset) * PAGE_SIZE, (offset + 1) * PAGE_SIZE - 1);
							finished = keys.size() < PAGE_SIZE;
							offset++;
							if (!keys.isEmpty()) {
								connection.del(keys.toArray(new byte[keys.size()][]));
							}
						} while (!finished);

						connection.del(setOfKnownKeys);
					} else {

						byte[] wildCard = "*".getBytes();
						byte[] prefixToUse = Arrays.copyOf(prefix, prefix.length + wildCard.length);
						System.arraycopy(wildCard, 0, prefixToUse, prefix.length, wildCard.length);

						connection.eval(REMOVE_KEYS_BY_PATTERN_LUA, ReturnType.INTEGER, 0, prefixToUse);
					}
					return null;
				} finally {
					connection.del(cacheLockName);
				}
			}
		}, true);
	}

	private byte[] computeKey(Object key) {

		byte[] keyBytes = convertToBytesIfNecessary(template.getKeySerializer(), key);

		if (prefix == null || prefix.length == 0) {
			return keyBytes;
		}

		byte[] result = Arrays.copyOf(prefix, prefix.length + keyBytes.length);
		System.arraycopy(keyBytes, 0, result, prefix.length, keyBytes.length);

		return result;
	}

	private boolean waitForLock(RedisConnection connection) {

		boolean retry;
		boolean foundLock = false;
		do {
			retry = false;
			if (connection.exists(cacheLockName)) {
				foundLock = true;
				try {
					Thread.sleep(WAIT_FOR_LOCK);
				} catch (InterruptedException ex) {
					Thread.currentThread().interrupt();
				}
				retry = true;
			}
		} while (retry);

		return foundLock;
	}

	private byte[] convertToBytesIfNecessary(RedisSerializer<Object> serializer, Object value) {

		if (serializer == null && value instanceof byte[]) {
			return (byte[]) value;
		}

		return serializer.serialize(value);
	}

	private Object deserializeIfNecessary(RedisSerializer<byte[]> serializer, byte[] value) {

		if (serializer != null) {
			return serializer.deserialize(value);
		}

		return value;
	}

	/**
	 * A set of keys needs to be maintained in case no prefix is provided. Clear cache depends on either a pattern to
	 * identify keys to remove or a maintained set of known keys, otherwise the entire db would be flushed potentially
	 * removing non cache entry values.
	 * 
	 * @return
	 */
	private boolean shouldTrackKeys() {
		return (prefix == null || prefix.length == 0);
	}

	abstract class AbstractRedisPutCallback<T> implements RedisCallback<T> {

		private final RedisCacheElement element;

		public AbstractRedisPutCallback(RedisCacheElement element) {
			this.element = element;
		}

		@Override
		public T doInRedis(RedisConnection connection) throws DataAccessException {
			return doInRedis(element, connection);
		}

		public abstract T doInRedis(RedisCacheElement element, RedisConnection connection) throws DataAccessException;

		protected void processKeyExpiration(RedisCacheElement element, RedisConnection connection) {
			if (!element.isEternal()) {
				connection.expire(element.getKey(), element.getTimeToLive());
			}
		}

		protected void maintainKnownKeys(RedisCacheElement element, RedisConnection connection) {

			if (!element.hasKeyPrefix()) {
				connection.zAdd(setOfKnownKeys, 0, element.getKey());
				connection.expire(setOfKnownKeys, element.getTimeToLive());
			}
		}

		protected abstract Object put(RedisCacheElement element, RedisConnection connection);
	}

	/**
	 * @author Christoph Strobl
	 * @since 1.5
	 */
	class RedisCachePutCallback extends AbstractRedisPutCallback<Void> {

		public RedisCachePutCallback(RedisCacheElement element) {
			super(element);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.cache.RedisCache.AbstractRedisPutCallback#doInRedis(org.springframework.data.redis.cache.RedisCache.RedisCacheElement, org.springframework.data.redis.connection.RedisConnection)
		 */
		@Override
		public Void doInRedis(RedisCacheElement element, RedisConnection connection) throws DataAccessException {

			waitForLock(connection);
			connection.multi();

			put(element, connection);

			processKeyExpiration(element, connection);
			maintainKnownKeys(element, connection);

			connection.exec();
			return null;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.cache.RedisCache.AbstractRedisPutCallback#put(org.springframework.data.redis.cache.RedisCache.RedisCacheElement, org.springframework.data.redis.connection.RedisConnection)
		 */
		@Override
		protected Object put(RedisCacheElement element, RedisConnection connection) {

			connection.set(element.getKey(), convertToBytesIfNecessary(template.getValueSerializer(), element.get()));
			return null;
		}

	}

	/**
	 * @author Christoph Strobl
	 * @since 1.5
	 */
	class RedisCachePutIfAbsentCallback extends AbstractRedisPutCallback<Object> {

		public RedisCachePutIfAbsentCallback(RedisCacheElement element) {
			super(element);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.cache.RedisCache.AbstractRedisPutCallback#doInRedis(org.springframework.data.redis.cache.RedisCache.RedisCacheElement, org.springframework.data.redis.connection.RedisConnection)
		 */
		@Override
		public Object doInRedis(RedisCacheElement element, RedisConnection connection) throws DataAccessException {

			waitForLock(connection);
			Object resultValue = put(element, connection);

			if (ObjectUtils.nullSafeEquals(element.get(), resultValue)) {
				processKeyExpiration(element, connection);
				maintainKnownKeys(element, connection);
			}

			return resultValue;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.cache.RedisCache.AbstractRedisPutCallback#put(org.springframework.data.redis.cache.RedisCache.RedisCacheElement, org.springframework.data.redis.connection.RedisConnection)
		 */
		@Override
		protected Object put(RedisCacheElement element, RedisConnection connection) {

			boolean valueWasSet = connection.setNX(element.getKey(),
					convertToBytesIfNecessary(template.getValueSerializer(), element.get()));
			return valueWasSet ? element.get() : deserializeIfNecessary(template.getValueSerializer(),
					connection.get(element.getKey()));
		}
	}

	/**
	 * @author Christoph Strobl
	 * @since 1.5
	 */
	public static class RedisCacheElement extends SimpleValueWrapper {

		private final byte[] rawKey;
		private byte[] prefix;
		private long timeToLive;

		public RedisCacheElement(byte[] key, Object value) {
			super(value);
			this.rawKey = key;
		}

		public byte[] getKey() {
			return computeKey();
		}

		public byte[] getRawKey() {
			return rawKey;
		}

		public void setPrefix(byte[] prefix) {
			this.prefix = prefix;
		}

		public byte[] getPrefix() {
			return prefix;
		}

		public void setTimeToLive(long timeToLive) {
			this.timeToLive = timeToLive;
		}

		public long getTimeToLive() {
			return timeToLive;
		}

		public boolean hasKeyPrefix() {
			return (prefix != null && prefix.length > 0);
		}

		public boolean isEternal() {
			return 0 == timeToLive;
		}

		public RedisCacheElement usePrefix(byte[] prefix) {

			setPrefix(prefix);
			return this;
		}

		public RedisCacheElement expireAfter(long seconds) {

			setTimeToLive(seconds);
			return this;
		}

		protected byte[] computeKey() {

			if (!hasKeyPrefix()) {
				return rawKey;
			}

			byte[] prefixedKey = Arrays.copyOf(prefix, prefix.length + rawKey.length);
			System.arraycopy(rawKey, 0, prefixedKey, prefix.length, rawKey.length);

			return prefixedKey;
		}

	}

}
