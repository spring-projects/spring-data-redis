/*
 * Copyright 2011 the original author or authors.
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
import org.springframework.cache.interceptor.DefaultValueWrapper;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.util.Assert;

/**
 * Cache implementation on top of Redis.
 * 
 * @author Costin Leau
 */
@SuppressWarnings("unchecked")
class RedisCache implements Cache {

	private static final int PAGE_SIZE = 128;
	private final String name;
	private final RedisTemplate template;
	private final byte[] prefix;
	private final byte[] setName;
	private final byte[] cacheLockName;
	private long WAIT_FOR_LOCK = 300;

	/**
	 * 
	 * Constructs a new <code>RedisCache</code> instance.
	 *
	 * @param name cache name
	 * @param prefix
	 * @param cachePrefix 
	 */
	RedisCache(String name, byte[] prefix, RedisTemplate<? extends Object, ? extends Object> template) {

		Assert.hasText(name, "non-empty cache name is required");
		this.name = name;
		this.template = template;
		this.prefix = prefix;

		StringRedisSerializer stringSerializer = new StringRedisSerializer();

		// name of the set holding the keys
		String sName = name + "~keys";
		this.setName = stringSerializer.serialize(sName);
		this.cacheLockName = stringSerializer.serialize(name + "~lock");
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	/**
	 * {@inheritDoc}
	 * 
	 * This implementation simply returns the RedisTemplate used for configuring the cache, giving access
	 * to the underlying Redis store.
	 */
	public Object getNativeCache() {
		return template;
	}

	@Override
	public ValueWrapper get(final Object key) {
		return (ValueWrapper) template.execute(new RedisCallback<ValueWrapper>() {
			@Override
			public ValueWrapper doInRedis(RedisConnection connection) throws DataAccessException {
				waitForLock(connection);
				byte[] bs = connection.get(computeKey(key));
				return (bs == null ? null : new DefaultValueWrapper(template.getValueSerializer().deserialize(bs)));
			}
		}, true);
	}

	@Override
	public void put(final Object key, final Object value) {
		final byte[] k = computeKey(key);

		template.execute(new RedisCallback<Object>() {
			public Object doInRedis(RedisConnection connection) throws DataAccessException {
				waitForLock(connection);
				connection.multi();
				connection.set(k, template.getValueSerializer().serialize(value));
				connection.zAdd(setName, 0, k);
				connection.exec();

				return null;
			}
		}, true);
	}

	@Override
	public void evict(Object key) {
		final byte[] k = computeKey(key);

		template.execute(new RedisCallback<Object>() {
			public Object doInRedis(RedisConnection connection) throws DataAccessException {
				connection.del(k);
				// remove key from set
				connection.zRem(setName, k);
				return null;
			}
		}, true);
	}

	@Override
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

					do {
						// need to paginate the keys
						Set<byte[]> keys = connection.zRange(setName, (offset) * PAGE_SIZE, (offset + 1) * PAGE_SIZE
								- 1);
						finished = keys.size() < PAGE_SIZE;
						offset++;
						if (!keys.isEmpty()) {
							connection.del(keys.toArray(new byte[keys.size()][]));
						}
					} while (!finished);

					connection.del(setName);
					return null;

				} finally {
					connection.del(cacheLockName);
				}
			}
		}, true);
	}

	private byte[] computeKey(Object key) {
		byte[] k = template.getKeySerializer().serialize(key);

		if (prefix == null || prefix.length == 0)
			return k;

		byte[] result = Arrays.copyOf(prefix, prefix.length + k.length);
		System.arraycopy(k, 0, result, prefix.length, k.length);
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
					Thread.currentThread().wait(WAIT_FOR_LOCK);
				} catch (InterruptedException ex) {
					// ignore
				}
				retry = true;
			}
		} while (retry);
		return foundLock;
	}
}