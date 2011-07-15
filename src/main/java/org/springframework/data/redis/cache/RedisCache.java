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
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.data.redis.support.collections.DefaultRedisSet;
import org.springframework.data.redis.support.collections.RedisSet;
import org.springframework.util.Assert;

/**
 * Cache implementation on top of Redis.
 * 
 * @author Costin Leau
 */
@SuppressWarnings("unchecked")
public class RedisCache implements Cache {

	private final String name;
	private final RedisSet<Object> keys;
	private final RedisTemplate template;
	private final byte[] nameAsBytes;
	private final byte[] setName;

	/**
	 * 
	 * Constructs a new <code>RedisCache</code> instance.
	 *
	 * @param name cache name
	 */
	public RedisCache(String name, RedisTemplate<? extends Object, ? extends Object> template) {
		Assert.hasText(name, "non-empty cache name is required");
		this.name = name;
		this.template = template;

		StringRedisSerializer stringSerializer = new StringRedisSerializer();

		this.nameAsBytes = stringSerializer.serialize(name + ":");


		// extract connection and create an ops for set
		RedisConnectionFactory connectionFactory = template.getConnectionFactory();
		RedisTemplate<String, Object> setTemplate = new RedisTemplate<String, Object>();
		setTemplate.setConnectionFactory(connectionFactory);
		setTemplate.setDefaultSerializer(template.getDefaultSerializer());
		setTemplate.setKeySerializer(stringSerializer);

		// the values of the set are the cache keys
		setTemplate.setValueSerializer(template.getKeySerializer());
		setTemplate.afterPropertiesSet();

		String sName = name + "-keys";
		this.keys = new DefaultRedisSet<Object>(sName, setTemplate);
		this.setName = stringSerializer.serialize(sName);
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
				byte[] bs = connection.get(computeKey(key));
				return (bs == null ? null : new DefaultValueWrapper(template.getValueSerializer().deserialize(bs)));
			}
		});
	}

	@Override
	public void put(final Object key, final Object value) {
		final byte[] k = computeKey(key);
		
		template.execute(new RedisCallback<Object>() {
			public Object doInRedis(RedisConnection connection) throws DataAccessException {
				connection.set(k, template.getValueSerializer().serialize(value));
				connection.sAdd(setName, k);
				return null;
			}
		});
	}

	@Override
	public void evict(Object key) {
		final byte[] k = computeKey(key);

		template.execute(new RedisCallback<Object>() {
			public Object doInRedis(RedisConnection connection) throws DataAccessException {
				connection.del(k);
				// remove key from set
				connection.sRem(setName, k);
				return null;
			}
		});
	}

	@Override
	public void clear() {
		// need to del each key individually
		template.execute(new RedisCallback<Object>() {
			public Object doInRedis(RedisConnection connection) throws DataAccessException {
				// need to paginate the keys
				Set<byte[]> keys = connection.sMembers(setName);
				for (byte[] bs : keys) {
					connection.del(bs);
				}
				connection.del(setName);
				return null;
			}
		});
	}

	private byte[] computeKey(Object key) {
		RedisSerializer keySerializer = template.getKeySerializer();
		byte[] k = keySerializer.serialize(key);

		byte[] result = Arrays.copyOf(nameAsBytes, nameAsBytes.length + k.length);
		System.arraycopy(k, 0, result, nameAsBytes.length, k.length);
		return result;
	}
}