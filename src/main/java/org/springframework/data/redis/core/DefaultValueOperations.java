/*
 * Copyright 2011-2021 the original author or authors.
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
package org.springframework.data.redis.core;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.BitFieldSubCommands;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisStringCommands.SetOption;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.lang.Nullable;

/**
 * Default implementation of {@link ValueOperations}.
 *
 * @author Costin Leau
 * @author Jennifer Hickey
 * @author Christoph Strobl
 * @author Jiahe Cai
 */
class DefaultValueOperations<K, V> extends AbstractOperations<K, V> implements ValueOperations<K, V> {

	DefaultValueOperations(RedisTemplate<K, V> template) {
		super(template);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ValueOperations#get(java.lang.Object)
	 */
	@Override
	public V get(Object key) {

		return execute(new ValueDeserializingRedisCallback(key) {

			@Override
			protected byte[] inRedis(byte[] rawKey, RedisConnection connection) {
				return connection.get(rawKey);
			}
		}, true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ValueOperations#getAndDelete(java.lang.Object)
	 */
	@Nullable
	@Override
	public V getAndDelete(K key) {

		return execute(new ValueDeserializingRedisCallback(key) {

			@Override
			protected byte[] inRedis(byte[] rawKey, RedisConnection connection) {
				return connection.getDel(rawKey);
			}
		}, true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ValueOperations#getAndPersist(java.lang.Object, long, java.util.concurrent.TimeUnit)
	 */
	@Nullable
	@Override
	public V getAndExpire(K key, long timeout, TimeUnit unit) {

		return execute(new ValueDeserializingRedisCallback(key) {

			@Override
			protected byte[] inRedis(byte[] rawKey, RedisConnection connection) {
				return connection.getEx(rawKey, Expiration.from(timeout, unit));
			}
		}, true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ValueOperations#getAndPersist(java.lang.Object, java.time.Duration)
	 */
	@Nullable
	@Override
	public V getAndExpire(K key, Duration timeout) {

		return execute(new ValueDeserializingRedisCallback(key) {

			@Override
			protected byte[] inRedis(byte[] rawKey, RedisConnection connection) {
				return connection.getEx(rawKey, Expiration.from(timeout));
			}
		}, true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ValueOperations#getAndPersist(java.lang.Object)
	 */
	@Nullable
	@Override
	public V getAndPersist(K key) {

		return execute(new ValueDeserializingRedisCallback(key) {

			@Override
			protected byte[] inRedis(byte[] rawKey, RedisConnection connection) {
				return connection.getEx(rawKey, Expiration.persistent());
			}
		}, true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ValueOperations#getAndSet(java.lang.Object, java.lang.Object)
	 */
	@Override
	public V getAndSet(K key, V newValue) {

		byte[] rawValue = rawValue(newValue);
		return execute(new ValueDeserializingRedisCallback(key) {

			@Override
			protected byte[] inRedis(byte[] rawKey, RedisConnection connection) {
				return connection.getSet(rawKey, rawValue);
			}
		}, true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ValueOperations#increment(java.lang.Object)
	 */
	@Override
	public Long increment(K key) {

		byte[] rawKey = rawKey(key);
		return execute(connection -> connection.incr(rawKey), true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ValueOperations#increment(java.lang.Object, long)
	 */
	@Override
	public Long increment(K key, long delta) {

		byte[] rawKey = rawKey(key);
		return execute(connection -> connection.incrBy(rawKey, delta), true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ValueOperations#increment(java.lang.Object, double)
	 */
	@Override
	public Double increment(K key, double delta) {

		byte[] rawKey = rawKey(key);
		return execute(connection -> connection.incrBy(rawKey, delta), true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ValueOperations#decrement(java.lang.Object)
	 */
	@Override
	public Long decrement(K key) {

		byte[] rawKey = rawKey(key);
		return execute(connection -> connection.decr(rawKey), true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ValueOperations#increment(java.lang.Object, long)
	 */
	@Override
	public Long decrement(K key, long delta) {

		byte[] rawKey = rawKey(key);
		return execute(connection -> connection.decrBy(rawKey, delta), true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ValueOperations#append(java.lang.Object, java.lang.String)
	 */
	@Override
	public Integer append(K key, String value) {

		byte[] rawKey = rawKey(key);
		byte[] rawString = rawString(value);

		return execute(connection -> {
			Long result = connection.append(rawKey, rawString);
			return (result != null) ? result.intValue() : null;
		}, true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ValueOperations#get(java.lang.Object, long, long)
	 */
	@Override
	public String get(K key, long start, long end) {
		byte[] rawKey = rawKey(key);
		byte[] rawReturn = execute(connection -> connection.getRange(rawKey, start, end), true);

		return deserializeString(rawReturn);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ValueOperations#multiGet(java.util.Collection)
	 */
	@Override
	public List<V> multiGet(Collection<K> keys) {

		if (keys.isEmpty()) {
			return Collections.emptyList();
		}

		byte[][] rawKeys = new byte[keys.size()][];

		int counter = 0;
		for (K hashKey : keys) {
			rawKeys[counter++] = rawKey(hashKey);
		}

		List<byte[]> rawValues = execute(connection -> connection.mGet(rawKeys), true);

		return deserializeValues(rawValues);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ValueOperations#multiSet(java.util.Map)
	 */
	@Override
	public void multiSet(Map<? extends K, ? extends V> m) {

		if (m.isEmpty()) {
			return;
		}

		Map<byte[], byte[]> rawKeys = new LinkedHashMap<>(m.size());

		for (Map.Entry<? extends K, ? extends V> entry : m.entrySet()) {
			rawKeys.put(rawKey(entry.getKey()), rawValue(entry.getValue()));
		}

		execute(connection -> {
			connection.mSet(rawKeys);
			return null;
		}, true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ValueOperations#multiSetIfAbsent(java.util.Map)
	 */
	@Override
	public Boolean multiSetIfAbsent(Map<? extends K, ? extends V> m) {

		if (m.isEmpty()) {
			return true;
		}

		Map<byte[], byte[]> rawKeys = new LinkedHashMap<>(m.size());

		for (Map.Entry<? extends K, ? extends V> entry : m.entrySet()) {
			rawKeys.put(rawKey(entry.getKey()), rawValue(entry.getValue()));
		}

		return execute(connection -> connection.mSetNX(rawKeys), true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ValueOperations#set(java.lang.Object, java.lang.Object)
	 */
	@Override
	public void set(K key, V value) {

		byte[] rawValue = rawValue(value);
		execute(new ValueDeserializingRedisCallback(key) {

			@Override
			protected byte[] inRedis(byte[] rawKey, RedisConnection connection) {
				connection.set(rawKey, rawValue);
				return null;
			}
		}, true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ValueOperations#set(java.lang.Object, java.lang.Object, long, java.util.concurrent.TimeUnit)
	 */
	@Override
	public void set(K key, V value, long timeout, TimeUnit unit) {

		byte[] rawKey = rawKey(key);
		byte[] rawValue = rawValue(value);

		execute(new RedisCallback<Object>() {

			@Override
			public Object doInRedis(RedisConnection connection) throws DataAccessException {

				potentiallyUsePsetEx(connection);
				return null;
			}

			public void potentiallyUsePsetEx(RedisConnection connection) {

				if (!TimeUnit.MILLISECONDS.equals(unit) || !failsafeInvokePsetEx(connection)) {
					connection.setEx(rawKey, TimeoutUtils.toSeconds(timeout, unit), rawValue);
				}
			}

			private boolean failsafeInvokePsetEx(RedisConnection connection) {

				boolean failed = false;
				try {
					connection.pSetEx(rawKey, timeout, rawValue);
				} catch (UnsupportedOperationException e) {
					// in case the connection does not support pSetEx return false to allow fallback to other operation.
					failed = true;
				}
				return !failed;
			}

		}, true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ValueOperations#setIfAbsent(java.lang.Object, java.lang.Object)
	 */
	@Override
	public Boolean setIfAbsent(K key, V value) {

		byte[] rawKey = rawKey(key);
		byte[] rawValue = rawValue(value);
		return execute(connection -> connection.setNX(rawKey, rawValue), true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ValueOperations#setIfAbsent(java.lang.Object, java.lang.Object, long, java.util.concurrent.TimeUnit)
	 */
	@Override
	public Boolean setIfAbsent(K key, V value, long timeout, TimeUnit unit) {

		byte[] rawKey = rawKey(key);
		byte[] rawValue = rawValue(value);

		Expiration expiration = Expiration.from(timeout, unit);
		return execute(connection -> connection.set(rawKey, rawValue, expiration, SetOption.ifAbsent()), true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ValueOperations#setIfPresent(java.lang.Object, java.lang.Object)
	 */
	@Nullable
	@Override
	public Boolean setIfPresent(K key, V value) {

		byte[] rawKey = rawKey(key);
		byte[] rawValue = rawValue(value);

		return execute(connection -> connection.set(rawKey, rawValue, Expiration.persistent(), SetOption.ifPresent()), true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ValueOperations#setIfPresent(java.lang.Object, java.lang.Object, long, java.util.concurrent.TimeUnit)
	 */
	@Nullable
	@Override
	public Boolean setIfPresent(K key, V value, long timeout, TimeUnit unit) {

		byte[] rawKey = rawKey(key);
		byte[] rawValue = rawValue(value);

		Expiration expiration = Expiration.from(timeout, unit);
		return execute(connection -> connection.set(rawKey, rawValue, expiration, SetOption.ifPresent()), true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ValueOperations#set(java.lang.Object, java.lang.Object, long)
	 */
	@Override
	public void set(K key, V value, long offset) {

		byte[] rawKey = rawKey(key);
		byte[] rawValue = rawValue(value);

		execute(connection -> {
			connection.setRange(rawKey, rawValue, offset);
			return null;
		}, true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ValueOperations#size(java.lang.Object)
	 */
	@Override
	public Long size(K key) {

		byte[] rawKey = rawKey(key);
		return execute(connection -> connection.strLen(rawKey), true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ValueOperations#setBit(java.lang.Object, long, boolean)
	 */
	@Override
	public Boolean setBit(K key, long offset, boolean value) {

		byte[] rawKey = rawKey(key);
		return execute(connection -> connection.setBit(rawKey, offset, value), true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ValueOperations#getBit(java.lang.Object, long)
	 */
	@Override
	public Boolean getBit(K key, long offset) {

		byte[] rawKey = rawKey(key);
		return execute(connection -> connection.getBit(rawKey, offset), true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ValueOperations#bitfield(Object, RedisStringCommands.BitfieldCommand)
	 */
	@Override
	public List<Long> bitField(K key, final BitFieldSubCommands subCommands) {

		byte[] rawKey = rawKey(key);
		return execute(connection -> connection.bitField(rawKey, subCommands), true);
	}
}
