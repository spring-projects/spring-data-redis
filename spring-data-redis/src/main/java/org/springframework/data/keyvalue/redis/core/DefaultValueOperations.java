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
package org.springframework.data.keyvalue.redis.core;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.springframework.dao.DataAccessException;
import org.springframework.data.keyvalue.redis.connection.RedisConnection;

/**
 * Default implementation of {@link ValueOperations}.
 * 
 * @author Costin Leau
 */
class DefaultValueOperations<K, V> extends AbstractOperations<K, V> implements ValueOperations<K, V> {

	DefaultValueOperations(RedisTemplate<K, V> template) {
		super(template);
	}

	@Override
	public V get(final Object key) {

		return execute(new ValueDeserializingRedisCallback(key) {
			@Override
			protected byte[] inRedis(byte[] rawKey, RedisConnection connection) {
				return connection.get(rawKey);
			}
		}, true);
	}

	@Override
	public V getAndSet(K key, V newValue) {
		final byte[] rawValue = rawValue(newValue);
		return execute(new ValueDeserializingRedisCallback(key) {
			@Override
			protected byte[] inRedis(byte[] rawKey, RedisConnection connection) {
				return connection.getSet(rawKey, rawValue);
			}
		}, true);
	}

	@Override
	public Long increment(K key, final long delta) {
		final byte[] rawKey = rawKey(key);
		// TODO add conversion service in here ?
		return execute(new RedisCallback<Long>() {
			@Override
			public Long doInRedis(RedisConnection connection) {
				if (delta == 1) {
					return connection.incr(rawKey);
				}

				if (delta == -1) {
					return connection.decr(rawKey);
				}

				if (delta < 0) {
					return connection.decrBy(rawKey, delta);
				}

				return connection.incrBy(rawKey, delta);
			}
		}, true);
	}

	@Override
	public Integer append(K key, String value) {
		final byte[] rawKey = rawKey(key);
		final byte[] rawString = rawString(value);

		return execute(new RedisCallback<Integer>() {
			@Override
			public Integer doInRedis(RedisConnection connection) {
				return connection.append(rawKey, rawString).intValue();
			}
		}, true);
	}

	@Override
	public String get(K key, final long start, final long end) {
		final byte[] rawKey = rawKey(key);

		byte[] rawReturn = execute(new RedisCallback<byte[]>() {
			@Override
			public byte[] doInRedis(RedisConnection connection) {
				return connection.getRange(rawKey, start, end);
			}
		}, true);

		return deserializeString(rawReturn);
	}

	@SuppressWarnings("unchecked")
	@Override
	public List<V> multiGet(Collection<K> keys) {
		if (keys.isEmpty()) {
			return Collections.emptyList();
		}

		final byte[][] rawKeys = new byte[keys.size()][];

		int counter = 0;
		for (K hashKey : keys) {
			rawKeys[counter++] = rawKey(hashKey);
		}

		List<byte[]> rawValues = execute(new RedisCallback<List<byte[]>>() {
			@Override
			public List<byte[]> doInRedis(RedisConnection connection) {
				return connection.mGet(rawKeys);
			}
		}, true);

		return deserializeValues(rawValues);
	}

	@Override
	public void multiSet(Map<? extends K, ? extends V> m) {
		if (m.isEmpty()) {
			return;
		}

		final Map<byte[], byte[]> rawKeys = new LinkedHashMap<byte[], byte[]>(m.size());

		for (Map.Entry<? extends K, ? extends V> entry : m.entrySet()) {
			rawKeys.put(rawKey(entry.getKey()), rawValue(entry.getValue()));
		}

		execute(new RedisCallback<Object>() {
			@Override
			public Object doInRedis(RedisConnection connection) {
				connection.mSet(rawKeys);
				return null;
			}
		}, true);
	}

	@Override
	public void multiSetIfAbsent(Map<? extends K, ? extends V> m) {
		if (m.isEmpty()) {
			return;
		}

		final Map<byte[], byte[]> rawKeys = new LinkedHashMap<byte[], byte[]>(m.size());

		for (Map.Entry<? extends K, ? extends V> entry : m.entrySet()) {
			rawKeys.put(rawKey(entry.getKey()), rawValue(entry.getValue()));
		}

		execute(new RedisCallback<Object>() {
			@Override
			public Object doInRedis(RedisConnection connection) {
				connection.mSetNX(rawKeys);
				return null;
			}
		}, true);
	}

	@Override
	public void set(K key, V value) {
		final byte[] rawValue = rawValue(value);
		execute(new ValueDeserializingRedisCallback(key) {
			@Override
			protected byte[] inRedis(byte[] rawKey, RedisConnection connection) {
				connection.set(rawKey, rawValue);
				return null;
			}
		}, true);
	}

	@Override
	public void set(K key, V value, long timeout, TimeUnit unit) {
		final byte[] rawKey = rawKey(key);
		final byte[] rawValue = rawValue(value);
		final long rawTimeout = unit.toSeconds(timeout);

		execute(new RedisCallback<Object>() {
			@Override
			public Object doInRedis(RedisConnection connection) throws DataAccessException {
				connection.setEx(rawKey, (int) rawTimeout, rawValue);
				return null;
			}
		}, true);
	}

	@Override
	public Boolean setIfAbsent(K key, V value) {
		final byte[] rawKey = rawKey(key);
		final byte[] rawValue = rawValue(value);

		return execute(new RedisCallback<Boolean>() {
			@Override
			public Boolean doInRedis(RedisConnection connection) throws DataAccessException {
				return connection.setNX(rawKey, rawValue);
			}
		}, true);
	}


	@Override
	public void set(K key, final V value, final long offset) {
		final byte[] rawKey = rawKey(key);
		final byte[] rawValue = rawValue(value);

		execute(new RedisCallback<Object>() {
			@Override
			public Object doInRedis(RedisConnection connection) {
				connection.setRange(rawKey, rawValue, offset);
				return null;
			}
		}, true);
	}

	@Override
	public Long size(K key) {
		final byte[] rawKey = rawKey(key);

		return execute(new RedisCallback<Long>() {
			@Override
			public Long doInRedis(RedisConnection connection) {
				return connection.strLen(rawKey);
			}
		}, true);
	}
}