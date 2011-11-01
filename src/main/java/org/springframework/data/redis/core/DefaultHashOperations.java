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
package org.springframework.data.redis.core;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.data.redis.connection.RedisConnection;

/**
 * Default implementation of {@link HashOperations}.
 *  
 * @author Costin Leau
 */
class DefaultHashOperations<K, HK, HV> extends AbstractOperations<K, Object> implements HashOperations<K, HK, HV> {

	@SuppressWarnings("unchecked")
	DefaultHashOperations(RedisTemplate<K, ?> template) {
		super((RedisTemplate<K, Object>) template);
	}

	@SuppressWarnings("unchecked")
	
	public HV get(K key, Object hashKey) {
		final byte[] rawKey = rawKey(key);
		final byte[] rawHashKey = rawHashKey(hashKey);

		byte[] rawHashValue = execute(new RedisCallback<byte[]>() {
			
			public byte[] doInRedis(RedisConnection connection) {
				return connection.hGet(rawKey, rawHashKey);
			}
		}, true);

		return (HV) deserializeHashValue(rawHashValue);
	}

	
	public Boolean hasKey(K key, Object hashKey) {
		final byte[] rawKey = rawKey(key);
		final byte[] rawHashKey = rawHashKey(hashKey);

		return execute(new RedisCallback<Boolean>() {
			
			public Boolean doInRedis(RedisConnection connection) {
				return connection.hExists(rawKey, rawHashKey);
			}
		}, true);
	}

	
	public Long increment(K key, HK hashKey, final long delta) {
		final byte[] rawKey = rawKey(key);
		final byte[] rawHashKey = rawHashKey(hashKey);

		return execute(new RedisCallback<Long>() {
			
			public Long doInRedis(RedisConnection connection) {
				return connection.hIncrBy(rawKey, rawHashKey, delta);
			}
		}, true);

	}

	
	public Set<HK> keys(K key) {
		final byte[] rawKey = rawKey(key);

		Set<byte[]> rawValues = execute(new RedisCallback<Set<byte[]>>() {
			
			public Set<byte[]> doInRedis(RedisConnection connection) {
				return connection.hKeys(rawKey);
			}
		}, true);

		return deserializeHashKeys(rawValues);
	}

	
	public Long size(K key) {
		final byte[] rawKey = rawKey(key);

		return execute(new RedisCallback<Long>() {
			
			public Long doInRedis(RedisConnection connection) {
				return connection.hLen(rawKey);
			}
		}, true);
	}

	
	public void putAll(K key, Map<? extends HK, ? extends HV> m) {
		if (m.isEmpty()) {
			return;
		}

		final byte[] rawKey = rawKey(key);

		final Map<byte[], byte[]> hashes = new LinkedHashMap<byte[], byte[]>(m.size());

		for (Map.Entry<? extends HK, ? extends HV> entry : m.entrySet()) {
			hashes.put(rawHashKey(entry.getKey()), rawHashValue(entry.getValue()));
		}

		execute(new RedisCallback<Object>() {
			
			public Object doInRedis(RedisConnection connection) {
				connection.hMSet(rawKey, hashes);
				return null;
			}
		}, true);
	}


	
	public Collection<HV> multiGet(K key, Collection<HK> fields) {
		if (fields.isEmpty()) {
			return Collections.emptyList();
		}

		final byte[] rawKey = rawKey(key);

		final byte[][] rawHashKeys = new byte[fields.size()][];

		int counter = 0;
		for (HK hashKey : fields) {
			rawHashKeys[counter++] = rawHashKey(hashKey);
		}

		List<byte[]> rawValues = execute(new RedisCallback<List<byte[]>>() {
			
			public List<byte[]> doInRedis(RedisConnection connection) {
				return connection.hMGet(rawKey, rawHashKeys);
			}
		}, true);

		return deserializeHashValues(rawValues);
	}

	
	public void put(K key, HK hashKey, HV value) {
		final byte[] rawKey = rawKey(key);
		final byte[] rawHashKey = rawHashKey(hashKey);
		final byte[] rawHashValue = rawHashValue(value);

		execute(new RedisCallback<Object>() {
			
			public Object doInRedis(RedisConnection connection) {
				connection.hSet(rawKey, rawHashKey, rawHashValue);
				return null;
			}
		}, true);
	}

	
	public Boolean putIfAbsent(K key, HK hashKey, HV value) {
		final byte[] rawKey = rawKey(key);
		final byte[] rawHashKey = rawHashKey(hashKey);
		final byte[] rawHashValue = rawHashValue(value);

		return execute(new RedisCallback<Boolean>() {
			
			public Boolean doInRedis(RedisConnection connection) {
				return connection.hSetNX(rawKey, rawHashKey, rawHashValue);
			}
		}, true);
	}


	
	public List<HV> values(K key) {
		final byte[] rawKey = rawKey(key);

		List<byte[]> rawValues = execute(new RedisCallback<List<byte[]>>() {
			
			public List<byte[]> doInRedis(RedisConnection connection) {
				return connection.hVals(rawKey);
			}
		}, true);

		return deserializeHashValues(rawValues);
	}

	
	public void delete(K key, Object hashKey) {
		final byte[] rawKey = rawKey(key);
		final byte[] rawHashKey = rawHashKey(hashKey);

		execute(new RedisCallback<Object>() {
			
			public Object doInRedis(RedisConnection connection) {
				connection.hDel(rawKey, rawHashKey);
				return null;
			}
		}, true);
	}

	
	public Map<HK, HV> entries(K key) {
		final byte[] rawKey = rawKey(key);

		Map<byte[], byte[]> entries = execute(new RedisCallback<Map<byte[], byte[]>>() {
			
			public Map<byte[], byte[]> doInRedis(RedisConnection connection) {
				return connection.hGetAll(rawKey);
			}
		}, true);

		return deserializeHashMap(entries);
	}
}