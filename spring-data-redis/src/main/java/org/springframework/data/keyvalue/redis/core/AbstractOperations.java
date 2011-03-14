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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.data.keyvalue.redis.connection.RedisConnection;
import org.springframework.data.keyvalue.redis.serializer.RedisSerializer;
import org.springframework.util.Assert;

/**
 * Internal base class used by various RedisTemplate XXXOperations implementations.
 * 
 * @author Costin Leau
 */
abstract class AbstractOperations<K, V> {

	// utility methods for the template internal methods
	abstract class ValueDeserializingRedisCallback implements RedisCallback<V> {
		private Object key;

		public ValueDeserializingRedisCallback(Object key) {
			this.key = key;
		}

		@Override
		public final V doInRedis(RedisConnection connection) {
			byte[] result = inRedis(rawKey(key), connection);
			return deserializeValue(result);
		}

		protected abstract byte[] inRedis(byte[] rawKey, RedisConnection connection);
	}

	RedisSerializer keySerializer = null;
	RedisSerializer valueSerializer = null;
	RedisSerializer hashKeySerializer = null;
	RedisSerializer hashValueSerializer = null;
	RedisSerializer stringSerializer = null;
	RedisTemplate<K, V> template;

	AbstractOperations(RedisTemplate<K, V> template) {
		keySerializer = template.getKeySerializer();
		valueSerializer = template.getValueSerializer();
		hashKeySerializer = template.getHashKeySerializer();
		hashValueSerializer = template.getHashValueSerializer();
		stringSerializer = template.getStringSerializer();

		this.template = template;
	}


	<T> T execute(RedisCallback<T> callback, boolean b) {
		return template.execute(callback, b);
	}

	public RedisOperations<K, V> getOperations() {
		return template;
	}

	@SuppressWarnings("unchecked")
	byte[] rawKey(Object key) {
		Assert.notNull(key, "non null key required");
		return keySerializer.serialize(key);
	}

	byte[] rawString(String key) {
		return stringSerializer.serialize(key);
	}

	@SuppressWarnings("unchecked")
	byte[] rawValue(Object value) {
		return valueSerializer.serialize(value);
	}

	@SuppressWarnings("unchecked")
	<HK> byte[] rawHashKey(HK hashKey) {
		Assert.notNull(hashKey, "non null hash key required");
		return hashKeySerializer.serialize(hashKey);
	}

	@SuppressWarnings("unchecked")
	<HV> byte[] rawHashValue(HV value) {
		return hashValueSerializer.serialize(value);
	}

	byte[][] rawKeys(K key, K otherKey) {
		final byte[][] rawKeys = new byte[2][];


		rawKeys[0] = rawKey(key);
		rawKeys[1] = rawKey(key);
		return rawKeys;
	}

	byte[][] rawKeys(Collection<K> keys) {
		return rawKeys(null, keys);
	}

	byte[][] rawKeys(K key, Collection<K> keys) {
		final byte[][] rawKeys = new byte[keys.size() + (key != null ? 1 : 0)][];

		int i = 0;

		if (key != null) {
			rawKeys[i++] = rawKey(key);
		}

		for (K k : keys) {
			rawKeys[i++] = rawKey(k);
		}

		return rawKeys;
	}

	<T extends Collection<V>> T deserializeValues(Collection<byte[]> rawValues, Class<T> type) {
		return SerializationUtils.deserializeValues(rawValues, type, valueSerializer);
	}

	@SuppressWarnings("unchecked")
	<T> Set<T> deserializeHashKeys(Collection<byte[]> rawKeys) {
		return SerializationUtils.deserializeValues(rawKeys, Set.class, hashKeySerializer);
	}

	@SuppressWarnings("unchecked")
	<T> List<T> deserializeHashValues(Collection<byte[]> rawValues) {
		return SerializationUtils.deserializeValues(rawValues, List.class, hashValueSerializer);
	}

	@SuppressWarnings("unchecked")
	<HK, HV> Map<HK, HV> deserializeHashMap(Map<byte[], byte[]> entries) {
		Map<HK, HV> map = new LinkedHashMap<HK, HV>(entries.size());

		for (Map.Entry<byte[], byte[]> entry : entries.entrySet()) {
			map.put((HK) deserializeHashKey(entry.getKey()), (HV) deserializeHashValue(entry.getValue()));
		}

		return map;
	}

	@SuppressWarnings("unchecked")
	K deserializeKey(byte[] value) {
		return (K) SerializationUtils.deserialize(value, keySerializer);
	}

	@SuppressWarnings("unchecked")
	V deserializeValue(byte[] value) {
		return (V) SerializationUtils.deserialize(value, valueSerializer);
	}

	@SuppressWarnings("unchecked")
	String deserializeString(byte[] value) {
		return (String) SerializationUtils.deserialize(value, stringSerializer);
	}

	@SuppressWarnings( { "unchecked" })
	<HK> HK deserializeHashKey(byte[] value) {
		return (HK) SerializationUtils.deserialize(value, hashKeySerializer);
	}

	@SuppressWarnings("unchecked")
	<HV> HV deserializeHashValue(byte[] value) {
		return (HV) SerializationUtils.deserialize(value, hashValueSerializer);
	}
}